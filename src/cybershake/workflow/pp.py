"""Post-processing workflow generation."""

from __future__ import annotations

import math
import re
import shutil
from typing import TYPE_CHECKING

from sqlalchemy.exc import SQLAlchemyError

from Pegasus.api import File, Job, SubWorkflow, Workflow

from cybershake.constants import (
    AWP_SGT_IDS,
    CARC_GLOBUS_ENDPOINT,
    DEFAULT_JOB_DB_CREDENTIALS,
    DEFAULT_PP_SEISMOGRAM_LENGTH_SECONDS,
    DEFAULT_PP_SEISMOGRAM_MEMORY_MB,
    DIRECT_SYNTH_VERSIONS,
)
from cybershake.exc import PpError, RunManagerError
from cybershake.runmanager import db as run_db
from cybershake.workflow.common import (
    RunInfo,
    add_config_metadata,
    add_local_replica,
    add_replica,
    create_db_products_workflow,
    dependency,
    final_outputs,
    job,
    load_run_info,
    parse_rupture_list,
    update_job,
    write_workflow,
)

if TYPE_CHECKING:
    from pathlib import Path

    from cybershake.config import Config
    from cybershake.runmanager.db import SQLAlchemyRunDB
    from cybershake.runmanager.schema import Rupture, RuptureVariation

_RUPTURE_VARIATION = re.compile(r"(?:variation-)?r(\d+)$")
_SLIP_HYPO = re.compile(r"variation-s(\d+)-h(\d+)$")


class PP:
    """Generate a post-processing workflow hierarchy."""

    _database: SQLAlchemyRunDB | None = None

    def __init__(
        self,
        *,
        config: Config,
        database: SQLAlchemyRunDB | None = None,
    ) -> None:
        """Store options shared by the post-processing workflow builders.

        Args:
            config (Config): Resolved CyberShake configuration.
            database (SQLAlchemyRunDB | None, optional): Query repository.
                Defaults to a connection created from ``server``.
        """
        self.no_insert = config.no_insert
        self.high_frequency = config.high_frequency
        self.no_mpi_cluster = config.no_mpi_cluster
        self.dir_hierarchy = config.dir_hierarchy
        self.rotd = config.rotd
        self.skip_md5 = config.skip_md5
        self.nonblocking_md5 = config.nonblocking_md5
        self.direct_synth = config.direct_synth
        self.debug = config.debug
        self.pp_site = config.pp_site
        self.spacing = config.pp_spacing if config.command == "full" else config.spacing
        self.server = config.server
        self.duration = config.duration
        self.seismogram_length = config.seismogram_length
        self.db_rv_seed = config.db_rv_seed
        self.z_comp = config.z_comp
        self.handoff = config.handoff
        self.period_duration = config.period_duration
        self.no_vertical_response = config.no_vertical_response
        self.rupture_list = config.rupture_list
        self.config = config
        if database is not None:
            self._database = database

    def create(self, *, run_id: int, run_directory: Path) -> None:
        """Generate the complete post-processing workflow hierarchy.

        Args:
            run_id (int): Run represented by the workflows.
            run_directory (Path): Directory receiving workflows and inputs.

        Raises:
            PpError: If metadata or rupture data cannot be loaded or generated.
        """
        info, pre_file, synthesis_file, database_file, post_file = (
            self.create_sub_workflows(run_id=run_id, run_directory=run_directory)
        )

        workflow = Workflow(f"CyberShake_{info.site}", infer_dependencies=False)
        add_config_metadata(workflow, self.config)
        pre = SubWorkflow(pre_file.name, _id="preDAX")
        pre_args = ["--force", "-q"]
        if self.pp_site is not None:
            pre_args.extend(("-s", f"{self.pp_site},shock"))
        pre.add_args(*pre_args)
        synthesis = SubWorkflow(synthesis_file.name, _id="dax_0")
        synthesis.add_args(
            "--cluster",
            "horizontal" if self.no_mpi_cluster else "label",
            "--force",
            "-q",
            "--output-sites",
            "shock",
        )
        synthesis.add_dagman_profiles(category="subwf")
        post = SubWorkflow(post_file.name, _id="postDax")
        post.add_args("--force", "-q")
        workflow.add_jobs(pre, synthesis, post)
        for node_file in (pre_file, synthesis_file, post_file):
            add_local_replica(workflow, File(node_file.name), node_file)
        if not self.nonblocking_md5:
            dependency(workflow, pre, synthesis)

        if database_file is None:
            dependency(workflow, synthesis, post)
        else:
            products = SubWorkflow(database_file.name, _id="dbDax")
            products.add_args("--force", "-q")
            workflow.add_jobs(products)
            add_local_replica(workflow, File(database_file.name), database_file)
            dependency(workflow, synthesis, products)
            dependency(workflow, products, post)

        write_workflow(workflow, run_directory / f"CyberShake_{info.site}.dax")

    def create_sub_workflows(
        self,
        *,
        run_id: int,
        run_directory: Path,
    ) -> tuple[RunInfo, Path, Path, Path | None, Path]:
        """Generate the child workflows shared by PP and integrated workflows.

        Args:
            run_id (int): Run represented by the workflows.
            run_directory (Path): Directory receiving workflows and inputs.

        Returns:
            tuple[RunInfo, Path, Path, Path | None, Path]: Run metadata and the
                pre, synthesis, optional database-products, and post workflow
                paths.

        Raises:
            PpError: If metadata or rupture data cannot be loaded or generated.
        """
        if self.high_frequency is not None and not self.direct_synth:
            raise PpError("High-frequency post-processing requires DirectSynth")
        info = self._run_info(run_id)
        try:
            updated = self._database_repository().update_run_frequencies(
                run_id,
                max_frequency=(
                    info.max_frequency
                    if self.high_frequency is not None
                    else info.low_frequency
                ),
                low_frequency_cutoff=(
                    self.high_frequency
                    if self.high_frequency is not None
                    else info.low_frequency
                ),
            )
        except (SQLAlchemyError, ValueError) as error:
            raise PpError(f"Unable to update frequencies for run {run_id}") from error
        if not updated:
            raise PpError(f"Run {run_id} disappeared while updating frequencies")
        run_directory.mkdir(parents=True, exist_ok=True)
        pre_file = run_directory / f"CyberShake_{info.site}_pre.dax"
        synthesis_file = run_directory / (
            f"CyberShake_{info.site}_Synth.dax"
            if self.direct_synth
            else f"CyberShake_{info.site}_0.dax"
        )
        database_file = run_directory / f"CyberShake_{info.site}_DB_Products.dax"
        post_file = run_directory / f"CyberShake_{info.site}_post.dax"

        self.create_pre(workflow_file=pre_file, run_id=run_id, site=info.site)
        self.create_synthesis(
            workflow_file=synthesis_file,
            run_id=run_id,
            workflow_index=0,
        )
        if not self.no_insert:
            self.create_db_products(
                workflow_file=database_file,
                run_id=run_id,
                sub_workflow_count=1,
                velocity_file=None,
            )
        self.create_post(workflow_file=post_file, run_id=run_id, site=info.site)
        return (
            info,
            pre_file,
            synthesis_file,
            None if self.no_insert else database_file,
            post_file,
        )

    def create_pre(
        self,
        *,
        workflow_file: Path,
        run_id: int,
        site: str,
    ) -> None:
        """Generate the workflow that prepares post-processing inputs.

        Args:
            workflow_file (Path): Destination workflow file.
            run_id (int): Run represented by the workflow.
            site (str): Expected CyberShake site short name.

        Raises:
            PpError: If the run is missing or belongs to another site.
        """
        info = self._run_info(run_id)
        if info.site != site:
            raise PpError(f"Run {run_id} belongs to {info.site}, not {site}")
        workflow = Workflow(f"CyberShake_{site}_pre", infer_dependencies=False)
        update = update_job(run_id, "PP_INIT", "PP_START")
        workflow.add_jobs(update)
        components = ("x", "y", "z") if self.z_comp else ("x", "y")
        for component in components:
            check = self._check_sgt_job(info, component)
            workflow.add_jobs(check)
            dependency(workflow, update, check)
        write_workflow(workflow, workflow_file)

    def create_synthesis(
        self,
        *,
        workflow_file: Path,
        run_id: int,
        workflow_index: int,
    ) -> None:
        """Generate a post-processing synthesis workflow.

        Args:
            workflow_file (Path): Destination workflow file.
            run_id (int): Run represented by the workflow.
            workflow_index (int): Zero-based synthesis-workflow index.

        Raises:
            PpError: If run or rupture metadata is invalid.
        """
        info = self._run_info(run_id)
        workflow_file.parent.mkdir(parents=True, exist_ok=True)
        workflow_name = (
            f"CyberShake_{info.site}_Synth"
            if self.direct_synth
            else f"CyberShake_{info.site}_{workflow_index}"
        )
        workflow = Workflow(workflow_name, infer_dependencies=False)
        if self.direct_synth:
            self._add_direct_synth(workflow, info, workflow_file.parent)
        else:
            self._add_synthesis_jobs(
                workflow,
                info,
                workflow_file.parent,
                workflow_index,
            )
        write_workflow(workflow, workflow_file)

    def create_db_products(
        self,
        *,
        workflow_file: Path,
        run_id: int,
        sub_workflow_count: int,
        velocity_file: Path | None,
    ) -> None:
        """Generate the post-processing database-products workflow.

        Args:
            workflow_file (Path): Destination workflow file.
            run_id (int): Run represented by the workflow.
            sub_workflow_count (int): Number of synthesis workflows. Retained
                for the public generator contract; active file-forwarding does
                not consume per-workflow zip files.
            velocity_file (Path | None): Optional broadband Vs30 file.

        Raises:
            PpError: If product workflow construction fails.
        """
        del sub_workflow_count
        info = self._run_info(run_id)
        try:
            workflow = create_db_products_workflow(
                info=info,
                server=self.server,
                calculate_rotd=self.rotd,
                calculate_durations=self.duration,
                stochastic=False,
                velocity_file=velocity_file,
                rupture_list=self.rupture_list,
            )
        except ValueError as error:
            raise PpError(str(error)) from error
        write_workflow(workflow, workflow_file)

    def create_post(
        self,
        *,
        workflow_file: Path,
        run_id: int,
        site: str,
    ) -> None:
        """Generate the workflow that finalizes post-processing.

        Args:
            workflow_file (Path): Destination workflow file.
            run_id (int): Run represented by the workflow.
            site (str): Expected CyberShake site short name.

        Raises:
            PpError: If the run is missing or belongs to another site.
        """
        info = self._run_info(run_id)
        if info.site != site:
            raise PpError(f"Run {run_id} belongs to {info.site}, not {site}")
        workflow = Workflow(f"CyberShake_{site}_post", infer_dependencies=False)
        workflow.add_jobs(update_job(run_id, "PP_START", "PP_END"))
        if self.handoff:
            handoff = job("Handoff", "Handoff")
            handoff.add_args(
                "handoff",
                f"-r {run_id}",
                "-s BB",
                f"-d {DEFAULT_JOB_DB_CREDENTIALS}",
            )
            workflow.add_jobs(handoff)
        write_workflow(workflow, workflow_file)

    def _database_repository(self) -> SQLAlchemyRunDB:
        """Return the injected or configured database repository.

        Returns:
            SQLAlchemyRunDB: Repository used by the generator.

        Raises:
            PpError: If a configured connection cannot be created.
        """
        if self._database is not None:
            return self._database
        try:
            return run_db.connect(server=self.server, config=self.config)
        except RunManagerError as error:
            raise PpError(str(error)) from error

    def _run_info(self, run_id: int) -> RunInfo:
        """Load common metadata for a post-processing run.

        Args:
            run_id (int): CyberShake run ID.

        Returns:
            RunInfo: Validated run and site metadata.

        Raises:
            PpError: If database access or validation fails.
        """
        try:
            return load_run_info(self._database_repository(), run_id)
        except (SQLAlchemyError, ValueError) as error:
            raise PpError(f"Unable to load run {run_id}: {error}") from error

    def _check_sgt_job(self, info: RunInfo, component: str) -> Job:
        """Build one SGT checksum-verification job.

        Args:
            info (RunInfo): Run metadata.
            component (str): Component letter: ``x``, ``y``, or ``z``.

        Returns:
            Job: Configured verification job.
        """
        node = job(
            "CheckSgtNoSums" if self.skip_md5 else "CheckSgt",
            f"CheckSgt_{info.site}_{component}",
        )
        sgt = File(f"{info.site}_f{component}_{info.run_id}.sgt")
        checksum = File(f"{sgt.lfn}.md5")
        node.add_args(sgt.lfn, checksum.lfn)
        node.add_inputs(sgt, checksum)
        if info.sgt_variation_id in AWP_SGT_IDS:
            node.add_inputs(File(f"{info.site}_f{component}_{info.run_id}.sgthead"))
        if self.nonblocking_md5:
            node.add_dagman_profiles(abort_dag_on="1 RETURN 1")
        return node

    def _selected_ruptures(self, info: RunInfo) -> tuple[Rupture, ...]:
        """Load ruptures selected for PP generation.

        Args:
            info (RunInfo): Run metadata.

        Returns:
            tuple[Rupture, ...]: Selected ruptures in legacy size order.

        Raises:
            PpError: If the rupture list is invalid or selects no ruptures.
        """
        try:
            selected = parse_rupture_list(self.rupture_list)
            ruptures = self._database_repository().get_ruptures_for_site(
                info.site,
                erf_id=info.erf_id,
                source_ruptures=selected,
            )
        except (OSError, SQLAlchemyError, ValueError) as error:
            message = f"Unable to load ruptures for {info.site}: {error}"
            raise PpError(message) from error
        if not ruptures:
            raise PpError(f"No ruptures found for site {info.site}")
        return ruptures

    def _variations(
        self,
        info: RunInfo,
        rupture: Rupture,
    ) -> tuple[RuptureVariation, ...]:
        """Load variations for one rupture.

        Args:
            info (RunInfo): Run metadata.
            rupture (Rupture): Rupture whose variations are needed.

        Returns:
            tuple[RuptureVariation, ...]: Variations ordered by ID.

        Raises:
            PpError: If no variations exist.
        """
        variations = self._database_repository().get_rupture_variations(
            erf_id=info.erf_id,
            rupture_variation_id=info.rupture_variation_id,
            source_ruptures=((rupture.source_id, rupture.rupture_id),),
        )
        if not variations:
            raise PpError(
                f"No variations found for source {rupture.source_id}, rupture "
                f"{rupture.rupture_id}"
            )
        return variations

    def _add_synthesis_jobs(
        self,
        workflow: Workflow,
        info: RunInfo,
        directory: Path,
        workflow_index: int,
    ) -> None:
        """Add active Extract-SGT and SeisPSA jobs.

        Args:
            workflow (Workflow): Synthesis workflow being assembled.
            info (RunInfo): Run metadata.
            directory (Path): Directory receiving the extraction list.
            workflow_index (int): Synthesis workflow index.
        """
        ruptures = self._selected_ruptures(info)
        list_file = directory / f"rupture_file_list_{info.site}_{workflow_index}"
        geometry_names = [
            f"e{info.erf_id}_rv{info.rupture_variation_id}_"
            f"{rupture.source_id}_{rupture.rupture_id}.txt"
            for rupture in ruptures
        ]
        list_file.write_text(
            f"{len(geometry_names)}\n" + "\n".join(geometry_names) + "\n",
            encoding="utf-8",
        )
        extract = self._extract_sgt_job(info, workflow_index, list_file.name)
        workflow.add_jobs(extract)
        add_local_replica(workflow, File(list_file.name), list_file)

        for count, rupture in enumerate(ruptures, start=1):
            variations = self._variations(info, rupture)
            for variation_index, variation in enumerate(variations):
                synthesis = self._seis_psa_job(
                    info,
                    rupture,
                    variation,
                    variation_index=variation_index,
                    group=count,
                    workflow_index=workflow_index,
                )
                workflow.add_jobs(synthesis)
                dependency(workflow, extract, synthesis)

    def _extract_sgt_job(
        self,
        info: RunInfo,
        workflow_index: int,
        rupture_list: str,
    ) -> Job:
        """Build the MPI SGT extraction job.

        Args:
            info (RunInfo): Run metadata.
            workflow_index (int): Synthesis workflow index.
            rupture_list (str): Logical rupture-list filename.

        Returns:
            Job: Configured extraction job.
        """
        awp = info.sgt_variation_id in AWP_SGT_IDS
        transformation = "Extract_SGT_MPI_AWP" if awp else "Extract_SGT_MPI"
        identifier = f"{transformation}_{workflow_index}"
        version = "3.3.1" if info.rupture_variation_id in {5, 6} else "1.0"
        node = job(transformation, identifier, version=version)
        sgt_x = File(f"{info.site}_fx_{info.run_id}.sgt")
        sgt_y = File(f"{info.site}_fy_{info.run_id}.sgt")
        node.add_args(
            f"--site {info.site}",
            f"--lat {info.latitude}",
            f"--lon {info.longitude}",
            f"--sgt-x {sgt_x.lfn}",
            f"--sgt-y {sgt_y.lfn}",
            f"--erf-id {info.erf_id}",
        )
        node.add_inputs(sgt_x, sgt_y)
        if awp:
            header_x = File(f"{info.site}_fx_{info.run_id}.sgthead")
            header_y = File(f"{info.site}_fy_{info.run_id}.sgthead")
            node.add_args(f"--header-x {header_x.lfn}", f"--header-y {header_y.lfn}")
            node.add_inputs(header_x, header_y)
        rupture_file = File(rupture_list)
        node.add_args(f"--rup-list {rupture_file.lfn}")
        node.add_inputs(rupture_file)
        return node

    def _seis_psa_job(
        self,
        info: RunInfo,
        rupture: Rupture,
        variation: RuptureVariation,
        *,
        variation_index: int,
        group: int,
        workflow_index: int,
    ) -> Job:
        """Build one active pipe-forwarded SeisPSA task.

        Args:
            info (RunInfo): Run metadata.
            rupture (Rupture): Rupture surface metadata.
            variation (RuptureVariation): Rupture variation metadata.
            variation_index (int): Position within the rupture's variation list.
            group (int): Pegasus clustering group.
            workflow_index (int): Synthesis workflow index.

        Returns:
            Job: Configured SeisPSA job.

        Raises:
            PpError: If the estimated memory exceeds the supported cutoff or the
                variation LFN cannot be interpreted.
        """
        timestep, timesteps = self._timing(info)
        memory = self._seis_memory(rupture.num_points, info, timestep, timesteps)
        if memory > DEFAULT_PP_SEISMOGRAM_MEMORY_MB:
            raise PpError(
                f"Source {rupture.source_id}, rupture {rupture.rupture_id} "
                f"requires {memory} MiB, above the "
                f"{DEFAULT_PP_SEISMOGRAM_MEMORY_MB} MiB limit"
            )
        version = "3.3.1" if info.rupture_variation_id in {5, 6} else "1.0"
        node = job(
            "Seis_PSA_header",
            f"ID2_{rupture.source_id}_{rupture.rupture_id}_{variation.rup_var_id}",
            version=version,
        )
        combined_seis = File(
            f"Seismogram_{info.site}_{info.run_id}_{rupture.source_id}_"
            f"{rupture.rupture_id}.grm"
        )
        combined_peak = File(
            f"PeakVals_{info.site}_{info.run_id}_{rupture.source_id}_"
            f"{rupture.rupture_id}.bsa"
        )
        node.add_pegasus_profiles(
            pmc_task_arguments=(
                f"-f GRM={combined_seis.lfn} -f PSA={combined_peak.lfn}"
            ),
            pmc_priority=rupture.num_points,
            group=str(group),
            label=str(workflow_index),
            pmc_request_memory=str(memory),
        )
        node.add_pegasus_profiles(runtime=120)
        if variation_index == 0:
            final_outputs(node, combined_seis, combined_peak)

        slip, hypocenter = self._variation_indices(variation.rup_var_lfn)
        geometry = File(
            f"e{info.erf_id}_rv{info.rupture_variation_id}_"
            f"{rupture.source_id}_{rupture.rupture_id}.txt"
        )
        sub_directory = (
            f"{rupture.source_id}/{rupture.rupture_id}/" if self.dir_hierarchy else ""
        )
        sub_x = File(
            f"{sub_directory}{info.site}_{rupture.source_id}_"
            f"{rupture.rupture_id}_subfx.sgt"
        )
        sub_y = File(
            f"{sub_directory}{info.site}_{rupture.source_id}_"
            f"{rupture.rupture_id}_subfy.sgt"
        )
        node.add_args(
            f"source_id={rupture.source_id}",
            f"rupture_id={rupture.rupture_id}",
            f"rup_var_id={variation.rup_var_id}",
            f"det_max_freq={info.low_frequency}",
            "stoch_max_freq=-1.0",
            "pipe_fwd=1",
            f"stat={info.site}",
            f"slon={info.longitude}",
            f"slat={info.latitude}",
            "extract_sgt=0",
            "outputBinary=1",
            "mergeOutput=1",
            f"ntout={timesteps}",
            f"dtout={timestep}",
            f"slip={slip}",
            f"hypo={hypocenter}",
            f"rup_geom_file={geometry.lfn}",
            f"sgt_xfile={sub_x.lfn}",
            f"sgt_yfile={sub_y.lfn}",
            "seis_file=/proc/self/fd/$GRM",
            "run_psa=1",
            "simulation_out_pointsX=2",
            "simulation_out_pointsY=1",
            f"simulation_out_timesamples={timesteps}",
            f"simulation_out_timeskip={timestep}",
            "surfseis_rspectra_seismogram_units=cmpersec",
            "surfseis_rspectra_output_units=cmpersec2",
            "surfseis_rspectra_output_type=aa",
            "surfseis_rspectra_period=all",
            "surfseis_rspectra_apply_filter_highHZ=5.0",
            "surfseis_rspectra_apply_byteswap=no",
            "out=/proc/self/fd/$PSA",
        )
        if info.rupture_variation_id == 5:
            node.add_args("rupture_spacing=random")
        elif info.rupture_variation_id == 6:
            node.add_args("rupture_spacing=uniform")
        node.add_inputs(geometry, sub_x, sub_y)
        return node

    @staticmethod
    def _variation_indices(lfn: str) -> tuple[int, int]:
        """Extract slip and hypocenter indices from a variation LFN.

        Args:
            lfn (str): Rupture variation logical filename.

        Returns:
            tuple[int, int]: Slip and hypocenter indices.

        Raises:
            PpError: If the suffix is unsupported.
        """
        match = _SLIP_HYPO.search(lfn)
        if match is not None:
            return int(match.group(1)), int(match.group(2))
        match = _RUPTURE_VARIATION.search(lfn)
        if match is not None:
            return int(match.group(1)), 0
        raise PpError(f"Unsupported rupture variation filename: {lfn}")

    def _timing(self, info: RunInfo) -> tuple[float, int]:
        """Calculate the legacy deterministic timestep and sample count.

        Args:
            info (RunInfo): Run frequency metadata.

        Returns:
            tuple[float, int]: Output timestep and number of samples.
        """
        timestep = (
            self.spacing / 2.0
            if self.spacing is not None and self.spacing > 0
            else 0.05 / info.low_frequency
        )
        timesteps = 3000
        if info.low_frequency == 1.0:
            timesteps = 8000
            if self.spacing is not None and self.spacing > 0:
                length = self.seismogram_length or DEFAULT_PP_SEISMOGRAM_LENGTH_SECONDS
                timesteps = 1000 * math.ceil(length / timestep / 1000)
        return timestep, timesteps

    @staticmethod
    def _seis_memory(
        rupture_points: int,
        info: RunInfo,
        timestep: float,
        timesteps: int,
    ) -> int:
        """Estimate active SeisPSA memory in MiB.

        Args:
            rupture_points (int): Rupture surface point count.
            info (RunInfo): Run frequency metadata.
            timestep (float): Output timestep in seconds.
            timesteps (int): Number of output samples.

        Returns:
            int: Requested memory in MiB.
        """
        components = 3
        sgt_timesteps = int(2000 * info.low_frequency / 0.5)
        rupture_memory = (
            14.8 * math.log10(rupture_points) * rupture_points**1.14 / (1024.0 * 1024.0)
        )
        rupture_memory = min(rupture_memory, 80.0) * 1.75 * 0.1 / timestep
        sgt_memory = (
            (components + 1)
            / (1024.0 * 1024.0)
            * (
                32
                + rupture_points
                * (24 + 128 + 6 * sgt_timesteps * (info.low_frequency / 0.5) * 4)
            )
        )
        seismogram_memory = timesteps * components * 4 / (1024.0 * 1024.0)
        psa_memory = (timesteps * components * 4) // (1024 * 1024)
        return (
            int((sgt_memory + rupture_memory + seismogram_memory) * 1.1)
            + 1
            + psa_memory
        )

    def _add_direct_synth(
        self,
        workflow: Workflow,
        info: RunInfo,
        directory: Path,
    ) -> None:
        """Add the active DirectSynth job and its generated inputs.

        Args:
            workflow (Workflow): Synthesis workflow being assembled.
            info (RunInfo): Run metadata.
            directory (Path): Directory receiving auxiliary input lists.

        Raises:
            PpError: If the rupture scenario is unsupported or database seed
                data is incomplete.
        """
        try:
            version = DIRECT_SYNTH_VERSIONS[info.rupture_variation_id]
        except KeyError as error:
            raise PpError(
                "DirectSynth does not support rupture variation scenario "
                f"{info.rupture_variation_id}"
            ) from error
        transformation = (
            "DirectSynth_RSQSim" if info.rupture_variation_id == 8 else "DirectSynth"
        )
        node = job(transformation, transformation, version=version)
        timestep, timesteps = self._timing(info)
        handlers = 42 if info.site == "TEST" else 1120
        if info.rupture_variation_id == 8 and info.erf_id in {62, 63}:
            handlers = 420
        elif info.rupture_variation_id == 8 and info.erf_id == 60:
            handlers = 324
        node.add_args(
            f"stat={info.site}",
            f"slat={info.latitude}",
            f"slon={info.longitude}",
            f"sgt_handlers={handlers}",
            f"run_id={info.run_id}",
            "debug=1",
            "max_buf_mb=512",
        )
        if info.rupture_variation_id == 5:
            node.add_args("rupture_spacing=random")
        elif info.rupture_variation_id != 8:
            node.add_args("rupture_spacing=uniform")
        node.add_args(f"ntout={timesteps}")

        try:
            selected = parse_rupture_list(self.rupture_list)
        except (OSError, ValueError) as error:
            raise PpError(f"Invalid rupture list: {error}") from error
        if self.db_rv_seed:
            self._add_seed_file(workflow, node, info, directory, selected)
        self._add_direct_ruptures(workflow, node, info, directory, selected)

        sgt_files: list[File] = []
        for component in ("x", "y", "z") if self.z_comp else ("x", "y"):
            sgt = File(f"{info.site}_f{component}_{info.run_id}.sgt")
            header = File(f"{info.site}_f{component}_{info.run_id}.sgthead")
            node.add_args(f"sgt_{component}file={sgt.lfn}")
            node.add_args(f"{component}_header={header.lfn}")
            sgt_files.extend((sgt, header))
        node.add_inputs(*sgt_files)
        node.add_args(
            f"det_max_freq={info.low_frequency}",
            (
                f"stoch_max_freq={info.max_frequency}"
                if self.high_frequency is not None
                else "stoch_max_freq=-1.0"
            ),
            f"run_psa={int(info.rupture_variation_id == 8)}",
            f"run_rotd={int(self.rotd)}",
            f"run_durations={int(self.duration)}",
            f"run_period_durations={int(self.period_duration)}",
        )
        if self.dir_hierarchy:
            node.add_args("dir_hierarchy=1")
        node.add_args(
            f"dtout={timestep}",
            f"simulation_out_pointsX={3 if self.z_comp else 2}",
            "simulation_out_pointsY=1",
            f"simulation_out_timesamples={timesteps}",
            f"simulation_out_timeskip={timestep}",
            "surfseis_rspectra_seismogram_units=cmpersec",
            "surfseis_rspectra_output_units=cmpersec2",
            "surfseis_rspectra_output_type=aa",
            "surfseis_rspectra_period=all",
            "surfseis_rspectra_apply_filter_highHZ=5.0",
            "surfseis_rspectra_apply_byteswap=no",
        )
        workflow.add_jobs(node)

    def _add_seed_file(
        self,
        workflow: Workflow,
        node: Job,
        info: RunInfo,
        directory: Path,
        selected: tuple[tuple[int, int], ...] | None,
    ) -> None:
        """Create and attach DirectSynth rupture-fraction and seed data.

        Args:
            workflow (Workflow): Workflow receiving the local replica.
            node (Job): DirectSynth job consuming the file.
            info (RunInfo): Run metadata.
            directory (Path): Directory receiving the file.
            selected (tuple[tuple[int, int], ...] | None): Rupture restriction.

        Raises:
            PpError: If seed data is missing.
        """
        seeds = self._database_repository().get_rupture_variations_with_seeds(
            erf_id=info.erf_id,
            rupture_variation_id=info.rupture_variation_id,
            site=info.site,
            cutoff_distance=info.cutoff_distance,
            source_ruptures=selected,
        )
        if not seeds or any(seed.rvfrac is None for seed in seeds):
            message = f"Incomplete rupture fraction/seed data for run {info.run_id}"
            raise PpError(message)
        filename = f"rvfrac_seed_values_{info.site}_{info.run_id}"
        path = directory / filename
        rows = [
            f"{seed.source_id} {seed.rupture_id} {seed.rup_var_id} "
            f"{seed.rvfrac} {seed.seed}"
            for seed in seeds
        ]
        path.write_text(f"{len(rows)}\n" + "\n".join(rows) + "\n", encoding="utf-8")
        file = File(filename)
        node.add_args(f"rv_info_file={filename}")
        node.add_inputs(file)
        staged_path = self._stage_direct_input(path)
        add_replica(
            workflow,
            file,
            f"go://{CARC_GLOBUS_ENDPOINT}/{staged_path}",
            site="shock",
        )

    def _add_direct_ruptures(
        self,
        workflow: Workflow,
        node: Job,
        info: RunInfo,
        directory: Path,
        selected: tuple[tuple[int, int], ...] | None,
    ) -> None:
        """Create and attach the DirectSynth rupture list and product files.

        Args:
            workflow (Workflow): Workflow receiving local replicas.
            node (Job): DirectSynth job consuming and producing the files.
            info (RunInfo): Run metadata.
            directory (Path): Directory receiving auxiliary files.
            selected (tuple[tuple[int, int], ...] | None): Rupture restriction.

        Raises:
            PpError: If no rupture data matches.
        """
        filename = f"rupture_file_list_{info.site}_{info.run_id}"
        path = directory / filename
        rows: list[str] = []
        directory_entries: list[str] = []
        if info.rupture_variation_id == 8:
            if selected is not None:
                raise PpError("Rupture lists are unsupported with SRF variations")
            variations = self._database_repository().get_rupture_variations(
                erf_id=info.erf_id,
                rupture_variation_id=info.rupture_variation_id,
                site=info.site,
                cutoff_distance=info.cutoff_distance,
                order_by_size=True,
            )
            for variation in variations:
                if self.dir_hierarchy:
                    source_path = f"{variation.source_id}/{variation.rup_var_lfn}"
                    rows.append(source_path)
                    directory_entries.append(source_path)
                else:
                    rows.append(variation.rup_var_lfn)
                    node.add_inputs(File(variation.rup_var_lfn))
                self._add_direct_outputs(
                    node,
                    info,
                    variation.source_id,
                    variation.rupture_id,
                )
        else:
            summaries = self._database_repository().get_rupture_summaries_for_site(
                info.site,
                erf_id=info.erf_id,
                rupture_variation_id=info.rupture_variation_id,
                cutoff_distance=info.cutoff_distance,
                source_ruptures=selected,
            )
            for summary in summaries:
                geometry = (
                    f"e{info.erf_id}_rv{info.rupture_variation_id}_"
                    f"{summary.source_id}_{summary.rupture_id}.txt"
                )
                rows.append(
                    f"{geometry} {summary.variation_count} 1 {summary.num_rows} "
                    f"{summary.num_columns} {summary.magnitude:.2f}"
                )
                if self.dir_hierarchy:
                    directory_entries.append(f"{summary.source_id}/{geometry}")
                else:
                    node.add_inputs(File(geometry))
                self._add_direct_outputs(
                    node,
                    info,
                    summary.source_id,
                    summary.rupture_id,
                )
        if not rows:
            raise PpError(f"No ruptures found for site {info.site}")
        path.write_text(f"{len(rows)}\n" + "\n".join(rows) + "\n", encoding="utf-8")
        rupture_file = File(filename)
        node.add_args(f"rup_list_file={filename}")
        node.add_inputs(rupture_file)
        staged_path = self._stage_direct_input(path)
        add_replica(
            workflow,
            rupture_file,
            f"go://{CARC_GLOBUS_ENDPOINT}/{staged_path}",
            site="shock",
        )
        if directory_entries:
            self._add_directories_job(
                workflow,
                node,
                info,
                directory,
                directory_entries,
            )

    @staticmethod
    def _add_directories_job(
        workflow: Workflow,
        direct_synth: Job,
        info: RunInfo,
        directory: Path,
        entries: list[str],
    ) -> None:
        """Add the DirectSynth source-directory preparation job.

        Args:
            workflow (Workflow): Synthesis workflow being assembled.
            direct_synth (Job): DirectSynth job consuming hierarchical inputs.
            info (RunInfo): Run metadata supplying the ERF ID.
            directory (Path): Directory receiving the generated input list.
            entries (list[str]): Source-prefixed rupture input paths.
        """
        filename = "directory_list.txt"
        path = directory / filename
        path.write_text("".join(f"{entry}\n" for entry in entries), encoding="utf-8")
        directory_file = File(filename)
        create_directories = job("DirCreate", "DirCreate")
        create_directories.add_args(filename, str(info.erf_id))
        create_directories.add_inputs(directory_file)
        workflow.add_jobs(create_directories)
        add_local_replica(workflow, directory_file, path)
        dependency(workflow, create_directories, direct_synth)

    def _stage_direct_input(self, path: Path) -> Path:
        """Copy a DirectSynth input into the Globus-visible staging directory."""
        staging_directory = self.config.output_directory / "test-runs"
        staging_directory.mkdir(parents=True, exist_ok=True)
        staged_path = staging_directory / path.name
        shutil.copyfile(path, staged_path)
        return staged_path.resolve()

    def _add_direct_outputs(
        self,
        node: Job,
        info: RunInfo,
        source_id: int,
        rupture_id: int,
    ) -> None:
        """Attach DirectSynth outputs for one rupture.

        Args:
            node (Job): DirectSynth job.
            info (RunInfo): Run metadata.
            source_id (int): ERF source ID.
            rupture_id (int): Rupture ID.
        """
        prefix = f"{info.site}_{info.run_id}_{source_id}_{rupture_id}"
        directory = f"{source_id}/" if self.dir_hierarchy else ""
        outputs = [File(f"{directory}Seismogram_{prefix}.grm")]
        if info.rupture_variation_id == 8:
            outputs.append(File(f"{directory}PeakVals_{prefix}.bsa"))
        if self.rotd:
            outputs.append(File(f"{directory}RotD_{prefix}.rotd"))
            if self.z_comp and not self.no_vertical_response:
                outputs.append(File(f"{directory}VerticalRSP_{prefix}.rsp"))
        if self.duration:
            outputs.append(File(f"{directory}Duration_{prefix}.dur"))
        if self.period_duration:
            outputs.append(File(f"{directory}PeriodDuration_{prefix}.dur"))
        final_outputs(node, *outputs)
