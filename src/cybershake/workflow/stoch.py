"""Stochastic workflow generation."""

from __future__ import annotations

import math
from typing import TYPE_CHECKING, cast

from sqlalchemy.exc import SQLAlchemyError

from Pegasus.api import File, Job, Namespace, SubWorkflow, Workflow

from cybershake.constants import (
    CARC_GLOBUS_ENDPOINT,
    DEFAULT_JOB_DB_CREDENTIALS,
    DEFAULT_STOCH_DURATION_SECONDS,
    DEFAULT_STOCH_H_FRACTION,
    DEFAULT_STOCH_HF_VEL_MODEL,
    DEFAULT_STOCH_LONG_DURATION_SECONDS,
    DEFAULT_STOCH_REFERENCE_VELOCITY,
    GEN_STOCH_WORKFLOW_TRANSFORMATION,
    HF_VEL_MODELS,
)
from cybershake.exc import RunManagerError, StochError
from cybershake.runmanager import db as run_db
from cybershake.vs30 import Vs30Error, lookup_vs30
from cybershake.workflow.common import (
    RunInfo,
    add_config_metadata,
    add_local_replica,
    add_replica,
    create_db_products_workflow,
    dependency,
    final_outputs,
    intermediate_outputs,
    job,
    load_run_info,
    parse_rupture_list,
    update_job,
    write_workflow,
)

if TYPE_CHECKING:
    from collections.abc import Mapping
    from pathlib import Path

    from cybershake.config import Config
    from cybershake.runmanager.db import (
        RuptureSummary,
        RuptureVariationSeed,
        SQLAlchemyRunDB,
    )


class Stoch:
    """Generate top-level and runtime stochastic workflows."""

    _database: SQLAlchemyRunDB | None = None

    def __init__(
        self,
        *,
        config: Config,
        database: SQLAlchemyRunDB | None = None,
    ) -> None:
        """Store options shared by the stochastic workflow builders.

        Args:
            config (Config): Resolved CyberShake configuration.
            database (SQLAlchemyRunDB | None, optional): Query repository.
                Defaults to a connection created from ``server``.
        """
        self.generator_merge_frequency = config.generator_merge_frequency
        self.no_rotd = config.no_rotd
        self.no_site_response = config.no_site_response
        self.no_low_site_response = config.no_low_site_response
        self.no_duration = config.no_duration
        self.debug = config.debug
        self.server = config.server
        self.db_rv_seed = config.db_rv_seed
        self.hf_vel_model = config.hf_vel_model
        self.period_duration = config.period_duration
        self.z_comp = config.z_comp
        self.no_vertical_response = config.no_vertical_response
        self.rupture_list = config.rupture_list
        self.config = config
        if database is not None:
            self._database = database

    def create(
        self,
        *,
        run_id: int,
        run_directory: Path,
        low_frequency_run_id: int,
    ) -> None:
        """Generate the complete stochastic workflow hierarchy.

        Args:
            run_id (int): Stochastic run represented by the workflows.
            run_directory (Path): Directory receiving workflows and inputs.
            low_frequency_run_id (int): Run providing low-frequency seismograms.

        Raises:
            StochError: If run metadata is inconsistent or workflow generation
                fails.
        """
        stoch_frequency = cast("float", self.config["stoch_frequency"])
        force_vs30 = self.config.force_vs30
        h_fraction = self.config.h_fraction
        info = self._ensure_vs30(self._run_info(run_id), force=force_vs30)
        low_info = self._run_info(low_frequency_run_id)
        self._validate_runs(info, low_info)
        if not math.isclose(stoch_frequency, info.max_frequency):
            raise StochError(
                f"Run {run_id} has maximum frequency {info.max_frequency}, "
                f"not {stoch_frequency}"
            )
        run_directory.mkdir(parents=True, exist_ok=True)
        velocity_name = f"velocity_info_{info.site}.txt"
        velocity_file = run_directory / velocity_name
        generated_file = File(f"CyberShake_Stoch_{info.site}.dax")
        database_file = run_directory / (
            f"CyberShake_Stoch_{info.site}_Stoch_DB_Products.dax"
        )
        self.create_db_products(
            workflow_file=database_file,
            run_id=run_id,
            sub_workflow_count=1,
            high_frequency_cutoff=info.low_frequency,
            velocity_file=velocity_file,
        )

        workflow = Workflow(
            f"CyberShake_Stoch_{info.site}_top.dax",
            infer_dependencies=False,
        )
        add_config_metadata(workflow, self.config)
        update = update_job(run_id, "PP_START", "PP_END")
        velocity = self._velocity_info_job(
            info,
            h_fraction=(
                h_fraction if h_fraction is not None else DEFAULT_STOCH_H_FRACTION
            ),
        )
        insert = self._velocity_insert_job(
            info,
            low_frequency_run_id=low_frequency_run_id,
            velocity_file=File(velocity_name),
        )
        generator = self._generator_job(
            info,
            low_frequency_run_id=low_frequency_run_id,
            generated_file=generated_file,
            velocity_file=File(velocity_name),
        )
        runtime = SubWorkflow(generated_file.lfn, _id=f"Sub_Stoch_{info.site}")
        runtime.add_args(
            "--cluster",
            "label",
            "--force",
            "--q",
            "--output-sites",
            "shock",
            "--basename",
            f"Sub_Stoch_{info.site}",
        )
        products = SubWorkflow(database_file.name, _id="dbDax")
        products.add_args("--force", "--q")
        products.add_inputs(File(velocity_name))
        workflow.add_jobs(update, velocity, insert, generator, runtime, products)
        add_local_replica(workflow, File(database_file.name), database_file)
        if self.rupture_list is not None:
            add_local_replica(
                workflow,
                File(self.rupture_list.name),
                self.rupture_list,
            )
        dependency(workflow, velocity, insert, generator, products)
        dependency(workflow, generator, runtime, products)
        dependency(workflow, runtime, products)
        dependency(workflow, products, update)
        write_workflow(
            workflow,
            run_directory / f"CyberShake_Stoch_{info.site}_top.dax",
        )

    def create_sub_stoch(
        self,
        *,
        run_id: int,
        low_frequency_run_id: int,
        velocity_file: Path,
        workflow_file: Path,
    ) -> None:
        """Generate a per-rupture stochastic sub-workflow.

        Args:
            run_id (int): Stochastic run represented by the sub-workflow.
            low_frequency_run_id (int): Run providing low-frequency seismograms.
            velocity_file (Path): Velocity information produced by the parent.
            workflow_file (Path): Destination workflow file.

        Raises:
            StochError: If metadata, velocity data, or rupture data is invalid.
        """
        info = self._ensure_vs30(self._run_info(run_id))
        low_info = self._run_info(low_frequency_run_id)
        self._validate_runs(info, low_info)
        timestep = 0.5 / info.max_frequency
        if timestep > 0.05:
            raise StochError(
                f"Run {run_id} maximum frequency {info.max_frequency} Hz is "
                "below the supported 10 Hz minimum"
            )
        velocities = self._read_velocity_file(velocity_file)
        summaries = self._ruptures(info)
        duration = (
            DEFAULT_STOCH_LONG_DURATION_SECONDS
            if low_info.low_frequency >= 1.0
            else DEFAULT_STOCH_DURATION_SECONDS
        )
        velocity_model = self.hf_vel_model or DEFAULT_STOCH_HF_VEL_MODEL
        try:
            model_file = HF_VEL_MODELS[velocity_model]
        except KeyError as error:
            raise StochError(
                f"Unsupported high-frequency velocity model {velocity_model!r}"
            ) from error

        workflow = Workflow(workflow_file.name, infer_dependencies=False)
        update = update_job(run_id, "PP_INIT", "PP_START")
        local_model = self._local_velocity_job(model_file)
        directories_path = workflow_file.parent / "dirs_list.txt"
        directories_path.parent.mkdir(parents=True, exist_ok=True)
        source_ids = sorted({rupture.source_id for rupture in summaries})
        directories_path.write_text(
            "".join(f"{source_id}\n" for source_id in source_ids),
            encoding="utf-8",
        )
        directories = self._create_directories_job(directories_path.name)
        workflow.add_jobs(update, local_model, directories)
        add_replica(
            workflow,
            File(directories_path.name),
            f"go://{CARC_GLOBUS_ENDPOINT}{directories_path.resolve()}",
            site="local",
        )
        dependency(workflow, update, local_model, directories)

        needs_validation_seed = info.rupture_variation_id == 8 and info.erf_id == 60
        seeds = self._seed_map(info) if self.db_rv_seed or needs_validation_seed else {}
        for summary in summaries:
            terminal_hf = self._add_hf_synthesis(
                workflow,
                info,
                summary,
                local_model=local_model,
                directories=directories,
                local_model_file=f"{model_file}.local",
                duration=duration,
                timestep=timestep,
                seeds=seeds,
            )
            merge = self._merge_job(
                info,
                low_info,
                summary,
                duration=duration,
                timestep=timestep,
            )
            workflow.add_jobs(merge)
            for parent in terminal_hf:
                dependency(workflow, parent, merge)
            if not self.no_low_site_response:
                low_response = self._low_site_response_job(
                    info,
                    low_info,
                    summary,
                    velocities=velocities,
                )
                workflow.add_jobs(low_response)
                for parent in terminal_hf:
                    dependency(workflow, parent, low_response)
                dependency(workflow, low_response, merge)

        write_workflow(workflow, workflow_file)

    def create_db_products(
        self,
        *,
        workflow_file: Path,
        run_id: int,
        sub_workflow_count: int,
        high_frequency_cutoff: float,
        velocity_file: Path,
    ) -> None:
        """Generate the stochastic database-products workflow.

        Args:
            workflow_file (Path): Destination workflow file.
            run_id (int): Stochastic run represented by the workflow.
            sub_workflow_count (int): Number of synthesis workflows. Active
                file-forwarding does not consume per-workflow zip files.
            high_frequency_cutoff (float): Expected merge cutoff in Hz.
            velocity_file (Path): Vs30 file used by comparison curves.

        Raises:
            StochError: If the run cutoff is inconsistent or construction fails.
        """
        del sub_workflow_count
        info = self._run_info(run_id)
        if not math.isclose(high_frequency_cutoff, info.low_frequency):
            raise StochError(
                f"Run {run_id} has merge frequency {info.low_frequency}, "
                f"not {high_frequency_cutoff}"
            )
        try:
            workflow = create_db_products_workflow(
                info=info,
                server=self.server,
                calculate_rotd=not self.no_rotd,
                calculate_durations=not self.no_duration,
                stochastic=True,
                velocity_file=velocity_file,
                rupture_list=self.rupture_list,
            )
        except ValueError as error:
            raise StochError(str(error)) from error
        write_workflow(workflow, workflow_file)

    def _database_repository(self) -> SQLAlchemyRunDB:
        """Return the injected or configured database repository.

        Returns:
            SQLAlchemyRunDB: Repository used by the generator.

        Raises:
            StochError: If a configured connection cannot be created.
        """
        if self._database is not None:
            return self._database
        try:
            return run_db.connect(server=self.server, config=self.config)
        except RunManagerError as error:
            raise StochError(str(error)) from error

    def _run_info(self, run_id: int) -> RunInfo:
        """Load common metadata for a stochastic run.

        Args:
            run_id (int): CyberShake run ID.

        Returns:
            RunInfo: Validated run and site metadata.

        Raises:
            StochError: If database access or validation fails.
        """
        try:
            return load_run_info(self._database_repository(), run_id)
        except (SQLAlchemyError, ValueError) as error:
            raise StochError(f"Unable to load run {run_id}: {error}") from error

    def _ensure_vs30(self, info: RunInfo, *, force: float | None = None) -> RunInfo:
        """Resolve and persist a run's target Vs30 when necessary.

        Args:
            info (RunInfo): Run metadata.
            force (float | None, optional): User-provided Vs30. Defaults to
                ``None``, using a stored or OpenSHA-derived value.

        Returns:
            RunInfo: Metadata containing the resolved target Vs30.

        Raises:
            StochError: If lookup or persistence fails.
        """
        source: str
        if force is not None:
            if force <= 0:
                raise StochError("Forced Vs30 must be positive")
            value = force
            source = "User-provided"
        elif info.target_vs30 is not None and info.target_vs30 > 0:
            return info
        else:
            try:
                classpath = (
                    None if self.config is None else str(self.config["vs30_classpath"])
                )
                result = lookup_vs30(
                    info.latitude,
                    info.longitude,
                    classpath=classpath,
                )
            except Vs30Error as error:
                raise StochError(str(error)) from error
            value = result.value
            source = result.source
        try:
            updated = self._database_repository().update_target_vs30(
                info.run_id,
                target_vs30=value,
                source=source,
            )
        except (SQLAlchemyError, ValueError) as error:
            raise StochError(f"Unable to store Vs30 for run {info.run_id}") from error
        if not updated:
            raise StochError(f"Run {info.run_id} disappeared while storing Vs30")
        return info._replace(target_vs30=value)

    @staticmethod
    def _validate_runs(info: RunInfo, low_info: RunInfo) -> None:
        """Validate stochastic and low-frequency run compatibility.

        Args:
            info (RunInfo): Stochastic run metadata.
            low_info (RunInfo): Low-frequency run metadata.

        Raises:
            StochError: If site, ERF, variation scenario, or frequency differs.
        """
        if low_info.site != info.site:
            raise StochError(
                f"Stochastic site {info.site} does not match low-frequency site "
                f"{low_info.site}"
            )
        if low_info.low_frequency < info.low_frequency:
            raise StochError(
                f"Low-frequency run stops at {low_info.low_frequency} Hz and "
                f"cannot merge at {info.low_frequency} Hz"
            )
        if low_info.erf_id != info.erf_id:
            raise StochError(
                f"ERF IDs differ: stochastic {info.erf_id}, "
                f"low-frequency {low_info.erf_id}"
            )
        if low_info.rupture_variation_id != info.rupture_variation_id:
            raise StochError(
                "Rupture variation scenario IDs differ: stochastic "
                f"{info.rupture_variation_id}, low-frequency "
                f"{low_info.rupture_variation_id}"
            )

    def _velocity_info_job(self, info: RunInfo, *, h_fraction: float) -> Job:
        """Build the UCVM surface-velocity query job.

        Args:
            info (RunInfo): Stochastic run metadata.
            h_fraction (float): Query depth as a grid-spacing fraction.

        Returns:
            Job: Configured velocity query job.
        """
        grid_spacing = int(100.0 / info.low_frequency)
        output = File(f"velocity_info_{info.site}.txt")
        node = job("Velocity_Info", "Velocity_Info")
        node.add_args(
            str(info.longitude),
            str(info.latitude),
            info.velocity_model,
            str(grid_spacing),
            str(grid_spacing * h_fraction),
            output.lfn,
        )
        intermediate_outputs(node, output)
        return node

    @staticmethod
    def _velocity_insert_job(
        info: RunInfo,
        *,
        low_frequency_run_id: int,
        velocity_file: File,
    ) -> Job:
        """Build the broadband velocity-parameter insertion job.

        Args:
            info (RunInfo): Stochastic run metadata.
            low_frequency_run_id (int): Linked low-frequency run ID.
            velocity_file (File): Velocity query output.

        Returns:
            Job: Configured insertion job.
        """
        node = job("BB_Velocity_Insert", "BB_Velocity_Insert")
        node.add_args(
            "insert-bb-velocity-params",
            f"-vi {velocity_file.lfn}",
            f"-bbid {info.run_id}",
            f"-lfid {low_frequency_run_id}",
            f"-d {DEFAULT_JOB_DB_CREDENTIALS}",
        )
        node.add_inputs(velocity_file)
        return node

    def _generator_job(
        self,
        info: RunInfo,
        *,
        low_frequency_run_id: int,
        generated_file: File,
        velocity_file: File,
    ) -> Job:
        """Build the deferred Python stochastic sub-workflow generator.

        Args:
            info (RunInfo): Stochastic run metadata.
            low_frequency_run_id (int): Linked low-frequency run ID.
            generated_file (File): Generated workflow output.
            velocity_file (File): Velocity query output consumed at runtime.

        Returns:
            Job: Local ``cybershake sub-stoch-wf`` job.
        """
        node = job(
            GEN_STOCH_WORKFLOW_TRANSFORMATION,
            f"GenStochDax_{info.site}",
        )
        node.add_args(
            "sub-stoch-wf",
            "--run-id",
            str(info.run_id),
            "--low-frequency-run-id",
            str(low_frequency_run_id),
            "--server",
            self.server,
        )
        self._add_generator_flags(node)
        if self.rupture_list is not None:
            rupture_file = File(self.rupture_list.name)
            node.add_args("--rupture-list", rupture_file.lfn)
            node.add_inputs(rupture_file)
        node.add_args(
            "--workflow-file",
            generated_file.lfn,
            "--velocity-file",
            velocity_file.lfn,
            "--hf-vel-model",
            self.hf_vel_model or DEFAULT_STOCH_HF_VEL_MODEL,
        )
        node.add_inputs(velocity_file)
        intermediate_outputs(node, generated_file)
        node.add_profiles(Namespace.SELECTOR, executionPool="local")
        return node

    def _add_generator_flags(self, node: Job) -> None:
        """Add enabled boolean options to a deferred generator job.

        Args:
            node (Job): Deferred stochastic generator job.
        """
        flags = (
            (self.no_rotd, "--no-rotd"),
            (self.no_site_response, "--no-site-response"),
            (self.no_low_site_response, "--no-low-site-response"),
            (self.no_duration, "--no-duration"),
            (self.debug, "--debug"),
            (self.db_rv_seed, "--db-rv-seed"),
            (not self.db_rv_seed, "--no-db-rv-seed"),
            (self.z_comp, "--z-comp"),
            (self.period_duration, "--period-duration"),
            (self.no_vertical_response or not self.z_comp, "--no-vertical-response"),
        )
        for enabled, flag in flags:
            if enabled:
                node.add_args(flag)

    @staticmethod
    def _read_velocity_file(path: Path) -> tuple[float, float, float]:
        """Read the three values emitted by ``Velocity_Info``.

        Args:
            path (Path): Velocity information file.

        Returns:
            tuple[float, float, float]: Vs30, Vs at 5H, and Vs at D+5H.

        Raises:
            StochError: If the file is missing or malformed.
        """
        try:
            content = path.read_text(encoding="utf-8")
            lines = [line for line in content.splitlines() if line]
            values = tuple(float(line.split("=", maxsplit=1)[1]) for line in lines[:3])
        except (OSError, IndexError, ValueError) as error:
            raise StochError(f"Invalid velocity file {path}: {error}") from error
        if len(values) != 3:
            raise StochError(f"Velocity file {path} must contain three values")
        return values

    def _ruptures(self, info: RunInfo) -> tuple[RuptureSummary, ...]:
        """Load rupture summaries selected for stochastic synthesis.

        Args:
            info (RunInfo): Stochastic run metadata.

        Returns:
            tuple[RuptureSummary, ...]: Ruptures in legacy size order.

        Raises:
            StochError: If the restriction is invalid or no ruptures match.
        """
        try:
            selected = parse_rupture_list(self.rupture_list)
            ruptures = self._database_repository().get_rupture_summaries_for_site(
                info.site,
                erf_id=info.erf_id,
                rupture_variation_id=info.rupture_variation_id,
                source_ruptures=selected,
            )
        except (OSError, SQLAlchemyError, ValueError) as error:
            message = f"Unable to load ruptures for {info.site}: {error}"
            raise StochError(message) from error
        if not ruptures:
            raise StochError(f"No ruptures found for site {info.site}")
        return ruptures

    def _seed_map(
        self,
        info: RunInfo,
    ) -> dict[tuple[int, int, int], RuptureVariationSeed]:
        """Load database rupture fractions and seeds.

        Args:
            info (RunInfo): Stochastic run metadata.

        Returns:
            dict[tuple[int, int, int], RuptureVariationSeed]: Records keyed by
            source, rupture, and variation ID.

        Raises:
            StochError: If no seed records are available.
        """
        selected = parse_rupture_list(self.rupture_list)
        records = self._database_repository().get_rupture_variations_with_seeds(
            erf_id=info.erf_id,
            rupture_variation_id=info.rupture_variation_id,
            site=info.site,
            cutoff_distance=info.cutoff_distance,
            source_ruptures=selected,
        )
        if not records:
            raise StochError(f"No rupture fraction/seed data for run {info.run_id}")
        return {
            (record.source_id, record.rupture_id, record.rup_var_id): record
            for record in records
        }

    @staticmethod
    def _local_velocity_job(model_file: str) -> Job:
        """Build the local 1-D velocity-model preparation job.

        Args:
            model_file (str): Staged source model filename.

        Returns:
            Job: Configured local-model job.
        """
        source = File(model_file)
        output = File(f"{model_file}.local")
        node = job("Local_VM", "Create_Local_VM")
        node.add_args(source, output)
        node.add_inputs(source)
        intermediate_outputs(node, output)
        return node

    @staticmethod
    def _create_directories_job(filename: str) -> Job:
        """Build the source-directory creation job.

        Args:
            filename (str): Logical source-directory list filename.

        Returns:
            Job: Configured directory creation job.
        """
        file = File(filename)
        node = job("Create_Dirs", "Create_Dirs")
        node.add_args(file)
        node.add_inputs(file)
        return node

    def _add_hf_synthesis(
        self,
        workflow: Workflow,
        info: RunInfo,
        rupture: RuptureSummary,
        *,
        local_model: Job,
        directories: Job,
        local_model_file: str,
        duration: float,
        timestep: float,
        seeds: Mapping[tuple[int, int, int], RuptureVariationSeed],
    ) -> tuple[Job, ...]:
        """Add partitioned high-frequency synthesis for one rupture.

        Args:
            workflow (Workflow): Runtime workflow being assembled.
            info (RunInfo): Stochastic run metadata.
            rupture (RuptureSummary): Rupture dimensions and variation count.
            local_model (Job): Local velocity-model producer.
            directories (Job): Source-directory producer.
            local_model_file (str): Prepared velocity-model filename.
            duration (float): Seismogram duration in seconds.
            timestep (float): High-frequency timestep in seconds.
            seeds (Mapping[tuple[int, int, int], RuptureVariationSeed]): Optional
                database fraction and seed records.

        Returns:
            tuple[Job, ...]: Jobs producing the complete seismogram and PGA data.
        """
        if info.rupture_variation_id == 8:
            task_count = rupture.variation_count
            variations_per_task = 1
        else:
            variations_per_task = max(1, math.ceil(1_000_000 / rupture.num_points))
            task_count = math.ceil(rupture.variation_count / variations_per_task)

        combine_seis: Job | None = None
        combine_pga: Job | None = None
        if task_count > 1:
            combine_seis = job(
                "Combine_HF_Synth",
                f"Combine_HF_Synth_{rupture.source_id}_{rupture.rupture_id}",
            )
            combine_pga = job(
                "Combine_HF_Synth",
                f"Combine_PGA_{rupture.source_id}_{rupture.rupture_id}",
            )
            workflow.add_jobs(combine_seis, combine_pga)

        tasks: list[Job] = []
        for task_index in range(task_count):
            suffix = f"_t{task_index}" if task_count > 1 else ""
            identifier = f"HF_Synth_{rupture.source_id}_{rupture.rupture_id}{suffix}"
            node = job("HF_Synth", identifier, version="3.0")
            output = File(
                f"{rupture.source_id}/Seismogram_{info.site}_{info.run_id}_"
                f"{rupture.source_id}_{rupture.rupture_id}_hf{suffix}.grm"
            )
            pga = File(
                f"{rupture.source_id}/HighFreqPGA_{info.site}_"
                f"{rupture.source_id}_{rupture.rupture_id}{suffix}.txt"
            )
            node.add_args(
                f"stat={info.site}",
                f"slat={info.latitude}",
                f"slon={info.longitude}",
                f"source_id={rupture.source_id}",
                f"rupture_id={rupture.rupture_id}",
            )
            if self.db_rv_seed:
                node.add_args("rvfrac_seed_given=1")
            self._add_hf_variations(
                node,
                info,
                rupture,
                task_index=task_index,
                variations_per_task=variations_per_task,
                seeds=seeds,
            )
            node.add_args(
                f"outfile={output.lfn}",
                f"pga_outfile={pga.lfn}",
                f"tlen={duration}",
                f"dt={timestep}",
                f"do_site_response={int(not self.no_site_response)}",
            )
            if not self.no_site_response:
                node.add_args(
                    f"vs30={info.target_vs30}",
                    f"vref={DEFAULT_STOCH_REFERENCE_VELOCITY}",
                    f"vpga={DEFAULT_STOCH_REFERENCE_VELOCITY}",
                )
            node.add_args(
                f"debug={int(self.debug)}",
                f"num_comps={3 if self.z_comp else 2}",
                f"vmod={local_model_file}",
            )
            node.add_inputs(File(local_model_file))
            intermediate_outputs(node, output, pga)
            node.add_pegasus_profiles(
                pmc_priority=rupture.num_points,
                pmc_request_memory=str(
                    self._hf_memory(rupture, duration=duration, timestep=timestep)
                ),
                label="pmc",
            )
            workflow.add_jobs(node)
            dependency(workflow, local_model, node)
            dependency(workflow, directories, node)
            tasks.append(node)
            if combine_seis is not None and combine_pga is not None:
                combine_seis.add_args(output.lfn)
                combine_seis.add_inputs(output)
                combine_pga.add_args(pga.lfn)
                combine_pga.add_inputs(pga)
                dependency(workflow, node, combine_seis, combine_pga)

        if combine_seis is None or combine_pga is None:
            return tuple(tasks)
        combined_output = File(
            f"{rupture.source_id}/Seismogram_{info.site}_{info.run_id}_"
            f"{rupture.source_id}_{rupture.rupture_id}_hf.grm"
        )
        combined_pga = File(
            f"{rupture.source_id}/HighFreqPGA_{info.site}_"
            f"{rupture.source_id}_{rupture.rupture_id}.txt"
        )
        combine_seis.add_args(combined_output.lfn)
        combine_pga.add_args(combined_pga.lfn)
        intermediate_outputs(combine_seis, combined_output)
        intermediate_outputs(combine_pga, combined_pga)
        combine_seis.add_pegasus_profiles(label="pmc")
        combine_pga.add_pegasus_profiles(label="pmc")
        return combine_seis, combine_pga

    def _add_hf_variations(
        self,
        node: Job,
        info: RunInfo,
        rupture: RuptureSummary,
        *,
        task_index: int,
        variations_per_task: int,
        seeds: Mapping[tuple[int, int, int], RuptureVariationSeed],
    ) -> None:
        """Attach rupture-variation inputs and arguments to an HF task.

        Args:
            node (Job): HF synthesis job.
            info (RunInfo): Stochastic run metadata.
            rupture (RuptureSummary): Rupture metadata.
            task_index (int): Zero-based task index.
            variations_per_task (int): Maximum variations in this task.
            seeds (Mapping[tuple[int, int, int], RuptureVariationSeed]): Optional
                database fraction and seed records.

        Raises:
            StochError: If required SRF or seed data is absent.
        """
        if info.rupture_variation_id == 8:
            variations = self._database_repository().get_rupture_variations(
                erf_id=info.erf_id,
                rupture_variation_id=info.rupture_variation_id,
                source_ruptures=((rupture.source_id, rupture.rupture_id),),
            )
            if not variations:
                raise StochError(
                    f"No SRF found for source {rupture.source_id}, rupture "
                    f"{rupture.rupture_id}"
                )
            srf = File(variations[0].rup_var_lfn)
            node.add_args(f"infile={srf.lfn}", f"rup_var_id={task_index}")
            node.add_inputs(srf)
            if info.erf_id == 60:
                record = seeds.get((rupture.source_id, rupture.rupture_id, task_index))
                if record is None:
                    raise StochError(
                        f"Missing seed for source {rupture.source_id}, rupture "
                        f"{rupture.rupture_id}, variation {task_index}"
                    )
                node.add_args(f"srf_seed={record.seed}")
            return

        start = task_index * variations_per_task
        end = min((task_index + 1) * variations_per_task, rupture.variation_count)
        geometry = File(
            f"e{info.erf_id}_rv{info.rupture_variation_id}_"
            f"{rupture.source_id}_{rupture.rupture_id}.txt"
        )
        encoded: list[str] = []
        for variation_id in range(start, end):
            if self.db_rv_seed:
                record = seeds.get(
                    (rupture.source_id, rupture.rupture_id, variation_id)
                )
                if record is None or record.rvfrac is None:
                    raise StochError(
                        f"Missing fraction/seed for source {rupture.source_id}, "
                        f"rupture {rupture.rupture_id}, variation {variation_id}"
                    )
                encoded.append(
                    f"({variation_id},{variation_id},0,{record.rvfrac},{record.seed})"
                )
            else:
                encoded.append(f"({variation_id},{variation_id},0)")
        node.add_args(
            f"rup_geom_file={geometry.lfn}",
            f"num_rup_vars={end - start}",
            f"rup_vars={';'.join(encoded)}",
        )
        node.add_inputs(geometry)

    @staticmethod
    def _hf_memory(
        rupture: RuptureSummary,
        *,
        duration: float,
        timestep: float,
    ) -> int:
        """Estimate one HF synthesis task's memory in MiB.

        Args:
            rupture (RuptureSummary): Rupture dimensions.
            duration (float): Seismogram duration in seconds.
            timestep (float): Seismogram timestep in seconds.

        Returns:
            int: Requested memory in MiB.
        """
        single_variation = min(
            120.0,
            6.491 * rupture.num_points**1.314 * 1.1 / (1024.0 * 1024.0),
        )
        columns = rupture.num_columns
        rows = rupture.num_rows
        if columns == -1 or rows == -1:
            columns = max(1, min(300, int(math.sqrt(rupture.num_points))))
            rows = math.ceil(rupture.num_points / columns)
        temporary = (
            8.0
            * (columns * rows * 13.8 + 32.7 * max(columns, rows) ** 2)
            / (1024.0 * 1024.0)
        )
        output = duration / timestep * 2.0 * 4.0 / (1024.0 * 1024.0)
        slip = 3.0 * 500.0 * 2000.0 * 10.0 * 4.0 / (1024.0 * 1024.0)
        nx = max(1, math.floor(columns / 5.0 + 0.5))
        nx_div = 1
        while (columns * nx_div) % nx != 0:
            nx_div += 1
        ny = max(1, math.floor(rows / 5.0 + 0.5))
        ny_div = 1
        while (rows * ny_div) % ny != 0:
            ny_div += 1
        synthesis = slip + (
            3.0 * nx_div * ny_div * columns * rows * 4.0 + 3 * nx * ny * 4.0
        ) / (1024.0 * 1024.0)
        return math.ceil(
            1.1 * (single_variation + temporary + output + slip + synthesis)
        )

    def _low_site_response_job(
        self,
        info: RunInfo,
        low_info: RunInfo,
        rupture: RuptureSummary,
        *,
        velocities: tuple[float, float, float],
    ) -> Job:
        """Build low-frequency site response for one rupture.

        Args:
            info (RunInfo): Stochastic run metadata.
            low_info (RunInfo): Low-frequency run metadata.
            rupture (RuptureSummary): Rupture metadata.
            velocities (tuple[float, float, float]): Vs30, Vs at 5H, and Vs at
                D+5H from the velocity query.

        Returns:
            Job: Configured low-frequency site-response job.
        """
        input_file = File(
            f"Seismogram_{info.site}_{low_info.run_id}_{rupture.source_id}_"
            f"{rupture.rupture_id}.grm"
        )
        pga = File(
            f"{rupture.source_id}/HighFreqPGA_{info.site}_"
            f"{rupture.source_id}_{rupture.rupture_id}.txt"
        )
        output = File(
            f"{rupture.source_id}/Seismogram_{info.site}_{low_info.run_id}_"
            f"{rupture.source_id}_{rupture.rupture_id}_site_response.grm"
        )
        reference = math.floor(velocities[0] * velocities[2] / velocities[1] * 10) / 10
        node = job(
            "LF_Site_Response",
            f"LF_Site_Response_{rupture.source_id}_{rupture.rupture_id}",
            version="2.0",
        )
        node.add_args(
            f"seis_in={input_file.lfn}",
            f"pga_infile={pga.lfn}",
            f"seis_out={output.lfn}",
            f"slat={info.latitude}",
            f"slon={info.longitude}",
            "module=bssa2014",
            f"vs30={info.target_vs30}",
            f"vref={reference}",
            f"vpga={DEFAULT_STOCH_REFERENCE_VELOCITY}",
        )
        node.add_inputs(input_file, pga)
        intermediate_outputs(node, output)
        node.add_pegasus_profiles(
            label="pmc",
            pmc_request_memory="1",
            pmc_priority=1,
        )
        return node

    def _merge_job(
        self,
        info: RunInfo,
        low_info: RunInfo,
        rupture: RuptureSummary,
        *,
        duration: float,
        timestep: float,
    ) -> Job:
        """Build broadband merging and intensity-measure calculation.

        Args:
            info (RunInfo): Stochastic run metadata.
            low_info (RunInfo): Low-frequency run metadata.
            rupture (RuptureSummary): Rupture metadata.
            duration (float): Seismogram duration in seconds.
            timestep (float): High-frequency timestep in seconds.

        Returns:
            Job: Configured merge job.
        """
        source = rupture.source_id
        rupture_id = rupture.rupture_id
        if self.no_low_site_response:
            low_name = (
                f"Seismogram_{info.site}_{low_info.run_id}_{source}_{rupture_id}.grm"
            )
        else:
            low_name = (
                f"{source}/Seismogram_{info.site}_{low_info.run_id}_{source}_"
                f"{rupture_id}_site_response.grm"
            )
        low_file = File(low_name)
        high_file = File(
            f"{source}/Seismogram_{info.site}_{info.run_id}_{source}_"
            f"{rupture_id}_hf.grm"
        )
        merged = File(
            f"{source}/Seismogram_{info.site}_{info.run_id}_{source}_"
            f"{rupture_id}_bb.grm"
        )
        peak = File(
            f"{source}/PeakVals_{info.site}_{info.run_id}_{source}_{rupture_id}_bb.bsa"
        )
        node = job("MergeIM", f"Merge_IM_{source}_{rupture_id}", version="2.0")
        node.add_args(
            f"lf_seis={low_file.lfn}",
            f"hf_seis={high_file.lfn}",
            f"seis_out={merged.lfn}",
            f"freq={info.low_frequency:.1f}",
            f"comps={3 if self.z_comp else 2}",
            f"num_rup_vars={rupture.variation_count}",
        )
        samples = int(duration / timestep + 0.5)
        filter_frequency = max(100.0, 2.0 * info.max_frequency)
        node.add_args(
            f"simulation_out_pointsX={3 if self.z_comp else 2}",
            "simulation_out_pointsY=1",
            f"simulation_out_timesamples={samples}",
            f"simulation_out_timeskip={timestep}",
            "surfseis_rspectra_seismogram_units=cmpersec",
            "surfseis_rspectra_output_units=cmpersec2",
            "surfseis_rspectra_output_type=aa",
            "surfseis_rspectra_period=all",
            f"surfseis_rspectra_apply_filter_highHZ={filter_frequency}",
            "surfseis_rspectra_apply_byteswap=no",
            f"out={peak.lfn}",
        )
        outputs = [merged, peak]
        if self.no_rotd:
            node.add_args("run_rotd=0")
        else:
            rotd = File(
                f"{source}/RotD_{info.site}_{info.run_id}_{source}_{rupture_id}_bb.rotd"
            )
            outputs.append(rotd)
            node.add_args("run_rotd=1", f"rotd_out={rotd.lfn}")
            if self.z_comp and not self.no_vertical_response:
                vertical = File(
                    f"{source}/VerticalRSP_{info.site}_{info.run_id}_{source}_"
                    f"{rupture_id}_bb.rsp"
                )
                outputs.append(vertical)
                node.add_args("run_vert_rsp=1", f"vert_rsp_out={vertical.lfn}")
            else:
                node.add_args("run_vert_rsp=0")
        if self.no_duration:
            node.add_args("run_duration=0")
        else:
            duration_file = File(
                f"{source}/Duration_{info.site}_{info.run_id}_{source}_"
                f"{rupture_id}_bb.dur"
            )
            outputs.append(duration_file)
            node.add_args("run_duration=1", f"duration_out={duration_file.lfn}")
        if self.period_duration:
            period_file = File(
                f"{source}/PeriodDuration_{info.site}_{info.run_id}_{source}_"
                f"{rupture_id}_bb.dur"
            )
            outputs.append(period_file)
            node.add_args(
                "run_period_durations=1",
                f"period_duration_out={period_file.lfn}",
            )
        else:
            node.add_args("run_period_durations=0")
        node.add_inputs(low_file, high_file)
        final_outputs(node, *outputs)
        memory = math.ceil(
            1.1
            * (rupture.variation_count * 2.0 * samples * 4 * 2 + 4 * 2_000_000.0)
            / (1024.0 * 1024.0)
        )
        node.add_pegasus_profiles(
            label="pmc",
            pmc_request_memory=str(memory),
        )
        return node
