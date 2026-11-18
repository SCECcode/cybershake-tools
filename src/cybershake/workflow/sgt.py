"""SGT workflow generation."""

from __future__ import annotations

import math
import re
from typing import TYPE_CHECKING, cast

from sqlalchemy.exc import SQLAlchemyError

from Pegasus.api import File, Job, Namespace, SubWorkflow, Workflow

from cybershake.constants import (
    AWP_SGT_IDS,
    DEFAULT_JOB_DB_CREDENTIALS,
    DEFAULT_SGT_H_FRACTION,
    DEFAULT_SGT_OUTPUT_SITE,
    DEFAULT_SGT_ROTATION,
    DEFAULT_SGT_TAPER_DEPTH,
    DEFAULT_SGT_TAPER_MODE,
    DEFAULT_SGT_TAPER_MODELS,
    DEFAULT_TOP_LEVEL_SGT_MIN_VS,
    GEN_SGT_WORKFLOW_TRANSFORMATION,
    SGT_SMOOTHING_DISTANCE_KM,
    VELOCITY_MODELS,
    VERSION,
)
from cybershake.exc import RunManagerError, SgtError
from cybershake.runmanager import db as run_db
from cybershake.vs30 import Vs30Error, lookup_vs30
from cybershake.workflow.common import (
    RunInfo as _RunInfo,
)
from cybershake.workflow.common import (
    add_config_metadata,
    load_run_info,
    update_job,
    write_workflow,
)
from cybershake.workflow.common import (
    dependency as _dependency,
)
from cybershake.workflow.common import (
    final_outputs as _final_outputs,
)
from cybershake.workflow.common import (
    intermediate_outputs as _intermediate_outputs,
)
from cybershake.workflow.common import (
    job as _job,
)

if TYPE_CHECKING:
    from pathlib import Path

    from cybershake.config import Config
    from cybershake.runmanager.db import SQLAlchemyRunDB

_GRID_DIMENSION = re.compile(r"\s*n([xyz])\s*=\s*(\d+)\s*")


class SGT:
    """Generate top-level and runtime AWP SGT workflows."""

    _database: SQLAlchemyRunDB | None = None

    def __init__(
        self,
        *,
        config: Config,
        database: SQLAlchemyRunDB | None = None,
    ) -> None:
        """Store options shared by the SGT workflow builders.

        Args:
            config (Config): Resolved CyberShake configuration.
            database (SQLAlchemyRunDB | None, optional): Query repository used to
                load run metadata. Defaults to a repository connected from the
                configured server and environment.
        """
        self.separate_md5 = config.separate_md5
        self.handoff = config.handoff
        self.server = config.server
        self.spacing = (
            config.sgt_spacing if config.command == "full" else config.spacing
        )
        self.min_vs = config.min_vs
        self.no_smoothing = config.no_smoothing
        self.h_fraction = config.h_fraction
        self.ely_taper = config.ely_taper
        self.taper_depth = config.taper_depth
        self.taper_models = config.taper_models
        self.z_comp = config.z_comp
        self.local_vparam_insert = config.local_vparam_insert
        self.config = config
        if database is not None:
            self._database = database

    def create(
        self,
        *,
        workflow_file: Path,
        run_directory: Path,
        run_ids: tuple[int, ...],
    ) -> None:
        """Generate a top-level SGT workflow.

        Args:
            workflow_file (Path): Destination workflow file.
            run_directory (Path): Directory containing workflow inputs and
                outputs.
            run_ids (tuple[int, ...]): Run IDs represented by the workflow. The
                pure-Python migration supports one site and therefore one ID.

        Raises:
            SgtError: If the run metadata is missing, inconsistent, or requests
                an unsupported SGT or velocity-model variation.
        """
        workflow = Workflow(workflow_file.name, infer_dependencies=False)
        add_config_metadata(workflow, self.config)
        self.add_to(
            workflow,
            run_ids=run_ids,
        )
        run_directory.mkdir(parents=True, exist_ok=True)
        write_workflow(workflow, workflow_file)

    def add_to(
        self,
        workflow: Workflow,
        *,
        run_ids: tuple[int, ...],
    ) -> tuple[_RunInfo, Job | SubWorkflow, tuple[Job | SubWorkflow, ...]]:
        """Add the SGT graph to an existing top-level workflow.

        Args:
            workflow (Workflow): Workflow receiving the SGT jobs.
            run_ids (tuple[int, ...]): Run IDs represented by the workflow. The
                pure-Python migration supports exactly one ID.

        Returns:
            tuple[_RunInfo, Job | SubWorkflow, tuple[Job | SubWorkflow, ...]]:
                Run metadata, the final state-changing SGT node, and nodes whose
                SGT outputs must precede post-processing.

        Raises:
            SgtError: If the run metadata is missing, inconsistent, or requests
                an unsupported SGT or velocity-model variation.
        """
        if len(run_ids) != 1:
            raise SgtError("SGT workflow generation requires exactly one run ID")
        velocity_model = cast("str", self.config["velocity_model"])
        sgt_site = self.config.sgt_site
        depth = self.config.depth
        rotation = self.config.rotation
        force_vs30 = self.config.force_vs30
        info = self._run_info(run_ids[0], force_vs30=force_vs30)
        expected_velocity_model_id = VELOCITY_MODELS.get(velocity_model)
        if expected_velocity_model_id != info.velocity_model_id:
            raise SgtError(
                f"Run {info.run_id} uses velocity model {info.velocity_model_id}, "
                f"not {velocity_model}"
            )

        spacing = self.spacing if self.spacing is not None else 0.1 / info.low_frequency
        min_vs = (
            self.min_vs if self.min_vs is not None else DEFAULT_TOP_LEVEL_SGT_MIN_VS
        )
        h_fraction = (
            self.h_fraction if self.h_fraction is not None else DEFAULT_SGT_H_FRACTION
        )
        taper_mode = self.ely_taper or DEFAULT_SGT_TAPER_MODE
        taper_depth = (
            self.taper_depth
            if self.taper_depth is not None
            else DEFAULT_SGT_TAPER_DEPTH
        )
        taper_models = (
            self.taper_models
            if self.taper_models is not None
            else DEFAULT_SGT_TAPER_MODELS
        )
        resolved_rotation = rotation if rotation is not None else DEFAULT_SGT_ROTATION

        update_start = self._update_job(info, "SGT_INIT", "SGT_START")
        pre_cvm = self._pre_cvm_job(
            info,
            spacing=spacing,
            depth=depth,
            rotation=resolved_rotation,
        )
        workflow.add_jobs(update_start, pre_cvm)
        _dependency(workflow, update_start, pre_cvm)

        if info.sgt_variation_id in AWP_SGT_IDS:
            terminal = self._add_awp_parent_jobs(
                workflow,
                info,
                pre_cvm=pre_cvm,
                sgt_site=sgt_site,
                spacing=spacing,
                min_vs=min_vs,
                h_fraction=h_fraction,
                taper_mode=taper_mode,
                taper_depth=taper_depth,
                taper_models=taper_models,
            )
            data_ready: tuple[Job | SubWorkflow, ...] = (terminal,)
        else:
            terminal, merge = self._add_rwg_jobs(
                workflow,
                info,
                pre_cvm=pre_cvm,
                spacing=spacing,
                min_vs=min_vs,
                h_fraction=h_fraction,
            )
            data_ready = (merge,)
        return info, terminal, data_ready

    def create_awp(
        self,
        *,
        run_id: int,
        grid_output_file: Path,
        workflow_file: Path,
    ) -> None:
        """Generate an AWP SGT sub-workflow.

        Args:
            run_id (int): Run represented by the sub-workflow.
            grid_output_file (Path): Grid description produced by PreCVM.
            workflow_file (Path): Destination workflow file.

        Raises:
            SgtError: If the run is not an AWP variation, the grid cannot be
                parsed, or its dimensions cannot use a supported processor
                layout.
        """
        info = self._run_info(run_id)
        if info.sgt_variation_id not in AWP_SGT_IDS:
            raise SgtError(
                f"Run {run_id} uses {info.sgt_type}; an AWP variation is required"
            )

        dimensions = self._volume_dimensions(grid_output_file)
        processors = self._processor_dimensions(info, dimensions)
        spacing = self.spacing
        h_fraction = (
            self.h_fraction if self.h_fraction is not None else DEFAULT_SGT_H_FRACTION
        )
        taper_mode = self.ely_taper or DEFAULT_SGT_TAPER_MODE
        taper_depth = (
            self.taper_depth
            if self.taper_depth is not None
            else DEFAULT_SGT_TAPER_DEPTH
        )
        taper_models = self.taper_models if self.taper_models is not None else "none"
        smoothing = not self.no_smoothing
        grid_name = grid_output_file.name

        workflow = Workflow(
            f"AWP_SGT_{info.site}.dax",
            infer_dependencies=False,
        )
        velocity = self._awp_velocity_mesh_job(
            info,
            grid_name=grid_name,
            spacing=spacing,
            h_fraction=h_fraction,
            taper_mode=taper_mode,
            taper_depth=taper_depth,
            taper_models=taper_models,
        )
        pre_sgt = self._awp_pre_sgt_job(
            info,
            grid_name=grid_name,
            spacing=spacing,
        )
        workflow.add_jobs(velocity, pre_sgt)

        smooth: Job | None = None
        if smoothing:
            smooth = self._smoothing_job(
                info,
                grid_name=grid_name,
                spacing=spacing,
            )
            workflow.add_jobs(smooth)
            _dependency(workflow, velocity, smooth)

        velocity_params = self._velocity_params_job(
            info,
            grid_name=grid_name,
            smoothing=smoothing,
        )
        pre_awp = self._pre_awp_job(
            info,
            grid_name=grid_name,
            processors=processors,
            spacing=spacing,
            smoothing=smoothing,
        )
        workflow.add_jobs(velocity_params, pre_awp)
        _dependency(workflow, smooth or velocity, velocity_params)
        _dependency(workflow, pre_sgt, velocity_params, pre_awp)
        _dependency(workflow, velocity, pre_awp)
        if smooth is not None:
            _dependency(workflow, smooth, pre_awp)

        post_jobs, nan_jobs = self._add_awp_component_jobs(
            workflow,
            info,
            pre_awp=pre_awp,
            smooth=smooth,
            processors=processors,
            grid_name=grid_name,
            smoothing=smoothing,
        )
        self._add_awp_completion_jobs(
            workflow,
            info,
            velocity_params=velocity_params,
            post_jobs=post_jobs,
            nan_jobs=nan_jobs,
        )

        write_workflow(workflow, workflow_file)

    def _add_awp_component_jobs(
        self,
        workflow: Workflow,
        info: _RunInfo,
        *,
        pre_awp: Job,
        smooth: Job | None,
        processors: tuple[int, int, int],
        grid_name: str,
        smoothing: bool,
    ) -> tuple[dict[str, Job], dict[str, Job]]:
        """Add simulation, post-processing, and NaN-check jobs by component.

        Args:
            workflow (Workflow): Runtime AWP workflow being assembled.
            info (_RunInfo): Run metadata used by each job.
            pre_awp (Job): PreAWP job that prepares simulation inputs.
            smooth (Job | None): Optional velocity smoothing job.
            processors (tuple[int, int, int]): Processor counts in X, Y, and Z.
            grid_name (str): Logical name of the PreCVM grid output.
            smoothing (bool): Whether jobs consume the smoothed velocity mesh.

        Returns:
            tuple[dict[str, Job], dict[str, Job]]: PostAWP and NaN-check jobs,
            keyed by component.
        """
        components = ("x", "y", "z") if self.z_comp else ("x", "y")
        post_jobs: dict[str, Job] = {}
        nan_jobs: dict[str, Job] = {}
        for component in components:
            simulation = self._awp_simulation_job(info, component, processors)
            post = self._post_awp_job(
                info,
                component,
                grid_name=grid_name,
                smoothing=smoothing,
            )
            nan_check = self._awp_nan_job(info, component)
            workflow.add_jobs(simulation, post, nan_check)
            _dependency(workflow, pre_awp, simulation)
            _dependency(workflow, simulation, post, nan_check)
            if smooth is not None:
                _dependency(workflow, smooth, post)
            post_jobs[component] = post
            nan_jobs[component] = nan_check
        return post_jobs, nan_jobs

    def _add_awp_completion_jobs(
        self,
        workflow: Workflow,
        info: _RunInfo,
        *,
        velocity_params: Job,
        post_jobs: dict[str, Job],
        nan_jobs: dict[str, Job],
    ) -> None:
        """Add checksum, insertion, handoff, and run-completion jobs.

        Args:
            workflow (Workflow): Runtime AWP workflow being assembled.
            info (_RunInfo): Run metadata used by completion jobs.
            velocity_params (Job): Velocity-parameter calculation job.
            post_jobs (dict[str, Job]): PostAWP jobs keyed by component.
            nan_jobs (dict[str, Job]): NaN-check jobs keyed by component.
        """
        update_end = self._update_job(info, "SGT_START", "SGT_END")
        workflow.add_jobs(update_end)
        _dependency(workflow, velocity_params, update_end)

        md5_jobs: dict[str, Job] = {}
        if self.separate_md5:
            for component in post_jobs:
                md5 = self._md5_job(info, component)
                workflow.add_jobs(md5)
                _dependency(workflow, md5, update_end)
                md5_jobs[component] = md5
        else:
            for post in post_jobs.values():
                _dependency(workflow, post, update_end)

        for nan_check in nan_jobs.values():
            _dependency(workflow, nan_check, update_end)
        if self.local_vparam_insert:
            insert = self._insert_velocity_params_job(info)
            workflow.add_jobs(insert)
            _dependency(workflow, velocity_params, insert)
            _dependency(workflow, insert, update_end)
        if self.handoff:
            handoff = self._handoff_job(info)
            workflow.add_jobs(handoff)
            for parent in (*post_jobs.values(), *nan_jobs.values(), *md5_jobs.values()):
                _dependency(workflow, parent, handoff)
            _dependency(workflow, handoff, update_end)

    def _run_info(self, run_id: int, *, force_vs30: float | None = None) -> _RunInfo:
        """Load and validate the run metadata used by SGT builders.

        Args:
            run_id (int): CyberShake run ID.
            force_vs30 (float | None): Full-workflow target Vs30 override.

        Returns:
            _RunInfo: Validated generator metadata.

        Raises:
            SgtError: If the database cannot be queried or required run, site,
                SGT, velocity-model, or frequency data is unavailable, or Vs30
                cannot be resolved or stored.
        """
        if force_vs30 is not None and force_vs30 <= 0:
            raise SgtError("Forced Vs30 must be positive")
        database = self._database
        if database is None:
            try:
                database = run_db.connect(server=self.server, config=self.config)
            except RunManagerError as error:
                raise SgtError(str(error)) from error
        try:
            info = load_run_info(database, run_id)
        except SQLAlchemyError as error:
            raise SgtError(f"Unable to load run {run_id}: {error}") from error
        except ValueError as error:
            raise SgtError(str(error)) from error
        if info.target_vs30 is None or info.target_vs30 <= 0:
            try:
                classpath = (
                    None if self.config is None else str(self.config["vs30_classpath"])
                )
                result = lookup_vs30(info.latitude, info.longitude, classpath=classpath)
            except Vs30Error as error:
                raise SgtError(str(error)) from error
            self._store_vs30(database, run_id, result.value, result.source)
            info = info._replace(target_vs30=result.value)
        if force_vs30 is not None:
            self._store_vs30(database, run_id, force_vs30, "User-provided")
            info = info._replace(target_vs30=force_vs30)
        return info

    @staticmethod
    def _store_vs30(
        database: SQLAlchemyRunDB, run_id: int, value: float, source: str
    ) -> None:
        """Store a run's target Vs30 and source."""
        try:
            updated = database.update_target_vs30(
                run_id, target_vs30=value, source=source
            )
        except (SQLAlchemyError, ValueError) as error:
            raise SgtError(f"Unable to store Vs30 for run {run_id}") from error
        if not updated:
            raise SgtError(f"Run {run_id} disappeared while storing Vs30")

    @staticmethod
    def _update_job(info: _RunInfo, old_state: str, new_state: str) -> Job:
        """Build a deferred run-state update job.

        Args:
            info (_RunInfo): Run metadata.
            old_state (str): Symbolic source state.
            new_state (str): Symbolic destination state.

        Returns:
            Job: Local run-state update job.
        """
        return update_job(
            info.run_id,
            old_state,
            new_state,
            identifier=f"UpdateRun_{new_state}_{info.site}",
            local=True,
        )

    def _pre_cvm_job(
        self,
        info: _RunInfo,
        *,
        spacing: float,
        depth: float | None,
        rotation: float,
    ) -> Job:
        """Build the job that creates the simulation grid and model files.

        Args:
            info (_RunInfo): Run metadata.
            spacing (float): Grid spacing in km.
            depth (float | None): Optional simulation depth in km.
            rotation (float): Simulation-volume rotation in degrees.

        Returns:
            Job: Configured PreCVM job.
        """
        job = _job("PreCVM", f"PreCVM_{info.site}")
        modelbox = File(f"{info.site}.modelbox")
        gridfile = File(f"gridfile_{info.site}")
        gridout = File(f"gridout_{info.site}")
        coordinates = File(f"model_coords_GC_{info.site}")
        parameters = File(f"model_params_GC_{info.site}")
        bounds = File(f"model_bounds_GC_{info.site}")
        job.add_args(
            f"--site {info.site}",
            f"--erf_id {info.erf_id}",
            f"--modelbox {modelbox.lfn}",
            f"--gridfile {gridfile.lfn}",
            f"--gridout {gridout.lfn}",
            f"--coordfile {coordinates.lfn}",
            f"--paramsfile {parameters.lfn}",
            f"--boundsfile {bounds.lfn}",
            f"--frequency {info.low_frequency:.1f}",
        )
        if info.sgt_type == "awp_gpu":
            job.add_args("--gpu")
        job.add_args(f"--server {self.server}")
        if spacing > 0:
            job.add_args(f"--spacing {spacing}")
        if depth is not None and depth > 0:
            job.add_args(f"--depth {depth}")
        job.add_args(f"--rotation {rotation}")
        _intermediate_outputs(
            job,
            modelbox,
            gridfile,
            gridout,
            coordinates,
            parameters,
            bounds,
        )
        return job

    def _add_awp_parent_jobs(
        self,
        workflow: Workflow,
        info: _RunInfo,
        *,
        pre_cvm: Job,
        sgt_site: str | None,
        spacing: float,
        min_vs: float,
        h_fraction: float,
        taper_mode: str,
        taper_depth: float,
        taper_models: str,
    ) -> SubWorkflow:
        """Add deferred AWP generation and its nested workflow node.

        Args:
            workflow (Workflow): Top-level SGT workflow.
            info (_RunInfo): Run metadata.
            pre_cvm (Job): Grid-generation parent job.
            sgt_site (str | None): Optional nested-workflow execution site.
            spacing (float): Grid spacing in km.
            min_vs (float): Minimum shear-wave velocity in m/s.
            h_fraction (float): Grid-point depth fraction.
            taper_mode (str): Ely taper selection mode.
            taper_depth (float): Ely taper depth in metres.
            taper_models (str): Models to which the taper applies.

        Returns:
            SubWorkflow: Nested AWP workflow that completes the SGT stage.
        """
        workflow_name = f"AWP_SGT_{info.site}.dax"
        generated_workflow = File(workflow_name)
        gridout = File(f"gridout_{info.site}")
        modelbox = File(f"{info.site}.modelbox")
        coordinates = File(f"model_coords_GC_{info.site}")
        generator = self._awp_generator_job(
            info,
            generated_workflow=generated_workflow,
            gridout=gridout,
            modelbox=modelbox,
            coordinates=coordinates,
            spacing=spacing,
            min_vs=min_vs,
            h_fraction=h_fraction,
            taper_mode=taper_mode,
            taper_depth=taper_depth,
            taper_models=taper_models,
        )

        sub_workflow = SubWorkflow(workflow_name, _id=f"AWP_SGT_{info.site}")
        arguments = ["--force"]
        if sgt_site is not None:
            arguments.extend(("-s", sgt_site))
        arguments.extend(
            (
                "--basename",
                f"AWP_SGT_{info.site}",
                "--cleanup",
                "inplace",
                "--output-sites",
                DEFAULT_SGT_OUTPUT_SITE,
            )
        )
        sub_workflow.add_args(*arguments)
        sub_workflow.add_inputs(modelbox, gridout, coordinates)
        workflow.add_jobs(generator, sub_workflow)
        _dependency(workflow, pre_cvm, generator, sub_workflow)
        _dependency(workflow, generator, sub_workflow)
        return sub_workflow

    def _awp_generator_job(
        self,
        info: _RunInfo,
        *,
        generated_workflow: File,
        gridout: File,
        modelbox: File,
        coordinates: File,
        spacing: float,
        min_vs: float,
        h_fraction: float,
        taper_mode: str,
        taper_depth: float,
        taper_models: str,
    ) -> Job:
        """Build the local command that generates the runtime AWP workflow.

        Args:
            info (_RunInfo): Run metadata.
            generated_workflow (File): Runtime workflow output file.
            gridout (File): PreCVM grid output.
            modelbox (File): PreCVM model-box output.
            coordinates (File): PreCVM coordinate output.
            spacing (float): Grid spacing in km.
            min_vs (float): Minimum shear-wave velocity in m/s.
            h_fraction (float): Grid-point depth fraction.
            taper_mode (str): Ely taper selection mode.
            taper_depth (float): Ely taper depth in metres.
            taper_models (str): Models to which the taper applies.

        Returns:
            Job: Local ``cybershake sub-sgt-wf`` job.
        """
        generator = _job(
            GEN_SGT_WORKFLOW_TRANSFORMATION,
            f"GenSGTDax_{info.site}",
        )
        generator.add_args(
            "sub-sgt-wf",
            "--run-id",
            str(info.run_id),
            "--grid-output-file",
            gridout.lfn,
            "--workflow-file",
            generated_workflow.lfn,
            "--server",
            self.server,
            "--ely-taper",
            taper_mode,
        )
        if spacing > 0:
            generator.add_args("--spacing", str(spacing))
        if min_vs > 0:
            generator.add_args("--min-vs", str(min_vs))
        if h_fraction >= 0:
            generator.add_args("--h-fraction", str(h_fraction))
        if self.separate_md5:
            generator.add_args("--separate-md5")
        if self.handoff:
            generator.add_args("--handoff")
        if self.no_smoothing:
            generator.add_args("--no-smoothing")
        if taper_mode != "none":
            generator.add_args(
                "--taper-depth",
                str(taper_depth),
                "--taper-models",
                taper_models,
            )
        if self.z_comp:
            generator.add_args("--z-comp")
        if self.local_vparam_insert:
            generator.add_args("--local-vparam-insert")
        generator.add_inputs(gridout, modelbox, coordinates)
        _intermediate_outputs(generator, generated_workflow)
        generator.add_profiles(Namespace.SELECTOR, executionPool="local")
        return generator

    def _add_rwg_jobs(
        self,
        workflow: Workflow,
        info: _RunInfo,
        *,
        pre_cvm: Job,
        spacing: float,
        min_vs: float,
        h_fraction: float,
    ) -> tuple[Job, Job]:
        """Add the complete static RWG SGT graph.

        Args:
            workflow (Workflow): Top-level SGT workflow.
            info (_RunInfo): Run metadata.
            pre_cvm (Job): Grid-generation parent job.
            spacing (float): Grid spacing in km.
            min_vs (float): Minimum shear-wave velocity in m/s.
            h_fraction (float): Grid-point depth fraction.

        Returns:
            tuple[Job, Job]: Final state-update job and SGT merge job.
        """
        velocity = self._rwg_velocity_mesh_job(
            info,
            spacing,
            min_vs=min_vs,
            h_fraction=h_fraction,
        )
        pre_sgt = self._rwg_pre_sgt_job(info, spacing)
        update_end = self._update_job(info, "SGT_START", "SGT_END")
        sgt_x = self._rwg_simulation_job(info, "x")
        sgt_y = self._rwg_simulation_job(info, "y")
        nan_test = self._rwg_nan_job(info)
        merge = self._rwg_merge_job(info)
        workflow.add_jobs(
            velocity,
            pre_sgt,
            update_end,
            sgt_x,
            sgt_y,
            nan_test,
            merge,
        )
        _dependency(workflow, pre_cvm, velocity, pre_sgt, sgt_x, sgt_y)
        _dependency(workflow, pre_sgt, sgt_x, sgt_y)
        _dependency(workflow, velocity, sgt_x, sgt_y)
        _dependency(workflow, sgt_x, nan_test, merge)
        _dependency(workflow, sgt_y, nan_test, merge)
        _dependency(workflow, nan_test, update_end)
        _dependency(workflow, merge, update_end)
        return update_end, merge

    @staticmethod
    def _rwg_velocity_mesh_job(
        info: _RunInfo,
        spacing: float,
        *,
        min_vs: float,
        h_fraction: float,
    ) -> Job:
        """Build an RWG velocity-mesh job.

        Args:
            info (_RunInfo): Run metadata.
            spacing (float): Grid spacing in km.
            min_vs (float): Minimum shear-wave velocity in m/s.
            h_fraction (float): Grid-point depth fraction.

        Returns:
            Job: Configured UCVMMesh job.
        """
        job = _job("UCVMMesh", f"UCVMMesh_{info.site}")
        gridout = File(f"gridout_{info.site}")
        coordinates = File(f"model_coords_GC_{info.site}")
        job.add_args(
            f"--site {info.site}",
            f"--gridout {gridout.lfn}",
            f"--coordfile {coordinates.lfn}",
            f"--models {info.velocity_model}",
            "--format rwg",
            f"--frequency {info.low_frequency:.1f}",
        )
        if spacing > 0:
            job.add_args(f"--spacing {spacing}")
        if min_vs > 0:
            job.add_args(f"--min_vs {min_vs}")
        job.add_args(f"--h_fraction {h_fraction}")
        job.add_inputs(gridout, coordinates)
        if info.site == "TEST":
            job.add_pegasus_profiles(cores=155)
        return job

    @staticmethod
    def _rwg_pre_sgt_job(info: _RunInfo, spacing: float) -> Job:
        """Build the RWG fault and SGT-coordinate preparation job.

        Args:
            info (_RunInfo): Run metadata.
            spacing (float): Grid spacing in km.

        Returns:
            Job: Configured PreSGT job.
        """
        job = _job("PreSGT", f"PreSGT_{info.site}")
        modelbox = File(f"{info.site}.modelbox")
        gridout = File(f"gridout_{info.site}")
        coordinates = File(f"model_coords_GC_{info.site}")
        fdloc = File(f"{info.site}.fdloc")
        faultlist = File(f"{info.site}.faultlist")
        radius = File(f"{info.site}.radiusfile")
        sgt_coordinates = File(f"{info.site}.cordfile")
        job.add_args(
            info.site,
            str(info.erf_id),
            modelbox,
            gridout,
            coordinates,
            fdloc,
            faultlist,
            radius,
            sgt_coordinates,
            str(spacing),
        )
        job.add_inputs(modelbox, gridout, coordinates)
        _intermediate_outputs(job, fdloc, faultlist, radius, sgt_coordinates)
        return job

    @staticmethod
    def _rwg_simulation_job(info: _RunInfo, component: str) -> Job:
        """Build one component of the RWG SGT simulation.

        Args:
            info (_RunInfo): Run metadata.
            component (str): Horizontal component, ``x`` or ``y``.

        Returns:
            Job: Configured SGTGen job.
        """
        version = "3.0.3" if info.sgt_type == "rwg3.0.3" else VERSION
        job = _job(
            "SGTGen",
            f"SGTGen_{component}_{info.site}",
            version=version,
        )
        gridout = File(f"gridout_{info.site}")
        modelbox = File(f"{info.site}.modelbox")
        fdloc = File(f"{info.site}.fdloc")
        coordinates = File(f"{info.site}.cordfile")
        job.add_args(info.site, gridout, modelbox, fdloc, coordinates, component)
        job.add_inputs(gridout, modelbox, fdloc, coordinates)
        if info.site == "TEST":
            job.add_pegasus_profiles(runtime=3600, nodes=60, cores=960)
        return job

    @staticmethod
    def _rwg_nan_job(info: _RunInfo) -> Job:
        """Build the combined RWG NaN validation job.

        Args:
            info (_RunInfo): Run metadata.

        Returns:
            Job: Configured NanTest job.
        """
        processors = 240 if info.site == "TEST" else 4000
        return _job("NanTest", f"NanTest_{info.site}").add_args(
            f"{info.site}-fx_sgt-",
            f"{info.site}-fy_sgt-",
            str(processors),
        )

    @staticmethod
    def _rwg_merge_job(info: _RunInfo) -> Job:
        """Build the job that merges and checksums RWG outputs.

        Args:
            info (_RunInfo): Run metadata.

        Returns:
            Job: Configured SGTMerge job with registered outputs.
        """
        job = _job("SGTMerge", f"SGTMerge_{info.site}")
        sgt_x = File(f"{info.site}_fx_{info.run_id}.sgt")
        sgt_y = File(f"{info.site}_fy_{info.run_id}.sgt")
        md5_x = File(f"{sgt_x.lfn}.md5")
        md5_y = File(f"{sgt_y.lfn}.md5")
        job.add_args(info.site, sgt_x, sgt_y)
        _final_outputs(job, sgt_x, md5_x, sgt_y, md5_y)
        job.add_pegasus_profiles(runtime=10800)
        return job

    @staticmethod
    def _volume_dimensions(grid_output_file: Path) -> tuple[int, int, int]:
        """Read AWP-ordered dimensions from a PreCVM grid file.

        Args:
            grid_output_file (Path): Grid file containing ``nx``, ``ny``, and
                ``nz`` assignments.

        Returns:
            tuple[int, int, int]: AWP X, Y, and Z point counts. PreCVM's RWG X
            and Y axes are exchanged.

        Raises:
            SgtError: If the file cannot be read or omits a dimension.
        """
        try:
            lines = grid_output_file.read_text(encoding="utf-8").splitlines()
        except OSError as error:
            raise SgtError(
                f"Unable to read grid file {grid_output_file}: {error}"
            ) from error
        dimensions: dict[str, int] = {}
        for line in lines:
            match = _GRID_DIMENSION.fullmatch(line)
            if match is not None:
                dimensions[match.group(1)] = int(match.group(2))
        if dimensions.keys() != {"x", "y", "z"}:
            raise SgtError(
                f"Grid file {grid_output_file} must contain nx, ny, and nz dimensions"
            )
        # PreCVM writes RWG axes; AWP exchanges X and Y.
        return dimensions["y"], dimensions["x"], dimensions["z"]

    @classmethod
    def _processor_dimensions(
        cls,
        info: _RunInfo,
        dimensions: tuple[int, int, int],
    ) -> tuple[int, int, int]:
        """Choose the CPU or GPU processor layout for an AWP volume.

        Args:
            info (_RunInfo): Run metadata identifying the AWP implementation.
            dimensions (tuple[int, int, int]): AWP X, Y, and Z point counts.

        Returns:
            tuple[int, int, int]: Processor counts in X, Y, and Z.

        Raises:
            SgtError: If the SGT variation is not an AWP implementation.
        """
        if info.sgt_type == "awp_gpu":
            return cls._gpu_processor_dimensions(info, dimensions)
        if info.sgt_type == "awp":
            return cls._cpu_processor_dimensions(dimensions)
        raise SgtError(f"Unsupported AWP type {info.sgt_type}")

    @staticmethod
    def _cpu_processor_dimensions(
        dimensions: tuple[int, int, int],
    ) -> tuple[int, int, int]:
        """Choose the legacy CPU AWP processor layout.

        Args:
            dimensions (tuple[int, int, int]): AWP X, Y, and Z point counts.

        Returns:
            tuple[int, int, int]: CPU processor counts in X, Y, and Z.

        Raises:
            SgtError: If the volume cannot be divided into supported chunks.
        """
        x_points, y_points, z_points = dimensions
        if z_points % 50:
            raise SgtError(f"AWP Z dimension {z_points} is not divisible by 50")

        if x_points % 64 == 0 or y_points % 64 == 0:
            x_chunk = 64 if x_points % 64 == 0 else 50
            y_chunk = 64 if y_points % 64 == 0 else 50
        else:
            chunk = 63
            adjustment = 2
            while chunk != 50 and x_points % chunk and y_points % chunk:
                adjustment = int(math.copysign(abs(adjustment) + 1, -adjustment))
                chunk += adjustment
            x_chunk = chunk if x_points % chunk == 0 else 50
            y_chunk = chunk if y_points % chunk == 0 else 50
        if x_points % x_chunk or y_points % y_chunk:
            raise SgtError(
                "AWP CPU dimensions "
                f"{x_points}x{y_points} cannot use {x_chunk}x{y_chunk} chunks"
            )
        return x_points // x_chunk, y_points // y_chunk, z_points // 50

    @staticmethod
    def _gpu_processor_dimensions(
        info: _RunInfo,
        dimensions: tuple[int, int, int],
    ) -> tuple[int, int, int]:
        """Choose the legacy GPU AWP processor layout.

        Args:
            info (_RunInfo): Run metadata containing the simulation frequency.
            dimensions (tuple[int, int, int]): AWP X, Y, and Z point counts.

        Returns:
            tuple[int, int, int]: GPU processor counts in X, Y, and Z.

        Raises:
            SgtError: If the horizontal dimensions do not support the selected
                layout and output decimation.
        """
        x_points, y_points, z_points = dimensions
        volume_points = x_points * y_points * z_points
        if info.low_frequency < 1.0 or volume_points < 2_000_000_000:
            x_processors, y_processors = 10, 10
        elif volume_points < 7_000_000_000:
            x_processors, y_processors = 20, 10
        else:
            x_processors, y_processors = 40, 20
        x_divisor = 2 * x_processors
        y_divisor = 2 * y_processors
        if x_points % x_divisor or y_points % y_divisor:
            raise SgtError(
                "AWP GPU dimensions "
                f"{x_points}x{y_points} are not divisible by "
                f"{x_divisor}x{y_divisor}"
            )
        return x_processors, y_processors, 1

    def _awp_velocity_mesh_job(
        self,
        info: _RunInfo,
        *,
        grid_name: str,
        spacing: float | None,
        h_fraction: float,
        taper_mode: str,
        taper_depth: float,
        taper_models: str,
    ) -> Job:
        """Build the AWP velocity-mesh generation job.

        Args:
            info (_RunInfo): Run metadata.
            grid_name (str): Logical name of the PreCVM grid output.
            spacing (float | None): Optional grid spacing override in km.
            h_fraction (float): Grid-point depth fraction.
            taper_mode (str): Ely taper selection mode.
            taper_depth (float): Ely taper depth in metres.
            taper_models (str): Models to which the taper applies.

        Returns:
            Job: Configured UCVMMesh job.
        """
        job = _job("UCVMMesh", f"UCVMMesh_{info.site}")
        gridout = File(grid_name)
        coordinates = File(f"model_coords_GC_{info.site}")
        mesh = File(f"awp.{info.site}.media")
        mesh_format = "awp" if taper_models in {"", "all", "none"} else "awpz"
        job.add_args(
            f"--site {info.site}",
            f"--gridout {gridout.lfn}",
            f"--coordfile {coordinates.lfn}",
            f"--models {info.velocity_model}",
            f"--format {mesh_format}",
            f"--frequency {info.low_frequency:.1f}",
        )
        if spacing is not None and spacing > 0:
            job.add_args(f"--spacing {spacing}")
        if self.min_vs is not None and self.min_vs > 0:
            job.add_args(f"--min_vs {self.min_vs}")
        job.add_args(
            f"--h_fraction {h_fraction}",
            f"--ely-taper {taper_mode}",
            f"--taper-depth {taper_depth}",
            f"--taper-models {taper_models}",
        )
        job.add_inputs(gridout, coordinates)
        _intermediate_outputs(job, mesh)
        if info.site == "TEST":
            job.add_pegasus_profiles(nodes="10")

        return job

    @staticmethod
    def _awp_pre_sgt_job(
        info: _RunInfo,
        *,
        grid_name: str,
        spacing: float | None,
    ) -> Job:
        """Build the AWP fault and SGT-coordinate preparation job.

        Args:
            info (_RunInfo): Run metadata.
            grid_name (str): Logical name of the PreCVM grid output.
            spacing (float | None): Optional grid spacing override in km.

        Returns:
            Job: Configured PreSGT job.
        """
        job = _job("PreSGT", f"PreSGT_{info.site}")
        modelbox = File(f"{info.site}.modelbox")
        gridout = File(grid_name)
        coordinates = File(f"model_coords_GC_{info.site}")
        fdloc = File(f"{info.site}.fdloc")
        faultlist = File(f"{info.site}.faultlist")
        radius = File(f"{info.site}.radiusfile")
        sgt_coordinates = File(f"{info.site}.cordfile")
        resolved_spacing = spacing or 0.1 / info.low_frequency
        job.add_args(
            f"--site {info.site}",
            f"--erf_id {info.erf_id}",
            f"--modelbox {modelbox.lfn}",
            f"--gridout {gridout.lfn}",
            f"--coordfile {coordinates.lfn}",
            f"--fdloc {fdloc.lfn}",
            f"--faultlist {faultlist.lfn}",
            f"--radiusfile {radius.lfn}",
            f"--sgtcords {sgt_coordinates.lfn}",
            f"--spacing {resolved_spacing}",
            f"--frequency {info.low_frequency:.1f}",
        )
        if info.rupture_variation_id == 8:
            job.add_args("--rsqsim")
        job.add_inputs(modelbox, gridout, coordinates)
        _intermediate_outputs(job, fdloc, faultlist, radius, sgt_coordinates)
        return job

    @staticmethod
    def _smoothing_job(
        info: _RunInfo,
        *,
        grid_name: str,
        spacing: float | None,
    ) -> Job:
        """Build the AWP velocity-model smoothing job.

        Args:
            info (_RunInfo): Run metadata.
            grid_name (str): Logical name of the PreCVM grid output.
            spacing (float | None): Optional grid spacing override in km.

        Returns:
            Job: Configured Smooth job.
        """
        job = _job("Smooth", f"Smooth_{info.site}")
        gridout = File(grid_name)
        coordinates = File(f"model_coords_GC_{info.site}")
        mesh = File(f"awp.{info.site}.media")
        smoothed_mesh = File(f"awp.{info.site}.smoothed.media")
        resolved_spacing = spacing or 0.1 / info.low_frequency
        smoothing_distance = math.ceil(SGT_SMOOTHING_DISTANCE_KM / resolved_spacing)
        job.add_args(
            f"--gridout {gridout.lfn}",
            f"--coords {coordinates.lfn}",
            f"--models {info.velocity_model}",
            f"--smoothing-dist {smoothing_distance}",
            f"--mesh {mesh.lfn}",
            f"--mesh-out {smoothed_mesh.lfn}",
        )
        job.add_inputs(gridout, coordinates, mesh)
        _intermediate_outputs(job, smoothed_mesh)
        return job

    def _velocity_params_job(
        self,
        info: _RunInfo,
        *,
        grid_name: str,
        smoothing: bool,
    ) -> Job:
        """Build the velocity-parameter calculation or insertion job.

        Args:
            info (_RunInfo): Run metadata and site coordinates.
            grid_name (str): Logical name of the PreCVM grid output.
            smoothing (bool): Whether to inspect the smoothed velocity mesh.

        Returns:
            Job: Configured Velocity_Params job.
        """
        job = _job("Velocity_Params", f"Velocity_Params_{info.site}")
        mesh_suffix = "smoothed.media" if smoothing else "media"
        mesh = File(f"awp.{info.site}.{mesh_suffix}")
        gridout = File(grid_name)
        fdloc = File(f"{info.site}.fdloc")
        job.add_args(
            f"-lat {info.latitude}",
            f"-lon {info.longitude}",
            f"-m {info.velocity_model}",
            f"-vmesh {mesh.lfn}",
            f"-fd {fdloc.lfn}",
            f"-go {gridout.lfn}",
            f"-s {self.server}",
            f"-r {info.run_id}",
        )
        if self.local_vparam_insert:
            parameter_file = File(f"{info.site}_{info.run_id}_velocity_params.txt")
            job.add_args(f"-o {parameter_file.lfn}")
            _intermediate_outputs(job, parameter_file)
        job.add_inputs(mesh, gridout, fdloc)
        return job

    def _pre_awp_job(
        self,
        info: _RunInfo,
        *,
        grid_name: str,
        processors: tuple[int, int, int],
        spacing: float | None,
        smoothing: bool,
    ) -> Job:
        """Build the job that prepares component inputs for AWP.

        Args:
            info (_RunInfo): Run metadata.
            grid_name (str): Logical name of the PreCVM grid output.
            processors (tuple[int, int, int]): Processor counts in X, Y, and Z.
            spacing (float | None): Optional grid spacing override in km.
            smoothing (bool): Whether to use the smoothed velocity mesh.

        Returns:
            Job: Configured CPU or GPU PreAWP job.
        """
        transformation = "PreAWP_GPU" if info.sgt_type == "awp_gpu" else "PreAWP"
        job = _job(transformation, f"{transformation}_{info.site}")
        gridout = File(grid_name)
        fdloc = File(f"{info.site}.fdloc")
        coordinates = File(f"{info.site}.cordfile")
        job.add_args(
            f"--site {info.site}",
            f"--gridout {gridout.lfn}",
            f"--fdloc {fdloc.lfn}",
            f"--cordfile {coordinates.lfn}",
            f"--frequency {info.low_frequency:.1f}",
            f"--px {processors[0]}",
            f"--py {processors[1]}",
            f"--pz {processors[2]}",
            f"--source-frequency {info.source_frequency}",
            f"--run_id {info.run_id}",
        )
        if spacing is not None and spacing > 0:
            job.add_args(f"--spacing {spacing}")
        if smoothing:
            job.add_args(f"--velocity-mesh awp.{info.site}.smoothed.media")
        if self.z_comp:
            job.add_args("--z_comp")
        job.add_inputs(gridout, fdloc, coordinates)
        return job

    @staticmethod
    def _awp_simulation_job(
        info: _RunInfo,
        component: str,
        processors: tuple[int, int, int],
    ) -> Job:
        """Build one CPU or GPU AWP simulation job.

        Args:
            info (_RunInfo): Run metadata identifying the AWP implementation.
            component (str): Simulation component, ``x``, ``y``, or ``z``.
            processors (tuple[int, int, int]): Processor counts in X, Y, and Z.

        Returns:
            Job: Configured AWP or AWP_GPU job.
        """
        transformation = "AWP_GPU" if info.sgt_type == "awp_gpu" else "AWP"
        job = _job(
            transformation,
            f"{transformation}_{info.site}_{component}",
        )
        if info.sgt_type == "awp":
            job.add_args(str(math.prod(processors)))
        job.add_args(f"IN3D.{info.site}.{component}")
        return job

    def _post_awp_job(
        self,
        info: _RunInfo,
        component: str,
        *,
        grid_name: str,
        smoothing: bool,
    ) -> Job:
        """Build one component's AWP output conversion job.

        Args:
            info (_RunInfo): Run metadata.
            component (str): AWP component, ``x``, ``y``, or ``z``.
            grid_name (str): Logical name of the PreCVM grid output.
            smoothing (bool): Whether to consume the smoothed velocity mesh.

        Returns:
            Job: Configured PostAWP job with registered SGT outputs.
        """
        job = _job("PostAWP", f"PostAWP_{info.site}_{component}")
        output_component = {"x": "y", "y": "x", "z": "z"}[component]
        input_strain = (
            f"comp_{component}/output_sgt/awp-strain-{info.site}-f{component}"
        )
        output_sgt = File(f"{info.site}_f{output_component}_{info.run_id}.sgt")
        header = File(f"{info.site}_f{output_component}_{info.run_id}.sgthead")
        modelbox = File(f"{info.site}.modelbox")
        coordinates = File(f"{info.site}.cordfile")
        fdloc = File(f"{info.site}.fdloc")
        gridout = File(grid_name)
        input_source = f"IN3D.{info.site}.{component}"
        mesh_suffix = "smoothed.media" if smoothing else "media"
        mesh = File(f"awp.{info.site}.{mesh_suffix}")
        job.add_args(
            info.site,
            input_strain,
            output_sgt,
            modelbox,
            coordinates,
            fdloc,
            gridout,
            input_source,
            mesh,
            component,
            str(info.run_id),
            header,
            f"{info.low_frequency:.1f}",
        )
        if self.separate_md5:
            job.add_args("-n")
        if info.source_frequency != info.low_frequency:
            job.add_args(f"-s {info.source_frequency}")
        if info.low_frequency > 0.5:
            job.add_args(str(info.low_frequency))
        job.add_inputs(modelbox, coordinates, fdloc, gridout, mesh)
        _final_outputs(job, output_sgt, header)
        if not self.separate_md5:
            _final_outputs(job, File(f"{output_sgt.lfn}.md5"))
        return job

    @staticmethod
    def _md5_job(info: _RunInfo, component: str) -> Job:
        """Build a separate checksum job for one AWP component.

        Args:
            info (_RunInfo): Run metadata.
            component (str): AWP component, ``x``, ``y``, or ``z``.

        Returns:
            Job: Configured MD5 job with a registered checksum output.
        """
        output_component = {"x": "y", "y": "x", "z": "z"}[component]
        sgt = File(f"{info.site}_f{output_component}_{info.run_id}.sgt")
        checksum = File(f"{sgt.lfn}.md5")
        job = _job("MD5", f"MD5_{component}").add_args(sgt.lfn)
        job.add_inputs(sgt)
        _final_outputs(job, checksum)
        return job

    @staticmethod
    def _awp_nan_job(info: _RunInfo, component: str) -> Job:
        """Build a NaN validation job for one AWP component.

        Args:
            info (_RunInfo): Run metadata.
            component (str): AWP component, ``x``, ``y``, or ``z``.

        Returns:
            Job: Configured AWP_NaN_Check job.
        """
        job = _job("AWP_NaN_Check", f"AWP_NaN_Check_{component}")
        strain = f"comp_{component}/output_sgt/awp-strain-{info.site}-f{component}"
        coordinates = File(f"{info.site}.cordfile")
        input_source = f"IN3D.{info.site}.{component}"
        job.add_args(strain, coordinates.lfn, input_source)
        job.add_inputs(coordinates)
        return job

    @staticmethod
    def _handoff_job(info: _RunInfo) -> Job:
        """Build the low-frequency completion handoff job.

        Args:
            info (_RunInfo): Run metadata.

        Returns:
            Job: Configured Handoff job.
        """
        return _job("Handoff", "Handoff").add_args(
            "handoff",
            f"-r {info.run_id}",
            "-s LF",
            f"-d {DEFAULT_JOB_DB_CREDENTIALS}",
        )

    @staticmethod
    def _insert_velocity_params_job(info: _RunInfo) -> Job:
        """Build the local job that inserts computed velocity parameters.

        Args:
            info (_RunInfo): Run metadata.

        Returns:
            Job: Configured InsertVelocityParams job.
        """
        job = _job("InsertVelocityParams", f"InsertVelocityParams_{info.site}")
        parameter_file = File(f"{info.site}_{info.run_id}_velocity_params.txt")
        job.add_args(
            "insert-velocity-params",
            f"-i {parameter_file.lfn}",
            f"-d {DEFAULT_JOB_DB_CREDENTIALS}",
            f"-r {info.run_id}",
        )
        job.add_inputs(parameter_file)
        return job
