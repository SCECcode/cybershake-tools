"""Reusable primitives for CyberShake workflow generators."""

from __future__ import annotations

import sysconfig
from importlib.metadata import distribution
from pathlib import Path
from typing import TYPE_CHECKING, NamedTuple

from Pegasus.api import (
    OS,
    Arch,
    File,
    Job,
    Namespace,
    ReplicaCatalog,
    SubWorkflow,
    Transformation,
    TransformationCatalog,
    TransformationSite,
    Workflow,
)

from cybershake.constants import (
    CURVE_OUTPUT_DIR,
    DB_CHECK_OUTPUT_DIR,
    DEFAULT_JOB_DB_CREDENTIALS,
    DISAGG_OUTPUT_DIR,
    LEGACY_OPENSHA_CONF_DIR,
    NAMESPACE,
    PP_STORAGE_DIR,
    SGT_STRINGS,
    UPDATE_RUN_TRANSFORMATION,
    VELOCITY_MODEL_STRINGS,
    VERSION,
)

if TYPE_CHECKING:
    from cybershake.config import Config
    from cybershake.runmanager.db import SQLAlchemyRunDB


class RunInfo(NamedTuple):
    """Validated run and site fields used by workflow generators.

    Attributes:
        run_id (int): CyberShake run ID.
        site_id (int): CyberShake site ID.
        site (str): Site short name.
        latitude (float): Site latitude in degrees.
        longitude (float): Site longitude in degrees.
        erf_id (int): Earthquake rupture forecast ID.
        rupture_variation_id (int): Rupture variation scenario ID.
        sgt_variation_id (int): SGT variation ID.
        sgt_type (str): Transformation-family name for the SGT variation.
        velocity_model_id (int): Velocity model ID.
        velocity_model (str): UCVM model-selection string.
        low_frequency (float): Low-frequency cutoff in Hz.
        max_frequency (float): Maximum simulated frequency in Hz.
        source_frequency (float): SGT source-filter frequency in Hz.
        cutoff_distance (float): Site rupture cutoff distance in km.
        target_vs30 (float | None): Target site-response Vs30 in m/s.
    """

    run_id: int
    site_id: int
    site: str
    latitude: float
    longitude: float
    erf_id: int
    rupture_variation_id: int
    sgt_variation_id: int
    sgt_type: str
    velocity_model_id: int
    velocity_model: str
    low_frequency: float
    max_frequency: float
    source_frequency: float
    cutoff_distance: float
    target_vs30: float | None


def load_run_info(database: SQLAlchemyRunDB, run_id: int) -> RunInfo:
    """Load common workflow-generator metadata for one run.

    Args:
        database (SQLAlchemyRunDB): Run database repository.
        run_id (int): CyberShake run ID.

    Returns:
        RunInfo: Validated run and site metadata.

    Raises:
        ValueError: If the run, site, region, variation, velocity model, or
            required frequency is unavailable.
    """
    run = database.get_run(run_id)
    if run is None:
        raise ValueError(f"Run {run_id} does not exist")
    site = database.get_site_by_id(run.site_id)
    if site is None:
        raise ValueError(f"Run {run_id} references missing site {run.site_id}")
    regions = database.get_site_regions(run.site_id, run.erf_id)
    if not regions:
        raise ValueError(
            f"Run {run_id} has no site region for site {run.site_id} and ERF "
            f"{run.erf_id}"
        )
    if len(regions) != 1:
        raise ValueError(
            f"Run {run_id} has {len(regions)} site regions for site "
            f"{run.site_id} and ERF {run.erf_id}"
        )
    try:
        sgt_type = SGT_STRINGS[run.sgt_variation_id]
    except KeyError as error:
        raise ValueError(
            f"Unsupported SGT variation ID {run.sgt_variation_id}"
        ) from error
    try:
        velocity_model = VELOCITY_MODEL_STRINGS[run.velocity_model_id]
    except KeyError as error:
        raise ValueError(
            f"Unsupported velocity model ID {run.velocity_model_id}"
        ) from error
    if run.low_frequency_cutoff is None or run.low_frequency_cutoff <= 0:
        raise ValueError(f"Run {run_id} has no positive low-frequency cutoff")
    if run.max_frequency is None or run.max_frequency <= 0:
        raise ValueError(f"Run {run_id} has no positive maximum frequency")
    if run.sgt_source_filter_frequency is None:
        raise ValueError(f"Run {run_id} has no SGT source-filter frequency")
    return RunInfo(
        run_id=run_id,
        site_id=run.site_id,
        site=site.cs_short_name,
        latitude=site.cs_site_lat,
        longitude=site.cs_site_lon,
        erf_id=run.erf_id,
        rupture_variation_id=run.rup_var_scenario_id,
        sgt_variation_id=run.sgt_variation_id,
        sgt_type=sgt_type,
        velocity_model_id=run.velocity_model_id,
        velocity_model=velocity_model,
        low_frequency=run.low_frequency_cutoff,
        max_frequency=run.max_frequency,
        source_frequency=run.sgt_source_filter_frequency,
        cutoff_distance=regions[0].cutoff_dist,
        target_vs30=run.target_vs30,
    )


def job(transformation: str, identifier: str, *, version: str = VERSION) -> Job:
    """Create a namespaced CyberShake job.

    Args:
        transformation (str): Transformation-catalog name.
        identifier (str): Unique workflow node ID.
        version (str, optional): Transformation version. Defaults to ``1.0``.

    Returns:
        Job: Unconfigured Pegasus job.
    """
    return Job(
        transformation,
        _id=identifier,
        namespace=NAMESPACE,
        version=version,
    )


def transformation_catalog() -> TransformationCatalog:
    """Return local and shock entries for installed transformations."""
    scripts = Path(sysconfig.get_path("scripts"))
    entry_point = next(
        entry
        for entry in distribution("cybershake-tools").entry_points
        if entry.group == "console_scripts"
        and entry.value == "cybershake.__main__:cybershake"
    )
    executable = str((scripts / entry_point.name).resolve(strict=True))
    executables = (
        ("GenSGTDax", executable),
        ("GenStochDax", executable),
        ("UpdateRun", executable),
        ("Handoff", executable),
        ("InsertVelocityParams", executable),
        ("BB_Velocity_Insert", executable),
        ("SetJobID", str((scripts / "get_condor_id.sh").resolve(strict=True))),
        ("Check_DB_Site", executable),
        ("Curve_Calc", executable),
        ("Curve_Calc_Wrapper", executable),
        ("Disaggregate", executable),
        ("Load_Amps", str((scripts / "insert_dir.sh").resolve(strict=True))),
    )
    catalog = TransformationCatalog()
    for name, pfn in executables:
        transformation = Transformation(name, namespace=NAMESPACE, version=VERSION)
        transformation.add_sites(
            TransformationSite("local", pfn, arch=Arch.X86, os_type=OS.LINUX),
            TransformationSite("shock", pfn, arch=Arch.X86, os_type=OS.LINUX),
        )
        if name == "UpdateRun":
            transformation.add_pegasus_profiles(clusters_size=1)
        catalog.add_transformations(transformation)
    return catalog


def dependency(
    workflow: Workflow,
    parent: Job | SubWorkflow,
    *children: Job | SubWorkflow,
) -> None:
    """Add one parent-to-many-children dependency.

    Args:
        workflow (Workflow): Workflow receiving the dependency.
        parent (Job | SubWorkflow): Parent node.
        *children (Job | SubWorkflow): Child nodes.
    """
    workflow.add_dependency(parent, children=list(children))


def intermediate_outputs(job_node: Job, *files: File) -> None:
    """Add files that remain in the workflow execution directory.

    Args:
        job_node (Job): Producing job.
        *files (File): Intermediate output files.
    """
    job_node.add_outputs(*files, stage_out=False, register_replica=False)


def final_outputs(job_node: Job, *files: File) -> None:
    """Add files staged to storage and registered in the replica catalog.

    Args:
        job_node (Job): Producing job.
        *files (File): Final output files.
    """
    job_node.add_outputs(*files, stage_out=True, register_replica=True)


def update_job(
    run_id: int,
    old_state: str,
    new_state: str,
    *,
    identifier: str | None = None,
    local: bool = False,
) -> Job:
    """Build a deferred run-state update job.

    Args:
        run_id (int): CyberShake run ID.
        old_state (str): Symbolic source state.
        new_state (str): Symbolic destination state.
        identifier (str | None, optional): Workflow node ID. Defaults to
            ``UpdateRun_<new_state>``.
        local (bool, optional): Select the local execution pool. Defaults to
            ``False``.

    Returns:
        Job: Configured update job.
    """
    node = job(
        UPDATE_RUN_TRANSFORMATION,
        identifier or f"UpdateRun_{new_state}",
    )
    node.add_args("run-manager", "update", str(run_id), old_state, new_state)
    node.add_pegasus_profiles(runtime=300)
    if local:
        node.add_profiles(Namespace.SELECTOR, executionPool="local")
    return node


def add_replica(workflow: Workflow, file: File, pfn: Path | str, *, site: str) -> None:
    """Register a physical file with an inline replica catalog."""
    catalog = workflow.replica_catalog
    if catalog is None:
        catalog = ReplicaCatalog()
        workflow.add_replica_catalog(catalog)
    catalog.add_replica(site, file, pfn)


def add_local_replica(workflow: Workflow, file: File, path: Path) -> None:
    """Register a generated local input with an inline replica catalog."""
    add_replica(workflow, file, path.resolve(), site="local")


def add_config_metadata(workflow: Workflow, config: Config | None) -> None:
    """Add a complete resolved configuration to workflow-level metadata.

    Pegasus metadata values are strings. Unset configuration values are stored
    as ``"None"`` so every canonical key remains present and distinguishable
    from an empty string.

    Args:
        workflow (Workflow): Main workflow receiving reproducibility metadata.
        config (Config | None): Complete resolved configuration. ``None`` is
            accepted for direct generator use without a CLI configuration.
    """
    if config is None:
        return
    metadata: dict[str, bool | float | int | str] = {
        key: "None" if value is None else value for key, value in config.items()
    }
    workflow.add_metadata(metadata)


def write_workflow(workflow: Workflow, path: Path) -> None:
    """Write a Pegasus workflow with its inline transformation catalog.

    Args:
        workflow (Workflow): Workflow to serialize.
        path (Path): Destination workflow file. Parent directories are created
            when absent.
    """
    workflow.add_transformation_catalog(transformation_catalog())
    path.parent.mkdir(parents=True, exist_ok=True)
    workflow.write(str(path))


def parse_rupture_list(path: Path | None) -> tuple[tuple[int, int], ...] | None:
    """Read an optional source/rupture CSV list.

    Args:
        path (Path | None): File containing ``source_id,rupture_id`` rows.
            Defaults to ``None``, which means no filtering.

    Returns:
        tuple[tuple[int, int], ...] | None: Selected pairs, or ``None``.

    Raises:
        ValueError: If a non-empty line is not a pair of integers.
        OSError: If the file cannot be read.
    """
    if path is None:
        return None
    pairs: list[tuple[int, int]] = []
    for line_number, raw_line in enumerate(
        path.read_text(encoding="utf-8").splitlines(), start=1
    ):
        line = raw_line.strip()
        if not line:
            continue
        pieces = [piece.strip() for piece in line.split(",")]
        if len(pieces) != 2:
            raise ValueError(f"Invalid rupture list row {line_number}: {raw_line!r}")
        try:
            pairs.append((int(pieces[0]), int(pieces[1])))
        except ValueError as error:
            raise ValueError(
                f"Invalid rupture list row {line_number}: {raw_line!r}"
            ) from error
    return tuple(pairs)


def create_db_products_workflow(
    *,
    info: RunInfo,
    server: str,
    calculate_rotd: bool,
    calculate_durations: bool,
    stochastic: bool,
    velocity_file: Path | None,
    rupture_list: Path | None,
) -> Workflow:
    """Build the shared PP/stochastic database-products workflow.

    Args:
        info (RunInfo): Run metadata used by all product jobs.
        server (str): Run database server.
        calculate_rotd (bool): Insert, validate, and plot RotD50 products.
        calculate_durations (bool): Insert and validate duration products.
        stochastic (bool): Include broadband periods and velocity-file input.
        velocity_file (Path | None): Vs30 file used by broadband curve jobs.
            Required when ``stochastic`` and curve generation are enabled.
        rupture_list (Path | None): Optional source/rupture restriction. Curve
            generation is disabled when present because the ERF is incomplete.

    Returns:
        Workflow: Configured database-products workflow.

    Raises:
        ValueError: If stochastic curve generation has no velocity file.
    """
    workflow = Workflow(f"CyberShake_{info.site}_db_products", infer_dependencies=False)
    previous_insert: Job | None = None
    if calculate_rotd:
        insert = _rotd_insert_job(info, server=server, stochastic=stochastic)
        check = _rotd_check_job(
            info,
            server=server,
            stochastic=stochastic,
            rupture_list=rupture_list,
        )
        workflow.add_jobs(insert, check)
        dependency(workflow, insert, check)
        previous_insert = insert
        if rupture_list is not None:
            rupture_file = File(rupture_list.name)
            check.add_inputs(rupture_file)
            add_local_replica(workflow, rupture_file, rupture_list)
        if rupture_list is None:
            curve = _curve_job(
                info,
                stochastic=stochastic,
                velocity_file=velocity_file,
            )
            disaggregate = _disaggregate_job(info)
            workflow.add_jobs(curve, disaggregate)
            dependency(workflow, check, curve)
            dependency(workflow, curve, disaggregate)

    if calculate_durations:
        duration = _duration_insert_job(info, server=server)
        duration_check = _duration_check_job(
            info,
            server=server,
            rupture_list=rupture_list,
        )
        workflow.add_jobs(duration, duration_check)
        if previous_insert is not None:
            dependency(workflow, previous_insert, duration)
        dependency(workflow, duration, duration_check)

    return workflow


def _database_server_name(server: str) -> str:
    """Return the database alias expected by product transformations.

    Args:
        server (str): Configured database host or alias.

    Returns:
        str: Legacy transformation server name.
    """
    if server in {"moment", "moment-carc.usc.edu"}:
        return "moment_carc"
    return server.split(".", maxsplit=1)[0]


def _rotd_insert_periods(info: RunInfo, *, stochastic: bool) -> str:
    """Return amplitude-loader periods for a run.

    Args:
        info (RunInfo): Run frequency metadata.
        stochastic (bool): Include broadband periods.

    Returns:
        str: Comma-separated period list.
    """
    periods = ["10", "7.5", "5", "4", "3"]
    if info.low_frequency >= 1.0:
        periods.extend(("2", "PGV"))
    if stochastic:
        periods.extend(("1", "0.75", "0.5", "0.4", "0.3", "0.2", "0.1", "PGA"))
        if info.max_frequency > 10.0:
            periods.extend(("0.075", "0.05"))
        if info.max_frequency > 20.0:
            periods.extend(("0.04", "0.03", "0.02", "0.01"))
    elif info.low_frequency >= 2.0:
        periods.append("1")
    return ",".join(periods)


def _product_periods(info: RunInfo, *, stochastic: bool, rotd: bool) -> str:
    """Return database-check periods for a run.

    Args:
        info (RunInfo): Run frequency metadata.
        stochastic (bool): Include broadband periods.
        rotd (bool): Use the extended RotD base periods.

    Returns:
        str: Comma-separated period list.
    """
    periods = ["10", "7.5", "5", "4", "3"] if rotd else ["10", "5", "3"]
    if stochastic:
        periods.extend(("2", "1", "0.5", "0.2", "0.1"))
    else:
        if info.low_frequency >= 1.0:
            periods.append("2")
        if info.low_frequency >= 2.0:
            periods.append("1")
    return ",".join(periods)


def _curve_periods(info: RunInfo, *, stochastic: bool) -> str:
    """Return RotD50 curve periods in the legacy plotting order.

    Args:
        info (RunInfo): Run frequency metadata.
        stochastic (bool): Include broadband periods.

    Returns:
        str: Comma-separated period list.
    """
    periods = ["3", "4", "5", "7.5", "10"]
    if stochastic:
        periods.extend(("2", "1", "0.5", "0.2", "0.1"))
    else:
        if info.low_frequency >= 1.0:
            periods.append("2")
        if info.low_frequency >= 2.0:
            periods.append("1")
    return ",".join(periods)


def _rotd_insert_job(info: RunInfo, *, server: str, stochastic: bool) -> Job:
    """Build the RotD amplitude insertion job.

    Args:
        info (RunInfo): Run metadata.
        server (str): Run database server.
        stochastic (bool): Include broadband periods.

    Returns:
        Job: Configured loader job.
    """
    node = job("Load_Amps", f"CS_Products_Load_Amps_RotD_{info.site}")
    node.add_args(
        f"-server {_database_server_name(server)}",
        "-rdf",
        "-c",
        f"-p {PP_STORAGE_DIR / info.site / str(info.run_id)}",
        f"-run {info.run_id}",
        f"-periods {_rotd_insert_periods(info, stochastic=stochastic)}",
    )
    node.add_pegasus_profiles(runtime=3600)
    node.add_profiles(Namespace.SELECTOR, executionPool="shock")
    return node


def _rotd_check_job(
    info: RunInfo,
    *,
    server: str,
    stochastic: bool,
    rupture_list: Path | None,
) -> Job:
    """Build the RotD database consistency-check job.

    Args:
        info (RunInfo): Run metadata.
        server (str): Run database server.
        stochastic (bool): Include broadband periods.
        rupture_list (Path | None): Optional restricted rupture list.

    Returns:
        Job: Configured check job.
    """
    node = job("Check_DB_Site", f"CS_Products_DB_Check_RotD_{info.site}")
    server_name = _database_server_name(server)
    node.add_args(
        "jobs",
        "check-db-site",
        "-r",
        str(info.run_id),
        "-o",
        str(DB_CHECK_OUTPUT_DIR / ("DB_Check_Out_RotD_" + info.site)),
        "-c",
        "RotD50",
        "-p",
        _product_periods(info, stochastic=stochastic, rotd=True),
        "-s",
        "moment" if server_name == "moment_carc" else server_name + ".usc.edu",
    )
    if rupture_list is not None:
        node.add_args("-rl", rupture_list.name)
    node.add_pegasus_profiles(runtime=900)
    node.add_profiles(Namespace.SELECTOR, executionPool="local")
    return node


def _erf_file(erf_id: int) -> Path:
    """Return the OpenSHA ERF configuration for a run.

    Args:
        erf_id (int): Earthquake rupture forecast ID.

    Returns:
        Path: Legacy OpenSHA configuration path.
    """
    special = {
        61: LEGACY_OPENSHA_CONF_DIR / "erf61_params.xml",
        62: LEGACY_OPENSHA_CONF_DIR / "erf62_params.xml",
        63: LEGACY_OPENSHA_CONF_DIR / "erf63_params.xml",
    }
    return special.get(erf_id, LEGACY_OPENSHA_CONF_DIR / "MeanUCERF.xml")


def _curve_job(
    info: RunInfo,
    *,
    stochastic: bool,
    velocity_file: Path | None,
) -> Job:
    """Build a RotD50 hazard-curve job.

    Args:
        info (RunInfo): Run metadata.
        stochastic (bool): Use the velocity-aware broadband wrapper.
        velocity_file (Path | None): Broadband velocity information file.

    Returns:
        Job: Configured curve-calculation job.

    Raises:
        ValueError: If broadband generation has no velocity file.
    """
    if stochastic and velocity_file is None:
        raise ValueError("A velocity file is required for stochastic curves")
    transformation = "Curve_Calc_Wrapper" if stochastic else "Curve_Calc"
    suffix = "_Wrapper" if stochastic else ""
    node = job(
        transformation,
        f"CS_Products_Curve_Calc_RotD50{suffix}_{info.site}",
    )
    node.add_args("jobs", "curve-plot")
    if velocity_file is not None and stochastic:
        velocity = File(velocity_file.name)
        node.add_args("--velocity-file", velocity.lfn)
        node.add_inputs(velocity)
    attenuation_files = ",".join(
        str(LEGACY_OPENSHA_CONF_DIR / name)
        for name in ("ask2014.xml", "bssa2014.xml", "cb2014.xml", "cy2014.xml")
    )
    node.add_args(
        "--site",
        info.site,
        "--run-id",
        str(info.run_id),
        "--erf-file",
        str(_erf_file(info.erf_id)),
        "--atten-rel-file",
        attenuation_files,
        "--period",
        _curve_periods(info, stochastic=stochastic),
        "--output-dir",
        str(CURVE_OUTPUT_DIR / info.site),
        "--type",
        "pdf,png",
        "--force-add",
        "-cmp",
        "RotD50",
        "--password-file",
        str(DEFAULT_JOB_DB_CREDENTIALS),
        "--plot-chars-file",
        str(LEGACY_OPENSHA_CONF_DIR / "robPlot.xml"),
    )
    node.add_pegasus_profiles(runtime=900)
    node.add_profiles(Namespace.SELECTOR, executionPool="local")
    return node


def _disaggregate_job(info: RunInfo) -> Job:
    """Build the standard RotD50 disaggregation job.

    Args:
        info (RunInfo): Run metadata.

    Returns:
        Job: Configured disaggregation job.
    """
    node = job("Disaggregate", f"Disaggregate_{info.site}")
    node.add_args(
        "jobs",
        "disagg-plot",
        "--run-id",
        str(info.run_id),
        "--period",
        "3",
        "--probs",
        "4.0e-4",
        "--output-dir",
        str(DISAGG_OUTPUT_DIR),
        "--type",
        "pdf,png,txt",
        "--component",
        "RotD50",
        "--erf-file",
        str(_erf_file(info.erf_id)),
    )
    node.add_pegasus_profiles(runtime=300)
    return node


def _duration_insert_job(info: RunInfo, *, server: str) -> Job:
    """Build the duration insertion job.

    Args:
        info (RunInfo): Run metadata.
        server (str): Run database server.

    Returns:
        Job: Configured duration loader.
    """
    node = job("Load_Amps", f"CS_Products_Load_Durations_{info.site}")
    node.add_args(
        f"-server {_database_server_name(server)}",
        "-u",
        f"-p {PP_STORAGE_DIR / info.site / str(info.run_id)}",
        f"-run {info.run_id}",
        "-periods 2,3,5,10",
    )
    node.add_pegasus_profiles(runtime=3600)
    node.add_profiles(Namespace.SELECTOR, executionPool="shock")
    return node


def _duration_check_job(
    info: RunInfo,
    *,
    server: str,
    rupture_list: Path | None,
) -> Job:
    """Build the duration consistency-check job.

    Args:
        info (RunInfo): Run metadata.
        server (str): Run database server.
        rupture_list (Path | None): Optional restricted rupture list.

    Returns:
        Job: Configured duration check.
    """
    node = job("Check_DB_Site", f"CS_Products_DB_Check_Duration_{info.site}")
    server_name = _database_server_name(server)
    node.add_args(
        "jobs",
        "check-db-site",
        "-r",
        str(info.run_id),
        "-o",
        str(DB_CHECK_OUTPUT_DIR / ("DB_Check_Out_Duration_" + info.site)),
        "-c",
        "duration",
        "-t",
        "176,177,180,181",
        "-s",
        "moment" if server_name == "moment_carc" else server_name + ".usc.edu",
    )
    if rupture_list is not None:
        node.add_args("-rl", rupture_list.name)
    node.add_pegasus_profiles(runtime=900)
    node.add_profiles(Namespace.SELECTOR, executionPool="local")
    return node
