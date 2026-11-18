"""CyberShake command line interface."""

from __future__ import annotations

import logging
import shutil
import signal
from pathlib import Path

import yaml
from rich.logging import RichHandler

import rich_click as click

from dotenv import load_dotenv

from sqlalchemy.exc import SQLAlchemyError

from cybershake import check_db as db_check
from cybershake import pegasus, plot_jobs, runmanager, utils, version
from cybershake import properties as pegasus_properties
from cybershake.config import Config, environment_config
from cybershake.constants import (
    CRON_RUN_CHECK_MODES,
    CRON_RUN_CHECK_TIMEOUT_SECONDS,
    DEFAULT_DB_SERVER,
    DEFAULT_FULL_PROPERTIES,
    DEFAULT_JOB_DB_CREDENTIALS,
    DEFAULT_PP_PROPERTIES,
    DEFAULT_SGT_PROPERTIES,
    LEGACY_CRON_LOCK_FILE,
    LEGACY_X509_USER_PROXY,
    PP_STARTED_STATE,
    PP_VELOCITY_MODELS,
    RUN_STATES,
    RUN_TABLE_FILENAME,
    SGT_GENERATED_STATE,
    SGT_STARTED_STATE,
    START_STATE,
    VALID_FIELDS,
    VELOCITY_MODELS,
)
from cybershake.exc import (
    CheckDBError,
    CronRunCheckTimeoutError,
    FullError,
    PlottingError,
    PpError,
    RunManagerError,
)
from cybershake.options import (
    EpilogGroup,
    FullCommand,
    PPCommand,
    common_options,
    config_option,
    envvar_option,
    full_spacing_options,
    handoff_option,
    low_frequency_options,
    plan_options,
    pp_high_frequency_option,
    pp_options,
    pp_stoch_options,
    run_options,
    sgt_options,
    sgt_pp_options,
    sgt_stoch_options,
    sgt_sub_workflow_options,
    stoch_options,
    stoch_sub_workflow_options,
    workflow_options,
)
from cybershake.runmanager import jobs as runtime_jobs
from cybershake.runmanager.db import SQLAlchemyRunDB, connect
from cybershake.workflow import full as full_workflow
from cybershake.workflow import pp as pp_workflow
from cybershake.workflow import sgt as sgt_workflow
from cybershake.workflow import stoch as stoch_workflow

load_dotenv()


def _success(message: str) -> None:
    """Write a successful command result with a green tick."""
    click.secho("✓", fg="green", nl=False)
    click.echo(f" {message}")


def _planned_pp_host(workflow_file: Path, default_host: str) -> str:
    """Return the PP site selected when the workflow was generated, if any."""
    workflow = yaml.safe_load(workflow_file.read_text(encoding="utf-8"))
    pp_site = (workflow.get("metadata") or {}).get("pp_site")
    return default_host if pp_site in (None, "None") else str(pp_site)


@click.group(cls=EpilogGroup)
@envvar_option(
    "-v",
    "--verbose",
    count=True,
    default=0,
    show_default=True,
    help="Increase logging verbosity; repeat for debug logging.",
)
@click.pass_context
def cybershake(ctx: click.Context, verbose: int) -> None:
    """CyberShake: SCEC workflow generator.

    \f
    Args:
        ctx (click.Context): Active context used to identify the subcommand.
    """  # noqa: D301
    if ctx.invoked_subcommand not in ["version"]:
        click.secho(
            rf"""   ____      _                   _           _
  / ___|   _| |__   ___ _ __ ___| |__   __ _| | _____
 | |  | | | | '_ \ / _ \ '__/ __| '_ \ / _` | |/ / _ \
 | |__| |_| | |_) |  __/ |  \__ \ | | | (_| |   <  __/
  \____\__, |_.__/ \___|_|  |___/_| |_|\__,_|_|\_\___|
       |___/                                           v{version.__version__}""",
            fg="magenta",
        )
    log_level = (logging.WARNING, logging.INFO, logging.DEBUG)[min(verbose, 2)]
    logging.basicConfig(level=log_level, handlers=[RichHandler(rich_tracebacks=True)])
    logging.getLogger().setLevel(log_level)
    if verbose < 2:
        logging.getLogger("Pegasus.api.workflow").propagate = False


@cybershake.command(name="sgt")
@common_options()
@low_frequency_options()
@workflow_options
@sgt_pp_options
@sgt_stoch_options
@sgt_options
@config_option
def sgt(
    *,
    velocity_model: str,
    erf_id: int,
    rupture_variation_id: int,
    sgt_variation_id: int,
    frequency: float,
    source_frequency: float | None,
    site: str,
    separate_md5: bool,
    handoff: bool,
    sgt_site: str | None,
    server: str,
    spacing: float | None,
    min_vs: float | None,
    no_smoothing: bool,
    depth: float | None,
    h_fraction: float | None,
    rotation: float | None,
    ely_taper: str | None,
    taper_depth: float | None,
    taper_models: str | None,
    z_comp: bool,
    local_vparam_insert: bool,
    config: Config,
) -> None:
    """Create an SGT workflow.

    \f
    Args:
        velocity_model (str): Velocity-model alias.
        erf_id (int): Earthquake rupture forecast ID.
        rupture_variation_id (int): Rupture variation scenario ID.
        sgt_variation_id (int): SGT variation ID.
        frequency (float): Simulation frequency in Hz.
        source_frequency (float | None, optional): SGT source-filter frequency.
            Defaults to ``None``, which uses ``frequency``.
        site (str): CyberShake site short name.
        separate_md5 (bool, optional): Run checksums separately from post-AWP
            jobs. Defaults to ``False``.
        handoff (bool, optional): Add the completed stage to the pending handoff
            file. Defaults to ``False``.
        sgt_site (str | None, optional): SGT execution site override. Defaults to
            ``None``.
        server (str, optional): Run database server. Defaults to
            ``moment`` (``DEFAULT_DB_SERVER``).
        spacing (float | None, optional): Grid spacing override in km. Defaults to
            ``None``.
        min_vs (float | None, optional): Minimum shear-wave velocity override in
            m/s. Defaults to ``None``.
        no_smoothing (bool, optional): Disable velocity-model smoothing. Defaults
            to ``False``.
        depth (float | None, optional): Simulation depth override in km. Defaults
            to ``None``.
        h_fraction (float | None, optional): Grid-point depth fraction for surface
            queries. Defaults to ``None``.
        rotation (float | None, optional): Simulation-volume rotation in degrees.
            Defaults to ``None``.
        ely_taper (str | None, optional): Ely taper mode. Defaults to ``None``.
        taper_depth (float | None, optional): Ely taper depth in metres. Defaults
            to ``None``.
        taper_models (str | None, optional): Comma-separated velocity models to
            taper. Defaults to ``None``.
        z_comp (bool, optional): Calculate the vertical component. Defaults to
            ``False``.
        local_vparam_insert (bool, optional): Insert velocity parameters in a
            local job. Defaults to ``False``.
        config (Config): Complete resolved CyberShake configuration.
    """  # noqa: D301
    # Click has already included these options in config.
    del (
        separate_md5,
        handoff,
        sgt_site,
        spacing,
        min_vs,
        no_smoothing,
        depth,
        h_fraction,
        rotation,
        ely_taper,
        taper_depth,
        taper_models,
        z_comp,
        local_vparam_insert,
    )
    resolved_source_frequency = (
        frequency if source_frequency is None else source_frequency
    )
    velocity_model_id = VELOCITY_MODELS[velocity_model]
    run_directory = config.output_directory / f"{site}_SGT_dax"
    run_directory.mkdir(parents=True, exist_ok=True)
    (run_directory / "sites.list").write_text(f"{site}\n", encoding="utf-8")
    run_file = run_directory / RUN_TABLE_FILENAME

    run_id = (
        utils.find_run(
            run_file,
            erf_id=erf_id,
            sgt_variation_id=sgt_variation_id,
            rupture_variation_id=rupture_variation_id,
            velocity_model_id=velocity_model_id,
            frequency=frequency,
            source_frequency=resolved_source_frequency,
            stage="SGT_PLAN",
            server=server,
            config=config,
        )
        if run_file.exists()
        else None
    )
    if run_id is None:
        run_id = utils.create_sgt_run(
            run_file,
            site=site,
            erf_id=erf_id,
            sgt_variation_id=sgt_variation_id,
            velocity_model_id=velocity_model_id,
            rupture_variation_id=rupture_variation_id,
            frequency=frequency,
            source_frequency=resolved_source_frequency,
            server=server,
            config=config,
        )

    output_directory = run_directory / f"run_{run_id}"
    output_directory.mkdir(parents=True, exist_ok=True)
    workflow_file = output_directory / f"CyberShake_SGT_{site}.dax"
    sgt_generator = sgt_workflow.SGT(config=config)
    sgt_generator.create(
        workflow_file=workflow_file,
        run_directory=output_directory,
        run_ids=(run_id,),
    )

    try:
        runmanager.edit(
            run_id=run_id,
            fields={
                "Status": START_STATE,
                "Comment": "SGT DAX created",
                "Job_ID": None,
                "Submit_Dir": None,
            },
            server=server,
            config=config,
        )
    except RunManagerError:
        click.echo(
            f"Unable to update Status, Comment, Job_ID, Submit_Dir for run {run_id}",
            err=True,
        )
    _success(f"Created SGT workflow: run={run_id} file={workflow_file.resolve()}")


@cybershake.command(name="sgt-plan")
@plan_options(DEFAULT_SGT_PROPERTIES)
def sgt_plan(*, site: str, run_id: int, remote_site: str, properties: Path) -> None:
    """Plan an SGT workflow.

    \f
    Args:
        site (str): CyberShake site short name.
        run_id (int): Run whose SGT workflow should be planned.
        remote_site (str): Pegasus execution and output site.
        properties (Path, optional): Pegasus properties file. Defaults to
            ``conf/properties.sgt`` (``DEFAULT_SGT_PROPERTIES``).
    """  # noqa: D301
    config = environment_config()
    root_directory = config.output_directory / f"{site}_SGT_dax"
    run_file = root_directory / RUN_TABLE_FILENAME
    run_site = utils.require_run_site(run_file, run_id)
    runmanager.valid(
        run_id=run_id,
        site=run_site,
        stage="SGT_PLAN",
        server=DEFAULT_DB_SERVER,
        config=config,
    )

    workflow_name = f"CyberShake_SGT_{site}.dax"
    utils.remove_plan_logs(root_directory, f"log-plan-{workflow_name}-*")
    pegasus_properties.dump(
        pegasus_properties.load(properties),
        root_directory / "properties.sgt.onesite",
    )
    run_directory = root_directory / f"run_{run_id}"
    execution_directory = pegasus.plan(
        workflow_file=run_directory / workflow_name,
        working_directory=run_directory,
        properties_file=properties,
        plan_log=run_directory / f"log-plan-{workflow_name}-{remote_site}",
        execution_sites=(remote_site, "shock"),
        output_site=remote_site,
        submit_directory=Path("dags"),
        rescue=100,
        cleanup="none",
        force=True,
        cluster=None,
        output_directory=Path("data") / "SgtFiles" / site,
        verbosity=0,
        environment=None,
    )
    runmanager.edit(
        run_id=run_id,
        fields={
            "Status": START_STATE,
            "SGT_Host": remote_site.split("_", maxsplit=1)[0],
            "Comment": "Planned SGT DAX, assigned sgt host",
            "Job_ID": None,
        },
        server=DEFAULT_DB_SERVER,
        config=config,
    )
    _success(
        f"Planned SGT workflow: run={run_id} site={remote_site} "
        f"submit_dir={execution_directory.resolve()}"
    )


@cybershake.command(name="sgt-run")
@run_options
def sgt_run(*, site: str, run_id: int, restart: bool, notify_user: str | None) -> None:
    """Submit a planned SGT workflow.

    \f
    Args:
        site (str): CyberShake site short name.
        run_id (int): Run whose planned workflow should be submitted.
        restart (bool, optional): Mark the submission as a restart. Defaults to
            ``False``.
        notify_user (str | None, optional): User or address recorded for workflow
            notifications. Defaults to ``None``.
    """  # noqa: D301
    config = environment_config()
    root_directory = config.output_directory / f"{site}_SGT_dax"
    run_file = root_directory / RUN_TABLE_FILENAME
    run_site = utils.require_run_site(run_file, run_id)
    runmanager.valid(
        run_id=run_id,
        site=run_site,
        stage="SGT_PLAN",
        server=DEFAULT_DB_SERVER,
        config=config,
    )

    run_directory = root_directory / f"run_{run_id}"
    cluster_id, execution_directory = utils.submit_workflow(
        run_directory,
        plan_pattern=f"log-plan-CyberShake_SGT_{site}*",
        run_log_name=f"log-run-CyberShake_SGT_{site}",
    )
    job_id = f"{utils.submission_host()}:{cluster_id}"
    runmanager.edit(
        run_id=run_id,
        fields={
            "Status": SGT_STARTED_STATE if restart else START_STATE,
            "Comment": "SGT workflow submitted",
            "Job_ID": job_id,
            "Submit_Dir": str(execution_directory),
            "Notify_User": notify_user,
        },
        server=DEFAULT_DB_SERVER,
        config=config,
    )
    _success(
        f"Submitted SGT workflow: run={run_id} job={job_id} "
        f"submit_dir={execution_directory.resolve()}"
    )


@cybershake.command(name="sub-sgt-wf")
@envvar_option("-r", "--run-id", type=int, required=True, help="Run ID.")
@envvar_option(
    "-gf",
    "--grid-output-file",
    type=click.Path(path_type=Path, file_okay=True, dir_okay=False),
    required=True,
    help="Grid output file produced by PreCVM.",
)
@envvar_option(
    "-o",
    "--workflow-file",
    type=click.Path(path_type=Path, file_okay=True, dir_okay=False),
    required=True,
    help="Output workflow file.",
)
@workflow_options
@sgt_pp_options
@sgt_stoch_options
@sgt_sub_workflow_options
@config_option
def sub_sgt_wf(
    *,
    run_id: int,
    grid_output_file: Path,
    workflow_file: Path,
    server: str,
    z_comp: bool,
    spacing: float | None,
    handoff: bool,
    h_fraction: float | None,
    separate_md5: bool,
    min_vs: float | None,
    no_smoothing: bool,
    ely_taper: str | None,
    taper_depth: float | None,
    taper_models: str | None,
    local_vparam_insert: bool,
    config: Config,
) -> None:
    """Generate an AWP SGT sub-workflow.

    \f
    Args:
        run_id (int): Run represented by the sub-workflow.
        grid_output_file (Path): Grid description produced by PreCVM.
        workflow_file (Path): Destination workflow file.
        server (str, optional): Run database server. Defaults to
            ``moment`` (``DEFAULT_DB_SERVER``).
        z_comp (bool, optional): Calculate the vertical component. Defaults to
            ``False``.
        spacing (float | None, optional): Grid spacing override in km. Defaults to
            ``None``.
        handoff (bool, optional): Add the completed stage to the pending handoff
            file. Defaults to ``False``.
        h_fraction (float | None, optional): Grid-point depth fraction for surface
            queries. Defaults to ``None``.
        separate_md5 (bool, optional): Run checksums separately from post-AWP
            jobs. Defaults to ``False``.
        min_vs (float | None, optional): Minimum shear-wave velocity override in
            m/s. Defaults to ``None``.
        no_smoothing (bool, optional): Disable velocity-model smoothing. Defaults
            to ``False``.
        ely_taper (str | None, optional): Ely taper mode. Defaults to ``None``.
        taper_depth (float | None, optional): Ely taper depth in metres. Defaults
            to ``None``.
        taper_models (str | None, optional): Comma-separated velocity models to
            taper. Defaults to ``None``.
        local_vparam_insert (bool, optional): Insert velocity parameters in a
            local job. Defaults to ``False``.
        config (Config): Complete resolved CyberShake configuration.
    """  # noqa: D301
    # Click has already included these options in config.
    del (
        server,
        z_comp,
        spacing,
        handoff,
        h_fraction,
        separate_md5,
        min_vs,
        no_smoothing,
        ely_taper,
        taper_depth,
        taper_models,
        local_vparam_insert,
    )
    sgt_generator = sgt_workflow.SGT(config=config)
    sgt_generator.create_awp(
        run_id=run_id,
        grid_output_file=grid_output_file,
        workflow_file=workflow_file,
    )
    _success(f"Created SGT sub-workflow: run={run_id} file={workflow_file.resolve()}")


@cybershake.command(cls=PPCommand, name="pp")
@common_options()
@low_frequency_options(PP_VELOCITY_MODELS)
@workflow_options
@sgt_pp_options
@pp_stoch_options
@pp_high_frequency_option("-hf", "--high-frequency")
@pp_options
@config_option
def pp(
    *,
    velocity_model: str,
    erf_id: int,
    rupture_variation_id: int,
    sgt_variation_id: int,
    frequency: float,
    source_frequency: float | None,
    site: str,
    no_insert: bool,
    high_frequency: float | None,
    no_mpi_cluster: bool,
    dir_hierarchy: bool,
    rotd: bool,
    skip_md5: bool,
    nonblocking_md5: bool,
    debug: bool,
    pp_site: str | None,
    spacing: float | None,
    server: str,
    duration: bool,
    seismogram_length: float | None,
    db_rv_seed: bool,
    z_comp: bool,
    handoff: bool,
    period_duration: bool,
    no_vertical_response: bool,
    rupture_list: Path | None,
    config: Config,
) -> None:
    """Create a PP workflow.

    \f
    Args:
        velocity_model (str): Velocity-model alias.
        erf_id (int): Earthquake rupture forecast ID.
        rupture_variation_id (int): Rupture variation scenario ID.
        sgt_variation_id (int): SGT variation ID.
        frequency (float): Simulation frequency in Hz.
        source_frequency (float | None, optional): SGT source-filter frequency.
            Defaults to ``None``, which uses ``frequency``.
        site (str): CyberShake site short name.
        no_insert (bool, optional): Do not insert ruptures into the database.
            Defaults to ``False``.
        high_frequency (float | None, optional): High-frequency processing cutoff.
            Defaults to ``None`` when absent or ``1.0`` when specified without a
            value.
        no_mpi_cluster (bool, optional): Disable ``pegasus-mpi-cluster``. Defaults
            to ``False``.
        dir_hierarchy (bool, optional): Use a compute-resource directory
            hierarchy. Defaults to ``False``.
        rotd (bool, optional): Calculate and insert RotD metrics. Defaults to
            ``True``.
        skip_md5 (bool, optional): Skip MD5 checksum jobs. Defaults to ``False``.
        nonblocking_md5 (bool, optional): Move checksums out of the critical path.
            Defaults to ``False``.
        debug (bool, optional): Enable generator debugging. Defaults to ``False``.
        pp_site (str | None, optional): Post-processing execution site override.
            Defaults to ``None``.
        spacing (float | None, optional): Grid spacing override in km. Defaults to
            ``None``.
        server (str, optional): Run database server. Defaults to
            ``moment`` (``DEFAULT_DB_SERVER``).
        duration (bool, optional): Calculate and insert duration metrics. Defaults
            to ``True``.
        seismogram_length (float | None, optional): Output seismogram length in
            seconds. Defaults to ``None``.
        db_rv_seed (bool, optional): Read rupture fractions and seeds from the
            database. Defaults to ``True``.
        z_comp (bool, optional): Calculate the vertical component. Defaults to
            ``False``.
        handoff (bool, optional): Add the completed stage to the pending handoff
            file. Defaults to ``False``.
        period_duration (bool, optional): Calculate period-dependent durations.
            Defaults to ``False``.
        no_vertical_response (bool, optional): Skip vertical response spectra.
            Defaults to ``False``.
        rupture_list (Path | None, optional): CSV file restricting source and
            rupture IDs. Defaults to ``None``.
        config (Config): Complete resolved CyberShake configuration.
    """  # noqa: D301
    # Click has already included these options in config.
    del (
        no_insert,
        high_frequency,
        no_mpi_cluster,
        dir_hierarchy,
        rotd,
        skip_md5,
        nonblocking_md5,
        debug,
        pp_site,
        spacing,
        duration,
        seismogram_length,
        db_rv_seed,
        z_comp,
        handoff,
        period_duration,
        no_vertical_response,
        rupture_list,
    )
    resolved_source_frequency = (
        frequency if source_frequency is None else source_frequency
    )
    velocity_model_id = VELOCITY_MODELS[velocity_model]
    run_directory = config.output_directory / f"{site}_PP_dax"
    run_directory.mkdir(parents=True, exist_ok=True)
    run_file = run_directory / RUN_TABLE_FILENAME

    run_id = (
        utils.find_run(
            run_file,
            erf_id=erf_id,
            sgt_variation_id=sgt_variation_id,
            rupture_variation_id=rupture_variation_id,
            velocity_model_id=velocity_model_id,
            frequency=frequency,
            source_frequency=resolved_source_frequency,
            stage="PP_PLAN",
            server=server,
            config=config,
        )
        if run_file.exists()
        else None
    )
    if run_id is None:
        run_id = utils.find_pp_run(
            run_file,
            site=site,
            erf_id=erf_id,
            sgt_variation_id=sgt_variation_id,
            rupture_variation_id=rupture_variation_id,
            velocity_model_id=velocity_model_id,
            frequency=frequency,
            source_frequency=resolved_source_frequency,
            server=server,
            config=config,
        )

    output_directory = run_directory / f"run_{run_id}"
    output_directory.mkdir(parents=True, exist_ok=True)
    pp_generator = pp_workflow.PP(config=config)
    pp_generator.create(run_id=run_id, run_directory=output_directory)

    try:
        runmanager.edit(
            run_id=run_id,
            fields={"Comment": "PP DAX created"},
            server=server,
            config=config,
        )
    except RunManagerError as error:
        msg = f"Unable to update comment for run {run_id}"
        raise PpError(msg) from error
    workflow_file = output_directory / f"CyberShake_{site}.dax"
    _success(f"Created PP workflow: run={run_id} file={workflow_file.resolve()}")


@cybershake.command(name="pp-plan")
@plan_options(DEFAULT_PP_PROPERTIES)
def pp_plan(*, site: str, run_id: int, remote_site: str, properties: Path) -> None:
    """Plan a post-processing workflow.

    \f
    Args:
        site (str): CyberShake site short name.
        run_id (int): Run whose post-processing workflow should be planned.
        remote_site (str): Pegasus execution site.
        properties (Path, optional): Pegasus properties file. Defaults to
            ``conf/properties.mpi-cluster.pp`` (``DEFAULT_PP_PROPERTIES``).
    """  # noqa: D301
    config = environment_config()
    root_directory = config.output_directory / f"{site}_PP_dax"
    run_file = root_directory / RUN_TABLE_FILENAME
    run_site = utils.require_run_site(run_file, run_id)
    runmanager.valid(
        run_id=run_id,
        site=run_site,
        stage="PP_PLAN",
        server=DEFAULT_DB_SERVER,
        config=config,
    )

    workflow_name = f"CyberShake_{site}.dax"
    run_directory = root_directory / f"run_{run_id}"
    pp_host = _planned_pp_host(run_directory / workflow_name, remote_site)
    utils.remove_plan_logs(run_directory, f"log-plan-{workflow_name}-*")
    plan_log = run_directory / f"log-plan-{workflow_name}-{remote_site}"
    execution_directory = pegasus.plan(
        workflow_file=run_directory / workflow_name,
        working_directory=run_directory,
        properties_file=properties,
        plan_log=plan_log,
        execution_sites=(remote_site, "shock"),
        output_site="shock",
        submit_directory=Path("dags"),
        rescue=100,
        cleanup="none",
        force=True,
        cluster="label",
        output_directory=None,
        verbosity=2,
        environment={"X509_USER_PROXY": str(LEGACY_X509_USER_PROXY)},
    )
    utils.prepare_post_plan_files(
        execution_directory=execution_directory,
        plan_log=plan_log,
        remote_site=remote_site,
        run_directory=run_directory,
        run_site=run_site,
        run_id=run_id,
    )
    runmanager.edit(
        run_id=run_id,
        fields={
            "Status": SGT_GENERATED_STATE,
            "PP_Host": pp_host,
            "Comment": "Planned, assigned pp host",
            "Job_ID": None,
        },
        server=DEFAULT_DB_SERVER,
        config=config,
    )
    _success(
        f"Planned PP workflow: run={run_id} site={remote_site} "
        f"submit_dir={execution_directory.resolve()}"
    )


@cybershake.command(name="pp-run")
@run_options
def pp_run(*, site: str, run_id: int, restart: bool, notify_user: str | None) -> None:
    """Submit a planned post-processing workflow.

    \f
    Args:
        site (str): CyberShake site short name.
        run_id (int): Run whose planned workflow should be submitted.
        restart (bool, optional): Mark the submission as a restart. Defaults to
            ``False``.
        notify_user (str | None, optional): User or address recorded for workflow
            notifications. Defaults to ``None``.
    """  # noqa: D301
    config = environment_config()
    root_directory = config.output_directory / f"{site}_PP_dax"
    run_file = root_directory / RUN_TABLE_FILENAME
    run_site = utils.require_run_site(run_file, run_id)
    runmanager.valid(
        run_id=run_id,
        site=run_site,
        stage="PP_PLAN",
        server=DEFAULT_DB_SERVER,
        config=config,
    )

    run_directory = root_directory / f"run_{run_id}"
    cluster_id, execution_directory = utils.submit_workflow(
        run_directory,
        plan_pattern=f"log-plan-CyberShake_{site}.dax-*",
        run_log_name=f"log-run-CyberShake_PP_{site}.dax",
    )
    job_id = f"{utils.submission_host()}:{cluster_id}"
    utils.append_condor_config(
        execution_directory,
        include_shock_default=True,
        allow_condor_lookup=False,
    )
    try:
        runmanager.edit(
            run_id=run_id,
            fields={
                "Status": PP_STARTED_STATE if restart else SGT_GENERATED_STATE,
                "Comment": "PP workflow submitted",
                "Job_ID": job_id,
                "Submit_Dir": str(execution_directory),
                "Notify_User": notify_user,
            },
            server=DEFAULT_DB_SERVER,
            config=config,
        )
    except RunManagerError:
        click.echo(
            "Unable to update Status, Comment, Job_ID, Submit_Dir, "
            f"Notify_User for run {run_id}",
            err=True,
        )
    _success(
        f"Submitted PP workflow: run={run_id} job={job_id} "
        f"submit_dir={execution_directory.resolve()}"
    )


@cybershake.command(name="stoch")
@common_options(merge_frequency=True)
@envvar_option(
    "-l",
    "--low-frequency-run-id",
    "low_frequency_run_id",
    type=int,
    required=True,
    help="Low-frequency run ID to combine.",
)
@workflow_options
@sgt_stoch_options
@pp_stoch_options
@stoch_options()
@config_option
def stoch(
    *,
    low_frequency_run_id: int,
    erf_id: int,
    rupture_variation_id: int,
    merge_frequency: float,
    site: str,
    generator_merge_frequency: float | None,
    stoch_frequency: float,
    no_rotd: bool,
    no_site_response: bool,
    no_low_site_response: bool,
    no_duration: bool,
    force_vs30: float | None,
    debug: bool,
    server: str,
    h_fraction: float | None,
    db_rv_seed: bool,
    hf_vel_model: str | None,
    period_duration: bool,
    z_comp: bool,
    no_vertical_response: bool,
    rupture_list: Path | None,
    config: Config,
) -> None:
    """Create a stochastic workflow.

    \f
    Args:
        low_frequency_run_id (int): Run providing low-frequency seismograms.
        erf_id (int): Earthquake rupture forecast ID.
        rupture_variation_id (int): Rupture variation scenario ID.
        merge_frequency (float): Frequency at which low- and high-frequency data
            merge.
        site (str): CyberShake site short name.
        generator_merge_frequency (float | None, optional): Generator
            merge-frequency override in Hz. Defaults to ``None``.
        stoch_frequency (float, optional): Maximum stochastic frequency in Hz.
            Defaults to ``10.0`` (``DEFAULT_STOCH_FREQUENCY``).
        no_rotd (bool, optional): Omit RotD calculations. Defaults to ``False``.
        no_site_response (bool, optional): Omit site-response calculations.
            Defaults to ``False``.
        no_low_site_response (bool, optional): Omit low-frequency site response.
            Defaults to ``False``.
        no_duration (bool, optional): Omit duration calculations. Defaults to
            ``False``.
        force_vs30 (float | None, optional): Forced Vs30 value in m/s. Defaults to
            ``None``.
        debug (bool, optional): Enable generator debugging. Defaults to ``False``.
        server (str, optional): Run database server. Defaults to
            ``moment`` (``DEFAULT_DB_SERVER``).
        h_fraction (float | None, optional): Grid-point depth fraction for surface
            queries. Defaults to ``None``.
        db_rv_seed (bool, optional): Read rupture fractions and seeds from the
            database. Defaults to ``True``.
        hf_vel_model (str | None, optional): One-dimensional high-frequency
            velocity model. Defaults to ``None``.
        period_duration (bool, optional): Calculate period-dependent durations.
            Defaults to ``False``.
        z_comp (bool, optional): Calculate the vertical component. Defaults to
            ``False``.
        no_vertical_response (bool, optional): Skip vertical response spectra.
            Defaults to ``False``.
        rupture_list (Path | None, optional): CSV file restricting source and
            rupture IDs. Defaults to ``None``.
        config (Config): Complete resolved CyberShake configuration.
    """  # noqa: D301
    # Click has already included these options in config.
    del (
        generator_merge_frequency,
        no_rotd,
        no_site_response,
        no_low_site_response,
        no_duration,
        force_vs30,
        debug,
        h_fraction,
        db_rv_seed,
        hf_vel_model,
        period_duration,
        z_comp,
        no_vertical_response,
        rupture_list,
    )
    run_directory = config.output_directory / f"{site}_Stoch_dax"
    run_directory.mkdir(parents=True, exist_ok=True)
    run_file = run_directory / RUN_TABLE_FILENAME

    if run_file.exists():
        run_id = utils.find_existing_stoch_run(
            run_file,
            low_frequency_run_id=low_frequency_run_id,
            erf_id=erf_id,
            rupture_variation_id=rupture_variation_id,
            merge_frequency=merge_frequency,
            stoch_frequency=stoch_frequency,
            server=server,
            config=config,
        )
        if run_id is None:
            run_id = utils.create_stoch_run(
                run_file,
                site=site,
                low_frequency_run_id=low_frequency_run_id,
                erf_id=erf_id,
                rupture_variation_id=rupture_variation_id,
                merge_frequency=merge_frequency,
                stoch_frequency=stoch_frequency,
                server=server,
                config=config,
            )
    else:
        run_id = utils.find_initial_stoch_run(
            run_file,
            site=site,
            low_frequency_run_id=low_frequency_run_id,
            erf_id=erf_id,
            rupture_variation_id=rupture_variation_id,
            merge_frequency=merge_frequency,
            stoch_frequency=stoch_frequency,
            server=server,
            config=config,
        )

    output_directory = run_directory / f"run_{run_id}"
    output_directory.mkdir(parents=True, exist_ok=True)
    stoch_generator = stoch_workflow.Stoch(config=config)
    stoch_generator.create(
        run_id=run_id,
        run_directory=output_directory,
        low_frequency_run_id=low_frequency_run_id,
    )
    workflow_file = output_directory / f"CyberShake_Stoch_{site}_top.dax"
    _success(
        f"Created stochastic workflow: run={run_id} "
        f"low_frequency_run={low_frequency_run_id} file={workflow_file.resolve()}"
    )


@cybershake.command(name="stoch-plan")
@plan_options(DEFAULT_FULL_PROPERTIES)
def stoch_plan(*, site: str, run_id: int, remote_site: str, properties: Path) -> None:
    """Plan a stochastic workflow.

    \f
    Args:
        site (str): CyberShake site short name.
        run_id (int): Run whose stochastic workflow should be planned.
        remote_site (str): Pegasus execution site.
        properties (Path, optional): Pegasus properties file. Defaults to
            ``conf/properties.full`` (``DEFAULT_FULL_PROPERTIES``).
    """  # noqa: D301
    config = environment_config()
    root_directory = config.output_directory / f"{site}_Stoch_dax"
    run_file = root_directory / RUN_TABLE_FILENAME
    run_site = utils.require_run_site(run_file, run_id)
    runmanager.valid(
        run_id=run_id,
        site=run_site,
        stage="PP_PLAN",
        server=DEFAULT_DB_SERVER,
        config=config,
    )

    workflow_name = f"CyberShake_Stoch_{site}_top.dax"
    run_directory = root_directory / f"run_{run_id}"
    utils.remove_plan_logs(run_directory, f"log-plan-{workflow_name}-*")
    plan_log = run_directory / f"log-plan-{workflow_name}-{remote_site}"
    execution_directory = pegasus.plan(
        workflow_file=run_directory / workflow_name,
        working_directory=run_directory,
        properties_file=properties,
        plan_log=plan_log,
        execution_sites=(remote_site, "shock"),
        output_site="shock",
        submit_directory=Path("dags"),
        rescue=100,
        cleanup="none",
        force=True,
        cluster="label",
        output_directory=None,
        verbosity=0,
        environment={"X509_USER_PROXY": str(LEGACY_X509_USER_PROXY)},
    )
    utils.prepare_post_plan_files(
        execution_directory=execution_directory,
        plan_log=plan_log,
        remote_site=remote_site,
        run_directory=run_directory,
        run_site=run_site,
        run_id=run_id,
    )
    runmanager.edit(
        run_id=run_id,
        fields={"PP_Host": remote_site, "Job_ID": None},
        server=DEFAULT_DB_SERVER,
        config=config,
    )
    _success(
        f"Planned stochastic workflow: run={run_id} site={remote_site} "
        f"submit_dir={execution_directory.resolve()}"
    )


@cybershake.command(name="stoch-run")
@run_options
def stoch_run(
    *, site: str, run_id: int, restart: bool, notify_user: str | None
) -> None:
    """Submit a planned stochastic workflow.

    \f
    Args:
        site (str): CyberShake site short name.
        run_id (int): Run whose planned workflow should be submitted.
        restart (bool, optional): Mark the submission as a restart. Defaults to
            ``False``.
        notify_user (str | None, optional): User or address recorded for workflow
            notifications. Defaults to ``None``.
    """  # noqa: D301
    config = environment_config()
    root_directory = config.output_directory / f"{site}_Stoch_dax"
    run_file = root_directory / RUN_TABLE_FILENAME
    run_site = utils.require_run_site(run_file, run_id)
    runmanager.valid(
        run_id=run_id,
        site=run_site,
        stage="PP_PLAN",
        server=DEFAULT_DB_SERVER,
        config=config,
    )

    run_directory = root_directory / f"run_{run_id}"
    cluster_id, execution_directory = utils.submit_workflow(
        run_directory,
        plan_pattern=f"log-plan-CyberShake_Stoch_{site}_top.dax-*",
        run_log_name=f"log-run-CyberShake_Stoch_{site}.dax",
    )
    job_id = f"{utils.submission_host()}:{cluster_id}"
    utils.append_condor_config(
        execution_directory,
        include_shock_default=False,
        allow_condor_lookup=True,
    )
    try:
        runmanager.edit(
            run_id=run_id,
            fields={
                "Status": PP_STARTED_STATE if restart else SGT_GENERATED_STATE,
                "Job_ID": job_id,
                "Submit_Dir": str(execution_directory),
                "Notify_User": notify_user,
            },
            server=DEFAULT_DB_SERVER,
            config=config,
        )
    except RunManagerError:
        click.echo(
            "Unable to update Status, Job_ID, Submit_Dir, "
            f"Notify_User for run {run_id}",
            err=True,
        )
    _success(
        f"Submitted stochastic workflow: run={run_id} job={job_id} "
        f"submit_dir={execution_directory.resolve()}"
    )


@cybershake.command(name="sub-stoch-wf")
@envvar_option("-r", "--run-id", type=int, required=True, help="Stochastic run ID.")
@envvar_option(
    "-lr",
    "--low-frequency-run-id",
    type=int,
    required=True,
    help="Low-frequency run ID.",
)
@envvar_option(
    "-v",
    "--velocity-file",
    type=click.Path(path_type=Path, file_okay=True, dir_okay=False),
    required=True,
    help="Velocity information file.",
)
@envvar_option(
    "-o",
    "--workflow-file",
    type=click.Path(path_type=Path, file_okay=True, dir_okay=False),
    required=True,
    help="Output workflow file.",
)
@workflow_options
@pp_stoch_options
@stoch_sub_workflow_options
@config_option
def sub_stoch_wf(
    *,
    run_id: int,
    low_frequency_run_id: int,
    velocity_file: Path,
    workflow_file: Path,
    server: str,
    z_comp: bool,
    debug: bool,
    db_rv_seed: bool,
    period_duration: bool,
    no_vertical_response: bool,
    rupture_list: Path | None,
    generator_merge_frequency: float | None,
    no_rotd: bool,
    no_site_response: bool,
    no_low_site_response: bool,
    no_duration: bool,
    hf_vel_model: str | None,
    config: Config,
) -> None:
    """Generate a per-rupture stochastic sub-workflow.

    \f
    Args:
        run_id (int): Stochastic run represented by the sub-workflow.
        low_frequency_run_id (int): Run providing low-frequency seismograms.
        velocity_file (Path): Velocity information used by stochastic synthesis.
        workflow_file (Path): Destination workflow file.
        server (str, optional): Run database server. Defaults to
            ``moment`` (``DEFAULT_DB_SERVER``).
        z_comp (bool, optional): Calculate the vertical component. Defaults to
            ``False``.
        debug (bool, optional): Enable generator debugging. Defaults to ``False``.
        db_rv_seed (bool, optional): Read rupture fractions and seeds from the
            database. Defaults to ``True``.
        period_duration (bool, optional): Calculate period-dependent durations.
            Defaults to ``False``.
        no_vertical_response (bool, optional): Skip vertical response spectra.
            Defaults to ``False``.
        rupture_list (Path | None, optional): CSV file restricting source and
            rupture IDs. Defaults to ``None``.
        generator_merge_frequency (float | None, optional): Generator
            merge-frequency override in Hz. Defaults to ``None``.
        no_rotd (bool, optional): Omit RotD calculations. Defaults to ``False``.
        no_site_response (bool, optional): Omit site-response calculations.
            Defaults to ``False``.
        no_low_site_response (bool, optional): Omit low-frequency site response.
            Defaults to ``False``.
        no_duration (bool, optional): Omit duration calculations. Defaults to
            ``False``.
        hf_vel_model (str | None, optional): One-dimensional high-frequency
            velocity model. Defaults to ``None``.
        config (Config): Complete resolved CyberShake configuration.
    """  # noqa: D301
    # Click has already included these options in config.
    del (
        server,
        z_comp,
        debug,
        db_rv_seed,
        period_duration,
        no_vertical_response,
        rupture_list,
        generator_merge_frequency,
        no_rotd,
        no_site_response,
        no_low_site_response,
        no_duration,
        hf_vel_model,
    )
    stoch_generator = stoch_workflow.Stoch(config=config)
    stoch_generator.create_sub_stoch(
        run_id=run_id,
        low_frequency_run_id=low_frequency_run_id,
        velocity_file=velocity_file,
        workflow_file=workflow_file,
    )
    _success(
        f"Created stochastic sub-workflow: run={run_id} "
        f"low_frequency_run={low_frequency_run_id} file={workflow_file.resolve()}"
    )


@cybershake.command(cls=FullCommand, name="full")
@common_options()
@low_frequency_options()
@envvar_option(
    "-bb",
    "--broadband",
    is_flag=True,
    help="Include stochastic high-frequency calculations.",
)
@workflow_options
@handoff_option
@full_spacing_options
@sgt_stoch_options
@pp_stoch_options
@sgt_options
@pp_high_frequency_option("--high-frequency")
@pp_options
@stoch_options(stoch_frequency_default=None, include_calculation_flags=False)
@config_option
def full(
    *,
    velocity_model: str,
    erf_id: int,
    rupture_variation_id: int,
    sgt_variation_id: int,
    frequency: float,
    source_frequency: float | None,
    site: str,
    broadband: bool,
    server: str,
    separate_md5: bool,
    sgt_site: str | None,
    sgt_spacing: float | None,
    pp_spacing: float | None,
    min_vs: float | None,
    no_smoothing: bool,
    depth: float | None,
    h_fraction: float | None,
    rotation: float | None,
    ely_taper: str | None,
    taper_depth: float | None,
    taper_models: str | None,
    local_vparam_insert: bool,
    no_insert: bool,
    high_frequency: float | None,
    no_mpi_cluster: bool,
    dir_hierarchy: bool,
    rotd: bool,
    skip_md5: bool,
    nonblocking_md5: bool,
    debug: bool,
    pp_site: str | None,
    duration: bool,
    seismogram_length: float | None,
    generator_merge_frequency: float | None,
    stoch_frequency: float | None,
    no_site_response: bool,
    no_low_site_response: bool,
    force_vs30: float | None,
    db_rv_seed: bool,
    hf_vel_model: str | None,
    period_duration: bool,
    z_comp: bool,
    handoff: bool,
    no_vertical_response: bool,
    rupture_list: Path | None,
    config: Config,
) -> None:
    """Create a full workflow.

    \f
    Args:
        velocity_model (str): Velocity-model alias.
        erf_id (int): Earthquake rupture forecast ID.
        rupture_variation_id (int): Rupture variation scenario ID.
        sgt_variation_id (int): SGT variation ID.
        frequency (float): Simulation frequency in Hz.
        source_frequency (float | None, optional): SGT source-filter frequency.
            Defaults to ``None``, which uses ``frequency``.
        site (str): CyberShake site short name.
        broadband (bool, optional): Include stochastic high-frequency
            calculations. Defaults to ``False``.
        server (str, optional): Run database server. Defaults to
            ``moment`` (``DEFAULT_DB_SERVER``).
        separate_md5 (bool, optional): Run checksums separately from post-AWP
            jobs. Defaults to ``False``.
        sgt_site (str | None, optional): SGT execution site override. Defaults to
            ``None``.
        sgt_spacing (float | None, optional): SGT grid spacing override in km.
            Defaults to ``None``, which derives spacing from the simulation
            frequency.
        pp_spacing (float | None, optional): PP grid spacing override in km.
            Defaults to ``None``.
        min_vs (float | None, optional): Minimum shear-wave velocity override in
            m/s. Defaults to ``None``.
        no_smoothing (bool, optional): Disable velocity-model smoothing. Defaults
            to ``False``.
        depth (float | None, optional): Simulation depth override in km. Defaults
            to ``None``.
        h_fraction (float | None, optional): Grid-point depth fraction for surface
            queries. Defaults to ``None``.
        rotation (float | None, optional): Simulation-volume rotation in degrees.
            Defaults to ``None``.
        ely_taper (str | None, optional): Ely taper mode. Defaults to ``None``.
        taper_depth (float | None, optional): Ely taper depth in metres. Defaults
            to ``None``.
        taper_models (str | None, optional): Comma-separated velocity models to
            taper. Defaults to ``None``.
        local_vparam_insert (bool, optional): Insert velocity parameters in a
            local job. Defaults to ``False``.
        no_insert (bool, optional): Do not insert ruptures into the database.
            Defaults to ``False``.
        high_frequency (float | None, optional): PP high-frequency cutoff.
            Defaults to ``None`` when absent or ``1.0`` when specified without a
            value.
        no_mpi_cluster (bool, optional): Disable ``pegasus-mpi-cluster``. Defaults
            to ``False``.
        dir_hierarchy (bool, optional): Use a compute-resource directory
            hierarchy. Defaults to ``False``.
        rotd (bool, optional): Calculate RotD metrics in PP and stochastic stages.
            Defaults to ``True``.
        skip_md5 (bool, optional): Skip MD5 checksum jobs. Defaults to ``False``.
        nonblocking_md5 (bool, optional): Move checksums out of the critical path.
            Defaults to ``False``.
        debug (bool, optional): Enable generator debugging. Defaults to ``False``.
        pp_site (str | None, optional): Post-processing execution site override.
            Defaults to ``None``.
        duration (bool, optional): Calculate duration metrics in PP and stochastic
            stages. Defaults to ``True``.
        seismogram_length (float | None, optional): Output seismogram length in
            seconds. Defaults to ``None``.
        generator_merge_frequency (float | None, optional): Stochastic
            merge-frequency override in Hz. Defaults to ``None``.
        stoch_frequency (float | None, optional): Maximum stochastic frequency;
            its presence enables broadband mode. Defaults to ``None``.
        no_site_response (bool, optional): Omit stochastic site response. Defaults
            to ``False``.
        no_low_site_response (bool, optional): Omit low-frequency site response.
            Defaults to ``False``.
        force_vs30 (float | None, optional): Forced Vs30 value in m/s. Defaults to
            ``None``.
        db_rv_seed (bool, optional): Read rupture fractions and seeds from the
            database. Defaults to ``True``.
        hf_vel_model (str | None, optional): One-dimensional high-frequency
            velocity model. Defaults to ``None``.
        period_duration (bool, optional): Calculate period-dependent durations.
            Defaults to ``False``.
        z_comp (bool, optional): Calculate the vertical component. Defaults to
            ``False``.
        handoff (bool, optional): Add completed stages to the pending handoff file.
            Defaults to ``False``.
        no_vertical_response (bool, optional): Skip vertical response spectra.
            Defaults to ``False``.
        rupture_list (Path | None, optional): CSV file restricting source and
            rupture IDs. Defaults to ``None``.
        config (Config): Complete resolved CyberShake configuration.
    """  # noqa: D301
    # Click has already included these options in config.
    del (
        broadband,
        separate_md5,
        sgt_site,
        sgt_spacing,
        pp_spacing,
        min_vs,
        no_smoothing,
        depth,
        h_fraction,
        rotation,
        ely_taper,
        taper_depth,
        taper_models,
        local_vparam_insert,
        no_insert,
        high_frequency,
        no_mpi_cluster,
        dir_hierarchy,
        rotd,
        skip_md5,
        nonblocking_md5,
        debug,
        pp_site,
        duration,
        seismogram_length,
        generator_merge_frequency,
        no_site_response,
        no_low_site_response,
        force_vs30,
        db_rv_seed,
        hf_vel_model,
        period_duration,
        z_comp,
        handoff,
        no_vertical_response,
        rupture_list,
    )
    resolved_source_frequency = (
        frequency if source_frequency is None else source_frequency
    )
    velocity_model_id = VELOCITY_MODELS[velocity_model]
    run_directory = config.output_directory / f"{site}_Integrated_dax"
    run_directory.mkdir(parents=True, exist_ok=True)
    run_file = run_directory / RUN_TABLE_FILENAME

    run_id = (
        utils.find_full_run(
            run_file,
            site=site,
            erf_id=erf_id,
            sgt_variation_id=sgt_variation_id,
            rupture_variation_id=rupture_variation_id,
            velocity_model_id=velocity_model_id,
            frequency=frequency,
            source_frequency=resolved_source_frequency,
            stoch_frequency=None,
            stage="SGT_PLAN",
            server=server,
            config=config,
        )
        if run_file.exists()
        else None
    )
    if run_id is None:
        run_id = utils.create_full_run(
            run_file,
            site=site,
            erf_id=erf_id,
            sgt_variation_id=sgt_variation_id,
            velocity_model_id=velocity_model_id,
            rupture_variation_id=rupture_variation_id,
            frequency=frequency,
            source_frequency=resolved_source_frequency,
            stoch_frequency=None,
            server=server,
            config=config,
        )

    broadband_run_id = None
    if stoch_frequency is not None:
        broadband_run_id = utils.find_full_run(
            run_file,
            site=site,
            erf_id=erf_id,
            sgt_variation_id=sgt_variation_id,
            rupture_variation_id=rupture_variation_id,
            velocity_model_id=velocity_model_id,
            frequency=frequency,
            source_frequency=resolved_source_frequency,
            stoch_frequency=stoch_frequency,
            stage="SGT_PLAN",
            server=server,
            config=config,
        )
        if broadband_run_id is None:
            broadband_run_id = utils.create_full_run(
                run_file,
                site=site,
                erf_id=erf_id,
                sgt_variation_id=sgt_variation_id,
                velocity_model_id=velocity_model_id,
                rupture_variation_id=rupture_variation_id,
                frequency=frequency,
                source_frequency=resolved_source_frequency,
                stoch_frequency=stoch_frequency,
                server=server,
                config=config,
            )

    output_directory = run_directory / f"run_{run_id}"
    output_directory.mkdir(parents=True, exist_ok=True)
    (output_directory / "sites.list").write_text(f"{site}\n", encoding="utf-8")
    if broadband_run_id is not None:
        (output_directory / "linked_bb_run_id.txt").write_text(
            f"{broadband_run_id}\n", encoding="utf-8"
        )

    workflow_file = output_directory / f"CyberShake_Integrated_{site}.dax"
    sgt_generator = sgt_workflow.SGT(config=config)
    pp_generator = pp_workflow.PP(config=config)
    stoch_generator = stoch_workflow.Stoch(config=config)
    full_generator = full_workflow.Full(
        sgt=sgt_generator,
        pp=pp_generator,
        stoch=stoch_generator,
    )
    full_generator.create(
        workflow_file=workflow_file,
        run_directory=output_directory,
        run_ids=(run_id,),
        broadband_run_id=broadband_run_id,
    )

    edited_run_ids = (
        (run_id, broadband_run_id) if broadband_run_id is not None else (run_id,)
    )
    for edited_run_id in edited_run_ids:
        try:
            runmanager.edit(
                run_id=edited_run_id,
                fields={
                    "Status": START_STATE,
                    "Comment": "SGT DAX created",
                    "Job_ID": None,
                    "Submit_Dir": None,
                },
                server=server,
                config=config,
            )
        except RunManagerError:
            click.echo(
                "Unable to update Status, Comment, Job_ID, Submit_Dir "
                f"for run {edited_run_id}",
                err=True,
            )
    run_details = f"run={run_id}"
    if broadband_run_id is not None:
        run_details += f" broadband_run={broadband_run_id}"
    _success(f"Created full workflow: {run_details} file={workflow_file.resolve()}")


@cybershake.command(name="full-plan")
@plan_options(DEFAULT_FULL_PROPERTIES)
def full_plan(*, site: str, run_id: int, remote_site: str, properties: Path) -> None:
    """Plan an integrated workflow.

    \f
    Args:
        site (str): CyberShake site short name.
        run_id (int): Run whose integrated workflow should be planned.
        remote_site (str): Pegasus execution and output site.
        properties (Path, optional): Pegasus properties file. Defaults to
            ``conf/properties.full`` (``DEFAULT_FULL_PROPERTIES``).
    """  # noqa: D301
    config = environment_config()
    root_directory = config.output_directory / f"{site}_Integrated_dax"
    run_file = root_directory / RUN_TABLE_FILENAME
    run_site = utils.require_run_site(run_file, run_id)
    runmanager.valid(
        run_id=run_id,
        site=run_site,
        stage="SGT_PLAN",
        server=DEFAULT_DB_SERVER,
        config=config,
    )

    run_directory = root_directory / f"run_{run_id}"
    shutil.copyfile(run_directory / "sites.list", run_directory / "sites.list.debug")
    workflow_name = f"CyberShake_Integrated_{site}.dax"
    pp_host = _planned_pp_host(
        run_directory / workflow_name,
        remote_site.split("_", maxsplit=1)[0],
    )
    utils.remove_plan_logs(run_directory, f"log-plan-{workflow_name}-*")
    local_properties = run_directory / "properties.full.onesite"
    pegasus_properties.dump(pegasus_properties.load(properties), local_properties)
    execution_directory = pegasus.plan(
        workflow_file=run_directory / workflow_name,
        working_directory=run_directory,
        properties_file=local_properties,
        plan_log=run_directory / f"log-plan-{workflow_name}-{remote_site}",
        execution_sites=(remote_site, "shock", "local"),
        output_site=remote_site,
        submit_directory=Path("dags"),
        rescue=100,
        cleanup="none",
        force=True,
        cluster=None,
        output_directory=None,
        verbosity=0,
        environment=None,
    )
    utils.write_full_plan_metadata(
        execution_directory=execution_directory,
        properties_file=local_properties,
        remote_site=remote_site,
    )

    remote_host = remote_site.split("_", maxsplit=1)[0]
    runmanager.edit(
        run_id=run_id,
        fields={
            "Status": START_STATE,
            "SGT_Host": remote_host,
            "Comment": "Planned SGT DAX, assigned sgt host",
            "Job_ID": None,
            "PP_Host": pp_host,
        },
        server=DEFAULT_DB_SERVER,
        config=config,
    )
    linked_run_file = run_directory / "linked_bb_run_id.txt"
    if linked_run_file.is_file():
        try:
            linked_run_id = int(linked_run_file.read_text(encoding="utf-8").strip())
        except ValueError as error:
            msg = f"Invalid linked run ID in {linked_run_file}"
            raise FullError(msg) from error
        runmanager.edit(
            run_id=linked_run_id,
            fields={
                "Status": START_STATE,
                "SGT_Host": remote_host,
                "Comment": "Assigned hosts",
                "Job_ID": None,
                "PP_Host": remote_host,
            },
            server=DEFAULT_DB_SERVER,
            config=config,
        )
    _success(
        f"Planned full workflow: run={run_id} site={remote_site} "
        f"submit_dir={execution_directory.resolve()}"
    )


@cybershake.command(name="full-run")
@run_options
def full_run(*, site: str, run_id: int, restart: bool, notify_user: str | None) -> None:
    """Submit a planned integrated workflow.

    \f
    Args:
        site (str): CyberShake site short name.
        run_id (int): Run whose planned workflow should be submitted.
        restart (bool, optional): Mark an SGT-stage submission as a restart.
            Defaults to ``False``.
        notify_user (str | None, optional): User or address recorded for workflow
            notifications. Defaults to ``None``.
    """  # noqa: D301
    config = environment_config()
    root_directory = config.output_directory / f"{site}_Integrated_dax"
    run_file = root_directory / RUN_TABLE_FILENAME
    run_site = utils.require_run_site(run_file, run_id)
    try:
        runmanager.valid(
            run_id=run_id,
            site=run_site,
            stage="SGT_PLAN",
            server=DEFAULT_DB_SERVER,
            config=config,
        )
    except RunManagerError:
        runmanager.valid(
            run_id=run_id,
            site=run_site,
            stage="PP_PLAN",
            server=DEFAULT_DB_SERVER,
            config=config,
        )
        new_state = PP_STARTED_STATE
    else:
        new_state = SGT_STARTED_STATE if restart else START_STATE

    run_directory = root_directory / f"run_{run_id}"
    linked_run_file = run_directory / "linked_bb_run_id.txt"
    linked_run_id = None
    if linked_run_file.is_file():
        try:
            linked_run_id = int(linked_run_file.read_text(encoding="utf-8").strip())
        except ValueError as error:
            msg = f"Invalid linked run ID in {linked_run_file}"
            raise FullError(msg) from error
        try:
            runmanager.valid(
                run_id=linked_run_id,
                site=run_site,
                stage="SGT_PLAN",
                server=DEFAULT_DB_SERVER,
                config=config,
            )
        except RunManagerError:
            runmanager.valid(
                run_id=linked_run_id,
                site=run_site,
                stage="PP_PLAN",
                server=DEFAULT_DB_SERVER,
                config=config,
            )

    cluster_id, execution_directory = utils.submit_workflow(
        run_directory,
        plan_pattern=f"log-plan-CyberShake_Integrated_{site}*",
        run_log_name=f"log-run-CyberShake_Integrated_{site}",
    )
    job_id = f"{utils.submission_host()}:{cluster_id}"
    fields = {
        "Status": new_state,
        "Comment": "workflow submitted",
        "Job_ID": job_id,
        "Submit_Dir": str(execution_directory),
        "Notify_User": notify_user,
    }
    try:
        runmanager.edit(
            run_id=run_id,
            fields=fields,
            server=DEFAULT_DB_SERVER,
            config=config,
        )
    except RunManagerError:
        click.echo(
            "Unable to update Status, Comment, Job_ID, Submit_Dir, "
            f"Notify_User for run {run_id}",
            err=True,
        )
    if linked_run_id is not None:
        runmanager.edit(
            run_id=linked_run_id,
            fields=fields,
            server=DEFAULT_DB_SERVER,
            config=config,
        )
    run_details = f"run={run_id}"
    if linked_run_id is not None:
        run_details += f" broadband_run={linked_run_id}"
    _success(
        f"Submitted full workflow: {run_details} job={job_id} "
        f"submit_dir={execution_directory.resolve()}"
    )


@cybershake.command()
@click.argument("mode", type=click.Choice(CRON_RUN_CHECK_MODES))
def cron_run_check(*, mode: str) -> None:
    """Run the periodic CyberShake run check.

    \f
    Args:
        mode (str): Run-check mode: ``MASTER`` or ``SECONDARY``.
    """  # noqa: D301
    previous_handler = signal.signal(
        signal.SIGALRM,
        utils.raise_cron_run_check_timeout,
    )
    signal.alarm(CRON_RUN_CHECK_TIMEOUT_SECONDS)
    try:
        runmanager.cron_run_check(mode=mode)
    except CronRunCheckTimeoutError:
        click.echo("Cron check didn't terminate naturally, so it was killed.", err=True)
        LEGACY_CRON_LOCK_FILE.unlink()
    else:
        _success(f"Completed run check: mode={mode}")
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous_handler)


@cybershake.group(cls=EpilogGroup)
def run_manager() -> None:
    """Manage CyberShake run records."""


@run_manager.command()
@envvar_option(
    "-o",
    "--override",
    "override",
    is_flag=True,
    help="Ignore state-transition constraints.",
)
@click.argument("run_id", type=int)
@click.argument("assignments", nargs=-1)
def edit(*, run_id: int, assignments: tuple[str, ...], override: bool) -> None:
    """Edit a run using FIELD=VALUE assignments.

    \f
    Args:
        run_id (int): Run to edit.
        assignments (tuple[str, ...]): Legacy run field assignments.
        override (bool): Skip state-transition constraints.
    """  # noqa: D301
    if not assignments:
        raise click.UsageError("Provide at least one FIELD=VALUE assignment")
    fields: dict[str, str | None] = {}
    for assignment in assignments:
        name, separator, value = assignment.partition("=")
        if not separator or name not in VALID_FIELDS:
            raise click.BadParameter(
                f"Expected one of {', '.join(VALID_FIELDS)} as FIELD=VALUE: "
                f"{assignment}",
                param_hint="assignments",
            )
        fields[name] = None if value == "NULL" else value
    try:
        runmanager.edit(
            run_id=run_id,
            fields=fields,
            override=override,
            server=DEFAULT_DB_SERVER,
            config=environment_config(),
        )
    except RunManagerError as error:
        raise click.ClickException(str(error)) from error
    _success(f"Updated run: run={run_id}")


@run_manager.command()
@click.argument("run_id", type=int)
@click.argument("old_state", type=click.Choice(tuple(RUN_STATES)))
@click.argument("new_state", type=click.Choice(tuple(RUN_STATES)))
def update(*, run_id: int, old_state: str, new_state: str) -> None:
    """Apply a deferred workflow-state transition to a run.

    \f
    Args:
        run_id (int): Run to update.
        old_state (str): Expected symbolic workflow state.
        new_state (str): New symbolic workflow state.
    """  # noqa: D301
    config = environment_config()
    runmanager.update(
        run_id=run_id,
        old_state=old_state,
        new_state=new_state,
        server=DEFAULT_DB_SERVER,
        config=config,
    )
    _success(f"Updated run state: run={run_id} {old_state}->{new_state}")


def _runtime_database(*, server: str, db_file: Path) -> SQLAlchemyRunDB:
    """Connect a workflow runtime command to the configured database."""
    return connect(
        server=server,
        credentials=db_file,
        config=environment_config(server=server),
    )


@cybershake.group()
def jobs() -> None:
    """Commands executed inside generated workflows."""


@jobs.command(name="check-db-site")
@envvar_option("-r", "--run-id", type=int, required=True)
@envvar_option("-o", "--output", type=click.Path(path_type=Path), required=True)
@envvar_option("-c", "--component", default="geometric")
@envvar_option("-p", "--periods")
@envvar_option("-t", "--type-ids")
@envvar_option("-s", "--server", default="moment.usc.edu")
@envvar_option("-rl", "--rupture-list", type=click.Path(path_type=Path))
@envvar_option("--classpath")
def check_db_site(
    *,
    run_id: int,
    output: Path,
    component: str,
    periods: str | None,
    type_ids: str | None,
    server: str,
    rupture_list: Path | None,
    classpath: str | None,
) -> None:
    """Check that every expected rupture variation has database products."""
    try:
        exit_code = db_check.check_db_site(
            run_id=run_id,
            output=output,
            component=component,
            periods=periods,
            type_ids=type_ids,
            server=server,
            rupture_list=rupture_list,
            classpath=classpath,
        )
    except CheckDBError as error:
        raise click.ClickException(str(error)) from error
    if exit_code:
        raise SystemExit(exit_code)
    _success(f"Checked database products: run={run_id}")


@jobs.command(
    name="curve-plot",
    context_settings={"ignore_unknown_options": True},
)
@envvar_option("--velocity-file", type=click.Path(path_type=Path))
@envvar_option("--classpath")
@click.argument("arguments", nargs=-1, type=click.UNPROCESSED)
def curve_plot(
    *, velocity_file: Path | None, classpath: str | None, arguments: tuple[str, ...]
) -> None:
    """Generate CyberShake hazard curves with OpenSHA."""
    try:
        exit_code = plot_jobs.run_plot(
            "curve", arguments, velocity_file=velocity_file, classpath=classpath
        )
    except PlottingError as error:
        raise click.ClickException(str(error)) from error
    if exit_code:
        raise SystemExit(exit_code)
    _success("Generated hazard curves")


@jobs.command(
    name="disagg-plot",
    context_settings={"ignore_unknown_options": True},
)
@envvar_option("--classpath")
@click.argument("arguments", nargs=-1, type=click.UNPROCESSED)
def disagg_plot(*, classpath: str | None, arguments: tuple[str, ...]) -> None:
    """Generate CyberShake disaggregation plots with OpenSHA."""
    try:
        exit_code = plot_jobs.run_plot("disagg", arguments, classpath=classpath)
    except PlottingError as error:
        raise click.ClickException(str(error)) from error
    if exit_code:
        raise SystemExit(exit_code)
    _success("Generated disaggregation plots")


@cybershake.command(name="insert-velocity-params")
@envvar_option("-i", "--input-file", type=click.Path(path_type=Path), required=True)
@envvar_option("-r", "--run-id", type=int, required=True)
@envvar_option(
    "-d",
    "--db-file",
    type=click.Path(path_type=Path),
    default=DEFAULT_JOB_DB_CREDENTIALS,
)
@envvar_option("-s", "--server", default=DEFAULT_DB_SERVER)
def insert_velocity_params(
    *, input_file: Path, run_id: int, db_file: Path, server: str
) -> None:
    """Insert SGT velocity parameters produced by the workflow."""
    try:
        runtime_jobs.insert_velocity_parameters(
            _runtime_database(server=server, db_file=db_file),
            input_file=input_file,
            run_id=run_id,
        )
    except (OSError, ValueError, RunManagerError, SQLAlchemyError) as error:
        raise click.ClickException(str(error)) from error
    _success(f"Inserted velocity parameters: run={run_id}")


@cybershake.command(name="insert-bb-velocity-params")
@envvar_option("-vi", "--velocity-info", type=click.Path(path_type=Path), required=True)
@envvar_option("-bbid", "--broadband-run-id", type=int, required=True)
@envvar_option("-lfid", "--low-frequency-run-id", type=int, required=True)
@envvar_option(
    "-d",
    "--db-file",
    type=click.Path(path_type=Path),
    default=DEFAULT_JOB_DB_CREDENTIALS,
)
@envvar_option("-s", "--server", default=DEFAULT_DB_SERVER)
def insert_bb_velocity_params(
    *,
    velocity_info: Path,
    broadband_run_id: int,
    low_frequency_run_id: int,
    db_file: Path,
    server: str,
) -> None:
    """Insert broadband Vs30 and copy low-frequency basin depths."""
    try:
        runtime_jobs.insert_broadband_velocity_parameters(
            _runtime_database(server=server, db_file=db_file),
            velocity_file=velocity_info,
            broadband_run_id=broadband_run_id,
            low_frequency_run_id=low_frequency_run_id,
        )
    except (OSError, ValueError, RunManagerError, SQLAlchemyError) as error:
        raise click.ClickException(str(error)) from error
    _success(f"Inserted broadband velocity parameters: run={broadband_run_id}")


@cybershake.command(name="handoff")
@envvar_option("-r", "--run-id", type=int, required=True)
@envvar_option("-s", "--stage", type=click.Choice(["LF", "BB"]), default="LF")
@envvar_option(
    "-d",
    "--db-file",
    type=click.Path(path_type=Path),
    default=DEFAULT_JOB_DB_CREDENTIALS,
)
@envvar_option("--server", default=DEFAULT_DB_SERVER)
def handoff(
    *, run_id: int, stage: runtime_jobs.HandoffStage, db_file: Path, server: str
) -> None:
    """Queue the next study stage after a workflow completes."""
    try:
        runtime_jobs.handoff(
            _runtime_database(server=server, db_file=db_file),
            run_id=run_id,
            stage=stage,
        )
    except (OSError, RunManagerError, SQLAlchemyError) as error:
        raise click.ClickException(str(error)) from error
    _success(f"Queued handoff: run={run_id} stage={stage}")


if __name__ == "__main__":
    cybershake()
