"""Run manager: create, validate and edit runs in the run database."""

from __future__ import annotations

import datetime as dt
import fcntl
import getpass
import logging
import socket
from contextlib import contextmanager, suppress
from typing import TYPE_CHECKING

from sqlalchemy.exc import SQLAlchemyError

from cybershake import condor, plotting
from cybershake.constants import (
    AWP_SGT_IDS,
    CRON_ERROR_STATE_MAP,
    CRON_RUN_CHECK_MODES,
    CURVES_GENERATED_STATE,
    DEFAULT_DB_SERVER,
    DONE_STATE,
    HOSTS,
    LEGACY_CRON_LOCK_FILE,
    MATCH_STATE_DICT,
    MAX_RUN_COMMENT,
    MAX_RUN_NOTIFY_USER,
    MAX_RUN_SUBMIT_DIR,
    PLAN_STAGES,
    PLOT_STATE,
    PP_STATES,
    RUN_STATES,
    SGT_GENERATED_STATE,
    SGT_STATES,
    START_STATE,
    STATE_MAP,
    STATUS_STD,
    USERS,
    VALID_FIELDS,
    VERIFY_ERROR_STATE,
)
from cybershake.exc import (
    CronRunCheckError,
    PlottingError,
    ReplicaCatalogError,
    RunManagerError,
)
from cybershake.mail import Mailer
from cybershake.runmanager.db import (
    ParameterKind,
    RunFieldValue,
    SQLAlchemyRunDB,
    connect,
)
from cybershake.runmanager.rc import ReplicaCatalog
from cybershake.runmanager.schema import Run

if TYPE_CHECKING:
    from collections.abc import Collection, Generator, Mapping, Sequence
    from pathlib import Path
    from typing import TextIO

    from cybershake.config import Config
    from cybershake.runmanager.db import RunStatistics


LOGGER = logging.getLogger("runmanager")
Notification = tuple[tuple[str, ...], str, str]


def _now() -> dt.datetime:
    """Return the current naive UTC database timestamp.

    Returns:
        datetime: Current UTC time without timezone information.
    """
    return dt.datetime.now(dt.UTC).replace(tzinfo=None)


def _current_user() -> str:
    """Return the user recorded in run-manager writes.

    Returns:
        str: Current operating-system user name.
    """
    return getpass.getuser()


@contextmanager
def _repository(
    operation: str,
    *,
    server: str,
    _config: Config | None,
) -> Generator[SQLAlchemyRunDB]:
    """Connect a service operation and normalize database failures.

    Args:
        operation (str): Description included in database failure messages.
        server (str): Default database server.
        _config (Config | None): Resolved CyberShake configuration.

    Yields:
        SQLAlchemyRunDB: Connected run repository.

    Raises:
        RunManagerError: If connection or SQL execution fails.
    """
    try:
        yield connect(server=server, config=_config)
    except RunManagerError:
        raise
    except SQLAlchemyError as error:
        raise RunManagerError(f"{operation} failed: {error}") from error


def _validate_creation_parameters(
    database: SQLAlchemyRunDB,
    *,
    site: str,
    erf_id: int,
    sgt_variation_id: int,
    velocity_model_id: int,
    rupture_variation_id: int,
) -> int:
    """Validate creation lookup values and return the site ID.

    Args:
        database (SQLAlchemyRunDB): Active repository.
        site (str): Site short name.
        erf_id (int): ERF ID.
        sgt_variation_id (int): SGT variation ID.
        velocity_model_id (int): Velocity-model ID.
        rupture_variation_id (int): Rupture-variation scenario ID.

    Returns:
        int: Database site ID.

    Raises:
        RunManagerError: If the site or a lookup value does not exist.
    """
    site_row = database.get_site(site)
    if site_row is None:
        raise RunManagerError(f"Site {site} does not exist")
    parameters: tuple[tuple[ParameterKind, int, str], ...] = (
        ("erf", erf_id, "ERF"),
        ("sgt_variation", sgt_variation_id, "SGT variation"),
        ("velocity_model", velocity_model_id, "velocity model"),
        ("rupture_variation", rupture_variation_id, "rupture variation"),
    )
    for kind, parameter_id, label in parameters:
        if not database.parameter_exists(kind, parameter_id):
            raise RunManagerError(f"Unknown {label} {parameter_id}")
    return site_row.cs_site_id


def _new_run(
    *,
    site_id: int,
    erf_id: int,
    sgt_variation_id: int,
    velocity_model_id: int,
    rupture_variation_id: int,
    low_frequency_cutoff: float,
    max_frequency: float,
    source_frequency: float | None,
    status: str,
    sgt_host: str = HOSTS[0],
) -> Run:
    """Build a run with the legacy creation defaults.

    Args:
        site_id (int): Database site ID.
        erf_id (int): ERF ID.
        sgt_variation_id (int): SGT variation ID.
        velocity_model_id (int): Velocity-model ID.
        rupture_variation_id (int): Rupture-variation scenario ID.
        low_frequency_cutoff (float): Low-frequency cutoff in Hz.
        max_frequency (float): Maximum frequency in Hz.
        source_frequency (float | None): SGT source-filter frequency in Hz.
        status (str): Initial run state.
        sgt_host (str, optional): SGT host. Defaults to unknown.

    Returns:
        Run: Populated transient run row.
    """
    timestamp = _now()
    return Run(
        site_id=site_id,
        erf_id=erf_id,
        sgt_variation_id=sgt_variation_id,
        rup_var_scenario_id=rupture_variation_id,
        velocity_model_id=velocity_model_id,
        max_frequency=max_frequency,
        low_frequency_cutoff=low_frequency_cutoff,
        sgt_source_filter_frequency=source_frequency,
        status=status,
        status_time=timestamp,
        sgt_host=sgt_host,
        pp_host=HOSTS[0],
        sgt_time=timestamp,
        pp_time=timestamp,
        comment="",
        last_user=_current_user(),
        job_id="",
        submit_dir="",
        notify_user="",
        minimum_vs=500.0,
    )


def _validated_low_frequency_run(
    database: SQLAlchemyRunDB,
    *,
    run_id: int,
    site: str,
    erf_id: int,
    rupture_variation_id: int,
) -> Run:
    """Return a verified low-frequency run matching stochastic parameters.

    Args:
        database (SQLAlchemyRunDB): Active repository.
        run_id (int): Candidate low-frequency run ID.
        site (str): Expected site short name.
        erf_id (int): Expected ERF ID.
        rupture_variation_id (int): Expected rupture-variation scenario ID.

    Returns:
        Run: Locked and validated low-frequency run.

    Raises:
        RunManagerError: If the run is absent, unverified, or mismatched.
    """
    run = database.get_run(run_id, for_update=True)
    if run is None:
        raise RunManagerError(f"Low-frequency run {run_id} does not exist")
    if run.status != DONE_STATE:
        raise RunManagerError(f"Low-frequency run {run_id} is not Verified")
    site_row = database.get_site_by_id(run.site_id)
    if (
        site_row is None
        or site_row.cs_short_name != site
        or run.erf_id != erf_id
        or run.rup_var_scenario_id != rupture_variation_id
    ):
        raise RunManagerError(
            f"Low-frequency run {run_id} parameters do not match stochastic parameters"
        )
    return run


def _clone_sgt_replicas(
    catalog: ReplicaCatalog,
    *,
    site: str,
    source_run_id: int,
    clone_run_id: int,
    awp: bool,
) -> None:
    """Clone the SGT replica mappings used by a new run.

    Args:
        catalog (ReplicaCatalog): Pegasus replica catalog.
        site (str): Site short name embedded in logical filenames.
        source_run_id (int): Run providing physical SGT files.
        clone_run_id (int): Run receiving new logical filenames.
        awp (bool): Include AWP SGT header mappings.

    Raises:
        ReplicaCatalogError: If a source mapping is absent or a catalog write
            fails.
    """
    templates = [
        "{site}_fx_{run_id}.sgt",
        "{site}_fy_{run_id}.sgt",
        "{site}_fx_{run_id}.sgt.md5",
        "{site}_fy_{run_id}.sgt.md5",
    ]
    if awp:
        templates.extend(["{site}_fx_{run_id}.sgthead", "{site}_fy_{run_id}.sgthead"])
    mappings: list[tuple[str, str, str | None]] = []
    for template in templates:
        source_lfn = template.format(site=site, run_id=source_run_id)
        entries = catalog.lookup(source_lfn)
        if not entries:
            raise ReplicaCatalogError(
                f"Replica-catalog entry for {source_lfn} was not found"
            )
        source = entries[0]
        clone_lfn = template.format(site=site, run_id=clone_run_id)
        mappings.append((clone_lfn, source.pfn, source.pool))

    inserted: list[tuple[str, str]] = []
    try:
        for clone_lfn, physical_filename, pool in mappings:
            catalog.insert(clone_lfn, physical_filename, pool=pool)
            inserted.append((clone_lfn, physical_filename))
    except ReplicaCatalogError:
        for clone_lfn, physical_filename in reversed(inserted):
            with suppress(ReplicaCatalogError):
                catalog.delete(clone_lfn, physical_filename)
        raise


def _clone_run(database: SQLAlchemyRunDB, source: Run) -> int:
    """Clone a completed run into the reusable SGT-generated state.

    Args:
        database (SQLAlchemyRunDB): Active transactional repository.
        source (Run): Preferred source run.

    Returns:
        int: New run ID.
    """
    timestamp = _now()
    return database.clone_run(
        source.run_id,
        changes={
            "status": SGT_GENERATED_STATE,
            "status_time": timestamp,
            "pp_host": HOSTS[0],
            "pp_time": timestamp,
            "comment": f"Cloned from run {source.run_id}",
            "last_user": _current_user(),
            "job_id": "",
            "submit_dir": "",
            "notify_user": "",
        },
    )


def _validate_state_requirements(
    run: Run,
    *,
    status: str,
    fields: Mapping[str, RunFieldValue],
) -> None:
    """Validate host requirements for an edited run state.

    Args:
        run (Run): Current database row.
        status (str): Prospective status.
        fields (Mapping[str, RunFieldValue]): Prospective Python-field updates.

    Raises:
        RunManagerError: If the required stage host is unset.
    """
    sgt_host = fields.get("sgt_host", run.sgt_host)
    pp_host = fields.get("pp_host", run.pp_host)
    if status in SGT_STATES and sgt_host in (None, HOSTS[0]):
        raise RunManagerError(f"State {status} requires an SGT host")
    if status in PP_STATES and pp_host in (None, HOSTS[0]):
        raise RunManagerError(f"State {status} requires a PP host")


def create(
    *,
    site: str,
    erf_id: int,
    sgt_variation_id: int,
    velocity_model_id: int,
    rupture_variation_id: int,
    frequency: float,
    source_frequency: float,
    server: str,
    config: Config | None,
    stoch_frequency: float | None = None,
) -> int:
    """Create a deterministic or broadband run.

    Args:
        site (str): CyberShake site short name.
        erf_id (int): Earthquake rupture forecast ID.
        sgt_variation_id (int): SGT variation ID.
        velocity_model_id (int): Velocity model ID.
        rupture_variation_id (int): Rupture variation scenario ID.
        frequency (float): Simulation frequency in Hz.
        source_frequency (float): SGT source-filter frequency in Hz.
        server (str): Run database server.
        config (Config | None): Resolved CyberShake configuration.
        stoch_frequency (float | None, optional): Maximum stochastic frequency for
            a broadband run. Defaults to ``None``.

    Returns:
        int: ID of the created run.

    Raises:
        RunManagerError: If parameters are invalid or the database write fails.
    """
    with (
        _repository("Run creation", server=server, _config=config) as database,
        database.transaction() as transaction,
    ):
        site_id = _validate_creation_parameters(
            transaction,
            site=site,
            erf_id=erf_id,
            sgt_variation_id=sgt_variation_id,
            velocity_model_id=velocity_model_id,
            rupture_variation_id=rupture_variation_id,
        )
        maximum = (
            frequency
            if stoch_frequency is None or stoch_frequency <= 0
            else stoch_frequency
        )
        run = _new_run(
            site_id=site_id,
            erf_id=erf_id,
            sgt_variation_id=sgt_variation_id,
            velocity_model_id=velocity_model_id,
            rupture_variation_id=rupture_variation_id,
            low_frequency_cutoff=frequency,
            max_frequency=maximum,
            source_frequency=source_frequency,
            status=START_STATE,
        )
        return transaction.insert_run(run)


def create_stoch(
    *,
    site: str,
    low_frequency_run_id: int,
    erf_id: int,
    rupture_variation_id: int,
    merge_frequency: float,
    stoch_frequency: float,
    server: str,
    config: Config | None,
) -> int:
    """Create a stochastic run.

    Args:
        site (str): CyberShake site short name.
        low_frequency_run_id (int): Run providing the low-frequency seismograms.
        erf_id (int): Earthquake rupture forecast ID.
        rupture_variation_id (int): Rupture variation scenario ID.
        merge_frequency (float): Frequency at which low- and high-frequency data
            merge.
        stoch_frequency (float): Maximum stochastic frequency in Hz.
        server (str): Run database server.
        config (Config | None): Resolved CyberShake configuration.

    Returns:
        int: ID of the created stochastic run.

    Raises:
        RunManagerError: If the low-frequency run is invalid or insertion fails.
    """
    with (
        _repository(
            "Stochastic run creation", server=server, _config=config
        ) as database,
        database.transaction() as transaction,
    ):
        low_run = _validated_low_frequency_run(
            transaction,
            run_id=low_frequency_run_id,
            site=site,
            erf_id=erf_id,
            rupture_variation_id=rupture_variation_id,
        )
        maximum = stoch_frequency if stoch_frequency > 0 else merge_frequency
        run = _new_run(
            site_id=low_run.site_id,
            erf_id=erf_id,
            sgt_variation_id=low_run.sgt_variation_id,
            velocity_model_id=low_run.velocity_model_id,
            rupture_variation_id=rupture_variation_id,
            low_frequency_cutoff=merge_frequency,
            max_frequency=maximum,
            source_frequency=low_run.sgt_source_filter_frequency,
            status=SGT_GENERATED_STATE,
            sgt_host=low_run.sgt_host or HOSTS[0],
        )
        return transaction.insert_run(run)


def edit(  # noqa: C901 - field-specific legacy validation is intentionally local
    *,
    run_id: int,
    fields: Mapping[str, str | None],
    server: str,
    config: Config | None,
    override: bool = False,
) -> None:
    """Edit selected run fields.

    Args:
        run_id (int): Run to update.
        fields (Mapping[str, str | None]): Legacy database field names and values.
            None clears string-like lifecycle metadata.
        server (str): Run database server.
        config (Config | None): Resolved CyberShake configuration.
        override (bool): Ignore state-transition constraints when true.

    Raises:
        RunManagerError: If the run, fields, transition, or write is invalid.
    """
    if not fields:
        raise RunManagerError("At least one run field is required")
    invalid = set(fields).difference(VALID_FIELDS)
    if invalid:
        raise RunManagerError(f"Unsupported run field(s): {', '.join(sorted(invalid))}")

    with (
        _repository("Run edit", server=server, _config=config) as database,
        database.transaction() as transaction,
    ):
        run = transaction.get_run(run_id, for_update=True)
        if run is None:
            raise RunManagerError(f"Run {run_id} does not exist")
        timestamp = _now()
        updates: dict[str, RunFieldValue] = {
            "status_time": timestamp,
            "last_user": _current_user(),
        }
        status = run.status
        for name, value in fields.items():
            if name == "Status":
                if value is None or value not in STATUS_STD:
                    raise RunManagerError(f"Invalid state {value}")
                if not override and value not in STATUS_STD.get(run.status, ()):
                    raise RunManagerError(
                        f"Invalid state transition from {run.status} to {value}"
                    )
                status = value
                updates["status"] = value
            elif name in ("SGT_Host", "PP_Host"):
                if value is None or value not in HOSTS:
                    raise RunManagerError(f"Invalid host {value}")
                field = "sgt_host" if name == "SGT_Host" else "pp_host"
                time_field = "sgt_time" if name == "SGT_Host" else "pp_time"
                updates[field] = value
                updates[time_field] = timestamp
            elif name == "Comment":
                updates["comment"] = (value or "")[:MAX_RUN_COMMENT]
            elif name == "Last_User":
                if value is None or value not in USERS:
                    raise RunManagerError(f"Invalid user {value}")
                updates["last_user"] = value
            elif name == "Job_ID":
                updates["job_id"] = value or ""
            elif name == "Submit_Dir":
                updates["submit_dir"] = (value or "")[:MAX_RUN_SUBMIT_DIR]
            else:
                updates["notify_user"] = (value or "")[:MAX_RUN_NOTIFY_USER]

        if status in SGT_STATES:
            updates["sgt_time"] = timestamp
        elif status in PP_STATES:
            updates["pp_time"] = timestamp
        _validate_state_requirements(run, status=status, fields=updates)
        if not transaction.update_run_fields(run_id, updates):
            raise RunManagerError(f"Run {run_id} does not exist")


def find(
    *,
    site: str,
    erf_id: int,
    sgt_variation_id: int,
    rupture_variation_id: int,
    velocity_model_id: int,
    frequency: float,
    source_frequency: float,
    server: str,
    config: Config | None,
) -> int:
    """Find or clone the preferred deterministic run.

    Args:
        site (str): CyberShake site short name.
        erf_id (int): Earthquake rupture forecast ID.
        sgt_variation_id (int): SGT variation ID.
        rupture_variation_id (int): Rupture variation scenario ID.
        velocity_model_id (int): Velocity model ID.
        frequency (float): Simulation frequency in Hz.
        source_frequency (float): SGT source-filter frequency in Hz.
        server (str): Run database server.
        config (Config | None): Resolved CyberShake configuration.

    Returns:
        int: ID of the preferred existing or cloned run.

    Raises:
        RunManagerError: If no run matches or cloning fails.
    """
    with (
        _repository("Run lookup", server=server, _config=config) as database,
        database.transaction() as transaction,
    ):
        reusable = transaction.find_preferred_run(
            site=site,
            erf_id=erf_id,
            sgt_variation_id=sgt_variation_id,
            rupture_variation_id=rupture_variation_id,
            velocity_model_id=velocity_model_id,
            low_frequency_cutoff=frequency,
            max_frequency=frequency,
            source_frequency=source_frequency,
            states=MATCH_STATE_DICT["USE"],
            for_update=True,
        )
        if reusable is not None:
            return reusable.run_id
        source = transaction.find_preferred_run(
            site=site,
            erf_id=erf_id,
            sgt_variation_id=sgt_variation_id,
            rupture_variation_id=rupture_variation_id,
            velocity_model_id=velocity_model_id,
            low_frequency_cutoff=frequency,
            max_frequency=frequency,
            source_frequency=source_frequency,
            states=MATCH_STATE_DICT["CLONE"],
            for_update=True,
        )
        if source is None:
            raise RunManagerError("No matching runs found")
        clone_id = _clone_run(transaction, source)
        _clone_sgt_replicas(
            ReplicaCatalog(config=config),
            site=site,
            source_run_id=source.run_id,
            clone_run_id=clone_id,
            awp=sgt_variation_id in AWP_SGT_IDS,
        )
        return clone_id


def find_stoch(
    *,
    site: str,
    low_frequency_run_id: int,
    erf_id: int,
    rupture_variation_id: int,
    merge_frequency: float,
    stoch_frequency: float,
    server: str,
    config: Config | None,
) -> int:
    """Find or clone a stochastic run.

    Args:
        site (str): CyberShake site short name.
        low_frequency_run_id (int): Run providing the low-frequency seismograms.
        erf_id (int): Earthquake rupture forecast ID.
        rupture_variation_id (int): Rupture variation scenario ID.
        merge_frequency (float): Frequency at which low- and high-frequency data
            merge.
        stoch_frequency (float): Maximum stochastic frequency in Hz.
        server (str): Run database server.
        config (Config | None): Resolved CyberShake configuration.

    Returns:
        int: ID of the preferred existing or cloned stochastic run.

    Raises:
        RunManagerError: If the low-frequency run is invalid, no run matches, or
            cloning fails.
    """
    with (
        _repository("Stochastic run lookup", server=server, _config=config) as database,
        database.transaction() as transaction,
    ):
        low_run = _validated_low_frequency_run(
            transaction,
            run_id=low_frequency_run_id,
            site=site,
            erf_id=erf_id,
            rupture_variation_id=rupture_variation_id,
        )
        reusable = transaction.find_runs(
            site=site,
            erf_id=erf_id,
            sgt_variation_id=low_run.sgt_variation_id,
            rupture_variation_id=rupture_variation_id,
            velocity_model_id=low_run.velocity_model_id,
            max_frequency=stoch_frequency,
            low_frequency_cutoff=merge_frequency,
            states=MATCH_STATE_DICT["USE"],
            order_by="sgt_time",
            descending=True,
            for_update=True,
        )
        if reusable:
            return reusable[0].run_id
        sources = transaction.find_runs(
            site=site,
            erf_id=erf_id,
            sgt_variation_id=low_run.sgt_variation_id,
            rupture_variation_id=rupture_variation_id,
            velocity_model_id=low_run.velocity_model_id,
            max_frequency=stoch_frequency,
            low_frequency_cutoff=merge_frequency,
            states=MATCH_STATE_DICT["CLONE"],
            order_by="sgt_time",
            descending=True,
            for_update=True,
        )
        if not sources:
            raise RunManagerError("No matching stochastic runs found")
        source = sources[0]
        clone_id = _clone_run(transaction, source)
        _clone_sgt_replicas(
            ReplicaCatalog(config=config),
            site=site,
            source_run_id=source.run_id,
            clone_run_id=clone_id,
            awp=True,
        )
        return clone_id


def valid(
    *,
    run_id: int,
    site: str,
    stage: str,
    server: str,
    config: Config | None,
) -> None:
    """Validate a run for a workflow planning stage.

    Args:
        run_id (int): Run to validate.
        site (str): Expected CyberShake site short name.
        stage (str): Planning stage for which the run must be valid.
        server (str): Run database server.
        config (Config | None): Resolved CyberShake configuration.

    Raises:
        RunManagerError: If the run, site, stage, or state is invalid.
    """
    if stage not in PLAN_STAGES:
        raise RunManagerError(f"Unknown planning stage {stage}")
    with _repository("Run validation", server=server, _config=config) as database:
        run = database.get_run(run_id)
        if run is None:
            raise RunManagerError(f"Run {run_id} does not exist")
        run_site = database.get_site_by_id(run.site_id)
        if run_site is None:
            raise RunManagerError(f"Site {run.site_id} for run {run_id} does not exist")
        if run_site.cs_short_name != site:
            raise RunManagerError(
                f"Run {run_id} belongs to site {run_site.cs_short_name}; "
                f"expected site {site}"
            )
        if run.status not in PLAN_STAGES[stage]:
            raise RunManagerError(
                f"Run {run_id} in state {run.status} is invalid for {stage}"
            )


def update(
    *,
    run_id: int,
    old_state: str,
    new_state: str,
    server: str,
    config: Config | None,
) -> None:
    """Update a run after a deferred workflow-state transition.

    Args:
        run_id (int): Run to update.
        old_state (str): Expected symbolic workflow state.
        new_state (str): New symbolic workflow state.
        server (str): Run database server.
        config (Config | None): Resolved CyberShake configuration.

    Raises:
        RunManagerError: If symbolic states, current state, hosts, or the database
            write are invalid.
    """
    if old_state == new_state:
        raise RunManagerError("No state change specified")
    if old_state not in RUN_STATES or new_state not in RUN_STATES:
        raise RunManagerError(
            f"Unknown workflow state in transition {old_state} -> {new_state}"
        )
    old_database_state = STATE_MAP[old_state]
    new_database_state = STATE_MAP[new_state]
    if new_database_state not in STATUS_STD[old_database_state]:
        raise RunManagerError(
            f"Invalid workflow state transition {old_state} -> {new_state}"
        )

    with (
        _repository("Run-state update", server=server, _config=config) as database,
        database.transaction() as transaction,
    ):
        run = transaction.get_run(run_id, for_update=True)
        if run is None:
            raise RunManagerError(f"Run {run_id} does not exist")
        expected = RUN_STATES[old_state]
        if run.status not in expected:
            raise RunManagerError(
                f"Run {run_id} has state {run.status}; expected one of {expected}"
            )
        timestamp = _now()
        fields: dict[str, RunFieldValue] = {
            "status": new_database_state,
            "status_time": timestamp,
        }
        if new_database_state in SGT_STATES:
            fields["sgt_time"] = timestamp
        elif new_database_state in PP_STATES:
            fields["pp_time"] = timestamp
        if new_state in ("SGT_END", "PP_END"):
            fields["job_id"] = ""
            fields["comment"] = "Workflow completed successfully"
        _validate_state_requirements(run, status=new_database_state, fields=fields)
        if not transaction.update_run_fields(run_id, fields):
            raise RunManagerError(f"Run {run_id} does not exist")


@contextmanager
def _cron_lock(lock_file: Path) -> Generator[None]:
    """Hold the nonblocking single-instance run-check lock.

    Args:
        lock_file (Path): File used for the advisory process lock.

    Yields:
        None: Control while the lock is held.

    Raises:
        CronRunCheckError: If the file cannot be opened or another instance owns
            the lock.
    """
    try:
        handle: TextIO = lock_file.open("a+", encoding="utf-8")
    except OSError as error:
        raise CronRunCheckError(
            f"Unable to open cron lock {lock_file}: {error}"
        ) from error
    try:
        try:
            fcntl.lockf(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as error:
            raise CronRunCheckError(
                "Cron run check is aborting because another instance is running"
            ) from error
        try:
            yield
        finally:
            with suppress(OSError):
                fcntl.lockf(handle, fcntl.LOCK_UN)
    finally:
        handle.close()


def _run_site(database: SQLAlchemyRunDB, run: Run) -> str:
    """Return the short site name for a run.

    Args:
        database (SQLAlchemyRunDB): Active repository.
        run (Run): Run whose site is required.

    Returns:
        str: CyberShake site short name.

    Raises:
        RunManagerError: If the run references an unknown site.
    """
    site = database.get_site_by_id(run.site_id)
    if site is None:
        raise RunManagerError(f"Site {run.site_id} for run {run.run_id} does not exist")
    return site.cs_short_name


def _notification(run: Run, *, site: str, issue: str) -> Notification | None:
    """Build an optional run notification from its recipient field.

    Args:
        run (Run): Run whose notification settings are used.
        site (str): CyberShake site short name.
        issue (str): Status description included in the message.

    Returns:
        Notification | None: Mail arguments, or ``None`` without recipients.
    """
    recipients = tuple(
        recipient.strip()
        for recipient in (run.notify_user or "").split(",")
        if recipient.strip()
    )
    if not recipients:
        return None
    subject = f"Run {run.run_id} (site {site}) status update"
    body = f"Run {run.run_id} (site {site}) encountered the following issue:\n\n{issue}"
    return recipients, subject, body


def _send_notifications(mailer: Mailer, notifications: Sequence[Notification]) -> None:
    """Send queued notifications without aborting lifecycle updates.

    Args:
        mailer (Mailer): SMTP adapter.
        notifications (Sequence[Notification]): Recipient, subject, and body
            tuples to send.
    """
    for recipients, subject, body in notifications:
        if not mailer.send(recipients, subject, body):
            LOGGER.warning("Unable to send run-manager notification: %s", subject)


def _transition_managed_run(
    database: SQLAlchemyRunDB,
    run: Run,
    *,
    status: str,
    comment: str,
) -> None:
    """Apply a cron-selected state and comment to a locked run.

    Args:
        database (SQLAlchemyRunDB): Transactional repository.
        run (Run): Locked run row.
        status (str): New database status.
        comment (str): New status comment.

    Raises:
        RunManagerError: If host requirements fail or the run disappears.
    """
    timestamp = _now()
    fields: dict[str, RunFieldValue] = {
        "status": status,
        "status_time": timestamp,
        "comment": comment,
    }
    if status in SGT_STATES:
        fields["sgt_time"] = timestamp
    elif status in PP_STATES:
        fields["pp_time"] = timestamp
    _validate_state_requirements(run, status=status, fields=fields)
    if not database.update_run_fields(run.run_id, fields):
        raise RunManagerError(f"Run {run.run_id} does not exist")


def _check_execution_errors(
    database: SQLAlchemyRunDB,
    *,
    active_job_ids: Collection[str],
    local_host: str,
) -> list[Notification]:
    """Move locally submitted missing workflows into their error states.

    Args:
        database (SQLAlchemyRunDB): Connected repository.
        active_job_ids (Collection[str]): Local active HTCondor cluster IDs.
        local_host (str): Short submission hostname.

    Returns:
        list[Notification]: Messages queued for successful state changes.
    """
    notifications: list[Notification] = []
    with database.transaction() as transaction:
        for current_state, error_state in CRON_ERROR_STATE_MAP.items():
            runs = transaction.find_runs(
                states=(current_state,),
                for_update=True,
            )
            for run in runs:
                job_reference = run.job_id or ""
                if job_reference in ("", "Null"):
                    if current_state in (START_STATE, SGT_GENERATED_STATE):
                        continue
                    comment = "Inconsistent state detected"
                    issue = (
                        f"In execution state '{current_state}' with no Job ID "
                        f"assigned. Moved to state '{error_state}'."
                    )
                else:
                    job_host, separator, job_id = job_reference.partition(":")
                    if separator and job_host != local_host:
                        continue
                    if separator and job_id in active_job_ids:
                        continue
                    comment = "Workflow terminated unexpectedly"
                    issue = (
                        f"No job '{job_reference}' found in HTCondor. "
                        f"Moved to state '{error_state}'."
                    )

                try:
                    _transition_managed_run(
                        transaction,
                        run,
                        status=error_state,
                        comment=comment,
                    )
                except RunManagerError as error:
                    LOGGER.warning("Unable to update run %s: %s", run.run_id, error)
                    continue
                notification = _notification(
                    run, site=_run_site(transaction, run), issue=issue
                )
                if notification is not None:
                    notifications.append(notification)
    return notifications


def _check_verification(
    database: SQLAlchemyRunDB,
) -> tuple[bool, list[Notification]]:
    """Verify completed products and select plotting or terminal states.

    Args:
        database (SQLAlchemyRunDB): Connected repository.

    Returns:
        tuple[bool, list[Notification]]: Whether public maps need refreshing and
            queued state-change notifications.
    """
    update_maps = False
    notifications: list[Notification] = []
    with database.transaction() as transaction:
        runs = transaction.find_runs(
            states=(CURVES_GENERATED_STATE,),
            for_update=True,
        )
        for run in runs:
            site = _run_site(transaction, run)
            statistics = transaction.get_run_statistics(run.run_id)
            if statistics.peak_amplitude_count > 0 and statistics.intensity_measures:
                update_maps = True
                if plotting.comparison_curves_exist(
                    site=site,
                    intensity_measures=statistics.intensity_measures,
                ):
                    status = DONE_STATE
                    comment = "Passed verification checks"
                    issue = (
                        f"Run passed verification checks. Moved to state '{status}'."
                    )
                else:
                    status = PLOT_STATE
                    comment = "Generating comparison curves"
                    issue = f"Site needs comparison curves. Moved to state '{status}'."
            else:
                status = VERIFY_ERROR_STATE
                comment = "Failed verification checks"
                issue = (
                    "Run failed verification checks (num_psa == 0 or "
                    "num_curves == 0). "
                    f"Moved to state '{status}'."
                )
            _transition_managed_run(
                transaction,
                run,
                status=status,
                comment=comment,
            )
            notification = _notification(run, site=site, issue=issue)
            if notification is not None:
                notifications.append(notification)
    return update_maps, notifications


def _generate_comparison_curves(
    database: SQLAlchemyRunDB,
) -> list[Notification]:
    """Generate missing comparison curves and persist per-run outcomes.

    Args:
        database (SQLAlchemyRunDB): Connected repository.

    Returns:
        list[Notification]: Messages queued for successful state changes.
    """
    notifications: list[Notification] = []
    candidates = database.find_runs(states=(PLOT_STATE,))
    for candidate in candidates:
        site = _run_site(database, candidate)
        statistics: RunStatistics = database.get_run_statistics(candidate.run_id)
        if not statistics.intensity_measures:
            LOGGER.warning("No hazard curves found for run %s", candidate.run_id)
            continue
        try:
            plotting.generate_comparison_curves(
                run_id=candidate.run_id,
                site=site,
                intensity_measures=statistics.intensity_measures,
            )
        except PlottingError:
            status = VERIFY_ERROR_STATE
            comment = "Failed to create comparison curves"
            issue = f"Failed to generate comparison curves. Moved to state '{status}'."
        else:
            status = DONE_STATE
            comment = "Passed verification checks"
            issue = f"Run passed verification checks. Moved to state '{status}'."

        with database.transaction() as transaction:
            run = transaction.get_run(candidate.run_id, for_update=True)
            if run is None or run.status != PLOT_STATE:
                continue
            _transition_managed_run(
                transaction,
                run,
                status=status,
                comment=comment,
            )
            notification = _notification(run, site=site, issue=issue)
            if notification is not None:
                notifications.append(notification)
    return notifications


def cron_run_check(*, mode: str) -> None:
    """Check managed runs in master or secondary mode.

    Both modes detect locally submitted workflows that disappeared from the
    HTCondor queue. ``MASTER`` additionally verifies database products, refreshes
    public maps, and creates missing comparison curves.

    Args:
        mode (str): ``MASTER`` or ``SECONDARY``.

    Raises:
        CronRunCheckError: If the mode is invalid or another checker owns the
            single-instance lock.
        CondorError: If the local HTCondor queue cannot be queried.
        RunManagerError: If a database operation fails.
    """
    if mode not in CRON_RUN_CHECK_MODES:
        raise CronRunCheckError(f"Unknown cron mode {mode}")

    with (
        _cron_lock(LEGACY_CRON_LOCK_FILE),
        _repository(
            "Cron run check",
            server=DEFAULT_DB_SERVER,
            _config=None,
        ) as database,
    ):
        mailer = Mailer()
        active_job_ids = condor.active_job_ids()
        local_host = socket.gethostname().split(".", 1)[0]
        notifications = _check_execution_errors(
            database,
            active_job_ids=active_job_ids,
            local_host=local_host,
        )
        _send_notifications(mailer, notifications)

        if mode == "SECONDARY":
            return

        update_maps, notifications = _check_verification(database)
        _send_notifications(mailer, notifications)
        if update_maps:
            for failure in plotting.generate_maps():
                LOGGER.warning("Unable to refresh CyberShake map: %s", failure)
        notifications = _generate_comparison_curves(database)
        _send_notifications(mailer, notifications)
