"""Shared utilities for CyberShake command orchestration."""

from __future__ import annotations

import os
import socket
from configparser import DEFAULTSECT
from contextlib import suppress
from pathlib import Path
from typing import TYPE_CHECKING

from cybershake import condor, pegasus, runmanager
from cybershake import properties as pegasus_properties
from cybershake.exc import (
    CondorError,
    CronRunCheckTimeoutError,
    PegasusError,
    RunManagerError,
    RunTableError,
)

if TYPE_CHECKING:
    from configparser import ConfigParser
    from types import FrameType

    from cybershake.config import Config


# =============================================================================
# Run manager utilities
# =============================================================================


def require_run_site(run_file: Path, run_id: int) -> str:
    """Return a run's site or raise a command-line error.

    Args:
        run_file (Path): Legacy workflow run table.
        run_id (int): Run ID to locate.

    Returns:
        str: Site short name recorded for the run.

    Raises:
        RunTableError: If the table or run ID does not exist.
    """
    if not run_file.is_file():
        msg = f"{run_file} not found"
        raise RunTableError(msg)
    site = find_run_site(run_file, run_id)
    if site is None:
        msg = f"Run ID {run_id} not found in {run_file}"
        raise RunTableError(msg)
    return site


def find_run_site(run_file: Path, run_id: int) -> str | None:
    """Return the site for a run in a legacy workflow run table.

    Args:
        run_file (Path): Legacy workflow run table.
        run_id (int): Run ID to locate.

    Returns:
        str | None: Site short name, or ``None`` when the run is absent.
    """
    for line in run_file.read_text(encoding="utf-8").splitlines():
        fields = line.split()
        if len(fields) < 2:
            continue
        try:
            row_run_id = int(fields[0])
        except ValueError:
            continue
        if row_run_id == run_id:
            return fields[1]
    return None


def append_run(
    run_file: Path,
    *,
    run_id: int,
    site: str,
    erf_id: int,
    sgt_variation_id: int,
    rupture_variation_id: int,
    velocity_model_id: int,
    frequency: float,
    source_frequency: float,
    stoch_frequency: float | None = None,
) -> None:
    """Append a run to a legacy workflow run table.

    Args:
        run_file (Path): Legacy workflow run table.
        run_id (int): Run ID to record.
        site (str): CyberShake site short name.
        erf_id (int): Earthquake rupture forecast ID.
        sgt_variation_id (int): SGT variation ID.
        rupture_variation_id (int): Rupture variation scenario ID.
        velocity_model_id (int): Velocity model ID.
        frequency (float): Simulation frequency in Hz.
        source_frequency (float): SGT source-filter frequency in Hz.
        stoch_frequency (float | None, optional): Maximum stochastic frequency for
            a broadband run. Defaults to ``None``.
    """
    with run_file.open("a", encoding="utf-8") as run_table:
        line = (
            f"{run_id} {site} {erf_id} {sgt_variation_id} "
            f"{rupture_variation_id} {velocity_model_id} {frequency} "
            f"{source_frequency}"
        )
        if stoch_frequency is not None:
            line += f" {stoch_frequency}"
        run_table.write(f"{line}\n")


def create_sgt_run(
    run_file: Path,
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
) -> int:
    """Create an SGT run and record it in a legacy run table.

    Args:
        run_file (Path): Legacy workflow run table.
        site (str): CyberShake site short name.
        erf_id (int): Earthquake rupture forecast ID.
        sgt_variation_id (int): SGT variation ID.
        velocity_model_id (int): Velocity model ID.
        rupture_variation_id (int): Rupture variation scenario ID.
        frequency (float): Simulation frequency in Hz.
        source_frequency (float): SGT source-filter frequency in Hz.
        server (str): Run database server.
        config (Config | None): Resolved CyberShake configuration.

    Returns:
        int: ID of the created run.
    """
    run_id = runmanager.create(
        site=site,
        erf_id=erf_id,
        sgt_variation_id=sgt_variation_id,
        velocity_model_id=velocity_model_id,
        rupture_variation_id=rupture_variation_id,
        frequency=frequency,
        source_frequency=source_frequency,
        server=server,
        config=config,
    )
    append_run(
        run_file,
        run_id=run_id,
        site=site,
        erf_id=erf_id,
        sgt_variation_id=sgt_variation_id,
        rupture_variation_id=rupture_variation_id,
        velocity_model_id=velocity_model_id,
        frequency=frequency,
        source_frequency=source_frequency,
    )
    return run_id


def find_run(
    run_file: Path,
    *,
    erf_id: int,
    sgt_variation_id: int,
    rupture_variation_id: int,
    velocity_model_id: int,
    frequency: float,
    source_frequency: float,
    stage: str,
    server: str,
    config: Config | None,
) -> int | None:
    """Find a matching valid run in a legacy seven/eight-column table.

    Args:
        run_file (Path): Legacy workflow run table.
        erf_id (int): Earthquake rupture forecast ID to match.
        sgt_variation_id (int): SGT variation ID to match.
        rupture_variation_id (int): Rupture variation scenario ID to match.
        velocity_model_id (int): Velocity model ID to match.
        frequency (float): Simulation frequency in Hz to match.
        source_frequency (float): SGT source-filter frequency in Hz to match.
        stage (str): Planning stage used to validate a matching run.
        server (str): Run database server.
        config (Config | None): Resolved CyberShake configuration.

    Returns:
        int | None: ID of the first matching valid run, or ``None``.

    Raises:
        RunManagerError: If a matching run fails validation.
    """
    for line in run_file.read_text(encoding="utf-8").splitlines():
        fields = line.split()
        if len(fields) not in (7, 8):
            continue
        try:
            run_id = int(fields[0])
            row_site = fields[1]
            row_erf_id = int(fields[2])
            row_sgt_variation_id = int(fields[3])
            row_rupture_variation_id = int(fields[4])
            row_velocity_model_id = int(fields[5])
            row_frequency = float(fields[6])
            row_source_frequency = (
                float(fields[7]) if len(fields) == 8 else row_frequency
            )
        except ValueError:
            continue

        # Compatibility: the shell script validates the row's site but does not
        # compare it with the requested site before reusing the run.
        if (
            row_erf_id != erf_id
            or row_sgt_variation_id != sgt_variation_id
            or row_rupture_variation_id != rupture_variation_id
            or row_velocity_model_id != velocity_model_id
            or row_frequency != frequency
            or row_source_frequency != source_frequency
        ):
            continue

        runmanager.valid(
            run_id=run_id,
            site=row_site,
            stage=stage,
            server=server,
            config=config,
        )
        return run_id
    return None


def find_pp_run(
    run_file: Path,
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
    """Find a PP run and record it in a legacy run table.

    Args:
        run_file (Path): Legacy workflow run table.
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
    """
    run_id = runmanager.find(
        site=site,
        erf_id=erf_id,
        sgt_variation_id=sgt_variation_id,
        rupture_variation_id=rupture_variation_id,
        velocity_model_id=velocity_model_id,
        frequency=frequency,
        source_frequency=source_frequency,
        server=server,
        config=config,
    )
    append_run(
        run_file,
        run_id=run_id,
        site=site,
        erf_id=erf_id,
        sgt_variation_id=sgt_variation_id,
        rupture_variation_id=rupture_variation_id,
        velocity_model_id=velocity_model_id,
        frequency=frequency,
        source_frequency=source_frequency,
    )
    return run_id


def create_full_run(
    run_file: Path,
    *,
    site: str,
    erf_id: int,
    sgt_variation_id: int,
    velocity_model_id: int,
    rupture_variation_id: int,
    frequency: float,
    source_frequency: float,
    stoch_frequency: float | None,
    server: str,
    config: Config | None,
) -> int:
    """Create and record a deterministic or broadband Full run.

    Args:
        run_file (Path): Legacy workflow run table.
        site (str): CyberShake site short name.
        erf_id (int): Earthquake rupture forecast ID.
        sgt_variation_id (int): SGT variation ID.
        velocity_model_id (int): Velocity model ID.
        rupture_variation_id (int): Rupture variation scenario ID.
        frequency (float): Simulation frequency in Hz.
        source_frequency (float): SGT source-filter frequency in Hz.
        stoch_frequency (float | None): Maximum stochastic frequency for a
            broadband run.
        server (str): Run database server.
        config (Config | None): Resolved CyberShake configuration.

    Returns:
        int: ID of the created run.
    """
    run_id = runmanager.create(
        site=site,
        erf_id=erf_id,
        sgt_variation_id=sgt_variation_id,
        velocity_model_id=velocity_model_id,
        rupture_variation_id=rupture_variation_id,
        frequency=frequency,
        source_frequency=source_frequency,
        stoch_frequency=stoch_frequency,
        server=server,
        config=config,
    )
    append_run(
        run_file,
        run_id=run_id,
        site=site,
        erf_id=erf_id,
        sgt_variation_id=sgt_variation_id,
        rupture_variation_id=rupture_variation_id,
        velocity_model_id=velocity_model_id,
        frequency=frequency,
        source_frequency=source_frequency,
        stoch_frequency=stoch_frequency,
    )
    return run_id


def find_full_run(
    run_file: Path,
    *,
    site: str,
    erf_id: int,
    sgt_variation_id: int,
    rupture_variation_id: int,
    velocity_model_id: int,
    frequency: float,
    source_frequency: float,
    stoch_frequency: float | None,
    stage: str,
    server: str,
    config: Config | None,
) -> int | None:
    """Find a matching valid deterministic or broadband Full run.

    Args:
        run_file (Path): Legacy workflow run table.
        site (str): CyberShake site short name to match.
        erf_id (int): Earthquake rupture forecast ID to match.
        sgt_variation_id (int): SGT variation ID to match.
        rupture_variation_id (int): Rupture variation scenario ID to match.
        velocity_model_id (int): Velocity model ID to match.
        frequency (float): Simulation frequency in Hz to match.
        source_frequency (float): SGT source-filter frequency in Hz to match.
        stoch_frequency (float | None): Maximum stochastic frequency to match,
            when broadband.
        stage (str): Planning stage used to validate a matching run.
        server (str): Run database server.
        config (Config | None): Resolved CyberShake configuration.

    Returns:
        int | None: ID of the first matching valid run, or ``None``.
    """
    for line in run_file.read_text(encoding="utf-8").splitlines():
        fields = line.split()
        if len(fields) < 7:
            continue
        try:
            run_id = int(fields[0])
            row_site = fields[1]
            row_erf_id = int(fields[2])
            row_sgt_variation_id = int(fields[3])
            row_rupture_variation_id = int(fields[4])
            row_velocity_model_id = int(fields[5])
            row_frequency = float(fields[6])
            row_source_frequency = (
                float(fields[7]) if len(fields) >= 8 else row_frequency
            )
            row_stoch_frequency = float(fields[8]) if len(fields) >= 9 else None
        except ValueError:
            continue

        if (
            row_site != site
            or row_erf_id != erf_id
            or row_sgt_variation_id != sgt_variation_id
            or row_rupture_variation_id != rupture_variation_id
            or row_velocity_model_id != velocity_model_id
            or row_frequency != frequency
            or row_source_frequency != source_frequency
        ):
            continue
        if stoch_frequency is not None and row_stoch_frequency != stoch_frequency:
            continue

        try:
            runmanager.valid(
                run_id=run_id,
                site=row_site,
                stage=stage,
                server=server,
                config=config,
            )
        except RunManagerError:
            continue
        return run_id
    return None


def append_stoch_run(
    run_file: Path,
    *,
    run_id: int,
    site: str,
    low_frequency_run_id: int,
    erf_id: int,
    rupture_variation_id: int,
    merge_frequency: float,
    stoch_frequency: float,
) -> None:
    """Append a stochastic run to its legacy run table.

    Args:
        run_file (Path): Legacy stochastic run table.
        run_id (int): Stochastic run ID to record.
        site (str): CyberShake site short name.
        low_frequency_run_id (int): Run providing the low-frequency seismograms.
        erf_id (int): Earthquake rupture forecast ID.
        rupture_variation_id (int): Rupture variation scenario ID.
        merge_frequency (float): Frequency at which low- and high-frequency data
            merge.
        stoch_frequency (float): Maximum stochastic frequency in Hz.
    """
    with run_file.open("a", encoding="utf-8") as run_table:
        run_table.write(
            f"{run_id} {site} {low_frequency_run_id} {erf_id} "
            f"{rupture_variation_id} {merge_frequency} {stoch_frequency}\n"
        )


def create_stoch_run(
    run_file: Path,
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
    """Create a stochastic run and record it in its legacy run table.

    Args:
        run_file (Path): Legacy stochastic run table.
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
    """
    run_id = runmanager.create_stoch(
        site=site,
        low_frequency_run_id=low_frequency_run_id,
        erf_id=erf_id,
        rupture_variation_id=rupture_variation_id,
        merge_frequency=merge_frequency,
        stoch_frequency=stoch_frequency,
        server=server,
        config=config,
    )
    append_stoch_run(
        run_file,
        run_id=run_id,
        site=site,
        low_frequency_run_id=low_frequency_run_id,
        erf_id=erf_id,
        rupture_variation_id=rupture_variation_id,
        merge_frequency=merge_frequency,
        stoch_frequency=stoch_frequency,
    )
    return run_id


def find_initial_stoch_run(
    run_file: Path,
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
    """Find or create a stochastic run and record it in its run table.

    Args:
        run_file (Path): Legacy stochastic run table.
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
        int: ID of the preferred existing, cloned, or newly created stochastic
            run.
    """
    try:
        run_id = runmanager.find_stoch(
            site=site,
            low_frequency_run_id=low_frequency_run_id,
            erf_id=erf_id,
            rupture_variation_id=rupture_variation_id,
            merge_frequency=merge_frequency,
            stoch_frequency=stoch_frequency,
            server=server,
            config=config,
        )
    except RunManagerError:
        return create_stoch_run(
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

    append_stoch_run(
        run_file,
        run_id=run_id,
        site=site,
        low_frequency_run_id=low_frequency_run_id,
        erf_id=erf_id,
        rupture_variation_id=rupture_variation_id,
        merge_frequency=merge_frequency,
        stoch_frequency=stoch_frequency,
    )
    return run_id


def find_existing_stoch_run(
    run_file: Path,
    *,
    low_frequency_run_id: int,
    erf_id: int,
    rupture_variation_id: int,
    merge_frequency: float,
    stoch_frequency: float,
    server: str,
    config: Config | None,
) -> int | None:
    """Find a matching valid run in a stochastic run table.

    Args:
        run_file (Path): Legacy stochastic run table.
        low_frequency_run_id (int): Low-frequency run ID to match.
        erf_id (int): Earthquake rupture forecast ID to match.
        rupture_variation_id (int): Rupture variation scenario ID to match.
        merge_frequency (float): Merge frequency in Hz to match.
        stoch_frequency (float): Maximum stochastic frequency in Hz to match.
        server (str): Run database server.
        config (Config | None): Resolved CyberShake configuration.

    Returns:
        int | None: ID of the first matching valid run, or ``None``.
    """
    for line in run_file.read_text(encoding="utf-8").splitlines():
        fields = line.split()
        if len(fields) != 7:
            continue
        try:
            run_id = int(fields[0])
            row_site = fields[1]
            row_low_frequency_run_id = int(fields[2])
            row_erf_id = int(fields[3])
            row_rupture_variation_id = int(fields[4])
            row_merge_frequency = float(fields[5])
            row_stoch_frequency = float(fields[6])
        except ValueError:
            continue

        if (
            row_low_frequency_run_id != low_frequency_run_id
            or row_erf_id != erf_id
            or row_rupture_variation_id != rupture_variation_id
            or row_merge_frequency != merge_frequency
            or row_stoch_frequency != stoch_frequency
        ):
            continue

        try:
            runmanager.valid(
                run_id=run_id,
                site=row_site,
                stage="PP_PLAN",
                server=server,
                config=config,
            )
        except RunManagerError:
            continue
        return run_id
    return None


# =============================================================================
# Cron utilities
# =============================================================================


def raise_cron_run_check_timeout(_signal_number: int, _frame: FrameType | None) -> None:
    """Raise the cron timeout exception from a process alarm.

    Args:
        _signal_number (int): Delivered signal number; unused.
        _frame (FrameType | None): Interrupted stack frame; unused.

    Raises:
        CronRunCheckTimeoutError: Always, to interrupt the cron run check.
    """
    raise CronRunCheckTimeoutError


# =============================================================================
# Submission utilities
# =============================================================================


def latest_plan_log(run_directory: Path, pattern: str) -> Path:
    """Return the newest plan log matching a glob pattern.

    Args:
        run_directory (Path): Directory containing planning logs.
        pattern (str): Glob pattern used to select planning logs.

    Returns:
        Path: Most recently modified matching planning log.

    Raises:
        PegasusError: If no planning log matches.
    """
    plan_logs = tuple(run_directory.glob(pattern))
    if not plan_logs:
        msg = f"No plan log matching {pattern} found in {run_directory}"
        raise PegasusError(msg)
    return max(
        plan_logs,
        key=lambda path: (path.stat().st_mtime_ns, path.name),
    )


def submit_workflow(
    run_directory: Path, *, plan_pattern: str, run_log_name: str
) -> tuple[str, Path]:
    """Submit the workflow referenced by the newest matching plan log.

    Args:
        run_directory (Path): Directory containing the planned workflow.
        plan_pattern (str): Glob pattern used to select its newest planning log.
        run_log_name (str): Name of the file that receives submission output.

    Returns:
        tuple[str, Path]: Pegasus cluster ID and workflow submit directory.
    """
    return pegasus.run(
        plan_log=latest_plan_log(run_directory, plan_pattern),
        run_log=run_directory / run_log_name,
    )


def submission_host() -> str:
    """Return the unqualified local submission hostname.

    Returns:
        str: Hostname with any domain suffix removed.
    """
    return socket.gethostname().split(".", maxsplit=1)[0]


# =============================================================================
# Condor utilities
# =============================================================================


def condor_config_candidates(*, include_shock_default: bool) -> tuple[Path, ...]:
    """Return local Condor configuration candidates in legacy search order.

    Args:
        include_shock_default (bool): Include the legacy Shock-specific path first.

    Returns:
        tuple[Path, ...]: Candidate configuration paths in lookup order.
    """
    candidates: list[Path] = []
    if include_shock_default:
        candidates.append(Path("/home/shock-ssd/scottcal/condor/condor_config.local"))
    configured = os.environ.get("CONDOR_CONFIG")
    if configured is not None:
        candidates.append(Path(f"{configured}.local"))
    candidates.append(Path("/etc/condor/condor_config.local"))
    with suppress(RuntimeError):
        candidates.append(Path("~condor/condor_config.local").expanduser())
    globus_location = os.environ.get("GLOBUS_LOCATION")
    if globus_location is not None:
        candidates.append(Path(globus_location) / "etc" / "condor_config")
    return tuple(candidates)


def find_condor_config(
    *, include_shock_default: bool, allow_condor_lookup: bool
) -> Path:
    """Return the first available local Condor configuration file.

    Args:
        include_shock_default (bool): Include the legacy Shock-specific path first.
        allow_condor_lookup (bool): Ask HTCondor for its local configuration as a
            fallback.

    Returns:
        Path: First available local configuration file.

    Raises:
        CondorError: If no configuration file can be found.
    """
    for candidate in condor_config_candidates(
        include_shock_default=include_shock_default
    ):
        if candidate.is_file():
            return candidate
    if allow_condor_lookup:
        return condor.local_config_file()
    msg = "No condor config file found."
    raise CondorError(msg)


def append_condor_config(
    execution_directory: Path,
    *,
    include_shock_default: bool,
    allow_condor_lookup: bool,
) -> None:
    """Append the selected local Condor configuration to workflow metadata.

    Args:
        execution_directory (Path): Workflow submit directory containing metadata.
        include_shock_default (bool): Include the legacy Shock-specific path first.
        allow_condor_lookup (bool): Ask HTCondor for its local configuration as a
            fallback.
    """
    config_file = find_condor_config(
        include_shock_default=include_shock_default,
        allow_condor_lookup=allow_condor_lookup,
    )
    with (execution_directory / "metadata.local").open(
        "a", encoding="utf-8"
    ) as metadata:
        metadata.write(f"Condor config local {config_file}\n")
        metadata.write(config_file.read_text(encoding="utf-8"))


# =============================================================================
# Planning utilities
# =============================================================================


def remove_plan_logs(directory: Path, pattern: str) -> None:
    """Remove plan logs matching a glob pattern.

    Args:
        directory (Path): Directory containing planning logs.
        pattern (str): Glob pattern identifying logs to remove.
    """
    for plan_log in directory.glob(pattern):
        plan_log.unlink()


def _required_property(
    properties: ConfigParser, *, key: str, properties_file: Path
) -> str:
    """Return a required value from loaded Pegasus properties.

    Args:
        properties (ConfigParser): Loaded Pegasus properties.
        key (str): Property name to locate.
        properties_file (Path): Source path used in an absent-property error.

    Returns:
        str: Property value.

    Raises:
        PegasusError: If the property is absent.
    """
    try:
        return properties[DEFAULTSECT][key]
    except KeyError as error:
        msg = f"Property {key} not found in {properties_file}"
        raise PegasusError(msg) from error


def prepare_post_plan_files(
    *,
    execution_directory: Path,
    plan_log: Path,
    remote_site: str,
    run_directory: Path,
    run_site: str,
    run_id: int,
) -> None:
    """Prepare runtime properties, metadata, and the PP/stochastic database.

    Args:
        execution_directory (Path): Workflow submit directory produced by Pegasus.
        plan_log (Path): Planning log whose runtime-properties path must be updated.
        remote_site (str): Pegasus execution site recorded in local metadata.
        run_directory (Path): Directory containing the workflow's local database.
        run_site (str): Site short name used in the database filename.
        run_id (int): Run ID used in the relocated database filename.

    Raises:
        PegasusError: If the submit directory does not contain exactly one Pegasus
            runtime-properties file or required properties are absent.
    """
    runtime_properties_files = tuple(execution_directory.glob("pegasus.*.properties"))
    if len(runtime_properties_files) != 1:
        msg = (
            f"Expected one Pegasus runtime properties file in "
            f"{execution_directory}, found {len(runtime_properties_files)}"
        )
        raise PegasusError(msg)
    runtime_properties = runtime_properties_files[0]
    properties = pegasus_properties.load(runtime_properties)
    top_properties = Path(f"{runtime_properties}.top")
    properties[DEFAULTSECT]["pegasus.exitcode.scope"] = "none"
    pegasus_properties.dump(properties, top_properties)
    plan_log.write_text(
        plan_log.read_text(encoding="utf-8").replace(
            str(runtime_properties), str(top_properties)
        ),
        encoding="utf-8",
    )
    # TODO: Check with scottcal
    properties[DEFAULTSECT]["pegasus.exitcode.scope"] = "all"
    pegasus_properties.dump(properties, runtime_properties)

    transformation_catalog = Path(
        _required_property(
            properties,
            key="pegasus.catalog.transformation.file",
            properties_file=runtime_properties,
        )
    )
    (execution_directory / "metadata.local").write_text(
        f"Site {remote_site}\nTC {transformation_catalog}\n"
        f"{transformation_catalog.read_text(encoding='utf-8')}",
        encoding="utf-8",
    )

    database = run_directory / f"{run_site}.db"
    if database.is_file():
        database.replace(execution_directory / f"{run_site}_{run_id}.db")


def write_full_plan_metadata(
    *, execution_directory: Path, properties_file: Path, remote_site: str
) -> None:
    """Write Full workflow planning catalogs to local metadata.

    Args:
        execution_directory (Path): Workflow submit directory receiving metadata.
        properties_file (Path): Pegasus properties containing catalog locations.
        remote_site (str): Pegasus execution site recorded in local metadata.

    Raises:
        PegasusError: If a required catalog property is absent.
    """
    properties = pegasus_properties.load(properties_file)
    transformation_catalog = Path(
        _required_property(
            properties,
            key="pegasus.catalog.transformation.file",
            properties_file=properties_file,
        )
    )
    site_catalog = Path(
        _required_property(
            properties,
            key="pegasus.catalog.site.file",
            properties_file=properties_file,
        )
    )
    replica_catalog = _required_property(
        properties,
        key="pegasus.catalog.replica.db.url",
        properties_file=properties_file,
    )
    (execution_directory / "metadata.local").write_text(
        f"Site {remote_site}\n"
        f"TC {transformation_catalog}\n"
        f"{transformation_catalog.read_text(encoding='utf-8')}"
        f"SC {site_catalog}\n"
        f"{site_catalog.read_text(encoding='utf-8')}"
        f"RC location {replica_catalog}\n",
        encoding="utf-8",
    )
