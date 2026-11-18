"""HTCondor integration boundaries."""

from __future__ import annotations

import subprocess
import time
from pathlib import Path

from cybershake.constants import (
    CONDOR_CONFIG_VAL_EXECUTABLE,
    CONDOR_Q_EXECUTABLE,
    CONDOR_QUERY_ATTEMPTS,
    CONDOR_QUERY_RETRY_SECONDS,
)
from cybershake.exc import CondorError


def _execute(command: list[str], *, attempts: int, operation: str) -> str:
    """Run an HTCondor command, retrying transient command failures.

    Args:
        command (list[str]): Executable and arguments, without a shell.
        attempts (int): Maximum number of command attempts.
        operation (str): Operation name included in failure messages.

    Returns:
        str: Combined standard output and error from the successful attempt.

    Raises:
        CondorError: If every attempt fails to start or exits unsuccessfully.
    """
    detail = "no output"
    for attempt in range(1, attempts + 1):
        try:
            result = subprocess.run(  # noqa: S603
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                check=False,
            )
        except OSError as error:
            detail = str(error)
        else:
            if result.returncode == 0:
                return result.stdout
            detail = result.stdout.strip() or f"exit status {result.returncode}"
        if attempt < attempts:
            time.sleep(CONDOR_QUERY_RETRY_SECONDS)

    msg = f"HTCondor {operation} failed after {attempts} attempt(s): {detail}"
    raise CondorError(msg)


def local_config_file() -> Path:
    """Return the local HTCondor configuration file reported by HTCondor.

    Returns:
        Path: Local HTCondor configuration file.

    Raises:
        CondorError: If HTCondor fails, returns malformed output, or reports a
            path that is not a file.
    """
    output = _execute(
        [CONDOR_CONFIG_VAL_EXECUTABLE, "LOCAL_CONFIG_FILE"],
        attempts=1,
        operation="configuration lookup",
    )
    lines = tuple(line.strip() for line in output.splitlines() if line.strip())
    if len(lines) != 1:
        msg = f"HTCondor configuration lookup returned {len(lines)} paths"
        raise CondorError(msg)
    config_file = Path(lines[0])
    if not config_file.is_file():
        msg = f"HTCondor local configuration is not a file: {config_file}"
        raise CondorError(msg)
    return config_file


def active_job_ids() -> frozenset[str]:
    """Return cluster IDs for jobs visible in the local HTCondor queue.

    The queue query is attempted three times, with a 30-second delay between
    failures, matching the legacy cron check's scheduler-load handling.

    Returns:
        frozenset[str]: Active numeric cluster IDs. An empty queue returns an
            empty set.

    Raises:
        CondorError: If HTCondor fails on all three attempts or returns a
            malformed cluster ID.
    """
    output = _execute(
        [CONDOR_Q_EXECUTABLE, "-autoformat", "ClusterId"],
        attempts=CONDOR_QUERY_ATTEMPTS,
        operation="queue query",
    )
    job_ids: set[str] = set()
    for line_number, line in enumerate(output.splitlines(), start=1):
        job_id = line.strip()
        if not job_id:
            continue
        if not job_id.isdecimal():
            msg = (
                f"Malformed HTCondor cluster ID on output line {line_number}: {job_id}"
            )
            raise CondorError(msg)
        job_ids.add(job_id)
    return frozenset(job_ids)
