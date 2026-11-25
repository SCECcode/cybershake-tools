"""Pegasus command integration boundaries."""

from __future__ import annotations

import os
import re
import shlex
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING

from cybershake.constants import PEGASUS_PLAN_EXECUTABLE, PEGASUS_RUN_EXECUTABLE
from cybershake.exc import PegasusError

if TYPE_CHECKING:
    from collections.abc import Mapping


def _execute(
    command: list[str],
    *,
    working_directory: Path,
    log_file: Path,
    environment: Mapping[str, str] | None,
    operation: str,
) -> str:
    """Execute a Pegasus command and persist its combined output.

    Args:
        command (list[str]): Command and arguments to execute without a shell.
        working_directory (Path): Directory from which to execute the command.
        log_file (Path): File receiving combined standard output and error.
        environment (Mapping[str, str] | None): Environment overrides merged with
            the current process, or ``None`` to inherit it unchanged.
        operation (str): Operation name used in failure messages.

    Returns:
        str: Combined command output.

    Raises:
        PegasusError: If the command cannot run, its output cannot be logged, or
            it exits unsuccessfully.
    """
    command_environment = None
    if environment is not None:
        command_environment = dict(os.environ)
        command_environment.update(environment)
    try:
        result = subprocess.run(  # noqa: S603
            command,
            cwd=working_directory,
            env=command_environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )
    except OSError as error:
        msg = f"Unable to run {command[0]}: {error}"
        raise PegasusError(msg) from error

    output = result.stdout
    try:
        log_file.write_text(output, encoding="utf-8")
    except OSError as error:
        msg = f"Unable to write Pegasus {operation} log {log_file}: {error}"
        raise PegasusError(msg) from error
    if result.returncode != 0:
        detail = output.strip() or "no output"
        msg = (
            f"Pegasus {operation} failed with exit status {result.returncode}: {detail}"
        )
        raise PegasusError(msg)
    return output


def _parse_run_command(output: str, *, source: Path) -> list[str]:
    """Extract the single safe ``pegasus-run`` command from planner output.

    Args:
        output (str): Planner output containing the deferred submission command.
        source (Path): Log path used to identify malformed output.

    Returns:
        list[str]: Parsed command beginning with the configured Pegasus runner.

    Raises:
        PegasusError: If the output has no unique, valid submission command.
    """
    executable_name = Path(PEGASUS_RUN_EXECUTABLE).name
    matches: list[list[str]] = []
    for line_number, line in enumerate(output.splitlines(), start=1):
        if executable_name not in line:
            continue
        try:
            tokens = shlex.split(line)
        except ValueError as error:
            msg = f"Malformed pegasus-run command in {source} on line {line_number}"
            raise PegasusError(msg) from error
        for index, token in enumerate(tokens):
            if Path(token).name == executable_name:
                matches.append(tokens[index:])
                break

    if not matches:
        msg = f"No pegasus-run command found in {source}"
        raise PegasusError(msg)
    if len(matches) != 1:
        msg = f"Expected one pegasus-run command in {source}, found {len(matches)}"
        raise PegasusError(msg)
    command = matches[0]
    if len(command) < 2 or command[-1].startswith("-"):
        msg = f"Pegasus submit directory missing from command in {source}"
        raise PegasusError(msg)
    command[0] = PEGASUS_RUN_EXECUTABLE
    return command


def _submit_directory(command: list[str], *, relative_to: Path) -> Path:
    """Resolve and validate the final submit-directory command argument.

    Args:
        command (list[str]): Parsed ``pegasus-run`` command.
        relative_to (Path): Base for a relative submit-directory argument.

    Returns:
        Path: Existing Pegasus submit directory.

    Raises:
        PegasusError: If the submit directory does not exist.
    """
    submit_directory = Path(command[-1])
    if not submit_directory.is_absolute():
        submit_directory = relative_to / submit_directory
    submit_directory = submit_directory.resolve()
    if not submit_directory.is_dir():
        msg = f"Pegasus submit directory not found: {submit_directory}"
        raise PegasusError(msg)
    return submit_directory


def plan(
    *,
    workflow_file: Path,
    working_directory: Path,
    properties_file: Path,
    plan_log: Path,
    execution_sites: tuple[str, ...],
    output_site: str,
    submit_directory: Path,
    rescue: int,
    cleanup: str,
    force: bool,
    cluster: str | None,
    output_directory: Path | None,
    verbosity: int,
    environment: Mapping[str, str] | None,
) -> Path:
    """Plan a workflow and return its generated submit directory.

    Args:
        workflow_file (Path): Workflow file to plan.
        working_directory (Path): Directory from which to run the planner.
        properties_file (Path): Pegasus properties used for planning.
        plan_log (Path): File that receives planner output.
        execution_sites (tuple[str, ...]): Pegasus sites on which jobs may execute.
        output_site (str): Pegasus site that receives workflow outputs.
        submit_directory (Path): Base directory for generated submit files.
        rescue (int): Maximum number of rescue attempts.
        cleanup (str): Pegasus cleanup strategy.
        force (bool): Whether to bypass planner safety checks.
        cluster (str | None): Clustering strategy, or ``None`` for the default.
        output_directory (Path | None): Output directory passed to Pegasus, if any.
        verbosity (int): Pegasus planner verbosity level.
        environment (Mapping[str, str] | None): Environment variables supplied to
            the planner, if any.

    Returns:
        Path: Directory containing the generated submit files.

    Raises:
        PegasusError: If planning fails or its submit directory cannot be parsed.
    """
    resolved_working_directory = working_directory.resolve()
    resolved_log = plan_log.resolve()
    resolved_workflow = workflow_file.resolve()
    try:
        workflow_argument = resolved_workflow.relative_to(resolved_working_directory)
    except ValueError:
        workflow_argument = resolved_workflow
    command = [PEGASUS_PLAN_EXECUTABLE]
    if verbosity > 0:
        command.append("-" + ("v" * verbosity))
    command.extend(
        (
            "--conf",
            str(properties_file.resolve()),
            "--rescue",
            str(rescue),
            "-s",
            ",".join(execution_sites),
            "--output-site",
            output_site,
            "--dir",
            str(submit_directory),
        )
    )
    if force:
        command.append("-f")
    if cluster is not None:
        command.extend(("--cluster", cluster))
    command.extend(("--cleanup", cleanup))
    if output_directory is not None:
        command.extend(("--output-dir", str(output_directory)))
    command.append(str(workflow_argument))

    output = _execute(
        command,
        working_directory=resolved_working_directory,
        log_file=resolved_log,
        environment=environment,
        operation="planning",
    )
    run_command = _parse_run_command(output, source=resolved_log)
    return _submit_directory(run_command, relative_to=resolved_working_directory)


def run(*, plan_log: Path, run_log: Path) -> tuple[str, Path]:
    """Submit a planned workflow.

    Args:
        plan_log (Path): Planner log from which to locate the submit directory.
        run_log (Path): File that receives submission output.

    Returns:
        tuple[str, Path]: Pegasus cluster ID and workflow submit directory.

    Raises:
        PegasusError: If the plan log is unavailable or malformed, submission
            fails, or its cluster ID cannot be parsed.
    """
    resolved_plan_log = plan_log.resolve()
    try:
        plan_output = resolved_plan_log.read_text(encoding="utf-8")
    except OSError as error:
        msg = f"Unable to read Pegasus plan log {resolved_plan_log}: {error}"
        raise PegasusError(msg) from error
    command = _parse_run_command(plan_output, source=resolved_plan_log)
    execution_directory = _submit_directory(
        command,
        relative_to=resolved_plan_log.parent,
    )
    output = _execute(
        command,
        working_directory=resolved_plan_log.parent,
        log_file=run_log.resolve(),
        environment=None,
        operation="submission",
    )
    match = re.search(
        r"\bsubmitted to cluster\s+(\d+)\.?",
        output,
        flags=re.IGNORECASE,
    )
    if match is None:
        msg = f"Pegasus cluster ID not found in {run_log.resolve()}"
        raise PegasusError(msg)
    return match.group(1), execution_directory
