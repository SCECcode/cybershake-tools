"""External plotting boundaries used by the periodic run check."""

from __future__ import annotations

import subprocess
from typing import TYPE_CHECKING

from cybershake import plot_jobs
from cybershake.constants import (
    CURVE_OUTPUT_DIR,
    DEFAULT_JOB_DB_CREDENTIALS,
    LEGACY_OPENSHA_ATTENUATION_XML,
    LEGACY_OPENSHA_ERF_XML,
    LEGACY_OPENSHA_INTERPOLATED_SCRIPT,
    LEGACY_OPENSHA_LOGIN,
    LEGACY_OPENSHA_SCATTER_SCRIPT,
    SSH_EXECUTABLE,
)
from cybershake.exc import PlottingError

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path

    from cybershake.runmanager.schema import IMType


def _execute(command: list[str], *, working_directory: Path | None = None) -> None:
    """Execute one plotting command without a shell.

    Args:
        command (list[str]): Executable and arguments.
        working_directory (Path | None, optional): Command working directory.
            Defaults to the current directory.

    Raises:
        PlottingError: If the command cannot start or exits unsuccessfully.
    """
    try:
        result = subprocess.run(  # noqa: S603
            command,
            cwd=working_directory,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )
    except OSError as error:
        raise PlottingError(f"Unable to run {command[0]}: {error}") from error
    if result.returncode != 0:
        detail = result.stdout.strip() or "no output"
        raise PlottingError(
            f"Plotting command exited with status {result.returncode}: {detail}"
        )


def comparison_curves_exist(
    *,
    site: str,
    intensity_measures: Sequence[IMType],
    curve_directory: Path = CURVE_OUTPUT_DIR,
) -> bool:
    """Return whether every intensity-measure period has a comparison PNG.

    Args:
        site (str): CyberShake site short name.
        intensity_measures (Sequence[IMType]): Stored curve intensity measures.
        curve_directory (Path, optional): Root comparison-curve directory.
            Defaults to ``/home/shock/scottcal/db_products/curves``.

    Returns:
        bool: ``True`` when each expected period appears in a PNG filename.
    """
    try:
        filenames = tuple(
            path.name
            for path in (curve_directory / site).iterdir()
            if ".png" in path.name
        )
    except OSError:
        return False
    for measure in intensity_measures:
        if measure.im_type_value is None:
            return False
        period = round(measure.im_type_value)
        if not any(f"SA_{period}sec" in filename for filename in filenames):
            return False
    return True


def generate_maps() -> tuple[str, ...]:
    """Refresh scatter and interpolated maps, attempting every command.

    Returns:
        tuple[str, ...]: Failure messages. An empty tuple means all commands
            succeeded.
    """
    commands = (
        [SSH_EXECUTABLE, LEGACY_OPENSHA_LOGIN, str(LEGACY_OPENSHA_SCATTER_SCRIPT)],
        [
            SSH_EXECUTABLE,
            LEGACY_OPENSHA_LOGIN,
            str(LEGACY_OPENSHA_INTERPOLATED_SCRIPT),
        ],
        [
            SSH_EXECUTABLE,
            LEGACY_OPENSHA_LOGIN,
            str(LEGACY_OPENSHA_INTERPOLATED_SCRIPT),
            "4",
        ],
    )
    failures: list[str] = []
    for command in commands:
        try:
            _execute(command)
        except PlottingError as error:
            failures.append(str(error))
    return tuple(failures)


def generate_comparison_curves(
    *,
    run_id: int,
    site: str,
    intensity_measures: Sequence[IMType],
    curve_directory: Path = CURVE_OUTPUT_DIR,
) -> None:
    """Generate RotD50 comparison curves for one run.

    Args:
        run_id (int): CyberShake run ID.
        site (str): CyberShake site short name.
        intensity_measures (Sequence[IMType]): Stored curve intensity measures.
        curve_directory (Path, optional): Root comparison-curve directory.
            Defaults to ``/home/shock/scottcal/db_products/curves``.

    Raises:
        PlottingError: If the comparison-curve command fails.
    """
    periods = ",".join(
        str(float(measure.im_type_value))
        for measure in intensity_measures
        if measure.im_type_component == "RotD50" and measure.im_type_value is not None
    )
    arguments = [
        "-R",
        str(run_id),
        "-s",
        site,
        "-n",
        "-o",
        str(curve_directory / site),
        "-ef",
        str(LEGACY_OPENSHA_ERF_XML),
        "-af",
        LEGACY_OPENSHA_ATTENUATION_XML,
        "-t",
        "png,pdf",
        "-cmp",
        "RotD50",
        "-pf",
        str(DEFAULT_JOB_DB_CREDENTIALS),
        "-p",
        periods,
    ]
    if status := plot_jobs.run_plot("curve", arguments):
        raise PlottingError(f"Curve plotter exited with status {status}")
