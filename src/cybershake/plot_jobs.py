"""Launch the legacy OpenSHA plotters from workflow jobs."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING

from cybershake.constants import JAVA_EXECUTABLE, LEGACY_OPENSHA_HOME
from cybershake.exc import PlottingError

if TYPE_CHECKING:
    from collections.abc import Sequence

_CURVE_CLASS = "org.opensha.sha.cybershake.plot.HazardCurvePlotter"
_DISAGG_CLASS = "org.opensha.sha.cybershake.plot.DisaggregationPlotter"
_CLASSPATH_ENV = "CYBERSHAKE_PLOT_CLASSPATH"


def default_classpath() -> str:
    """Return the deployed CyberShake OpenSHA jar or its override."""
    return os.environ.get(_CLASSPATH_ENV) or str(
        LEGACY_OPENSHA_HOME / "opensha-cybershake-all.jar"
    )


def velocity_vs30(path: Path) -> str:
    """Read the Vs30 value written by the broadband velocity query."""
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as error:
        raise PlottingError(f"Unable to read {path}: {error}") from error
    for line in lines:
        if "Vs30" in line and "=" in line:
            value = line.split("=", 1)[1].strip()
            if value:
                return value
    raise PlottingError(f"Vs30 not found in {path}")


def run_plot(
    plot: str,
    arguments: Sequence[str],
    *,
    velocity_file: Path | None = None,
    classpath: str | None = None,
    java: str | None = None,
) -> int:
    """Run a curve or disaggregation plotter and return its exit status."""
    java_home = os.environ.get("JAVA_HOME")
    executable = java or (
        str(Path(java_home) / "bin/java") if java_home else JAVA_EXECUTABLE
    )
    class_name, max_heap = {
        "curve": (_CURVE_CLASS, "2G"),
        "disagg": (_DISAGG_CLASS, "12G"),
    }[plot]
    command = [
        executable,
        "-Xms512M",
        f"-Xmx{max_heap}",
        "-cp",
        classpath or default_classpath(),
        "-Dcybershake.db.host=moment",
        class_name,
    ]
    if velocity_file is not None:
        command.extend(("--force-vs30", velocity_vs30(velocity_file)))
    command.extend(arguments)
    try:
        return subprocess.run(  # noqa: S603
            command, cwd=LEGACY_OPENSHA_HOME, check=False
        ).returncode
    except OSError as error:
        raise PlottingError(f"Unable to run {executable}: {error}") from error
