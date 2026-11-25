"""Vs30 lookup through the small OpenSHA-based Java CLI in ``share/opensha``.

The CLI runs from its source file (Java 11+ compiles it in memory), so nothing
has to be built or written next to the installed package.
"""

from __future__ import annotations

import json
import os
import subprocess
from dataclasses import dataclass

from cybershake.constants import (
    CLASSPATH_ENV,
    CLI_SOURCE,
    JAVA_EXECUTABLE,
    OPENSHA_DIR,
)


class Vs30Error(Exception):
    """The Vs30 lookup failed."""


@dataclass(frozen=True)
class Vs30:
    """A Vs30 value (m/s) and the model it came from."""

    value: float
    source: str


def default_classpath() -> str:
    """Return the Java classpath for the CLI.

    ``$CYBERSHAKE_VS30_CLASSPATH`` if set, else the OpenSHA jar in
    ``share/opensha/jars``.

    Returns:
        str: Configured Java classpath, or the packaged OpenSHA jar wildcard.
    """
    return os.environ.get(CLASSPATH_ENV) or str(OPENSHA_DIR / "jars" / "*")


def lookup_vs30(
    lat: float,
    lon: float,
    *,
    classpath: str | None = None,
    java: str = JAVA_EXECUTABLE,
) -> Vs30:
    """Look up Vs30 at a location.

    Uses Thompson et al. (2020), falling back to the Wald & Allen topographic
    slope where Thompson has no value.

    Args:
        lat (float): Latitude in degrees.
        lon (float): Longitude in degrees.
        classpath (str | None, optional): Java classpath; see
            ``default_classpath``. Defaults to ``None``.
        java (str, optional): Java executable. Defaults to ``java``
            (``JAVA_EXECUTABLE``).

    Returns:
        Vs30: Vs30 value and its source.

    Raises:
        Vs30Error: If Java fails or prints something unexpected.
    """
    cmd = [
        java,
        "-cp",
        classpath or default_classpath(),
        str(CLI_SOURCE),
        str(lat),
        str(lon),
    ]
    try:
        result = subprocess.run(  # noqa: S603
            cmd, capture_output=True, text=True, check=False
        )
    except OSError as e:
        msg = f"Unable to run {java}: {e}"
        raise Vs30Error(msg) from e
    if result.returncode != 0:
        msg = f"Vs30 lookup failed ({result.returncode}): {result.stderr.strip()}"
        raise Vs30Error(msg)
    try:
        data = json.loads(result.stdout.strip().splitlines()[-1])
        return Vs30(float(data["vs30"]), str(data["source"]))
    except (IndexError, KeyError, ValueError) as e:
        msg = f"Unexpected Vs30 CLI output: {result.stdout!r}"
        raise Vs30Error(msg) from e
