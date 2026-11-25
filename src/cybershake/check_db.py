"""Run the packaged database-product checker through Java source mode."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from tempfile import TemporaryDirectory

from cybershake.constants import (
    CHECK_DB_CLASSPATH_ENV,
    CHECK_DB_CONNECT_SOURCE,
    CHECK_DB_JARS,
    CHECK_DB_SOURCE,
    DB_CREDENTIALS_ENV,
    DEFAULT_DB_USER,
    DEFAULT_JOB_DB_CREDENTIALS,
    JAVA_EXECUTABLE,
    OPENSHA_DIR,
)
from cybershake.exc import CheckDBError


def default_classpath() -> str:
    """Locate the packaged JDBC and CLI dependencies."""
    return os.environ.get(CHECK_DB_CLASSPATH_ENV) or os.pathsep.join(
        (
            str(CHECK_DB_JARS / "*"),
            str(OPENSHA_DIR / "jars/opensha-cybershake-all.jar"),
        )
    )


def check_db_site(
    *,
    run_id: int,
    output: Path,
    component: str,
    periods: str | None,
    type_ids: str | None,
    server: str,
    rupture_list: Path | None,
    classpath: str | None = None,
    java: str | None = None,
    credentials: Path | None = None,
) -> int:
    """Return the Java checker's exit status, preserving its output streams."""
    java_home = os.environ.get("JAVA_HOME")
    executable = java or (
        str(Path(java_home) / "bin/java") if java_home else JAVA_EXECUTABLE
    )
    compiler = str(Path(java_home) / "bin/javac") if java_home else "javac"
    environment = os.environ.copy()
    if not server.startswith("sqlite:"):
        credential_path = credentials or Path(
            environment.get(DB_CREDENTIALS_ENV, DEFAULT_JOB_DB_CREDENTIALS)
        )
        try:
            credential_text = credential_path.read_text(encoding="utf-8").strip()
        except OSError as error:
            raise CheckDBError(
                f"Unable to read database credentials from {credential_path}: {error}"
            ) from error
        username, separator, password = credential_text.partition(":")
        if not separator:
            username, password = DEFAULT_DB_USER, username
        if not username or not password:
            raise CheckDBError(f"Invalid database credentials in {credential_path}")
        environment["CYBERSHAKE_DB_USER"] = username
        environment["CYBERSHAKE_DB_PASSWORD"] = password
    try:
        with TemporaryDirectory() as classes:
            compile_result = subprocess.run(  # noqa: S603
                [compiler, "-d", classes, str(CHECK_DB_CONNECT_SOURCE)],
                check=False,
            )
            if compile_result.returncode:
                return compile_result.returncode
            command = [
                executable,
                "-cp",
                os.pathsep.join((classes, classpath or default_classpath())),
                str(CHECK_DB_SOURCE),
                "-r",
                str(run_id),
                "-o",
                str(output),
                "-c",
                component,
                "-s",
                server,
            ]
            if periods is not None:
                command.extend(("-p", periods))
            if type_ids is not None:
                command.extend(("-t", type_ids))
            if rupture_list is not None:
                command.extend(("-rl", str(rupture_list)))
            return subprocess.run(  # noqa: S603
                command, env=environment, check=False
            ).returncode
    except OSError as error:
        raise CheckDBError(f"Unable to run {executable}: {error}") from error
