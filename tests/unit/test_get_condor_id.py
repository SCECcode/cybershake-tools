"""Tests for the Condor job-ID workflow wrapper."""

from __future__ import annotations

import os
import shutil
import stat
import subprocess
from pathlib import Path

import pytest

_SCRIPT = Path(__file__).parents[2] / "scripts/get_condor_id.sh"


@pytest.mark.parametrize("cli_status", [0, 7])
def test_get_condor_id_uses_sibling_cli(tmp_path: Path, cli_status: int) -> None:
    scripts = tmp_path / "bin"
    scripts.mkdir()
    wrapper = scripts / "get_condor_id.sh"
    shutil.copyfile(_SCRIPT, wrapper)
    wrapper.chmod(wrapper.stat().st_mode | stat.S_IEXEC)
    cli = scripts / "cybershake"
    cli.write_text(
        '#!/bin/sh\nprintf "%s\\n" "$@" > "$CAPTURE_ARGS"\nexit "$CLI_STATUS"\n',
        encoding="utf-8",
    )
    cli.chmod(cli.stat().st_mode | stat.S_IEXEC)
    hostname = scripts / "hostname"
    hostname.write_text("#!/bin/sh\necho submit-host\n", encoding="utf-8")
    hostname.chmod(hostname.stat().st_mode | stat.S_IEXEC)
    run_directory = tmp_path / "run"
    run_directory.mkdir()
    (run_directory / "log-run-CyberShake_TEST").write_text(
        "A B submitted to cluster 12345.\n", encoding="utf-8"
    )
    captured = tmp_path / "args.txt"
    environment = {
        **os.environ,
        "PATH": f"{scripts}{os.pathsep}{os.environ['PATH']}",
        "CAPTURE_ARGS": str(captured),
        "CLI_STATUS": str(cli_status),
    }

    result = subprocess.run(  # noqa: S603
        [str(wrapper), str(run_directory), "900008"],
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )

    assert captured.read_text(encoding="utf-8").splitlines() == [
        "run-manager",
        "edit",
        "900008",
        "Job_ID=submit-host:12345",
    ]
    assert result.returncode == (0 if cli_status == 0 else 2)
    if cli_status:
        assert "Unable to update Job ID for run 900008" in result.stdout
