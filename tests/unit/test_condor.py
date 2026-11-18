"""Tests for the external HTCondor command boundaries."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from cybershake import condor
from cybershake.exc import CondorError

if TYPE_CHECKING:
    from pathlib import Path

    from pytest_mock import MockerFixture


def _executable(path: Path, body: str) -> Path:
    path.write_text(f"#!/bin/sh\nset -eu\n{body}", encoding="utf-8")
    path.chmod(0o755)
    return path


def test_local_config_file_uses_reported_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_file = tmp_path / "local config"
    config_file.write_text("SETTING = value\n", encoding="utf-8")
    executable = _executable(
        tmp_path / "condor_config_val",
        'printf "%s\\n" "$FAKE_CONFIG"\n',
    )
    monkeypatch.setattr(condor, "CONDOR_CONFIG_VAL_EXECUTABLE", str(executable))
    monkeypatch.setenv("FAKE_CONFIG", str(config_file))

    assert condor.local_config_file() == config_file


@pytest.mark.parametrize(
    ("body", "message"),
    [
        ("exit 7\n", "failed after 1 attempt"),
        ("true\n", "returned 0 paths"),
        ("printf 'one\\ntwo\\n'\n", "returned 2 paths"),
        ("printf '/missing/config\\n'\n", "is not a file"),
    ],
)
def test_local_config_file_rejects_command_and_output_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    body: str,
    message: str,
) -> None:
    executable = _executable(tmp_path / "condor_config_val", body)
    monkeypatch.setattr(condor, "CONDOR_CONFIG_VAL_EXECUTABLE", str(executable))

    with pytest.raises(CondorError, match=message):
        condor.local_config_file()


def test_local_config_file_rejects_missing_executable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    executable = tmp_path / "missing-condor-config-val"
    monkeypatch.setattr(condor, "CONDOR_CONFIG_VAL_EXECUTABLE", str(executable))

    with pytest.raises(CondorError, match="failed after 1 attempt"):
        condor.local_config_file()


def test_active_job_ids_uses_machine_readable_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    arguments = tmp_path / "arguments"
    executable = _executable(
        tmp_path / "condor_q",
        'printf "%s\\n" "$@" > "$FAKE_ARGUMENTS"\nprintf \'123\\n\\n123\\n456\\n\'\n',
    )
    monkeypatch.setattr(condor, "CONDOR_Q_EXECUTABLE", str(executable))
    monkeypatch.setenv("FAKE_ARGUMENTS", str(arguments))

    assert condor.active_job_ids() == frozenset({"123", "456"})
    assert arguments.read_text(encoding="utf-8").splitlines() == [
        "-autoformat",
        "ClusterId",
    ]


def test_active_job_ids_accepts_an_empty_queue(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    executable = _executable(tmp_path / "condor_q", "true\n")
    monkeypatch.setattr(condor, "CONDOR_Q_EXECUTABLE", str(executable))

    assert condor.active_job_ids() == frozenset()


def test_active_job_ids_retries_transient_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mocker: MockerFixture,
) -> None:
    attempts = tmp_path / "attempts"
    executable = _executable(
        tmp_path / "condor_q",
        'attempt=$(cat "$FAKE_ATTEMPTS" 2>/dev/null || printf 0)\n'
        "attempt=$((attempt + 1))\n"
        'printf "%s" "$attempt" > "$FAKE_ATTEMPTS"\n'
        'if test "$attempt" -lt 3; then\n'
        "  printf 'scheduler unavailable\\n'\n"
        "  exit 1\n"
        "fi\n"
        "printf '789\\n'\n",
    )
    monkeypatch.setattr(condor, "CONDOR_Q_EXECUTABLE", str(executable))
    monkeypatch.setenv("FAKE_ATTEMPTS", str(attempts))
    sleep = mocker.patch("cybershake.condor.time.sleep")

    assert condor.active_job_ids() == frozenset({"789"})
    assert attempts.read_text(encoding="utf-8") == "3"
    assert sleep.call_count == 2


def test_active_job_ids_rejects_failures_and_malformed_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mocker: MockerFixture,
) -> None:
    executable = _executable(
        tmp_path / "condor_q",
        "printf 'scheduler unavailable\\n'\nexit 1\n",
    )
    monkeypatch.setattr(condor, "CONDOR_Q_EXECUTABLE", str(executable))
    sleep = mocker.patch("cybershake.condor.time.sleep")

    with pytest.raises(CondorError, match="failed after 3 attempt"):
        condor.active_job_ids()
    assert sleep.call_count == 2

    executable.write_text("#!/bin/sh\nprintf 'not-a-cluster\\n'\n", encoding="utf-8")
    with pytest.raises(CondorError, match="Malformed HTCondor cluster ID"):
        condor.active_job_ids()
