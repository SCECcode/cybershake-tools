"""Tests for the shell-compatible cron wrapper command."""

import signal
from pathlib import Path
from unittest.mock import call

import pytest
from click.testing import CliRunner
from pytest_mock import MockerFixture

from cybershake.__main__ import cybershake
from cybershake.constants import CRON_RUN_CHECK_TIMEOUT_SECONDS
from cybershake.exc import CronRunCheckTimeoutError


@pytest.mark.parametrize("mode", ["MASTER", "SECONDARY"])
def test_cron_run_check_forwards_mode_with_timeout(
    mode: str, mocker: MockerFixture
) -> None:
    check = mocker.patch("cybershake.runmanager.cron_run_check")
    alarm = mocker.patch("cybershake.__main__.signal.alarm")
    set_handler = mocker.patch(
        "cybershake.__main__.signal.signal", return_value=signal.SIG_DFL
    )

    result = CliRunner().invoke(cybershake, ["cron-run-check", mode])

    assert result.exit_code == 0
    assert result.output.endswith(f"✓ Completed run check: mode={mode}\n")
    check.assert_called_once_with(mode=mode)
    assert alarm.call_args_list == [call(CRON_RUN_CHECK_TIMEOUT_SECONDS), call(0)]
    assert set_handler.call_count == 2


def test_cron_run_check_removes_lock_after_timeout(
    tmp_path: Path, mocker: MockerFixture
) -> None:
    lock_file = tmp_path / "lock"
    lock_file.touch()
    mocker.patch(
        "cybershake.runmanager.cron_run_check",
        side_effect=CronRunCheckTimeoutError,
    )
    mocker.patch("cybershake.__main__.signal.alarm")
    mocker.patch("cybershake.__main__.signal.signal", return_value=signal.SIG_DFL)
    mocker.patch("cybershake.__main__.LEGACY_CRON_LOCK_FILE", lock_file)

    result = CliRunner().invoke(cybershake, ["cron-run-check", "MASTER"])

    assert result.exit_code == 0
    assert "Cron check didn't terminate naturally, so it was killed." in result.output
    assert not lock_file.exists()


def test_cron_run_check_requires_mode(mocker: MockerFixture) -> None:
    check = mocker.patch("cybershake.runmanager.cron_run_check")

    result = CliRunner().invoke(cybershake, ["cron-run-check"])

    assert result.exit_code == 2
    check.assert_not_called()


def test_cron_run_check_rejects_unknown_mode(mocker: MockerFixture) -> None:
    check = mocker.patch("cybershake.runmanager.cron_run_check")

    result = CliRunner().invoke(cybershake, ["cron-run-check", "OTHER"])

    assert result.exit_code == 2
    check.assert_not_called()


def test_cron_run_check_propagates_non_timeout_failure(
    mocker: MockerFixture,
) -> None:
    error = RuntimeError("check failed")
    mocker.patch("cybershake.runmanager.cron_run_check", side_effect=error)
    alarm = mocker.patch("cybershake.__main__.signal.alarm")
    mocker.patch("cybershake.__main__.signal.signal", return_value=signal.SIG_DFL)

    result = CliRunner().invoke(cybershake, ["cron-run-check", "MASTER"])

    assert result.exit_code == 1
    assert result.exception is error
    assert alarm.call_args_list == [call(CRON_RUN_CHECK_TIMEOUT_SECONDS), call(0)]
