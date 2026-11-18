"""Tests for contextual CyberShake exceptions."""

from pathlib import Path

import pytest
from click.testing import CliRunner

import rich_click as click

from cybershake import utils
from cybershake.exc import (
    CommandError,
    CondorError,
    ConfigError,
    CronRunCheckError,
    FullError,
    PegasusError,
    PpError,
    RunTableError,
    SgtError,
    StochError,
)


@pytest.mark.parametrize(
    "error_type",
    [
        ConfigError,
        CronRunCheckError,
        RunTableError,
        PegasusError,
        CondorError,
        SgtError,
        PpError,
        StochError,
        FullError,
    ],
)
def test_command_errors_use_click_rendering(
    error_type: type[CommandError],
) -> None:
    @click.command()
    def command() -> None:
        raise error_type("context failed")

    result = CliRunner().invoke(command)

    assert result.exit_code == 1
    assert "Error" in result.output
    assert "context failed" in result.output


def test_require_run_site_raises_run_table_error(tmp_path: Path) -> None:
    with pytest.raises(RunTableError, match="run_table.txt not found"):
        utils.require_run_site(tmp_path / "run_table.txt", 73)


def test_latest_plan_log_raises_pegasus_error(tmp_path: Path) -> None:
    with pytest.raises(PegasusError, match="No plan log matching"):
        utils.latest_plan_log(tmp_path, "log-plan-*")


def test_find_condor_config_raises_condor_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(utils, "condor_config_candidates", lambda **_kwargs: ())

    with pytest.raises(CondorError, match="No condor config file found"):
        utils.find_condor_config(
            include_shock_default=False,
            allow_condor_lookup=False,
        )
