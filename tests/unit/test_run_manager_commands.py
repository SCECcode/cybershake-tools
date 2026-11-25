"""Tests for run-manager commands used by generated workflows."""

from unittest.mock import ANY

from click.testing import CliRunner
from pytest_mock import MockerFixture

from cybershake.__main__ import cybershake, run_manager
from cybershake.constants import DEFAULT_DB_SERVER


def test_run_manager_commands() -> None:
    assert set(run_manager.commands) == {"edit", "update"}


def test_edit_dispatches_legacy_fields_and_override(mocker: MockerFixture) -> None:
    edit_run = mocker.patch("cybershake.runmanager.edit")

    result = CliRunner().invoke(
        cybershake,
        [
            "run-manager",
            "edit",
            "101",
            "-o",
            "Status=Verified",
            "Comment=contains=equals",
            "Job_ID=NULL",
        ],
        color=True,
    )

    assert result.exit_code == 0
    assert result.output.endswith("\x1b[32m✓\x1b[0m Updated run: run=101\n")
    edit_run.assert_called_once_with(
        run_id=101,
        fields={
            "Status": "Verified",
            "Comment": "contains=equals",
            "Job_ID": None,
        },
        override=True,
        server=DEFAULT_DB_SERVER,
        config=ANY,
    )


def test_edit_rejects_invalid_assignments(mocker: MockerFixture) -> None:
    edit_run = mocker.patch("cybershake.runmanager.edit")

    result = CliRunner().invoke(
        cybershake,
        ["run-manager", "edit", "101", "Unknown=value"],
    )

    assert result.exit_code == 2
    edit_run.assert_not_called()


def test_update_dispatches_transition(mocker: MockerFixture) -> None:
    update_run = mocker.patch("cybershake.runmanager.update")

    result = CliRunner().invoke(
        cybershake,
        ["run-manager", "update", "101", "SGT_START", "SGT_END"],
        color=True,
    )

    assert result.exit_code == 0
    assert result.output.endswith(
        "\x1b[32m✓\x1b[0m Updated run state: run=101 SGT_START->SGT_END\n"
    )
    update_run.assert_called_once_with(
        run_id=101,
        old_state="SGT_START",
        new_state="SGT_END",
        server=DEFAULT_DB_SERVER,
        config=ANY,
    )


def test_update_rejects_unknown_workflow_state(mocker: MockerFixture) -> None:
    update_run = mocker.patch("cybershake.runmanager.update")

    result = CliRunner().invoke(
        cybershake,
        ["run-manager", "update", "101", "UNKNOWN", "SGT_END"],
    )

    assert result.exit_code == 2
    update_run.assert_not_called()
