"""Tests for shell-compatible workflow submission commands."""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import ANY, Mock, call

import pytest
from click.testing import CliRunner

from cybershake.__main__ import cybershake
from cybershake.constants import (
    DEFAULT_DB_SERVER,
    PP_STARTED_STATE,
    SGT_GENERATED_STATE,
    SGT_STARTED_STATE,
)
from cybershake.exc import RunManagerError

if TYPE_CHECKING:
    from pytest_mock import MockerFixture


def _prepare_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    directory_name: str,
    run_id: int = 73,
    plan_names: tuple[str, ...],
) -> tuple[Path, Path]:
    monkeypatch.chdir(tmp_path)
    root_directory = Path(directory_name)
    run_directory = root_directory / f"run_{run_id}"
    run_directory.mkdir(parents=True)
    (root_directory / "run_table.txt").write_text(
        f"{run_id} TEST 64 8 10 15 1.0 2.0\n", encoding="utf-8"
    )
    for index, name in enumerate(plan_names, start=1):
        plan_log = run_directory / name
        plan_log.write_text("pegasus-run placeholder\n", encoding="utf-8")
        os.utime(plan_log, (index, index))
    return root_directory, run_directory


def _stub_submission(
    mocker: MockerFixture, execution_directory: Path
) -> tuple[Mock, Mock, Mock]:
    valid_run = mocker.patch("cybershake.runmanager.valid")
    edit_run = mocker.patch("cybershake.runmanager.edit")
    submit = mocker.patch(
        "cybershake.pegasus.run", return_value=("456", execution_directory)
    )
    mocker.patch("cybershake.utils.socket.gethostname", return_value="submit.test")
    execution_directory.mkdir()
    return valid_run, edit_run, submit


def test_sgt_run_migrates_shell_orchestration(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
) -> None:
    _root_directory, run_directory = _prepare_run(
        tmp_path,
        monkeypatch,
        directory_name="TEST_SGT_dax",
        plan_names=(
            "log-plan-CyberShake_SGT_TEST.dax-old",
            "log-plan-CyberShake_SGT_TEST.dax-new",
        ),
    )
    execution_directory = tmp_path / "execution"
    valid_run, edit_run, submit = _stub_submission(mocker, execution_directory)

    result = CliRunner().invoke(
        cybershake, ["sgt-run", "-r", "-n", "analyst", "TEST", "73"]
    )

    assert result.exit_code == 0
    assert result.output.endswith(
        "✓ Submitted SGT workflow: run=73 job=submit:456 "
        f"submit_dir={execution_directory}\n"
    )
    valid_run.assert_called_once_with(
        run_id=73,
        site="TEST",
        stage="SGT_PLAN",
        server=DEFAULT_DB_SERVER,
        config=ANY,
    )
    submit.assert_called_once_with(
        plan_log=run_directory / "log-plan-CyberShake_SGT_TEST.dax-new",
        run_log=run_directory / "log-run-CyberShake_SGT_TEST",
    )
    edit_run.assert_called_once_with(
        run_id=73,
        fields={
            "Status": SGT_STARTED_STATE,
            "Comment": "SGT workflow submitted",
            "Job_ID": "submit:456",
            "Submit_Dir": str(execution_directory),
            "Notify_User": "analyst",
        },
        server=DEFAULT_DB_SERVER,
        config=ANY,
    )


def test_pp_run_migrates_shell_orchestration_and_condor_metadata(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
) -> None:
    _root_directory, run_directory = _prepare_run(
        tmp_path,
        monkeypatch,
        directory_name="TEST_PP_dax",
        plan_names=("log-plan-CyberShake_TEST.dax-frontier",),
    )
    execution_directory = tmp_path / "execution"
    valid_run, edit_run, submit = _stub_submission(mocker, execution_directory)
    condor_base = tmp_path / "condor_config"
    condor_config = Path(f"{condor_base}.local")
    condor_config.write_text("LOCAL_CONFIG = value\n", encoding="utf-8")
    mocker.patch(
        "cybershake.utils.condor_config_candidates",
        return_value=(condor_config,),
    )

    result = CliRunner().invoke(cybershake, ["pp-run", "TEST", "73"])

    assert result.exit_code == 0
    assert result.output.endswith(
        "✓ Submitted PP workflow: run=73 job=submit:456 "
        f"submit_dir={execution_directory}\n"
    )
    valid_run.assert_called_once_with(
        run_id=73,
        site="TEST",
        stage="PP_PLAN",
        server=DEFAULT_DB_SERVER,
        config=ANY,
    )
    submit.assert_called_once_with(
        plan_log=run_directory / "log-plan-CyberShake_TEST.dax-frontier",
        run_log=run_directory / "log-run-CyberShake_PP_TEST.dax",
    )
    assert (execution_directory / "metadata.local").read_text() == (
        f"Condor config local {condor_config}\nLOCAL_CONFIG = value\n"
    )
    edit_run.assert_called_once_with(
        run_id=73,
        fields={
            "Status": SGT_GENERATED_STATE,
            "Comment": "PP workflow submitted",
            "Job_ID": "submit:456",
            "Submit_Dir": str(execution_directory),
            "Notify_User": None,
        },
        server=DEFAULT_DB_SERVER,
        config=ANY,
    )


def test_stoch_run_uses_condor_lookup_placeholder(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
) -> None:
    _root_directory, run_directory = _prepare_run(
        tmp_path,
        monkeypatch,
        directory_name="TEST_Stoch_dax",
        plan_names=("log-plan-CyberShake_Stoch_TEST_top.dax-frontier",),
    )
    execution_directory = tmp_path / "execution"
    valid_run, edit_run, submit = _stub_submission(mocker, execution_directory)
    condor_config = tmp_path / "resolved-condor-config"
    condor_config.write_text("SETTING = value\n", encoding="utf-8")
    mocker.patch("cybershake.utils.condor_config_candidates", return_value=())
    config_lookup = mocker.patch(
        "cybershake.condor.local_config_file", return_value=condor_config
    )

    result = CliRunner().invoke(
        cybershake, ["stoch-run", "-r", "-n", "analyst", "TEST", "73"]
    )

    assert result.exit_code == 0
    assert result.output.endswith(
        "✓ Submitted stochastic workflow: run=73 job=submit:456 "
        f"submit_dir={execution_directory}\n"
    )
    valid_run.assert_called_once_with(
        run_id=73,
        site="TEST",
        stage="PP_PLAN",
        server=DEFAULT_DB_SERVER,
        config=ANY,
    )
    submit.assert_called_once_with(
        plan_log=run_directory / "log-plan-CyberShake_Stoch_TEST_top.dax-frontier",
        run_log=run_directory / "log-run-CyberShake_Stoch_TEST.dax",
    )
    config_lookup.assert_called_once_with()
    assert (execution_directory / "metadata.local").read_text() == (
        f"Condor config local {condor_config}\nSETTING = value\n"
    )
    edit_run.assert_called_once_with(
        run_id=73,
        fields={
            "Status": PP_STARTED_STATE,
            "Job_ID": "submit:456",
            "Submit_Dir": str(execution_directory),
            "Notify_User": "analyst",
        },
        server=DEFAULT_DB_SERVER,
        config=ANY,
    )


def test_full_run_falls_back_to_pp_and_updates_linked_run_after_edit_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
) -> None:
    _root_directory, run_directory = _prepare_run(
        tmp_path,
        monkeypatch,
        directory_name="TEST_Integrated_dax",
        plan_names=("log-plan-CyberShake_Integrated_TEST.dax-frontier",),
    )
    (run_directory / "linked_bb_run_id.txt").write_text("74\n", encoding="utf-8")
    execution_directory = tmp_path / "execution"
    valid_run, edit_run, submit = _stub_submission(mocker, execution_directory)
    valid_run.side_effect = (RunManagerError, None, None)
    edit_run.side_effect = (RunManagerError, None)

    result = CliRunner().invoke(cybershake, ["full-run", "-n", "analyst", "TEST", "73"])

    assert result.exit_code == 0
    assert valid_run.call_args_list == [
        call(
            run_id=73,
            site="TEST",
            stage="SGT_PLAN",
            server=DEFAULT_DB_SERVER,
            config=ANY,
        ),
        call(
            run_id=73,
            site="TEST",
            stage="PP_PLAN",
            server=DEFAULT_DB_SERVER,
            config=ANY,
        ),
        call(
            run_id=74,
            site="TEST",
            stage="SGT_PLAN",
            server=DEFAULT_DB_SERVER,
            config=ANY,
        ),
    ]
    submit.assert_called_once_with(
        plan_log=run_directory / "log-plan-CyberShake_Integrated_TEST.dax-frontier",
        run_log=run_directory / "log-run-CyberShake_Integrated_TEST",
    )
    expected_fields = {
        "Status": PP_STARTED_STATE,
        "Comment": "workflow submitted",
        "Job_ID": "submit:456",
        "Submit_Dir": str(execution_directory),
        "Notify_User": "analyst",
    }
    assert edit_run.call_args_list == [
        call(
            run_id=73,
            fields=expected_fields,
            server=DEFAULT_DB_SERVER,
            config=ANY,
        ),
        call(
            run_id=74,
            fields=expected_fields,
            server=DEFAULT_DB_SERVER,
            config=ANY,
        ),
    ]
    assert "Unable to update" in result.output
    assert (
        "✓ Submitted full workflow: run=73 broadband_run=74 job=submit:456 "
        f"submit_dir={execution_directory}\n"
    ) in result.output


def test_full_run_propagates_linked_run_edit_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
) -> None:
    _root_directory, run_directory = _prepare_run(
        tmp_path,
        monkeypatch,
        directory_name="TEST_Integrated_dax",
        plan_names=("log-plan-CyberShake_Integrated_TEST.dax-frontier",),
    )
    (run_directory / "linked_bb_run_id.txt").write_text("74\n", encoding="utf-8")
    execution_directory = tmp_path / "execution"
    _valid_run, edit_run, _submit = _stub_submission(mocker, execution_directory)
    edit_run.side_effect = (None, RunManagerError)

    result = CliRunner().invoke(cybershake, ["full-run", "TEST", "73"])

    assert result.exit_code != 0
    assert isinstance(result.exception, RunManagerError)
    assert [edit_call.kwargs["run_id"] for edit_call in edit_run.call_args_list] == [
        73,
        74,
    ]


@pytest.mark.parametrize(
    ("command", "directory_name"),
    [
        ("sgt-run", "TEST_SGT_dax"),
        ("pp-run", "TEST_PP_dax"),
        ("stoch-run", "TEST_Stoch_dax"),
        ("full-run", "TEST_Integrated_dax"),
    ],
)
def test_run_commands_require_the_run_table(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    command: str,
    directory_name: str,
) -> None:
    monkeypatch.chdir(tmp_path)

    result = CliRunner().invoke(cybershake, [command, "TEST", "73"])

    assert result.exit_code != 0
    assert f"{directory_name}/run_table.txt not found" in result.output


def test_run_commands_do_not_expose_multi_site_timestamp_mode() -> None:
    result = CliRunner().invoke(cybershake, ["full-run", "--help"])

    assert result.exit_code == 0
    assert "--timestamp" not in result.output
    assert "--restart" in result.output
    assert "--notify-user" in result.output
    assert "SITE RUN_ID" in result.output
