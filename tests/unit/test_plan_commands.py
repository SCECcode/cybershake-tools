"""Tests for shell-compatible workflow planning commands."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import ANY, Mock, call

import pytest
from click.testing import CliRunner

from cybershake import properties as pegasus_properties
from cybershake.__main__ import cybershake
from cybershake.constants import DEFAULT_DB_SERVER, SGT_GENERATED_STATE, START_STATE

if TYPE_CHECKING:
    from collections.abc import Callable

    from pytest_mock import MockerFixture


def _prepare_plan_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    root_name: str,
    workflow_name: str,
) -> tuple[Path, Path, Path]:
    monkeypatch.chdir(tmp_path)
    root_directory = Path(root_name)
    run_directory = root_directory / "run_73"
    run_directory.mkdir(parents=True)
    (root_directory / "run_table.txt").write_text(
        "73 TEST 64 8 10 15 1.0 2.0\n", encoding="utf-8"
    )
    (run_directory / workflow_name).write_text("name: test\n", encoding="utf-8")
    properties = tmp_path / "planner.properties"
    properties.write_text("property = value\n", encoding="utf-8")
    return root_directory, run_directory, properties


def _stub_plan_dependencies(
    mocker: MockerFixture, *, execution_directory: Path
) -> tuple[Mock, Mock, Mock]:
    valid_run = mocker.patch("cybershake.runmanager.valid")
    edit_run = mocker.patch("cybershake.runmanager.edit")
    plan = mocker.patch("cybershake.pegasus.plan", return_value=execution_directory)
    return valid_run, edit_run, plan


def _post_plan_placeholder(
    *, execution_directory: Path, transformation_catalog: Path
) -> Callable[..., Path]:
    def plan(**options: object) -> Path:
        execution_directory.mkdir()
        runtime_properties = execution_directory / "pegasus.123.properties"
        runtime_properties.write_text(
            "pegasus.exitcode.scope=none\n"
            f"pegasus.catalog.transformation.file={transformation_catalog}\n",
            encoding="utf-8",
        )
        plan_log = options["plan_log"]
        assert isinstance(plan_log, Path)
        plan_log.write_text(f"pegasus-run {runtime_properties}\n", encoding="utf-8")
        return execution_directory

    return plan


def test_sgt_plan_migrates_shell_orchestration(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
) -> None:
    root_directory, run_directory, properties = _prepare_plan_run(
        tmp_path,
        monkeypatch,
        root_name="TEST_SGT_dax",
        workflow_name="CyberShake_SGT_TEST.dax",
    )
    stale_log = root_directory / "log-plan-CyberShake_SGT_TEST.dax-old"
    stale_log.write_text("old\n", encoding="utf-8")
    execution_directory = tmp_path / "execution"
    valid_run, edit_run, plan = _stub_plan_dependencies(
        mocker, execution_directory=execution_directory
    )

    result = CliRunner().invoke(
        cybershake,
        [
            "sgt-plan",
            "TEST",
            "73",
            "frontier_noglideins",
            "-p",
            str(properties),
        ],
    )

    assert result.exit_code == 0
    assert result.output.endswith(
        "✓ Planned SGT workflow: run=73 site=frontier_noglideins "
        f"submit_dir={execution_directory}\n"
    )
    assert not stale_log.exists()
    local_properties = pegasus_properties.load(
        root_directory / "properties.sgt.onesite"
    )
    assert local_properties.defaults()["property"] == "value"
    valid_run.assert_called_once_with(
        run_id=73,
        site="TEST",
        stage="SGT_PLAN",
        server=DEFAULT_DB_SERVER,
        config=ANY,
    )
    plan.assert_called_once_with(
        workflow_file=run_directory / "CyberShake_SGT_TEST.dax",
        working_directory=run_directory,
        properties_file=properties,
        plan_log=run_directory / "log-plan-CyberShake_SGT_TEST.dax-frontier_noglideins",
        execution_sites=("frontier_noglideins", "shock"),
        output_site="frontier_noglideins",
        submit_directory=Path("dags"),
        rescue=100,
        cleanup="none",
        force=True,
        cluster=None,
        output_directory=Path("data/SgtFiles/TEST"),
        verbosity=0,
        environment=None,
    )
    edit_run.assert_called_once_with(
        run_id=73,
        fields={
            "Status": START_STATE,
            "SGT_Host": "frontier",
            "Comment": "Planned SGT DAX, assigned sgt host",
            "Job_ID": None,
        },
        server=DEFAULT_DB_SERVER,
        config=ANY,
    )


@pytest.mark.parametrize(
    ("configured_pp_site", "expected_pp_host"),
    [(None, "frontier"), ("None", "frontier"), ("shock", "shock")],
)
def test_pp_plan_migrates_shell_orchestration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mocker: MockerFixture,
    configured_pp_site: str | None,
    expected_pp_host: str,
) -> None:
    _root_directory, run_directory, properties = _prepare_plan_run(
        tmp_path,
        monkeypatch,
        root_name="TEST_PP_dax",
        workflow_name="CyberShake_TEST.dax",
    )
    if configured_pp_site is not None:
        (run_directory / "CyberShake_TEST.dax").write_text(
            f"metadata:\n  pp_site: {configured_pp_site}\n",
            encoding="utf-8",
        )
    database = run_directory / "TEST.db"
    database.write_text("sqlite\n", encoding="utf-8")
    transformation_catalog = tmp_path / "tc.txt"
    transformation_catalog.write_text("transformations\n", encoding="utf-8")
    execution_directory = tmp_path / "execution"
    valid_run = mocker.patch("cybershake.runmanager.valid")
    edit_run = mocker.patch("cybershake.runmanager.edit")
    plan = mocker.patch(
        "cybershake.pegasus.plan",
        side_effect=_post_plan_placeholder(
            execution_directory=execution_directory,
            transformation_catalog=transformation_catalog,
        ),
    )

    result = CliRunner().invoke(
        cybershake,
        ["pp-plan", "TEST", "73", "frontier", "-p", str(properties)],
    )

    assert result.exit_code == 0
    assert result.output.endswith(
        "✓ Planned PP workflow: run=73 site=frontier "
        f"submit_dir={execution_directory}\n"
    )
    valid_run.assert_called_once_with(
        run_id=73,
        site="TEST",
        stage="PP_PLAN",
        server=DEFAULT_DB_SERVER,
        config=ANY,
    )
    plan.assert_called_once_with(
        workflow_file=run_directory / "CyberShake_TEST.dax",
        working_directory=run_directory,
        properties_file=properties,
        plan_log=run_directory / "log-plan-CyberShake_TEST.dax-frontier",
        execution_sites=("frontier", "shock"),
        output_site="shock",
        submit_directory=Path("dags"),
        rescue=100,
        cleanup="none",
        force=True,
        cluster="label",
        output_directory=None,
        verbosity=2,
        environment={"X509_USER_PROXY": "/home/shock-ssd/scottcal/condor/x509up_u7588"},
    )
    runtime_properties = execution_directory / "pegasus.123.properties"
    top_properties = execution_directory / "pegasus.123.properties.top"
    assert (
        pegasus_properties.load(runtime_properties).defaults()["pegasus.exitcode.scope"]
        == "all"
    )
    assert (
        pegasus_properties.load(top_properties).defaults()["pegasus.exitcode.scope"]
        == "none"
    )
    assert (
        str(top_properties)
        in (run_directory / "log-plan-CyberShake_TEST.dax-frontier").read_text()
    )
    assert (execution_directory / "metadata.local").read_text() == (
        f"Site frontier\nTC {transformation_catalog}\ntransformations\n"
    )
    assert not database.exists()
    assert (execution_directory / "TEST_73.db").read_text() == "sqlite\n"
    edit_run.assert_called_once_with(
        run_id=73,
        fields={
            "Status": SGT_GENERATED_STATE,
            "PP_Host": expected_pp_host,
            "Comment": "Planned, assigned pp host",
            "Job_ID": None,
        },
        server=DEFAULT_DB_SERVER,
        config=ANY,
    )


def test_stoch_plan_migrates_shell_orchestration(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
) -> None:
    _root_directory, run_directory, properties = _prepare_plan_run(
        tmp_path,
        monkeypatch,
        root_name="TEST_Stoch_dax",
        workflow_name="CyberShake_Stoch_TEST_top.dax",
    )
    transformation_catalog = tmp_path / "tc.txt"
    transformation_catalog.write_text("transformations\n", encoding="utf-8")
    execution_directory = tmp_path / "execution"
    valid_run = mocker.patch("cybershake.runmanager.valid")
    edit_run = mocker.patch("cybershake.runmanager.edit")
    plan = mocker.patch(
        "cybershake.pegasus.plan",
        side_effect=_post_plan_placeholder(
            execution_directory=execution_directory,
            transformation_catalog=transformation_catalog,
        ),
    )

    result = CliRunner().invoke(
        cybershake,
        ["stoch-plan", "TEST", "73", "frontier", "-p", str(properties)],
    )

    assert result.exit_code == 0
    assert result.output.endswith(
        "✓ Planned stochastic workflow: run=73 site=frontier "
        f"submit_dir={execution_directory}\n"
    )
    valid_run.assert_called_once_with(
        run_id=73,
        site="TEST",
        stage="PP_PLAN",
        server=DEFAULT_DB_SERVER,
        config=ANY,
    )
    assert plan.call_args.kwargs == {
        "workflow_file": run_directory / "CyberShake_Stoch_TEST_top.dax",
        "working_directory": run_directory,
        "properties_file": properties,
        "plan_log": run_directory / "log-plan-CyberShake_Stoch_TEST_top.dax-frontier",
        "execution_sites": ("frontier", "shock"),
        "output_site": "shock",
        "submit_directory": Path("dags"),
        "rescue": 100,
        "cleanup": "none",
        "force": True,
        "cluster": "label",
        "output_directory": None,
        "verbosity": 0,
        "environment": {
            "X509_USER_PROXY": "/home/shock-ssd/scottcal/condor/x509up_u7588"
        },
    }
    edit_run.assert_called_once_with(
        run_id=73,
        fields={"PP_Host": "frontier", "Job_ID": None},
        server=DEFAULT_DB_SERVER,
        config=ANY,
    )


@pytest.mark.parametrize(
    ("configured_pp_site", "expected_pp_host"),
    [(None, "frontier"), ("None", "frontier"), ("shock", "shock")],
)
def test_full_plan_migrates_shell_orchestration_and_linked_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mocker: MockerFixture,
    configured_pp_site: str | None,
    expected_pp_host: str,
) -> None:
    root_directory, run_directory, properties = _prepare_plan_run(
        tmp_path,
        monkeypatch,
        root_name="TEST_Integrated_dax",
        workflow_name="CyberShake_Integrated_TEST.dax",
    )
    if configured_pp_site is not None:
        (run_directory / "CyberShake_Integrated_TEST.dax").write_text(
            f"metadata:\n  pp_site: {configured_pp_site}\n",
            encoding="utf-8",
        )
    (run_directory / "sites.list").write_text("TEST\n", encoding="utf-8")
    (run_directory / "linked_bb_run_id.txt").write_text("74\n", encoding="utf-8")
    transformation_catalog = tmp_path / "tc.txt"
    transformation_catalog.write_text("transformations\n", encoding="utf-8")
    site_catalog = tmp_path / "sites.yml"
    site_catalog.write_text("sites\n", encoding="utf-8")
    properties.write_text(
        f"pegasus.catalog.transformation.file={transformation_catalog}\n"
        f"pegasus.catalog.site.file={site_catalog}\n"
        "pegasus.catalog.replica.db.url=jdbc:sqlite:rc.sqlite\n",
        encoding="utf-8",
    )
    execution_directory = tmp_path / "execution"
    execution_directory.mkdir()
    valid_run, edit_run, plan = _stub_plan_dependencies(
        mocker, execution_directory=execution_directory
    )

    result = CliRunner().invoke(
        cybershake,
        [
            "full-plan",
            "TEST",
            "73",
            "frontier_noglideins",
            "-p",
            str(properties),
        ],
    )

    assert result.exit_code == 0
    assert result.output.endswith(
        "✓ Planned full workflow: run=73 site=frontier_noglideins "
        f"submit_dir={execution_directory}\n"
    )
    assert (run_directory / "sites.list.debug").read_text() == "TEST\n"
    local_properties = run_directory / "properties.full.onesite"
    loaded_local_properties = pegasus_properties.load(local_properties).defaults()
    assert loaded_local_properties["pegasus.catalog.transformation.file"] == str(
        transformation_catalog
    )
    assert loaded_local_properties["pegasus.catalog.site.file"] == str(site_catalog)
    assert (
        loaded_local_properties["pegasus.catalog.replica.db.url"]
        == "jdbc:sqlite:rc.sqlite"
    )
    valid_run.assert_called_once_with(
        run_id=73,
        site="TEST",
        stage="SGT_PLAN",
        server=DEFAULT_DB_SERVER,
        config=ANY,
    )
    plan.assert_called_once_with(
        workflow_file=run_directory / "CyberShake_Integrated_TEST.dax",
        working_directory=run_directory,
        properties_file=local_properties,
        plan_log=run_directory
        / "log-plan-CyberShake_Integrated_TEST.dax-frontier_noglideins",
        execution_sites=("frontier_noglideins", "shock", "local"),
        output_site="frontier_noglideins",
        submit_directory=Path("dags"),
        rescue=100,
        cleanup="none",
        force=True,
        cluster=None,
        output_directory=None,
        verbosity=0,
        environment=None,
    )
    assert (execution_directory / "metadata.local").read_text() == (
        f"Site frontier_noglideins\nTC {transformation_catalog}\n"
        f"transformations\nSC {site_catalog}\nsites\n"
        "RC location jdbc:sqlite:rc.sqlite\n"
    )
    assert edit_run.call_args_list == [
        call(
            run_id=73,
            fields={
                "Status": START_STATE,
                "SGT_Host": "frontier",
                "Comment": "Planned SGT DAX, assigned sgt host",
                "Job_ID": None,
                "PP_Host": expected_pp_host,
            },
            server=DEFAULT_DB_SERVER,
            config=ANY,
        ),
        call(
            run_id=74,
            fields={
                "Status": START_STATE,
                "SGT_Host": "frontier",
                "Comment": "Assigned hosts",
                "Job_ID": None,
                "PP_Host": "frontier",
            },
            server=DEFAULT_DB_SERVER,
            config=ANY,
        ),
    ]
    assert root_directory.is_dir()


@pytest.mark.parametrize(
    ("command", "root_name"),
    [
        ("sgt-plan", "TEST_SGT_dax"),
        ("pp-plan", "TEST_PP_dax"),
        ("stoch-plan", "TEST_Stoch_dax"),
        ("full-plan", "TEST_Integrated_dax"),
    ],
)
def test_plan_commands_require_the_run_table(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    command: str,
    root_name: str,
) -> None:
    monkeypatch.chdir(tmp_path)
    properties = tmp_path / "planner.properties"
    properties.write_text("property=value\n", encoding="utf-8")

    result = CliRunner().invoke(
        cybershake,
        [command, "TEST", "73", "frontier", "-p", str(properties)],
    )

    assert result.exit_code != 0
    assert f"{root_name}/run_table.txt not found" in result.output
