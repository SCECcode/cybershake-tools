"""Tests for the external Pegasus command boundaries."""

from pathlib import Path

import pytest

from cybershake import pegasus, utils
from cybershake import properties as pegasus_properties
from cybershake.exc import PegasusError


def _executable(path: Path, body: str) -> Path:
    path.write_text(f"#!/bin/sh\nset -eu\n{body}", encoding="utf-8")
    path.chmod(0o755)
    return path


def _plan(
    tmp_path: Path,
    *,
    environment: dict[str, str] | None = None,
    cluster: str | None = "label",
    output_directory: Path | None = Path("output"),
    verbosity: int = 2,
    force: bool = True,
) -> Path:
    working_directory = tmp_path / "work"
    working_directory.mkdir(exist_ok=True)
    workflow_file = working_directory / "workflow.dax"
    workflow_file.write_text("workflow\n", encoding="utf-8")
    properties_file = tmp_path / "properties"
    properties_file.write_text("property=value\n", encoding="utf-8")
    return pegasus.plan(
        workflow_file=workflow_file,
        working_directory=working_directory,
        properties_file=properties_file,
        plan_log=working_directory / "plan.log",
        execution_sites=("frontier", "shock"),
        output_site="shock",
        submit_directory=Path("dags"),
        rescue=100,
        cleanup="none",
        force=force,
        cluster=cluster,
        output_directory=output_directory,
        verbosity=verbosity,
        environment=environment,
    )


def test_plan_runs_legacy_command_and_returns_submit_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    arguments_file = tmp_path / "arguments"
    submit_directory = tmp_path / "submit"
    submit_directory.mkdir()
    executable = _executable(
        tmp_path / "pegasus-plan",
        'printf \'%s\\n\' "$@" > "$FAKE_ARGUMENTS"\n'
        "printf 'proxy=%s\\n' \"${X509_USER_PROXY-}\"\n"
        "printf 'pegasus-run --conf=%s/pegasus.test.properties %s\\n' "
        '"$FAKE_SUBMIT" "$FAKE_SUBMIT"\n',
    )
    monkeypatch.setattr(pegasus, "PEGASUS_PLAN_EXECUTABLE", str(executable))
    monkeypatch.setenv("FAKE_ARGUMENTS", str(arguments_file))
    monkeypatch.setenv("FAKE_SUBMIT", str(submit_directory))
    proxy = tmp_path / "proxy"

    result = _plan(
        tmp_path,
        environment={"X509_USER_PROXY": str(proxy)},
    )

    assert result == submit_directory
    assert arguments_file.read_text(encoding="utf-8").splitlines() == [
        "-vv",
        "--conf",
        str((tmp_path / "properties").resolve()),
        "--rescue",
        "100",
        "-s",
        "frontier,shock",
        "--output-site",
        "shock",
        "--dir",
        "dags",
        "-f",
        "--cluster",
        "label",
        "--cleanup",
        "none",
        "--output-dir",
        "output",
        "workflow.dax",
    ]
    assert (tmp_path / "work" / "plan.log").read_text(encoding="utf-8") == (
        f"proxy={proxy}\n"
        f"pegasus-run --conf={submit_directory}/pegasus.test.properties "
        f"{submit_directory}\n"
    )


def test_plan_omits_disabled_optional_arguments(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    arguments_file = tmp_path / "arguments"
    submit_directory = tmp_path / "submit"
    submit_directory.mkdir()
    executable = _executable(
        tmp_path / "pegasus-plan",
        'printf \'%s\\n\' "$@" > "$FAKE_ARGUMENTS"\n'
        "printf 'pegasus-run %s\\n' \"$FAKE_SUBMIT\"\n",
    )
    monkeypatch.setattr(pegasus, "PEGASUS_PLAN_EXECUTABLE", str(executable))
    monkeypatch.setenv("FAKE_ARGUMENTS", str(arguments_file))
    monkeypatch.setenv("FAKE_SUBMIT", str(submit_directory))

    _plan(
        tmp_path,
        cluster=None,
        output_directory=None,
        verbosity=0,
        force=False,
    )

    arguments = arguments_file.read_text(encoding="utf-8").splitlines()
    assert "-vv" not in arguments
    assert "-f" not in arguments
    assert "--cluster" not in arguments
    assert "--output-dir" not in arguments


def test_plan_records_output_and_raises_on_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    executable = _executable(
        tmp_path / "pegasus-plan",
        "printf 'planner failed\\n' >&2\nexit 7\n",
    )
    monkeypatch.setattr(pegasus, "PEGASUS_PLAN_EXECUTABLE", str(executable))

    with pytest.raises(PegasusError, match="exit status 7: planner failed"):
        _plan(tmp_path)

    assert (tmp_path / "work" / "plan.log").read_text(encoding="utf-8") == (
        "planner failed\n"
    )


def test_plan_rejects_success_without_run_command(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    executable = _executable(
        tmp_path / "pegasus-plan",
        "printf 'planning complete\\n'\n",
    )
    monkeypatch.setattr(pegasus, "PEGASUS_PLAN_EXECUTABLE", str(executable))

    with pytest.raises(PegasusError, match="No pegasus-run command"):
        _plan(tmp_path)


def test_run_executes_planner_command_and_parses_cluster(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    arguments_file = tmp_path / "arguments"
    submit_directory = tmp_path / "submit"
    submit_directory.mkdir()
    executable = _executable(
        tmp_path / "pegasus-run",
        'printf \'%s\\n\' "$@" > "$FAKE_ARGUMENTS"\n'
        "printf 'runner diagnostic\\n' >&2\n"
        "printf 'Submitting job(s).\\n1 job(s) submitted to cluster 456.\\n'\n",
    )
    plan_log = tmp_path / "plan.log"
    properties = submit_directory / "pegasus.test.properties.top"
    plan_log.write_text(
        f"planner output\npegasus-run --conf={properties} {submit_directory}\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(pegasus, "PEGASUS_RUN_EXECUTABLE", str(executable))
    monkeypatch.setenv("FAKE_ARGUMENTS", str(arguments_file))
    run_log = tmp_path / "run.log"

    cluster_id, result_directory = pegasus.run(
        plan_log=plan_log,
        run_log=run_log,
    )

    assert cluster_id == "456"
    assert result_directory == submit_directory
    assert arguments_file.read_text(encoding="utf-8").splitlines() == [
        f"--conf={properties}",
        str(submit_directory),
    ]
    output = run_log.read_text(encoding="utf-8")
    assert "runner diagnostic" in output
    assert "submitted to cluster 456." in output


def test_run_records_output_and_raises_on_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    submit_directory = tmp_path / "submit"
    submit_directory.mkdir()
    executable = _executable(
        tmp_path / "pegasus-run",
        "printf 'submission failed\\n' >&2\nexit 9\n",
    )
    plan_log = tmp_path / "plan.log"
    plan_log.write_text(f"pegasus-run {submit_directory}\n", encoding="utf-8")
    monkeypatch.setattr(pegasus, "PEGASUS_RUN_EXECUTABLE", str(executable))
    run_log = tmp_path / "run.log"

    with pytest.raises(PegasusError, match="exit status 9: submission failed"):
        pegasus.run(plan_log=plan_log, run_log=run_log)

    assert run_log.read_text(encoding="utf-8") == "submission failed\n"


def test_run_rejects_missing_cluster_id(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    submit_directory = tmp_path / "submit"
    submit_directory.mkdir()
    executable = _executable(
        tmp_path / "pegasus-run",
        "printf 'submission complete without an ID\\n'\n",
    )
    plan_log = tmp_path / "plan.log"
    plan_log.write_text(f"pegasus-run {submit_directory}\n", encoding="utf-8")
    monkeypatch.setattr(pegasus, "PEGASUS_RUN_EXECUTABLE", str(executable))

    with pytest.raises(PegasusError, match="cluster ID"):
        pegasus.run(plan_log=plan_log, run_log=tmp_path / "run.log")


def test_prepare_post_plan_files_serializes_pegasus_properties(
    tmp_path: Path,
) -> None:
    execution_directory = tmp_path / "submit"
    execution_directory.mkdir()
    transformation_catalog = tmp_path / "tc.txt"
    transformation_catalog.write_text("transformations\n", encoding="utf-8")
    runtime_properties = execution_directory / "pegasus.123.properties"
    runtime_properties.write_text(
        "pegasus.exitcode.scope: all\n"
        f"pegasus.catalog.transformation.file: {transformation_catalog}\n",
        encoding="utf-8",
    )
    plan_log = tmp_path / "plan.log"
    plan_log.write_text(
        f"pegasus-run --conf={runtime_properties} {execution_directory}\n",
        encoding="utf-8",
    )
    database = tmp_path / "TEST.db"
    database.write_text("database\n", encoding="utf-8")

    utils.prepare_post_plan_files(
        execution_directory=execution_directory,
        plan_log=plan_log,
        remote_site="frontier",
        run_directory=tmp_path,
        run_site="TEST",
        run_id=73,
    )

    top_properties = Path(f"{runtime_properties}.top")
    assert (
        pegasus_properties.load(runtime_properties).defaults()["pegasus.exitcode.scope"]
        == "all"
    )
    assert (
        pegasus_properties.load(top_properties).defaults()["pegasus.exitcode.scope"]
        == "none"
    )
    assert str(top_properties) in plan_log.read_text(encoding="utf-8")
    assert (execution_directory / "metadata.local").read_text(encoding="utf-8") == (
        f"Site frontier\nTC {transformation_catalog}\ntransformations\n"
    )
    assert not database.exists()
    assert (execution_directory / "TEST_73.db").read_text(encoding="utf-8") == (
        "database\n"
    )


def test_write_full_plan_metadata_loads_pegasus_properties(tmp_path: Path) -> None:
    execution_directory = tmp_path / "submit"
    execution_directory.mkdir()
    transformation_catalog = tmp_path / "tc.txt"
    transformation_catalog.write_text("transformations\n", encoding="utf-8")
    site_catalog = tmp_path / "sites.yml"
    site_catalog.write_text("sites\n", encoding="utf-8")
    properties_file = tmp_path / "pegasus.properties"
    properties_file.write_text(
        f"pegasus.catalog.transformation.file: {transformation_catalog}\n"
        f"pegasus.catalog.site.file: {site_catalog}\n"
        "pegasus.catalog.replica.db.url: jdbc:sqlite:rc.sqlite\n",
        encoding="utf-8",
    )

    utils.write_full_plan_metadata(
        execution_directory=execution_directory,
        properties_file=properties_file,
        remote_site="frontier",
    )

    assert (execution_directory / "metadata.local").read_text(encoding="utf-8") == (
        f"Site frontier\nTC {transformation_catalog}\ntransformations\n"
        f"SC {site_catalog}\nsites\nRC location jdbc:sqlite:rc.sqlite\n"
    )
