"""Tests for the packaged database-check Java command."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from click.testing import CliRunner

from cybershake.__main__ import cybershake
from cybershake.check_db import check_db_site, default_classpath
from cybershake.constants import CHECK_DB_CONNECT_SOURCE, CHECK_DB_SOURCE

if TYPE_CHECKING:
    from pytest_mock import MockerFixture


def test_java_source_command(tmp_path: Path, mocker: MockerFixture) -> None:
    process = mocker.patch("cybershake.check_db.subprocess.run")
    process.side_effect = [
        subprocess.CompletedProcess([], 0),
        subprocess.CompletedProcess([], 3),
    ]
    rupture_list = tmp_path / "ruptures.csv"
    credentials = tmp_path / "credentials.txt"
    credentials.write_text("test-user:test-password", encoding="utf-8")

    status = check_db_site(
        run_id=123,
        output=tmp_path / "missing.txt",
        component="RotD50",
        periods="3,5",
        type_ids=None,
        server="moment.usc.edu",
        rupture_list=rupture_list,
        classpath="test-classpath",
        java="test-java",
        credentials=credentials,
    )

    assert status == 3
    assert CHECK_DB_SOURCE.is_file()
    assert CHECK_DB_CONNECT_SOURCE.is_file()
    assert process.call_count == 2
    compile_args = process.call_args_list[0].args[0]
    classes = compile_args[2]
    assert Path(compile_args[0]).name == "javac"
    assert compile_args[1:] == ["-d", classes, str(CHECK_DB_CONNECT_SOURCE)]
    process.assert_any_call(
        [
            "test-java",
            "-cp",
            os.pathsep.join((classes, "test-classpath")),
            str(CHECK_DB_SOURCE),
            "-r",
            "123",
            "-o",
            str(tmp_path / "missing.txt"),
            "-c",
            "RotD50",
            "-s",
            "moment.usc.edu",
            "-p",
            "3,5",
            "-rl",
            str(rupture_list),
        ],
        env=mocker.ANY,
        check=False,
    )
    assert process.call_args.kwargs["env"]["CYBERSHAKE_DB_USER"] == "test-user"
    assert (
        process.call_args.kwargs["env"]["CYBERSHAKE_DB_PASSWORD"]
        == (credentials.read_text(encoding="utf-8").partition(":")[2])
    )


def test_default_classpath_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CYBERSHAKE_CHECK_DB_CLASSPATH", raising=False)
    assert "share/db/jars" in default_classpath()
    monkeypatch.setenv("CYBERSHAKE_CHECK_DB_CLASSPATH", "other-classpath")
    assert default_classpath() == "other-classpath"


@pytest.mark.parametrize("status", [0, 3])
def test_cli_forwards_options_and_status(
    tmp_path: Path, mocker: MockerFixture, status: int
) -> None:
    check = mocker.patch("cybershake.__main__.db_check.check_db_site")
    check.return_value = status
    result = CliRunner().invoke(
        cybershake,
        [
            "jobs",
            "check-db-site",
            "-r",
            "123",
            "-o",
            str(tmp_path / "missing.txt"),
            "-c",
            "duration",
            "-t",
            "176,177",
            "-s",
            "moment.usc.edu",
            "-rl",
            str(tmp_path / "ruptures.csv"),
        ],
    )

    assert result.exit_code == status, result.output
    check.assert_called_once_with(
        run_id=123,
        output=tmp_path / "missing.txt",
        component="duration",
        periods=None,
        type_ids="176,177",
        server="moment.usc.edu",
        rupture_list=tmp_path / "ruptures.csv",
        classpath=None,
    )
