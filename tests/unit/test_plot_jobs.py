"""Tests for workflow OpenSHA plot commands."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from click.testing import CliRunner

from cybershake import plot_jobs
from cybershake.__main__ import cybershake

if TYPE_CHECKING:
    from pathlib import Path

    from pytest_mock import MockerFixture


def test_curve_plot_reads_velocity_file_and_runs_java(
    tmp_path: Path, mocker: MockerFixture
) -> None:
    velocity = tmp_path / "velocity.txt"
    velocity.write_text("Vs at site = 760\nVs30 = 578.23\n", encoding="utf-8")
    process = mocker.patch("cybershake.plot_jobs.subprocess.run")
    process.return_value.returncode = 0

    status = plot_jobs.run_plot(
        "curve",
        ("--site", "TEST", "-cmp", "RotD50"),
        velocity_file=velocity,
        classpath="test.jar",
        java="test-java",
    )

    assert status == 0
    command = process.call_args.args[0]
    assert command == [
        "test-java",
        "-Xms512M",
        "-Xmx2G",
        "-cp",
        "test.jar",
        "-Dcybershake.db.host=moment",
        "org.opensha.sha.cybershake.plot.HazardCurvePlotter",
        "--force-vs30",
        "578.23",
        "--site",
        "TEST",
        "-cmp",
        "RotD50",
    ]


def test_disagg_plot_uses_larger_heap(mocker: MockerFixture) -> None:
    process = mocker.patch("cybershake.plot_jobs.subprocess.run")
    process.return_value.returncode = 3

    assert (
        plot_jobs.run_plot(
            "disagg", ("--run-id", "123"), classpath="test.jar", java="test-java"
        )
        == 3
    )
    command = process.call_args.args[0]
    assert command[:7] == [
        "test-java",
        "-Xms512M",
        "-Xmx12G",
        "-cp",
        "test.jar",
        "-Dcybershake.db.host=moment",
        "org.opensha.sha.cybershake.plot.DisaggregationPlotter",
    ]


def test_missing_vs30_is_reported(tmp_path: Path) -> None:
    velocity = tmp_path / "velocity.txt"
    velocity.write_text("Vs at site = 760\n", encoding="utf-8")
    with pytest.raises(plot_jobs.PlottingError, match="Vs30 not found"):
        plot_jobs.velocity_vs30(velocity)


def test_classpath_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CYBERSHAKE_PLOT_CLASSPATH", raising=False)
    assert plot_jobs.default_classpath().endswith("opensha-cybershake-all.jar")
    monkeypatch.setenv("CYBERSHAKE_PLOT_CLASSPATH", "custom.jar")
    assert plot_jobs.default_classpath() == "custom.jar"


@pytest.mark.parametrize("command", ["curve-plot", "disagg-plot"])
@pytest.mark.parametrize("status", [0, 3])
def test_cli_passes_plotter_arguments(
    command: str, status: int, mocker: MockerFixture
) -> None:
    run = mocker.patch("cybershake.__main__.plot_jobs.run_plot", return_value=status)
    result = CliRunner().invoke(
        cybershake,
        ["jobs", command, "--site", "TEST", "-cmp", "RotD50"],
    )

    assert result.exit_code == status, result.output
    assert run.call_args.args == (
        "curve" if command == "curve-plot" else "disagg",
        ("--site", "TEST", "-cmp", "RotD50"),
    )
