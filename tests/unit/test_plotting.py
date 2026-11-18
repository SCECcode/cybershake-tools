"""Tests for the run-manager plotting adapter."""

from pathlib import Path

from pytest_mock import MockerFixture

from cybershake import plotting
from cybershake.runmanager.schema import IMType


def _measure(value: float, component: str = "RotD50") -> IMType:
    return IMType(
        im_type_measure="spectral acceleration",
        im_type_value=value,
        units="cm/s/s",
        im_type_component=component,
    )


def test_comparison_curves_exist_uses_site_curve_directory(tmp_path: Path) -> None:
    site_directory = tmp_path / "TEST"
    site_directory.mkdir()
    (site_directory / "TEST_SA_3sec.png").touch()

    assert plotting.comparison_curves_exist(
        site="TEST",
        intensity_measures=(_measure(3.0),),
        curve_directory=tmp_path,
    )
    assert not plotting.comparison_curves_exist(
        site="TEST",
        intensity_measures=(_measure(5.0),),
        curve_directory=tmp_path,
    )


def test_generate_maps_attempts_all_commands_after_failure(
    mocker: MockerFixture,
) -> None:
    execute = mocker.patch(
        "cybershake.plotting._execute",
        side_effect=[plotting.PlottingError("scatter failed"), None, None],
    )

    failures = plotting.generate_maps()

    assert failures == ("scatter failed",)
    assert execute.call_count == 3


def test_generate_comparison_curves_passes_rotd50_periods(
    tmp_path: Path, mocker: MockerFixture
) -> None:
    execute = mocker.patch("cybershake.plotting.plot_jobs.run_plot", return_value=0)
    curve_directory = tmp_path / "curves"

    plotting.generate_comparison_curves(
        run_id=42,
        site="TEST",
        intensity_measures=(_measure(3.0), _measure(5.0, "RotD100")),
        curve_directory=curve_directory,
    )

    assert execute.call_args.args[0] == "curve"
    command = execute.call_args.args[1]
    assert command[command.index("-R") + 1] == "42"
    assert command[command.index("-s") + 1] == "TEST"
    assert command[command.index("-o") + 1] == str(curve_directory / "TEST")
    assert command[command.index("-p") + 1] == "3.0"
