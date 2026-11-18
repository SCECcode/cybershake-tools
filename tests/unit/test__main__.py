"""CLI integration tests for cybershake.__main__."""

import logging
from collections.abc import Callable
from inspect import Parameter, signature
from pathlib import Path
from unittest.mock import ANY, Mock, call

import pytest
from click.testing import CliRunner
from pytest_mock import MockerFixture

from cybershake.__main__ import (
    cybershake,
    full,
    pp,
    sgt,
    stoch,
    sub_sgt_wf,
    sub_stoch_wf,
)
from cybershake.config import CONFIG_DEFAULTS, Config
from cybershake.constants import (
    CONTEXT_SETTINGS,
    DEFAULT_STOCH_FREQUENCY,
)
from cybershake.exc import RunManagerError
from cybershake.options import EPILOG, EpilogGroup

SGT_REQUIRED_ARGS = (
    "sgt",
    "-s",
    "TEST",
    "-v",
    "v246",
    "-e",
    "64",
    "-r",
    "10",
    "-g",
    "8",
    "-f",
    "1.0",
)
PP_REQUIRED_ARGS = (
    "pp",
    "-s",
    "TEST",
    "-v",
    "v246",
    "-e",
    "64",
    "-r",
    "10",
    "-g",
    "8",
    "-f",
    "1.0",
)
STOCH_REQUIRED_ARGS = (
    "stoch",
    "-s",
    "TEST",
    "-l",
    "10",
    "-e",
    "64",
    "-r",
    "10",
    "--merge-frequency",
    "1.0",
)
FULL_REQUIRED_ARGS = (
    "full",
    "-s",
    "TEST",
    "-v",
    "v246",
    "-e",
    "64",
    "-r",
    "10",
    "-g",
    "8",
    "-f",
    "1.0",
)


def _stub_workflow_generator(
    mocker: MockerFixture,
    *,
    class_path: str,
    method_name: str,
    side_effect: Callable[..., None],
) -> Mock:
    invocation = Mock(side_effect=side_effect)

    def construct(**common_options: object) -> Mock:
        instance = Mock()

        def invoke(**specific_options: object) -> None:
            invocation(**common_options, **specific_options)

        getattr(instance, method_name).side_effect = invoke
        return instance

    mocker.patch(class_path, side_effect=construct)
    return invocation


def _stub_sgt_dependencies(
    mocker: MockerFixture, *, run_id: int = 73
) -> tuple[Mock, Mock, Mock, Mock]:
    create_run = mocker.patch("cybershake.runmanager.create", return_value=run_id)
    valid_run = mocker.patch("cybershake.runmanager.valid")

    def create_workflow_placeholder(*, workflow_file: Path, **_options: object) -> None:
        workflow_file.write_text("placeholder workflow\n", encoding="utf-8")

    create_workflow = _stub_workflow_generator(
        mocker,
        class_path="cybershake.__main__.sgt_workflow.SGT",
        method_name="create",
        side_effect=create_workflow_placeholder,
    )
    edit_run = mocker.patch("cybershake.runmanager.edit")
    return create_run, valid_run, create_workflow, edit_run


def _stub_pp_dependencies(
    mocker: MockerFixture, *, run_id: int = 73
) -> tuple[Mock, Mock, Mock, Mock]:
    find_run = mocker.patch("cybershake.runmanager.find", return_value=run_id)
    valid_run = mocker.patch("cybershake.runmanager.valid")

    def create_workflow_placeholder(*, run_directory: Path, **_options: object) -> None:
        (run_directory / "placeholder.yml").write_text(
            "placeholder workflow\n", encoding="utf-8"
        )

    create_workflow = _stub_workflow_generator(
        mocker,
        class_path="cybershake.__main__.pp_workflow.PP",
        method_name="create",
        side_effect=create_workflow_placeholder,
    )
    edit_run = mocker.patch("cybershake.runmanager.edit")
    return find_run, valid_run, create_workflow, edit_run


def _stub_stoch_dependencies(
    mocker: MockerFixture, *, found_run_id: int = 73, created_run_id: int = 74
) -> tuple[Mock, Mock, Mock, Mock]:
    find_run = mocker.patch(
        "cybershake.runmanager.find_stoch", return_value=found_run_id
    )
    create_run = mocker.patch(
        "cybershake.runmanager.create_stoch", return_value=created_run_id
    )
    valid_run = mocker.patch("cybershake.runmanager.valid")

    def create_workflow_placeholder(*, run_directory: Path, **_options: object) -> None:
        (run_directory / "placeholder.yml").write_text(
            "placeholder workflow\n", encoding="utf-8"
        )

    create_workflow = _stub_workflow_generator(
        mocker,
        class_path="cybershake.__main__.stoch_workflow.Stoch",
        method_name="create",
        side_effect=create_workflow_placeholder,
    )
    return find_run, create_run, valid_run, create_workflow


def _stub_full_dependencies(
    mocker: MockerFixture, *, run_ids: tuple[int, ...] = (73, 74)
) -> tuple[Mock, Mock, Mock, Mock]:
    create_run = mocker.patch("cybershake.runmanager.create", side_effect=run_ids)
    valid_run = mocker.patch("cybershake.runmanager.valid")

    def create_workflow_placeholder(*, workflow_file: Path, **_options: object) -> None:
        workflow_file.write_text("placeholder workflow\n", encoding="utf-8")

    create_workflow = Mock(side_effect=create_workflow_placeholder)

    def construct_full(*, sgt: object, pp: object, stoch: object) -> Mock:
        _ = pp, stoch
        instance = Mock()

        def invoke(**specific_options: object) -> None:
            create_workflow(config=vars(sgt)["config"], **specific_options)

        instance.create.side_effect = invoke
        return instance

    mocker.patch("cybershake.__main__.full_workflow.Full", side_effect=construct_full)
    edit_run = mocker.patch("cybershake.runmanager.edit")
    return create_run, valid_run, create_workflow, edit_run


def test_cybershake_help() -> None:
    runner = CliRunner()
    result = runner.invoke(cybershake, ["--help"])
    assert result.exit_code == 0
    assert "--verbose" in result.output


@pytest.mark.parametrize(
    ("verbosity", "log_level"),
    [
        ([], logging.WARNING),
        (["-v"], logging.INFO),
        (["-vv"], logging.DEBUG),
        (["-vvv"], logging.DEBUG),
    ],
)
def test_verbose_controls_logging(
    verbosity: list[str], log_level: int, mocker: MockerFixture
) -> None:
    basic_config = mocker.patch("cybershake.__main__.logging.basicConfig")
    set_level = mocker.patch.object(logging.getLogger(), "setLevel")
    mocker.patch("cybershake.runmanager.update")

    result = CliRunner().invoke(
        cybershake,
        [*verbosity, "run-manager", "update", "101", "SGT_START", "SGT_END"],
    )

    assert result.exit_code == 0
    basic_config.assert_called_once_with(level=log_level, handlers=[ANY])
    set_level.assert_called_once_with(log_level)


def test_epilog_group_applies_the_epilog_to_commands() -> None:
    assert isinstance(cybershake, EpilogGroup)
    assert cybershake.epilog == EPILOG
    assert cybershake.context_settings == CONTEXT_SETTINGS
    assert all(command.epilog == EPILOG for command in cybershake.commands.values())

    run_manager = cybershake.commands["run-manager"]
    assert isinstance(run_manager, EpilogGroup)
    assert run_manager.context_settings == CONTEXT_SETTINGS
    assert all(command.epilog == EPILOG for command in run_manager.commands.values())


@pytest.mark.parametrize(
    ("group_name", "help_text"),
    [
        ("sgt", "Create an SGT workflow."),
        ("pp", "Create a PP workflow."),
        ("stoch", "Create a stochastic workflow."),
        ("full", "Create a full workflow."),
        ("run-manager", "Manage CyberShake run records."),
    ],
)
def test_click_commands_have_docstrings(group_name: str, help_text: str) -> None:
    result = CliRunner().invoke(cybershake, [group_name, "--help"])

    assert result.exit_code == 0
    assert help_text in result.output


def test_workflow_callbacks_have_explicit_typed_options() -> None:
    for command in (sgt, pp, stoch, full, sub_sgt_wf, sub_stoch_wf):
        assert command.callback is not None
        parameters = signature(command.callback).parameters

        assert all(
            parameter.kind is not Parameter.VAR_KEYWORD
            for parameter in parameters.values()
        )
        assert all(
            parameter.annotation is not Parameter.empty
            for parameter in parameters.values()
        )
        assert {parameter.name for parameter in command.params} == (
            parameters.keys() - {"_ctx"}
        )


def test_legacy_sh_commands_are_removed() -> None:
    runner = CliRunner()

    for command in ("sgt-sh", "pp-sh", "stoch-sh", "full-sh"):
        result = runner.invoke(cybershake, [command, "--help"])

        assert result.exit_code == 2
        assert f"No such command '{command}'" in result.output


def test_sgt_exposes_migrated_options() -> None:
    result = CliRunner().invoke(cybershake, ["sgt", "--help"])
    command = sgt

    assert result.exit_code == 0
    assert {parameter.name: parameter.opts for parameter in command.params} == {
        "velocity_model": ["-v", "--velocity-model"],
        "erf_id": ["-e", "--erf-id"],
        "rupture_variation_id": ["-r", "--rupture-variation-id"],
        "sgt_variation_id": ["-g", "--sgt-variation-id"],
        "frequency": ["-f", "--frequency"],
        "source_frequency": ["-q", "--source-frequency"],
        "site": ["-s", "--site"],
        "separate_md5": ["-sm", "--separate-md5"],
        "handoff": ["-hd", "--handoff"],
        "sgt_site": ["-ss", "--sgt-site"],
        "server": ["-sr", "--server"],
        "spacing": ["-sp", "--spacing"],
        "min_vs": ["-mv", "--min-vs"],
        "no_smoothing": ["-ns", "--no-smoothing"],
        "depth": ["-dep", "--depth"],
        "h_fraction": ["-hf", "--h-fraction"],
        "rotation": ["-rt", "--rotation"],
        "ely_taper": ["-et", "--ely-taper"],
        "taper_depth": ["-td", "--taper-depth"],
        "taper_models": ["-tm", "--taper-models"],
        "z_comp": ["-z", "--z-comp"],
        "local_vparam_insert": ["-lv", "--local-vparam-insert"],
        "config": ["-c", "--config"],
    }


def test_sgt_migrates_shell_orchestration(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
) -> None:
    create_run, valid_run, create_workflow, edit_run = _stub_sgt_dependencies(mocker)
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(
        cybershake,
        [
            *SGT_REQUIRED_ARGS,
            "-q",
            "2.0",
            "--server",
            "moment",
            "-dep",
            "50.0",
            "-hf",
            "0.25",
            "-mv",
            "400.0",
            "-rt",
            "-36.0",
            "-et",
            "ifless",
            "-td",
            "700.0",
            "-tm",
            "sfcvm,cca,bbp1d",
            "-sp",
            "0.08",
            "-z",
            "-hd",
            "-lv",
        ],
    )

    assert result.exit_code == 0
    assert result.output.endswith(
        "✓ Created SGT workflow: run=73 "
        f"file={tmp_path / 'TEST_SGT_dax/run_73/CyberShake_SGT_TEST.dax'}\n"
    )
    assert Path("TEST_SGT_dax/sites.list").read_text() == "TEST\n"
    assert Path("TEST_SGT_dax/run_table.txt").read_text() == (
        "73 TEST 64 8 10 15 1.0 2.0\n"
    )
    assert (
        Path("TEST_SGT_dax/run_73/CyberShake_SGT_TEST.dax").read_text()
        == "placeholder workflow\n"
    )

    create_run.assert_called_once_with(
        site="TEST",
        erf_id=64,
        sgt_variation_id=8,
        velocity_model_id=15,
        rupture_variation_id=10,
        frequency=1.0,
        source_frequency=2.0,
        server="moment",
        config=ANY,
    )
    resolved_config = create_run.call_args.kwargs["config"]
    assert isinstance(resolved_config, Config)
    assert set(resolved_config) == set(CONFIG_DEFAULTS)
    assert resolved_config.command == "sgt"
    assert resolved_config["site"] == "TEST"
    assert resolved_config["spacing"] == 0.08
    valid_run.assert_not_called()
    workflow_options = create_workflow.call_args.kwargs
    assert workflow_options == {
        "workflow_file": Path("TEST_SGT_dax/run_73/CyberShake_SGT_TEST.dax"),
        "run_directory": Path("TEST_SGT_dax/run_73"),
        "run_ids": (73,),
        "config": ANY,
    }
    edit_run.assert_called_once_with(
        run_id=73,
        fields={
            "Status": "Initial",
            "Comment": "SGT DAX created",
            "Job_ID": None,
            "Submit_Dir": None,
        },
        server="moment",
        config=ANY,
    )


def test_sgt_reuses_valid_legacy_run_table(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
) -> None:
    create_run, valid_run, create_workflow, _edit_run = _stub_sgt_dependencies(mocker)
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)

    run_directory = Path("TEST_SGT_dax")
    run_directory.mkdir()
    (run_directory / "run_table.txt").write_text(
        "73 TEST 64 8 10 15 1.0\n", encoding="utf-8"
    )

    result = runner.invoke(cybershake, SGT_REQUIRED_ARGS)

    assert result.exit_code == 0
    create_run.assert_not_called()
    valid_run.assert_called_once_with(
        run_id=73,
        site="TEST",
        stage="SGT_PLAN",
        server="moment",
        config=ANY,
    )
    assert create_workflow.call_args.kwargs["run_ids"] == (73,)


def test_sgt_output_directory_comes_from_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
) -> None:
    _stub_sgt_dependencies(mocker)
    output_root = tmp_path / "configured-runs"
    monkeypatch.chdir(tmp_path)

    result = CliRunner().invoke(
        cybershake,
        SGT_REQUIRED_ARGS,
        env={"CYBERSHAKE_OUTPUT_DIRECTORY": str(output_root)},
    )

    assert result.exit_code == 0
    assert (output_root / "TEST_SGT_dax/run_73/CyberShake_SGT_TEST.dax").is_file()
    assert not Path("TEST_SGT_dax").exists()


def test_sgt_output_directory_comes_from_config_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
) -> None:
    _stub_sgt_dependencies(mocker)
    config = tmp_path / "sgt.conf"
    config.write_text("output-directory=configured-runs\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)

    result = CliRunner().invoke(
        cybershake,
        [*SGT_REQUIRED_ARGS, "--config", str(config)],
    )

    assert result.exit_code == 0
    assert Path("configured-runs/TEST_SGT_dax/run_73/CyberShake_SGT_TEST.dax").is_file()


def test_sgt_continues_when_final_edit_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
) -> None:
    _create_run, _valid_run, _create_workflow, edit_run = _stub_sgt_dependencies(mocker)
    edit_run.side_effect = RunManagerError
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(cybershake, SGT_REQUIRED_ARGS)

    assert result.exit_code == 0
    assert "Unable to update Status" in result.output


def test_sub_sgt_wf_exposes_migrated_options() -> None:
    result = CliRunner().invoke(cybershake, ["sub-sgt-wf", "--help"])

    assert result.exit_code == 0
    assert {parameter.name: parameter.opts for parameter in sub_sgt_wf.params} == {
        "run_id": ["-r", "--run-id"],
        "grid_output_file": ["-gf", "--grid-output-file"],
        "workflow_file": ["-o", "--workflow-file"],
        "server": ["-sr", "--server"],
        "z_comp": ["-z", "--z-comp"],
        "spacing": ["-sp", "--spacing"],
        "handoff": ["-hd", "--handoff"],
        "h_fraction": ["-hf", "--h-fraction"],
        "separate_md5": ["-sm", "--separate-md5"],
        "min_vs": ["-mv", "--min-vs"],
        "no_smoothing": ["-ns", "--no-smoothing"],
        "ely_taper": ["-et", "--ely-taper"],
        "taper_depth": ["-td", "--taper-depth"],
        "taper_models": ["-tm", "--taper-models"],
        "local_vparam_insert": ["-lv", "--local-vparam-insert"],
        "config": ["-c", "--config"],
    }


def test_sub_sgt_wf_dispatches_directly_to_workflow_placeholder(
    tmp_path: Path, mocker: MockerFixture
) -> None:
    create_workflow = _stub_workflow_generator(
        mocker,
        class_path="cybershake.__main__.sgt_workflow.SGT",
        method_name="create_awp",
        side_effect=lambda **_options: None,
    )
    grid_output_file = tmp_path / "gridout_TEST"
    workflow_file = tmp_path / "AWP_SGT_TEST.dax"

    result = CliRunner().invoke(
        cybershake,
        [
            "sub-sgt-wf",
            "-r",
            "73",
            "-gf",
            str(grid_output_file),
            "-o",
            str(workflow_file),
            "-sm",
            "-hd",
            "-sp",
            "0.08",
            "-mv",
            "400.0",
            "-ns",
            "-hf",
            "0.25",
            "-et",
            "ifless",
            "-td",
            "700.0",
            "-tm",
            "sfcvm,cca,bbp1d",
            "-z",
            "-lv",
        ],
    )

    assert result.exit_code == 0
    assert result.output.endswith(
        f"✓ Created SGT sub-workflow: run=73 file={workflow_file}\n"
    )
    create_workflow.assert_called_once_with(
        run_id=73,
        grid_output_file=grid_output_file,
        workflow_file=workflow_file,
        config=ANY,
    )


def test_sgt_config_rejects_directory(tmp_path: Path) -> None:
    result = CliRunner().invoke(
        cybershake,
        ["sgt", *SGT_REQUIRED_ARGS[1:], "--config", str(tmp_path)],
    )

    assert result.exit_code == 2
    assert "is a directory" in result.output


def test_sgt_config_rejects_missing_and_accepts_empty_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
) -> None:
    _stub_sgt_dependencies(mocker)
    missing = tmp_path / "missing.ini"
    empty = tmp_path / "empty.ini"
    empty.touch()
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)

    missing_result = runner.invoke(
        cybershake,
        ["sgt", *SGT_REQUIRED_ARGS[1:], "--config", str(missing)],
    )
    empty_result = runner.invoke(
        cybershake,
        ["sgt", *SGT_REQUIRED_ARGS[1:], "--config", str(empty)],
    )

    assert missing_result.exit_code == 2
    assert "does not exist" in missing_result.output
    assert empty_result.exit_code == 0


def test_pp_exposes_migrated_options() -> None:
    result = CliRunner().invoke(cybershake, ["pp", "--help"])
    command = pp

    assert result.exit_code == 0
    assert {parameter.name: parameter.opts for parameter in command.params} == {
        "velocity_model": ["-v", "--velocity-model"],
        "erf_id": ["-e", "--erf-id"],
        "rupture_variation_id": ["-r", "--rupture-variation-id"],
        "sgt_variation_id": ["-g", "--sgt-variation-id"],
        "frequency": ["-f", "--frequency"],
        "source_frequency": ["-q", "--source-frequency"],
        "site": ["-s", "--site"],
        "no_insert": ["-ni", "--no-insert"],
        "high_frequency": ["-hf", "--high-frequency"],
        "no_mpi_cluster": ["-nc", "--no-mpi-cluster"],
        "dir_hierarchy": ["-dh", "--dir-hierarchy"],
        "rotd": ["-R", "--rotd"],
        "skip_md5": ["-k", "--skip-md5"],
        "nonblocking_md5": ["-nb", "--nonblocking-md5"],
        "debug": ["-d", "--debug"],
        "pp_site": ["-ps", "--pp-site"],
        "spacing": ["-sp", "--spacing"],
        "server": ["-sr", "--server"],
        "duration": ["-du", "--duration"],
        "seismogram_length": ["-sl", "--seismogram-length"],
        "db_rv_seed": ["-dbrs", "--db-rv-seed"],
        "z_comp": ["-z", "--z-comp"],
        "handoff": ["-hd", "--handoff"],
        "period_duration": ["-pd", "--period-duration"],
        "no_vertical_response": ["-nvr", "--no-vertical-response"],
        "rupture_list": ["-rl", "--rupture-list"],
        "config": ["-c", "--config"],
    }


def test_pp_migrates_shell_orchestration(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
) -> None:
    find_run, valid_run, create_workflow, edit_run = _stub_pp_dependencies(mocker)
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(
        cybershake,
        [
            *PP_REQUIRED_ARGS,
            "-q",
            "2.0",
            "-nb",
            "--rotd",
            "-du",
            "-dbrs",
            "-sp",
            "0.08",
            "-z",
            "-hd",
            "-pd",
        ],
    )

    assert result.exit_code == 0
    assert result.output.endswith(
        "✓ Created PP workflow: run=73 "
        f"file={tmp_path / 'TEST_PP_dax/run_73/CyberShake_TEST.dax'}\n"
    )
    assert Path("TEST_PP_dax/run_table.txt").read_text() == (
        "73 TEST 64 8 10 15 1.0 2.0\n"
    )
    assert Path("TEST_PP_dax/run_73/placeholder.yml").read_text() == (
        "placeholder workflow\n"
    )
    find_run.assert_called_once_with(
        site="TEST",
        erf_id=64,
        sgt_variation_id=8,
        rupture_variation_id=10,
        velocity_model_id=15,
        frequency=1.0,
        source_frequency=2.0,
        server="moment",
        config=ANY,
    )
    valid_run.assert_not_called()
    assert create_workflow.call_args.kwargs == {
        "run_id": 73,
        "run_directory": Path("TEST_PP_dax/run_73"),
        "config": ANY,
    }
    edit_run.assert_called_once_with(
        run_id=73,
        fields={"Comment": "PP DAX created"},
        server="moment",
        config=ANY,
    )


def test_pp_reuses_valid_legacy_run_table(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
) -> None:
    find_run, valid_run, create_workflow, _edit_run = _stub_pp_dependencies(mocker)
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)

    run_directory = Path("TEST_PP_dax")
    run_directory.mkdir()
    (run_directory / "run_table.txt").write_text(
        "73 TEST 64 8 10 15 1.0\n", encoding="utf-8"
    )

    result = runner.invoke(cybershake, PP_REQUIRED_ARGS)

    assert result.exit_code == 0
    find_run.assert_not_called()
    valid_run.assert_called_once_with(
        run_id=73,
        site="TEST",
        stage="PP_PLAN",
        server="moment",
        config=ANY,
    )
    assert create_workflow.call_args.kwargs["run_id"] == 73


def test_pp_fails_when_final_edit_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
) -> None:
    _find_run, _valid_run, _create_workflow, edit_run = _stub_pp_dependencies(mocker)
    edit_run.side_effect = RunManagerError
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(cybershake, PP_REQUIRED_ARGS)

    assert result.exit_code == 1
    assert "Unable to update comment" in result.output


def test_pp_high_frequency_cutoff_is_optional(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
) -> None:
    _stub_pp_dependencies(mocker)
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)

    bare_result = runner.invoke(
        cybershake,
        [*PP_REQUIRED_ARGS, "-hf", "-nb"],
    )
    explicit_result = runner.invoke(
        cybershake,
        [*PP_REQUIRED_ARGS, "--high-frequency", "2.0"],
    )

    assert bare_result.exit_code == 0
    assert explicit_result.exit_code == 0


def test_stoch_exposes_migrated_options() -> None:
    result = CliRunner().invoke(cybershake, ["stoch", "--help"])
    command = stoch

    assert result.exit_code == 0
    assert {parameter.name: parameter.opts for parameter in command.params} == {
        "low_frequency_run_id": ["-l", "--low-frequency-run-id"],
        "erf_id": ["-e", "--erf-id"],
        "rupture_variation_id": ["-r", "--rupture-variation-id"],
        "merge_frequency": ["--merge-frequency"],
        "site": ["-s", "--site"],
        "generator_merge_frequency": ["-mf", "--generator-merge-frequency"],
        "stoch_frequency": ["-sf", "--stoch-frequency"],
        "no_rotd": ["-nr", "--no-rotd"],
        "no_site_response": ["-nsr", "--no-site-response"],
        "no_low_site_response": ["-nls", "--no-low-site-response"],
        "no_duration": ["-nd", "--no-duration"],
        "force_vs30": ["-fvs", "--force-vs30"],
        "debug": ["-d", "--debug"],
        "server": ["-sr", "--server"],
        "h_fraction": ["-hf", "--h-fraction"],
        "db_rv_seed": ["-dbrs", "--db-rv-seed"],
        "hf_vel_model": ["-hfv", "--hf-vel-model"],
        "period_duration": ["-pd", "--period-duration"],
        "z_comp": ["-z", "--z-comp"],
        "no_vertical_response": ["-nvr", "--no-vertical-response"],
        "rupture_list": ["-rl", "--rupture-list"],
        "config": ["-c", "--config"],
    }


def test_stoch_migrates_shell_orchestration(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
) -> None:
    find_run, create_run, valid_run, create_workflow = _stub_stoch_dependencies(mocker)
    rupture_list = tmp_path / "ruptures.csv"
    monkeypatch.chdir(tmp_path)

    result = CliRunner().invoke(
        cybershake,
        [
            *STOCH_REQUIRED_ARGS,
            "--h-fraction",
            "0.25",
            "-mf",
            "1.5",
            "-sf",
            "50.0",
            "-nr",
            "-nsr",
            "-dbrs",
            "-nls",
            "-nd",
            "-fvs",
            "760.0",
            "-d",
            "-pd",
            "--hf-vel-model",
            "nocal",
            "-z",
            "-nvr",
            "-rl",
            str(rupture_list),
        ],
    )

    assert result.exit_code == 0
    assert result.output.endswith(
        "✓ Created stochastic workflow: run=73 low_frequency_run=10 "
        f"file={tmp_path / 'TEST_Stoch_dax/run_73/CyberShake_Stoch_TEST_top.dax'}\n"
    )
    assert Path("TEST_Stoch_dax/run_table.txt").read_text() == (
        "73 TEST 10 64 10 1.0 50.0\n"
    )
    assert Path("TEST_Stoch_dax/run_73/placeholder.yml").read_text() == (
        "placeholder workflow\n"
    )
    find_run.assert_called_once_with(
        site="TEST",
        low_frequency_run_id=10,
        erf_id=64,
        rupture_variation_id=10,
        merge_frequency=1.0,
        stoch_frequency=50.0,
        server="moment",
        config=ANY,
    )
    create_run.assert_not_called()
    valid_run.assert_not_called()
    assert create_workflow.call_args.kwargs == {
        "run_id": 73,
        "run_directory": Path("TEST_Stoch_dax/run_73"),
        "low_frequency_run_id": 10,
        "config": ANY,
    }


def test_stoch_creates_run_when_initial_find_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
) -> None:
    find_run, create_run, _valid_run, create_workflow = _stub_stoch_dependencies(mocker)
    find_run.side_effect = RunManagerError
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(cybershake, STOCH_REQUIRED_ARGS)

    assert result.exit_code == 0
    create_run.assert_called_once_with(
        site="TEST",
        low_frequency_run_id=10,
        erf_id=64,
        rupture_variation_id=10,
        merge_frequency=1.0,
        stoch_frequency=DEFAULT_STOCH_FREQUENCY,
        server="moment",
        config=ANY,
    )
    assert Path("TEST_Stoch_dax/run_table.txt").read_text() == (
        "74 TEST 10 64 10 1.0 10.0\n"
    )
    assert create_workflow.call_args.kwargs["run_id"] == 74


def test_stoch_reuses_valid_legacy_run_table(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
) -> None:
    find_run, create_run, valid_run, create_workflow = _stub_stoch_dependencies(mocker)
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)
    run_directory = Path("TEST_Stoch_dax")
    run_directory.mkdir()
    (run_directory / "run_table.txt").write_text(
        "73 TEST 10 64 10 1.0 10.0\n", encoding="utf-8"
    )

    result = runner.invoke(cybershake, STOCH_REQUIRED_ARGS)

    assert result.exit_code == 0
    find_run.assert_not_called()
    create_run.assert_not_called()
    valid_run.assert_called_once_with(
        run_id=73,
        site="TEST",
        stage="PP_PLAN",
        server="moment",
        config=ANY,
    )
    assert create_workflow.call_args.kwargs["run_id"] == 73


def test_stoch_creates_run_when_matching_table_run_is_invalid(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
) -> None:
    find_run, create_run, valid_run, create_workflow = _stub_stoch_dependencies(mocker)
    valid_run.side_effect = RunManagerError
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)
    run_directory = Path("TEST_Stoch_dax")
    run_directory.mkdir()
    run_file = run_directory / "run_table.txt"
    run_file.write_text("73 TEST 10 64 10 1.0 10.0\n", encoding="utf-8")

    result = runner.invoke(cybershake, STOCH_REQUIRED_ARGS)

    assert result.exit_code == 0
    find_run.assert_not_called()
    create_run.assert_called_once()
    assert run_file.read_text() == (
        "73 TEST 10 64 10 1.0 10.0\n74 TEST 10 64 10 1.0 10.0\n"
    )
    assert create_workflow.call_args.kwargs["run_id"] == 74


def test_stoch_accepts_long_stochastic_frequency_alias(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
) -> None:
    _stub_stoch_dependencies(mocker)
    monkeypatch.chdir(tmp_path)

    result = CliRunner().invoke(
        cybershake,
        [*STOCH_REQUIRED_ARGS, "--stoch-frequency", "25.0"],
    )

    assert result.exit_code == 0


def test_stoch_frequency_default_is_unset_only_for_full() -> None:
    stoch_command = stoch
    full_command = full
    stoch_frequency = next(
        parameter
        for parameter in stoch_command.params
        if parameter.name == "stoch_frequency"
    )
    full_stoch_frequency = next(
        parameter
        for parameter in full_command.params
        if parameter.name == "stoch_frequency"
    )

    assert stoch_frequency.default == DEFAULT_STOCH_FREQUENCY
    assert full_stoch_frequency.default is None


def test_sub_stoch_wf_exposes_migrated_options() -> None:
    result = CliRunner().invoke(cybershake, ["sub-stoch-wf", "--help"])

    assert result.exit_code == 0
    assert {parameter.name: parameter.opts for parameter in sub_stoch_wf.params} == {
        "run_id": ["-r", "--run-id"],
        "low_frequency_run_id": ["-lr", "--low-frequency-run-id"],
        "velocity_file": ["-v", "--velocity-file"],
        "workflow_file": ["-o", "--workflow-file"],
        "server": ["-sr", "--server"],
        "z_comp": ["-z", "--z-comp"],
        "debug": ["-d", "--debug"],
        "db_rv_seed": ["-dbrs", "--db-rv-seed"],
        "period_duration": ["-pd", "--period-duration"],
        "no_vertical_response": ["-nvr", "--no-vertical-response"],
        "rupture_list": ["-rl", "--rupture-list"],
        "generator_merge_frequency": ["-mf", "--generator-merge-frequency"],
        "no_rotd": ["-nr", "--no-rotd"],
        "no_site_response": ["-nsr", "--no-site-response"],
        "no_low_site_response": ["-nls", "--no-low-site-response"],
        "no_duration": ["-nd", "--no-duration"],
        "hf_vel_model": ["-hfv", "--hf-vel-model"],
        "config": ["-c", "--config"],
    }


def test_sub_stoch_wf_dispatches_directly_to_workflow_placeholder(
    tmp_path: Path, mocker: MockerFixture
) -> None:
    create_workflow = _stub_workflow_generator(
        mocker,
        class_path="cybershake.__main__.stoch_workflow.Stoch",
        method_name="create_sub_stoch",
        side_effect=lambda **_options: None,
    )
    velocity_file = tmp_path / "velocity_info_TEST.txt"
    workflow_file = tmp_path / "CyberShake_Stoch_TEST.dax"
    rupture_list = tmp_path / "ruptures.csv"

    result = CliRunner().invoke(
        cybershake,
        [
            "sub-stoch-wf",
            "-r",
            "74",
            "-lr",
            "73",
            "-v",
            str(velocity_file),
            "-o",
            str(workflow_file),
            "-mf",
            "1.0",
            "-nr",
            "-nsr",
            "-nls",
            "-nd",
            "-d",
            "-dbrs",
            "-hfv",
            "nocal",
            "-pd",
            "-z",
            "-nvr",
            "-rl",
            str(rupture_list),
        ],
    )

    assert result.exit_code == 0
    assert result.output.endswith(
        "✓ Created stochastic sub-workflow: run=74 low_frequency_run=73 "
        f"file={workflow_file}\n"
    )
    create_workflow.assert_called_once_with(
        run_id=74,
        low_frequency_run_id=73,
        velocity_file=velocity_file,
        workflow_file=workflow_file,
        config=ANY,
    )


def test_full_exposes_migrated_options() -> None:
    result = CliRunner().invoke(cybershake, ["full", "--help"])
    command = full

    assert result.exit_code == 0
    assert {parameter.name: parameter.opts for parameter in command.params} == {
        "velocity_model": ["-v", "--velocity-model"],
        "erf_id": ["-e", "--erf-id"],
        "rupture_variation_id": ["-r", "--rupture-variation-id"],
        "sgt_variation_id": ["-g", "--sgt-variation-id"],
        "frequency": ["-f", "--frequency"],
        "source_frequency": ["-q", "--source-frequency"],
        "site": ["-s", "--site"],
        "broadband": ["-bb", "--broadband"],
        "server": ["-sr", "--server"],
        "separate_md5": ["-sm", "--separate-md5"],
        "sgt_site": ["-ss", "--sgt-site"],
        "sgt_spacing": ["--sgt-spacing"],
        "pp_spacing": ["--pp-spacing"],
        "min_vs": ["-mv", "--min-vs"],
        "no_smoothing": ["-ns", "--no-smoothing"],
        "depth": ["-dep", "--depth"],
        "h_fraction": ["-hf", "--h-fraction"],
        "rotation": ["-rt", "--rotation"],
        "ely_taper": ["-et", "--ely-taper"],
        "taper_depth": ["-td", "--taper-depth"],
        "taper_models": ["-tm", "--taper-models"],
        "local_vparam_insert": ["-lv", "--local-vparam-insert"],
        "no_insert": ["-ni", "--no-insert"],
        "high_frequency": ["--high-frequency"],
        "no_mpi_cluster": ["-nc", "--no-mpi-cluster"],
        "dir_hierarchy": ["-dh", "--dir-hierarchy"],
        "rotd": ["-R", "--rotd"],
        "skip_md5": ["-k", "--skip-md5"],
        "nonblocking_md5": ["-nb", "--nonblocking-md5"],
        "debug": ["-d", "--debug"],
        "pp_site": ["-ps", "--pp-site"],
        "duration": ["-du", "--duration"],
        "seismogram_length": ["-sl", "--seismogram-length"],
        "generator_merge_frequency": ["-mf", "--generator-merge-frequency"],
        "stoch_frequency": ["-sf", "--stoch-frequency"],
        "no_site_response": ["-nsr", "--no-site-response"],
        "no_low_site_response": ["-nls", "--no-low-site-response"],
        "force_vs30": ["-fvs", "--force-vs30"],
        "db_rv_seed": ["-dbrs", "--db-rv-seed"],
        "hf_vel_model": ["-hfv", "--hf-vel-model"],
        "period_duration": ["-pd", "--period-duration"],
        "z_comp": ["-z", "--z-comp"],
        "handoff": ["-hd", "--handoff"],
        "no_vertical_response": ["-nvr", "--no-vertical-response"],
        "rupture_list": ["-rl", "--rupture-list"],
        "config": ["-c", "--config"],
    }


def test_full_migrates_shell_orchestration(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
) -> None:
    create_run, valid_run, create_workflow, edit_run = _stub_full_dependencies(mocker)
    rupture_list = tmp_path / "ruptures.csv"
    monkeypatch.chdir(tmp_path)

    result = CliRunner().invoke(
        cybershake,
        [
            *FULL_REQUIRED_ARGS,
            "-q",
            "2.0",
            "-bb",
            "-sm",
            "-lv",
            "-ss",
            "frontier",
            "--server",
            "moment",
            "-hf",
            "0.25",
            "-mv",
            "400.0",
            "-ns",
            "-dep",
            "50.0",
            "-rt",
            "-36.0",
            "-et",
            "ifless",
            "-td",
            "700.0",
            "-tm",
            "sfcvm,cca,bbp1d",
            "-z",
            "-hd",
            "-ni",
            "--high-frequency",
            "2.0",
            "-nc",
            "-dh",
            "-k",
            "-nb",
            "-no-r",
            "-d",
            "-ps",
            "frontier",
            "-no-du",
            "-sl",
            "500.0",
            "-dbrs",
            "--sgt-spacing",
            "0.12",
            "--pp-spacing",
            "0.08",
            "-mf",
            "1.5",
            "-sf",
            "50.0",
            "-nsr",
            "-nls",
            "-fvs",
            "760.0",
            "-pd",
            "--hf-vel-model",
            "nocal",
            "-nvr",
            "-rl",
            str(rupture_list),
        ],
    )

    assert result.exit_code == 0
    workflow_file = (
        tmp_path / "TEST_Integrated_dax/run_73/CyberShake_Integrated_TEST.dax"
    )
    assert result.output.endswith(
        f"✓ Created full workflow: run=73 broadband_run=74 file={workflow_file}\n"
    )
    assert Path("TEST_Integrated_dax/run_table.txt").read_text() == (
        "73 TEST 64 8 10 15 1.0 2.0\n74 TEST 64 8 10 15 1.0 2.0 50.0\n"
    )
    output_directory = Path("TEST_Integrated_dax/run_73")
    assert (output_directory / "sites.list").read_text() == "TEST\n"
    assert (output_directory / "linked_bb_run_id.txt").read_text() == "74\n"
    assert (output_directory / "CyberShake_Integrated_TEST.dax").read_text() == (
        "placeholder workflow\n"
    )
    assert create_run.call_args_list == [
        call(
            site="TEST",
            erf_id=64,
            sgt_variation_id=8,
            velocity_model_id=15,
            rupture_variation_id=10,
            frequency=1.0,
            source_frequency=2.0,
            stoch_frequency=None,
            server="moment",
            config=ANY,
        ),
        call(
            site="TEST",
            erf_id=64,
            sgt_variation_id=8,
            velocity_model_id=15,
            rupture_variation_id=10,
            frequency=1.0,
            source_frequency=2.0,
            stoch_frequency=50.0,
            server="moment",
            config=ANY,
        ),
    ]
    valid_run.assert_not_called()
    assert create_workflow.call_args.kwargs == {
        "workflow_file": output_directory / "CyberShake_Integrated_TEST.dax",
        "run_directory": output_directory,
        "run_ids": (73,),
        "broadband_run_id": 74,
        "config": ANY,
    }
    full_config = create_workflow.call_args.kwargs["config"]
    assert (full_config.rotd, full_config.no_rotd) == (False, True)
    assert (full_config.duration, full_config.no_duration) == (False, True)
    assert edit_run.call_args_list == [
        call(
            run_id=73,
            fields={
                "Status": "Initial",
                "Comment": "SGT DAX created",
                "Job_ID": None,
                "Submit_Dir": None,
            },
            server="moment",
            config=ANY,
        ),
        call(
            run_id=74,
            fields={
                "Status": "Initial",
                "Comment": "SGT DAX created",
                "Job_ID": None,
                "Submit_Dir": None,
            },
            server="moment",
            config=ANY,
        ),
    ]


def test_full_reuses_valid_deterministic_and_broadband_runs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
) -> None:
    create_run, valid_run, create_workflow, _edit_run = _stub_full_dependencies(mocker)
    monkeypatch.chdir(tmp_path)
    run_directory = Path("TEST_Integrated_dax")
    run_directory.mkdir()
    (run_directory / "run_table.txt").write_text(
        "73 TEST 64 8 10 15 1.0 2.0\n74 TEST 64 8 10 15 1.0 2.0 50.0\n",
        encoding="utf-8",
    )

    result = CliRunner().invoke(
        cybershake,
        [*FULL_REQUIRED_ARGS, "-q", "2.0", "-bb", "-sf", "50.0"],
    )

    assert result.exit_code == 0
    create_run.assert_not_called()
    assert valid_run.call_args_list == [
        call(
            run_id=73,
            site="TEST",
            stage="SGT_PLAN",
            server="moment",
            config=ANY,
        ),
        call(
            run_id=74,
            site="TEST",
            stage="SGT_PLAN",
            server="moment",
            config=ANY,
        ),
    ]
    assert create_workflow.call_args.kwargs["run_ids"] == (73,)
    assert create_workflow.call_args.kwargs["broadband_run_id"] == 74


def test_full_continues_when_final_edits_fail(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
) -> None:
    _create_run, _valid_run, _create_workflow, edit_run = _stub_full_dependencies(
        mocker
    )
    edit_run.side_effect = (RunManagerError, None)
    monkeypatch.chdir(tmp_path)

    result = CliRunner().invoke(cybershake, [*FULL_REQUIRED_ARGS, "-sf", "50.0"])

    assert result.exit_code == 0
    assert "Unable to update Status" in result.output
    assert [edit_call.kwargs["run_id"] for edit_call in edit_run.call_args_list] == [
        73,
        74,
    ]


def test_full_pp_high_frequency_cutoff_is_optional(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
) -> None:
    _stub_full_dependencies(mocker, run_ids=(73,))
    monkeypatch.chdir(tmp_path)
    bare_result = CliRunner().invoke(
        cybershake,
        [*FULL_REQUIRED_ARGS, "--high-frequency", "-nb"],
    )
    explicit_result = CliRunner().invoke(
        cybershake,
        [*FULL_REQUIRED_ARGS, "--high-frequency", "2.0"],
    )

    assert bare_result.exit_code == 0
    assert explicit_result.exit_code == 0
