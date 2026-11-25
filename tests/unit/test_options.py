"""Tests for reusable Click option behavior."""

from collections.abc import Iterator
from io import StringIO
from pathlib import Path

import pytest
from click.testing import CliRunner

import rich_click as click

from cybershake import properties
from cybershake.__main__ import cybershake, full, pp, stoch, sub_stoch_wf
from cybershake.config import CONFIG_DEFAULTS, Config
from cybershake.constants import CLI_ENVVAR_PREFIX
from cybershake.options import config_option, envvar_option


def _commands(command: click.Command) -> Iterator[click.Command]:
    """Yield a command and all of its nested commands."""
    yield command
    if isinstance(command, click.Group):
        for child in command.commands.values():
            yield from _commands(child)


def _config_command() -> click.Command:
    """Create a command exercising configuration defaults."""

    @click.command()
    @envvar_option("--site", required=True)
    @envvar_option("--high-frequency", type=float, default=1.0)
    @envvar_option("--enabled", is_flag=True)
    @config_option
    def command(
        *, site: str, high_frequency: float, enabled: bool, config: Config
    ) -> None:
        click.echo(
            f"{site}|{high_frequency}|{enabled}|{config.source}|"
            f"{config['site']}|{config['high_frequency']}|{len(config)}"
        )

    return command


def test_every_cli_option_has_a_long_form_envvar() -> None:
    options = [
        parameter
        for command in _commands(cybershake)
        for parameter in command.params
        if isinstance(parameter, click.Option)
    ]

    assert options
    for option in options:
        long_options = [name for name in option.opts if name.startswith("--")]
        assert len(long_options) == 1
        expected = (
            f"{CLI_ENVVAR_PREFIX}_{long_options[0][2:].upper().replace('-', '_')}"
        )
        assert option.envvar == expected
        assert option.show_envvar is True


@pytest.mark.parametrize("command", [pp, stoch, sub_stoch_wf, full])
def test_database_seeds_default_on_and_can_be_disabled(command: click.Command) -> None:
    default = click.Context(command, resilient_parsing=True)
    command.parse_args(default, [])
    disabled = click.Context(command, resilient_parsing=True)
    command.parse_args(disabled, ["-no-dbrs"])

    assert default.params["db_rv_seed"] is True
    assert disabled.params["db_rv_seed"] is False


@pytest.mark.parametrize("command", [pp, full])
@pytest.mark.parametrize("flags", [("-no-r", "-no-du"), ("--no-rotd", "--no-duration")])
def test_pp_rotd_and_duration_default_on_and_can_be_disabled(
    command: click.Command, flags: tuple[str, str]
) -> None:
    default = click.Context(command, resilient_parsing=True)
    command.parse_args(default, [])
    disabled = click.Context(command, resilient_parsing=True)
    command.parse_args(disabled, list(flags))

    assert default.params["rotd"] is True
    assert default.params["duration"] is True
    assert disabled.params["rotd"] is False
    assert disabled.params["duration"] is False
    if command is full:
        assert "no_rotd" not in disabled.params
        assert "no_duration" not in disabled.params


def test_envvar_supplies_an_option_value() -> None:
    @click.command()
    @envvar_option("--high-frequency", type=float, required=True)
    def command(*, high_frequency: float) -> None:
        click.echo(high_frequency)

    result = CliRunner().invoke(
        command,
        env={"CYBERSHAKE_HIGH_FREQUENCY": "2.5"},
    )

    assert result.exit_code == 0
    assert result.output == "2.5\n"


def test_envvar_option_supports_a_workflow_scope() -> None:
    @click.command()
    @envvar_option("--properties", envvar_scope="sgt")
    def command(*, properties: str | None) -> None:
        click.echo(properties)

    result = CliRunner().invoke(
        command,
        env={"CYBERSHAKE_SGT_PROPERTIES": "properties.sgt"},
    )

    assert result.exit_code == 0
    assert result.output == "properties.sgt\n"


def test_config_file_sets_typed_option_defaults(tmp_path: Path) -> None:
    config = tmp_path / "defaults.conf"
    config.write_text(
        "# Workflow defaults\nsite = TEST\n--high-frequency = 2.5\nenabled = true\n",
        encoding="utf-8",
    )

    result = CliRunner().invoke(_config_command(), ["--config", str(config)])

    assert result.exit_code == 0
    assert result.output == (
        f"TEST|2.5|True|{config}|TEST|2.5|{len(CONFIG_DEFAULTS)}\n"
    )


def test_dumped_properties_file_supplies_config_defaults(tmp_path: Path) -> None:
    config = tmp_path / "defaults.conf"
    properties.dump(
        properties.load(StringIO("site=TEST\nhigh-frequency=2.5\n")),
        config,
    )

    result = CliRunner().invoke(_config_command(), ["--config", str(config)])

    assert result.exit_code == 0
    assert result.output == (
        f"TEST|2.5|False|{config}|TEST|2.5|{len(CONFIG_DEFAULTS)}\n"
    )


def test_config_file_rejects_duplicate_keys(tmp_path: Path) -> None:
    config = tmp_path / "defaults.conf"
    config.write_text("site=FIRST\nsite=TEST\n", encoding="utf-8")

    result = CliRunner(env={"COLUMNS": "500"}).invoke(
        _config_command(), ["--config", str(config)]
    )

    assert result.exit_code == 1
    assert "option 'site' in section 'DEFAULT' already exists" in result.output


def test_config_file_sets_hyphenated_deployment_defaults(tmp_path: Path) -> None:
    @click.command()
    @config_option
    def command(*, config: Config) -> None:
        click.echo(
            f"{config['output_directory']}|{config['database_port']}|"
            f"{config['database_host']}"
        )

    config = tmp_path / "defaults.conf"
    config.write_text(
        "output-directory=file-results\n"
        "database-port=3307\n"
        "database-host=file-server\n",
        encoding="utf-8",
    )

    result = CliRunner().invoke(command, ["--config", str(config)])
    overridden = CliRunner().invoke(
        command,
        ["--config", str(config)],
        env={"CYBERSHAKE_OUTPUT_DIRECTORY": "env-results"},
    )

    assert result.exit_code == 0
    assert result.output == "file-results|3307|file-server\n"
    assert overridden.exit_code == 0
    assert overridden.output == "env-results|3307|file-server\n"


def test_config_file_rejects_underscored_deployment_key(tmp_path: Path) -> None:
    config = tmp_path / "defaults.conf"
    config.write_text("output_directory=results\n", encoding="utf-8")

    result = CliRunner().invoke(_config_command(), ["--config", str(config)])

    assert result.exit_code == 1
    assert "Unknown option 'output_directory'" in result.output


def test_cli_and_environment_override_config_defaults(tmp_path: Path) -> None:
    config = tmp_path / "defaults.conf"
    config.write_text(
        "site=FILE\nhigh-frequency=1.5\n",
        encoding="utf-8",
    )

    result = CliRunner().invoke(
        _config_command(),
        ["--high-frequency", "3.5", "--config", str(config)],
        env={
            "CYBERSHAKE_SITE": "ENV",
            "CYBERSHAKE_HIGH_FREQUENCY": "2.5",
        },
    )

    assert result.exit_code == 0
    assert result.output == (f"ENV|3.5|False|{config}|ENV|3.5|{len(CONFIG_DEFAULTS)}\n")


def test_omitted_and_empty_config_files_are_valid(tmp_path: Path) -> None:
    empty = tmp_path / "empty.conf"
    empty.touch()

    omitted_result = CliRunner().invoke(_config_command(), ["--site", "TEST"])
    empty_result = CliRunner().invoke(
        _config_command(),
        ["--site", "TEST", "--config", str(empty)],
    )

    assert omitted_result.exit_code == 0
    assert omitted_result.output == (
        f"TEST|1.0|False|None|TEST|1.0|{len(CONFIG_DEFAULTS)}\n"
    )
    assert empty_result.exit_code == 0
    assert empty_result.output == (
        f"TEST|1.0|False|{empty}|TEST|1.0|{len(CONFIG_DEFAULTS)}\n"
    )


def test_provided_config_file_must_exist(tmp_path: Path) -> None:
    missing = tmp_path / "missing.conf"

    result = CliRunner().invoke(
        _config_command(),
        ["--site", "TEST", "--config", str(missing)],
    )

    assert result.exit_code == 2
    assert "does not exist" in result.output


def test_config_file_rejects_malformed_lines(tmp_path: Path) -> None:
    config = tmp_path / "defaults.conf"
    config.write_text("site TEST\n", encoding="utf-8")

    result = CliRunner().invoke(_config_command(), ["--config", str(config)])

    assert result.exit_code == 1
    assert "Unable to load configuration file" in result.output
    assert "site TEST" in result.output


def test_config_file_rejects_unknown_options(tmp_path: Path) -> None:
    config = tmp_path / "defaults.conf"
    config.write_text("unknown=value\n", encoding="utf-8")

    result = CliRunner().invoke(_config_command(), ["--config", str(config)])

    assert result.exit_code == 1
    assert "Unknown option 'unknown'" in result.output
