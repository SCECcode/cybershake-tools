"""Tests for the canonical workflow configuration catalog."""

from __future__ import annotations

import inspect
import json
from dataclasses import FrozenInstanceError, is_dataclass
from pathlib import Path

import click
import pytest

from cybershake.__main__ import cybershake
from cybershake.config import (
    CONFIG_DEFAULTS,
    DEPLOYMENT_CONFIG_DEFAULTS,
    WORKFLOW_CONFIG_DEFAULTS,
    Config,
    resolve_config,
)
from cybershake.constants import DEFAULT_DB_SERVER, DEFAULT_STOCH_FREQUENCY
from cybershake.exc import ConfigError
from cybershake.version import __version__


def _command(path: str) -> click.Command:
    command: click.Command = cybershake
    for component in path.split("."):
        assert isinstance(command, click.Group)
        command = command.commands[component]
    return command


@pytest.mark.parametrize("path", WORKFLOW_CONFIG_DEFAULTS)
def test_configuration_keys_match_command_callbacks(path: str) -> None:
    callback = _command(path).callback

    assert callback is not None
    assert set(WORKFLOW_CONFIG_DEFAULTS[path]) == set(
        inspect.signature(callback).parameters
    )


def test_configuration_catalog_records_command_specific_defaults() -> None:
    for defaults in WORKFLOW_CONFIG_DEFAULTS.values():
        assert defaults["server"] == DEFAULT_DB_SERVER
        assert defaults["config"] is None

    assert (
        WORKFLOW_CONFIG_DEFAULTS["stoch"]["stoch_frequency"] == DEFAULT_STOCH_FREQUENCY
    )
    assert WORKFLOW_CONFIG_DEFAULTS["full"]["stoch_frequency"] is None


def test_configuration_defaults_are_serializable() -> None:
    assert json.loads(json.dumps(WORKFLOW_CONFIG_DEFAULTS)) == WORKFLOW_CONFIG_DEFAULTS
    assert json.loads(json.dumps(CONFIG_DEFAULTS)) == CONFIG_DEFAULTS


def test_resolved_configuration_always_contains_every_key() -> None:
    config = resolve_config(
        "sgt",
        {
            "site": "TEST",
            "frequency": 1.0,
            "config": None,
            "server": "database.example",
        },
        environ={},
    )

    assert isinstance(config, Config)
    assert is_dataclass(config)
    assert config.command == "sgt"
    assert set(config) == set(CONFIG_DEFAULTS)
    assert config["site"] == "TEST"
    assert config["frequency"] == 1.0
    assert config["merge_frequency"] is None
    assert config["database_host"] == "database.example"
    assert config["cybershake_version"] == __version__
    assert config.source is None
    assert json.loads(json.dumps(config.as_dict())) == config.as_dict()

    with pytest.raises(FrozenInstanceError):
        config.__setattr__("command", "pp")


def test_from_dict_converts_values_from_field_types() -> None:
    config = Config.from_dict(
        {
            "erf_id": "64",
            "frequency": "1.0",
            "no_smoothing": "false",
            "database_port": "3307",
            "output_directory": "results",
            "config": "defaults.conf",
        },
        command="sgt",
    )

    assert config.erf_id == 64
    assert config.frequency == 1.0
    assert config.no_smoothing is False
    assert config.database_port == 3307
    assert config.output_directory == Path("results")
    assert config.source == Path("defaults.conf")
    assert config.as_dict()["output_directory"] == "results"
    assert config["no_smoothing"] is False


def test_from_dict_rejects_unknown_and_invalid_values() -> None:
    with pytest.raises(ConfigError, match="Unknown configuration key: unknown"):
        Config.from_dict({"unknown": "value"})
    with pytest.raises(ConfigError, match="Invalid configuration"):
        Config.from_dict({"database_port": '"3307"'})


def test_resolved_configuration_uses_command_specific_defaults() -> None:
    pp = resolve_config("pp", {}, environ={})
    stoch = resolve_config("stoch", {}, environ={})
    full = resolve_config("full", {}, environ={})

    assert pp.direct_synth is True
    assert full.direct_synth is True
    assert stoch["stoch_frequency"] == DEFAULT_STOCH_FREQUENCY
    assert full["stoch_frequency"] is None


def test_full_uses_the_same_rotd_and_duration_settings_for_both_stages() -> None:
    enabled = resolve_config("full", {}, environ={})
    disabled = resolve_config("full", {"rotd": False, "duration": False}, environ={})

    assert (enabled.rotd, enabled.no_rotd, enabled.duration, enabled.no_duration) == (
        True,
        False,
        True,
        False,
    )
    assert (
        disabled.rotd,
        disabled.no_rotd,
        disabled.duration,
        disabled.no_duration,
    ) == (
        False,
        True,
        False,
        True,
    )


def test_deployment_environment_overrides_defaults() -> None:
    config = resolve_config(
        "sgt",
        {"server": "cli-server"},
        environ={
            "CYBERSHAKE_OUTPUT_DIRECTORY": "/runs",
            "CYBERSHAKE_DB_HOST": "environment-server",
            "CYBERSHAKE_DB_PORT": "3307",
            "CYBERSHAKE_DB_CREDENTIALS": "/secure/db.txt",
            "CYBERSHAKE_RC_DB": "/catalog/rc.sqlite",
            "CYBERSHAKE_VS30_CLASSPATH": "/opensha/*",
        },
    )

    assert config["output_directory"] == "/runs"
    assert config["database_host"] == "environment-server"
    assert config["database_port"] == 3307
    assert config["database_credentials"] == "/secure/db.txt"
    assert config["replica_catalog_database"] == "/catalog/rc.sqlite"
    assert config["vs30_classpath"] == "/opensha/*"
    assert set(DEPLOYMENT_CONFIG_DEFAULTS) <= set(config)


def test_deployment_file_values_are_typed_and_environment_overrides_them() -> None:
    config = resolve_config(
        "sgt",
        {"server": "cli-server"},
        deployment_defaults={
            "output_directory": "file-results",
            "database_host": "file-server",
            "database_port": "3307",
        },
        environ={"CYBERSHAKE_OUTPUT_DIRECTORY": "env-results"},
    )

    assert config["output_directory"] == "env-results"
    assert config["database_host"] == "file-server"
    assert config["database_port"] == 3307


def test_invalid_typed_deployment_environment_is_rejected() -> None:
    with pytest.raises(ConfigError, match="CYBERSHAKE_DB_PORT"):
        resolve_config(
            "sgt",
            {},
            environ={"CYBERSHAKE_DB_PORT": "not-an-integer"},
        )


def test_invalid_typed_deployment_file_value_is_rejected() -> None:
    with pytest.raises(ConfigError, match="database-port must be an integer"):
        resolve_config(
            "sgt",
            {},
            deployment_defaults={"database_port": "not-an-integer"},
            environ={},
        )
