"""Resolve complete, serializable CyberShake configurations.

The flat ``--config`` file supplies defaults only for options on the active
command.  Click resolves those file values together with option environment
variables and explicit command-line arguments.  This module then combines the
resolved command values with deployment settings and built-in defaults.  Every
result contains the same key set so it can be serialized for reproducibility.
"""

from __future__ import annotations

import os
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, fields
from pathlib import Path
from typing import Final, TypeAlias, cast

import click
from dacite import Config as DaciteConfig
from dacite import from_dict
from dacite.exceptions import DaciteError

from cybershake.constants import (
    CLASSPATH_ENV,
    DB_CREDENTIALS_ENV,
    DB_HOST_ENV,
    DEFAULT_DB_CREDENTIALS,
    DEFAULT_DB_NAME,
    DEFAULT_DB_PORT,
    DEFAULT_DB_SERVER,
    DEFAULT_DB_USER,
    DEFAULT_REPLICA_CATALOG_DB,
    DEFAULT_STOCH_FREQUENCY,
    OPENSHA_DIR,
    REPLICA_CATALOG_DB_ENV,
)
from cybershake.exc import ConfigError
from cybershake.version import __version__

ConfigValue: TypeAlias = bool | float | int | str | None

_CONFIG = ("config",)
_COMMON = ("erf_id", "rupture_variation_id", "frequency", "site")
_LOW_FREQUENCY = ("velocity_model", "sgt_variation_id", "source_frequency")
_WORKFLOW = ("server", "z_comp")
_SGT_PP = ("spacing", "handoff")
_SGT_STOCH = ("h_fraction",)
_PP_STOCH = (
    "debug",
    "db_rv_seed",
    "period_duration",
    "no_vertical_response",
    "rupture_list",
)
_SGT_SUB_WORKFLOW = (
    "separate_md5",
    "min_vs",
    "no_smoothing",
    "ely_taper",
    "taper_depth",
    "taper_models",
    "local_vparam_insert",
)
_SGT = ("sgt_site", "depth", "rotation")
_PP = (
    "no_insert",
    "high_frequency",
    "no_mpi_cluster",
    "dir_hierarchy",
    "rotd",
    "skip_md5",
    "nonblocking_md5",
    "pp_site",
    "duration",
    "seismogram_length",
)
_STOCH_SUB_WORKFLOW = (
    "generator_merge_frequency",
    "no_rotd",
    "no_site_response",
    "no_low_site_response",
    "no_duration",
    "hf_vel_model",
)

_DEPLOYMENT_ENVVARS: Final[dict[str, str]] = {
    "output_directory": "CYBERSHAKE_OUTPUT_DIRECTORY",
    "database_host": DB_HOST_ENV,
    "database_port": "CYBERSHAKE_DB_PORT",
    "database_name": "CYBERSHAKE_DB_NAME",
    "database_user": "CYBERSHAKE_DB_USER",
    "database_credentials": DB_CREDENTIALS_ENV,
    "replica_catalog_database": REPLICA_CATALOG_DB_ENV,
    "vs30_classpath": CLASSPATH_ENV,
    "condor_config": "CONDOR_CONFIG",
    "globus_location": "GLOBUS_LOCATION",
}


@dataclass(frozen=True, slots=True, eq=False)
class Config(Mapping[str, ConfigValue]):
    """Typed, immutable resolved configuration for one command invocation."""

    command: str = ""
    erf_id: int | None = None
    rupture_variation_id: int | None = None
    frequency: float | None = None
    site: str | None = None
    velocity_model: str | None = None
    sgt_variation_id: int | None = None
    source_frequency: float | None = None
    server: str = DEFAULT_DB_SERVER
    z_comp: bool = False
    spacing: float | None = None
    handoff: bool = False
    h_fraction: float | None = None
    separate_md5: bool = False
    min_vs: float | None = None
    no_smoothing: bool = False
    ely_taper: str | None = None
    taper_depth: float | None = None
    taper_models: str | None = None
    local_vparam_insert: bool = False
    sgt_site: str | None = None
    depth: float | None = None
    rotation: float | None = None
    config: Path | None = None
    run_id: int | None = None
    grid_output_file: Path | None = None
    workflow_file: Path | None = None
    debug: bool = False
    db_rv_seed: bool = True
    period_duration: bool = False
    no_vertical_response: bool = False
    rupture_list: Path | None = None
    no_insert: bool = False
    high_frequency: float | None = None
    no_mpi_cluster: bool = False
    dir_hierarchy: bool = False
    rotd: bool = True
    skip_md5: bool = False
    nonblocking_md5: bool = False
    direct_synth: bool = True
    pp_site: str | None = None
    duration: bool = True
    seismogram_length: float | None = None
    low_frequency_run_id: int | None = None
    merge_frequency: float | None = None
    generator_merge_frequency: float | None = None
    no_rotd: bool = False
    no_site_response: bool = False
    no_low_site_response: bool = False
    no_duration: bool = False
    hf_vel_model: str | None = None
    stoch_frequency: float | None = None
    force_vs30: float | None = None
    velocity_file: Path | None = None
    broadband: bool = False
    sgt_spacing: float | None = None
    pp_spacing: float | None = None
    output_directory: Path = Path()
    database_host: str = DEFAULT_DB_SERVER
    database_port: int = DEFAULT_DB_PORT
    database_name: str = DEFAULT_DB_NAME
    database_user: str = DEFAULT_DB_USER
    database_credentials: Path = DEFAULT_DB_CREDENTIALS
    replica_catalog_database: Path = DEFAULT_REPLICA_CATALOG_DB
    vs30_classpath: str = str(OPENSHA_DIR / "jars" / "*")
    condor_config: str | None = None
    globus_location: str | None = None
    cybershake_version: str = __version__

    @classmethod
    def from_dict(
        cls,
        values: Mapping[str, object],
        *,
        command: str = "",
    ) -> Config:
        """Build a typed configuration from the field annotations and defaults.

        Args:
            values (Mapping[str, object]): Configuration values to override.
            command (str, optional): Active command name. Defaults to empty.

        Returns:
            Config: Typed resolved configuration.

        Raises:
            ConfigError: If a key is unknown or a value has the wrong type.
        """
        unknown = values.keys() - _CONFIG_KEY_SET
        if unknown:
            raise ConfigError(f"Unknown configuration key: {min(unknown)}")
        try:
            return from_dict(
                data_class=cls,
                data={**values, "command": command},
                config=DaciteConfig(
                    cast=[int, float, Path],
                    type_hooks={bool: _boolean},
                    strict=True,
                ),
            )
        except (DaciteError, ValueError, TypeError, click.BadParameter) as error:
            raise ConfigError(f"Invalid configuration: {error}") from error

    def __getitem__(self, key: str) -> ConfigValue:
        """Return a resolved value by key.

        Args:
            key (str): Configuration key.

        Returns:
            ConfigValue: Resolved scalar value.
        """
        if key not in _CONFIG_KEY_SET:
            raise KeyError(key)
        return _serialized(getattr(self, key))

    def __iter__(self) -> Iterator[str]:
        """Iterate over every canonical configuration key.

        Returns:
            Iterator[str]: Configuration-key iterator.
        """
        return iter(_CONFIG_KEYS)

    def __len__(self) -> int:
        """Return the number of canonical configuration keys.

        Returns:
            int: Number of keys in the resolved configuration.
        """
        return len(_CONFIG_KEYS)

    @property
    def source(self) -> Path | None:
        """Return the optional ``--config`` source path.

        Returns:
            Path | None: Configuration-file path, or ``None`` when omitted.
        """
        return self.config

    def as_dict(self) -> dict[str, ConfigValue]:
        """Copy values into a directly serializable dictionary.

        Returns:
            dict[str, ConfigValue]: Mutable copy of all resolved values.
        """
        return {key: self[key] for key in self}


_CONFIG_KEYS: Final = tuple(
    field.name for field in fields(Config) if field.name != "command"
)
_CONFIG_KEY_SET: Final = frozenset(_CONFIG_KEYS)


def _boolean(value: object) -> bool:
    """Convert a configuration boolean using Click's flag rules.

    Args:
        value (object): Raw value supplied to dacite.

    Returns:
        bool: Converted boolean value.
    """
    return cast("bool", click.BOOL.convert(value, None, None))


def _serialized(value: object) -> ConfigValue:
    """Normalize one Click value to the serializable configuration scalar set.

    Args:
        value (object): Resolved Click parameter value.

    Returns:
        ConfigValue: JSON-compatible scalar value.

    Raises:
        ConfigError: If a configured value has an unsupported type.
    """
    if isinstance(value, Path):
        return str(value)
    if value is None or isinstance(value, bool | float | int | str):
        return value
    raise ConfigError(f"Configuration value {value!r} is not serializable")


#: Complete stable key set used by every :class:`Config` object.
CONFIG_DEFAULTS: Final[dict[str, ConfigValue]] = Config().as_dict()

#: Deployment keys, with defaults taken from the typed configuration.
DEPLOYMENT_CONFIG_DEFAULTS: Final[dict[str, ConfigValue]] = {
    key: CONFIG_DEFAULTS[key] for key in _DEPLOYMENT_ENVVARS
}


def _option_defaults(*groups: tuple[str, ...]) -> dict[str, ConfigValue]:
    """Select command option defaults from the dataclass fields.

    Args:
        *groups (tuple[str, ...]): Groups of keys accepted by a command.

    Returns:
        dict[str, ConfigValue]: Defaults for the selected keys.
    """
    return {key: CONFIG_DEFAULTS[key] for group in groups for key in group}


#: Option keys and defaults for each configurable workflow command.
WORKFLOW_CONFIG_DEFAULTS: Final[dict[str, dict[str, ConfigValue]]] = {
    "sgt": _option_defaults(
        _COMMON,
        _LOW_FREQUENCY,
        _WORKFLOW,
        _SGT_PP,
        _SGT_STOCH,
        _SGT_SUB_WORKFLOW,
        _SGT,
        _CONFIG,
    ),
    "sub-sgt-wf": _option_defaults(
        ("run_id", "grid_output_file", "workflow_file"),
        _WORKFLOW,
        _SGT_PP,
        _SGT_STOCH,
        _SGT_SUB_WORKFLOW,
        _CONFIG,
    ),
    "pp": _option_defaults(
        _COMMON, _LOW_FREQUENCY, _WORKFLOW, _SGT_PP, _PP_STOCH, _PP, _CONFIG
    ),
    "stoch": {
        **_option_defaults(
            (
                "low_frequency_run_id",
                "erf_id",
                "rupture_variation_id",
                "merge_frequency",
                "site",
            ),
            _WORKFLOW,
            _SGT_STOCH,
            _PP_STOCH,
            _STOCH_SUB_WORKFLOW,
            ("stoch_frequency", "force_vs30"),
            _CONFIG,
        ),
        "stoch_frequency": DEFAULT_STOCH_FREQUENCY,
    },
    "sub-stoch-wf": _option_defaults(
        ("run_id", "low_frequency_run_id", "velocity_file", "workflow_file"),
        _WORKFLOW,
        _PP_STOCH,
        _STOCH_SUB_WORKFLOW,
        _CONFIG,
    ),
    "full": _option_defaults(
        _COMMON,
        _LOW_FREQUENCY,
        ("broadband",),
        _WORKFLOW,
        ("handoff", "sgt_spacing", "pp_spacing"),
        _SGT_STOCH,
        _PP_STOCH,
        _SGT_SUB_WORKFLOW,
        _SGT,
        _PP,
        tuple(
            key for key in _STOCH_SUB_WORKFLOW if key not in ("no_rotd", "no_duration")
        ),
        ("stoch_frequency", "force_vs30"),
        _CONFIG,
    ),
}


def _deployment_value(key: str, raw_value: str, *, source: str) -> ConfigValue:
    """Convert a deployment value to its declared scalar type.

    Args:
        key (str): Canonical deployment key.
        raw_value (str): Value read from a file or the process environment.
        source (str): Setting name used in error messages.

    Returns:
        ConfigValue: Converted value.

    Raises:
        ConfigError: If an integer setting contains a non-integer value.
    """
    if key == "database_port":
        try:
            return int(raw_value)
        except ValueError as error:
            raise ConfigError(f"{source} must be an integer") from error
    return raw_value


def resolve_config(
    command: str,
    parameters: Mapping[str, object],
    *,
    environ: Mapping[str, str],
    deployment_defaults: Mapping[str, str] | None = None,
) -> Config:
    """Resolve a complete configuration after Click processes a command.

    ``parameters`` must contain Click's final callback values, so explicit CLI
    arguments, option environment variables, and ``--config`` file defaults
    have already been resolved in the correct order.

    Args:
        command (str): Active Click command name.
        parameters (Mapping[str, object]): Final Click callback parameters.
        environ (Mapping[str, str]): Process environment used for deployment
            settings.
        deployment_defaults (Mapping[str, str] | None, optional): Deployment
            values read from the selected configuration file.

    Returns:
        Config: Complete immutable resolved configuration.
    """
    values = dict(CONFIG_DEFAULTS)
    values.update(WORKFLOW_CONFIG_DEFAULTS.get(command, {}))
    for key, value in parameters.items():
        if key in values:
            values[key] = _serialized(value)

    if command == "full":
        values["no_rotd"] = not values["rotd"]
        values["no_duration"] = not values["duration"]

    server = values.get("server")
    if isinstance(server, str):
        values["database_host"] = server
    for key, file_value in (deployment_defaults or {}).items():
        values[key] = _deployment_value(key, file_value, source=key.replace("_", "-"))
    for key, envvar in _DEPLOYMENT_ENVVARS.items():
        raw_value = environ.get(envvar)
        if raw_value is not None:
            values[key] = _deployment_value(key, raw_value, source=envvar)
    return Config.from_dict(values, command=command)


def environment_config(*, server: str = DEFAULT_DB_SERVER) -> Config:
    """Resolve deployment settings for commands without ``--config``.

    Args:
        server (str, optional): Database host fallback. Defaults to ``moment``.

    Returns:
        Config: Complete configuration using deployment environment variables
        and built-in workflow defaults.
    """
    return resolve_config(
        "",
        {"server": server, "config": None},
        environ=os.environ,
    )
