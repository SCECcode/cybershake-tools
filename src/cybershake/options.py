"""Reusable Click command-line option groups."""

from __future__ import annotations

import os
from collections.abc import Callable, Iterable
from configparser import Error as ConfigParserError
from functools import wraps
from pathlib import Path
from typing import Any, TypeVar, cast

import rich_click as click

from cybershake import properties, version
from cybershake.config import DEPLOYMENT_CONFIG_DEFAULTS, resolve_config
from cybershake.constants import (
    CLI_ENVVAR_PREFIX,
    CONTEXT_SETTINGS,
    DEFAULT_DB_SERVER,
    DEFAULT_STOCH_FREQUENCY,
    HF_VEL_MODELS,
    SGT_VELOCITY_MODELS,
)
from cybershake.exc import ConfigError

_Command = TypeVar("_Command", bound=Callable[..., object])
EPILOG = f"Check our docs at {version.__documentation__} for more details."


def envvar_option(
    *param_decls: str,
    envvar_scope: str | None = None,
    **attrs: Any,  # noqa: ANN401
) -> Callable[[_Command], _Command]:
    """Create a Click option with a documented CyberShake environment variable.

    Args:
        *param_decls (str): Click option declarations, including one long option.
        envvar_scope (str | None, optional): Optional workflow scope inserted
            after ``CYBERSHAKE``. Defaults to ``None``.
        **attrs (Any): Additional arguments forwarded to ``click.option``.

    Returns:
        Callable[[_Command], _Command]: Click option decorator.

    Raises:
        ValueError: If the declarations do not contain exactly one long option.
    """
    long_options = [
        declaration.split("/", 1)[0]
        for declaration in param_decls
        if declaration.startswith("--")
    ]
    if len(long_options) != 1:
        msg = "Environment-backed options require exactly one long option"
        raise ValueError(msg)
    name_parts = [CLI_ENVVAR_PREFIX]
    if envvar_scope is not None:
        name_parts.append(envvar_scope.upper().replace("-", "_"))
    name_parts.append(long_options[0][2:].upper().replace("-", "_"))
    return click.option(
        *param_decls,
        envvar="_".join(name_parts),
        show_envvar=True,
        **attrs,
    )


class EpilogGroup(click.RichGroup):
    """Click group that supplies the project epilog to itself and its commands."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:  # noqa: ANN401
        """Set the default epilog and context settings for the group.

        Args:
            *args (Any): Positional arguments forwarded to ``click.RichGroup``.
            **kwargs (Any): Keyword arguments forwarded to ``click.RichGroup``.
        """
        kwargs["epilog"] = kwargs.get("epilog", EPILOG)
        kwargs["context_settings"] = kwargs.get("context_settings", CONTEXT_SETTINGS)
        super().__init__(*args, **kwargs)

    def add_command(
        self,
        cmd: click.Command,
        name: str | None = None,
        aliases: Iterable[str] | None = None,
        panel: str | None = None,
    ) -> None:
        """Register a command with the default epilog.

        Args:
            cmd (click.Command): Command to register.
            name (str | None, optional): Registered command name. Defaults to
                the command's own name.
            aliases (Iterable[str] | None, optional): Additional command names.
                Defaults to ``None``.
            panel (str | None, optional): Rich Click help panel. Defaults to
                ``None``.
        """
        if cmd.epilog is None:
            cmd.epilog = EPILOG
        super().add_command(cmd, name, aliases, panel)


class PPCommand(click.Command):
    """Click command supporting the legacy optional high-frequency cutoff."""

    high_frequency_options: tuple[str, ...] = ("-hf", "--high-frequency")

    def parse_args(self, ctx: click.Context, args: list[str]) -> list[str]:
        """Supply the legacy cutoff when a high-frequency option has no value.

        Args:
            ctx (click.Context): Active Click context.
            args (list[str]): Command-line arguments to normalize and parse.

        Returns:
            list[str]: Arguments Click did not consume.
        """
        normalized: list[str] = []
        for index, argument in enumerate(args):
            normalized.append(argument)
            if argument not in self.high_frequency_options:
                continue
            if index + 1 == len(args):
                normalized.append("1.0")
                continue
            next_argument = args[index + 1]
            try:
                float(next_argument)
            except ValueError:
                if next_argument.startswith("-"):
                    normalized.append("1.0")
        return super().parse_args(ctx, normalized)


class FullCommand(PPCommand):
    """Full command with an unambiguous optional PP high-frequency option."""

    high_frequency_options: tuple[str, ...] = ("--high-frequency",)


def load_config_defaults(
    ctx: click.Context,
    _parameter: click.Parameter,
    path: Path | None,
) -> Path | None:
    """Load Click and deployment defaults from a properties file.

    Keys are long option names without the leading ``--``. Deployment keys
    require hyphens.

    Args:
        ctx (click.Context): Active command context whose defaults are updated.
        _parameter (click.Parameter): The ``--config`` parameter invoking the
            callback.
        path (Path | None): Configuration file selected by Click, if provided.

    Returns:
        Path | None: The selected configuration path, or ``None`` when omitted.

    Raises:
        ConfigError: If the file cannot be read or parsed, or
            names an unknown option or deployment setting.
    """
    if path is None:
        return path
    try:
        file_defaults = properties.load(path).defaults()
    except (OSError, ConfigParserError) as error:
        msg = f"Unable to load configuration file {path}: {error}"
        raise ConfigError(msg) from error

    option_names = {
        option[2:].replace("-", "_"): parameter.name
        for parameter in ctx.command.params
        if isinstance(parameter, click.Option)
        for option in parameter.opts
        if option.startswith("--")
    }
    defaults: dict[str, str] = {}
    deployment_keys = {key.replace("_", "-"): key for key in DEPLOYMENT_CONFIG_DEFAULTS}
    deployment_defaults: dict[str, str] = {}
    for key, value in file_defaults.items():
        file_key = key.removeprefix("--")
        normalized_key = file_key.replace("-", "_")
        deployment_key = deployment_keys.get(file_key)
        if deployment_key is not None:
            deployment_defaults[deployment_key] = value
            continue
        parameter_name = option_names.get(normalized_key)
        if parameter_name is None:
            msg = f"Unknown option {key!r} in {path}"
            raise ConfigError(msg)
        defaults[parameter_name] = value

    ctx.default_map = defaults
    ctx.meta["cybershake_deployment_defaults"] = deployment_defaults
    return path


def config_option(command: _Command) -> _Command:
    """Add the configuration-file option to a command.

    Args:
        command (_Command): Click callback to decorate.

    Returns:
        _Command: Decorated callback that eagerly loads CLI defaults from
            ``--config`` when provided.
    """

    @wraps(command)
    def invoke_with_config(*args: object, **kwargs: object) -> object:
        context = click.get_current_context()
        if context is None:
            raise ConfigError("No active Click context for configuration resolution")
        command_name = context.command.name or command.__name__.replace("_", "-")
        kwargs["config"] = resolve_config(
            command_name,
            kwargs,
            environ=os.environ,
            deployment_defaults=cast(
                "dict[str, str]",
                context.meta.get("cybershake_deployment_defaults", {}),
            ),
        )
        return command(*args, **kwargs)

    return cast(
        "_Command",
        envvar_option(
            "-c",
            "--config",
            type=click.Path(
                path_type=Path,
                exists=True,
                file_okay=True,
                dir_okay=False,
            ),
            callback=load_config_defaults,
            is_eager=True,
            help="Existing properties file containing CLI defaults.",
        )(invoke_with_config),
    )


def run_options(command: _Command) -> _Command:
    """Add arguments and options shared by workflow submission commands.

    Args:
        command (_Command): Click callback to decorate.

    Returns:
        _Command: Decorated callback with shared submission parameters.
            ``restart`` defaults to ``False`` and ``notify_user`` to ``None``.
    """
    decorators = (
        click.argument("site"),
        click.argument("run_id", type=int),
        envvar_option(
            "-r",
            "--restart",
            is_flag=True,
            help="Restart a workflow that was submitted previously.",
        ),
        envvar_option(
            "-n",
            "--notify-user",
            help="User or address recorded for workflow notifications.",
        ),
    )
    for decorator in reversed(decorators):
        command = decorator(command)
    return command


def plan_options(properties_default: Path) -> Callable[[_Command], _Command]:
    """Build a decorator for shared workflow planning parameters.

    Args:
        properties_default (Path): Default Pegasus properties file.

    Returns:
        Callable[[_Command], _Command]: Decorator that adds the shared planning
            parameters to a callback. ``properties`` defaults to
            ``properties_default``.
    """

    def add_options(command: _Command) -> _Command:
        decorators = (
            click.argument("site"),
            click.argument("run_id", type=int),
            click.argument("remote_site"),
            envvar_option(
                "-p",
                "--properties",
                type=click.Path(
                    path_type=Path,
                    exists=True,
                    file_okay=True,
                    dir_okay=False,
                ),
                default=properties_default,
                show_default=True,
                help="Pegasus properties file.",
            ),
        )
        for decorator in reversed(decorators):
            command = decorator(command)
        return command

    return add_options


def common_options(*, merge_frequency: bool = False) -> Callable[[_Command], _Command]:
    """Build a decorator for required workflow creation options.

    Args:
        merge_frequency (bool, optional): Use ``--merge-frequency`` instead of
            ``--frequency``. Defaults to ``False``.

    Returns:
        Callable[[_Command], _Command]: Decorator that adds ERF, rupture,
            frequency, and site options.
    """

    def add_options(command: _Command) -> _Command:
        frequency_declarations = (
            ("--merge-frequency", "merge_frequency")
            if merge_frequency
            else ("-f", "--frequency", "frequency")
        )
        decorators = (
            envvar_option(
                "-e",
                "--erf-id",
                "erf_id",
                type=int,
                required=True,
                help="ERF ID.",
            ),
            envvar_option(
                "-r",
                "--rupture-variation-id",
                "rupture_variation_id",
                type=int,
                required=True,
                help="Rupture variation ID.",
            ),
            envvar_option(
                *frequency_declarations,
                type=float,
                required=True,
                help=(
                    "Frequency at which to merge LF and stochastic results."
                    if merge_frequency
                    else "Simulation frequency in Hz."
                ),
            ),
            envvar_option(
                "-s", "--site", "site", required=True, help="Site short name."
            ),
        )
        for decorator in reversed(decorators):
            command = decorator(command)
        return command

    return add_options


def low_frequency_options(
    velocity_models: tuple[str, ...] = SGT_VELOCITY_MODELS,
) -> Callable[[_Command], _Command]:
    """Build a decorator for low-frequency workflow inputs.

    Args:
        velocity_models (tuple[str, ...], optional): Accepted velocity-model
            aliases. Defaults to ``SGT_VELOCITY_MODELS``.

    Returns:
        Callable[[_Command], _Command]: Decorator that adds shared SGT, PP, and
            Full inputs. ``source_frequency`` defaults to ``None``.
    """

    def add_options(command: _Command) -> _Command:
        decorators = (
            envvar_option(
                "-v",
                "--velocity-model",
                "velocity_model",
                type=click.Choice(velocity_models),
                required=True,
                metavar="VELOCITY_MODEL",
                help="Velocity model alias.",
            ),
            envvar_option(
                "-g",
                "--sgt-variation-id",
                "sgt_variation_id",
                type=int,
                required=True,
                help="SGT variation ID.",
            ),
            envvar_option(
                "-q",
                "--source-frequency",
                "source_frequency",
                type=float,
                help="SGT source-filter frequency; defaults to simulation frequency.",
            ),
        )
        for decorator in reversed(decorators):
            command = decorator(command)
        return command

    return add_options


def workflow_options(command: _Command) -> _Command:
    """Add options shared by every workflow creation command.

    Args:
        command (_Command): Click callback to decorate.

    Returns:
        _Command: Decorated callback with database-server and Z-component
            options. ``server`` defaults to ``moment`` (``DEFAULT_DB_SERVER``)
            and ``z_comp`` to ``False``.
    """
    decorators = (
        envvar_option(
            "-sr",
            "--server",
            default=DEFAULT_DB_SERVER,
            show_default=True,
            help="Database server used for site parameters and inserts.",
        ),
        envvar_option(
            "-z",
            "--z-comp",
            is_flag=True,
            help="Calculate the vertical component.",
        ),
    )
    for decorator in reversed(decorators):
        command = decorator(command)
    return command


def sgt_pp_options(command: _Command) -> _Command:
    """Add options shared by SGT and PP creation.

    Args:
        command (_Command): Click callback to decorate.

    Returns:
        _Command: Decorated callback with spacing and handoff options.
            ``spacing`` defaults to ``None`` and ``handoff`` to ``False``.
    """
    decorators = (
        envvar_option(
            "-sp",
            "--spacing",
            "spacing",
            type=float,
            help="Grid spacing override in km.",
        ),
        handoff_option,
    )
    for decorator in reversed(decorators):
        command = decorator(command)
    return command


def handoff_option(command: _Command) -> _Command:
    """Add the workflow handoff option.

    Args:
        command (_Command): Click callback to decorate.

    Returns:
        _Command: Decorated callback with ``handoff`` defaulting to ``False``.
    """
    return envvar_option(
        "-hd",
        "--handoff",
        is_flag=True,
        help="Add the completed stage to the pending handoff file.",
    )(command)


def full_spacing_options(command: _Command) -> _Command:
    """Add independent SGT and PP spacing options to Full creation.

    Args:
        command (_Command): Click callback to decorate.

    Returns:
        _Command: Decorated callback with ``sgt_spacing`` and ``pp_spacing``
            options, both defaulting to ``None``.
    """
    decorators = (
        envvar_option(
            "--sgt-spacing",
            type=float,
            help="SGT grid spacing override in km.",
        ),
        envvar_option(
            "--pp-spacing",
            type=float,
            help="PP grid spacing override in km.",
        ),
    )
    for decorator in reversed(decorators):
        command = decorator(command)
    return command


def sgt_stoch_options(command: _Command) -> _Command:
    """Add options shared by SGT and stochastic creation.

    Args:
        command (_Command): Click callback to decorate.

    Returns:
        _Command: Decorated callback with the horizontal grid fraction option,
            which defaults to ``None``.
    """
    return envvar_option(
        "-hf",
        "--h-fraction",
        type=float,
        help="Grid-point depth fraction used for surface UCVM queries.",
    )(command)


def pp_stoch_options(command: _Command) -> _Command:
    """Add options shared by PP and stochastic creation.

    Args:
        command (_Command): Click callback to decorate.

    Returns:
        _Command: Decorated callback with shared post-processing options.
            ``db_rv_seed`` defaults to ``True`` and ``rupture_list`` to ``None``.
    """
    decorators = (
        envvar_option("-d", "--debug", is_flag=True, help="Enable debugging."),
        envvar_option(
            "-dbrs/-no-dbrs",
            "--db-rv-seed/--no-db-rv-seed",
            default=True,
            help="Use database rupture-variation fractions and seeds by default.",
        ),
        envvar_option(
            "-pd",
            "--period-duration",
            is_flag=True,
            help="Calculate period-dependent durations.",
        ),
        envvar_option(
            "-nvr",
            "--no-vertical-response",
            is_flag=True,
            help="Skip vertical response spectra when Z is present.",
        ),
        envvar_option(
            "-rl",
            "--rupture-list",
            "rupture_list",
            type=click.Path(path_type=Path, file_okay=True, dir_okay=False),
            metavar="RUPTURE_LIST",
            help="CSV file containing source_id,rupture_id rows to include.",
        ),
    )
    for decorator in reversed(decorators):
        command = decorator(command)
    return command


def pp_high_frequency_option(*param_decls: str) -> Callable[[_Command], _Command]:
    """Build a decorator for PP's optional high-frequency cutoff.

    Args:
        *param_decls (str): Click option declarations for the cutoff.

    Returns:
        Callable[[_Command], _Command]: Decorator that adds the cutoff option
            using the requested aliases. The cutoff defaults to ``None`` when the
            option is absent and to ``1.0`` when it has no explicit value.
    """
    return envvar_option(
        *param_decls,
        type=float,
        help="Enable high-frequency processing with an optional cutoff (default 1.0).",
    )


def sgt_sub_workflow_options(command: _Command) -> _Command:
    """Add options shared by SGT and its runtime sub-workflow generator.

    Args:
        command (_Command): Click callback to decorate.

    Returns:
        _Command: Decorated callback with SGT sub-workflow controls. Flags
            default to ``False`` and value options to ``None``.
    """
    decorators = (
        envvar_option(
            "-sm",
            "--separate-md5",
            is_flag=True,
            help="Run MD5 jobs separately from PostAWP jobs.",
        ),
        envvar_option(
            "-mv", "--min-vs", "min_vs", type=float, help="Minimum Vs override."
        ),
        envvar_option(
            "-ns",
            "--no-smoothing",
            is_flag=True,
            help="Disable velocity-model smoothing.",
        ),
        envvar_option(
            "-et",
            "--ely-taper",
            type=click.Choice(("all", "none", "ifless")),
            help="Ely taper mode.",
        ),
        envvar_option(
            "-td",
            "--taper-depth",
            type=float,
            help="Ely taper depth in metres.",
        ),
        envvar_option(
            "-tm",
            "--taper-models",
            help="Comma-separated models to taper, 'all', or 'none'.",
        ),
        envvar_option(
            "-lv",
            "--local-vparam-insert",
            is_flag=True,
            help="Insert velocity parameters in a separate local job.",
        ),
    )
    for decorator in reversed(decorators):
        command = decorator(command)
    return command


def sgt_options(command: _Command) -> _Command:
    """Add options specific to top-level SGT creation.

    Args:
        command (_Command): Click callback to decorate.

    Returns:
        _Command: Decorated callback with SGT execution and volume options.
            Value options default to ``None``.
    """
    decorators = (
        envvar_option(
            "-ss",
            "--sgt-site",
            "sgt_site",
            metavar="SGT_SITE",
            help="SGT execution site.",
        ),
        envvar_option(
            "-dep", "--depth", "depth", type=float, help="Simulation depth in km."
        ),
        envvar_option(
            "-rt",
            "--rotation",
            type=float,
            help="Simulation volume rotation in degrees.",
        ),
    )
    for decorator in reversed(decorators):
        command = decorator(command)
    return sgt_sub_workflow_options(command)


def pp_options(command: _Command) -> _Command:
    """Add options specific to PP creation.

    Args:
        command (_Command): Click callback to decorate.

    Returns:
        _Command: Decorated callback with post-processing controls. RotD and
            duration default to ``True``; value options default to ``None``.
    """
    decorators = (
        envvar_option(
            "-ni",
            "--no-insert",
            is_flag=True,
            help="Do not insert ruptures into the database.",
        ),
        envvar_option(
            "-nc",
            "--no-mpi-cluster",
            is_flag=True,
            help="Do not use pegasus-mpi-cluster.",
        ),
        envvar_option(
            "-dh",
            "--dir-hierarchy",
            is_flag=True,
            help="Use a directory hierarchy on the compute resource.",
        ),
        envvar_option(
            "-R/-no-r",
            "--rotd/--no-rotd",
            default=True,
            help="Calculate and insert RotD50, its angle, and RotD100 by default.",
        ),
        envvar_option(
            "-k",
            "--skip-md5",
            is_flag=True,
            help="Skip MD5 checksums for debugging.",
        ),
        envvar_option(
            "-nb",
            "--nonblocking-md5",
            is_flag=True,
            help="Move MD5 checksums out of the critical path.",
        ),
        envvar_option(
            "-ps",
            "--pp-site",
            "pp_site",
            metavar="PP_SITE",
            help="PP execution site.",
        ),
        envvar_option(
            "-du/-no-du",
            "--duration/--no-duration",
            default=True,
            help="Calculate and insert duration metrics by default.",
        ),
        envvar_option(
            "-sl",
            "--seismogram-length",
            "seismogram_length",
            type=float,
            help="Output seismogram length in seconds.",
        ),
    )
    for decorator in reversed(decorators):
        command = decorator(command)
    return command


def stoch_sub_workflow_options(
    command: _Command, *, include_calculation_flags: bool = True
) -> _Command:
    """Add options shared by stochastic and its sub-workflow generator.

    Args:
        command (_Command): Click callback to decorate.
        include_calculation_flags (bool, optional): Include separate stochastic
            RotD and duration switches. Defaults to ``True``.

    Returns:
        _Command: Decorated callback with stochastic sub-workflow controls. Flags
            default to ``False`` and value options to ``None``.
    """
    decorators: tuple[Callable[[_Command], _Command], ...] = (
        envvar_option(
            "-mf",
            "--generator-merge-frequency",
            "generator_merge_frequency",
            type=float,
            help="Generator merge-frequency override in Hz.",
        ),
        envvar_option(
            "-nsr",
            "--no-site-response",
            is_flag=True,
            help="Omit site-response calculations.",
        ),
        envvar_option(
            "-nls",
            "--no-low-site-response",
            is_flag=True,
            help="Omit site response for low-frequency seismograms.",
        ),
        envvar_option(
            "-hfv",
            "--hf-vel-model",
            type=click.Choice(tuple(HF_VEL_MODELS)),
            help="1-D velocity model used for high-frequency synthesis.",
        ),
    )
    if include_calculation_flags:
        decorators += (
            envvar_option(
                "-nr",
                "--no-rotd",
                is_flag=True,
                help="Omit RotD calculations.",
            ),
            envvar_option(
                "-nd",
                "--no-duration",
                is_flag=True,
                help="Omit duration calculations.",
            ),
        )
    for decorator in reversed(decorators):
        command = decorator(command)
    return command


def stoch_options(
    *,
    stoch_frequency_default: float | None = DEFAULT_STOCH_FREQUENCY,
    include_calculation_flags: bool = True,
) -> Callable[[_Command], _Command]:
    """Build a decorator for top-level stochastic workflow options.

    Args:
        stoch_frequency_default (float | None, optional): Default maximum
            stochastic frequency, or ``None`` when merely supplying the option
            enables broadband processing. Defaults to ``10.0``
            (``DEFAULT_STOCH_FREQUENCY``).
        include_calculation_flags (bool, optional): Include separate stochastic
            RotD and duration switches. Defaults to ``True``.

    Returns:
        Callable[[_Command], _Command]: Decorator that adds top-level stochastic
            workflow options. ``force_vs30`` defaults to ``None``.
    """

    def add_options(command: _Command) -> _Command:
        decorators = (
            envvar_option(
                "-sf",
                "--stoch-frequency",
                type=float,
                default=stoch_frequency_default,
                show_default=stoch_frequency_default is not None,
                help="Maximum stochastic calculation frequency in Hz.",
            ),
            envvar_option(
                "-fvs",
                "--force-vs30",
                type=float,
                help="Force the Vs30 value used for site response.",
            ),
        )
        for decorator in reversed(decorators):
            command = decorator(command)
        return stoch_sub_workflow_options(
            command, include_calculation_flags=include_calculation_flags
        )

    return add_options
