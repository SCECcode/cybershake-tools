"""CyberShake exceptions."""

import rich_click as click


class CommandError(click.ClickException):
    """Base class for errors rendered by the CyberShake CLI."""

    def __init__(self, message: str = "") -> None:
        """Initialize a command error.

        Args:
            message (str, optional): User-facing error message. Defaults to an
                empty string.
        """
        super().__init__(message)


class ConfigError(CommandError):
    """Raised when a CLI defaults file is invalid or cannot be read."""


class RunManagerError(Exception):
    """Raised when a run-manager operation fails."""


class ReplicaCatalogError(RunManagerError):
    """Raised when a replica-catalog operation fails."""


class CronRunCheckTimeoutError(Exception):
    """Raised when the cron run check exceeds its wrapper timeout."""


class CronRunCheckError(CommandError):
    """Raised when the periodic run check cannot run safely."""


class PlottingError(Exception):
    """Raised when an external CyberShake plotting command fails."""


class CheckDBError(Exception):
    """Raised when the database-check Java command cannot start."""


class RunTableError(CommandError):
    """Raised when a legacy workflow run table is missing or invalid."""


class PegasusError(CommandError):
    """Raised when Pegasus planning or submission metadata is invalid."""


class CondorError(CommandError):
    """Raised when an HTCondor configuration or queue operation fails."""


class SgtError(CommandError):
    """Raised when a SGT operation fails."""


class PpError(CommandError):
    """Raised when a post processing operation fails."""


class StochError(CommandError):
    """Raised when a stoch operation fails."""


class FullError(CommandError):
    """Raised when an integrated workflow operation fails."""
