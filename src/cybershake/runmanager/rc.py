"""Pegasus replica-catalog integration used when cloning runs."""

from __future__ import annotations

import os
import shlex
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING, NamedTuple

from cybershake.constants import (
    DEFAULT_REPLICA_CATALOG_DB,
    PEGASUS_RC_EXECUTABLE,
    REPLICA_CATALOG_DB_ENV,
)
from cybershake.exc import ReplicaCatalogError

if TYPE_CHECKING:
    from cybershake.config import Config


class ReplicaCatalogEntry(NamedTuple):
    """One logical-to-physical replica-catalog mapping.

    Attributes:
        lfn (str): Logical filename.
        pfn (str): Physical filename.
        pool (str | None): Optional execution or storage pool.
    """

    lfn: str
    pfn: str
    pool: str | None = None


class ReplicaCatalog:
    """Client for the SQLite-backed Pegasus replica catalog."""

    def __init__(
        self,
        *,
        database: Path | None = None,
        executable: str = PEGASUS_RC_EXECUTABLE,
        config: Config | None = None,
    ) -> None:
        """Initialize a replica-catalog client.

        Args:
            database (Path | None, optional): SQLite catalog path. Defaults to
                ``$CYBERSHAKE_RC_DB``, then
                ``/home/shock-ssd/scottcal/workflow/RC.sqlite``.
            executable (str, optional): Pegasus replica-catalog executable.
                Defaults to ``pegasus-rc-client``.
            config (Config | None, optional): Resolved command configuration.
                Defaults to ``None``.
        """
        configured_database = os.environ.get(REPLICA_CATALOG_DB_ENV)
        if database is not None:
            self.database = database
        elif config is not None:
            self.database = Path(str(config["replica_catalog_database"]))
        elif configured_database:
            self.database = Path(configured_database)
        else:
            self.database = DEFAULT_REPLICA_CATALOG_DB
        self.executable = executable

    def lookup(self, lfn: str) -> tuple[ReplicaCatalogEntry, ...]:
        """Return all replica-catalog entries for a logical filename.

        Args:
            lfn (str): Logical filename to look up.

        Returns:
            tuple[ReplicaCatalogEntry, ...]: Matching mappings in client-output
                order.

        Raises:
            ReplicaCatalogError: If the client fails or returns malformed output.
        """
        output = self._run("lookup", lfn)
        entries: list[ReplicaCatalogEntry] = []
        for line_number, line in enumerate(output.splitlines(), start=1):
            if not line.strip():
                continue
            try:
                fields = shlex.split(line)
            except ValueError as error:
                msg = f"Malformed replica-catalog output on line {line_number}: {line}"
                raise ReplicaCatalogError(msg) from error
            if len(fields) < 2 or fields[0] != lfn:
                msg = f"Malformed replica-catalog output on line {line_number}: {line}"
                raise ReplicaCatalogError(msg)

            pool = None
            for attribute in fields[2:]:
                name, separator, value = attribute.partition("=")
                if not separator:
                    msg = (
                        "Malformed replica-catalog output on line "
                        f"{line_number}: {line}"
                    )
                    raise ReplicaCatalogError(msg)
                if name == "pool":
                    pool = value
            entries.append(
                ReplicaCatalogEntry(
                    lfn=fields[0],
                    pfn=fields[1],
                    pool=pool,
                )
            )
        return tuple(entries)

    def insert(self, lfn: str, pfn: str, *, pool: str | None = None) -> None:
        """Insert a logical-to-physical replica-catalog mapping.

        Args:
            lfn (str): Logical filename to insert.
            pfn (str): Physical filename to associate with ``lfn``.
            pool (str | None, optional): Execution or storage pool. Defaults to
                ``None``.

        Raises:
            ReplicaCatalogError: If the client fails.
        """
        arguments = [lfn, pfn]
        if pool is not None:
            arguments.append(f'pool="{pool}"')
        self._run("insert", *arguments)

    def delete(self, lfn: str, pfn: str) -> None:
        """Delete a logical-to-physical replica-catalog mapping.

        Args:
            lfn (str): Logical filename to delete.
            pfn (str): Physical filename to disassociate from ``lfn``.

        Raises:
            ReplicaCatalogError: If the client fails.
        """
        self._run("delete", lfn, pfn)

    def _run(self, operation: str, *arguments: str) -> str:
        command = [
            self.executable,
            "-Dpegasus.catalog.replica=JDBCRC",
            "-Dpegasus.catalog.replica.db.driver=sqlite",
            f"-Dpegasus.catalog.replica.db.url=jdbc:sqlite:{self.database}",
            operation,
            *arguments,
        ]
        try:
            result = subprocess.run(  # noqa: S603
                command,
                capture_output=True,
                text=True,
                check=False,
            )
        except OSError as error:
            msg = f"Unable to run {self.executable}: {error}"
            raise ReplicaCatalogError(msg) from error
        if result.returncode != 0:
            detail = result.stderr.strip() or result.stdout.strip() or "no output"
            msg = (
                f"Replica-catalog {operation} failed with exit status "
                f"{result.returncode}: {detail}"
            )
            raise ReplicaCatalogError(msg)
        return result.stdout
