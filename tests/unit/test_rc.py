"""Tests for the Pegasus replica-catalog adapter."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from cybershake.config import resolve_config
from cybershake.constants import (
    DEFAULT_REPLICA_CATALOG_DB,
    REPLICA_CATALOG_DB_ENV,
)
from cybershake.exc import ReplicaCatalogError
from cybershake.runmanager.rc import ReplicaCatalog, ReplicaCatalogEntry

if TYPE_CHECKING:
    from pathlib import Path


def _fake_client(tmp_path: Path, body: str) -> Path:
    client = tmp_path / "pegasus-rc-client"
    client.write_text(f"#!/bin/sh\n{body}\n", encoding="utf-8")
    client.chmod(0o755)
    return client


def test_lookup_parses_entries(tmp_path: Path) -> None:
    client = _fake_client(
        tmp_path,
        """
printf '\\n'
printf '%s file:///data/first.sgt pool=\"shock\"\\n' "$5"
printf '%s file:///data/second.sgt checksum=abc\\n' "$5"
""",
    )
    catalog = ReplicaCatalog(
        database=tmp_path / "RC catalog.sqlite",
        executable=str(client),
    )

    assert catalog.lookup("TEST_fx_73.sgt") == (
        ReplicaCatalogEntry(
            lfn="TEST_fx_73.sgt",
            pfn="file:///data/first.sgt",
            pool="shock",
        ),
        ReplicaCatalogEntry(
            lfn="TEST_fx_73.sgt",
            pfn="file:///data/second.sgt",
        ),
    )


def test_insert_uses_safe_argument_list(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    client = _fake_client(tmp_path, 'printf \'%s\\n\' "$@" > "$RC_TEST_LOG"')
    log = tmp_path / "arguments"
    database = tmp_path / "RC catalog.sqlite"
    monkeypatch.setenv("RC_TEST_LOG", str(log))
    catalog = ReplicaCatalog(database=database, executable=str(client))

    catalog.insert(
        "TEST_fx_74.sgt",
        "file:///data/TEST_fx_73.sgt",
        pool="shock",
    )

    assert log.read_text(encoding="utf-8").splitlines() == [
        "-Dpegasus.catalog.replica=JDBCRC",
        "-Dpegasus.catalog.replica.db.driver=sqlite",
        f"-Dpegasus.catalog.replica.db.url=jdbc:sqlite:{database}",
        "insert",
        "TEST_fx_74.sgt",
        "file:///data/TEST_fx_73.sgt",
        'pool="shock"',
    ]


def test_delete_uses_safe_argument_list(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    client = _fake_client(tmp_path, 'printf \'%s\\n\' "$@" > "$RC_TEST_LOG"')
    log = tmp_path / "arguments"
    monkeypatch.setenv("RC_TEST_LOG", str(log))
    catalog = ReplicaCatalog(database=tmp_path / "RC.sqlite", executable=str(client))

    catalog.delete("TEST_fx_74.sgt", "file:///data/TEST_fx_73.sgt")

    assert log.read_text(encoding="utf-8").splitlines()[-3:] == [
        "delete",
        "TEST_fx_74.sgt",
        "file:///data/TEST_fx_73.sgt",
    ]


def test_lookup_rejects_malformed_output(tmp_path: Path) -> None:
    client = _fake_client(tmp_path, "printf 'missing-pfn\\n'")
    catalog = ReplicaCatalog(database=tmp_path / "RC.sqlite", executable=str(client))

    with pytest.raises(ReplicaCatalogError, match="Malformed replica-catalog output"):
        catalog.lookup("TEST_fx_73.sgt")


def test_lookup_rejects_malformed_quoting(tmp_path: Path) -> None:
    client = _fake_client(tmp_path, 'printf "\'unterminated\\n"')
    catalog = ReplicaCatalog(database=tmp_path / "RC.sqlite", executable=str(client))

    with pytest.raises(ReplicaCatalogError, match="Malformed replica-catalog output"):
        catalog.lookup("TEST_fx_73.sgt")


def test_lookup_rejects_malformed_attribute(tmp_path: Path) -> None:
    client = _fake_client(
        tmp_path, "printf '%s file:///data/file.sgt broken\\n' \"$5\""
    )
    catalog = ReplicaCatalog(database=tmp_path / "RC.sqlite", executable=str(client))

    with pytest.raises(ReplicaCatalogError, match="Malformed replica-catalog output"):
        catalog.lookup("TEST_fx_73.sgt")


def test_client_nonzero_exit_is_typed_failure(tmp_path: Path) -> None:
    client = _fake_client(
        tmp_path,
        "printf 'catalog unavailable\\n' >&2\nexit 7",
    )
    catalog = ReplicaCatalog(database=tmp_path / "RC.sqlite", executable=str(client))

    with pytest.raises(
        ReplicaCatalogError,
        match="lookup failed with exit status 7: catalog unavailable",
    ):
        catalog.lookup("TEST_fx_73.sgt")


def test_missing_client_is_typed_failure(tmp_path: Path) -> None:
    catalog = ReplicaCatalog(
        database=tmp_path / "RC.sqlite",
        executable=str(tmp_path / "missing-client"),
    )

    with pytest.raises(ReplicaCatalogError, match="Unable to run"):
        catalog.lookup("TEST_fx_73.sgt")


def test_database_uses_environment_override(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = tmp_path / "override.sqlite"
    monkeypatch.setenv(REPLICA_CATALOG_DB_ENV, str(database))

    assert ReplicaCatalog().database == database


def test_database_uses_resolved_configuration(tmp_path: Path) -> None:
    database = tmp_path / "resolved.sqlite"
    config = resolve_config(
        "sgt",
        {},
        environ={REPLICA_CATALOG_DB_ENV: str(database)},
    )

    assert ReplicaCatalog(config=config).database == database


def test_database_uses_legacy_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(REPLICA_CATALOG_DB_ENV, raising=False)

    assert ReplicaCatalog().database == DEFAULT_REPLICA_CATALOG_DB
