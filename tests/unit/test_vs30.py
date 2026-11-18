"""Tests for the Vs30 CLI wrapper, with a fake ``java``."""

from __future__ import annotations

import shutil
import stat
from typing import TYPE_CHECKING

import pytest

from cybershake.vs30 import (
    CLASSPATH_ENV,
    CLI_SOURCE,
    OPENSHA_DIR,
    Vs30,
    Vs30Error,
    default_classpath,
    lookup_vs30,
)

if TYPE_CHECKING:
    from pathlib import Path


def fake_java(tmp_path: Path, stdout: str = "", code: int = 0) -> str:
    java = tmp_path / "java"
    java.write_text(
        f'#!/bin/sh\necho "$@" > "{tmp_path}/args"\ncat <<EOF\n{stdout}\nEOF\n'
        f"echo oops >&2\nexit {code}\n"
    )
    java.chmod(java.stat().st_mode | stat.S_IEXEC)
    return str(java)


def test_lookup_parses_the_last_line(tmp_path: Path) -> None:
    java = fake_java(
        tmp_path, 'noise\n{"vs30": 733.4, "source": "Thompson et al. (2020)"}'
    )
    vs30 = lookup_vs30(37.786, -122.391, classpath="cp", java=java)
    assert vs30 == Vs30(733.4, "Thompson et al. (2020)")
    assert (tmp_path / "args").read_text().split() == [
        "-cp", "cp", str(CLI_SOURCE), "37.786", "-122.391",
    ]  # fmt: skip


@pytest.mark.parametrize(
    ("stdout", "code", "match"),
    [
        ("", 2, r"failed \(2\): oops"),
        ("not json", 0, "Unexpected Vs30 CLI output"),
        ('{"vs30": 1}', 0, "Unexpected Vs30 CLI output"),
        ("", 0, "Unexpected Vs30 CLI output"),
    ],
)
def test_lookup_errors(tmp_path: Path, stdout: str, code: int, match: str) -> None:
    with pytest.raises(Vs30Error, match=match):
        lookup_vs30(0, 0, java=fake_java(tmp_path, stdout, code))


def test_missing_java(tmp_path: Path) -> None:
    with pytest.raises(Vs30Error, match="Unable to run"):
        lookup_vs30(0, 0, java=str(tmp_path / "nope"))


def test_classpath_env_overrides_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(CLASSPATH_ENV, raising=False)
    assert "opensha/jars" in default_classpath()
    monkeypatch.setenv(CLASSPATH_ENV, "/x/y")
    assert default_classpath() == "/x/y"


@pytest.mark.skipif(
    shutil.which("java") is None
    or not (OPENSHA_DIR / "jars" / "opensha-all.jar").exists(),
    reason="needs java and the OpenSHA jar",
)
@pytest.mark.integration
def test_real_cli_from_source() -> None:
    assert lookup_vs30(37.786, -122.391) == Vs30(733.4, "Thompson et al. (2020)")
