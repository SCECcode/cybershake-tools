"""Tests for headerless Pegasus properties files."""

from configparser import DEFAULTSECT, ConfigParser, DuplicateOptionError
from io import StringIO
from pathlib import Path

import pytest

from cybershake import properties


def test_load_adds_default_section_and_preserves_key_case(tmp_path: Path) -> None:
    properties_file = tmp_path / "pegasus.properties"
    properties_file.write_text(
        "pegasus.catalog.site.file = sites.yml\nenv.PEGASUS_HOME: /opt/pegasus\n",
        encoding="utf-8",
    )

    loaded = properties.load(properties_file)

    assert loaded.sections() == []
    assert loaded[DEFAULTSECT]["pegasus.catalog.site.file"] == "sites.yml"
    assert loaded[DEFAULTSECT]["env.PEGASUS_HOME"] == "/opt/pegasus"


def test_load_accepts_readable_text_stream() -> None:
    loaded = properties.load(StringIO("pegasus.exitcode.scope=all\n"))

    assert loaded.defaults()["pegasus.exitcode.scope"] == "all"


def test_load_rejects_duplicate_keys_by_default() -> None:
    with pytest.raises(DuplicateOptionError):
        properties.load(StringIO("site=FIRST\nsite=TEST\n"))


def test_dump_omits_default_section_and_round_trips(tmp_path: Path) -> None:
    loaded = properties.load(
        StringIO("pegasus.catalog.site.file=sites.yml\nenv.PEGASUS_HOME=/opt/pegasus\n")
    )
    output = tmp_path / "output.properties"

    properties.dump(loaded, output)

    content = output.read_text(encoding="utf-8")
    assert "[DEFAULT]" not in content
    assert properties.load(output).defaults() == loaded.defaults()


def test_dump_accepts_writable_text_stream() -> None:
    loaded = ConfigParser()
    loaded[DEFAULTSECT] = {"pegasus.exitcode.scope": "none"}
    output = StringIO()

    properties.dump(loaded, output)

    assert output.getvalue() == "pegasus.exitcode.scope = none\n\n"


@pytest.mark.parametrize("value", [None, 1, object()])
def test_load_rejects_invalid_source(value: object) -> None:
    with pytest.raises(TypeError, match="expected a path or readable text stream"):
        properties.load(value)  # type: ignore[arg-type]


@pytest.mark.parametrize("value", [1, object()])
def test_dump_rejects_invalid_destination(value: object) -> None:
    loaded = properties.load(StringIO("pegasus.exitcode.scope=none\n"))

    with pytest.raises(TypeError, match="expected a path or writable text stream"):
        properties.dump(loaded, value)  # type: ignore[arg-type]
