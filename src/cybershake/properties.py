"""Load and dump headerless Pegasus properties files."""

from __future__ import annotations

from configparser import DEFAULTSECT, ConfigParser
from io import StringIO
from pathlib import Path
from typing import TextIO


class _CaseSensitiveConfigParser(ConfigParser):
    """Preserve Pegasus property key case while parsing."""

    def optionxform(self, optionstr: str) -> str:
        """Return a property key without normalizing its case.

        Args:
            optionstr (str): Property key read by ``ConfigParser``.

        Returns:
            str: Unchanged property key.
        """
        return optionstr


def load(file: str | Path | TextIO) -> ConfigParser:
    """Load a headerless Pegasus properties file.

    A synthetic ``[DEFAULT]`` section is prepended in memory because
    :class:`configparser.ConfigParser` requires section headers while Pegasus
    properties files omit them.

    Args:
        file (str | Path | TextIO): Properties path or readable text stream.

    Returns:
        ConfigParser: Parsed properties stored in the default section.

    Raises:
        TypeError: If ``file`` is not a path or readable text stream.
        OSError: If a path cannot be read.
        configparser.Error: If the properties content cannot be parsed.
    """
    if isinstance(file, (str, Path)):
        with Path(file).open(encoding="utf-8") as stream:
            content = stream.read()
    elif hasattr(file, "read"):
        content = file.read()
    else:
        msg = f"invalid file: {file}; expected a path or readable text stream"
        raise TypeError(msg)

    properties = _CaseSensitiveConfigParser()
    properties.read_string(f"[{DEFAULTSECT}]\n{content}")
    return properties


def dump(properties: ConfigParser, file: str | Path | TextIO) -> None:
    """Dump Pegasus properties without the synthetic default-section header.

    Args:
        properties (ConfigParser): Properties to serialize. Values from its
            default section are written as headerless Pegasus properties.
        file (str | Path | TextIO): Destination path or writable text stream.

    Raises:
        TypeError: If ``file`` is not a path or writable text stream.
        OSError: If a path cannot be written.
    """
    with StringIO() as stream:
        properties.write(stream)
        content = stream.getvalue()

    content = content.removeprefix(f"[{DEFAULTSECT}]\n")

    if isinstance(file, (str, Path)):
        Path(file).write_text(content, encoding="utf-8")
    elif hasattr(file, "write"):
        file.write(content)
    else:
        msg = f"invalid file: {file}; expected a path or writable text stream"
        raise TypeError(msg)
