"""Integration contracts for the distributable non-Full workflow package."""

from __future__ import annotations

import re
import tomllib
from fnmatch import fnmatch
from pathlib import Path

ROOT = Path(__file__).parents[2]


def _catalog_transformations() -> set[tuple[str, str]]:
    catalog = (ROOT / "conf/tc.txt").read_text(encoding="utf-8")
    return set(re.findall(r"^tr scec::([^:]+):([^ ]+) \{", catalog, re.MULTILINE))


def test_catalog_covers_all_pp_generator_paths() -> None:
    expected = {
        ("CheckSgt", "1.0"),
        ("CheckSgtNoSums", "1.0"),
        ("DirCreate", "1.0"),
        ("DirectSynth", "1.0"),
        ("DirectSynth", "2.0"),
        ("DirectSynth", "3.0"),
        ("DirectSynth", "4.0"),
        ("DirectSynth_RSQSim", "1.0"),
        ("Extract_SGT_MPI", "1.0"),
        ("Extract_SGT_MPI", "3.3.1"),
        ("Extract_SGT_MPI_AWP", "1.0"),
        ("Extract_SGT_MPI_AWP", "3.3.1"),
        ("Seis_PSA_header", "1.0"),
        ("Seis_PSA_header", "3.3.1"),
    }

    assert expected <= _catalog_transformations()


def test_wheel_package_data_is_minimal_and_complete() -> None:
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    patterns = project["tool"]["setuptools"]["package-data"]["cybershake"]
    packaged_files = {
        path.relative_to(ROOT / "src/cybershake").as_posix()
        for path in (ROOT / "src/cybershake").rglob("*")
        if path.is_file()
        and any(
            fnmatch(path.relative_to(ROOT / "src/cybershake").as_posix(), pattern)
            for pattern in patterns
        )
    }
    expected = {
        "py.typed",
        "share/opensha/jars/opensha-all.jar",
        "share/opensha/jars/opensha-cybershake-all.jar",
        "share/db/src/main/java/CheckDBDataForSite.java",
        "share/db/src/main/java/org/scec/cme/cybershake/dax3/DBConnect.java",
        "share/db/jars/commons-cli-1.0.jar",
        "share/db/jars/mysql-connector-java-8.0.18.jar",
        ("share/opensha/src/main/java/org/scec/cme/cybershake/vs30/Vs30Cli.java"),
    }
    expected.update(
        path.relative_to(ROOT / "src/cybershake").as_posix()
        for path in (ROOT / "src/cybershake/share/opensha/conf").rglob("*")
        if path.is_file()
    )

    assert expected <= packaged_files
    assert not any("db_pass" in path.lower() for path in packaged_files)
    assert not any("__pycache__" in path for path in packaged_files)
