"""Tests for database-backed workflow runtime commands."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from click.testing import CliRunner

from cybershake.__main__ import cybershake
from cybershake.runmanager import jobs

if TYPE_CHECKING:
    from pathlib import Path

    from pytest_mock import MockerFixture

    from cybershake.runmanager.db import SQLAlchemyRunDB


def test_insert_velocity_parameters(db: SQLAlchemyRunDB, tmp_path: Path) -> None:
    source = tmp_path / "velocity.txt"
    source.write_text(
        "Model Vs30 = 578.23\nMesh Vs = 700\nZ1.0 = 1.5\nZ2.5 = 2.75\n",
        encoding="utf-8",
    )

    jobs.insert_velocity_parameters(db, input_file=source, run_id=900008)

    values = db.get_velocity_parameters(900008)
    assert values is not None
    assert values[:4] == (578.23, 700.0, 1.5, 2.75)


def test_insert_broadband_velocity_parameters(
    db: SQLAlchemyRunDB, tmp_path: Path
) -> None:
    db.update_run_fields(900008, {"z1_0": None, "z2_5": 3.0})
    source = tmp_path / "velocity_info.txt"
    source.write_text("Vs at site = 760\nVs30 = 578.23\n", encoding="utf-8")

    jobs.insert_broadband_velocity_parameters(
        db,
        velocity_file=source,
        broadband_run_id=901010,
        low_frequency_run_id=900008,
    )

    values = db.get_velocity_parameters(901010)
    assert values is not None
    assert values.model_vs30 == 578.23
    assert values.z1_0 is None
    assert values.z2_5 == 3.0


def test_insert_broadband_velocity_requires_vs30(
    db: SQLAlchemyRunDB, tmp_path: Path
) -> None:
    source = tmp_path / "velocity_info.txt"
    source.write_text("Vs at site = 760\n", encoding="utf-8")

    with pytest.raises(ValueError, match="Vs30 not found"):
        jobs.insert_broadband_velocity_parameters(
            db,
            velocity_file=source,
            broadband_run_id=901010,
            low_frequency_run_id=900008,
        )


def test_handoff_preserves_pending_queue_order(
    db: SQLAlchemyRunDB, tmp_path: Path
) -> None:
    pending = tmp_path / "pending.txt"
    pending.write_text("PP older\nstoch older\nother\n", encoding="utf-8")

    jobs.handoff(db, run_id=901010, stage="LF", pending_path=pending)
    jobs.handoff(db, run_id=900008, stage="BB", pending_path=pending)

    assert pending.read_text(encoding="utf-8").splitlines() == [
        "PP older",
        "PP TEST 64 10 8 15 50.000000 2.000000 -hf",
        "stoch older",
        "stoch TEST 64 10 8 15 1.000000 2.000000 50.000000",
        "other",
    ]


@pytest.mark.parametrize(
    ("args", "expected"),
    [
        (["insert-velocity-params", "-i", "value.txt", "-r", "900008"], "sgt"),
        (
            [
                "insert-bb-velocity-params",
                "-vi",
                "value.txt",
                "-bbid",
                "901010",
                "-lfid",
                "900008",
            ],
            "bb",
        ),
        (["handoff", "-r", "900008", "-s", "LF"], "handoff"),
    ],
)
def test_runtime_cli_dispatch(
    args: list[str], expected: str, tmp_path: Path, mocker: MockerFixture
) -> None:
    mocker.patch("cybershake.__main__._runtime_database")
    source = tmp_path / "value.txt"
    source.write_text("Vs30 = 500\n", encoding="utf-8")
    calls = {
        "sgt": mocker.patch(
            "cybershake.__main__.runtime_jobs.insert_velocity_parameters"
        ),
        "bb": mocker.patch(
            "cybershake.__main__.runtime_jobs.insert_broadband_velocity_parameters"
        ),
        "handoff": mocker.patch("cybershake.__main__.runtime_jobs.handoff"),
    }

    invocation = [argument.replace("value.txt", str(source)) for argument in args]
    result = CliRunner().invoke(cybershake, invocation)

    assert result.exit_code == 0, result.output
    calls[expected].assert_called_once()
