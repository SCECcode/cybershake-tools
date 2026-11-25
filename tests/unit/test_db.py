"""Tests for the SQLAlchemy CyberShake query repository."""

from __future__ import annotations

import datetime as dt
from typing import TYPE_CHECKING

import pytest

from sqlalchemy.exc import IntegrityError

from cybershake.config import resolve_config
from cybershake.constants import (
    DB_CREDENTIALS_ENV,
    DB_HOST_ENV,
    PP_STARTED_STATE,
    SGT_GENERATED_STATE,
)
from cybershake.exc import RunManagerError
from cybershake.runmanager import db as db_module
from cybershake.runmanager.schema import (
    HazardCurve,
    IMType,
    PeakAmplitude,
    Run,
)

if TYPE_CHECKING:
    from pathlib import Path

    from pytest_mock import MockerFixture

    from sqlalchemy.orm import Session

    from cybershake.runmanager.db import ParameterKind, SQLAlchemyRunDB


def _run(
    *,
    status: str = SGT_GENERATED_STATE,
    status_time: dt.datetime | None = None,
    sgt_time: dt.datetime | None = None,
    max_frequency: float = 1.0,
) -> Run:
    timestamp = status_time or dt.datetime(2026, 1, 1, tzinfo=None)
    return Run(
        site_id=9000,
        erf_id=64,
        sgt_variation_id=8,
        rup_var_scenario_id=10,
        velocity_model_id=15,
        max_frequency=max_frequency,
        low_frequency_cutoff=1.0,
        sgt_source_filter_frequency=2.0,
        status=status,
        status_time=timestamp,
        sgt_time=sgt_time,
        last_user="mayani",
    )


def test_lookup_queries_cover_sites_and_regions(
    db: SQLAlchemyRunDB,
) -> None:
    site = db.get_site("TEST")

    assert site is not None
    assert site.cs_site_id == 9000
    site_by_id = db.get_site_by_id(9000)

    assert site_by_id is not None
    assert site_by_id.cs_short_name == "TEST"
    assert db.get_site("NONE") is None
    assert [region.cutoff_dist for region in db.get_site_regions(9000, 64)] == [20.0]


@pytest.mark.parametrize(
    ("kind", "parameter_id"),
    [
        ("erf", 64),
        ("sgt_variation", 8),
        ("rupture_variation", 10),
        ("velocity_model", 15),
    ],
)
def test_parameter_exists(
    db: SQLAlchemyRunDB, kind: ParameterKind, parameter_id: int
) -> None:
    assert db.parameter_exists(kind, parameter_id)
    assert not db.parameter_exists(kind, 9999)


def test_run_insert_find_order_and_clone(db: SQLAlchemyRunDB) -> None:
    older_id = db.insert_run(
        _run(
            status_time=dt.datetime(2026, 1, 1),
            sgt_time=dt.datetime(2026, 1, 2),
        )
    )
    newer_id = db.insert_run(
        _run(
            status_time=dt.datetime(2026, 1, 3),
            sgt_time=dt.datetime(2026, 1, 4),
        )
    )

    older = db.get_run(older_id)
    assert older is not None
    assert older.run_id == older_id
    assert [
        run.run_id
        for run in db.find_runs(site="TEST", erf_id=64)
        if run.run_id in (older_id, newer_id)
    ] == [
        older_id,
        newer_id,
    ]
    assert [
        run.run_id
        for run in db.find_runs(
            states=(SGT_GENERATED_STATE,),
            order_by="status_time",
            descending=True,
        )
        if run.run_id in (older_id, newer_id)
    ] == [newer_id, older_id]
    preferred = db.find_preferred_run(
        site="TEST",
        erf_id=64,
        sgt_variation_id=8,
        rupture_variation_id=10,
        velocity_model_id=15,
        low_frequency_cutoff=1.0,
        max_frequency=1.0,
        source_frequency=2.0,
        states=(SGT_GENERATED_STATE,),
    )
    assert preferred is not None
    assert preferred.run_id == newer_id

    clone_id = db.clone_run(
        newer_id,
        changes={"status": PP_STARTED_STATE, "comment": f"Cloned from {newer_id}"},
    )
    clone = db.get_run(clone_id)
    assert clone is not None
    assert clone.run_id != newer_id
    assert clone.status == PP_STARTED_STATE
    assert clone.max_frequency == 1.0


def test_transaction_commits_and_rolls_back(db: SQLAlchemyRunDB) -> None:
    with db.transaction() as transaction:
        committed_id = transaction.insert_run(_run())

    assert db.get_run(committed_id) is not None

    rolled_back_id = 0

    def insert_then_fail() -> None:
        nonlocal rolled_back_id
        with db.transaction() as transaction:
            rolled_back_id = transaction.insert_run(_run())
            assert transaction.get_run(rolled_back_id) is not None
            raise RuntimeError("rollback")

    with pytest.raises(RuntimeError, match="rollback"):
        insert_then_fail()

    assert db.get_run(rolled_back_id) is None


def test_failed_insert_rolls_back_standalone_session(db: SQLAlchemyRunDB) -> None:
    invalid = _run()
    invalid.site_id = 9999

    with pytest.raises(IntegrityError):
        db.insert_run(invalid)

    assert db.insert_run(_run()) > 0


def test_run_writes_reject_missing_rows_and_invalid_fields(
    db: SQLAlchemyRunDB,
) -> None:
    assert not db.update_run_fields(9999, {"comment": "missing"})
    with pytest.raises(ValueError, match="Unsupported"):
        db.update_run_fields(9999, {"not_a_field": 1})
    with pytest.raises(ValueError, match="run_id"):
        db.update_run_fields(9999, {"run_id": 1})
    with pytest.raises(RunManagerError, match="does not exist"):
        db.clone_run(9999, changes={"comment": "missing"})


def test_run_field_and_frequency_updates(db: SQLAlchemyRunDB) -> None:
    run_id = db.insert_run(_run())

    assert db.update_run_fields(
        run_id,
        {"comment": "updated", "job_id": None},
    )
    assert db.update_run_frequencies(
        run_id,
        max_frequency=50.0,
        low_frequency_cutoff=1.0,
    )
    assert db.update_target_vs30(run_id, target_vs30=760.0, source="User-provided")

    run = db.get_run(run_id)
    parameters = db.get_velocity_parameters(run_id)
    assert run is not None
    assert run.comment == "updated"
    assert run.max_frequency == 50.0
    assert parameters is not None
    assert parameters.target_vs30 == 760.0


def test_copy_broadband_velocity_parameters(db: SQLAlchemyRunDB) -> None:
    low_frequency_id = db.insert_run(_run())
    broadband_id = db.insert_run(_run(max_frequency=50.0))
    assert db.update_run_fields(
        low_frequency_id,
        {
            "model_vs30": 710.0,
            "mesh_vsitop": 510.0,
            "z1_0": 110.0,
            "z2_5": 2.6,
        },
    )

    assert db.copy_broadband_velocity_parameters(
        low_frequency_id,
        broadband_id,
        model_vs30=760.0,
    )

    parameters = db.get_velocity_parameters(broadband_id)
    assert parameters is not None
    assert parameters.model_vs30 == 760.0
    assert parameters.z1_0 == 110.0
    assert parameters.z2_5 == 2.6


def test_rupture_and_variation_queries(db: SQLAlchemyRunDB) -> None:
    ruptures = db.get_ruptures_for_site("TEST", erf_id=64, cutoff_distance=20)
    summaries = db.get_rupture_summaries_for_site(
        "TEST",
        erf_id=64,
        rupture_variation_id=10,
        cutoff_distance=20,
    )
    variations = db.get_rupture_variations(
        erf_id=64,
        rupture_variation_id=10,
        site="TEST",
        cutoff_distance=20,
    )
    selected = db.get_rupture_variations(
        erf_id=64,
        rupture_variation_id=10,
        source_ruptures=((0, 0),),
    )
    seeds = db.get_rupture_variations_with_seeds(
        erf_id=64,
        rupture_variation_id=10,
        site="TEST",
        cutoff_distance=20,
    )

    assert len(db.get_ruptures_for_site("TEST", erf_id=64)) == 1
    assert (
        len(
            db.get_rupture_summaries_for_site(
                "TEST", erf_id=64, rupture_variation_id=10
            )
        )
        == 1
    )
    assert [(rupture.source_id, rupture.rupture_id) for rupture in ruptures] == [
        (0, 0),
    ]
    assert len(summaries) == 1
    assert summaries[0].variation_count == 3
    assert [variation.rup_var_id for variation in variations] == [0, 1, 2]
    assert [variation.rup_var_id for variation in selected] == [0, 1, 2]
    assert [(seed.rup_var_id, seed.seed) for seed in seeds] == [
        (0, 0),
        (1, 551188310),
        (2, 1157490780),
    ]
    assert (
        db.get_rupture_variations(
            erf_id=64,
            rupture_variation_id=10,
            source_ruptures=(),
        )
        == ()
    )
    with pytest.raises(ValueError, match="provided together"):
        db.get_rupture_variations(
            erf_id=64,
            rupture_variation_id=10,
            site="TEST",
        )


def test_run_statistics_queries(db: SQLAlchemyRunDB, seeded_session: Session) -> None:
    run_id = db.insert_run(_run())
    intensity_measure = IMType(
        im_type_measure="spectral acceleration",
        im_type_value=3.0,
        units="cm/s/s",
        im_type_component="RotD50",
    )
    seeded_session.add(intensity_measure)
    seeded_session.flush()
    seeded_session.add_all(
        [
            HazardCurve(run_id=run_id, im_type_id=intensity_measure.im_type_id),
            PeakAmplitude(
                run_id=run_id,
                source_id=0,
                rupture_id=0,
                rup_var_id=0,
                im_type_id=intensity_measure.im_type_id,
                im_value=1.2,
            ),
        ]
    )
    seeded_session.commit()

    statistics = db.get_run_statistics(run_id)

    assert statistics.peak_amplitude_count == 1
    assert [im.im_type_value for im in statistics.intensity_measures] == [3.0]


def test_connect_uses_environment_host_and_credentials(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mocker: MockerFixture,
) -> None:
    credentials = tmp_path / "database.txt"
    credentials.write_text("test-user:test-password\n", encoding="utf-8")
    monkeypatch.setenv(DB_HOST_ENV, "database.example")
    monkeypatch.setenv(DB_CREDENTIALS_ENV, str(credentials))
    engine = mocker.Mock()
    create_engine = mocker.patch(
        "cybershake.runmanager.db.create_engine",
        return_value=engine,
    )

    repository = db_module.connect(server="ignored.example")

    url = create_engine.call_args.args[0]
    assert repository._engine is engine
    assert url.drivername == "mysql+pymysql"
    assert url.username == "test-user"
    assert url.password == credentials.read_text().strip().partition(":")[2]
    assert url.host == "database.example"
    assert create_engine.call_args.kwargs == {"pool_pre_ping": True}


def test_connect_uses_resolved_configuration(
    tmp_path: Path,
    mocker: MockerFixture,
) -> None:
    credentials = tmp_path / "database.txt"
    credentials.write_text("configured-password\n", encoding="utf-8")
    config = resolve_config(
        "sgt",
        {"server": "option-server"},
        environ={
            "CYBERSHAKE_DB_HOST": "configured.example",
            "CYBERSHAKE_DB_PORT": "3307",
            "CYBERSHAKE_DB_NAME": "CyberShake_Test",
            "CYBERSHAKE_DB_USER": "configured-user",
            "CYBERSHAKE_DB_CREDENTIALS": str(credentials),
        },
    )
    engine = mocker.Mock()
    create_engine = mocker.patch(
        "cybershake.runmanager.db.create_engine",
        return_value=engine,
    )

    repository = db_module.connect(server="ignored.example", config=config)

    url = create_engine.call_args.args[0]
    assert repository._engine is engine
    assert url.username == "configured-user"
    assert url.password == credentials.read_text(encoding="utf-8").strip()
    assert url.host == "configured.example"
    assert url.port == 3307
    assert url.database == "CyberShake_Test"
