"""Tests for run-manager lifecycle services."""

from __future__ import annotations

import datetime as dt
from typing import TYPE_CHECKING
from unittest.mock import Mock

import pytest

from cybershake import runmanager
from cybershake.constants import (
    CURVES_GENERATED_STATE,
    DONE_STATE,
    PP_ERROR_STATE,
    PP_STARTED_STATE,
    SGT_GENERATED_STATE,
    SGT_STARTED_STATE,
    START_STATE,
)
from cybershake.exc import ReplicaCatalogError, RunManagerError
from cybershake.runmanager.rc import ReplicaCatalogEntry
from cybershake.runmanager.schema import Run

if TYPE_CHECKING:
    from pytest_mock import MockerFixture

    from cybershake.runmanager.db import SQLAlchemyRunDB


SERVER = "moment"
NOW = dt.datetime(2026, 10, 23, 12, 0)


def _service_dependencies(mocker: MockerFixture, db: SQLAlchemyRunDB) -> None:
    mocker.patch("cybershake.runmanager.connect", return_value=db)
    mocker.patch("cybershake.runmanager._current_user", return_value="mayani")
    mocker.patch("cybershake.runmanager._now", return_value=NOW)


def _insert_run(
    db: SQLAlchemyRunDB,
    *,
    status: str,
    maximum_frequency: float = 1.0,
    low_frequency: float = 1.0,
    source_frequency: float = 2.5,
    sgt_time: dt.datetime | None = NOW,
    sgt_host: str = "frontier",
    pp_host: str = "frontera",
    last_user: str = "scottcal",
) -> int:
    return db.insert_run(
        Run(
            site_id=9000,
            erf_id=64,
            sgt_variation_id=8,
            rup_var_scenario_id=10,
            velocity_model_id=15,
            max_frequency=maximum_frequency,
            low_frequency_cutoff=low_frequency,
            sgt_source_filter_frequency=source_frequency,
            status=status,
            status_time=NOW,
            sgt_host=sgt_host,
            pp_host=pp_host,
            sgt_time=sgt_time,
            pp_time=NOW,
            last_user=last_user,
            comment="original",
            job_id="shock:1",
            submit_dir="dags/run",
            notify_user="user@example.org",
            minimum_vs=450.0,
            model_vs30=700.0,
            z1_0=100.0,
            z2_5=200.0,
        )
    )


def test_create_populates_legacy_defaults(
    db: SQLAlchemyRunDB, mocker: MockerFixture
) -> None:
    _service_dependencies(mocker, db)

    run_id = runmanager.create(
        site="TEST",
        erf_id=64,
        sgt_variation_id=8,
        velocity_model_id=15,
        rupture_variation_id=10,
        frequency=1.0,
        source_frequency=2.0,
        stoch_frequency=50.0,
        server=SERVER,
        config=None,
    )

    created = db.get_run(run_id)
    assert created is not None
    assert created.site_id == 9000
    assert created.status == START_STATE
    assert created.max_frequency == 50.0
    assert created.low_frequency_cutoff == 1.0
    assert created.sgt_source_filter_frequency == 2.0
    assert created.minimum_vs == 500.0
    assert created.sgt_host == created.pp_host == "unknown"
    assert created.sgt_time == created.pp_time == created.status_time == NOW
    assert created.last_user == "mayani"
    assert created.comment == created.job_id == created.submit_dir == ""
    assert created.notify_user == ""


def test_create_rejects_unknown_parameters(
    db: SQLAlchemyRunDB, mocker: MockerFixture
) -> None:
    _service_dependencies(mocker, db)
    run_ids = {run.run_id for run in db.find_runs(site="TEST")}

    with pytest.raises(RunManagerError, match="velocity model 999"):
        runmanager.create(
            site="TEST",
            erf_id=64,
            sgt_variation_id=8,
            velocity_model_id=999,
            rupture_variation_id=10,
            frequency=1.0,
            source_frequency=2.0,
            server=SERVER,
            config=None,
        )

    assert {run.run_id for run in db.find_runs(site="TEST")} == run_ids


def test_create_uses_deterministic_frequency_as_maximum(
    db: SQLAlchemyRunDB, mocker: MockerFixture
) -> None:
    _service_dependencies(mocker, db)

    run_id = runmanager.create(
        site="TEST",
        erf_id=64,
        sgt_variation_id=8,
        velocity_model_id=15,
        rupture_variation_id=10,
        frequency=1.0,
        source_frequency=2.0,
        server=SERVER,
        config=None,
    )

    created = db.get_run(run_id)
    assert created is not None
    assert created.max_frequency == 1.0


def test_create_stoch_copies_verified_low_frequency_parameters(
    db: SQLAlchemyRunDB, mocker: MockerFixture
) -> None:
    _service_dependencies(mocker, db)
    low_id = _insert_run(db, status=DONE_STATE, source_frequency=1.5)

    run_id = runmanager.create_stoch(
        site="TEST",
        low_frequency_run_id=low_id,
        erf_id=64,
        rupture_variation_id=10,
        merge_frequency=1.0,
        stoch_frequency=50.0,
        server=SERVER,
        config=None,
    )

    created = db.get_run(run_id)
    assert created is not None
    assert created.status == SGT_GENERATED_STATE
    assert created.sgt_variation_id == 8
    assert created.velocity_model_id == 15
    assert created.sgt_source_filter_frequency == 1.5
    assert created.max_frequency == 50.0
    assert created.low_frequency_cutoff == 1.0
    assert created.sgt_host == "frontier"


@pytest.mark.parametrize(
    ("status", "erf_id", "message"),
    [
        (SGT_GENERATED_STATE, 64, "is not Verified"),
        (DONE_STATE, 36, "parameters do not match"),
    ],
)
def test_create_stoch_rejects_invalid_low_frequency_run(
    db: SQLAlchemyRunDB,
    mocker: MockerFixture,
    status: str,
    erf_id: int,
    message: str,
) -> None:
    _service_dependencies(mocker, db)
    low_id = _insert_run(db, status=status)

    with pytest.raises(RunManagerError, match=message):
        runmanager.create_stoch(
            site="TEST",
            low_frequency_run_id=low_id,
            erf_id=erf_id,
            rupture_variation_id=10,
            merge_frequency=1.0,
            stoch_frequency=50.0,
            server=SERVER,
            config=None,
        )


@pytest.mark.parametrize(
    ("stage", "status"),
    [
        ("SGT_PLAN", START_STATE),
        ("SGT_PLAN", "SGT Error"),
        ("PP_PLAN", SGT_GENERATED_STATE),
        ("PP_PLAN", PP_ERROR_STATE),
    ],
)
def test_valid_accepts_each_planning_state(
    db: SQLAlchemyRunDB,
    mocker: MockerFixture,
    stage: str,
    status: str,
) -> None:
    _service_dependencies(mocker, db)
    run_id = _insert_run(db, status=status)

    runmanager.valid(
        run_id=run_id,
        site="TEST",
        stage=stage,
        server=SERVER,
        config=None,
    )


@pytest.mark.parametrize(
    ("site", "stage", "status", "message"),
    [
        ("OTHER", "SGT_PLAN", START_STATE, "expected site OTHER"),
        ("TEST", "UNKNOWN", START_STATE, "Unknown planning stage"),
        ("TEST", "PP_PLAN", START_STATE, "is invalid for PP_PLAN"),
    ],
)
def test_valid_rejects_site_stage_and_state(
    db: SQLAlchemyRunDB,
    mocker: MockerFixture,
    site: str,
    stage: str,
    status: str,
    message: str,
) -> None:
    _service_dependencies(mocker, db)
    run_id = _insert_run(db, status=status)

    with pytest.raises(RunManagerError, match=message):
        runmanager.valid(
            run_id=run_id,
            site=site,
            stage=stage,
            server=SERVER,
            config=None,
        )


def test_edit_validates_fields_and_updates_timestamps(
    db: SQLAlchemyRunDB, mocker: MockerFixture
) -> None:
    _service_dependencies(mocker, db)
    run_id = _insert_run(db, status=START_STATE, sgt_host="unknown")

    runmanager.edit(
        run_id=run_id,
        fields={
            "Status": SGT_STARTED_STATE,
            "SGT_Host": "frontier",
            "Comment": "x" * 140,
            "Job_ID": None,
            "Submit_Dir": None,
            "Notify_User": None,
        },
        server=SERVER,
        config=None,
    )

    edited = db.get_run(run_id)
    assert edited is not None
    assert edited.status == SGT_STARTED_STATE
    assert edited.sgt_host == "frontier"
    assert edited.sgt_time == edited.status_time == NOW
    assert edited.last_user == "mayani"
    assert edited.comment == "x" * 128
    assert edited.job_id == edited.submit_dir == edited.notify_user == ""


def test_edit_rejects_invalid_transition_without_partial_update(
    db: SQLAlchemyRunDB, mocker: MockerFixture
) -> None:
    _service_dependencies(mocker, db)
    run_id = _insert_run(db, status=START_STATE)

    with pytest.raises(RunManagerError, match="Invalid state transition"):
        runmanager.edit(
            run_id=run_id,
            fields={"Status": DONE_STATE, "Comment": "must roll back"},
            server=SERVER,
            config=None,
        )

    unchanged = db.get_run(run_id)
    assert unchanged is not None
    assert unchanged.status == START_STATE
    assert unchanged.comment == "original"


def test_edit_override_allows_valid_state_without_transition(
    db: SQLAlchemyRunDB, mocker: MockerFixture
) -> None:
    _service_dependencies(mocker, db)
    run_id = _insert_run(db, status=START_STATE)

    runmanager.edit(
        run_id=run_id,
        fields={"Status": DONE_STATE},
        override=True,
        server=SERVER,
        config=None,
    )

    edited = db.get_run(run_id)
    assert edited is not None
    assert edited.status == DONE_STATE


def test_edit_override_rejects_unknown_state(
    db: SQLAlchemyRunDB, mocker: MockerFixture
) -> None:
    _service_dependencies(mocker, db)
    run_id = _insert_run(db, status=START_STATE)

    with pytest.raises(RunManagerError, match="Invalid state UNKNOWN"):
        runmanager.edit(
            run_id=run_id,
            fields={"Status": "UNKNOWN"},
            override=True,
            server=SERVER,
            config=None,
        )

    unchanged = db.get_run(run_id)
    assert unchanged is not None
    assert unchanged.status == START_STATE


def test_find_reuses_sgt_generated_run(
    db: SQLAlchemyRunDB, mocker: MockerFixture
) -> None:
    _service_dependencies(mocker, db)
    older = _insert_run(
        db,
        status=SGT_GENERATED_STATE,
        sgt_time=NOW - dt.timedelta(days=1),
    )
    newer = _insert_run(db, status=SGT_GENERATED_STATE, sgt_time=NOW)
    catalog = mocker.patch("cybershake.runmanager.ReplicaCatalog")

    found = runmanager.find(
        site="TEST",
        erf_id=64,
        sgt_variation_id=8,
        rupture_variation_id=10,
        velocity_model_id=15,
        frequency=1.0,
        source_frequency=2.5,
        server=SERVER,
        config=None,
    )

    assert found == newer
    assert found != older
    catalog.assert_not_called()


def test_find_clones_run_and_sgt_replicas(
    db: SQLAlchemyRunDB, mocker: MockerFixture
) -> None:
    _service_dependencies(mocker, db)
    source_id = _insert_run(db, status=DONE_STATE)
    catalog = Mock()
    catalog.lookup.side_effect = lambda lfn: (
        ReplicaCatalogEntry(lfn, f"file:///data/{lfn}", "frontier"),
    )
    catalog_type = mocker.patch(
        "cybershake.runmanager.ReplicaCatalog", return_value=catalog
    )

    clone_id = runmanager.find(
        site="TEST",
        erf_id=64,
        sgt_variation_id=8,
        rupture_variation_id=10,
        velocity_model_id=15,
        frequency=1.0,
        source_frequency=2.5,
        server=SERVER,
        config=None,
    )

    clone = db.get_run(clone_id)
    assert clone is not None
    assert clone_id != source_id
    assert clone.status == SGT_GENERATED_STATE
    assert clone.comment == f"Cloned from run {source_id}"
    assert clone.pp_host == "unknown"
    assert clone.job_id == clone.submit_dir == clone.notify_user == ""
    assert clone.model_vs30 == 700.0
    catalog_type.assert_called_once_with(config=None)
    assert catalog.insert.call_count == 6
    inserted_lfns = {call.args[0] for call in catalog.insert.call_args_list}
    assert f"TEST_fx_{clone_id}.sgt" in inserted_lfns
    assert f"TEST_fy_{clone_id}.sgthead" in inserted_lfns


def test_find_rolls_back_clone_when_replica_copy_fails(
    db: SQLAlchemyRunDB, mocker: MockerFixture
) -> None:
    _service_dependencies(mocker, db)
    source_id = _insert_run(db, status=DONE_STATE)
    catalog = Mock()
    catalog.lookup.return_value = ()
    mocker.patch("cybershake.runmanager.ReplicaCatalog", return_value=catalog)
    run_ids = {run.run_id for run in db.find_runs(site="TEST")}

    with pytest.raises(ReplicaCatalogError, match="not found"):
        runmanager.find(
            site="TEST",
            erf_id=64,
            sgt_variation_id=8,
            rupture_variation_id=10,
            velocity_model_id=15,
            frequency=1.0,
            source_frequency=2.5,
            server=SERVER,
            config=None,
        )

    assert source_id in run_ids
    assert {run.run_id for run in db.find_runs(site="TEST")} == run_ids
    catalog.insert.assert_not_called()


def test_find_removes_inserted_replicas_when_later_insert_fails(
    db: SQLAlchemyRunDB, mocker: MockerFixture
) -> None:
    _service_dependencies(mocker, db)
    source_id = _insert_run(db, status=DONE_STATE)
    catalog = Mock()
    catalog.lookup.side_effect = lambda lfn: (
        ReplicaCatalogEntry(lfn, f"file:///data/{lfn}", "frontier"),
    )
    catalog.insert.side_effect = [None, ReplicaCatalogError("insert failed")]
    mocker.patch("cybershake.runmanager.ReplicaCatalog", return_value=catalog)
    run_ids = {run.run_id for run in db.find_runs(site="TEST")}

    with pytest.raises(ReplicaCatalogError, match="insert failed"):
        runmanager.find(
            site="TEST",
            erf_id=64,
            sgt_variation_id=8,
            rupture_variation_id=10,
            velocity_model_id=15,
            frequency=1.0,
            source_frequency=2.5,
            server=SERVER,
            config=None,
        )

    assert source_id in run_ids
    assert {run.run_id for run in db.find_runs(site="TEST")} == run_ids
    inserted_lfn, inserted_pfn = catalog.insert.call_args_list[0].args
    catalog.delete.assert_called_once_with(inserted_lfn, inserted_pfn)


def test_find_stoch_reuses_matching_run(
    db: SQLAlchemyRunDB, mocker: MockerFixture
) -> None:
    _service_dependencies(mocker, db)
    low_id = _insert_run(db, status=DONE_STATE)
    stochastic_id = _insert_run(
        db,
        status=SGT_GENERATED_STATE,
        maximum_frequency=40.0,
        low_frequency=1.0,
        source_frequency=9.0,
    )

    found = runmanager.find_stoch(
        site="TEST",
        low_frequency_run_id=low_id,
        erf_id=64,
        rupture_variation_id=10,
        merge_frequency=1.0,
        stoch_frequency=40.0,
        server=SERVER,
        config=None,
    )

    assert found == stochastic_id


def test_find_stoch_clones_matching_run_and_awp_replicas(
    db: SQLAlchemyRunDB, mocker: MockerFixture
) -> None:
    _service_dependencies(mocker, db)
    low_id = _insert_run(db, status=DONE_STATE)
    source_id = _insert_run(
        db,
        status=DONE_STATE,
        maximum_frequency=40.0,
        low_frequency=1.0,
    )
    catalog = Mock()
    catalog.lookup.side_effect = lambda lfn: (
        ReplicaCatalogEntry(lfn, f"file:///data/{lfn}", "frontier"),
    )
    mocker.patch("cybershake.runmanager.ReplicaCatalog", return_value=catalog)

    clone_id = runmanager.find_stoch(
        site="TEST",
        low_frequency_run_id=low_id,
        erf_id=64,
        rupture_variation_id=10,
        merge_frequency=1.0,
        stoch_frequency=40.0,
        server=SERVER,
        config=None,
    )

    clone = db.get_run(clone_id)
    assert clone is not None
    assert clone_id not in (low_id, source_id)
    assert clone.status == SGT_GENERATED_STATE
    assert catalog.insert.call_count == 6
    inserted_lfns = {call.args[0] for call in catalog.insert.call_args_list}
    assert f"TEST_fx_{clone_id}.sgthead" in inserted_lfns


def test_update_applies_workflow_transition_and_completion_cleanup(
    db: SQLAlchemyRunDB, mocker: MockerFixture
) -> None:
    _service_dependencies(mocker, db)
    run_id = _insert_run(db, status=SGT_STARTED_STATE)

    runmanager.update(
        run_id=run_id,
        old_state="SGT_START",
        new_state="SGT_END",
        server=SERVER,
        config=None,
    )

    updated = db.get_run(run_id)
    assert updated is not None
    assert updated.status == SGT_GENERATED_STATE
    assert updated.status_time == updated.sgt_time == NOW
    assert updated.comment == "Workflow completed successfully"
    assert updated.job_id == ""
    assert updated.last_user == "scottcal"


@pytest.mark.parametrize(
    ("old_state", "new_state", "database_state", "expected_state"),
    [
        ("SGT_INIT", "SGT_START", START_STATE, SGT_STARTED_STATE),
        ("SGT_INIT", "SGT_START", SGT_STARTED_STATE, SGT_STARTED_STATE),
        ("PP_INIT", "PP_START", SGT_GENERATED_STATE, PP_STARTED_STATE),
        ("PP_INIT", "PP_START", PP_STARTED_STATE, PP_STARTED_STATE),
        ("PP_START", "PP_END", PP_STARTED_STATE, CURVES_GENERATED_STATE),
    ],
)
def test_update_accepts_workflow_transition_matrix(
    db: SQLAlchemyRunDB,
    mocker: MockerFixture,
    old_state: str,
    new_state: str,
    database_state: str,
    expected_state: str,
) -> None:
    _service_dependencies(mocker, db)
    run_id = _insert_run(db, status=database_state)

    runmanager.update(
        run_id=run_id,
        old_state=old_state,
        new_state=new_state,
        server=SERVER,
        config=None,
    )

    updated = db.get_run(run_id)
    assert updated is not None
    assert updated.status == expected_state
    assert updated.status_time == NOW


@pytest.mark.parametrize(
    ("old_state", "new_state", "message"),
    [
        ("SGT_START", "SGT_START", "No state change"),
        ("UNKNOWN", "SGT_END", "Unknown workflow state"),
        ("SGT_INIT", "PP_START", "Invalid workflow state transition"),
    ],
)
def test_update_rejects_invalid_symbolic_transition(
    db: SQLAlchemyRunDB,
    mocker: MockerFixture,
    old_state: str,
    new_state: str,
    message: str,
) -> None:
    _service_dependencies(mocker, db)
    run_id = _insert_run(db, status=START_STATE)

    with pytest.raises(RunManagerError, match=message):
        runmanager.update(
            run_id=run_id,
            old_state=old_state,
            new_state=new_state,
            server=SERVER,
            config=None,
        )


def test_update_rejects_unexpected_database_state(
    db: SQLAlchemyRunDB, mocker: MockerFixture
) -> None:
    _service_dependencies(mocker, db)
    run_id = _insert_run(db, status=PP_STARTED_STATE)

    with pytest.raises(RunManagerError, match="expected one of"):
        runmanager.update(
            run_id=run_id,
            old_state="SGT_START",
            new_state="SGT_END",
            server=SERVER,
            config=None,
        )

    unchanged = db.get_run(run_id)
    assert unchanged is not None
    assert unchanged.status == PP_STARTED_STATE
    assert unchanged.comment == "original"


def test_find_reports_no_matching_run(
    db: SQLAlchemyRunDB, mocker: MockerFixture
) -> None:
    _service_dependencies(mocker, db)

    with pytest.raises(RunManagerError, match="No matching runs"):
        runmanager.find(
            site="TEST",
            erf_id=64,
            sgt_variation_id=8,
            rupture_variation_id=10,
            velocity_model_id=15,
            frequency=1.0,
            source_frequency=2.5,
            server=SERVER,
            config=None,
        )


def test_find_stoch_rejects_unverified_low_frequency_run(
    db: SQLAlchemyRunDB, mocker: MockerFixture
) -> None:
    _service_dependencies(mocker, db)
    low_id = _insert_run(db, status=CURVES_GENERATED_STATE)

    with pytest.raises(RunManagerError, match="is not Verified"):
        runmanager.find_stoch(
            site="TEST",
            low_frequency_run_id=low_id,
            erf_id=64,
            rupture_variation_id=10,
            merge_frequency=1.0,
            stoch_frequency=50.0,
            server=SERVER,
            config=None,
        )


def test_find_stoch_rolls_back_clone_when_replica_copy_fails(
    db: SQLAlchemyRunDB, mocker: MockerFixture
) -> None:
    _service_dependencies(mocker, db)
    low_id = _insert_run(db, status=DONE_STATE)
    source_id = _insert_run(
        db,
        status=DONE_STATE,
        maximum_frequency=40.0,
        low_frequency=1.0,
    )
    catalog = Mock()
    catalog.lookup.return_value = ()
    mocker.patch("cybershake.runmanager.ReplicaCatalog", return_value=catalog)
    run_ids = {run.run_id for run in db.find_runs(site="TEST")}

    with pytest.raises(ReplicaCatalogError, match="not found"):
        runmanager.find_stoch(
            site="TEST",
            low_frequency_run_id=low_id,
            erf_id=64,
            rupture_variation_id=10,
            merge_frequency=1.0,
            stoch_frequency=40.0,
            server=SERVER,
            config=None,
        )

    assert {low_id, source_id}.issubset(run_ids)
    assert {run.run_id for run in db.find_runs(site="TEST")} == run_ids
