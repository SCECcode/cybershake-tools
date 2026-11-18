"""Tests for the periodic run-manager service."""

from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import Mock, call

import pytest

from cybershake import runmanager
from cybershake.constants import (
    CURVES_GENERATED_STATE,
    DONE_STATE,
    PLOT_STATE,
    PP_ERROR_STATE,
    PP_STARTED_STATE,
    SGT_ERROR_STATE,
    SGT_STARTED_STATE,
    VERIFY_ERROR_STATE,
)
from cybershake.exc import CronRunCheckError, PlottingError
from cybershake.runmanager.schema import HazardCurve, IMType, PeakAmplitude, Run

if TYPE_CHECKING:
    from pathlib import Path

    from pytest_mock import MockerFixture

    from sqlalchemy.orm import Session

    from cybershake.runmanager.db import SQLAlchemyRunDB


def _run(*, status: str, job_id: str = "", notify_user: str = "") -> Run:
    return Run(
        site_id=9000,
        erf_id=64,
        sgt_variation_id=8,
        rup_var_scenario_id=10,
        velocity_model_id=15,
        status=status,
        status_time=runmanager._now(),
        sgt_host="frontier",
        pp_host="frontera",
        last_user="mayani",
        comment="original",
        job_id=job_id,
        submit_dir="",
        notify_user=notify_user,
    )


def _results(session: Session, run_id: int) -> None:
    measure = IMType(
        im_type_measure="spectral acceleration",
        im_type_value=3.0,
        units="cm/s/s",
        im_type_component="RotD50",
    )
    session.add(measure)
    session.flush()
    session.add_all(
        [
            HazardCurve(run_id=run_id, im_type_id=measure.im_type_id),
            PeakAmplitude(
                run_id=run_id,
                source_id=0,
                rupture_id=0,
                rup_var_id=0,
                im_type_id=measure.im_type_id,
                im_value=1.0,
            ),
        ]
    )
    session.commit()


def _dependencies(
    mocker: MockerFixture,
    db: SQLAlchemyRunDB,
    tmp_path: Path,
) -> Mock:
    mocker.patch("cybershake.runmanager.connect", return_value=db)
    mocker.patch("cybershake.runmanager.LEGACY_CRON_LOCK_FILE", tmp_path / "lock")
    mocker.patch("cybershake.runmanager.socket.gethostname", return_value="submit.test")
    mailer = Mock()
    mailer.send.return_value = True
    mocker.patch("cybershake.runmanager.Mailer", return_value=mailer)
    return mailer


def test_secondary_marks_missing_local_jobs_and_ignores_active_and_remote_jobs(
    db: SQLAlchemyRunDB,
    mocker: MockerFixture,
    tmp_path: Path,
) -> None:
    mailer = _dependencies(mocker, db, tmp_path)
    active_id = db.insert_run(
        _run(status=SGT_STARTED_STATE, job_id="submit:123", notify_user="a@x,b@y")
    )
    missing_id = db.insert_run(
        _run(status=SGT_STARTED_STATE, job_id="submit:456", notify_user="a@x")
    )
    no_job_id = db.insert_run(_run(status=PP_STARTED_STATE, notify_user="b@y"))
    remote_id = db.insert_run(
        _run(status=PP_STARTED_STATE, job_id="other:999", notify_user="c@z")
    )
    mocker.patch("cybershake.runmanager.condor.active_job_ids", return_value={"123"})
    maps = mocker.patch("cybershake.runmanager.plotting.generate_maps")
    curves = mocker.patch("cybershake.runmanager.plotting.generate_comparison_curves")

    runmanager.cron_run_check(mode="SECONDARY")

    active = db.get_run(active_id)
    missing = db.get_run(missing_id)
    no_job = db.get_run(no_job_id)
    remote = db.get_run(remote_id)
    assert active is not None
    assert active.status == SGT_STARTED_STATE
    assert missing is not None
    assert missing.status == SGT_ERROR_STATE
    assert missing.comment == "Workflow terminated unexpectedly"
    assert no_job is not None
    assert no_job.status == PP_ERROR_STATE
    assert no_job.comment == "Inconsistent state detected"
    assert remote is not None
    assert remote.status == PP_STARTED_STATE
    assert mailer.send.call_count == 2
    assert mailer.send.call_args_list[0].args[0] == ("a@x",)
    maps.assert_not_called()
    curves.assert_not_called()


def test_master_verifies_then_generates_missing_comparison_curves(
    db: SQLAlchemyRunDB,
    seeded_session: Session,
    mocker: MockerFixture,
    tmp_path: Path,
) -> None:
    mailer = _dependencies(mocker, db, tmp_path)
    run_id = db.insert_run(
        _run(status=CURVES_GENERATED_STATE, notify_user="owner@example.org")
    )
    _results(seeded_session, run_id)
    mocker.patch("cybershake.runmanager.condor.active_job_ids", return_value=set())
    mocker.patch(
        "cybershake.runmanager.plotting.comparison_curves_exist",
        return_value=False,
    )
    maps = mocker.patch("cybershake.runmanager.plotting.generate_maps", return_value=())
    curves = mocker.patch("cybershake.runmanager.plotting.generate_comparison_curves")

    runmanager.cron_run_check(mode="MASTER")

    checked = db.get_run(run_id)
    assert checked is not None
    assert checked.status == DONE_STATE
    assert checked.comment == "Passed verification checks"
    maps.assert_called_once_with()
    curves.assert_called_once()
    assert mailer.send.call_count == 2


def test_master_moves_empty_results_to_verification_error(
    db: SQLAlchemyRunDB,
    mocker: MockerFixture,
    tmp_path: Path,
) -> None:
    mailer = _dependencies(mocker, db, tmp_path)
    run_id = db.insert_run(
        _run(status=CURVES_GENERATED_STATE, notify_user="owner@example.org")
    )
    mocker.patch("cybershake.runmanager.condor.active_job_ids", return_value=set())
    maps = mocker.patch("cybershake.runmanager.plotting.generate_maps")
    curves = mocker.patch("cybershake.runmanager.plotting.generate_comparison_curves")

    runmanager.cron_run_check(mode="MASTER")

    checked = db.get_run(run_id)
    assert checked is not None
    assert checked.status == VERIFY_ERROR_STATE
    assert checked.comment == "Failed verification checks"
    mailer.send.assert_called_once()
    maps.assert_not_called()
    curves.assert_not_called()


def test_master_verifies_run_with_existing_comparison_curves(
    db: SQLAlchemyRunDB,
    seeded_session: Session,
    mocker: MockerFixture,
    tmp_path: Path,
) -> None:
    _dependencies(mocker, db, tmp_path)
    run_id = db.insert_run(_run(status=CURVES_GENERATED_STATE))
    _results(seeded_session, run_id)
    mocker.patch("cybershake.runmanager.condor.active_job_ids", return_value=set())
    mocker.patch(
        "cybershake.runmanager.plotting.comparison_curves_exist",
        return_value=True,
    )
    mocker.patch("cybershake.runmanager.plotting.generate_maps", return_value=())
    curves = mocker.patch("cybershake.runmanager.plotting.generate_comparison_curves")

    runmanager.cron_run_check(mode="MASTER")

    checked = db.get_run(run_id)
    assert checked is not None
    assert checked.status == DONE_STATE
    curves.assert_not_called()


def test_master_records_comparison_curve_failure(
    db: SQLAlchemyRunDB,
    seeded_session: Session,
    mocker: MockerFixture,
    tmp_path: Path,
) -> None:
    _dependencies(mocker, db, tmp_path)
    run_id = db.insert_run(_run(status=PLOT_STATE))
    _results(seeded_session, run_id)
    mocker.patch("cybershake.runmanager.condor.active_job_ids", return_value=set())
    mocker.patch("cybershake.runmanager.plotting.generate_maps", return_value=())
    mocker.patch(
        "cybershake.runmanager.plotting.generate_comparison_curves",
        side_effect=PlottingError("plot failed"),
    )

    runmanager.cron_run_check(mode="MASTER")

    checked = db.get_run(run_id)
    assert checked is not None
    assert checked.status == VERIFY_ERROR_STATE
    assert checked.comment == "Failed to create comparison curves"


def test_cron_rejects_unknown_mode(
    db: SQLAlchemyRunDB,
    mocker: MockerFixture,
    tmp_path: Path,
) -> None:
    _dependencies(mocker, db, tmp_path)

    with pytest.raises(CronRunCheckError, match="Unknown cron mode"):
        runmanager.cron_run_check(mode="OTHER")


def test_cron_releases_lock_when_queue_query_fails(
    db: SQLAlchemyRunDB,
    mocker: MockerFixture,
    tmp_path: Path,
) -> None:
    _dependencies(mocker, db, tmp_path)
    lockf = mocker.patch("cybershake.runmanager.fcntl.lockf")
    error = RuntimeError("queue failed")
    mocker.patch("cybershake.runmanager.condor.active_job_ids", side_effect=error)

    with pytest.raises(RuntimeError, match="queue failed"):
        runmanager.cron_run_check(mode="SECONDARY")

    assert lockf.call_args_list == [
        call(mocker.ANY, runmanager.fcntl.LOCK_EX | runmanager.fcntl.LOCK_NB),
        call(mocker.ANY, runmanager.fcntl.LOCK_UN),
    ]


def test_cron_rejects_concurrent_instance(
    db: SQLAlchemyRunDB,
    mocker: MockerFixture,
    tmp_path: Path,
) -> None:
    _dependencies(mocker, db, tmp_path)
    mocker.patch("cybershake.runmanager.fcntl.lockf", side_effect=BlockingIOError)
    connect = mocker.patch("cybershake.runmanager.connect")

    with pytest.raises(CronRunCheckError, match="another instance"):
        runmanager.cron_run_check(mode="MASTER")

    connect.assert_not_called()
