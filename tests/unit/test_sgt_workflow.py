"""Tests for the pure-Python SGT workflow generators."""

from __future__ import annotations

import datetime as dt
from typing import TYPE_CHECKING, Any

import pytest
import yaml

from cybershake.config import resolve_config
from cybershake.constants import START_STATE
from cybershake.exc import SgtError
from cybershake.runmanager.schema import Run
from cybershake.vs30 import Vs30
from cybershake.workflow.sgt import SGT

if TYPE_CHECKING:
    from pathlib import Path

    from pytest_mock import MockerFixture

    from cybershake.runmanager.db import SQLAlchemyRunDB


def _insert_run(
    database: SQLAlchemyRunDB,
    *,
    sgt_variation_id: int = 8,
    low_frequency: float = 1.0,
    target_vs30: float | None = 760.0,
) -> int:
    return database.insert_run(
        Run(
            site_id=9000,
            erf_id=64,
            sgt_variation_id=sgt_variation_id,
            rup_var_scenario_id=10,
            velocity_model_id=15,
            max_frequency=low_frequency,
            low_frequency_cutoff=low_frequency,
            sgt_source_filter_frequency=2.0,
            status=START_STATE,
            status_time=dt.datetime(2026, 10, 23),
            last_user="mayani",
            target_vs30=target_vs30,
        )
    )


def _generator(
    database: SQLAlchemyRunDB,
    *,
    separate_md5: bool = False,
    handoff: bool = False,
    spacing: float | None = None,
    min_vs: float | None = None,
    no_smoothing: bool = False,
    h_fraction: float | None = None,
    ely_taper: str | None = None,
    taper_depth: float | None = None,
    taper_models: str | None = None,
    z_comp: bool = False,
    local_vparam_insert: bool = False,
    depth: float | None = None,
    rotation: float | None = None,
    velocity_model: str = "v246",
) -> SGT:
    config = resolve_config(
        "sgt",
        {
            "site": "TEST",
            "velocity_model": velocity_model,
            "depth": depth,
            "rotation": rotation,
            "separate_md5": separate_md5,
            "handoff": handoff,
            "spacing": spacing,
            "min_vs": min_vs,
            "no_smoothing": no_smoothing,
            "h_fraction": h_fraction,
            "ely_taper": ely_taper,
            "taper_depth": taper_depth,
            "taper_models": taper_models,
            "z_comp": z_comp,
            "local_vparam_insert": local_vparam_insert,
        },
        environ={},
    )
    return SGT(
        config=config,
        database=database,
    )


def _load_workflow(path: Path) -> dict[str, Any]:
    loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(loaded, dict)
    return loaded


def _jobs_by_id(workflow: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {job["id"]: job for job in workflow["jobs"]}


def _dependencies(workflow: dict[str, Any]) -> dict[str, set[str]]:
    return {
        dependency["id"]: set(dependency["children"])
        for dependency in workflow["jobDependencies"]
    }


def test_create_awp_parent_workflow_matches_legacy_structure(
    tmp_path: Path,
    db: SQLAlchemyRunDB,
    mocker: MockerFixture,
) -> None:
    run_id = _insert_run(db)
    workflow_file = tmp_path / "run" / "CyberShake_SGT_TEST.dax"
    lookup = mocker.patch("cybershake.workflow.sgt.lookup_vs30")

    generator = _generator(
        db,
        handoff=True,
        spacing=0.08,
        min_vs=400.0,
        h_fraction=0.25,
        ely_taper="ifless",
        taper_depth=700.0,
        taper_models="sfcvm,cca,bbp1d",
        z_comp=True,
        local_vparam_insert=True,
        depth=50.0,
        rotation=-36.0,
    )
    config = generator.config
    generator.create(
        workflow_file=workflow_file,
        run_directory=workflow_file.parent,
        run_ids=(run_id,),
    )

    workflow = _load_workflow(workflow_file)
    jobs = _jobs_by_id(workflow)
    assert workflow["name"] == "CyberShake_SGT_TEST.dax"
    assert workflow["metadata"] == {key: str(value) for key, value in config.items()}
    assert set(jobs) == {
        "UpdateRun_SGT_START_TEST",
        "PreCVM_TEST",
        "GenSGTDax_TEST",
        "AWP_SGT_TEST",
    }
    assert jobs["PreCVM_TEST"]["arguments"][-5:] == [
        "--gpu",
        "--server moment",
        "--spacing 0.08",
        "--depth 50.0",
        "--rotation -36.0",
    ]
    generator_args = jobs["GenSGTDax_TEST"]["arguments"]
    assert generator_args[0] == "sub-sgt-wf"
    assert "--z-comp" in generator_args
    assert "--local-vparam-insert" in generator_args
    assert "--handoff" in generator_args
    assert jobs["AWP_SGT_TEST"]["arguments"] == [
        "--force",
        "--basename",
        "AWP_SGT_TEST",
        "--cleanup",
        "inplace",
        "--output-sites",
        "frontera",
    ]
    assert _dependencies(workflow) == {
        "UpdateRun_SGT_START_TEST": {"PreCVM_TEST"},
        "PreCVM_TEST": {"GenSGTDax_TEST", "AWP_SGT_TEST"},
        "GenSGTDax_TEST": {"AWP_SGT_TEST"},
    }
    lookup.assert_not_called()


def test_create_stores_missing_target_vs30(
    tmp_path: Path,
    db: SQLAlchemyRunDB,
    mocker: MockerFixture,
) -> None:
    run_id = _insert_run(db, target_vs30=None)
    lookup = mocker.patch(
        "cybershake.workflow.sgt.lookup_vs30",
        return_value=Vs30(578.23, "Thompson et al. (2020)"),
    )

    _generator(db).create(
        workflow_file=tmp_path / "CyberShake_SGT_TEST.dax",
        run_directory=tmp_path,
        run_ids=(run_id,),
    )

    lookup.assert_called_once_with(
        34.0001, -118.0001, classpath=str(_generator(db).config["vs30_classpath"])
    )
    run = db.get_run(run_id)
    assert run is not None
    assert run.target_vs30 == 578.23
    assert run.vs30_source == "Thompson et al. (2020)"


def test_create_rwg_workflow_contains_complete_static_graph(
    tmp_path: Path,
    db: SQLAlchemyRunDB,
) -> None:
    run_id = _insert_run(db, sgt_variation_id=7, low_frequency=0.5)
    workflow_file = tmp_path / "CyberShake_SGT_TEST.dax"

    _generator(db).create(
        workflow_file=workflow_file,
        run_directory=tmp_path,
        run_ids=(run_id,),
    )

    workflow = _load_workflow(workflow_file)
    jobs = _jobs_by_id(workflow)
    assert set(jobs) == {
        "UpdateRun_SGT_START_TEST",
        "PreCVM_TEST",
        "UCVMMesh_TEST",
        "PreSGT_TEST",
        "UpdateRun_SGT_END_TEST",
        "SGTGen_x_TEST",
        "SGTGen_y_TEST",
        "NanTest_TEST",
        "SGTMerge_TEST",
    }
    assert jobs["SGTGen_x_TEST"]["version"] == "3.0.3"
    assert "--spacing 0.2" in jobs["PreCVM_TEST"]["arguments"]
    assert {entry["lfn"] for entry in jobs["SGTMerge_TEST"]["uses"]} == {
        f"TEST_fx_{run_id}.sgt",
        f"TEST_fx_{run_id}.sgt.md5",
        f"TEST_fy_{run_id}.sgt",
        f"TEST_fy_{run_id}.sgt.md5",
    }
    assert all(
        entry["type"] == "output" and entry["stageOut"] and entry["registerReplica"]
        for entry in jobs["SGTMerge_TEST"]["uses"]
    )


def test_create_awp_sub_workflow_includes_optional_jobs_and_dependencies(
    tmp_path: Path,
    db: SQLAlchemyRunDB,
) -> None:
    run_id = _insert_run(db)
    grid_file = tmp_path / "gridout_TEST"
    grid_file.write_text("nx=200\nny=200\nnz=100\n", encoding="utf-8")
    workflow_file = tmp_path / "AWP_SGT_TEST.dax"

    _generator(
        db,
        separate_md5=True,
        handoff=True,
        spacing=0.08,
        min_vs=400.0,
        h_fraction=0.25,
        ely_taper="ifless",
        taper_depth=700.0,
        taper_models="sfcvm,cca,bbp1d",
        z_comp=True,
        local_vparam_insert=True,
    ).create_awp(
        run_id=run_id,
        grid_output_file=grid_file,
        workflow_file=workflow_file,
    )

    workflow = _load_workflow(workflow_file)
    jobs = _jobs_by_id(workflow)
    assert workflow["name"] == "AWP_SGT_TEST.dax"
    assert {
        "UCVMMesh_TEST",
        "Smooth_TEST",
        "PreSGT_TEST",
        "Velocity_Params_TEST",
        "PreAWP_GPU_TEST",
        "AWP_GPU_TEST_x",
        "AWP_GPU_TEST_y",
        "AWP_GPU_TEST_z",
        "PostAWP_TEST_x",
        "PostAWP_TEST_y",
        "PostAWP_TEST_z",
        "AWP_NaN_Check_x",
        "AWP_NaN_Check_y",
        "AWP_NaN_Check_z",
        "MD5_x",
        "MD5_y",
        "MD5_z",
        "InsertVelocityParams_TEST",
        "Handoff",
        "UpdateRun_SGT_END_TEST",
    } == set(jobs)
    pre_awp_args = jobs["PreAWP_GPU_TEST"]["arguments"]
    assert "--px 10" in pre_awp_args
    assert "--py 10" in pre_awp_args
    assert "--pz 1" in pre_awp_args
    assert "--z_comp" in pre_awp_args
    dependencies = _dependencies(workflow)
    assert "MD5_x" not in dependencies["PostAWP_TEST_x"]
    assert "Handoff" in dependencies["MD5_x"]
    assert "UpdateRun_SGT_END_TEST" in dependencies["Handoff"]
    assert "InsertVelocityParams_TEST" in dependencies["Velocity_Params_TEST"]
    assert jobs["Handoff"]["arguments"][0] == "handoff"
    assert jobs["InsertVelocityParams_TEST"]["arguments"][0] == (
        "insert-velocity-params"
    )


def test_create_cpu_awp_sub_workflow_calculates_processor_layout(
    tmp_path: Path,
    db: SQLAlchemyRunDB,
) -> None:
    run_id = _insert_run(db, sgt_variation_id=6)
    grid_file = tmp_path / "gridout_TEST"
    grid_file.write_text("nx=3200\nny=4416\nnz=100\n", encoding="utf-8")
    workflow_file = tmp_path / "AWP_SGT_TEST.dax"

    _generator(db, no_smoothing=True).create_awp(
        run_id=run_id,
        grid_output_file=grid_file,
        workflow_file=workflow_file,
    )

    workflow = _load_workflow(workflow_file)
    jobs = _jobs_by_id(workflow)
    assert "Smooth_TEST" not in jobs
    assert jobs["AWP_TEST_x"]["arguments"] == [
        "6900",
        "IN3D.TEST.x",
    ]
    assert "AWP_TEST_z" not in jobs
    assert {
        entry["lfn"]
        for entry in jobs["PostAWP_TEST_x"]["uses"]
        if entry["type"] == "output"
    } == {
        f"TEST_fy_{run_id}.sgt",
        f"TEST_fy_{run_id}.sgthead",
        f"TEST_fy_{run_id}.sgt.md5",
    }
    assert "UpdateRun_SGT_END_TEST" in _dependencies(workflow)["PostAWP_TEST_x"]


def test_create_awp_rejects_invalid_grid(
    tmp_path: Path,
    db: SQLAlchemyRunDB,
) -> None:
    run_id = _insert_run(db)
    grid_file = tmp_path / "gridout_TEST"
    grid_file.write_text("nx=200\nny=200\n", encoding="utf-8")

    with pytest.raises(SgtError, match="nx, ny, and nz"):
        _generator(db).create_awp(
            run_id=run_id,
            grid_output_file=grid_file,
            workflow_file=tmp_path / "AWP_SGT_TEST.dax",
        )


def test_create_requires_one_matching_run(
    tmp_path: Path,
    db: SQLAlchemyRunDB,
) -> None:
    run_id = _insert_run(db)
    generator = _generator(db)

    with pytest.raises(SgtError, match="exactly one"):
        generator.create(
            workflow_file=tmp_path / "workflow.yml",
            run_directory=tmp_path,
            run_ids=(run_id, run_id),
        )
    generator = _generator(db, velocity_model="v4")
    with pytest.raises(SgtError, match="not v4"):
        generator.create(
            workflow_file=tmp_path / "workflow.yml",
            run_directory=tmp_path,
            run_ids=(run_id,),
        )
