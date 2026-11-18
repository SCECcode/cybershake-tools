"""Tests for pure-Python post-processing workflow generation."""

from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
import yaml

from cybershake.config import resolve_config
from cybershake.constants import SGT_GENERATED_STATE
from cybershake.exc import PpError
from cybershake.runmanager.schema import Run
from cybershake.workflow.common import load_run_info
from cybershake.workflow.pp import PP

if TYPE_CHECKING:
    from cybershake.runmanager.db import SQLAlchemyRunDB


def _insert_run(database: SQLAlchemyRunDB) -> int:
    return database.insert_run(
        Run(
            site_id=9000,
            erf_id=64,
            sgt_variation_id=8,
            rup_var_scenario_id=10,
            velocity_model_id=15,
            max_frequency=1.0,
            low_frequency_cutoff=1.0,
            sgt_source_filter_frequency=2.0,
            status=SGT_GENERATED_STATE,
            status_time=dt.datetime(2026, 10, 23),
            last_user="mayani",
            target_vs30=760.0,
        )
    )


def _generator(
    database: SQLAlchemyRunDB,
    *,
    direct_synth: bool,
    output_directory: Path | None = None,
    high_frequency: float | None = None,
    no_insert: bool = False,
    nonblocking_md5: bool = False,
    rupture_list: Path | None = None,
    dir_hierarchy: bool = False,
    rotd: bool = True,
    duration: bool = True,
    db_rv_seed: bool = True,
) -> PP:
    config = resolve_config(
        "pp",
        {
            "no_insert": no_insert,
            "high_frequency": high_frequency,
            "dir_hierarchy": dir_hierarchy,
            "rotd": rotd,
            "nonblocking_md5": nonblocking_md5,
            "direct_synth": direct_synth,
            "spacing": 0.08,
            "duration": duration,
            "seismogram_length": 400.0,
            "db_rv_seed": db_rv_seed,
            "z_comp": True,
            "handoff": True,
            "period_duration": True,
            "rupture_list": rupture_list,
            "output_directory": output_directory or Path(),
        },
        environ={},
    )
    return PP(
        config=config,
        database=database,
    )


def _load(path: Path) -> dict[str, Any]:
    loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(loaded, dict)
    return loaded


def _jobs(workflow: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {node["id"]: node for node in workflow["jobs"]}


def _dependencies(workflow: dict[str, Any]) -> dict[str, set[str]]:
    return {
        edge["id"]: set(edge["children"])
        for edge in workflow.get("jobDependencies", [])
    }


def test_create_direct_synth_hierarchy_and_auxiliary_files(
    tmp_path: Path,
    db: SQLAlchemyRunDB,
) -> None:
    run_id = _insert_run(db)
    generator = _generator(
        db, direct_synth=True, nonblocking_md5=True, output_directory=tmp_path
    )
    config = generator.config

    generator.create(run_id=run_id, run_directory=tmp_path)

    expected = {
        f"CyberShake_TEST{suffix}.dax"
        for suffix in ("", "_pre", "_Synth", "_DB_Products", "_post")
    }
    assert expected.issubset({path.name for path in tmp_path.iterdir()})
    top = _load(tmp_path / "CyberShake_TEST.dax")
    assert top["metadata"] == {key: str(value) for key, value in config.items()}
    assert set(_jobs(top)) == {"preDAX", "dax_0", "dbDax", "postDax"}
    assert _dependencies(top) == {
        "dax_0": {"dbDax"},
        "dbDax": {"postDax"},
    }

    pre = _load(tmp_path / "CyberShake_TEST_pre.dax")
    assert set(_jobs(pre)) == {
        "UpdateRun_PP_START",
        "CheckSgt_TEST_x",
        "CheckSgt_TEST_y",
        "CheckSgt_TEST_z",
    }
    synthesis = _load(tmp_path / "CyberShake_TEST_Synth.dax")
    replicas = {
        entry["lfn"]: entry["pfns"] for entry in synthesis["replicaCatalog"]["replicas"]
    }
    for name in (
        f"rupture_file_list_TEST_{run_id}",
        f"rvfrac_seed_values_TEST_{run_id}",
    ):
        staged_path = tmp_path / "test-runs" / name
        assert staged_path.read_bytes() == (tmp_path / name).read_bytes()
        assert replicas[name] == [
            {
                "pfn": (f"go://56569ec1-af41-4745-a8d1-8514231c7a6d/{staged_path}"),
                "site": "shock",
            }
        ]
    direct = _jobs(synthesis)["DirectSynth"]
    assert direct["version"] == "4.0"
    assert "run_rotd=1" in direct["arguments"]
    assert "run_durations=1" in direct["arguments"]
    assert "run_period_durations=1" in direct["arguments"]
    assert f"sgt_zfile=TEST_fz_{run_id}.sgt" in direct["arguments"]
    assert {entry["lfn"] for entry in direct["uses"] if entry["type"] == "output"} == {
        f"Seismogram_TEST_{run_id}_0_0.grm",
        f"RotD_TEST_{run_id}_0_0.rotd",
        f"VerticalRSP_TEST_{run_id}_0_0.rsp",
        f"Duration_TEST_{run_id}_0_0.dur",
        f"PeriodDuration_TEST_{run_id}_0_0.dur",
    }
    rupture_lines = (
        (tmp_path / f"rupture_file_list_TEST_{run_id}")
        .read_text(encoding="utf-8")
        .splitlines()
    )
    assert rupture_lines == ["1", "e64_rv10_0_0.txt 3 1 22 294 5.95"]
    seed_lines = (
        (tmp_path / f"rvfrac_seed_values_TEST_{run_id}")
        .read_text(encoding="utf-8")
        .splitlines()
    )
    assert seed_lines[0] == "3"
    assert seed_lines[1].startswith("0 0 0 0.843884 ")

    products = _load(tmp_path / "CyberShake_TEST_DB_Products.dax")
    product_jobs = _jobs(products)
    assert set(product_jobs) == {
        "CS_Products_Load_Amps_RotD_TEST",
        "CS_Products_DB_Check_RotD_TEST",
        "CS_Products_Curve_Calc_RotD50_TEST",
        "Disaggregate_TEST",
        "CS_Products_Load_Durations_TEST",
        "CS_Products_DB_Check_Duration_TEST",
    }
    assert product_jobs["CS_Products_DB_Check_RotD_TEST"]["arguments"][:2] == [
        "jobs",
        "check-db-site",
    ]
    assert product_jobs["CS_Products_DB_Check_RotD_TEST"]["arguments"][2:4] == [
        "-r",
        str(run_id),
    ]
    assert "-o" in product_jobs["CS_Products_DB_Check_RotD_TEST"]["arguments"]
    assert product_jobs["CS_Products_DB_Check_Duration_TEST"]["arguments"][:2] == [
        "jobs",
        "check-db-site",
    ]
    curve_args = product_jobs["CS_Products_Curve_Calc_RotD50_TEST"]["arguments"]
    assert curve_args[:2] == ["jobs", "curve-plot"]
    assert curve_args[curve_args.index("--period") + 1] == "3,4,5,7.5,10,2"
    assert product_jobs["Disaggregate_TEST"]["arguments"][:2] == [
        "jobs",
        "disagg-plot",
    ]
    post = _load(tmp_path / "CyberShake_TEST_post.dax")
    assert set(_jobs(post)) == {"UpdateRun_PP_END", "Handoff"}
    assert _jobs(post)["Handoff"]["arguments"][0] == "handoff"


def test_direct_synth_can_disable_default_postprocessing(
    tmp_path: Path, db: SQLAlchemyRunDB
) -> None:
    run_id = _insert_run(db)
    generator = _generator(
        db,
        direct_synth=True,
        output_directory=tmp_path,
        rotd=False,
        duration=False,
        db_rv_seed=False,
    )

    generator.create(run_id=run_id, run_directory=tmp_path)

    direct = _jobs(_load(tmp_path / "CyberShake_TEST_Synth.dax"))["DirectSynth"]
    assert "run_rotd=0" in direct["arguments"]
    assert "run_durations=0" in direct["arguments"]
    assert not any(
        argument.startswith("rv_info_file=") for argument in direct["arguments"]
    )


def test_pp_site_does_not_add_set_host_job(
    tmp_path: Path,
    db: SQLAlchemyRunDB,
) -> None:
    run_id = _insert_run(db)
    workflow_file = tmp_path / "CyberShake_TEST_pre.dax"
    generator = PP(
        config=resolve_config("pp", {"pp_site": "shock"}, environ={}),
        database=db,
    )

    generator.create_pre(workflow_file=workflow_file, run_id=run_id, site="TEST")

    assert "SetPPHost" not in _jobs(_load(workflow_file))


def test_create_normal_synthesis_builds_extract_and_variation_jobs(
    tmp_path: Path,
    db: SQLAlchemyRunDB,
) -> None:
    run_id = _insert_run(db)
    workflow_file = tmp_path / "CyberShake_TEST_0.dax"
    rupture_list = tmp_path / "ruptures.csv"
    rupture_list.write_text("0,0\n", encoding="utf-8")

    _generator(
        db,
        direct_synth=False,
        rupture_list=rupture_list,
    ).create_synthesis(
        workflow_file=workflow_file,
        run_id=run_id,
        workflow_index=0,
    )

    workflow = _load(workflow_file)
    jobs = _jobs(workflow)
    assert set(jobs) == {
        "Extract_SGT_MPI_AWP_0",
        "ID2_0_0_0",
        "ID2_0_0_1",
        "ID2_0_0_2",
    }
    assert _dependencies(workflow) == {
        "Extract_SGT_MPI_AWP_0": {"ID2_0_0_0", "ID2_0_0_1", "ID2_0_0_2"}
    }
    first = jobs["ID2_0_0_0"]
    assert "slip=0" in first["arguments"]
    assert "hypo=0" in first["arguments"]
    assert {entry["lfn"] for entry in first["uses"] if entry["type"] == "output"} == {
        f"Seismogram_TEST_{run_id}_0_0.grm",
        f"PeakVals_TEST_{run_id}_0_0.bsa",
    }
    assert (tmp_path / "rupture_file_list_TEST_0").read_text(
        encoding="utf-8"
    ) == "1\ne64_rv10_0_0.txt\n"


def test_seis_memory_matches_legacy_integer_division(db: SQLAlchemyRunDB) -> None:
    info = load_run_info(db, 900008)
    assert PP._seis_memory(100, info, 0.05, 8000) == 81


def test_no_insert_routes_synthesis_directly_to_post(
    tmp_path: Path,
    db: SQLAlchemyRunDB,
) -> None:
    run_id = _insert_run(db)

    _generator(db, direct_synth=True, output_directory=tmp_path, no_insert=True).create(
        run_id=run_id,
        run_directory=tmp_path,
    )

    workflow = _load(tmp_path / "CyberShake_TEST.dax")
    assert "dbDax" not in _jobs(workflow)
    assert _dependencies(workflow)["dax_0"] == {"postDax"}


def test_direct_synth_directory_hierarchy_adds_preparation_job(
    tmp_path: Path,
    db: SQLAlchemyRunDB,
) -> None:
    run_id = _insert_run(db)

    _generator(
        db, direct_synth=True, output_directory=tmp_path, dir_hierarchy=True
    ).create_synthesis(
        workflow_file=tmp_path / "CyberShake_TEST_Synth.dax",
        run_id=run_id,
        workflow_index=0,
    )

    workflow = _load(tmp_path / "CyberShake_TEST_Synth.dax")
    assert set(_jobs(workflow)) == {"DirCreate", "DirectSynth"}
    assert _dependencies(workflow) == {"DirCreate": {"DirectSynth"}}
    assert (tmp_path / "directory_list.txt").read_text(
        encoding="utf-8"
    ) == "0/e64_rv10_0_0.txt\n"
    direct = _jobs(workflow)["DirectSynth"]
    assert "dir_hierarchy=1" in direct["arguments"]
    assert "e64_rv10_0_0.txt" not in {
        entry["lfn"] for entry in direct["uses"] if entry["type"] == "input"
    }


def test_high_frequency_requires_direct_synth(
    tmp_path: Path,
    db: SQLAlchemyRunDB,
) -> None:
    run_id = _insert_run(db)
    generator = _generator(db, direct_synth=False)
    generator.high_frequency = 1.0

    with pytest.raises(PpError, match="requires DirectSynth"):
        generator.create(run_id=run_id, run_directory=tmp_path)


def test_high_frequency_updates_run_and_uses_pp_products(
    tmp_path: Path,
    db: SQLAlchemyRunDB,
) -> None:
    run_id = _insert_run(db)

    _generator(
        db, direct_synth=True, output_directory=tmp_path, high_frequency=0.75
    ).create(
        run_id=run_id,
        run_directory=tmp_path,
    )

    run = db.get_run(run_id)
    assert run is not None
    assert run.max_frequency == 1.0
    assert run.low_frequency_cutoff == 0.75
    synthesis = _jobs(_load(tmp_path / "CyberShake_TEST_Synth.dax"))
    assert "stoch_max_freq=1.0" in synthesis["DirectSynth"]["arguments"]
    products = _jobs(_load(tmp_path / "CyberShake_TEST_DB_Products.dax"))
    assert "CS_Products_Curve_Calc_RotD50_TEST" in products
    assert "CS_Products_Curve_Calc_RotD50_Wrapper_TEST" not in products


def test_normal_directory_hierarchy_uses_nested_sgt_subsets(
    tmp_path: Path,
    db: SQLAlchemyRunDB,
) -> None:
    run_id = _insert_run(db)
    rupture_list = tmp_path / "ruptures.csv"
    rupture_list.write_text("0,0\n", encoding="utf-8")
    workflow_file = tmp_path / "CyberShake_TEST_0.dax"

    _generator(
        db,
        direct_synth=False,
        dir_hierarchy=True,
        rupture_list=rupture_list,
    ).create_synthesis(
        workflow_file=workflow_file,
        run_id=run_id,
        workflow_index=0,
    )

    first = _jobs(_load(workflow_file))["ID2_0_0_0"]
    assert "sgt_xfile=0/0/TEST_0_0_subfx.sgt" in first["arguments"]
    assert "sgt_yfile=0/0/TEST_0_0_subfy.sgt" in first["arguments"]
