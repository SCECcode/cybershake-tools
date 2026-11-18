"""Tests for pure-Python stochastic workflow generation."""

from __future__ import annotations

import datetime as dt
from typing import TYPE_CHECKING, Any

import pytest
import yaml

from Pegasus.api import Job

from cybershake.config import resolve_config
from cybershake.constants import SGT_GENERATED_STATE
from cybershake.runmanager.db import RuptureSummary
from cybershake.runmanager.schema import Run
from cybershake.workflow.stoch import Stoch

if TYPE_CHECKING:
    from pathlib import Path

    from pytest_mock import MockerFixture

    from cybershake.runmanager.db import SQLAlchemyRunDB


def _insert_run(
    database: SQLAlchemyRunDB,
    *,
    maximum_frequency: float,
    low_frequency: float,
    target_vs30: float | None = 760.0,
) -> int:
    return database.insert_run(
        Run(
            site_id=9000,
            erf_id=64,
            sgt_variation_id=8,
            rup_var_scenario_id=10,
            velocity_model_id=15,
            max_frequency=maximum_frequency,
            low_frequency_cutoff=low_frequency,
            sgt_source_filter_frequency=2.0,
            status=SGT_GENERATED_STATE,
            status_time=dt.datetime(2026, 10, 23),
            last_user="mayani",
            target_vs30=target_vs30,
            vs30_source="User-provided" if target_vs30 is not None else None,
        )
    )


def _generator(
    database: SQLAlchemyRunDB, **overrides: bool | float | str | None
) -> Stoch:
    options: dict[str, bool | float | str | None] = {
        "generator_merge_frequency": None,
        "no_rotd": False,
        "no_site_response": False,
        "no_low_site_response": False,
        "no_duration": False,
        "debug": False,
        "db_rv_seed": True,
        "hf_vel_model": "nocal",
        "period_duration": True,
        "z_comp": True,
        "no_vertical_response": False,
        "stoch_frequency": 50.0,
        "force_vs30": None,
    }
    options.update(overrides)
    return Stoch(config=resolve_config("stoch", options, environ={}), database=database)


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


@pytest.mark.parametrize(
    ("z_comp", "no_vertical_response", "expected"),
    [
        (False, False, True),
        (False, True, True),
        (True, False, False),
        (True, True, True),
    ],
)
def test_deferred_stoch_vertical_response_flag(
    db: SQLAlchemyRunDB,
    *,
    z_comp: bool,
    no_vertical_response: bool,
    expected: bool,
) -> None:
    generator = _generator(
        db,
        z_comp=z_comp,
        no_vertical_response=no_vertical_response,
    )
    node = Job("GenStoch")
    generator._add_generator_flags(node)
    assert ("--no-vertical-response" in node.args) is expected


def test_deferred_stoch_disables_database_seeds(db: SQLAlchemyRunDB) -> None:
    generator = _generator(db, db_rv_seed=False)
    node = Job("GenStoch")

    generator._add_generator_flags(node)

    assert "--no-db-rv-seed" in node.args


def test_create_top_level_stochastic_workflow(
    tmp_path: Path,
    db: SQLAlchemyRunDB,
) -> None:
    low_id = _insert_run(db, maximum_frequency=1.0, low_frequency=1.0)
    run_id = _insert_run(db, maximum_frequency=50.0, low_frequency=1.0)

    generator = _generator(db, no_low_site_response=True)
    config = generator.config
    generator.create(
        run_id=run_id,
        run_directory=tmp_path,
        low_frequency_run_id=low_id,
    )

    top = _load(tmp_path / "CyberShake_Stoch_TEST_top.dax")
    assert top["metadata"] == {key: str(value) for key, value in config.items()}
    jobs = _jobs(top)
    assert set(jobs) == {
        "UpdateRun_PP_END",
        "Velocity_Info",
        "BB_Velocity_Insert",
        "GenStochDax_TEST",
        "Sub_Stoch_TEST",
        "dbDax",
    }
    assert jobs["Velocity_Info"]["arguments"][-3:] == [
        "100",
        "25.0",
        "velocity_info_TEST.txt",
    ]
    assert jobs["BB_Velocity_Insert"]["arguments"][0] == ("insert-bb-velocity-params")
    generator_args = jobs["GenStochDax_TEST"]["arguments"]
    assert generator_args[0] == "sub-stoch-wf"
    assert "--no-low-site-response" in generator_args
    assert "--db-rv-seed" in generator_args
    assert "--no-db-rv-seed" not in generator_args
    assert "--z-comp" in generator_args
    assert _dependencies(top) == {
        "Velocity_Info": {
            "BB_Velocity_Insert",
            "GenStochDax_TEST",
            "dbDax",
        },
        "GenStochDax_TEST": {"Sub_Stoch_TEST", "dbDax"},
        "Sub_Stoch_TEST": {"dbDax"},
        "dbDax": {"UpdateRun_PP_END"},
    }
    products = _load(tmp_path / "CyberShake_Stoch_TEST_Stoch_DB_Products.dax")
    product_jobs = _jobs(products)
    assert "CS_Products_Curve_Calc_RotD50_Wrapper_TEST" in product_jobs
    curve_args = product_jobs["CS_Products_Curve_Calc_RotD50_Wrapper_TEST"]["arguments"]
    assert curve_args[:4] == [
        "jobs",
        "curve-plot",
        "--velocity-file",
        "velocity_info_TEST.txt",
    ]
    assert curve_args[curve_args.index("--period") + 1] == (
        "3,4,5,7.5,10,2,1,0.5,0.2,0.1"
    )
    insert_periods = (
        "-periods 10,7.5,5,4,3,2,PGV,1,0.75,0.5,0.4,0.3,0.2,0.1,"
        "PGA,0.075,0.05,0.04,0.03,0.02,0.01"
    )
    assert (
        insert_periods in product_jobs["CS_Products_Load_Amps_RotD_TEST"]["arguments"]
    )


def test_create_persists_forced_vs30(
    tmp_path: Path,
    db: SQLAlchemyRunDB,
) -> None:
    low_id = _insert_run(db, maximum_frequency=1.0, low_frequency=1.0)
    run_id = _insert_run(
        db,
        maximum_frequency=50.0,
        low_frequency=1.0,
        target_vs30=None,
    )

    _generator(db, force_vs30=625.0).create(
        run_id=run_id,
        run_directory=tmp_path,
        low_frequency_run_id=low_id,
    )

    run = db.get_run(run_id)
    assert run is not None
    assert run.target_vs30 == 625.0
    assert run.vs30_source == "User-provided"


def test_create_runtime_stochastic_workflow(
    tmp_path: Path,
    db: SQLAlchemyRunDB,
) -> None:
    low_id = _insert_run(db, maximum_frequency=1.0, low_frequency=1.0)
    run_id = _insert_run(db, maximum_frequency=50.0, low_frequency=1.0)
    velocity_file = tmp_path / "velocity_info_TEST.txt"
    velocity_file.write_text(
        "Vs at site = 600\nVs30 = 500\nVsD = 700\n",
        encoding="utf-8",
    )
    workflow_file = tmp_path / "CyberShake_Stoch_TEST.dax"

    _generator(db).create_sub_stoch(
        run_id=run_id,
        low_frequency_run_id=low_id,
        velocity_file=velocity_file,
        workflow_file=workflow_file,
    )

    workflow = _load(workflow_file)
    assert workflow["replicaCatalog"]["replicas"] == [
        {
            "lfn": "dirs_list.txt",
            "pfns": [
                {
                    "pfn": (
                        "go://56569ec1-af41-4745-a8d1-8514231c7a6d"
                        f"{tmp_path / 'dirs_list.txt'}"
                    ),
                    "site": "local",
                }
            ],
        }
    ]
    jobs = _jobs(workflow)
    assert set(jobs) == {
        "UpdateRun_PP_START",
        "Create_Local_VM",
        "Create_Dirs",
        "HF_Synth_0_0",
        "LF_Site_Response_0_0",
        "Merge_IM_0_0",
    }
    hf = jobs["HF_Synth_0_0"]
    assert "tlen=400.0" in hf["arguments"]
    assert "rvfrac_seed_given=1" in hf["arguments"]
    assert (
        "rup_vars=(0,0,0,0.843884,0);(1,1,0,0.685883,551188310);"
        "(2,2,0,0.779819,1157490780)"
    ) in hf["arguments"]
    assert "num_comps=3" in hf["arguments"]
    low_response = jobs["LF_Site_Response_0_0"]
    assert "vref=840.0" in low_response["arguments"]
    merge = jobs["Merge_IM_0_0"]
    assert "run_rotd=1" in merge["arguments"]
    assert "run_vert_rsp=1" in merge["arguments"]
    assert "run_duration=1" in merge["arguments"]
    assert "run_period_durations=1" in merge["arguments"]
    assert {entry["lfn"] for entry in merge["uses"] if entry["type"] == "output"} == {
        f"0/Seismogram_TEST_{run_id}_0_0_bb.grm",
        f"0/PeakVals_TEST_{run_id}_0_0_bb.bsa",
        f"0/RotD_TEST_{run_id}_0_0_bb.rotd",
        f"0/VerticalRSP_TEST_{run_id}_0_0_bb.rsp",
        f"0/Duration_TEST_{run_id}_0_0_bb.dur",
        f"0/PeriodDuration_TEST_{run_id}_0_0_bb.dur",
    }
    dependencies = _dependencies(workflow)
    assert dependencies["UpdateRun_PP_START"] == {
        "Create_Local_VM",
        "Create_Dirs",
    }
    assert dependencies["HF_Synth_0_0"] == {
        "LF_Site_Response_0_0",
        "Merge_IM_0_0",
    }
    assert (tmp_path / "dirs_list.txt").read_text(encoding="utf-8") == "0\n"


def test_runtime_options_remove_optional_product_jobs(
    tmp_path: Path,
    db: SQLAlchemyRunDB,
) -> None:
    low_id = _insert_run(db, maximum_frequency=1.0, low_frequency=1.0)
    run_id = _insert_run(db, maximum_frequency=10.0, low_frequency=1.0)
    velocity_file = tmp_path / "velocity_info_TEST.txt"
    velocity_file.write_text("a=600\nb=500\nc=700\n", encoding="utf-8")
    workflow_file = tmp_path / "runtime.dax"

    _generator(
        db,
        no_rotd=True,
        no_site_response=True,
        no_low_site_response=True,
        no_duration=True,
        db_rv_seed=False,
        period_duration=False,
        z_comp=False,
    ).create_sub_stoch(
        run_id=run_id,
        low_frequency_run_id=low_id,
        velocity_file=velocity_file,
        workflow_file=workflow_file,
    )

    jobs = _jobs(_load(workflow_file))
    assert "LF_Site_Response_0_0" not in jobs
    assert "do_site_response=0" in jobs["HF_Synth_0_0"]["arguments"]
    merge_args = jobs["Merge_IM_0_0"]["arguments"]
    assert "run_rotd=0" in merge_args
    assert "run_duration=0" in merge_args
    assert "run_period_durations=0" in merge_args


def test_runtime_partitions_large_ruptures_and_combines_outputs(
    tmp_path: Path,
    db: SQLAlchemyRunDB,
    mocker: MockerFixture,
) -> None:
    low_id = _insert_run(db, maximum_frequency=1.0, low_frequency=1.0)
    run_id = _insert_run(db, maximum_frequency=50.0, low_frequency=1.0)
    velocity_file = tmp_path / "velocity_info_TEST.txt"
    velocity_file.write_text("a=600\nb=500\nc=700\n", encoding="utf-8")
    generator = _generator(
        db,
        db_rv_seed=False,
        no_low_site_response=True,
    )
    mocker.patch.object(
        generator,
        "_ruptures",
        return_value=(RuptureSummary(0, 0, 6468, 22, 294, 5.95, 400),),
    )
    workflow_file = tmp_path / "runtime.dax"

    generator.create_sub_stoch(
        run_id=run_id,
        low_frequency_run_id=low_id,
        velocity_file=velocity_file,
        workflow_file=workflow_file,
    )

    workflow = _load(workflow_file)
    jobs = _jobs(workflow)
    assert {
        "HF_Synth_0_0_t0",
        "HF_Synth_0_0_t1",
        "HF_Synth_0_0_t2",
        "Combine_HF_Synth_0_0",
        "Combine_PGA_0_0",
    }.issubset(jobs)
    dependencies = _dependencies(workflow)
    assert dependencies["Combine_HF_Synth_0_0"] == {"Merge_IM_0_0"}
    assert dependencies["Combine_PGA_0_0"] == {"Merge_IM_0_0"}
