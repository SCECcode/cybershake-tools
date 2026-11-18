"""Tests for pure-Python integrated workflow generation."""

from __future__ import annotations

import datetime as dt
from typing import TYPE_CHECKING, Any

import pytest
import yaml

from cybershake.config import resolve_config
from cybershake.constants import START_STATE
from cybershake.exc import FullError
from cybershake.runmanager.schema import Run
from cybershake.vs30 import Vs30
from cybershake.workflow.full import Full
from cybershake.workflow.pp import PP
from cybershake.workflow.sgt import SGT
from cybershake.workflow.stoch import Stoch

if TYPE_CHECKING:
    from pathlib import Path

    from pytest_mock import MockerFixture

    from cybershake.runmanager.db import SQLAlchemyRunDB


def _insert_run(
    database: SQLAlchemyRunDB,
    *,
    maximum_frequency: float,
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
            low_frequency_cutoff=1.0,
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
    broadband: bool = True,
    stoch_frequency: float | None = 50.0,
    force_vs30: float | None = None,
) -> Full:
    config = resolve_config(
        "full",
        {
            "site": "TEST",
            "velocity_model": "v246",
            "rotd": True,
            "broadband": broadband,
            "stoch_frequency": stoch_frequency,
            "force_vs30": force_vs30,
        },
        environ={},
    )
    sgt = SGT(config=config, database=database)
    pp = PP(config=config, database=database)
    stoch = Stoch(config=config, database=database)
    return Full(sgt=sgt, pp=pp, stoch=stoch)


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
    ("initial_lf_vs30", "force_vs30", "expected_lf_vs30", "expected_lf_source"),
    [
        (None, None, 578.23, "Thompson et al. (2020)"),
        (None, 760.0, 760.0, "User-provided"),
        (578.23, 760.0, 760.0, "User-provided"),
    ],
)
def test_create_composes_stage_workflows_in_legacy_order(
    tmp_path: Path,
    db: SQLAlchemyRunDB,
    mocker: MockerFixture,
    initial_lf_vs30: float | None,
    force_vs30: float | None,
    expected_lf_vs30: float,
    expected_lf_source: str,
) -> None:
    low_frequency_run_id = _insert_run(
        db, maximum_frequency=1.0, target_vs30=initial_lf_vs30
    )
    broadband_run_id = _insert_run(db, maximum_frequency=50.0)
    workflow_file = tmp_path / "CyberShake_Integrated_TEST.dax"
    lookup = mocker.patch(
        "cybershake.workflow.sgt.lookup_vs30",
        return_value=Vs30(578.23, "Thompson et al. (2020)"),
    )

    generator = _generator(db, force_vs30=force_vs30)
    config = generator.config
    generator.create(
        workflow_file=workflow_file,
        run_directory=tmp_path,
        run_ids=(low_frequency_run_id,),
        broadband_run_id=broadband_run_id,
    )

    workflow = _load(workflow_file)
    jobs = _jobs(workflow)
    assert workflow["name"] == "CyberShake_Integrated_TEST"
    assert workflow["metadata"] == {key: str(value) for key, value in config.items()}
    assert set(jobs) == {
        "UpdateRun_SGT_START_TEST",
        "PreCVM_TEST",
        "GenSGTDax_TEST",
        "AWP_SGT_TEST",
        f"SetJobID_TEST_{low_frequency_run_id}",
        "TEST_preWf",
        "TEST_synthesisWf_0",
        "TEST_productsWf",
        "TEST_postWf",
        f"UpdateRun{broadband_run_id}_SGT_START",
        f"UpdateRun{broadband_run_id}_PP_INIT",
        "TEST_stochWf",
    }
    dependencies = _dependencies(workflow)
    assert dependencies["AWP_SGT_TEST"] == {
        f"SetJobID_TEST_{low_frequency_run_id}",
        "TEST_preWf",
    }
    assert dependencies[f"SetJobID_TEST_{low_frequency_run_id}"] == {"TEST_preWf"}
    assert dependencies["TEST_preWf"] == {
        "TEST_synthesisWf_0",
        f"UpdateRun{broadband_run_id}_SGT_START",
    }
    assert dependencies["TEST_synthesisWf_0"] == {
        "TEST_productsWf",
        "TEST_stochWf",
    }
    assert dependencies["TEST_productsWf"] == {"TEST_postWf"}
    assert dependencies["TEST_stochWf"] == {"TEST_postWf"}

    expected_files = {
        "CyberShake_TEST_pre.dax",
        "CyberShake_TEST_Synth.dax",
        "CyberShake_TEST_DB_Products.dax",
        "CyberShake_TEST_post.dax",
        "CyberShake_Stoch_TEST_top.dax",
        "CyberShake_Stoch_TEST_Stoch_DB_Products.dax",
    }
    assert expected_files <= {path.name for path in tmp_path.iterdir()}
    if initial_lf_vs30 is None:
        lookup.assert_called_once_with(
            34.0001, -118.0001, classpath=str(config["vs30_classpath"])
        )
    else:
        lookup.assert_not_called()
    low_run = db.get_run(low_frequency_run_id)
    assert low_run is not None
    assert low_run.target_vs30 == expected_lf_vs30
    assert low_run.vs30_source == expected_lf_source
    if force_vs30 is not None:
        broadband_run = db.get_run(broadband_run_id)
        assert broadband_run is not None
        assert broadband_run.target_vs30 == force_vs30
        assert broadband_run.vs30_source == "User-provided"


def test_create_requires_broadband_metadata(
    tmp_path: Path,
    db: SQLAlchemyRunDB,
) -> None:
    run_id = _insert_run(db, maximum_frequency=1.0)

    with pytest.raises(FullError, match="linked run ID"):
        _generator(db, stoch_frequency=None).create(
            workflow_file=tmp_path / "CyberShake_Integrated_TEST.dax",
            run_directory=tmp_path,
            run_ids=(run_id,),
            broadband_run_id=None,
        )
