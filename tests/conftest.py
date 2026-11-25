"""Shared pytest fixtures."""

from __future__ import annotations

import datetime as dt
import os
from typing import TYPE_CHECKING

import pytest

from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session

from cybershake.constants import DONE_STATE, SGT_GENERATED_STATE
from cybershake.runmanager.db import SQLAlchemyRunDB
from cybershake.runmanager.schema import (
    ERF,
    Base,
    ERFMetadata,
    ERFProbabilityModel,
    Run,
    Rupture,
    RuptureVariation,
    RuptureVariationScenario,
    RupVarSeed,
    SGTVariation,
    Site,
    SiteRegion,
    SiteRupture,
    SiteType,
    TimeSpan,
    VelocityModel,
)

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

    from sqlalchemy.engine import Engine


@pytest.fixture
def fake_bin(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Directory put first on ``PATH`` for fake external tools."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")
    return bin_dir


@pytest.fixture(scope="session")
def engine(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Engine]:
    """SQLite engine on a temp-dir database with the CyberShake schema created."""
    path: Path = tmp_path_factory.mktemp("db") / "cybershake.db"
    eng = create_engine(f"sqlite:///{path}")

    @event.listens_for(eng, "connect")
    def _enable_foreign_keys(dbapi_conn, _record) -> None:  # noqa: ANN001
        dbapi_conn.execute("PRAGMA foreign_keys=ON")

    Base.metadata.create_all(eng)
    yield eng
    eng.dispose()


@pytest.fixture
def session(engine: Engine) -> Iterator[Session]:
    """ORM session on the shared database; all rows are removed afterwards.

    Committed rows are deleted too (children first, so foreign keys hold),
    so each test starts with empty tables.
    """
    with Session(engine) as s:
        yield s
        s.rollback()
        for table in reversed(Base.metadata.sorted_tables):
            s.execute(table.delete())
        s.commit()


@pytest.fixture
def db(seeded_session: Session, engine: Engine) -> SQLAlchemyRunDB:
    """Run database backed by the seeded SQLite fixture."""
    seeded_session.commit()
    return SQLAlchemyRunDB(engine)


@pytest.fixture
def cwd(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Temporary working directory."""
    monkeypatch.chdir(tmp_path)
    return tmp_path


@pytest.fixture
def seeded_session(session: Session) -> Session:
    """``session`` containing the complete TEST workflow-generator dataset."""
    site_id = 9000
    session.add(
        SiteType(
            cs_site_type_id=3,
            cs_site_type_name="Workflow Generator Test Site",
            cs_site_type_short_name="TEST",
        )
    )
    session.flush()
    session.add(
        Site(
            cs_site_id=site_id,
            cs_site_name="Workflow Generator Test Site",
            cs_short_name="TEST",
            cs_site_lat=34.0001,
            cs_site_lon=-118.0001,
            cs_site_type_id=3,
        )
    )
    session.flush()
    session.add_all(
        [
            ERFProbabilityModel(
                prob_model_id=1,
                prob_model_name="Poisson",
                prob_model_description="Time independent probabilities",
            ),
            TimeSpan(time_span_id=1, time_span_units="years", time_span_duration=1),
        ]
    )
    session.flush()
    session.add(
        ERF(
            erf_id=64,
            erf_name="WGCEP (2007) UCERF2 NorCal - Single Branch 200m",
            erf_description=(
                "NorCal Mean UCERF 2 - Single Branch Earthquake Rupture Forecast"
                " FINAL, 200m"
            ),
            default_prob_model_id=1,
            default_time_span_id=1,
        )
    )
    session.flush()
    session.add(
        SGTVariation(
            sgt_variation_id=8,
            sgt_variation_name="AWP_ODC_SGT GPU",
            sgt_variation_description="SGTs generated with AWP-ODC-SGT GPU",
        )
    )
    session.flush()
    session.add(
        VelocityModel(
            velocity_model_id=15,
            velocity_model_name="Study 24.6 Stitched",
            velocity_model_version="",
        )
    )
    session.flush()
    session.add(
        ERF(
            erf_id=36,
            erf_name="WGCEP (2007) UCERF2 - Single Branch 200m",
            erf_description=(
                "Mean UCERF 2 - Single Branch Earthquake Rupture Forecast FINAL, 200m"
            ),
            default_prob_model_id=1,
            default_time_span_id=1,
        )
    )
    session.flush()
    session.add(
        RuptureVariationScenario(
            rup_var_scenario_id=10,
            erf_id=36,
            rup_var_scenario_name="genslip-v5.5.2",
            rup_var_scenario_description=(
                "Graves & Pitarka (2022), same as in BBP v22.4; uniform grid down dip"
                " hypocenters with target_hypo_spacing=4 km"
            ),
        )
    )
    session.flush()
    session.add(
        ERFMetadata(
            erf_id=64,
            erf_attr_name="Rupture Surface Resolution",
            erf_attr_value="0.2",
            erf_attr_type="Float",
            erf_attr_units="km",
        )
    )
    session.flush()
    session.add(
        SiteRegion(
            cs_site_id=site_id,
            erf_id=64,
            cutoff_dist=20,
            max_lat=34.1,
            max_lat_source_id=0,
            max_lat_rupture_id=0,
            max_lon=-117.9,
            max_lon_source_id=0,
            max_lon_rupture_id=0,
            min_lat=33.9,
            min_lat_source_id=0,
            min_lat_rupture_id=0,
            min_lon=-118.1,
            min_lon_source_id=0,
            min_lon_rupture_id=0,
        )
    )
    session.add_all(
        [
            Rupture(
                erf_id=64,
                source_id=0,
                rupture_id=rid,
                source_name="Dummy source",
                source_type="test",
                mag=mag,
                prob=prob,
                grid_spacing=0.2,
                num_rows=22,
                num_columns=294,
                num_points=6468,
                start_lat=37.444294,
                start_lon=-121.8064,
                start_depth=4.28,
                end_lat=36.99136,
                end_lon=-121.468895,
                end_depth=4.28,
            )
            for rid, mag, prob in [(0, 5.95, 0.0003038352)]
        ]
    )
    session.flush()
    session.add_all(
        [
            RuptureVariation(
                erf_id=64,
                source_id=0,
                rupture_id=0,
                rup_var_id=vid,
                rup_var_scenario_id=10,
                rup_var_lfn=f"e64_rv10_0_0.txt.variation-s{vid}-h0",
                hypocenter_lat=lat,
                hypocenter_lon=lon,
                hypocenter_depth=depth,
                rvfrac=frac,
            )
            for vid, lat, lon, depth, frac in [
                (0, 37.42942, -121.76797, 5.68, 0.843884),
                (1, 37.39082, -121.75667, 5.08, 0.685883),
                (2, 37.36213, -121.73324, 5.28, 0.779819),
            ]
        ]
    )
    session.flush()
    session.add_all(
        [
            RupVarSeed(
                erf_id=64,
                rup_var_scenario_id=10,
                source_id=0,
                rupture_id=0,
                rup_var_id=vid,
                rup_var_seed=seed,
            )
            for vid, seed in [(0, 0), (1, 551188310), (2, 1157490780)]
        ]
    )
    session.add_all(
        [
            SiteRupture(
                cs_site_id=site_id,
                erf_id=64,
                source_id=0,
                rupture_id=rid,
                cutoff_dist=20,
                site_rupture_dist=10.0,
            )
            for rid in (0,)
        ]
    )
    session.flush()
    session.add_all(
        [
            ERF(
                erf_id=erf_id,
                erf_name=f"Dummy ERF {erf_id}",
                erf_description="Workflow-generator integration fixture",
                default_prob_model_id=1,
                default_time_span_id=1,
            )
            for erf_id in (60, 62, 63)
        ]
    )
    session.add_all(
        [
            SGTVariation(
                sgt_variation_id=variation_id,
                sgt_variation_name=name,
                sgt_variation_description="Workflow-generator integration fixture",
            )
            for variation_id, name in (
                (5, "RWG"),
                (6, "AWP CPU"),
                (7, "RWG 3.0.3"),
            )
        ]
    )
    session.add_all(
        [
            RuptureVariationScenario(
                rup_var_scenario_id=scenario_id,
                erf_id=erf_id,
                rup_var_scenario_name=f"Dummy scenario {scenario_id}",
                rup_var_scenario_description=("Workflow-generator integration fixture"),
            )
            for scenario_id, erf_id in (
                (5, 64),
                (6, 64),
                (7, 64),
                (8, 60),
                (9, 64),
            )
        ]
    )
    session.flush()

    session.add_all(
        [
            SiteRegion(
                cs_site_id=site_id,
                erf_id=erf_id,
                cutoff_dist=20,
                max_lat=34.1,
                max_lat_source_id=9000,
                max_lat_rupture_id=0,
                max_lon=-117.9,
                max_lon_source_id=9000,
                max_lon_rupture_id=0,
                min_lat=33.9,
                min_lat_source_id=9000,
                min_lat_rupture_id=0,
                min_lon=-118.1,
                min_lon_source_id=9000,
                min_lon_rupture_id=0,
            )
            for erf_id in (60, 62, 63)
        ]
    )
    session.add_all(
        [
            Rupture(
                erf_id=erf_id,
                source_id=9000,
                rupture_id=0,
                source_name="Dummy source",
                source_type="test",
                mag=6.0,
                prob=0.01,
                grid_spacing=1.0,
                num_rows=10,
                num_columns=10,
                num_points=100,
                start_lat=34.0,
                start_lon=-118.0,
                start_depth=1.0,
                end_lat=34.1,
                end_lon=-117.9,
                end_depth=10.0,
            )
            for erf_id in (60, 62, 63)
        ]
    )
    session.flush()
    session.add_all(
        [
            SiteRupture(
                cs_site_id=site_id,
                erf_id=erf_id,
                source_id=9000,
                rupture_id=0,
                cutoff_dist=20,
                site_rupture_dist=10.0,
            )
            for erf_id in (60, 62, 63)
        ]
    )

    combinations = (
        (64, 5),
        (64, 6),
        (64, 7),
        (64, 8),
        (64, 9),
        (60, 8),
        (62, 8),
        (63, 8),
    )
    variations: list[RuptureVariation] = []
    for erf_id, scenario_id in combinations:
        source_id = 0 if erf_id == 64 else 9000
        for variation_id in range(1 if scenario_id == 8 else 2):
            if scenario_id == 8:
                lfn = f"e{erf_id}_rv8_{source_id}_0_event{variation_id}.srf"
            else:
                lfn = (
                    f"e{erf_id}_rv{scenario_id}_{source_id}_0.txt."
                    f"variation-s{variation_id}-h0"
                )
            variations.append(
                RuptureVariation(
                    erf_id=erf_id,
                    source_id=source_id,
                    rupture_id=0,
                    rup_var_id=variation_id,
                    rup_var_scenario_id=scenario_id,
                    rup_var_lfn=lfn,
                    hypocenter_lat=34.01 + variation_id * 0.01,
                    hypocenter_lon=-118.01 - variation_id * 0.01,
                    hypocenter_depth=5.0 + variation_id,
                    rvfrac=0.5,
                )
            )
    session.add_all(variations)
    session.flush()
    session.add_all(
        [
            RupVarSeed(
                erf_id=variation.erf_id,
                rup_var_scenario_id=variation.rup_var_scenario_id,
                source_id=variation.source_id,
                rupture_id=variation.rupture_id,
                rup_var_id=variation.rup_var_id,
                rup_var_seed=(
                    variation.erf_id * 10000
                    + variation.rup_var_scenario_id * 100
                    + variation.rup_var_id
                ),
            )
            for variation in variations
        ]
    )

    timestamp = dt.datetime(2026, 1, 1)
    run_specs: tuple[tuple[int, int, int, int, str, float, float, float, str], ...] = (
        (900005, 64, 5, 5, SGT_GENERATED_STATE, 1.0, 1.0, 2.0, "RWG"),
        (900006, 64, 6, 6, SGT_GENERATED_STATE, 1.0, 1.0, 2.0, "AWP CPU"),
        (900007, 64, 7, 7, SGT_GENERATED_STATE, 0.5, 0.5, 1.0, "RWG 3.0.3"),
        (900008, 64, 8, 10, DONE_STATE, 1.0, 1.0, 2.0, "Standard LF"),
        (900009, 64, 8, 9, SGT_GENERATED_STATE, 1.0, 1.0, 2.0, "Scenario 9"),
        (900010, 64, 8, 10, SGT_GENERATED_STATE, 1.0, 1.0, 2.0, "Scenario 10"),
        (900860, 60, 8, 8, DONE_STATE, 1.0, 1.0, 2.0, "RSQSim LF"),
        (900861, 60, 8, 8, SGT_GENERATED_STATE, 1.0, 1.0, 2.0, "RSQSim 60"),
        (900862, 62, 8, 8, SGT_GENERATED_STATE, 1.0, 1.0, 2.0, "RSQSim 62"),
        (900863, 63, 8, 8, SGT_GENERATED_STATE, 1.0, 1.0, 2.0, "RSQSim 63"),
        (900864, 64, 8, 8, SGT_GENERATED_STATE, 1.0, 1.0, 2.0, "RSQSim 64"),
        (901010, 64, 8, 10, SGT_GENERATED_STATE, 50.0, 1.0, 2.0, "Standard BB"),
        (901860, 60, 8, 8, SGT_GENERATED_STATE, 50.0, 1.0, 2.0, "RSQSim BB"),
    )
    session.add_all(
        [
            Run(
                run_id=run_id,
                site_id=site_id,
                erf_id=erf_id,
                sgt_variation_id=sgt_variation_id,
                rup_var_scenario_id=scenario_id,
                status=status,
                status_time=timestamp,
                sgt_host="hpc",
                sgt_time=timestamp,
                last_user="mayani",
                comment=comment,
                velocity_model_id=15,
                max_frequency=max_frequency,
                low_frequency_cutoff=low_frequency,
                model_vs30=500.0,
                mesh_vsitop=600.0,
                vs30_source="Dummy fixture",
                z2_5=2.5,
                z1_0=1.0,
                minimum_vs=500.0,
                target_vs30=760.0,
                sgt_source_filter_frequency=source_frequency,
            )
            for (
                run_id,
                erf_id,
                sgt_variation_id,
                scenario_id,
                status,
                max_frequency,
                low_frequency,
                source_frequency,
                comment,
            ) in run_specs
        ]
    )
    session.flush()
    return session
