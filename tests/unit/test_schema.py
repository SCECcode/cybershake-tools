"""Tests that the SQLAlchemy schema works on MySQL and SQLite."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from sqlalchemy import Table, func, inspect, select
from sqlalchemy.dialects import mysql
from sqlalchemy.exc import IntegrityError
from sqlalchemy.schema import CreateTable

from cybershake.runmanager.schema import (
    ERF,
    Base,
    ERFMetadata,
    Point,
    Run,
    RuptureVariationScenario,
    SGTVariation,
    Site,
    SiteRegion,
    SiteType,
    VelocityModel,
)

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine
    from sqlalchemy.orm import Session


def test_mysql_ddl_is_unsigned() -> None:
    run_table = Run.__table__
    assert isinstance(run_table, Table)
    ddl = str(CreateTable(run_table).compile(dialect=mysql.dialect()))
    assert "`Run_ID` INTEGER UNSIGNED NOT NULL AUTO_INCREMENT" in ddl
    assert "ENGINE=InnoDB" in ddl
    assert "CHARSET=latin1" in ddl
    point_table = Point.__table__
    assert isinstance(point_table, Table)
    point = str(CreateTable(point_table).compile(dialect=mysql.dialect()))
    assert "`Point_ID` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT" in point


@pytest.mark.parametrize("table", Base.metadata.sorted_tables, ids=lambda t: t.name)
def test_every_table_compiles_on_mysql(table: Table) -> None:
    assert str(CreateTable(table).compile(dialect=mysql.dialect()))


def test_sqlite_creates_all_tables(engine: Engine) -> None:
    assert set(inspect(engine).get_table_names()) == set(Base.metadata.tables)


def test_sqlite_autoincrement(session: Session) -> None:
    site_type = SiteType(cs_site_type_name="x", cs_site_type_short_name="x")
    session.add(site_type)
    session.flush()
    site = Site(
        cs_site_name="USC",
        cs_short_name="USC",
        cs_site_lat=34.0,
        cs_site_lon=-118.0,
        cs_site_type_id=site_type.cs_site_type_id,
    )
    session.add(site)
    session.flush()
    assert site.cs_site_id is not None


def test_sqlite_enforces_foreign_keys(session: Session) -> None:
    session.add(ERF(erf_name="e", default_prob_model_id=99, default_time_span_id=99))
    with pytest.raises(IntegrityError, match="FOREIGN KEY"):
        session.flush()


def test_session_rolls_back(session: Session) -> None:
    assert session.scalars(select(Site)).first() is None


def test_committed_rows_are_cleaned_up(session: Session) -> None:
    session.add(SiteType(cs_site_type_name="y", cs_site_type_short_name="y"))
    session.commit()


def test_session_is_empty_after_commit(session: Session) -> None:
    assert session.scalars(select(SiteType)).first() is None


def test_seeded_session_has_site_test(seeded_session: Session) -> None:
    site = seeded_session.get(Site, 9000)
    assert site is not None
    assert (site.cs_short_name, site.cs_site_name) == (
        "TEST",
        "Workflow Generator Test Site",
    )


def test_seeded_session_has_erf_64(seeded_session: Session) -> None:
    erf = seeded_session.get(ERF, 64)
    assert erf is not None
    assert erf.default_prob_model_id == 1


def test_seeded_session_has_sgt_variation_8(seeded_session: Session) -> None:
    variation = seeded_session.get(SGTVariation, 8)
    assert variation is not None
    assert variation.sgt_variation_name == "AWP_ODC_SGT GPU"


def test_seeded_session_has_velocity_models(seeded_session: Session) -> None:
    assert seeded_session.scalar(select(func.count()).select_from(VelocityModel)) == 1


def test_seeded_session_has_rup_var_scenario_10(seeded_session: Session) -> None:
    scenario = seeded_session.get(RuptureVariationScenario, 10)
    assert scenario is not None
    assert (scenario.erf_id, scenario.rup_var_scenario_name) == (36, "genslip-v5.5.2")


def test_seeded_session_has_erf_64_metadata(seeded_session: Session) -> None:
    meta = seeded_session.get(ERFMetadata, (64, "Rupture Surface Resolution", "0.2"))
    assert meta is not None
    assert meta.erf_attr_units == "km"


def test_seeded_session_has_site_region(seeded_session: Session) -> None:
    region = seeded_session.get(SiteRegion, (9000, 64, 20))
    assert region is not None
    assert region.max_lat == 34.1


def test_seeded_session_has_all_site_9000_regions(seeded_session: Session) -> None:
    count = seeded_session.scalar(select(func.count()).select_from(SiteRegion))
    assert count == 4


def test_seeded_session_has_test(seeded_session: Session) -> None:
    site = seeded_session.get(Site, 9000)
    assert site is not None
    assert site.cs_short_name == "TEST"
