"""SQLAlchemy models for the CyberShake MySQL database."""

from __future__ import annotations

import datetime as dt  # noqa: TC003 - needed at runtime by SQLAlchemy annotations
from typing import Any

from sqlalchemy import (
    BigInteger,
    Date,
    DateTime,
    Double,
    Enum,
    Float,
    ForeignKey,
    ForeignKeyConstraint,
    Integer,
    String,
    Text,
)
from sqlalchemy.dialects.mysql import BIGINT, INTEGER
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

# Unsigned on MySQL; plain INTEGER on SQLite. SQLite only auto-increments an
# ``INTEGER PRIMARY KEY``, so the BIGINT variant maps to INTEGER there too.
UInt = Integer().with_variant(INTEGER(unsigned=True), "mysql")
UBigInt = (
    BigInteger()
    .with_variant(BIGINT(unsigned=True), "mysql")
    .with_variant(Integer(), "sqlite")
)

_TABLE_ARGS: dict[str, Any] = {
    "mysql_engine": "InnoDB",
    "mysql_charset": "latin1",
}


class Base(DeclarativeBase):
    """Declarative base for all CyberShake tables."""

    __table_args__: Any = _TABLE_ARGS


# --------------------------------------------------------------------------
# Lookup tables
# --------------------------------------------------------------------------


class Study(Base):
    """A CyberShake study."""

    __tablename__ = "Studies"

    study_id: Mapped[int] = mapped_column(
        "Study_ID", UInt, primary_key=True, autoincrement=True
    )
    study_name: Mapped[str] = mapped_column("Study_Name", String(50))
    study_start_time: Mapped[dt.datetime | None] = mapped_column(
        "Study_Start_Time", DateTime
    )
    study_end_time: Mapped[dt.datetime | None] = mapped_column(
        "Study_End_Time", DateTime
    )


class TimeSpan(Base):
    """Time span used for probability calculations."""

    __tablename__ = "Time_Spans"

    time_span_id: Mapped[int] = mapped_column(
        "Time_Span_ID", UInt, primary_key=True, autoincrement=True
    )
    time_span_units: Mapped[str] = mapped_column(
        "Time_Span_Units", Enum("days", "years")
    )
    time_span_duration: Mapped[int] = mapped_column("Time_Span_Duration", UInt)


class ERFProbabilityModel(Base):
    """ERF probability model."""

    __tablename__ = "ERF_Probability_Models"

    prob_model_id: Mapped[int] = mapped_column(
        "Prob_Model_ID", UInt, primary_key=True, autoincrement=True
    )
    prob_model_name: Mapped[str] = mapped_column("Prob_Model_Name", String(50))
    prob_model_description: Mapped[str] = mapped_column(
        "Prob_Model_Description", String(200)
    )


class ERF(Base):
    """Earthquake rupture forecast."""

    __tablename__ = "ERF_IDs"

    erf_id: Mapped[int] = mapped_column(
        "ERF_ID", UInt, primary_key=True, autoincrement=True
    )
    erf_name: Mapped[str] = mapped_column("ERF_Name", String(50))
    erf_description: Mapped[str | None] = mapped_column("ERF_Description", String(150))
    default_prob_model_id: Mapped[int] = mapped_column(
        "Default_Prob_Model_ID",
        UInt,
        ForeignKey("ERF_Probability_Models.Prob_Model_ID"),
    )
    default_time_span_id: Mapped[int] = mapped_column(
        "Default_Time_Span_ID", UInt, ForeignKey("Time_Spans.Time_Span_ID")
    )


class ERFMetadata(Base):
    """ERF attributes (e.g. ``Rupture Surface Resolution``)."""

    __tablename__ = "ERF_Metadata"

    erf_id: Mapped[int] = mapped_column(
        "ERF_ID", UInt, ForeignKey("ERF_IDs.ERF_ID"), primary_key=True
    )
    erf_attr_name: Mapped[str] = mapped_column(
        "ERF_Attr_Name", String(50), primary_key=True
    )
    erf_attr_value: Mapped[str] = mapped_column(
        "ERF_Attr_Value", String(200), primary_key=True
    )
    erf_attr_type: Mapped[str | None] = mapped_column("ERF_Attr_Type", String(20))
    erf_attr_units: Mapped[str | None] = mapped_column("ERF_Attr_Units", String(20))


class AttenRel(Base):
    """Attenuation relationship."""

    __tablename__ = "Atten_Rels"

    ar_id: Mapped[int] = mapped_column(
        "AR_ID", UInt, primary_key=True, autoincrement=True
    )
    ar_name: Mapped[str] = mapped_column("AR_Name", String(50))
    ar_short_name: Mapped[str] = mapped_column("AR_Short_Name", String(30))


class AttenRelMetadata(Base):
    """Attenuation relationship attributes."""

    __tablename__ = "Atten_Rel_Metadata"

    ar_id: Mapped[int] = mapped_column(
        "AR_ID", UInt, ForeignKey("Atten_Rels.AR_ID"), primary_key=True
    )
    ar_attr_name: Mapped[str] = mapped_column(
        "AR_Attr_Name", String(50), primary_key=True
    )
    ar_attr_value: Mapped[str] = mapped_column(
        "AR_Attr_Value", String(100), primary_key=True
    )
    ar_attr_type: Mapped[str | None] = mapped_column("AR_Attr_Type", String(20))
    ar_attr_units: Mapped[str | None] = mapped_column("AR_Attr_Units", String(20))


class VelocityModel(Base):
    """Velocity model. IDs match ``VELOCITY_MODELS`` in ``constants``."""

    __tablename__ = "Velocity_Models"

    velocity_model_id: Mapped[int] = mapped_column(
        "Velocity_Model_ID", UInt, primary_key=True, autoincrement=True
    )
    velocity_model_name: Mapped[str] = mapped_column("Velocity_Model_Name", String(50))
    velocity_model_version: Mapped[str] = mapped_column(
        "Velocity_Model_Version", String(50)
    )


class VelocityModelMetadata(Base):
    """Velocity model attributes."""

    __tablename__ = "Velocity_Model_Metadata"

    velocity_model_id: Mapped[int] = mapped_column(
        "Velocity_Model_ID",
        UInt,
        ForeignKey("Velocity_Models.Velocity_Model_ID"),
        primary_key=True,
    )
    velocity_model_attr_name: Mapped[str] = mapped_column(
        "Velocity_Model_Attr_Name", String(50), primary_key=True
    )
    velocity_model_attr_value: Mapped[str] = mapped_column(
        "Velocity_Model_Attr_Value", String(50)
    )
    velocity_model_attr_type: Mapped[str] = mapped_column(
        "Velocity_Model_Attr_Type", String(20)
    )
    velocity_model_attr_units: Mapped[str | None] = mapped_column(
        "Velocity_Model_Attr_Units", String(20)
    )


class SGTVariation(Base):
    """SGT variation."""

    __tablename__ = "SGT_Variation_IDs"

    sgt_variation_id: Mapped[int] = mapped_column(
        "SGT_Variation_ID", UInt, primary_key=True, autoincrement=True
    )
    sgt_variation_name: Mapped[str] = mapped_column("SGT_Variation_Name", String(50))
    sgt_variation_description: Mapped[str | None] = mapped_column(
        "SGT_Variation_Description", String(150)
    )


class SGTVariationMetadata(Base):
    """SGT variation attributes."""

    __tablename__ = "SGT_Variation_Metadata"

    sgt_variation_id: Mapped[int] = mapped_column(
        "SGT_Variation_ID",
        UInt,
        ForeignKey("SGT_Variation_IDs.SGT_Variation_ID"),
        primary_key=True,
    )
    sgt_variation_attr_name: Mapped[str] = mapped_column(
        "SGT_Variation_Attr_Name", String(50), primary_key=True
    )
    sgt_variation_attr_value: Mapped[str] = mapped_column(
        "SGT_Variation_Attr_Value", String(50), primary_key=True
    )
    sgt_variation_attr_type: Mapped[str] = mapped_column(
        "SGT_Variation_Attr_Type", String(20)
    )
    sgt_variation_attr_units: Mapped[str | None] = mapped_column(
        "SGT_Variation_Attr_Units", String(20)
    )


class RuptureVariationScenario(Base):
    """Rupture variation scenario."""

    __tablename__ = "Rupture_Variation_Scenario_IDs"

    rup_var_scenario_id: Mapped[int] = mapped_column(
        "Rup_Var_Scenario_ID", UInt, primary_key=True, autoincrement=True
    )
    erf_id: Mapped[int] = mapped_column("ERF_ID", UInt, ForeignKey("ERF_IDs.ERF_ID"))
    rup_var_scenario_name: Mapped[str] = mapped_column(
        "Rup_Var_Scenario_Name", String(50)
    )
    rup_var_scenario_description: Mapped[str | None] = mapped_column(
        "Rup_Var_Scenario_Description", Text
    )


class RuptureVariationScenarioMetadata(Base):
    """Rupture variation scenario attributes."""

    __tablename__ = "Rupture_Variation_Scenario_Metadata"

    rup_var_scenario_id: Mapped[int] = mapped_column(
        "Rup_Var_Scenario_ID",
        UInt,
        ForeignKey("Rupture_Variation_Scenario_IDs.Rup_Var_Scenario_ID"),
        primary_key=True,
    )
    rup_var_scenario_attr_name: Mapped[str] = mapped_column(
        "Rup_Var_Scenario_Attr_Name", String(50), primary_key=True
    )
    rup_var_scenario_attr_value: Mapped[str] = mapped_column(
        "Rup_Var_Scenario_Attr_Value", String(50), primary_key=True
    )
    rup_var_scenario_attr_type: Mapped[str] = mapped_column(
        "Rup_Var_Scenario_Attr_Type", String(20)
    )
    rup_var_scenario_attr_units: Mapped[str | None] = mapped_column(
        "Rup_Var_Scenario_Attr_Units", String(20)
    )


class RuptureVariationProbabilityModifier(Base):
    """Rupture variation probability modifier."""

    __tablename__ = "Rupture_Variation_Probability_Modifier"

    rup_var_prob_mod_id: Mapped[int] = mapped_column(
        "Rup_Var_Prob_Mod_ID", UInt, primary_key=True, autoincrement=True
    )
    rup_var_prob_mod_name: Mapped[str] = mapped_column(
        "Rup_Var_Prob_Mod_Name", String(50)
    )
    rup_var_prob_mod_description: Mapped[str] = mapped_column(
        "Rup_Var_Prob_Mod_Description", String(500)
    )


class MeshVsitopMetadata(Base):
    """Mesh Vs-top method."""

    __tablename__ = "Mesh_Vsitop_Metadata"

    mesh_vsitop_id: Mapped[int] = mapped_column(
        "Mesh_Vsitop_ID", UInt, primary_key=True, autoincrement=True
    )
    mesh_vsitop_name: Mapped[str] = mapped_column("Mesh_Vsitop_Name", String(50))
    mesh_vsitop_description: Mapped[str] = mapped_column(
        "Mesh_Vsitop_Description", String(500)
    )


class VrefEffMetadata(Base):
    """Effective Vref method."""

    __tablename__ = "Vref_eff_Metadata"

    vref_eff_id: Mapped[int] = mapped_column(
        "Vref_eff_ID", UInt, primary_key=True, autoincrement=True
    )
    vref_eff_name: Mapped[str] = mapped_column("Vref_eff_Name", String(50))
    vref_eff_description: Mapped[str] = mapped_column(
        "Vref_eff_Description", String(500)
    )


class IMType(Base):
    """Intensity measure type."""

    __tablename__ = "IM_Types"

    im_type_id: Mapped[int] = mapped_column(
        "IM_Type_ID", UInt, primary_key=True, autoincrement=True
    )
    im_type_measure: Mapped[str] = mapped_column("IM_Type_Measure", String(50))
    im_type_value: Mapped[float | None] = mapped_column("IM_Type_Value", Double)
    units: Mapped[str] = mapped_column("Units", String(50))
    im_type_component: Mapped[str] = mapped_column("IM_Type_Component", String(50))


class SiteType(Base):
    """CyberShake site type."""

    __tablename__ = "CyberShake_Site_Types"

    cs_site_type_id: Mapped[int] = mapped_column(
        "CS_Site_Type_ID", UInt, primary_key=True, autoincrement=True
    )
    cs_site_type_name: Mapped[str] = mapped_column("CS_Site_Type_Name", String(50))
    cs_site_type_short_name: Mapped[str] = mapped_column(
        "CS_Site_Type_Short_Name", String(10)
    )


class Site(Base):
    """CyberShake site."""

    __tablename__ = "CyberShake_Sites"

    cs_site_id: Mapped[int] = mapped_column(
        "CS_Site_ID", UInt, primary_key=True, autoincrement=True
    )
    cs_site_name: Mapped[str] = mapped_column("CS_Site_Name", String(50))
    cs_short_name: Mapped[str] = mapped_column("CS_Short_Name", String(5), unique=True)
    cs_site_lat: Mapped[float] = mapped_column("CS_Site_Lat", Double)
    cs_site_lon: Mapped[float] = mapped_column("CS_Site_Lon", Double)
    cs_site_type_id: Mapped[int | None] = mapped_column(
        "CS_Site_Type_ID", UInt, ForeignKey("CyberShake_Site_Types.CS_Site_Type_ID")
    )


# --------------------------------------------------------------------------
# Ruptures
# --------------------------------------------------------------------------


class Rupture(Base):
    """Rupture in an ERF."""

    __tablename__ = "Ruptures"

    erf_id: Mapped[int] = mapped_column(
        "ERF_ID", UInt, ForeignKey("ERF_IDs.ERF_ID"), primary_key=True
    )
    source_id: Mapped[int] = mapped_column("Source_ID", UInt, primary_key=True)
    rupture_id: Mapped[int] = mapped_column("Rupture_ID", UInt, primary_key=True)
    source_name: Mapped[str] = mapped_column("Source_Name", String(100))
    source_type: Mapped[str | None] = mapped_column("Source_Type", String(20))
    mag: Mapped[float] = mapped_column("Mag", Double)
    prob: Mapped[float] = mapped_column("Prob", Double)
    grid_spacing: Mapped[float] = mapped_column("Grid_Spacing", Double)
    num_rows: Mapped[int] = mapped_column("Num_Rows", Integer)
    num_columns: Mapped[int] = mapped_column("Num_Columns", Integer)
    num_points: Mapped[int] = mapped_column("Num_Points", Integer)
    start_lat: Mapped[float] = mapped_column("Start_Lat", Double)
    start_lon: Mapped[float] = mapped_column("Start_Lon", Double)
    start_depth: Mapped[float] = mapped_column("Start_Depth", Double)
    end_lat: Mapped[float] = mapped_column("End_Lat", Double)
    end_lon: Mapped[float] = mapped_column("End_Lon", Double)
    end_depth: Mapped[float] = mapped_column("End_Depth", Double)


class Point(Base):
    """Point on a rupture surface."""

    __tablename__ = "Points"
    __table_args__ = (
        ForeignKeyConstraint(
            ["ERF_ID", "Source_ID", "Rupture_ID"],
            ["Ruptures.ERF_ID", "Ruptures.Source_ID", "Ruptures.Rupture_ID"],
        ),
        _TABLE_ARGS,
    )

    point_id: Mapped[int] = mapped_column(
        "Point_ID", UBigInt, primary_key=True, autoincrement=True
    )
    erf_id: Mapped[int] = mapped_column("ERF_ID", UInt, ForeignKey("ERF_IDs.ERF_ID"))
    source_id: Mapped[int] = mapped_column("Source_ID", UInt)
    rupture_id: Mapped[int] = mapped_column("Rupture_ID", UInt)
    lat: Mapped[float] = mapped_column("Lat", Double)
    lon: Mapped[float] = mapped_column("Lon", Double)
    depth: Mapped[float] = mapped_column("Depth", Double)
    rake: Mapped[float] = mapped_column("Rake", Double)
    dip: Mapped[float] = mapped_column("Dip", Double)
    strike: Mapped[float] = mapped_column("Strike", Double)


class RuptureVariation(Base):
    """Rupture variation."""

    __tablename__ = "Rupture_Variations"
    __table_args__ = (
        ForeignKeyConstraint(
            ["ERF_ID", "Source_ID", "Rupture_ID"],
            ["Ruptures.ERF_ID", "Ruptures.Source_ID", "Ruptures.Rupture_ID"],
        ),
        _TABLE_ARGS,
    )

    erf_id: Mapped[int] = mapped_column(
        "ERF_ID", UInt, ForeignKey("ERF_IDs.ERF_ID"), primary_key=True
    )
    source_id: Mapped[int] = mapped_column("Source_ID", UInt, primary_key=True)
    rupture_id: Mapped[int] = mapped_column("Rupture_ID", UInt, primary_key=True)
    rup_var_id: Mapped[int] = mapped_column("Rup_Var_ID", UBigInt, primary_key=True)
    rup_var_scenario_id: Mapped[int] = mapped_column(
        "Rup_Var_Scenario_ID",
        UInt,
        ForeignKey("Rupture_Variation_Scenario_IDs.Rup_Var_Scenario_ID"),
        primary_key=True,
    )
    rup_var_lfn: Mapped[str] = mapped_column("Rup_Var_LFN", String(50))
    hypocenter_lat: Mapped[float | None] = mapped_column("Hypocenter_Lat", Double)
    hypocenter_lon: Mapped[float | None] = mapped_column("Hypocenter_Lon", Double)
    hypocenter_depth: Mapped[float | None] = mapped_column("Hypocenter_Depth", Double)
    rvfrac: Mapped[float | None] = mapped_column(Double)


class RupVarSeed(Base):
    """Random seed for a rupture variation."""

    __tablename__ = "Rup_Var_Seeds"
    __table_args__ = (
        ForeignKeyConstraint(
            ["ERF_ID", "Source_ID", "Rupture_ID"],
            ["Ruptures.ERF_ID", "Ruptures.Source_ID", "Ruptures.Rupture_ID"],
        ),
        ForeignKeyConstraint(
            ["ERF_ID", "Source_ID", "Rupture_ID", "Rup_Var_ID", "Rup_Var_Scenario_ID"],
            [
                "Rupture_Variations.ERF_ID",
                "Rupture_Variations.Source_ID",
                "Rupture_Variations.Rupture_ID",
                "Rupture_Variations.Rup_Var_ID",
                "Rupture_Variations.Rup_Var_Scenario_ID",
            ],
        ),
        _TABLE_ARGS,
    )

    erf_id: Mapped[int] = mapped_column(
        "ERF_ID", UInt, ForeignKey("ERF_IDs.ERF_ID"), primary_key=True
    )
    rup_var_scenario_id: Mapped[int] = mapped_column(
        "Rup_Var_Scenario_ID",
        UInt,
        ForeignKey("Rupture_Variation_Scenario_IDs.Rup_Var_Scenario_ID"),
        primary_key=True,
    )
    source_id: Mapped[int] = mapped_column("Source_ID", UInt, primary_key=True)
    rupture_id: Mapped[int] = mapped_column("Rupture_ID", UInt, primary_key=True)
    rup_var_id: Mapped[int] = mapped_column("Rup_Var_ID", UBigInt, primary_key=True)
    rup_var_seed: Mapped[int] = mapped_column("Rup_Var_Seed", UInt)


class SiteRegion(Base):
    """Bounding region of ruptures within a cutoff distance of a site."""

    __tablename__ = "CyberShake_Site_Regions"

    cs_site_id: Mapped[int] = mapped_column(
        "CS_Site_ID", UInt, ForeignKey("CyberShake_Sites.CS_Site_ID"), primary_key=True
    )
    erf_id: Mapped[int] = mapped_column(
        "ERF_ID", UInt, ForeignKey("ERF_IDs.ERF_ID"), primary_key=True
    )
    cutoff_dist: Mapped[float] = mapped_column("Cutoff_Dist", Double, primary_key=True)
    max_lat: Mapped[float] = mapped_column("Max_Lat", Double)
    max_lat_source_id: Mapped[int] = mapped_column("Max_Lat_Source_ID", UInt)
    max_lat_rupture_id: Mapped[int] = mapped_column("Max_Lat_Rupture_ID", UInt)
    max_lon: Mapped[float] = mapped_column("Max_Lon", Double)
    max_lon_source_id: Mapped[int] = mapped_column("Max_Lon_Source_ID", UInt)
    max_lon_rupture_id: Mapped[int] = mapped_column("Max_Lon_Rupture_ID", UInt)
    min_lat: Mapped[float] = mapped_column("Min_Lat", Double)
    min_lat_source_id: Mapped[int] = mapped_column("Min_Lat_Source_ID", UInt)
    min_lat_rupture_id: Mapped[int] = mapped_column("Min_Lat_Rupture_ID", UInt)
    min_lon: Mapped[float] = mapped_column("Min_Lon", Double)
    min_lon_source_id: Mapped[int] = mapped_column("Min_Lon_Source_ID", UInt)
    min_lon_rupture_id: Mapped[int] = mapped_column("Min_Lon_Rupture_ID", UInt)


class SiteRupture(Base):
    """Rupture within the cutoff distance of a site."""

    __tablename__ = "CyberShake_Site_Ruptures"
    __table_args__ = (
        ForeignKeyConstraint(
            ["ERF_ID", "Source_ID", "Rupture_ID"],
            ["Ruptures.ERF_ID", "Ruptures.Source_ID", "Ruptures.Rupture_ID"],
        ),
        ForeignKeyConstraint(
            ["CS_Site_ID", "ERF_ID", "Cutoff_Dist"],
            [
                "CyberShake_Site_Regions.CS_Site_ID",
                "CyberShake_Site_Regions.ERF_ID",
                "CyberShake_Site_Regions.Cutoff_Dist",
            ],
        ),
        _TABLE_ARGS,
    )

    cs_site_id: Mapped[int] = mapped_column(
        "CS_Site_ID", UInt, ForeignKey("CyberShake_Sites.CS_Site_ID"), primary_key=True
    )
    erf_id: Mapped[int] = mapped_column(
        "ERF_ID", UInt, ForeignKey("ERF_IDs.ERF_ID"), primary_key=True
    )
    source_id: Mapped[int] = mapped_column("Source_ID", UInt, primary_key=True)
    rupture_id: Mapped[int] = mapped_column("Rupture_ID", UInt, primary_key=True)
    cutoff_dist: Mapped[float] = mapped_column("Cutoff_Dist", Double, primary_key=True)
    site_rupture_dist: Mapped[float | None] = mapped_column("Site_Rupture_Dist", Double)


# --------------------------------------------------------------------------
# Runs
# --------------------------------------------------------------------------


class Run(Base):
    """A CyberShake run (managed by ``cybershake.runmanager``).

    ``ERF_ID``, ``SGT_Variation_ID``, ``Rup_Var_Scenario_ID`` and
    ``Velocity_Model_ID`` have no foreign keys in the production DB (indexed
    only), so they are not declared as such here either.
    """

    __tablename__ = "CyberShake_Runs"

    run_id: Mapped[int] = mapped_column(
        "Run_ID", UInt, primary_key=True, autoincrement=True
    )
    site_id: Mapped[int] = mapped_column(
        "Site_ID", UInt, ForeignKey("CyberShake_Sites.CS_Site_ID"), index=True
    )
    erf_id: Mapped[int] = mapped_column("ERF_ID", UInt, index=True)
    sgt_variation_id: Mapped[int] = mapped_column("SGT_Variation_ID", UInt, index=True)
    rup_var_scenario_id: Mapped[int] = mapped_column(
        "Rup_Var_Scenario_ID", UInt, index=True
    )
    sgt_host: Mapped[str | None] = mapped_column("SGT_Host", String(100))
    pp_host: Mapped[str | None] = mapped_column("PP_Host", String(100))
    sgt_time: Mapped[dt.datetime | None] = mapped_column("SGT_Time", DateTime)
    pp_time: Mapped[dt.datetime | None] = mapped_column("PP_Time", DateTime)
    status: Mapped[str] = mapped_column("Status", String(32))
    status_time: Mapped[dt.datetime] = mapped_column("Status_Time", DateTime)
    last_user: Mapped[str] = mapped_column("Last_User", String(32))
    comment: Mapped[str | None] = mapped_column("Comment", String(128))
    job_id: Mapped[str | None] = mapped_column("Job_ID", String(64))
    submit_dir: Mapped[str | None] = mapped_column("Submit_Dir", String(256))
    notify_user: Mapped[str | None] = mapped_column("Notify_User", String(128))
    velocity_model_id: Mapped[int] = mapped_column(
        "Velocity_Model_ID", UInt, index=True
    )
    max_frequency: Mapped[float | None] = mapped_column("Max_Frequency", Double)
    low_frequency_cutoff: Mapped[float | None] = mapped_column(
        "Low_Frequency_Cutoff", Double
    )
    model_vs30: Mapped[float | None] = mapped_column("Model_Vs30", Double)
    mesh_vsitop_id: Mapped[int | None] = mapped_column(
        "Mesh_Vsitop_ID", UInt, ForeignKey("Mesh_Vsitop_Metadata.Mesh_Vsitop_ID")
    )
    mesh_vsitop: Mapped[float | None] = mapped_column("Mesh_Vsitop", Double)
    vs30_source: Mapped[str | None] = mapped_column("Vs30_Source", String(100))
    wills_vs30: Mapped[float | None] = mapped_column("Wills_Vs30", Double)
    z2_5: Mapped[float | None] = mapped_column("Z2_5", Double)
    z1_0: Mapped[float | None] = mapped_column("Z1_0", Double)
    minimum_vs: Mapped[float | None] = mapped_column("Minimum_Vs", Double)
    vref_eff_id: Mapped[int | None] = mapped_column(
        "Vref_eff_ID", UInt, ForeignKey("Vref_eff_Metadata.Vref_eff_ID")
    )
    vref_eff: Mapped[float | None] = mapped_column("Vref_eff", Double)
    target_vs30: Mapped[float | None] = mapped_column("Target_Vs30", Double)
    sgt_source_filter_frequency: Mapped[float | None] = mapped_column(
        "SGT_Source_Filter_Frequency", Double
    )
    study_id: Mapped[int | None] = mapped_column(
        "Study_ID", UInt, ForeignKey("Studies.Study_ID")
    )


# --------------------------------------------------------------------------
# Results
# --------------------------------------------------------------------------


class HazardDataset(Base):
    """Hazard dataset."""

    __tablename__ = "Hazard_Datasets"

    hazard_dataset_id: Mapped[int] = mapped_column(
        "Hazard_Dataset_ID", UInt, primary_key=True, autoincrement=True
    )
    erf_id: Mapped[int] = mapped_column("ERF_ID", UInt, ForeignKey("ERF_IDs.ERF_ID"))
    rup_var_scenario_id: Mapped[int] = mapped_column(
        "Rup_Var_Scenario_ID",
        UInt,
        ForeignKey("Rupture_Variation_Scenario_IDs.Rup_Var_Scenario_ID"),
    )
    sgt_variation_id: Mapped[int] = mapped_column(
        "SGT_Variation_ID", UInt, ForeignKey("SGT_Variation_IDs.SGT_Variation_ID")
    )
    velocity_model_id: Mapped[int] = mapped_column(
        "Velocity_Model_ID", UInt, ForeignKey("Velocity_Models.Velocity_Model_ID")
    )
    prob_model_id: Mapped[int] = mapped_column(
        "Prob_Model_ID", UInt, ForeignKey("ERF_Probability_Models.Prob_Model_ID")
    )
    time_span_id: Mapped[int] = mapped_column(
        "Time_Span_ID", UInt, ForeignKey("Time_Spans.Time_Span_ID")
    )
    time_span_start_date: Mapped[dt.date | None] = mapped_column(
        "Time_Span_Start_Date", Date
    )
    max_frequency: Mapped[float | None] = mapped_column("Max_Frequency", Double)
    low_frequency_cutoff: Mapped[float | None] = mapped_column(
        "Low_Frequency_Cutoff", Double
    )
    rup_var_prob_mod_id: Mapped[int | None] = mapped_column(
        "Rup_Var_Prob_Mod_ID",
        UInt,
        ForeignKey("Rupture_Variation_Probability_Modifier.Rup_Var_Prob_Mod_ID"),
    )
    study_id: Mapped[int | None] = mapped_column(
        "Study_ID", UInt, ForeignKey("Studies.Study_ID")
    )
    background_seis_ar_id: Mapped[int | None] = mapped_column(
        "Background_Seis_AR_ID", UInt, ForeignKey("Atten_Rels.AR_ID")
    )


class HazardCurve(Base):
    """Hazard curve for a run."""

    __tablename__ = "Hazard_Curves"

    hazard_curve_id: Mapped[int] = mapped_column(
        "Hazard_Curve_ID", UInt, primary_key=True, autoincrement=True
    )
    run_id: Mapped[int] = mapped_column(
        "Run_ID", UInt, ForeignKey("CyberShake_Runs.Run_ID")
    )
    curve_date: Mapped[dt.date | None] = mapped_column("Curve_Date", Date)
    im_type_id: Mapped[int] = mapped_column(
        "IM_Type_ID", UInt, ForeignKey("IM_Types.IM_Type_ID")
    )
    hazard_dataset_id: Mapped[int | None] = mapped_column(
        "Hazard_Dataset_ID", UInt, ForeignKey("Hazard_Datasets.Hazard_Dataset_ID")
    )


class HazardCurvePoint(Base):
    """Point on a hazard curve."""

    __tablename__ = "Hazard_Curve_Points"

    hazard_curve_point_id: Mapped[int] = mapped_column(
        "Hazard_Curve_Point_ID", UInt, primary_key=True, autoincrement=True
    )
    hazard_curve_id: Mapped[int] = mapped_column(
        "Hazard_Curve_ID", UInt, ForeignKey("Hazard_Curves.Hazard_Curve_ID")
    )
    x_value: Mapped[float] = mapped_column("X_Value", Float)
    y_value: Mapped[float] = mapped_column("Y_Value", Float)


class PeakAmplitude(Base):
    """Intensity measure value for one rupture variation of a run."""

    __tablename__ = "PeakAmplitudes"

    run_id: Mapped[int] = mapped_column(
        "Run_ID", UInt, ForeignKey("CyberShake_Runs.Run_ID"), primary_key=True
    )
    source_id: Mapped[int] = mapped_column("Source_ID", UInt, primary_key=True)
    rupture_id: Mapped[int] = mapped_column("Rupture_ID", UInt, primary_key=True)
    rup_var_id: Mapped[int] = mapped_column("Rup_Var_ID", UBigInt, primary_key=True)
    im_type_id: Mapped[int] = mapped_column(
        "IM_Type_ID",
        UInt,
        ForeignKey("IM_Types.IM_Type_ID"),
        primary_key=True,
        index=True,
    )
    im_value: Mapped[float] = mapped_column("IM_Value", Double)


class ARHazardDataset(Base):
    """Attenuation-relationship hazard dataset."""

    __tablename__ = "AR_Hazard_Datasets"

    ar_hazard_dataset_id: Mapped[int] = mapped_column(
        "AR_Hazard_Dataset_ID", UInt, primary_key=True, autoincrement=True
    )
    ar_id: Mapped[int] = mapped_column("AR_ID", UInt, ForeignKey("Atten_Rels.AR_ID"))
    erf_id: Mapped[int] = mapped_column("ERF_ID", UInt, ForeignKey("ERF_IDs.ERF_ID"))
    prob_model_id: Mapped[int] = mapped_column(
        "Prob_Model_ID", UInt, ForeignKey("ERF_Probability_Models.Prob_Model_ID")
    )
    time_span_id: Mapped[int] = mapped_column(
        "Time_Span_ID", UInt, ForeignKey("Time_Spans.Time_Span_ID")
    )
    time_span_start_date: Mapped[dt.date | None] = mapped_column(
        "Time_Span_Start_Date", Date
    )
    min_lat: Mapped[float | None] = mapped_column("Min_Lat", Double)
    max_lat: Mapped[float | None] = mapped_column("Max_Lat", Double)
    min_lon: Mapped[float | None] = mapped_column("Min_Lon", Double)
    max_lon: Mapped[float | None] = mapped_column("Max_Lon", Double)
    grid_spacing: Mapped[float | None] = mapped_column("Grid_Spacing", Double)
    # Indexed but not a foreign key in the production DB.
    velocity_model_id: Mapped[int | None] = mapped_column(
        "Velocity_Model_ID", UInt, index=True
    )


class ARHazardCurve(Base):
    """Attenuation-relationship hazard curve."""

    __tablename__ = "AR_Hazard_Curves"

    ar_hazard_curve_id: Mapped[int] = mapped_column(
        "AR_Hazard_Curve_ID", UInt, primary_key=True, autoincrement=True
    )
    lat: Mapped[float] = mapped_column("Lat", Double)
    lon: Mapped[float] = mapped_column("Lon", Double)
    curve_date: Mapped[dt.date | None] = mapped_column("Curve_Date", Date)
    ar_hazard_dataset_id: Mapped[int] = mapped_column(
        "AR_Hazard_Dataset_ID",
        UInt,
        ForeignKey("AR_Hazard_Datasets.AR_Hazard_Dataset_ID"),
    )
    im_type_id: Mapped[int] = mapped_column(
        "IM_Type_ID", UInt, ForeignKey("IM_Types.IM_Type_ID")
    )


class ARHazardCurvePoint(Base):
    """Point on an attenuation-relationship hazard curve."""

    __tablename__ = "AR_Hazard_Curve_Points"

    ar_hazard_curve_point_id: Mapped[int] = mapped_column(
        "AR_Hazard_Curve_Point_ID", UInt, primary_key=True, autoincrement=True
    )
    ar_hazard_curve_id: Mapped[int] = mapped_column(
        "AR_Hazard_Curve_ID", UInt, ForeignKey("AR_Hazard_Curves.AR_Hazard_Curve_ID")
    )
    x_value: Mapped[float] = mapped_column("X_Value", Float)
    y_value: Mapped[float] = mapped_column("Y_Value", Float)
