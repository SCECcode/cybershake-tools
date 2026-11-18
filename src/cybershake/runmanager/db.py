"""SQLAlchemy queries used by the CyberShake migration."""

from __future__ import annotations

import datetime as dt
import os
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Literal, NamedTuple, TypeAlias

from sqlalchemy import URL, and_, create_engine, exists, false, func, or_, select
from sqlalchemy.orm import Session

from cybershake.constants import (
    DB_CREDENTIALS_ENV,
    DB_HOST_ENV,
    DEFAULT_DB_CREDENTIALS,
    DEFAULT_DB_NAME,
    DEFAULT_DB_PORT,
    DEFAULT_DB_USER,
)
from cybershake.exc import RunManagerError
from cybershake.runmanager.schema import (
    ERF,
    HazardCurve,
    IMType,
    PeakAmplitude,
    Run,
    Rupture,
    RuptureVariation,
    RuptureVariationScenario,
    RupVarSeed,
    SGTVariation,
    Site,
    SiteRegion,
    SiteRupture,
    VelocityModel,
)

if TYPE_CHECKING:
    from collections.abc import Collection, Generator, Mapping

    from sqlalchemy.engine import Engine
    from sqlalchemy.orm import Mapper
    from sqlalchemy.orm.attributes import InstrumentedAttribute
    from sqlalchemy.sql.elements import ColumnElement

    from cybershake.config import Config

ParameterKind: TypeAlias = Literal[
    "erf", "sgt_variation", "rupture_variation", "velocity_model"
]
"""Name of a run parameter backed by a lookup table."""

RunOrder: TypeAlias = Literal["run_id", "status_time", "sgt_time"]
"""Supported ordering for run searches."""

RunFieldValue: TypeAlias = str | int | float | dt.datetime | None
"""Value accepted by a run-table update."""

SourceRupture: TypeAlias = tuple[int, int]
"""Source and rupture identifier pair."""


class VelocityParameters(NamedTuple):
    """Velocity values stored with a run.

    Attributes:
        model_vs30 (float | None): Velocity-model Vs30 in metres per second.
        mesh_vsitop (float | None): Vs value at the top of the simulation mesh.
        z1_0 (float | None): Depth to the 1.0 km/s shear-wave horizon.
        z2_5 (float | None): Depth to the 2.5 km/s shear-wave horizon.
        target_vs30 (float | None): Target site-response Vs30.
        vs30_source (str | None): Source used for ``target_vs30``.
    """

    model_vs30: float | None
    mesh_vsitop: float | None
    z1_0: float | None
    z2_5: float | None
    target_vs30: float | None
    vs30_source: str | None


class RuptureSummary(NamedTuple):
    """Rupture dimensions and variation count used by workflow generators.

    Attributes:
        source_id (int): ERF source identifier.
        rupture_id (int): Rupture identifier within the source.
        num_points (int): Number of points on the rupture surface.
        num_rows (int): Number of rows on the rupture surface.
        num_columns (int): Number of columns on the rupture surface.
        magnitude (float): Rupture magnitude.
        variation_count (int): Number of matching rupture variations.
    """

    source_id: int
    rupture_id: int
    num_points: int
    num_rows: int
    num_columns: int
    magnitude: float
    variation_count: int


class RuptureVariationSeed(NamedTuple):
    """Rupture variation data paired with its stochastic seed.

    Attributes:
        source_id (int): ERF source identifier.
        rupture_id (int): Rupture identifier within the source.
        rup_var_id (int): Rupture variation identifier.
        lfn (str): Logical file name containing the variation.
        rvfrac (float | None): Relative rupture-variation fraction.
        seed (int): Stochastic random seed.
    """

    source_id: int
    rupture_id: int
    rup_var_id: int
    lfn: str
    rvfrac: float | None
    seed: int


class RunStatistics(NamedTuple):
    """Persisted output statistics for a run.

    Attributes:
        peak_amplitude_count (int): Number of stored peak-amplitude rows.
        intensity_measures (tuple[IMType, ...]): Intensity measures with hazard
            curves, ordered by numeric value.
    """

    peak_amplitude_count: int
    intensity_measures: tuple[IMType, ...]


def _source_rupture_condition(
    source_column: InstrumentedAttribute[int],
    rupture_column: InstrumentedAttribute[int],
    source_ruptures: Collection[SourceRupture] | None,
) -> ColumnElement[bool] | None:
    """Build a portable predicate for selected source/rupture pairs.

    Args:
        source_column (InstrumentedAttribute[int]): Source ID column.
        rupture_column (InstrumentedAttribute[int]): Rupture ID column.
        source_ruptures (Collection[SourceRupture] | None): Pairs to retain.
            ``None`` means no filter; an empty collection matches no rows.

    Returns:
        ColumnElement[bool] | None: SQL predicate, or ``None`` for no filter.
    """
    if source_ruptures is None:
        return None
    if not source_ruptures:
        return false()
    return or_(
        *(
            and_(source_column == source_id, rupture_column == rupture_id)
            for source_id, rupture_id in source_ruptures
        )
    )


class SQLAlchemyRunDB:
    """Execute CyberShake run-manager and generator queries with SQLAlchemy."""

    def __init__(self, engine: Engine) -> None:
        """Initialize the repository.

        Args:
            engine (Engine): SQLAlchemy engine connected to the run database.
        """
        self._engine = engine
        self._transaction_session: Session | None = None

    @contextmanager
    def transaction(self) -> Generator[SQLAlchemyRunDB]:
        """Create a repository whose writes share one atomic transaction.

        Yields:
            SQLAlchemyRunDB: Repository bound to the transaction.

        Raises:
            RunManagerError: If called from an already active transaction.
            Exception: Re-raised after rolling back all writes.
        """
        if self._transaction_session is not None:
            raise RunManagerError("A database transaction is already active")
        with Session(self._engine, expire_on_commit=False) as session:
            transactional = SQLAlchemyRunDB(self._engine)
            transactional._transaction_session = session
            with session.begin():
                yield transactional

    @contextmanager
    def _session(self, *, write: bool = False) -> Generator[Session]:
        """Yield the active session or create a short-lived one.

        Args:
            write (bool, optional): Commit the short-lived session on success.
                Defaults to ``False``. An enclosing transaction owns its commit.

        Yields:
            Session: Session for one repository operation.
        """
        if self._transaction_session is not None:
            yield self._transaction_session
            return
        with Session(self._engine, expire_on_commit=False) as session:
            if write:
                with session.begin():
                    yield session
            else:
                yield session

    @staticmethod
    def _parameter_column(kind: ParameterKind) -> InstrumentedAttribute[int]:
        """Return the ID column for a named run parameter.

        Args:
            kind (ParameterKind): Lookup-table parameter name.

        Returns:
            InstrumentedAttribute[int]: SQLAlchemy ID column.

        Raises:
            ValueError: If ``kind`` is unsupported.
        """
        if kind == "erf":
            return ERF.erf_id
        if kind == "sgt_variation":
            return SGTVariation.sgt_variation_id
        if kind == "rupture_variation":
            return RuptureVariationScenario.rup_var_scenario_id
        if kind == "velocity_model":
            return VelocityModel.velocity_model_id
        raise ValueError(f"Unknown run parameter kind: {kind}")

    @staticmethod
    def _run_values(run: Run) -> dict[str, RunFieldValue]:
        """Copy mapped values from a run without its primary key.

        Args:
            run (Run): Source run.

        Returns:
            dict[str, RunFieldValue]: Values suitable for constructing a clone.
        """
        mapper: Mapper[Run] = Run.__mapper__
        return {
            attribute.key: getattr(run, attribute.key)
            for attribute in mapper.column_attrs
            if attribute.key != "run_id"
        }

    @staticmethod
    def _validate_run_fields(
        fields: Mapping[str, RunFieldValue],
    ) -> dict[str, RunFieldValue]:
        """Validate and copy run fields accepted by updates and clones.

        Args:
            fields (Mapping[str, RunFieldValue]): Python model attributes and
                values to persist.

        Returns:
            dict[str, RunFieldValue]: Validated mutable copy of ``fields``.

        Raises:
            ValueError: If a field is not mapped or tries to replace ``run_id``.
        """
        mapped_fields = {attribute.key for attribute in Run.__mapper__.column_attrs}
        invalid = set(fields).difference(mapped_fields)
        if "run_id" in fields:
            invalid.add("run_id")
        if invalid:
            names = ", ".join(sorted(invalid))
            raise ValueError(f"Unsupported run field(s): {names}")
        return dict(fields)

    # ------------------------------------------------------------------
    # Lookup-table and site queries
    # ------------------------------------------------------------------

    def get_site(self, short_name: str) -> Site | None:
        """Find a site by short name.

        Args:
            short_name (str): Case-sensitive CyberShake site short name.

        Returns:
            Site | None: Matching site, or ``None`` when absent.
        """
        with self._session() as session:
            return session.scalar(select(Site).where(Site.cs_short_name == short_name))

    def get_site_by_id(self, site_id: int) -> Site | None:
        """Find a site by ID.

        Args:
            site_id (int): CyberShake site ID.

        Returns:
            Site | None: Matching site, or ``None`` when absent.
        """
        with self._session() as session:
            return session.get(Site, site_id)

    def get_site_regions(self, site_id: int, erf_id: int) -> tuple[SiteRegion, ...]:
        """Return all cutoff regions for a site and ERF.

        Args:
            site_id (int): CyberShake site ID.
            erf_id (int): Earthquake rupture forecast ID.

        Returns:
            tuple[SiteRegion, ...]: Regions ordered by cutoff distance.
        """
        statement = (
            select(SiteRegion)
            .where(SiteRegion.cs_site_id == site_id, SiteRegion.erf_id == erf_id)
            .order_by(SiteRegion.cutoff_dist)
        )
        with self._session() as session:
            return tuple(session.scalars(statement))

    def parameter_exists(self, kind: ParameterKind, parameter_id: int) -> bool:
        """Check whether a run parameter ID is valid.

        Args:
            kind (ParameterKind): Lookup-table parameter name.
            parameter_id (int): ID to test.

        Returns:
            bool: ``True`` when the lookup-table row exists.

        Raises:
            ValueError: If ``kind`` is unsupported.
        """
        column = self._parameter_column(kind)
        statement = select(exists().where(column == parameter_id))
        with self._session() as session:
            return bool(session.scalar(statement))

    # ------------------------------------------------------------------
    # Run queries
    # ------------------------------------------------------------------

    def get_run(self, run_id: int, *, for_update: bool = False) -> Run | None:
        """Find a run by ID.

        Args:
            run_id (int): CyberShake run ID.
            for_update (bool, optional): Lock the row until the active transaction
                ends. Defaults to ``False``.

        Returns:
            Run | None: Matching run, or ``None`` when absent.
        """
        statement = select(Run).where(Run.run_id == run_id)
        if for_update:
            statement = statement.with_for_update()
        with self._session() as session:
            return session.scalar(statement)

    def find_runs(
        self,
        *,
        site: str | None = None,
        site_id: int | None = None,
        erf_id: int | None = None,
        sgt_variation_id: int | None = None,
        rupture_variation_id: int | None = None,
        velocity_model_id: int | None = None,
        max_frequency: float | None = None,
        low_frequency_cutoff: float | None = None,
        source_frequency: float | None = None,
        states: Collection[str] | None = None,
        order_by: RunOrder = "run_id",
        descending: bool = False,
        for_update: bool = False,
    ) -> tuple[Run, ...]:
        """Find runs matching exact lifecycle parameters.

        Args:
            site (str | None, optional): Site short name. Defaults to ``None``.
            site_id (int | None, optional): Site ID. Defaults to ``None``.
            erf_id (int | None, optional): ERF ID. Defaults to ``None``.
            sgt_variation_id (int | None, optional): SGT variation ID. Defaults to
                ``None``.
            rupture_variation_id (int | None, optional): Rupture variation
                scenario ID. Defaults to ``None``.
            velocity_model_id (int | None, optional): Velocity model ID. Defaults
                to ``None``.
            max_frequency (float | None, optional): Exact maximum frequency.
                Defaults to ``None``.
            low_frequency_cutoff (float | None, optional): Exact low-frequency
                cutoff. Defaults to ``None``.
            source_frequency (float | None, optional): Exact SGT source-filter
                frequency. Defaults to ``None``.
            states (Collection[str] | None, optional): Accepted statuses. Defaults
                to ``None``. An empty collection matches no runs.
            order_by (RunOrder, optional): ``run_id``, ``status_time``, or
                ``sgt_time``. Defaults to ``run_id``.
            descending (bool, optional): Reverse the primary order. Defaults to
                ``False``.
            for_update (bool, optional): Lock rows until the active transaction
                ends. Defaults to ``False``.

        Returns:
            tuple[Run, ...]: Matching runs in the requested stable order.
        """
        statement = select(Run)
        if site is not None:
            statement = statement.join(Site, Run.site_id == Site.cs_site_id).where(
                Site.cs_short_name == site
            )
        filters = (
            (Run.site_id, site_id),
            (Run.erf_id, erf_id),
            (Run.sgt_variation_id, sgt_variation_id),
            (Run.rup_var_scenario_id, rupture_variation_id),
            (Run.velocity_model_id, velocity_model_id),
            (Run.max_frequency, max_frequency),
            (Run.low_frequency_cutoff, low_frequency_cutoff),
            (Run.sgt_source_filter_frequency, source_frequency),
        )
        for column, value in filters:
            if value is not None:
                statement = statement.where(column == value)
        if states is not None:
            if not states:
                return ()
            statement = statement.where(Run.status.in_(states))

        if order_by == "run_id":
            statement = statement.order_by(
                Run.run_id.desc() if descending else Run.run_id.asc()
            )
        elif order_by == "status_time":
            statement = statement.order_by(
                Run.status_time.desc() if descending else Run.status_time.asc(),
                Run.run_id.desc() if descending else Run.run_id.asc(),
            )
        else:
            statement = statement.order_by(
                Run.sgt_time.desc() if descending else Run.sgt_time.asc(),
                Run.run_id.desc() if descending else Run.run_id.asc(),
            )
        if for_update:
            statement = statement.with_for_update()
        with self._session() as session:
            return tuple(session.scalars(statement))

    def find_preferred_run(
        self,
        *,
        site: str,
        erf_id: int,
        sgt_variation_id: int,
        rupture_variation_id: int,
        velocity_model_id: int,
        low_frequency_cutoff: float,
        max_frequency: float,
        source_frequency: float,
        states: Collection[str],
        for_update: bool = False,
    ) -> Run | None:
        """Find the newest SGT among otherwise identical runs.

        Args:
            site (str): Site short name.
            erf_id (int): ERF ID.
            sgt_variation_id (int): SGT variation ID.
            rupture_variation_id (int): Rupture variation scenario ID.
            velocity_model_id (int): Velocity model ID.
            low_frequency_cutoff (float): Low-frequency cutoff in Hz.
            max_frequency (float): Maximum frequency in Hz.
            source_frequency (float): SGT source-filter frequency in Hz.
            states (Collection[str]): Accepted statuses.
            for_update (bool, optional): Lock matches until the active transaction
                ends. Defaults to ``False``.

        Returns:
            Run | None: Preferred run, or ``None`` when none match.
        """
        runs = self.find_runs(
            site=site,
            erf_id=erf_id,
            sgt_variation_id=sgt_variation_id,
            rupture_variation_id=rupture_variation_id,
            velocity_model_id=velocity_model_id,
            low_frequency_cutoff=low_frequency_cutoff,
            max_frequency=max_frequency,
            source_frequency=source_frequency,
            states=states,
            order_by="sgt_time",
            descending=True,
            for_update=for_update,
        )
        return runs[0] if runs else None

    # ------------------------------------------------------------------
    # Rupture and workflow-generator queries
    # ------------------------------------------------------------------

    def get_ruptures_for_site(
        self,
        site: str,
        *,
        erf_id: int,
        cutoff_distance: float | None = None,
        source_ruptures: Collection[SourceRupture] | None = None,
    ) -> tuple[Rupture, ...]:
        """Return ruptures selected for a site's cutoff region.

        Args:
            site (str): Site short name.
            erf_id (int): ERF ID.
            cutoff_distance (float | None, optional): Rupture cutoff distance in
                kilometres. Defaults to ``None``, retaining all site-rupture rows
                for the ERF as the legacy PP query does.
            source_ruptures (Collection[SourceRupture] | None, optional): Explicit
                pairs to retain. Defaults to ``None``.

        Returns:
            tuple[Rupture, ...]: Ruptures ordered by descending surface size,
            then source and rupture ID.
        """
        statement = (
            select(Rupture)
            .join(
                SiteRupture,
                and_(
                    SiteRupture.erf_id == Rupture.erf_id,
                    SiteRupture.source_id == Rupture.source_id,
                    SiteRupture.rupture_id == Rupture.rupture_id,
                ),
            )
            .join(Site, Site.cs_site_id == SiteRupture.cs_site_id)
            .where(
                Site.cs_short_name == site,
                SiteRupture.erf_id == erf_id,
            )
        )
        if cutoff_distance is not None:
            statement = statement.where(SiteRupture.cutoff_dist == cutoff_distance)
        selected = _source_rupture_condition(
            Rupture.source_id, Rupture.rupture_id, source_ruptures
        )
        if selected is not None:
            statement = statement.where(selected)
        statement = statement.order_by(
            Rupture.num_points.desc(), Rupture.source_id, Rupture.rupture_id
        )
        with self._session() as session:
            return tuple(session.scalars(statement))

    def get_rupture_summaries_for_site(
        self,
        site: str,
        *,
        erf_id: int,
        rupture_variation_id: int,
        cutoff_distance: float | None = None,
        source_ruptures: Collection[SourceRupture] | None = None,
    ) -> tuple[RuptureSummary, ...]:
        """Return generator metadata and variation counts for site ruptures.

        Args:
            site (str): Site short name.
            erf_id (int): ERF ID.
            rupture_variation_id (int): Rupture variation scenario ID.
            cutoff_distance (float | None, optional): Rupture cutoff distance in
                kilometres. Defaults to ``None``, retaining all site-rupture rows
                as the legacy stochastic summary query does.
            source_ruptures (Collection[SourceRupture] | None, optional): Explicit
                pairs to retain. Defaults to ``None``.

        Returns:
            tuple[RuptureSummary, ...]: Summaries ordered by descending rupture
            surface size, then source and rupture ID.
        """
        statement = (
            select(
                Rupture.source_id,
                Rupture.rupture_id,
                Rupture.num_points,
                Rupture.num_rows,
                Rupture.num_columns,
                Rupture.mag,
                func.count(RuptureVariation.rup_var_id),
            )
            .join(
                SiteRupture,
                and_(
                    SiteRupture.erf_id == Rupture.erf_id,
                    SiteRupture.source_id == Rupture.source_id,
                    SiteRupture.rupture_id == Rupture.rupture_id,
                ),
            )
            .join(Site, Site.cs_site_id == SiteRupture.cs_site_id)
            .join(
                RuptureVariation,
                and_(
                    RuptureVariation.erf_id == Rupture.erf_id,
                    RuptureVariation.source_id == Rupture.source_id,
                    RuptureVariation.rupture_id == Rupture.rupture_id,
                ),
            )
            .where(
                Site.cs_short_name == site,
                SiteRupture.erf_id == erf_id,
                RuptureVariation.rup_var_scenario_id == rupture_variation_id,
            )
        )
        if cutoff_distance is not None:
            statement = statement.where(SiteRupture.cutoff_dist == cutoff_distance)
        selected = _source_rupture_condition(
            Rupture.source_id, Rupture.rupture_id, source_ruptures
        )
        if selected is not None:
            statement = statement.where(selected)
        statement = statement.group_by(
            Rupture.erf_id,
            Rupture.source_id,
            Rupture.rupture_id,
            Rupture.num_points,
            Rupture.num_rows,
            Rupture.num_columns,
            Rupture.mag,
        ).order_by(Rupture.num_points.desc(), Rupture.source_id, Rupture.rupture_id)
        with self._session() as session:
            return tuple(RuptureSummary(*row) for row in session.execute(statement))

    def get_rupture_variations(
        self,
        *,
        erf_id: int,
        rupture_variation_id: int,
        site: str | None = None,
        cutoff_distance: float | None = None,
        source_ruptures: Collection[SourceRupture] | None = None,
        order_by_size: bool = False,
    ) -> tuple[RuptureVariation, ...]:
        """Return rupture variations used by PP and stochastic generators.

        Args:
            erf_id (int): ERF ID.
            rupture_variation_id (int): Rupture variation scenario ID.
            site (str | None, optional): Restrict to site ruptures. Defaults to
                ``None``.
            cutoff_distance (float | None, optional): Site cutoff in kilometres.
                Required with ``site``; defaults to ``None``.
            source_ruptures (Collection[SourceRupture] | None, optional): Explicit
                source/rupture pairs. Defaults to ``None``.
            order_by_size (bool, optional): Put larger rupture surfaces first.
                Defaults to ``False``.

        Returns:
            tuple[RuptureVariation, ...]: Matching variations in stable order.

        Raises:
            ValueError: If exactly one of ``site`` and ``cutoff_distance`` is set.
        """
        if (site is None) != (cutoff_distance is None):
            raise ValueError("site and cutoff_distance must be provided together")
        statement = select(RuptureVariation)
        if site is not None:
            statement = statement.join(
                SiteRupture,
                and_(
                    SiteRupture.erf_id == RuptureVariation.erf_id,
                    SiteRupture.source_id == RuptureVariation.source_id,
                    SiteRupture.rupture_id == RuptureVariation.rupture_id,
                ),
            ).join(Site, Site.cs_site_id == SiteRupture.cs_site_id)
        if order_by_size:
            statement = statement.join(
                Rupture,
                and_(
                    Rupture.erf_id == RuptureVariation.erf_id,
                    Rupture.source_id == RuptureVariation.source_id,
                    Rupture.rupture_id == RuptureVariation.rupture_id,
                ),
            )
        statement = statement.where(
            RuptureVariation.erf_id == erf_id,
            RuptureVariation.rup_var_scenario_id == rupture_variation_id,
        )
        if site is not None:
            statement = statement.where(
                Site.cs_short_name == site,
                SiteRupture.cutoff_dist == cutoff_distance,
            )
        selected = _source_rupture_condition(
            RuptureVariation.source_id,
            RuptureVariation.rupture_id,
            source_ruptures,
        )
        if selected is not None:
            statement = statement.where(selected)
        if order_by_size:
            statement = statement.order_by(
                Rupture.num_points.desc(),
                RuptureVariation.source_id,
                RuptureVariation.rupture_id,
                RuptureVariation.rup_var_id,
            )
        else:
            statement = statement.order_by(
                RuptureVariation.source_id,
                RuptureVariation.rupture_id,
                RuptureVariation.rup_var_id,
            )
        with self._session() as session:
            return tuple(session.scalars(statement))

    def get_rupture_variations_with_seeds(
        self,
        *,
        erf_id: int,
        rupture_variation_id: int,
        site: str | None = None,
        cutoff_distance: float | None = None,
        source_ruptures: Collection[SourceRupture] | None = None,
    ) -> tuple[RuptureVariationSeed, ...]:
        """Return rupture variations paired with database seeds.

        Args:
            erf_id (int): ERF ID.
            rupture_variation_id (int): Rupture variation scenario ID.
            site (str | None, optional): Restrict to site ruptures. Defaults to
                ``None``.
            cutoff_distance (float | None, optional): Site cutoff in kilometres.
                Required with ``site``; defaults to ``None``.
            source_ruptures (Collection[SourceRupture] | None, optional): Explicit
                source/rupture pairs. Defaults to ``None``.

        Returns:
            tuple[RuptureVariationSeed, ...]: Variations and seeds ordered by
            source, rupture, and variation ID.

        Raises:
            ValueError: If exactly one of ``site`` and ``cutoff_distance`` is set.
        """
        if (site is None) != (cutoff_distance is None):
            raise ValueError("site and cutoff_distance must be provided together")
        statement = select(
            RuptureVariation.source_id,
            RuptureVariation.rupture_id,
            RuptureVariation.rup_var_id,
            RuptureVariation.rup_var_lfn,
            RuptureVariation.rvfrac,
            RupVarSeed.rup_var_seed,
        ).join(
            RupVarSeed,
            and_(
                RupVarSeed.erf_id == RuptureVariation.erf_id,
                RupVarSeed.source_id == RuptureVariation.source_id,
                RupVarSeed.rupture_id == RuptureVariation.rupture_id,
                RupVarSeed.rup_var_id == RuptureVariation.rup_var_id,
                RupVarSeed.rup_var_scenario_id == RuptureVariation.rup_var_scenario_id,
            ),
        )
        if site is not None:
            statement = statement.join(
                SiteRupture,
                and_(
                    SiteRupture.erf_id == RuptureVariation.erf_id,
                    SiteRupture.source_id == RuptureVariation.source_id,
                    SiteRupture.rupture_id == RuptureVariation.rupture_id,
                ),
            ).join(Site, Site.cs_site_id == SiteRupture.cs_site_id)
        statement = statement.where(
            RuptureVariation.erf_id == erf_id,
            RuptureVariation.rup_var_scenario_id == rupture_variation_id,
        )
        if site is not None:
            statement = statement.where(
                Site.cs_short_name == site,
                SiteRupture.cutoff_dist == cutoff_distance,
            )
        selected = _source_rupture_condition(
            RuptureVariation.source_id,
            RuptureVariation.rupture_id,
            source_ruptures,
        )
        if selected is not None:
            statement = statement.where(selected)
        statement = statement.order_by(
            RuptureVariation.source_id,
            RuptureVariation.rupture_id,
            RuptureVariation.rup_var_id,
        )
        with self._session() as session:
            return tuple(
                RuptureVariationSeed(*row) for row in session.execute(statement)
            )

    def get_run_statistics(self, run_id: int) -> RunStatistics:
        """Return peak-amplitude and hazard-curve statistics for a run.

        Args:
            run_id (int): CyberShake run ID.

        Returns:
            RunStatistics: Stored peak-amplitude count and intensity measures.
        """
        peak_statement = (
            select(func.count())
            .select_from(PeakAmplitude)
            .where(PeakAmplitude.run_id == run_id)
        )
        measure_statement = (
            select(IMType)
            .join(HazardCurve, HazardCurve.im_type_id == IMType.im_type_id)
            .where(HazardCurve.run_id == run_id)
            .order_by(IMType.im_type_value)
        )
        with self._session() as session:
            peak_count = int(session.scalar(peak_statement) or 0)
            measures = tuple(session.scalars(measure_statement))
        return RunStatistics(peak_count, measures)

    # ------------------------------------------------------------------
    # Run writes
    # ------------------------------------------------------------------

    def insert_run(self, run: Run) -> int:
        """Insert a run and return its generated ID.

        Args:
            run (Run): Complete run row without a required ``run_id``.

        Returns:
            int: Generated run ID.

        Raises:
            sqlalchemy.exc.IntegrityError: If a database constraint fails. A
                standalone insert is rolled back before the error is re-raised.
        """
        with self._session(write=True) as session:
            session.add(run)
            session.flush()
            return run.run_id

    def clone_run(
        self,
        source_run_id: int,
        *,
        changes: Mapping[str, RunFieldValue],
    ) -> int:
        """Clone a run and apply explicit field changes atomically.

        Args:
            source_run_id (int): Run to copy.
            changes (Mapping[str, RunFieldValue]): Fields that differ on the new
                run. The primary key cannot be replaced.

        Returns:
            int: Generated clone run ID.

        Raises:
            RunManagerError: If the source run does not exist.
            ValueError: If ``changes`` contains an unsupported field.
        """
        updates = self._validate_run_fields(changes)
        with self._session(write=True) as session:
            statement = select(Run).where(Run.run_id == source_run_id).with_for_update()
            source = session.scalar(statement)
            if source is None:
                raise RunManagerError(f"Run {source_run_id} does not exist")
            values = self._run_values(source)
            values.update(updates)
            clone = Run(**values)
            session.add(clone)
            session.flush()
            return clone.run_id

    def update_run_fields(
        self,
        run_id: int,
        fields: Mapping[str, RunFieldValue],
    ) -> bool:
        """Update selected mapped fields on a run.

        Args:
            run_id (int): Run to update.
            fields (Mapping[str, RunFieldValue]): Python model attributes and new
                values. The primary key cannot be replaced.

        Returns:
            bool: ``True`` when the run exists and was targeted.

        Raises:
            ValueError: If no fields are supplied or a field is unsupported.
        """
        values = self._validate_run_fields(fields)
        if not values:
            raise ValueError("At least one run field is required")
        with self._session(write=True) as session:
            run = session.scalar(
                select(Run).where(Run.run_id == run_id).with_for_update()
            )
            if run is None:
                return False
            for name, value in values.items():
                setattr(run, name, value)
            session.flush()
            return True

    def update_run_frequencies(
        self,
        run_id: int,
        *,
        max_frequency: float,
        low_frequency_cutoff: float | None = None,
    ) -> bool:
        """Persist frequencies selected by a workflow generator.

        Args:
            run_id (int): Run to update.
            max_frequency (float): Maximum frequency in Hz.
            low_frequency_cutoff (float | None, optional): Broadband merge
                frequency in Hz. Defaults to ``None``, leaving the stored cutoff
                unchanged.

        Returns:
            bool: ``True`` when the run exists and was targeted.
        """
        values: dict[str, RunFieldValue] = {"max_frequency": max_frequency}
        if low_frequency_cutoff is not None:
            values["low_frequency_cutoff"] = low_frequency_cutoff
        return self.update_run_fields(run_id, values)

    def update_target_vs30(
        self, run_id: int, *, target_vs30: float, source: str
    ) -> bool:
        """Persist a target Vs30 and its provenance.

        Args:
            run_id (int): Run to update.
            target_vs30 (float): Target Vs30 in metres per second.
            source (str): Description of the Vs30 source.

        Returns:
            bool: ``True`` when the run exists and was targeted.
        """
        return self.update_run_fields(
            run_id, {"target_vs30": target_vs30, "vs30_source": source}
        )

    def get_velocity_parameters(self, run_id: int) -> VelocityParameters | None:
        """Return velocity and site-response parameters for a run.

        Args:
            run_id (int): CyberShake run ID.

        Returns:
            VelocityParameters | None: Stored values, or ``None`` when absent.
        """
        statement = select(
            Run.model_vs30,
            Run.mesh_vsitop,
            Run.z1_0,
            Run.z2_5,
            Run.target_vs30,
            Run.vs30_source,
        ).where(Run.run_id == run_id)
        with self._session() as session:
            row = session.execute(statement).one_or_none()
            return VelocityParameters(*row) if row is not None else None

    def copy_broadband_velocity_parameters(
        self,
        low_frequency_run_id: int,
        broadband_run_id: int,
        *,
        model_vs30: float,
    ) -> bool:
        """Copy basin depths from a low-frequency run to a broadband run.

        Args:
            low_frequency_run_id (int): Run providing ``Z1_0`` and ``Z2_5``.
            broadband_run_id (int): Run receiving the values.
            model_vs30 (float): Broadband model Vs30 in metres per second.

        Returns:
            bool: ``True`` when the broadband run exists and was targeted.

        Raises:
            RunManagerError: If the low-frequency run does not exist.
        """
        if self._transaction_session is None:
            with self.transaction() as transaction:
                return transaction.copy_broadband_velocity_parameters(
                    low_frequency_run_id,
                    broadband_run_id,
                    model_vs30=model_vs30,
                )
        parameters = self.get_velocity_parameters(low_frequency_run_id)
        if parameters is None:
            raise RunManagerError(f"Run {low_frequency_run_id} does not exist")
        return self.update_run_fields(
            broadband_run_id,
            {
                "model_vs30": model_vs30,
                "z1_0": parameters.z1_0,
                "z2_5": parameters.z2_5,
            },
        )


def connect(
    *,
    server: str,
    credentials: Path | None = None,
    config: Config | None = None,
) -> SQLAlchemyRunDB:
    """Create a repository connected to the configured CyberShake database.

    The credential file accepts either ``user:password`` or a password by
    itself. The latter uses the legacy read-only ``cybershk_ro`` account.

    Args:
        server (str): Default database host. ``CYBERSHAKE_DB_HOST`` overrides
            this value when set.
        credentials (Path | None, optional): Default credential-file path.
            Defaults to ``conf/db_pass.txt``. ``CYBERSHAKE_DB_CREDENTIALS``
            overrides either value when set.
        config (Config | None, optional): Resolved command configuration. When
            present, its database host, port, name, user, and credential path
            are used. Defaults to ``None``.

    Returns:
        SQLAlchemyRunDB: Repository backed by a PyMySQL SQLAlchemy engine.

    Raises:
        RunManagerError: If the credential file cannot be read or is empty.
    """
    if config is not None:
        credential_path = credentials or Path(str(config["database_credentials"]))
        default_user = str(config["database_user"])
        host = str(config["database_host"])
        port = int(str(config["database_port"]))
        database_name = str(config["database_name"])
    else:
        configured_credentials = os.environ.get(DB_CREDENTIALS_ENV)
        credential_path = (
            Path(configured_credentials)
            if configured_credentials
            else credentials or DEFAULT_DB_CREDENTIALS
        )
        default_user = DEFAULT_DB_USER
        host = os.environ.get(DB_HOST_ENV, server)
        port = DEFAULT_DB_PORT
        database_name = DEFAULT_DB_NAME
    try:
        credential_text = credential_path.read_text(encoding="utf-8").strip()
    except OSError as error:
        msg = f"Unable to read database credentials from {credential_path}: {error}"
        raise RunManagerError(msg) from error
    if not credential_text:
        raise RunManagerError(f"Database credential file {credential_path} is empty")

    username, separator, password = credential_text.partition(":")
    if not separator:
        username, password = default_user, username
    if not username or not password:
        raise RunManagerError(
            f"Database credential file {credential_path} must contain user:password"
        )

    engine = create_engine(
        URL.create(
            "mysql+pymysql",
            username=username,
            password=password,
            host=host,
            port=port,
            database=database_name,
        ),
        pool_pre_ping=True,
    )
    return SQLAlchemyRunDB(engine)
