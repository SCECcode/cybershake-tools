"""Tests for the workflow-generator class boundaries."""

import pytest

from cybershake.config import resolve_config
from cybershake.constants import LEGACY_OPENSHA_CONF_DIR
from cybershake.workflow import common
from cybershake.workflow.full import Full
from cybershake.workflow.pp import PP
from cybershake.workflow.sgt import SGT
from cybershake.workflow.stoch import Stoch


@pytest.fixture
def sgt_generator() -> SGT:
    return SGT(config=resolve_config("sgt", {}, environ={}))


@pytest.fixture
def pp_generator() -> PP:
    return PP(config=resolve_config("pp", {}, environ={}))


@pytest.fixture
def stoch_generator() -> Stoch:
    return Stoch(config=resolve_config("stoch", {}, environ={}))


def test_full_composes_stage_generators(
    sgt_generator: SGT,
    pp_generator: PP,
    stoch_generator: Stoch,
) -> None:
    generator = Full(
        sgt=sgt_generator,
        pp=pp_generator,
        stoch=stoch_generator,
    )

    assert generator.sgt is sgt_generator
    assert generator.pp is pp_generator
    assert generator.stoch is stoch_generator


def test_full_stage_spacing_comes_from_resolved_config() -> None:
    config = resolve_config(
        "full", {"sgt_spacing": 0.12, "pp_spacing": 0.08}, environ={}
    )

    assert SGT(config=config).spacing == 0.12
    assert PP(config=config).spacing == 0.08


@pytest.mark.parametrize("erf_id", [42, 48])
def test_removed_erfs_use_default_opensha_configuration(erf_id: int) -> None:
    assert common._erf_file(erf_id) == LEGACY_OPENSHA_CONF_DIR / "MeanUCERF.xml"
