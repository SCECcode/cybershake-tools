"""Tests for constants preserved from the legacy implementation."""

from cybershake.constants import (
    AWP_SGT_IDS,
    CLI_ENVVAR_PREFIX,
    CLI_SOURCE,
    CONDOR_CONFIG_VAL_EXECUTABLE,
    CONDOR_Q_EXECUTABLE,
    CONDOR_QUERY_ATTEMPTS,
    CONDOR_QUERY_RETRY_SECONDS,
    CRON_ERROR_STATE_MAP,
    CRON_RUN_CHECK_MODES,
    DB_CREDENTIALS_ENV,
    DB_HOST_ENV,
    DEFAULT_DB_SERVER,
    DEFAULT_JOB_DB_CREDENTIALS,
    DEFAULT_REPLICA_CATALOG_DB,
    DEFAULT_SGT_OUTPUT_SITE,
    DEFAULT_STOCH_LONG_DURATION_SECONDS,
    DEFAULT_TOP_LEVEL_SGT_MIN_VS,
    DIRECT_SYNTH_VERSIONS,
    DONE_STATE,
    HF_VEL_MODELS,
    MATCH_STATE_DICT,
    OPENSHA_DIR,
    PLAN_STAGES,
    PP_ERROR_STATE,
    REPLICA_CATALOG_DB_ENV,
    SGT_ERROR_STATE,
    SGT_GENERATED_STATE,
    SGT_STRINGS,
    START_STATE,
    STATUS_STD,
    VELOCITY_MODEL_STRINGS,
    VELOCITY_MODELS,
)


def test_package_and_cli_paths() -> None:
    assert DEFAULT_DB_SERVER == "moment"
    assert OPENSHA_DIR.name == "opensha"
    assert CLI_SOURCE.is_file()
    assert DEFAULT_REPLICA_CATALOG_DB.name == "RC.sqlite"
    assert REPLICA_CATALOG_DB_ENV == "CYBERSHAKE_RC_DB"
    assert CONDOR_CONFIG_VAL_EXECUTABLE == "condor_config_val"
    assert CONDOR_Q_EXECUTABLE == "condor_q"
    assert CONDOR_QUERY_ATTEMPTS == 3
    assert CONDOR_QUERY_RETRY_SECONDS == 30
    assert CLI_ENVVAR_PREFIX == "CYBERSHAKE"
    assert DB_HOST_ENV == "CYBERSHAKE_DB_HOST"
    assert DB_CREDENTIALS_ENV == "CYBERSHAKE_DB_CREDENTIALS"


def test_velocity_model_tables() -> None:
    assert VELOCITY_MODELS["v246"] == 15
    assert VELOCITY_MODELS["vhs"] == 3
    assert 3 not in VELOCITY_MODEL_STRINGS
    assert VELOCITY_MODEL_STRINGS[15] == "sfcvm,cca,bbp1d"
    assert HF_VEL_MODELS["nocal"] == "lp-vs500.fk1d"


def test_sgt_tables_and_defaults() -> None:
    assert SGT_STRINGS == {
        5: "rwg",
        6: "awp",
        7: "rwg3.0.3",
        8: "awp_gpu",
    }
    assert {6, 8} == AWP_SGT_IDS
    assert DEFAULT_TOP_LEVEL_SGT_MIN_VS == 500.0
    assert DEFAULT_SGT_OUTPUT_SITE == "frontera"
    assert DEFAULT_JOB_DB_CREDENTIALS.name == "db_pass.txt"
    assert DEFAULT_STOCH_LONG_DURATION_SECONDS == 400.0


def test_run_state_tables() -> None:
    assert STATUS_STD[START_STATE] == (
        START_STATE,
        "SGT Started",
        SGT_ERROR_STATE,
        "Deleted",
    )
    assert PLAN_STAGES["PP_PLAN"] == (SGT_GENERATED_STATE, PP_ERROR_STATE)
    assert MATCH_STATE_DICT["USE"] == (SGT_GENERATED_STATE,)
    assert DONE_STATE not in MATCH_STATE_DICT["USE"]
    assert CRON_RUN_CHECK_MODES == ("MASTER", "SECONDARY")
    assert CRON_ERROR_STATE_MAP[START_STATE] == SGT_ERROR_STATE


def test_direct_synth_versions() -> None:
    assert DIRECT_SYNTH_VERSIONS == {
        5: "1.0",
        6: "1.0",
        7: "2.0",
        8: "1.0",
        9: "3.0",
        10: "4.0",
    }
