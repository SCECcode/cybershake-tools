"""Stable values shared by CyberShake commands and workflow builders.

Deployment-specific values in this module are legacy-compatible defaults.  They
may later be overridden by configuration, but keeping the defaults here gives
the migration one documented source of truth.  Secrets must never be added to
this module.
"""

from __future__ import annotations

from pathlib import Path
from typing import Final

#: Installed Python package directory.
PACKAGE_DIR: Final = Path(__file__).resolve().parent
#: Click settings that enable both short and long help options.
CONTEXT_SETTINGS: Final = {"help_option_names": ["-h", "--help"]}
#: Prefix shared by environment variables that supply Click option values.
CLI_ENVVAR_PREFIX: Final = "CYBERSHAKE"
#: Default Pegasus properties file for SGT planning.
DEFAULT_SGT_PROPERTIES: Final = Path("conf/properties.sgt")
#: Default Pegasus properties file for post-processing planning.
DEFAULT_PP_PROPERTIES: Final = Path("conf/properties.mpi-cluster.pp")
#: Default Pegasus properties file shared by stochastic and Full planning.
DEFAULT_FULL_PROPERTIES: Final = Path("conf/properties.full")
#: X.509 proxy path exported by legacy PP and stochastic planning wrappers.
LEGACY_X509_USER_PROXY: Final = Path("/home/shock-ssd/scottcal/condor/x509up_u7588")
#: Maximum runtime enforced by the legacy cron wrapper, in seconds.
CRON_RUN_CHECK_TIMEOUT_SECONDS: Final = 840
#: Lock removed by the legacy cron wrapper when its run check times out.
LEGACY_CRON_LOCK_FILE: Final = Path(
    "/home/shock/scottcal/runs/cybershake-tools/runmanager/lock"
)
#: Accepted periodic run-check modes.
CRON_RUN_CHECK_MODES: Final = ("MASTER", "SECONDARY")
#: SSH executable used to trigger legacy remote map generation.
SSH_EXECUTABLE: Final = "ssh"

#: Environment variable that overrides the OpenSHA Java classpath.
CLASSPATH_ENV: Final = "CYBERSHAKE_VS30_CLASSPATH"
#: Directory containing the packaged OpenSHA resources.
OPENSHA_DIR: Final = PACKAGE_DIR / "share" / "opensha"
#: Vs30 Java source file run directly by Java 11 or newer.
CLI_SOURCE: Final = (
    OPENSHA_DIR / "src/main/java/org/scec/cme/cybershake/vs30/Vs30Cli.java"
)

#: Java executable used only at the external Vs30 boundary.
JAVA_EXECUTABLE: Final = "java"
#: Packaged Java source for database product checks.
CHECK_DB_SOURCE: Final = PACKAGE_DIR / "share/db/src/main/java/CheckDBDataForSite.java"
CHECK_DB_CONNECT_SOURCE: Final = (
    PACKAGE_DIR / "share/db/src/main/java/org/scec/cme/cybershake/dax3/DBConnect.java"
)
CHECK_DB_JARS: Final = PACKAGE_DIR / "share/db/jars"
#: Optional override for the database-check Java classpath.
CHECK_DB_CLASSPATH_ENV: Final = "CYBERSHAKE_CHECK_DB_CLASSPATH"
#: Pegasus planning executable.
PEGASUS_PLAN_EXECUTABLE: Final = "pegasus-plan"
#: Pegasus submission executable.
PEGASUS_RUN_EXECUTABLE: Final = "pegasus-run"
#: Pegasus replica-catalog executable.
PEGASUS_RC_EXECUTABLE: Final = "pegasus-rc-client"
#: HTCondor configuration lookup executable.
CONDOR_CONFIG_VAL_EXECUTABLE: Final = "condor_config_val"
#: HTCondor queue query executable.
CONDOR_Q_EXECUTABLE: Final = "condor_q"
#: Number of attempts for an HTCondor queue query.
CONDOR_QUERY_ATTEMPTS: Final = 3
#: Delay between failed HTCondor queue queries, in seconds.
CONDOR_QUERY_RETRY_SECONDS: Final = 30
#: Environment variable overriding the replica-catalog database path.
REPLICA_CATALOG_DB_ENV: Final = "CYBERSHAKE_RC_DB"
#: Default SQLite replica-catalog database used when cloning runs.
DEFAULT_REPLICA_CATALOG_DB: Final = Path("/home/shock-ssd/scottcal/workflow/RC.sqlite")

#: Default CyberShake database server used by the CLI and generated jobs.
DEFAULT_DB_SERVER: Final = "moment"
#: Default CyberShake database TCP port.
DEFAULT_DB_PORT: Final = 3306
#: Default CyberShake database name.
DEFAULT_DB_NAME: Final = "CyberShake"
#: Default read-only CyberShake database user.
DEFAULT_DB_USER: Final = "cybershk_ro"
#: Default credential file; passwords are deliberately absent from this module.
DEFAULT_DB_CREDENTIALS: Final = Path("conf/db_pass.txt")
#: Environment variable overriding the database host used by Python clients.
DB_HOST_ENV: Final = "CYBERSHAKE_DB_HOST"
#: Environment variable overriding the database credential-file path.
DB_CREDENTIALS_ENV: Final = "CYBERSHAKE_DB_CREDENTIALS"

#: Velocity model aliases accepted by shell-compatible entry points.
#: Model ID 3 remains for PP parsing, although RunIDQuery cannot translate that
#: half-space model into a UCVM model string.
VELOCITY_MODELS: Final = {
    "v4": 1,
    "vhs": 3,
    "vh": 4,
    "vsi": 5,
    "vs1": 6,
    "vhng": 7,
    "vbbp": 8,
    "vcca1d": 9,
    "vcca": 10,
    "vusgs": 11,
    "v188": 12,
    "vtaper": 13,
    "vsfcvm": 14,
    "v246": 15,
}
#: Velocity model aliases accepted by the SGT shell-compatible command.
SGT_VELOCITY_MODELS: Final = tuple(alias for alias in VELOCITY_MODELS if alias != "vhs")
#: Velocity model aliases accepted by the PP shell-compatible command.
PP_VELOCITY_MODELS: Final = tuple(
    alias for alias in VELOCITY_MODELS if alias != "vusgs"
)

#: Database velocity model IDs mapped to UCVM model strings passed to jobs.
#: ID 3 is intentionally absent because legacy RunIDQuery rejects the 0-D model.
VELOCITY_MODEL_STRINGS: Final = {
    1: "cvms",
    2: "cvmh11.2",
    4: "cvmh",
    5: "cvmsi",
    6: "scec1d",
    7: "cvmh_nogtl",
    8: "bbp1d",
    9: "cca1d",
    10: "cca,usgs,cvmsi",
    11: "usgs,cca,cvmsi",
    12: "cca,usgs,cvmsi",
    13: "cvmsi",
    14: "sfcvm",
    15: "sfcvm,cca,bbp1d",
}

#: SGT variation IDs mapped to the code names used by generated jobs.
SGT_STRINGS: Final = {
    5: "rwg",
    6: "awp",
    7: "rwg3.0.3",
    8: "awp_gpu",
}
#: SGT variation IDs implemented by AWP rather than RWG.
AWP_SGT_IDS: Final = frozenset({6, 8})

#: High-frequency 1-D velocity model aliases and their staged filenames.
HF_VEL_MODELS: Final = {
    "labasin": "nr02-vs500.fk1d",
    "mojave": "mj-vs500.fk1d",
    "southernsierra": "ssn2-vs500.fk1d",
    "centralcal": "pf01-vs500.fk1d",
    "nocal": "lp-vs500.fk1d",
}

#: Initial run state stored in ``CyberShake_Runs``.
START_STATE: Final = "Initial"
#: State indicating that SGT generation has started.
SGT_STARTED_STATE: Final = "SGT Started"
#: State indicating that SGT generation completed successfully.
SGT_GENERATED_STATE: Final = "SGT Generated"
#: State indicating that SGT generation failed.
SGT_ERROR_STATE: Final = "SGT Error"
#: State indicating that post-processing has started.
PP_STARTED_STATE: Final = "PP Started"
#: State indicating that post-processing generated hazard curves.
CURVES_GENERATED_STATE: Final = "Curves Generated"
#: State indicating that post-processing failed.
PP_ERROR_STATE: Final = "PP Error"
#: State indicating that plotting is in progress.
PLOT_STATE: Final = "Plotting"
#: State indicating that run verification failed.
VERIFY_ERROR_STATE: Final = "Verify Error"
#: State indicating that a run has been verified.
DONE_STATE: Final = "Verified"
#: Terminal state for a deleted run.
DELETED_STATE: Final = "Deleted"

#: States that update the SGT status timestamp.
SGT_STATES: Final = (
    SGT_STARTED_STATE,
    SGT_GENERATED_STATE,
    SGT_ERROR_STATE,
)
#: States that update the post-processing status timestamp.
PP_STATES: Final = (
    PP_STARTED_STATE,
    CURVES_GENERATED_STATE,
    PP_ERROR_STATE,
)
#: Allowed next states when run-state transition checking is enabled.
STATUS_STD: Final = {
    START_STATE: (START_STATE, SGT_STARTED_STATE, SGT_ERROR_STATE, DELETED_STATE),
    SGT_STARTED_STATE: (
        SGT_STARTED_STATE,
        SGT_GENERATED_STATE,
        SGT_ERROR_STATE,
        DELETED_STATE,
    ),
    SGT_ERROR_STATE: (
        START_STATE,
        SGT_STARTED_STATE,
        SGT_ERROR_STATE,
        DELETED_STATE,
    ),
    SGT_GENERATED_STATE: (
        SGT_GENERATED_STATE,
        PP_STARTED_STATE,
        PP_ERROR_STATE,
        DELETED_STATE,
    ),
    PP_STARTED_STATE: (
        PP_STARTED_STATE,
        CURVES_GENERATED_STATE,
        PP_ERROR_STATE,
        DELETED_STATE,
    ),
    PP_ERROR_STATE: (
        SGT_GENERATED_STATE,
        PP_STARTED_STATE,
        PP_ERROR_STATE,
        DELETED_STATE,
    ),
    CURVES_GENERATED_STATE: (
        CURVES_GENERATED_STATE,
        PLOT_STATE,
        DONE_STATE,
        VERIFY_ERROR_STATE,
        DELETED_STATE,
    ),
    PLOT_STATE: (PLOT_STATE, DONE_STATE, VERIFY_ERROR_STATE, DELETED_STATE),
    VERIFY_ERROR_STATE: (
        VERIFY_ERROR_STATE,
        PLOT_STATE,
        DONE_STATE,
        DELETED_STATE,
    ),
    DONE_STATE: (DONE_STATE,),
    DELETED_STATE: (DELETED_STATE,),
}

#: Database states from which each planning stage is valid.
PLAN_STAGES: Final = {
    "SGT_PLAN": (START_STATE, SGT_ERROR_STATE),
    "PP_PLAN": (SGT_GENERATED_STATE, PP_ERROR_STATE),
}

#: Expected database states for each deferred UpdateRun workflow state.
#: Empty tuples are legacy completion transitions with no enforced start state.
RUN_STATES: Final = {
    "SGT_INIT": (START_STATE, SGT_STARTED_STATE),
    "SGT_START": (SGT_STARTED_STATE,),
    "SGT_END": (),
    "PP_INIT": (SGT_GENERATED_STATE, PP_STARTED_STATE),
    "PP_START": (PP_STARTED_STATE,),
    "PP_END": (),
}
#: Database state represented by each deferred UpdateRun workflow state.
STATE_MAP: Final = {
    "SGT_INIT": START_STATE,
    "SGT_START": SGT_STARTED_STATE,
    "SGT_END": SGT_GENERATED_STATE,
    "PP_INIT": SGT_GENERATED_STATE,
    "PP_START": PP_STARTED_STATE,
    "PP_END": CURVES_GENERATED_STATE,
}

#: Run-state preference groups used by deterministic and stochastic run lookup.
#: Runs in ``USE`` states are reused; ``CLONE`` states seed a new run.
MATCH_STATE_DICT: Final = {
    "USE": (SGT_GENERATED_STATE,),
    "CLONE": (
        PP_STARTED_STATE,
        PP_ERROR_STATE,
        CURVES_GENERATED_STATE,
        PLOT_STATE,
        DONE_STATE,
    ),
}

#: Error state selected when a submitted workflow disappears from HTCondor.
CRON_ERROR_STATE_MAP: Final = {
    SGT_STARTED_STATE: SGT_ERROR_STATE,
    START_STATE: SGT_ERROR_STATE,
    PP_STARTED_STATE: PP_ERROR_STATE,
    SGT_GENERATED_STATE: PP_ERROR_STATE,
}

#: Compute hosts accepted by legacy run-manager validation.
HOSTS: Final = ("unknown", "hpc", "frontera", "frontier", "vista")
#: User names accepted by legacy run-manager validation.
USERS: Final = ("cybershk", "scottcal", "kmilner", "mayani")
#: Database fields accepted by the run-manager edit command.
VALID_FIELDS: Final = (
    "Status",
    "SGT_Host",
    "PP_Host",
    "Comment",
    "Last_User",
    "Job_ID",
    "Submit_Dir",
    "Notify_User",
)
#: Maximum length of ``CyberShake_Runs.Submit_Dir``.
MAX_RUN_SUBMIT_DIR: Final = 256
#: Maximum length of ``CyberShake_Runs.Comment``.
MAX_RUN_COMMENT: Final = 128
#: Maximum length of ``CyberShake_Runs.Notify_User``.
MAX_RUN_NOTIFY_USER: Final = 128

#: Default namespace of CyberShake Pegasus transformations.
NAMESPACE: Final = "scec"
#: Default version of CyberShake Pegasus transformations.
VERSION: Final = "1.0"
#: Filename used to persist generated run metadata.
RUN_TABLE_FILENAME: Final = "run_table.txt"
#: Transformation name of deferred run-state updates.
UPDATE_RUN_TRANSFORMATION: Final = "UpdateRun"
#: Transformation name of the deferred SGT sub-workflow generator.
GEN_SGT_WORKFLOW_TRANSFORMATION: Final = "GenSGTDax"
#: Transformation name of the deferred stochastic sub-workflow generator.
GEN_STOCH_WORKFLOW_TRANSFORMATION: Final = "GenStochDax"
#: DirectSynth transformation versions by rupture variation scenario ID.
DIRECT_SYNTH_VERSIONS: Final = {
    5: "1.0",
    6: "1.0",
    7: "2.0",
    8: "1.0",
    9: "3.0",
    10: "4.0",
}

#: Minimum velocity supplied by the top-level SGT generator when not overridden.
DEFAULT_TOP_LEVEL_SGT_MIN_VS: Final = 500.0
#: Default SGT grid-spacing fraction.
DEFAULT_SGT_H_FRACTION: Final = 0.25
#: Default SGT model rotation in degrees.
DEFAULT_SGT_ROTATION: Final = -55.0
#: Default SGT Ely taper selection mode.
DEFAULT_SGT_TAPER_MODE: Final = "none"
#: Default SGT Ely taper depth in metres.
DEFAULT_SGT_TAPER_DEPTH: Final = 700.0
#: Default comma-separated models to which the SGT taper applies.
DEFAULT_SGT_TAPER_MODELS: Final = "all"
#: Storage site used by the dynamically planned AWP SGT sub-workflow.
DEFAULT_SGT_OUTPUT_SITE: Final = "frontera"
#: Approximate physical width smoothed around velocity-model boundaries, in km.
SGT_SMOOTHING_DISTANCE_KM: Final = 20.0
#: Database credential path visible to deferred compute-host insertion jobs.
DEFAULT_JOB_DB_CREDENTIALS: Final = Path("/home/shock/scottcal/runs/config/db_pass.txt")

#: Default SeisPSA memory cutoff in mebibytes.
DEFAULT_PP_SEISMOGRAM_MEMORY_MB: Final = 56 * 1024
#: Default seismogram duration in seconds.
DEFAULT_PP_SEISMOGRAM_LENGTH_SECONDS: Final = 400.0
#: Globus endpoint for generated inputs staged from the submission host.
CARC_GLOBUS_ENDPOINT: Final = "56569ec1-af41-4745-a8d1-8514231c7a6d"

#: Default high-frequency stochastic simulation frequency in hertz.
DEFAULT_STOCH_FREQUENCY: Final = 10.0
#: Default stochastic seismogram duration in seconds.
DEFAULT_STOCH_DURATION_SECONDS: Final = 300.0
#: Stochastic seismogram duration when the low-frequency run reaches 1 Hz.
DEFAULT_STOCH_LONG_DURATION_SECONDS: Final = 400.0
#: Default stochastic reference velocity in metres per second.
DEFAULT_STOCH_REFERENCE_VELOCITY: Final = 500.0
#: Default stochastic grid-spacing fraction.
DEFAULT_STOCH_H_FRACTION: Final = 0.25
#: Default high-frequency 1-D velocity model alias.
DEFAULT_STOCH_HF_VEL_MODEL: Final = "labasin"

#: Persistent post-processing result directory on the legacy storage site.
PP_STORAGE_DIR: Final = Path("/project2/scec_608/cybershake/results/PPFiles")
#: Directory receiving database-product consistency-check output.
DB_CHECK_OUTPUT_DIR: Final = Path("/home/shock/scottcal/db_products/db_check_outfiles")
#: Directory receiving generated hazard curves.
CURVE_OUTPUT_DIR: Final = Path("/home/shock/scottcal/db_products/curves")
#: Directory receiving disaggregation plots and data.
DISAGG_OUTPUT_DIR: Final = Path("/home/shock/scottcal/db_products/disagg")
#: Legacy OpenSHA working directory used by periodic plotting.
LEGACY_OPENSHA_HOME: Final = Path("/home/shock-ssd/scottcal/opensha")
#: OpenSHA configuration directory used by database-product transformations.
LEGACY_OPENSHA_CONF_DIR: Final = LEGACY_OPENSHA_HOME / "conf"
#: Remote login used to refresh the public CyberShake maps.
LEGACY_OPENSHA_LOGIN: Final = "cybershk@shock.usc.edu"
#: Remote script that creates the CyberShake scatter map.
LEGACY_OPENSHA_SCATTER_SCRIPT: Final = LEGACY_OPENSHA_HOME / "make_scatter_map.sh"
#: Remote script that creates interpolated CyberShake maps.
LEGACY_OPENSHA_INTERPOLATED_SCRIPT: Final = (
    LEGACY_OPENSHA_HOME / "make_interpolated_map.sh"
)
#: Legacy OpenSHA wrapper that creates comparison curves.
#: ERF XML supplied to the legacy comparison-curve wrapper.
LEGACY_OPENSHA_ERF_XML: Final = LEGACY_OPENSHA_CONF_DIR / "MeanUCERF.xml"
#: Attenuation-relation XML files supplied to comparison-curve generation.
LEGACY_OPENSHA_ATTENUATION_XML: Final = ",".join(
    str(LEGACY_OPENSHA_CONF_DIR / filename)
    for filename in ("cb2014.xml", "bssa2014.xml", "cy2014.xml", "ask2014.xml")
)
#: Status page included in automated run-manager email notifications.
RUN_MANAGER_STATUS_URL: Final = "http://strike.scec.org/cybershake/status/"
