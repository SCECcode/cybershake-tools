# Configuration

CyberShake supports flat configuration files for workflow-creation commands and
environment variables for both command options and deployment settings. The
files may also set deployment values for workflow creation.

## Configuration files

The following commands accept `-c FILE` or `--config FILE`:

- `sgt`
- `sub-sgt-wf`
- `pp`
- `stoch`
- `sub-stoch-wf`
- `full`

The file may instead be selected with `CYBERSHAKE_CONFIG`. An explicit
`--config` takes precedence over that environment variable. There is no default
file and no automatic file discovery. When selected, the path must be an
existing file; an empty file is valid.

Plan and run commands do not accept configuration files. They use their command
arguments and options plus the deployment environment variables described
below.

### File format

Files are UTF-8, headerless properties files containing one `key=value`
assignment per line:

```ini
# sgt.conf
erf-id=64
rupture-variation-id=10
frequency=1.0
site=SFRH
velocity-model=v246
sgt-variation-id=8
source-frequency=2.0
server=moment
spacing=0.08
h-fraction=0.25
no-smoothing=true
output-directory=/scratch2/cybershake/runs
database-port=3306
```

Use the active command's long option names or the deployment names in the table
below as keys. The leading `--` is optional. For command options, hyphens and
underscores are equivalent, so `h-fraction`, `h_fraction`, and `--h-fraction`
name the same option. Deployment keys require hyphens: use `output-directory`,
not `output_directory`. Keys are case-sensitive.

Blank lines and lines whose first non-whitespace character is `#` or `;` are
ignored. `:` also separates keys and values. Whitespace around keys and values
is stripped. Inline comments are not supported. A value may contain `=` because
only the first separator splits the key from its value. Duplicate keys are
rejected. Files use the same parser as Pegasus properties files
(`cybershake.properties.load` and `cybershake.properties.dump`).

Command-option values are converted and validated using the corresponding Click
option type. For example, integer and floating-point options require numeric
values, choices must be valid for that command, and flags accept boolean values
such as `true` and `false`. Deployment values use their declared types;
`database-port` must be an integer and the others are strings. Required options
may be supplied by the file. Unknown keys, malformed lines, invalid values, and
unreadable files are errors.

Invoke `cybershake COMMAND --help` for option types, choices, and short aliases.

### Precedence

Each workflow option is resolved in this order:

1. Explicit command-line option
1. Option environment variable
1. Configuration-file value
1. Built-in default

Every option has an environment variable formed from `CYBERSHAKE_` and its long
name in uppercase with hyphens changed to underscores. For example,
`--h-fraction` uses `CYBERSHAKE_H_FRACTION` and `--velocity-model` uses
`CYBERSHAKE_VELOCITY_MODEL`.

`CYBERSHAKE_VERBOSE` controls the root `--verbose` count but is not a valid
workflow configuration-file value.

For deployment keys, the environment variable takes precedence over the
configuration-file value, which takes precedence over the built-in default.
When neither is set, `database_host` follows the resolved `server` option.

## Configuration key reference

These tables cover every key in `cybershake.config.CONFIG_DEFAULTS`. The CLI
option column shows the long spelling; command help also lists legacy short
aliases. A dash means the key is not available from that source. `unset` is
serialized as `None` in a `Config` object. Required options have no built-in
value and must be supplied by the CLI, their environment variable, or the
configuration file.

### Core workflow keys

| Config key             | Commands                     | CLI option               | Environment variable              | Config-file key        | Default         | Description                                                                |
| ---------------------- | ---------------------------- | ------------------------ | --------------------------------- | ---------------------- | --------------- | -------------------------------------------------------------------------- |
| `erf_id`               | `sgt`, `pp`, `stoch`, `full` | `--erf-id`               | `CYBERSHAKE_ERF_ID`               | `erf-id`               | unset; required | Earthquake rupture forecast ID.                                            |
| `rupture_variation_id` | `sgt`, `pp`, `stoch`, `full` | `--rupture-variation-id` | `CYBERSHAKE_RUPTURE_VARIATION_ID` | `rupture-variation-id` | unset; required | Rupture variation scenario ID.                                             |
| `frequency`            | `sgt`, `pp`, `full`          | `--frequency`            | `CYBERSHAKE_FREQUENCY`            | `frequency`            | unset; required | Simulation frequency in Hz.                                                |
| `site`                 | `sgt`, `pp`, `stoch`, `full` | `--site`                 | `CYBERSHAKE_SITE`                 | `site`                 | unset; required | CyberShake site short name.                                                |
| `velocity_model`       | `sgt`, `pp`, `full`          | `--velocity-model`       | `CYBERSHAKE_VELOCITY_MODEL`       | `velocity-model`       | unset; required | Velocity-model alias.                                                      |
| `sgt_variation_id`     | `sgt`, `pp`, `full`          | `--sgt-variation-id`     | `CYBERSHAKE_SGT_VARIATION_ID`     | `sgt-variation-id`     | unset; required | SGT variation ID.                                                          |
| `source_frequency`     | `sgt`, `pp`, `full`          | `--source-frequency`     | `CYBERSHAKE_SOURCE_FREQUENCY`     | `source-frequency`     | unset           | SGT source-filter frequency; uses `frequency` when unset.                  |
| `server`               | all configurable commands    | `--server`               | `CYBERSHAKE_SERVER`               | `server`               | `moment`        | Database server used for site parameters, generated jobs, and inserts.     |
| `z_comp`               | all configurable commands    | `--z-comp`               | `CYBERSHAKE_Z_COMP`               | `z-comp`               | `false`         | Calculate the vertical component.                                          |
| `config`               | all configurable commands    | `--config`               | `CYBERSHAKE_CONFIG`               | `config`               | unset           | Select the defaults file; a `config` entry inside that file has no effect. |

### SGT and shared generation keys

| Config key            | Commands                             | CLI option              | Environment variable             | Config-file key       | Default         | Description                                          |
| --------------------- | ------------------------------------ | ----------------------- | -------------------------------- | --------------------- | --------------- | ---------------------------------------------------- |
| `spacing`             | `sgt`, `sub-sgt-wf`, `pp`            | `--spacing`             | `CYBERSHAKE_SPACING`             | `spacing`             | unset           | Grid-spacing override in km.                         |
| `handoff`             | `sgt`, `sub-sgt-wf`, `pp`, `full`    | `--handoff`             | `CYBERSHAKE_HANDOFF`             | `handoff`             | `false`         | Add the completed stage to the pending handoff file. |
| `h_fraction`          | `sgt`, `sub-sgt-wf`, `stoch`, `full` | `--h-fraction`          | `CYBERSHAKE_H_FRACTION`          | `h-fraction`          | unset           | Grid-point depth fraction for surface UCVM queries.  |
| `separate_md5`        | `sgt`, `sub-sgt-wf`, `full`          | `--separate-md5`        | `CYBERSHAKE_SEPARATE_MD5`        | `separate-md5`        | `false`         | Run MD5 jobs separately from PostAWP jobs.           |
| `min_vs`              | `sgt`, `sub-sgt-wf`, `full`          | `--min-vs`              | `CYBERSHAKE_MIN_VS`              | `min-vs`              | unset           | Minimum shear-wave velocity override.                |
| `no_smoothing`        | `sgt`, `sub-sgt-wf`, `full`          | `--no-smoothing`        | `CYBERSHAKE_NO_SMOOTHING`        | `no-smoothing`        | `false`         | Disable velocity-model smoothing.                    |
| `ely_taper`           | `sgt`, `sub-sgt-wf`, `full`          | `--ely-taper`           | `CYBERSHAKE_ELY_TAPER`           | `ely-taper`           | unset           | Ely taper mode.                                      |
| `taper_depth`         | `sgt`, `sub-sgt-wf`, `full`          | `--taper-depth`         | `CYBERSHAKE_TAPER_DEPTH`         | `taper-depth`         | unset           | Ely taper depth in metres.                           |
| `taper_models`        | `sgt`, `sub-sgt-wf`, `full`          | `--taper-models`        | `CYBERSHAKE_TAPER_MODELS`        | `taper-models`        | unset           | Comma-separated models to taper, `all`, or `none`.   |
| `local_vparam_insert` | `sgt`, `sub-sgt-wf`, `full`          | `--local-vparam-insert` | `CYBERSHAKE_LOCAL_VPARAM_INSERT` | `local-vparam-insert` | `false`         | Insert velocity parameters in a separate local job.  |
| `sgt_site`            | `sgt`, `full`                        | `--sgt-site`            | `CYBERSHAKE_SGT_SITE`            | `sgt-site`            | unset           | SGT execution site override.                         |
| `depth`               | `sgt`, `full`                        | `--depth`               | `CYBERSHAKE_DEPTH`               | `depth`               | unset           | Simulation depth in km.                              |
| `rotation`            | `sgt`, `full`                        | `--rotation`            | `CYBERSHAKE_ROTATION`            | `rotation`            | unset           | Simulation-volume rotation in degrees.               |
| `run_id`              | `sub-sgt-wf`, `sub-stoch-wf`         | `--run-id`              | `CYBERSHAKE_RUN_ID`              | `run-id`              | unset; required | Run ID represented by the generated sub-workflow.    |
| `grid_output_file`    | `sub-sgt-wf`                         | `--grid-output-file`    | `CYBERSHAKE_GRID_OUTPUT_FILE`    | `grid-output-file`    | unset; required | Grid description produced by PreCVM.                 |
| `workflow_file`       | `sub-sgt-wf`, `sub-stoch-wf`         | `--workflow-file`       | `CYBERSHAKE_WORKFLOW_FILE`       | `workflow-file`       | unset; required | Destination workflow file.                           |

### Post-processing and shared product keys

| Config key             | Commands                              | CLI option                | Environment variable              | Config-file key        | Default | Description                                                                               |
| ---------------------- | ------------------------------------- | ------------------------- | --------------------------------- | ---------------------- | ------- | ----------------------------------------------------------------------------------------- |
| `debug`                | `pp`, `stoch`, `sub-stoch-wf`, `full` | `--debug`                 | `CYBERSHAKE_DEBUG`                | `debug`                | `false` | Enable generator debugging.                                                               |
| `db_rv_seed`           | `pp`, `stoch`, `sub-stoch-wf`, `full` | `-no-dbrs`                | `CYBERSHAKE_DB_RV_SEED`           | `db-rv-seed`           | `true`  | Use database rupture-variation fractions and seeds by default.                            |
| `period_duration`      | `pp`, `stoch`, `sub-stoch-wf`, `full` | `--period-duration`       | `CYBERSHAKE_PERIOD_DURATION`      | `period-duration`      | `false` | Calculate period-dependent durations.                                                     |
| `no_vertical_response` | `pp`, `stoch`, `sub-stoch-wf`, `full` | `--no-vertical-response`  | `CYBERSHAKE_NO_VERTICAL_RESPONSE` | `no-vertical-response` | `false` | Skip vertical response spectra when the Z component is present.                           |
| `rupture_list`         | `pp`, `stoch`, `sub-stoch-wf`, `full` | `--rupture-list`          | `CYBERSHAKE_RUPTURE_LIST`         | `rupture-list`         | unset   | CSV file of `source_id,rupture_id` rows to include.                                       |
| `no_insert`            | `pp`, `full`                          | `--no-insert`             | `CYBERSHAKE_NO_INSERT`            | `no-insert`            | `false` | Do not insert ruptures into the database.                                                 |
| `high_frequency`       | `pp`, `full`                          | `--high-frequency`        | `CYBERSHAKE_HIGH_FREQUENCY`       | `high-frequency`       | unset   | High-frequency cutoff; a bare CLI option uses `1.0`.                                      |
| `no_mpi_cluster`       | `pp`, `full`                          | `--no-mpi-cluster`        | `CYBERSHAKE_NO_MPI_CLUSTER`       | `no-mpi-cluster`       | `false` | Do not use `pegasus-mpi-cluster`.                                                         |
| `dir_hierarchy`        | `pp`, `full`                          | `--dir-hierarchy`         | `CYBERSHAKE_DIR_HIERARCHY`        | `dir-hierarchy`        | `false` | Use a directory hierarchy on the compute resource.                                        |
| `rotd`                 | `pp`, `full`                          | `-no-r`, `--no-rotd`      | `CYBERSHAKE_ROTD`                 | `rotd`                 | `true`  | Calculate RotD by default; in `full`, controls both PP and stochastic stages.             |
| `skip_md5`             | `pp`, `full`                          | `--skip-md5`              | `CYBERSHAKE_SKIP_MD5`             | `skip-md5`             | `false` | Skip MD5 checksum jobs.                                                                   |
| `nonblocking_md5`      | `pp`, `full`                          | `--nonblocking-md5`       | `CYBERSHAKE_NONBLOCKING_MD5`      | `nonblocking-md5`      | `false` | Move MD5 checksums out of the critical path.                                              |
| `direct_synth`         | `pp`, `full`                          | none                      | none                              | none                   | `true`  | Always use DirectSynth for post-processing.                                               |
| `pp_site`              | `pp`, `full`                          | `--pp-site`               | `CYBERSHAKE_PP_SITE`              | `pp-site`              | unset   | Post-processing execution site override.                                                  |
| `duration`             | `pp`, `full`                          | `-no-du`, `--no-duration` | `CYBERSHAKE_DURATION`             | `duration`             | `true`  | Calculate duration metrics by default; in `full`, controls both PP and stochastic stages. |
| `seismogram_length`    | `pp`, `full`                          | `--seismogram-length`     | `CYBERSHAKE_SEISMOGRAM_LENGTH`    | `seismogram-length`    | unset   | Output seismogram length in seconds.                                                      |

### Stochastic keys

| Config key                  | Commands                        | CLI option                    | Environment variable                   | Config-file key             | Default                          | Description                                                 |
| --------------------------- | ------------------------------- | ----------------------------- | -------------------------------------- | --------------------------- | -------------------------------- | ----------------------------------------------------------- |
| `low_frequency_run_id`      | `stoch`, `sub-stoch-wf`         | `--low-frequency-run-id`      | `CYBERSHAKE_LOW_FREQUENCY_RUN_ID`      | `low-frequency-run-id`      | unset; required                  | Low-frequency run ID to combine.                            |
| `merge_frequency`           | `stoch`                         | `--merge-frequency`           | `CYBERSHAKE_MERGE_FREQUENCY`           | `merge-frequency`           | unset; required                  | Frequency for merging low-frequency and stochastic results. |
| `generator_merge_frequency` | `stoch`, `sub-stoch-wf`, `full` | `--generator-merge-frequency` | `CYBERSHAKE_GENERATOR_MERGE_FREQUENCY` | `generator-merge-frequency` | unset                            | Generator merge-frequency override in Hz.                   |
| `no_rotd`                   | `stoch`, `sub-stoch-wf`         | `--no-rotd`                   | `CYBERSHAKE_NO_ROTD`                   | `no-rotd`                   | `false`                          | Omit RotD calculations.                                     |
| `no_site_response`          | `stoch`, `sub-stoch-wf`, `full` | `--no-site-response`          | `CYBERSHAKE_NO_SITE_RESPONSE`          | `no-site-response`          | `false`                          | Omit site-response calculations.                            |
| `no_low_site_response`      | `stoch`, `sub-stoch-wf`, `full` | `--no-low-site-response`      | `CYBERSHAKE_NO_LOW_SITE_RESPONSE`      | `no-low-site-response`      | `false`                          | Omit site response for low-frequency seismograms.           |
| `no_duration`               | `stoch`, `sub-stoch-wf`         | `--no-duration`               | `CYBERSHAKE_NO_DURATION`               | `no-duration`               | `false`                          | Omit duration calculations.                                 |
| `hf_vel_model`              | `stoch`, `sub-stoch-wf`, `full` | `--hf-vel-model`              | `CYBERSHAKE_HF_VEL_MODEL`              | `hf-vel-model`              | unset                            | 1-D velocity model for high-frequency synthesis.            |
| `stoch_frequency`           | `stoch`, `full`                 | `--stoch-frequency`           | `CYBERSHAKE_STOCH_FREQUENCY`           | `stoch-frequency`           | `10.0` (`stoch`); unset (`full`) | Maximum stochastic calculation frequency in Hz.             |
| `force_vs30`                | `stoch`, `full`                 | `--force-vs30`                | `CYBERSHAKE_FORCE_VS30`                | `force-vs30`                | unset                            | Force the Vs30 value used for site response.                |
| `velocity_file`             | `sub-stoch-wf`                  | `--velocity-file`             | `CYBERSHAKE_VELOCITY_FILE`             | `velocity-file`             | unset; required                  | Velocity information file.                                  |

For `full`, `no_rotd` and `no_duration` are derived from `rotd` and `duration`;
they have no separate CLI, environment, or config-file settings.

### Full-only keys

| Config key    | Commands | CLI option      | Environment variable     | Config-file key | Default | Description                                     |
| ------------- | -------- | --------------- | ------------------------ | --------------- | ------- | ----------------------------------------------- |
| `broadband`   | `full`   | `--broadband`   | `CYBERSHAKE_BROADBAND`   | `broadband`     | `false` | Include stochastic high-frequency calculations. |
| `sgt_spacing` | `full`   | `--sgt-spacing` | `CYBERSHAKE_SGT_SPACING` | `sgt-spacing`   | unset   | SGT grid-spacing override in km.                |
| `pp_spacing`  | `full`   | `--pp-spacing`  | `CYBERSHAKE_PP_SPACING`  | `pp-spacing`    | unset   | Post-processing grid-spacing override in km.    |

Leaving `sgt_spacing` unset preserves frequency-derived SGT spacing.

### Deployment and derived keys

These keys have no CLI option. Workflow-creation commands accept their
hyphenated config-file keys; deployment environment variables take precedence.
`cybershake_version` is derived from the installed package.

| Config key                 | CLI option | Environment variable          | Config-file key            | Default                                       | Description                                                      |
| -------------------------- | ---------- | ----------------------------- | -------------------------- | --------------------------------------------- | ---------------------------------------------------------------- |
| `output_directory`         | —          | `CYBERSHAKE_OUTPUT_DIRECTORY` | `output-directory`         | `.`                                           | Parent directory for generated workflow run trees.               |
| `database_host`            | —          | `CYBERSHAKE_DB_HOST`          | `database-host`            | resolved `server` (`moment`)                  | Host used by local Python database clients.                      |
| `database_port`            | —          | `CYBERSHAKE_DB_PORT`          | `database-port`            | `3306`                                        | Database TCP port; file and environment values must be integers. |
| `database_name`            | —          | `CYBERSHAKE_DB_NAME`          | `database-name`            | `CyberShake`                                  | Database name.                                                   |
| `database_user`            | —          | `CYBERSHAKE_DB_USER`          | `database-user`            | `cybershk_ro`                                 | Database user name.                                              |
| `database_credentials`     | —          | `CYBERSHAKE_DB_CREDENTIALS`   | `database-credentials`     | `conf/db_pass.txt`                            | Path to the `user:password` credential file.                     |
| `replica_catalog_database` | —          | `CYBERSHAKE_RC_DB`            | `replica-catalog-database` | `/home/shock-ssd/scottcal/workflow/RC.sqlite` | SQLite replica-catalog database used when cloning runs.          |
| `vs30_classpath`           | —          | `CYBERSHAKE_VS30_CLASSPATH`   | `vs30-classpath`           | packaged OpenSHA `jars/*`                     | Java classpath used for Vs30 lookup.                             |
| `condor_config`            | —          | `CONDOR_CONFIG`               | `condor-config`            | unset                                         | HTCondor configuration path recorded in resolved configuration.  |
| `globus_location`          | —          | `GLOBUS_LOCATION`             | `globus-location`          | unset                                         | Globus installation path recorded in resolved configuration.     |
| `cybershake_version`       | —          | —                             | —                          | installed package version                     | CyberShake version recorded for reproducibility.                 |

`CYBERSHAKE_DB_HOST` or `database-host` overrides the host derived from the
workflow `server` option. Other deployment values remain strings.

Create commands make `output_directory` when needed. Plan and run commands do
not accept `--config`, so set `CYBERSHAKE_OUTPUT_DIRECTORY` for them to use the
same generated-workflow tree:

```sh
export CYBERSHAKE_OUTPUT_DIRECTORY=/scratch2/cybershake/runs
cybershake sgt --config sgt.conf
cybershake sgt-plan SFRH 123 frontera
cybershake sgt-run SFRH 123
```

Relative output and credential paths are resolved from the process working
directory.

## Resolved configuration

`cybershake.config.WORKFLOW_CONFIG_DEFAULTS` is the canonical catalog of keys
and CLI-level defaults for each configurable workflow command.
`cybershake.config.CONFIG_DEFAULTS` combines the union of those workflow keys,
the deployment keys, and `cybershake_version`.

After Click resolves a configurable command, `resolve_config()` returns an
immutable `Config` containing the complete `CONFIG_DEFAULTS` key set. Values for
options that do not apply to the active command remain at their canonical
default, normally `None`. `Config.source` identifies the selected file,
`Config.output_directory` exposes the output root as a `Path`, and
`Config.as_dict()` returns a JSON-serializable copy in which paths are strings.

The SGT, PP, stochastic, and Full main workflows write this complete snapshot
to workflow-level Pegasus metadata for reproducibility. Unset values are stored
there as the string `"None"` so they remain distinct from empty strings.
