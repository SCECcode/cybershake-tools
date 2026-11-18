# AGENTS.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

# MIGRATION

We are migrating the code to a pure Python package. The old code is in ../cybershake-tools/.claude/worktrees/main

## Map of what migrates to where.

scripts/create_full_wf.sh → src/cybershake/__main__.py:full → src/cybershake/workflow/
scripts/create_pp_wf.sh → src/cybershake/__main__.py:pp → src/cybershake/workflow/
scripts/create_sgt_dax.sh → src/cybershake/__main__.py:sgt → src/cybershake/workflow/
scripts/create_stoch_wf.sh → src/cybershake/__main__.py:stoch → src/cybershake/workflow/
scripts/generate_sgt_dax.sh → src/cybershake/__main__.py:sub_sgt_wf → src/cybershake/workflow/
scripts/generate_sub_stoch_dax.sh → src/cybershake/__main__.py:sub_stoch_wf → src/cybershake/workflow/

scripts/plan_full.sh → src/cybershake/__main__.py:full_plan
scripts/plan_pp.sh → src/cybershake/__main__.py:pp_plan
scripts/plan_sgt.sh → src/cybershake/__main__.py:sgt_plan
scripts/plan_stoch.sh → src/cybershake/__main__.py:stoch_plan

scripts/run_full.sh → src/cybershake/__main__.py:full_run
scripts/run_pp.sh → src/cybershake/__main__.py:pp_run
scripts/run_sgt.sh → src/cybershake/__main__.py:sgt_run
scripts/run_stoch.sh → src/cybershake/__main__.py:stoch_run

runmanager/create_run.py → src/cybershake/runmanager/__init__.py:create
runmanager/create_stoch_run.py → src/cybershake/runmanager/__init__.py:create_stoch
runmanager/edit_run.py → src/cybershake/__main__.py:edit → src/cybershake/runmanager/__init__.py:edit
runmanager/valid_run.py → src/cybershake/runmanager/__init__.py:valid
runmanager/find_run.py → src/cybershake/runmanager/__init__.py:find
runmanager/find_stoch_run.py → src/cybershake/runmanager/__init__.py:find_stoch
runmanager/RunManager/UpdateRunState.py → src/cybershake/__main__.py:update → src/cybershake/runmanager/__init__.py
runmanager/RunManager/RC.py → src/cybershake/runmanager/rc.py

runmanager/cron_wrapper.sh → src/cybershake/__main__.py:cron_run_check
runmanager/cron_run_check.py → src/cybershake/runmanager/__init__.py:cron_run_check

## CLI argument migration

This table lists changed or newly added spellings for the create and run commands
and deferred workflow generators.
Unlisted arguments are unchanged. Except where an argument is marked removed,
legacy short spellings remain supported aliases.

| Workflow(s)                              | Old argument                   | New argument                                      |
| ---------------------------------------- | ------------------------------ | ------------------------------------------------- |
| SGT, PP, Stoch, Full, Sub-SGT, Sub-Stoch | none                           | `-c`, `--config`                                  |
| SGT, PP, Stoch, Full                     | `-e`                           | `-e`, `--erf-id`                                  |
| SGT, PP, Stoch, Full                     | `-r`                           | `-r`, `--rupture-variation-id`                    |
| SGT, PP, Full                            | `-f`                           | `-f`, `--frequency`                               |
| SGT, PP, Stoch, Full                     | `-s`                           | `-s`, `--site`                                    |
| SGT, PP, Full                            | `-v`                           | `-v`, `--velocity-model`                          |
| SGT, PP, Full                            | `-g`                           | `-g`, `--sgt-variation-id`                        |
| SGT, PP, Full                            | `-q`                           | `-q`, `--source-frequency`                        |
| Stoch                                    | `-f`                           | `--merge-frequency`                               |
| Stoch                                    | `-l`                           | `-l`, `--low-frequency-run-id`                    |
| SGT, PP, Sub-SGT                         | `-sp`                          | `-sp`, `--spacing`                                |
| Full (SGT)                               | `--sgtargs -sp`                | `--sgt-spacing`                                   |
| Full (PP)                                | `--ppargs -sp`                 | `--pp-spacing`                                    |
| SGT, Full                                | `-ss`                          | `-ss`, `--sgt-site`                               |
| SGT, Full, Sub-SGT                       | `-mv`                          | `-mv`, `--min-vs`                                 |
| SGT, Full                                | `-dep`                         | `-dep`, `--depth`                                 |
| PP, Full                                 | `-ps`                          | `-ps`, `--pp-site`                                |
| PP, Full                                 | `-sl`                          | `-sl`, `--seismogram-length`                      |
| PP, Stoch, Full, Sub-Stoch               | `-rl`                          | `-rl`, `--rupture-list`                           |
| Stoch, Full, Sub-Stoch                   | `-mf`, `--merge_frequency`     | `-mf`, `--generator-merge-frequency`              |
| SGT, PP, Stoch, Full, Sub-SGT, Sub-Stoch | `-z`, `--z_comp`               | `-z`, `--z-comp`                                  |
| SGT, Stoch, Full, Sub-SGT                | `-hf`, `--h_fraction`          | `-hf`, `--h-fraction`                             |
| SGT, Full, Sub-SGT                       | `-lv`, `--local_vparam_insert` | `-lv`, `--local-vparam-insert`                    |
| Stoch, Full                              | `-sf`, `--stoch_frequency`     | `-sf`, `--stoch-frequency`                        |
| Stoch, Full                              | `-fvs`, `--force_vs30`         | `-fvs`, `--force-vs30`                            |
| Stoch, Full, Sub-Stoch                   | `-hfv`, `--hf_vel_model`       | `-hfv`, `--hf-vel-model`                          |
| PP, Full (RotD)                          | forwarded `-r`                 | `--rotd`                                          |
| Full (PP high frequency)                 | forwarded `-hf`                | `--high-frequency`                                |
| SGT, Full                                | `--sgtargs`                    | removed; options are flat                         |
| PP, Full                                 | `--ppargs`                     | removed; options are flat                         |
| Stoch                                    | `--daxargs`                    | removed; options are flat                         |
| Full                                     | `--bbargs`                     | removed; options are flat                         |
| Sub-SGT, Sub-Stoch                       | `-r`                           | `-r`, `--run-id`                                  |
| Sub-SGT                                  | `-gf`                          | `-gf`, `--grid-output-file`                       |
| Sub-SGT, Sub-Stoch                       | `-o`                           | `-o`, `--workflow-file`                           |
| Sub-Stoch                                | `-lr`                          | `-lr`, `--low-frequency-run-id`                   |
| Sub-Stoch                                | `-v`                           | `-v`, `--velocity-file`                           |
| Run-SGT, Run-PP, Run-Stoch, Run-Full     | `-r`                           | `-r`, `--restart`                                 |
| Run-SGT, Run-PP, Run-Stoch, Run-Full     | `-n`                           | `-n`, `--notify-user`                             |
| Run-SGT, Run-Full                        | `-t`                           | removed; multi-site timestamp mode is unsupported |
| Plan-SGT, Plan-PP, Plan-Stoch, Plan-Full | hardcoded properties file      | `-p`, `--properties`                              |

## Remember

Old code was Java, so python code was invoked as CLI. New one is Python, so migrate those parts as direct calls instead of via subprocesses.
Extract constants from the above and put them in src/cybershake/constants.py
Reusable option groups should be extracted from the above and put them in src/cybershake/options.py
pegasus-plan/pegasus-run should be extracted into src/cybershake/pegasus.py
Define exceptions in `src/cybershake/exc.py`.
Add a --config option to all create\_\* scripts. It has no default; when provided,
it must be an existing file, and it may be empty.
In RunIDQuery, the OpenSHA code is extracted in `src/cybershake/vs30.py`, use it directly.
DO NOT MIGRATE conwatch/\*
DO NOT create unnecessary Python `dataclasses`.
Database queries should be replaced with sqlalchemy. Schema in `src/cybershake/runmanager/schema.py`.
All python code in this repo should be type-annotated. Use minimal noqa comments.
Avoid using word dax. Use workflow or wf instead. Avoid using word dax. Use workflow or wf instead.
No need to support multi-site runs.

## Verification

Run

```sh
./check.sh
./e2e.sh
```

Compare results against.
../cybershake-tools/.claude/worktrees/main/goldens/

The following differences may be ignored during golden comparison, provided the
normalized values are otherwise identical:

- generated run IDs and absolute generation-directory prefixes;
- YAML formatting, quoting, writer metadata, and timestamps;
- resolved-configuration metadata added to main workflows for reproducibility;
- serialized ordering of jobs, file uses, replicas, and dependencies;
- omitted `stageOut`/`registerReplica` fields on inputs, Python API
  `forPlanning` fields, a sub-workflow's input use of its own file, and empty
  `uses` or `arguments` collections;
- tokenized Python argument lists versus equivalent legacy combined arguments;
- migrated deferred-command names and kebab-case options, such as
  `cybershake sub-sgt-wf` and `cybershake sub-stoch-wf`;
- a missing `file://` prefix when the resolved local PFN is identical;
- absent Globus resource profiles when the same job has equivalent Pegasus
  profiles: `globus.maxruntime` or `globus.maxWallTime` (minutes) to
  `pegasus.runtime` (seconds),
  `globus.hostcount` or `globus.host_count` to `pegasus.nodes`, and
  `globus.count` to `pegasus.cores`;
- ordering of DirectSynth `key=value` arguments; and
- redundant or trailing path separators that resolve to the same path.

Do not ignore other differences. In particular, execution topology and
dependencies, job IDs, argument values, file contents, replica site or protocol,
credential paths, handoff behavior, and database changes must match or be
explicitly resolved as migration decisions.

# END MIGRATION

## Overview

Part of SCEC CyberShake (seismic hazard analysis). Tools for creating, planning, and running CyberShake Pegasus workflows. Installed on a workflow **submission host** (not a cluster). Docs: https://strike.scec.org/scecpedia/CyberShake_Workflow_Framework

- Python package `cybershake` in `src/` (Java >= 11, Python >=3.11; uv, tox, ruff, pytest, mypy, pre-commit).

## CLI (`src/cybershake`)

```bash
# SGT invocation
cybershake sgt -s <site> -v v246 -e 64 -r 10 -g 8 -f 1.0 -q 2.0 --server moment.usc.edu -dep 50.0 -hf 0.25 -mv 400.0 -rt "-36.0" -et ifless -td 700.0 -tm sfcvm,cca,bbp1d -sp 0.08 -z -hd -lv
cybershake sgt-plan <site> <run-id> <remote-site> -p conf/properties.sgt
cybershake sgt-run <site> <run-id>

# PP invocation
cybershake pp -s <site> -v v246 -e 64 -r 10 -g 8 -f 1.0 -q 2.0 -nb --rotd -du -dbrs -sp 0.08 -z -hd -pd
cybershake pp-plan <site> <run-id> <remote-site> -p conf/properties.mpi-cluster.pp
cybershake pp-run <site> <run-id>

# Stochastic invocation
cybershake stoch -s <site> -l <low-frequency run ID> -e 64 -r 10 --merge-frequency 1.0 --h-fraction 0.25 -sf 50.0 -dbrs -nls -pd --hf-vel-model nocal -z
cybershake stoch-plan <site> <run-id> <remote-site> -p conf/properties.full
cybershake stoch-run <site> <run-id>

# Full invocation
cybershake full -s <site> -v v246 -e 64 -r 10 -g 8 -f 1.0 -q 2.0 -bb --pp-spacing 0.08 --h-fraction 0.25 -mf 1.0 -sf 50.0 -dbrs -nls -pd --hf-vel-model nocal -z
cybershake full-plan <site> <run-id> <remote-site> -p conf/properties.full
cybershake full-run <site> <run-id>
```

## Dev commands

```bash
uv sync                          # install deps
uvx tox                          # pytest, 75% coverage floor (tests in tests/)
uvx tox -e docs                  # Sphinx; must be warning-free
uvx pre-commit run --all-files   # ruff, mypy, mdformat, codespell, shellcheck
```

### Running a single test / lint

```bash
uvx tox -e py312 -- tests/sgt/test_commands.py::test_name   # posargs replace default `tests`
uvx tox -e lint                                              # ruff check + format --diff
```

- Tox matrix py311–py315; 75% coverage floor.
- Ruff: line length 88, Google docstrings; tests ignore `S101`/`D101`/`D103`/`D104`.
- Versioning via commitizen (Conventional Commits); version mirrored in `pyproject.toml`, `src/cybershake/version.py`, `docs/conf.py`.

## Architecture

- Entry point `cybershake.__main__:cybershake` (rich-click group). Each workflow type uses `<type>` for creation, plus `<type>-plan` and `<type>-run`. Also `cron-run-check` and a `run-manager` group.
- `src/cybershake/workflow/{common,sgt,pp,stoch,full}.py`: Pegasus workflow generators (`pegasus-wms.api>=5.1.3`).
- `src/cybershake/config.py`: immutable, serializable resolved configuration.
  Flat `--config` files supply active-command option defaults; deployment values
  use environment variables. Every resolved object retains the complete key set.
- `src/cybershake/runmanager/schema.py`: SQLAlchemy models for the CyberShake DB (MySQL via pymysql).
- Console scripts `cybershake sub-sgt-wf` / `cybershake sub-stoch-wf` run on the submit host as jobs *inside* the SGT/stoch workflows.
- `conf/`: legacy Pegasus properties, site/transformation/replica catalogs used by plan steps.

## Current state caveats

- Production-service golden comparisons and submission-host smoke tests remain
  pending; the mapped Python command and workflow implementations are no longer
  stubs.
