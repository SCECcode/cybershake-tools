# AGENTS.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

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
