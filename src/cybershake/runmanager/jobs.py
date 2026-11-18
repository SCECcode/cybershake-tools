"""Database-backed commands executed by generated workflows."""

from __future__ import annotations

import fcntl
from pathlib import Path
from typing import TYPE_CHECKING, Literal

from cybershake.exc import RunManagerError

if TYPE_CHECKING:
    from cybershake.runmanager.db import SQLAlchemyRunDB

HandoffStage = Literal["LF", "BB"]
PENDING_PATH = Path("/home/shock/scottcal/runs/Study_24.8_management/pending.txt")
BB_FREQUENCY = 50.0


def insert_velocity_parameters(
    database: SQLAlchemyRunDB, *, input_file: Path, run_id: int
) -> None:
    """Store the four values emitted by velocity_params.py for a run."""
    lines = input_file.read_text(encoding="utf-8").splitlines()
    if len(lines) < 4:
        raise ValueError(f"Expected four velocity values in {input_file}")
    if any("=" not in line for line in lines[:4]):
        raise ValueError(f"Expected key=value velocity data in {input_file}")
    model_vs30, mesh_vsitop, z1_0, z2_5 = [
        float(line.split("=", 1)[1]) for line in lines[:4]
    ]
    if not database.update_run_fields(
        run_id,
        {
            "model_vs30": model_vs30,
            "mesh_vsitop": mesh_vsitop,
            "z1_0": z1_0,
            "z2_5": z2_5,
        },
    ):
        raise RunManagerError(f"Run {run_id} does not exist")


def insert_broadband_velocity_parameters(
    database: SQLAlchemyRunDB,
    *,
    velocity_file: Path,
    broadband_run_id: int,
    low_frequency_run_id: int,
) -> None:
    """Store broadband Vs30 and copy basin depths from the low-frequency run."""
    vs30 = None
    for line in velocity_file.read_text(encoding="utf-8").splitlines():
        key, separator, value = line.partition("=")
        if separator and key.strip() == "Vs30":
            vs30 = float(value)
            break
    if vs30 is None or vs30 < 0:
        raise ValueError(f"Vs30 not found in velocity info file {velocity_file}")
    if not database.copy_broadband_velocity_parameters(
        low_frequency_run_id, broadband_run_id, model_vs30=vs30
    ):
        raise RunManagerError(f"Run {broadband_run_id} does not exist")


def handoff(
    database: SQLAlchemyRunDB,
    *,
    run_id: int,
    stage: HandoffStage,
    pending_path: Path = PENDING_PATH,
) -> None:
    """Queue the next study stage while holding the pending-file lock."""
    run = database.get_run(run_id)
    if run is None:
        raise RunManagerError(f"Run {run_id} does not exist")
    site = database.get_site_by_id(run.site_id)
    if site is None:
        raise RunManagerError(f"Run {run_id} references missing site {run.site_id}")
    if run.low_frequency_cutoff is None or run.sgt_source_filter_frequency is None:
        raise RunManagerError(f"Run {run_id} lacks required frequency values")
    low_frequency = run.low_frequency_cutoff
    maximum = low_frequency if run.max_frequency is None else run.max_frequency
    prefix = (
        f"{site.cs_short_name} {run.erf_id} {run.rup_var_scenario_id} "
        f"{run.sgt_variation_id} {run.velocity_model_id}"
    )
    if stage == "LF":
        entry = f"PP {prefix} {maximum:f} {run.sgt_source_filter_frequency:f}"
        if low_frequency != maximum:
            entry += " -hf"
    else:
        entry = (
            f"stoch {prefix} {low_frequency:f} "
            f"{run.sgt_source_filter_frequency:f} {BB_FREQUENCY:f}"
        )

    with pending_path.open("r+", encoding="utf-8") as pending:
        fcntl.lockf(pending, fcntl.LOCK_EX)
        try:
            lines = pending.readlines()
            index = 0
            while index < len(lines) and lines[index].split()[0] == "PP":
                index += 1
            if stage == "BB":
                while index < len(lines) and lines[index].split()[0] == "stoch":
                    index += 1
            lines.insert(index, entry + "\n")
            pending.seek(0)
            pending.writelines(lines)
            pending.truncate()
            pending.flush()
        finally:
            fcntl.lockf(pending, fcntl.LOCK_UN)
