"""Normalize Pegasus workflow YAML so legacy (Java) and Python output compare.

Dropped as not meaningful: writer metadata and timestamps; ``stageOut`` /
``registerReplica`` on inputs (the Python API can't set them and the planner
ignores them); ``forPlanning`` and the input a sub-workflow job has on its own
file (added by the Python API); empty ``uses`` and ``arguments``; ordering of
jobs, uses, replicas and dependencies.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import yaml

if TYPE_CHECKING:
    from pathlib import Path

    from Pegasus.api import Workflow

from cybershake.workflow.common import write_workflow


def load_workflow(workflow: Workflow, tmp_path: Path) -> dict[str, Any]:
    """Write a workflow and return its normalized form."""
    path = tmp_path / "wf.dax"
    write_workflow(workflow, path)
    return normalize(path)


def _use(use: dict[str, Any]) -> tuple[Any, ...]:
    if use["type"] == "input":
        return (use["lfn"], "input")
    return (
        use["lfn"],
        use["type"],
        use.get("stageOut", True),
        use.get("registerReplica", True),
    )


def _job(job: dict[str, Any]) -> dict[str, Any]:
    out = {
        k: v
        for k, v in job.items()
        if k not in ("uses", "arguments", "profiles") and v not in (None, [], {})
    }
    for key in ("arguments", "profiles"):
        if job.get(key):
            out[key] = job[key]
    out["uses"] = sorted(
        _use(u) for u in job.get("uses", []) if u["lfn"] != job.get("file")
    )
    return out


def normalize(path: Path, mask: dict[str, str] | None = None) -> dict[str, Any]:
    """Load a workflow file and return its comparable form.

    Args:
        path: Workflow file.
        mask: Text replacements applied to the file first, e.g. to hide the
            directory it was generated in.
    """
    text = path.read_text()
    for old, new in (mask or {}).items():
        text = text.replace(old, new)
    data = yaml.safe_load(text)
    replicas = {
        r["lfn"]: sorted((p["site"], p["pfn"]) for p in r["pfns"])
        for r in data.get("replicaCatalog", {}).get("replicas", [])
    }
    return {
        "name": data["name"],
        "jobs": {job["id"]: _job(job) for job in data["jobs"]},
        "dependencies": {
            d["id"]: sorted(d["children"]) for d in data.get("jobDependencies", [])
        },
        "replicas": replicas,
    }
