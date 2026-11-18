"""Repository-backed Pegasus transformation catalog entries."""

import sysconfig
from importlib.metadata import distribution
from pathlib import Path

import yaml

from Pegasus.api import Workflow

from cybershake.workflow.common import (
    transformation_catalog,
    update_job,
    write_workflow,
)


def test_transformation_catalog_covers_repository_runtime_commands() -> None:
    catalog = transformation_catalog()
    entry_point = next(
        entry
        for entry in distribution("cybershake-tools").entry_points
        if entry.group == "console_scripts" and entry.name == "cybershake"
    )
    console_script = str(
        (Path(sysconfig.get_path("scripts")) / entry_point.name).resolve(strict=True)
    )
    expected = {
        "GenSGTDax": console_script,
        "GenStochDax": console_script,
        "UpdateRun": console_script,
        "Handoff": console_script,
        "InsertVelocityParams": console_script,
        "BB_Velocity_Insert": console_script,
        "Check_DB_Site": console_script,
        "Curve_Calc": console_script,
        "Curve_Calc_Wrapper": console_script,
        "Disaggregate": console_script,
        "SetJobID": "get_condor_id.sh",
        "Load_Amps": "insert_dir.sh",
    }

    assert set(catalog.transformations) == {f"scec::{name}::1.0" for name in expected}
    for name, executable in expected.items():
        transformation = catalog.transformations[f"scec::{name}::1.0"]
        assert set(transformation.sites) == {"local", "shock"}
        for site in transformation.sites.values():
            if executable == console_script:
                assert site.pfn == executable
            else:
                assert Path(site.pfn).name == executable
                assert (
                    Path(site.pfn).parent
                    == Path(sysconfig.get_path("scripts")).resolve()
                )
                assert Path(site.pfn).is_file()

    update = catalog.transformations["scec::UpdateRun::1.0"]
    assert update.profiles["pegasus"]["clusters.size"] == 1


def test_write_workflow_embeds_transformation_catalog(tmp_path: Path) -> None:
    workflow_file = tmp_path / "workflow.yml"
    write_workflow(Workflow("test", infer_dependencies=False), workflow_file)

    workflow = yaml.safe_load(workflow_file.read_text(encoding="utf-8"))
    assert len(workflow["transformationCatalog"]["transformations"]) == 12


def test_update_job_invokes_console_entrypoint() -> None:
    node = update_job(123, "SGT_INIT", "SGT_START")
    assert node.args == [
        "run-manager",
        "update",
        "123",
        "SGT_INIT",
        "SGT_START",
    ]
