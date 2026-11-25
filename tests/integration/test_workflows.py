"""End-to-end workflow-generation tests using the seeded test database."""

from __future__ import annotations

import re
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
import yaml
from click.testing import CliRunner

from cybershake.__main__ import cybershake
from cybershake.vs30 import Vs30

if TYPE_CHECKING:
    from pytest_mock import MockerFixture

    from cybershake.runmanager.db import SQLAlchemyRunDB


def _deterministic_args(
    command: str,
    *,
    erf_id: int,
    rupture_variation_id: int,
    sgt_variation_id: int,
    frequency: float = 1.0,
    source_frequency: float = 2.0,
) -> tuple[str, ...]:
    """Build common SGT, PP, and Full command arguments."""
    return (
        command,
        "--site",
        "TEST",
        "--velocity-model",
        "v246",
        "--erf-id",
        str(erf_id),
        "--rupture-variation-id",
        str(rupture_variation_id),
        "--sgt-variation-id",
        str(sgt_variation_id),
        "--frequency",
        str(frequency),
        "--source-frequency",
        str(source_frequency),
    )


def _assert_inline_catalogs(directory: Path) -> None:
    """Check every generated workflow has the shared catalog."""
    workflow_files = list(directory.rglob("*.dax"))
    assert workflow_files
    for workflow_file in workflow_files:
        workflow = yaml.safe_load(workflow_file.read_text(encoding="utf-8"))
        assert len(workflow["transformationCatalog"]["transformations"]) == 12


_SGT_WORKFLOW = "TEST_SGT_dax/run_*/CyberShake_SGT_TEST.dax"
_PP_WORKFLOW = "TEST_PP_dax/run_*/CyberShake_TEST.dax"
_STOCH_WORKFLOW = "TEST_Stoch_dax/run_*/CyberShake_Stoch_TEST_top.dax"
_FULL_WORKFLOW = "TEST_Integrated_dax/run_*/CyberShake_Integrated_TEST.dax"
_RUPTURE_LIST = str(Path(__file__).with_name("test-ruptures.csv"))

_CASES = (
    pytest.param(
        _deterministic_args(
            "sgt", erf_id=64, rupture_variation_id=5, sgt_variation_id=5
        ),
        _SGT_WORKFLOW,
        id="sgt-rwg",
    ),
    pytest.param(
        _deterministic_args(
            "sgt", erf_id=64, rupture_variation_id=6, sgt_variation_id=6
        ),
        _SGT_WORKFLOW,
        id="sgt-awp-cpu",
    ),
    pytest.param(
        _deterministic_args(
            "sgt",
            erf_id=64,
            rupture_variation_id=7,
            sgt_variation_id=7,
            frequency=0.5,
            source_frequency=1.0,
        ),
        _SGT_WORKFLOW,
        id="sgt-rwg-3-0-3",
    ),
    pytest.param(
        _deterministic_args(
            "sgt", erf_id=64, rupture_variation_id=10, sgt_variation_id=8
        ),
        _SGT_WORKFLOW,
        id="sgt-awp-gpu",
    ),
    *(
        pytest.param(
            (
                *_deterministic_args(
                    "pp",
                    erf_id=erf_id,
                    rupture_variation_id=scenario_id,
                    sgt_variation_id=sgt_variation_id,
                    frequency=frequency,
                    source_frequency=source_frequency,
                ),
                "--db-rv-seed",
            ),
            _PP_WORKFLOW,
            id=case_id,
        )
        for (
            case_id,
            erf_id,
            scenario_id,
            sgt_variation_id,
            frequency,
            source_frequency,
        ) in (
            ("pp-direct-scenario-5", 64, 5, 5, 1.0, 2.0),
            ("pp-direct-scenario-6", 64, 6, 6, 1.0, 2.0),
            ("pp-direct-scenario-7", 64, 7, 7, 0.5, 1.0),
            ("pp-rsqsim-erf-60", 60, 8, 8, 1.0, 2.0),
            ("pp-rsqsim-erf-62", 62, 8, 8, 1.0, 2.0),
            ("pp-rsqsim-erf-63", 63, 8, 8, 1.0, 2.0),
            ("pp-rsqsim-default-erf", 64, 8, 8, 1.0, 2.0),
            ("pp-direct-scenario-9", 64, 9, 8, 1.0, 2.0),
            ("pp-direct-scenario-10", 64, 10, 8, 1.0, 2.0),
        )
    ),
    pytest.param(
        (
            "stoch",
            "--site",
            "TEST",
            "--low-frequency-run-id",
            "900008",
            "--erf-id",
            "64",
            "--rupture-variation-id",
            "10",
            "--merge-frequency",
            "1.0",
            "--stoch-frequency",
            "50.0",
            "--db-rv-seed",
        ),
        _STOCH_WORKFLOW,
        id="stoch-standard",
    ),
    pytest.param(
        (
            "stoch",
            "--site",
            "TEST",
            "--low-frequency-run-id",
            "900860",
            "--erf-id",
            "60",
            "--rupture-variation-id",
            "8",
            "--merge-frequency",
            "1.0",
            "--stoch-frequency",
            "50.0",
            "--db-rv-seed",
        ),
        _STOCH_WORKFLOW,
        id="stoch-rsqsim",
    ),
    pytest.param(
        (
            *_deterministic_args(
                "full", erf_id=64, rupture_variation_id=10, sgt_variation_id=8
            ),
            "--db-rv-seed",
        ),
        _FULL_WORKFLOW,
        id="full-deterministic",
    ),
    pytest.param(
        (
            *_deterministic_args(
                "full", erf_id=64, rupture_variation_id=10, sgt_variation_id=8
            ),
            "--broadband",
            "--stoch-frequency",
            "50.0",
            "--force-vs30",
            "760.0",
            "--db-rv-seed",
        ),
        _FULL_WORKFLOW,
        id="full-standard-broadband",
    ),
    pytest.param(
        (
            *_deterministic_args(
                "full", erf_id=60, rupture_variation_id=8, sgt_variation_id=8
            ),
            "--broadband",
            "--stoch-frequency",
            "50.0",
            "--force-vs30",
            "760.0",
            "--db-rv-seed",
        ),
        _FULL_WORKFLOW,
        id="full-rsqsim-broadband",
    ),
    pytest.param(
        (
            *_deterministic_args(
                "pp", erf_id=64, rupture_variation_id=10, sgt_variation_id=8
            ),
            "--db-rv-seed",
            "--no-insert",
            "--nonblocking-md5",
            "--pp-site",
            "shock",
            "--handoff",
            "--dir-hierarchy",
            "--rupture-list",
            _RUPTURE_LIST,
            "--z-comp",
        ),
        _PP_WORKFLOW,
        id="pp-no-insert-hierarchy",
    ),
    pytest.param(
        (
            *_deterministic_args(
                "pp", erf_id=64, rupture_variation_id=10, sgt_variation_id=8
            ),
            "--db-rv-seed",
            "--rotd",
            "--duration",
            "--z-comp",
        ),
        _PP_WORKFLOW,
        id="pp-products",
    ),
    pytest.param(
        (
            "stoch",
            "--site",
            "TEST",
            "--low-frequency-run-id",
            "900008",
            "--erf-id",
            "64",
            "--rupture-variation-id",
            "10",
            "--merge-frequency",
            "1.0",
            "--stoch-frequency",
            "50.0",
            "--db-rv-seed",
            "--no-rotd",
            "--no-duration",
            "--no-low-site-response",
            "--rupture-list",
            _RUPTURE_LIST,
            "--z-comp",
        ),
        _STOCH_WORKFLOW,
        id="stoch-no-products-rupture-list",
    ),
    pytest.param(
        (
            *_deterministic_args(
                "full", erf_id=64, rupture_variation_id=5, sgt_variation_id=5
            ),
            "--broadband",
            "--stoch-frequency",
            "50.0",
            "--force-vs30",
            "760.0",
            "--db-rv-seed",
            "--no-insert",
        ),
        _FULL_WORKFLOW,
        id="full-rwg-extract-no-insert-broadband",
    ),
    pytest.param(
        (
            *_deterministic_args(
                "pp", erf_id=64, rupture_variation_id=10, sgt_variation_id=8
            ),
            "--db-rv-seed",
            "--rotd",
            "--duration",
            "--rupture-list",
            _RUPTURE_LIST,
            "--z-comp",
        ),
        _PP_WORKFLOW,
        id="pp-products-rupture-list",
    ),
)


@pytest.mark.parametrize(("args", "workflow_glob"), _CASES)
def test_workflow_generation(
    args: tuple[str, ...],
    workflow_glob: str,
    db: SQLAlchemyRunDB,
    cwd: Path,
    monkeypatch: pytest.MonkeyPatch,
    mocker: MockerFixture,
) -> None:
    """Generate a real workflow through the CLI using only the seeded DB."""
    monkeypatch.setenv("CYBERSHAKE_OUTPUT_DIRECTORY", str(cwd))
    mocker.patch("cybershake.runmanager.connect", return_value=db)
    mocker.patch("cybershake.runmanager.db.connect", return_value=db)
    mocker.patch("cybershake.runmanager._current_user", return_value="mayani")
    mocker.patch(
        "cybershake.workflow.sgt.lookup_vs30",
        return_value=Vs30(578.23, "Thompson et al. (2020)"),
    )

    result = CliRunner().invoke(cybershake, list(args))

    assert result.exit_code == 0, result.output
    assert "✓ Created" in result.output
    assert len(list(cwd.glob(workflow_glob))) == 1
    _assert_inline_catalogs(cwd)
    if args[0] == "full" and "--force-vs30" in args:
        match = re.search(r"run=(\d+) broadband_run=(\d+)", result.output)
        assert match is not None
        for run_id in map(int, match.groups()):
            run = db.get_run(run_id)
            assert run is not None
            assert run.target_vs30 == 760.0
            assert run.vs30_source == "User-provided"


@pytest.mark.parametrize(
    ("run_id", "flags", "expected", "absent"),
    [
        pytest.param(
            900006,
            ("--no-smoothing",),
            ("AWP_TEST_x",),
            ("Smooth_TEST", "Handoff", "MD5_x"),
            id="sub-sgt-awp-cpu",
        ),
        pytest.param(
            900008,
            ("--separate-md5", "--handoff", "--local-vparam-insert", "--z-comp"),
            (
                "AWP_GPU_TEST_z",
                "Smooth_TEST",
                "MD5_z",
                "InsertVelocityParams_TEST",
                "Handoff",
            ),
            (),
            id="sub-sgt-awp-gpu",
        ),
    ],
)
def test_deferred_sgt_workflow_generation(
    run_id: int,
    flags: tuple[str, ...],
    expected: tuple[str, ...],
    absent: tuple[str, ...],
    db: SQLAlchemyRunDB,
    cwd: Path,
    mocker: MockerFixture,
) -> None:
    """Generate AWP runtime workflows from a synthetic PreCVM grid file."""
    grid = cwd / "gridout_TEST"
    grid.write_text(
        "gridout\nnx=100\n" + "0\n" * 101 + "ny=100\n" + "0\n" * 101 + "nz=50\n",
        encoding="utf-8",
    )
    mocker.patch("cybershake.runmanager.db.connect", return_value=db)

    result = CliRunner().invoke(
        cybershake,
        [
            "sub-sgt-wf",
            "--run-id",
            str(run_id),
            "--grid-output-file",
            grid.name,
            "--workflow-file",
            "AWP_SGT_TEST.dax",
            "--server",
            "moment",
            *flags,
        ],
    )

    assert result.exit_code == 0, result.output
    _assert_inline_catalogs(cwd)
    workflow = yaml.safe_load((cwd / "AWP_SGT_TEST.dax").read_text(encoding="utf-8"))
    jobs = {job["id"] for job in workflow["jobs"]}
    assert set(expected) <= jobs
    assert not set(absent) & jobs


@pytest.mark.parametrize(
    ("run_id", "low_id", "flags", "source_id", "low_response", "rotd"),
    [
        pytest.param(
            901010,
            900008,
            ("--db-rv-seed", "--z-comp", "--period-duration"),
            0,
            True,
            True,
            id="sub-stoch-standard",
        ),
        pytest.param(
            901860,
            900860,
            (
                "--db-rv-seed",
                "--no-rotd",
                "--no-duration",
                "--no-site-response",
                "--no-low-site-response",
            ),
            9000,
            False,
            False,
            id="sub-stoch-rsqsim",
        ),
    ],
)
def test_deferred_stoch_workflow_generation(
    run_id: int,
    low_id: int,
    flags: tuple[str, ...],
    source_id: int,
    low_response: bool,
    rotd: bool,
    db: SQLAlchemyRunDB,
    cwd: Path,
    mocker: MockerFixture,
) -> None:
    """Generate per-rupture workflows from synthetic Velocity_Info output."""
    velocity = cwd / "velocity_info_TEST.txt"
    velocity.write_text(
        "Vs at site = 760\nVs at 5H = 800\nVs at D+5H = 900\n",
        encoding="utf-8",
    )
    mocker.patch("cybershake.runmanager.db.connect", return_value=db)

    result = CliRunner().invoke(
        cybershake,
        [
            "sub-stoch-wf",
            "--run-id",
            str(run_id),
            "--low-frequency-run-id",
            str(low_id),
            "--velocity-file",
            velocity.name,
            "--workflow-file",
            "CyberShake_Stoch_TEST.dax",
            "--server",
            "moment",
            *flags,
        ],
    )

    assert result.exit_code == 0, result.output
    _assert_inline_catalogs(cwd)
    workflow = yaml.safe_load(
        (cwd / "CyberShake_Stoch_TEST.dax").read_text(encoding="utf-8")
    )
    jobs = {job["id"]: job for job in workflow["jobs"]}
    assert f"HF_Synth_{source_id}_0" in jobs
    assert (f"LF_Site_Response_{source_id}_0" in jobs) == low_response
    merge = jobs[f"Merge_IM_{source_id}_0"]
    assert f"run_rotd={int(rotd)}" in merge["arguments"]
