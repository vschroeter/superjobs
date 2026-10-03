"""Tests for manifest replay reproduction helpers (issue #27)."""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
RUNNER = REPO_ROOT / "tools" / "verify_manifest_replay_repro.py"

from scripts.dev_check.process_tree import StageProcessResult  # noqa: E402
from tools.manifest_replay_repro_support.broker_probe import attach_broker_snapshots
from tools.manifest_replay_repro_support.extract import (
    count_validation_event_failures,
    failure_rows_from_combinations,
)
from tools.manifest_replay_repro_support.report_paths import (
    legacy_runtime_report_path,
    resolve_fresh_diagnostic_report,
)
from tools.manifest_replay_repro_support.subjects import (
    MANIFEST_JOB_CANONICAL_NAME,
    execution_kv_key,
    observation_subject,
)
from tools.performance_support.contracts import PERFORMANCE_MANIFEST_JOB
import tools.verify_manifest_replay_repro as repro_runner  # noqa: E402
from tools.verify_manifest_replay_repro import EXIT_INFRA, _classify_attempt  # noqa: E402
from superjobs.transport.implementations.nats import NatsQueueConfig


def test_observation_subject_matches_backend_layout() -> None:
    job_id = "150aea80-e2eb-4626-b9c6-3fcbd9f56c65"
    token = hashlib.sha256(job_id.encode("utf-8")).hexdigest()
    subject = observation_subject(job_id)
    assert subject == f"superjobs.obs.{MANIFEST_JOB_CANONICAL_NAME}.{token}"
    assert execution_kv_key(job_id) == token
    assert PERFORMANCE_MANIFEST_JOB.canonical_name == MANIFEST_JOB_CANONICAL_NAME


def test_count_validation_event_failures() -> None:
    report = {
        "combinations": [
            {
                "workload": "manifest",
                "concurrency": 8,
                "samples": [
                    {
                        "failure_diagnostics": [
                            {
                                "phase": "validation_events",
                                "execution_id": "a",
                            },
                            {"phase": "outcome"},
                        ],
                    },
                ],
            },
        ],
    }
    assert count_validation_event_failures(report) == 1
    rows = failure_rows_from_combinations(report["combinations"])
    assert rows[0]["workload"] == "manifest"
    assert rows[0]["sample_index"] == 1


def test_attach_broker_snapshots_merges_per_execution() -> None:
    samples = [
        {
            "failure_diagnostics": [
                {
                    "execution_id": "exec-1",
                    "phase": "validation_events",
                },
            ],
        },
    ]
    broker_payload = {
        "executions": {
            "exec-1": {"observation_subject": "superjobs.obs.example"},
        },
    }
    merged = attach_broker_snapshots(samples, broker_payload)
    row = merged[0]["failure_diagnostics"][0]
    assert row["broker_snapshot"]["observation_subject"] == "superjobs.obs.example"


def test_resolve_fresh_diagnostic_report_accepts_run_token_dir(tmp_path: Path) -> None:
    before = set()
    run_dir = tmp_path / "run-abc123"
    run_dir.mkdir()
    report = run_dir / "diagnostic-report.json"
    report.write_text("{}", encoding="utf-8")
    path, error = resolve_fresh_diagnostic_report(
        tmp_path,
        python_version="3.12",
        run_dirs_before=before,
    )
    assert error is None
    assert path == report


def test_resolve_fresh_diagnostic_report_rejects_legacy_only(tmp_path: Path) -> None:
    legacy = legacy_runtime_report_path(tmp_path, "3.12")
    legacy.parent.mkdir(parents=True)
    legacy.write_text("{}", encoding="utf-8")
    path, error = resolve_fresh_diagnostic_report(
        tmp_path,
        python_version="3.12",
        run_dirs_before=set(),
    )
    assert path is None
    assert error is not None
    assert "stale" in error


def test_classify_attempt_marks_harness_error_without_report() -> None:
    completed = StageProcessResult(
        returncode=1,
        timed_out=False,
        stdout="",
        stderr="wheel build failed",
        duration_seconds=1.0,
        pid=1,
    )
    outcome = _classify_attempt(
        index=1,
        completed=completed,
        duration=1.0,
        report_path=None,
        report_error=None,
    )
    assert outcome.status == "harness_error"
    assert "wheel" in (outcome.error or "")


def test_classify_attempt_marks_stale_report_resolution_as_infra() -> None:
    completed = StageProcessResult(
        returncode=0,
        timed_out=False,
        stdout="",
        stderr="",
        duration_seconds=1.0,
        pid=1,
    )
    outcome = _classify_attempt(
        index=1,
        completed=completed,
        duration=1.0,
        report_path=None,
        report_error="stale evidence: found legacy path",
    )
    assert outcome.status == "error"


def test_classify_attempt_marks_cleanup_errors_as_infra() -> None:
    completed = StageProcessResult(
        returncode=None,
        timed_out=True,
        stdout="",
        stderr="",
        duration_seconds=10.0,
        pid=99,
        cleanup_errors=("communicate after kill timed out",),
    )
    outcome = _classify_attempt(
        index=1,
        completed=completed,
        duration=10.0,
        report_path=None,
        report_error=None,
    )
    assert outcome.status == "error"
    assert "communicate after kill" in (outcome.error or "")


def test_classify_attempt_marks_spawn_failure_as_infra() -> None:
    completed = StageProcessResult(
        returncode=127,
        timed_out=False,
        stdout="",
        stderr="ENOENT: uv",
        duration_seconds=0.1,
        pid=None,
    )
    outcome = _classify_attempt(
        index=1,
        completed=completed,
        duration=0.1,
        report_path=None,
        report_error=None,
    )
    assert outcome.status == "error"
    assert "ENOENT" in (outcome.error or "")


def test_classify_attempt_marks_malformed_report_as_infra(tmp_path: Path) -> None:
    report_path = tmp_path / "diagnostic-report.json"
    report_path.write_text("{not json", encoding="utf-8")
    completed = StageProcessResult(
        returncode=0,
        timed_out=False,
        stdout="",
        stderr="",
        duration_seconds=1.0,
        pid=1,
    )
    outcome = _classify_attempt(
        index=1,
        completed=completed,
        duration=1.0,
        report_path=report_path,
        report_error=None,
    )
    assert outcome.status == "error"
    assert "malformed diagnostic report" in (outcome.error or "")


def test_classify_attempt_marks_infra_timeout() -> None:
    completed = StageProcessResult(
        returncode=None,
        timed_out=True,
        stdout="",
        stderr="",
        duration_seconds=10.0,
        pid=1,
    )
    outcome = _classify_attempt(
        index=1,
        completed=completed,
        duration=10.0,
        report_path=None,
        report_error=None,
    )
    assert outcome.status == "error"


def _clean_performance_result() -> StageProcessResult:
    return StageProcessResult(
        returncode=0,
        timed_out=False,
        stdout="ok",
        stderr="",
        duration_seconds=0.5,
        pid=42,
    )


def _install_fresh_report(attempt_dir: Path) -> Path:
    run_dir = attempt_dir / "run-testtoken"
    run_dir.mkdir(parents=True, exist_ok=True)
    report = run_dir / "diagnostic-report.json"
    report.write_text(json.dumps({"combinations": []}), encoding="utf-8")
    return report


def test_main_spawn_failure_is_fatal_and_summarized(tmp_path: Path) -> None:
    artifact_parent = tmp_path / "artifacts"
    with patch.object(
        repro_runner,
        "_run_performance_diagnostic",
        return_value=StageProcessResult(
            returncode=127,
            timed_out=False,
            stdout="",
            stderr="spawn failed",
            duration_seconds=0.1,
            pid=None,
        ),
    ):
        code = repro_runner.main(
            [
                "--max-attempts",
                "1",
                "--artifact-dir",
                str(artifact_parent),
                "--attempt-timeout-seconds",
                "30",
            ],
        )
    assert code == EXIT_INFRA
    run_dir = next(artifact_parent.glob("run-*"))
    summary = json.loads((run_dir / "reproduction-summary.json").read_text(encoding="utf-8"))
    assert summary["fatal_errors"]
    assert summary["outcomes"][0]["status"] == "error"


def test_main_stale_report_resolution_is_fatal(tmp_path: Path) -> None:
    artifact_parent = tmp_path / "artifacts"
    with patch.object(
        repro_runner,
        "_run_performance_diagnostic",
        return_value=_clean_performance_result(),
    ):
        code = repro_runner.main(
            [
                "--max-attempts",
                "1",
                "--artifact-dir",
                str(artifact_parent),
                "--attempt-timeout-seconds",
                "30",
            ],
        )
    assert code == EXIT_INFRA
    summary = json.loads(
        next(artifact_parent.glob("run-*/reproduction-summary.json")).read_text(
            encoding="utf-8",
        ),
    )
    assert summary["outcomes"][0]["status"] == "error"


def test_main_timeout_is_fatal(tmp_path: Path) -> None:
    artifact_parent = tmp_path / "artifacts"
    with patch.object(
        repro_runner,
        "_run_performance_diagnostic",
        return_value=StageProcessResult(
            returncode=None,
            timed_out=True,
            stdout="",
            stderr="",
            duration_seconds=5.0,
            pid=7,
        ),
    ):
        code = repro_runner.main(
            [
                "--max-attempts",
                "1",
                "--artifact-dir",
                str(artifact_parent),
                "--attempt-timeout-seconds",
                "30",
            ],
        )
    assert code == EXIT_INFRA
    assert "timed out" in (summary_error(tmp_path / "artifacts") or "")


def summary_error(artifact_parent: Path) -> str | None:
    summary = json.loads(
        next(artifact_parent.glob("run-*/reproduction-summary.json")).read_text(
            encoding="utf-8",
        ),
    )
    return summary["outcomes"][0].get("error")


def test_main_malformed_report_is_fatal(tmp_path: Path) -> None:
    artifact_parent = tmp_path / "artifacts"

    def _run_and_seed_report(**_kwargs: object) -> StageProcessResult:
        attempt_dir = _kwargs["artifact_dir"]
        assert isinstance(attempt_dir, Path)
        _install_fresh_report(attempt_dir).write_text("not-json", encoding="utf-8")
        return _clean_performance_result()

    with patch.object(repro_runner, "_run_performance_diagnostic", side_effect=_run_and_seed_report):
        code = repro_runner.main(
            [
                "--max-attempts",
                "1",
                "--artifact-dir",
                str(artifact_parent),
                "--attempt-timeout-seconds",
                "30",
            ],
        )
    assert code == EXIT_INFRA
    assert "malformed diagnostic report" in (summary_error(artifact_parent) or "")


def test_main_two_invocations_do_not_overwrite_summaries(tmp_path: Path) -> None:
    artifact_parent = tmp_path / "shared-root"

    def _run_and_seed_report(**kwargs: object) -> StageProcessResult:
        attempt_dir = kwargs["artifact_dir"]
        assert isinstance(attempt_dir, Path)
        _install_fresh_report(attempt_dir)
        return _clean_performance_result()

    with patch.object(repro_runner, "_run_performance_diagnostic", side_effect=_run_and_seed_report):
        code_one = repro_runner.main(
            [
                "--max-attempts",
                "1",
                "--artifact-dir",
                str(artifact_parent),
                "--attempt-timeout-seconds",
                "30",
            ],
        )
        code_two = repro_runner.main(
            [
                "--max-attempts",
                "1",
                "--artifact-dir",
                str(artifact_parent),
                "--attempt-timeout-seconds",
                "30",
            ],
        )
    assert code_one == 0
    assert code_two == 0
    summaries = sorted(artifact_parent.glob("run-*/reproduction-summary.json"))
    assert len(summaries) == 2
    invocations = {
        json.loads(path.read_text(encoding="utf-8"))["invocation"] for path in summaries
    }
    assert len(invocations) == 2


def test_main_rejects_non_positive_attempt_timeout(tmp_path: Path) -> None:
    with pytest.raises(SystemExit):
        repro_runner.main(
            [
                "--max-attempts",
                "1",
                "--artifact-dir",
                str(tmp_path / "artifacts"),
                "--attempt-timeout-seconds",
                "0",
            ],
        )


def test_repro_runner_help_lists_flags() -> None:
    completed = subprocess.run(
        [sys.executable, str(RUNNER), "--help"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
        creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0,
    )
    assert completed.returncode == 0
    assert "--max-attempts" in completed.stdout
    assert "--skip-contract-typing" in completed.stdout


@pytest.mark.nats
def test_broker_probe_consumers_info_on_real_broker(nats_owned_server) -> None:
    from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy

    import nats

    from tools.manifest_replay_repro_support.broker_probe import collect_broker_snapshots

    _owner, target = nats_owned_server
    config = NatsQueueConfig()
    job_id = "150aea80-e2eb-4626-b9c6-3fcbd9f56c65"
    subject = observation_subject(job_id, queue_config=config)

    async def _setup() -> None:
        connection = await nats.connect(target.url, connect_timeout=5, max_reconnect_attempts=0)
        jetstream = connection.jetstream(timeout=5)
        await jetstream.add_stream(name=config.observation_stream, subjects=[f"{config.subject_prefix}.obs.>"])
        await jetstream.add_consumer(
            config.observation_stream,
            ConsumerConfig(
                durable_name="sj-probe-test",
                filter_subject=subject,
                deliver_policy=DeliverPolicy.ALL,
                ack_policy=AckPolicy.EXPLICIT,
            ),
        )
        await connection.close()

    import asyncio

    asyncio.run(_setup())
    payload = collect_broker_snapshots(target.url, [job_id])
    execution = payload["executions"][job_id]
    assert "consumers_error" not in execution
    matched = execution.get("observation_consumers_for_subject") or []
    assert any(item.get("name") == "sj-probe-test" for item in matched)
