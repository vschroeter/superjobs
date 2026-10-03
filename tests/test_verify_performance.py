"""Deterministic checks for performance accounting and verify_performance harness."""

from __future__ import annotations

import asyncio
import importlib.util
import json
import subprocess
import sys
import time
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
RUNNER = REPO_ROOT / "tools" / "verify_performance.py"
SUPPORT_ROOT = REPO_ROOT / "tools" / "performance_support"

from tools.performance_support.accounting import (  # noqa: E402
    CompletionRecord,
    SampleAccounting,
    SampleWindow,
    aggregate_exit_code,
    classify_terminal_time,
    percentile_summary,
    producer_exit_code,
    throughput_variation,
)
from tools.performance_support.config import (  # noqa: E402
    BASELINE_SAMPLE_COUNT,
    EXIT_INCOMPLETE_DRAIN,
    EXIT_OK,
    EXIT_VALIDATION,
    MIN_SAMPLES_FOR_PERCENTILES,
)
from tools.performance_support.fixtures import (  # noqa: E402
    measure_fixture_byte_range,
    measure_fixture_sizes,
)
from tools.performance_support.reporting import (  # noqa: E402
    MANIFEST_RELIABILITY_ISSUE_URL,
    build_report_payload,
    compare_reports,
    finalize_run_outcome,
    normalized_comparison_environment,
)
from tools.verify_cross_program import _write_evidence  # noqa: E402
from tools.verify_contract_typing import runtime_evidence_tag  # noqa: E402
from tools.performance_support.scheduling import (  # noqa: E402
    SampleRunConfig,
    acquire_slot_before,
    run_sample,
)
from tools.performance_support.measurement_clock import metadata as measurement_clock_metadata  # noqa: E402
from tools.performance_support.metadata import (  # noqa: E402
    broker_storage_for_path,
    probe_measurement_clocks,
    probe_role_dependency_versions,
)
from tools import verify_performance as vp  # noqa: E402


def test_classify_terminal_time_uses_fixed_submission_boundary() -> None:
    assert classify_terminal_time(10.0, 10.0) == "success_in_window"
    assert classify_terminal_time(10.1, 10.0) == "success_late_drain"


def test_throughput_counts_only_in_window_successes() -> None:
    window = SampleWindow(
        submission_start_mono=0.0,
        submission_end_mono=10.0,
        measured_submission_end_mono=10.05,
        submission_loop_stop_mono=9.95,
    )
    accounting = SampleAccounting(window=window)
    accounting.add(
        CompletionRecord(
            sequence=1,
            submit_started_mono=0.1,
            submit_finished_mono=0.2,
            terminal_mono=9.97,
            bucket="success_in_window",
        ),
    )
    accounting.add(
        CompletionRecord(
            sequence=2,
            submit_started_mono=0.2,
            submit_finished_mono=0.3,
            terminal_mono=12.0,
            bucket="success_late_drain",
        ),
    )
    assert accounting.throughput_per_second() == 0.1
    assert accounting.window.elapsed_seconds == 10.05


def test_producer_exit_code_marks_incomplete_empty_and_validation() -> None:
    window = SampleWindow(submission_start_mono=0.0, submission_end_mono=1.0)
    incomplete = SampleAccounting(window=window)
    incomplete.add(
        CompletionRecord(
            sequence=1,
            submit_started_mono=0.0,
            submit_finished_mono=0.1,
            terminal_mono=None,
            bucket="incomplete_drain",
        ),
    )
    assert producer_exit_code(incomplete, expect_success=True) == EXIT_INCOMPLETE_DRAIN

    empty = SampleAccounting(window=window)
    assert producer_exit_code(empty, expect_success=True) == EXIT_VALIDATION

    validation = SampleAccounting(window=window)
    validation.add(
        CompletionRecord(
            sequence=1,
            submit_started_mono=0.0,
            submit_finished_mono=0.1,
            terminal_mono=0.5,
            bucket="validation_error",
        ),
    )
    assert producer_exit_code(validation, expect_success=True) == EXIT_VALIDATION

    failure_mode = SampleAccounting(window=window)
    failure_mode.add(
        CompletionRecord(
            sequence=1,
            submit_started_mono=0.0,
            submit_finished_mono=0.1,
            terminal_mono=0.5,
            bucket="failure",
        ),
    )
    assert producer_exit_code(failure_mode, expect_success=False) == EXIT_VALIDATION


def test_aggregate_exit_code_prefers_first_failure() -> None:
    window = SampleWindow(submission_start_mono=0.0, submission_end_mono=1.0)
    ok_sample = SampleAccounting(window=window)
    ok_sample.add(
        CompletionRecord(
            sequence=1,
            submit_started_mono=0.0,
            submit_finished_mono=0.1,
            terminal_mono=0.2,
            bucket="success_in_window",
        ),
    )
    bad_sample = SampleAccounting(window=window)
    bad_sample.add(
        CompletionRecord(
            sequence=1,
            submit_started_mono=0.0,
            submit_finished_mono=0.1,
            terminal_mono=None,
            bucket="incomplete_drain",
        ),
    )
    assert aggregate_exit_code([ok_sample, bad_sample], expect_success=True) == EXIT_INCOMPLETE_DRAIN


def test_percentile_summary_requires_minimum_samples() -> None:
    values = [float(index) for index in range(MIN_SAMPLES_FOR_PERCENTILES - 1)]
    summary = percentile_summary(values)
    assert summary["median"] is None
    assert summary["p95"] is None
    enough = [float(index) for index in range(MIN_SAMPLES_FOR_PERCENTILES)]
    filled = percentile_summary(enough)
    assert filled["median"] is not None
    assert filled["p95"] is not None


def test_fixture_sizes_near_targets() -> None:
    sizes = measure_fixture_sizes()
    assert 900 <= sizes.telemetry_request_bytes <= 1100
    assert 60 * 1024 <= sizes.manifest_request_bytes <= 66 * 1024


def test_fixture_byte_range_stable_with_fixed_width_sequence() -> None:
    telemetry_min, telemetry_max = measure_fixture_byte_range("telemetry", sequences=range(100))
    manifest_min, manifest_max = measure_fixture_byte_range("manifest", sequences=range(100))
    assert telemetry_min == telemetry_max
    assert manifest_min == manifest_max


def test_throughput_variation_across_samples() -> None:
    samples = [
        {"throughput_per_second": 10.0},
        {"throughput_per_second": 12.0},
        {"throughput_per_second": 11.0},
    ]
    variation = throughput_variation(samples)
    assert variation["min"] == 10.0
    assert variation["max"] == 12.0
    assert variation["spread"] == 2.0


def test_smoke_report_marks_non_baseline() -> None:
    payload = build_report_payload(
        profile="smoke",
        baseline_eligible=False,
        combinations=[],
        controls=[],
        repo_root=REPO_ROOT,
    )
    assert payload["baseline_eligible"] is False
    assert payload["profile"] == "smoke"
    assert payload["historical_comparison"] is None


def _baseline_combo(
    *,
    workload: str = "telemetry",
    concurrency: int = 1,
    samples: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    return {
        "workload": workload,
        "concurrency": concurrency,
        "inflight": concurrency,
        "effective_timing": {
            "warmup_seconds": 3.0,
            "sample_seconds": 20.0,
            "sample_count": 3,
            "drain_seconds": 60.0,
        },
        "samples": samples
        or [{"throughput_per_second": 10.0}, {"throughput_per_second": 11.0}],
    }


def test_compare_reports_requires_matching_environment() -> None:
    left = build_report_payload(
        profile="baseline",
        baseline_eligible=True,
        combinations=[_baseline_combo()],
        controls=[],
        repo_root=REPO_ROOT,
    )
    env = left["environment"]
    right = {
        **left,
        "environment": {
            **env,
            "hardware": {**(env.get("hardware") or {}), "cpu_model": "other"},
        },
    }
    result = compare_reports(left, right)
    assert result["compatible_environment"] is False
    assert not result["ratios"]


def test_compare_reports_ignores_ephemeral_broker_store_path() -> None:
    left = build_report_payload(
        profile="baseline",
        baseline_eligible=True,
        combinations=[_baseline_combo()],
        controls=[],
        repo_root=REPO_ROOT,
    )
    env = dict(left["environment"])
    right = {
        **left,
        "environment": {
            **env,
            "broker_jetstream_store_path": r"C:\Temp\owned-nats-a\jetstream",
        },
    }
    left["environment"] = {
        **env,
        "broker_jetstream_store_path": r"C:\Temp\owned-nats-b\jetstream",
    }
    assert normalized_comparison_environment(left["environment"]) == normalized_comparison_environment(
        right["environment"],
    )
    result = compare_reports(left, right)
    assert result["compatible_environment"] is True
    assert len(result["ratios"]) == 1


def test_compare_reports_rejects_measurement_clock_mismatch() -> None:
    left = build_report_payload(
        profile="baseline",
        baseline_eligible=True,
        combinations=[_baseline_combo()],
        controls=[],
        repo_root=REPO_ROOT,
    )
    env = dict(left["environment"])
    env["measurement_clock"] = {
        "function": "perf_counter",
        "implementation": "QueryPerformanceCounter()",
        "resolution": 1e-07,
        "monotonic": True,
        "adjustable": False,
    }
    left = {**left, "environment": env}
    right = {
        **left,
        "environment": {
            **env,
            "measurement_clock": {
                "function": "monotonic",
                "implementation": "GetTickCount64()",
                "resolution": 0.015625,
                "monotonic": True,
                "adjustable": False,
            },
        },
    }
    result = compare_reports(left, right)
    assert result["compatible_environment"] is False
    assert not result["ratios"]


def test_compare_reports_rejects_timing_mismatch() -> None:
    left = build_report_payload(
        profile="baseline",
        baseline_eligible=True,
        combinations=[_baseline_combo()],
        controls=[],
        repo_root=REPO_ROOT,
    )
    right = {
        **left,
        "combinations": [
            {
                **_baseline_combo(),
                "effective_timing": {
                    "warmup_seconds": 3.0,
                    "sample_seconds": 1.0,
                    "sample_count": 3,
                    "drain_seconds": 60.0,
                },
            },
        ],
    }
    result = compare_reports(left, right)
    assert result["compatible_environment"] is True
    assert not result["ratios"]


def test_acquire_slot_does_not_wait_past_deadline() -> None:
    async def exercise() -> None:
        sem = asyncio.Semaphore(0)
        start = time.perf_counter()
        acquired = await acquire_slot_before(sem, start + 0.05)
        assert acquired is False
        assert time.perf_counter() - start < 0.25

    asyncio.run(exercise())


def test_run_sample_stalled_submit_counts_uncertain_within_budget() -> None:
    async def exercise() -> None:
        async def submit_fn(_seq: int) -> object:
            await asyncio.sleep(0.2)
            return object()

        async def track_fn(*_args, **_kwargs) -> None:
            return None

        start = time.perf_counter()
        config = SampleRunConfig(
            inflight=1,
            sample_seconds=0.08,
            drain_seconds=0.05,
            sequence_start=0,
        )
        accounting, _next_seq = await run_sample(submit_fn, track_fn, config=config)
        elapsed = time.perf_counter() - start
        assert elapsed < 0.3
        assert accounting.counts()["submit_uncertain"] >= 1
        assert accounting.window.drain_elapsed_seconds <= config.drain_seconds + 0.05

    asyncio.run(exercise())


def test_sample_window_elapsed_uses_boundary_reached_not_loop_stop() -> None:
    window = SampleWindow(
        submission_start_mono=0.0,
        submission_end_mono=10.0,
        measured_submission_end_mono=10.0,
        submission_loop_stop_mono=9.95,
        lifecycle_end_mono=70.0,
        drain_start_mono=10.0,
        drain_end_mono=10.5,
    )
    assert window.elapsed_seconds == 10.0
    assert window.intended_submission_window_seconds == 10.0
    assert window.submission_loop_stop_seconds == 9.95
    assert window.submission_boundary_reached_seconds == 10.0
    assert window.drain_elapsed_seconds == 0.5


def test_run_sample_waits_for_boundary_after_early_loop_stop() -> None:
    async def exercise() -> None:
        sample_accounting = SampleAccounting(window=SampleWindow(0.0, 0.0))

        async def submit_fn(_seq: int) -> object:
            return object()

        async def track_fn(_handle, **kwargs) -> None:
            boundary_mono = kwargs["boundary_mono"]
            submit_started = kwargs["submit_started"]
            submit_finished = kwargs.get("submit_finished")
            await asyncio.sleep(0.001)
            terminal_mono = time.perf_counter()
            bucket = classify_terminal_time(terminal_mono, boundary_mono)
            sample_accounting.add(
                CompletionRecord(
                    sequence=kwargs["seq"],
                    submit_started_mono=submit_started,
                    submit_finished_mono=submit_finished,
                    terminal_mono=terminal_mono,
                    bucket=bucket,
                ),
            )

        start = time.perf_counter()
        config = SampleRunConfig(
            inflight=1,
            sample_seconds=1.0,
            drain_seconds=0.05,
            sequence_start=0,
        )
        accounting, _next_seq = await run_sample(
            submit_fn,
            track_fn,
            config=config,
            accounting=sample_accounting,
        )
        elapsed = accounting.window.elapsed_seconds
        loop_stop = accounting.window.submission_loop_stop_seconds
        assert accounting.window.intended_submission_window_seconds == config.sample_seconds
        assert elapsed >= config.sample_seconds - 0.05
        assert abs(elapsed - config.sample_seconds) < 0.08
        assert loop_stop <= elapsed + 0.02
        if loop_stop < config.sample_seconds - 0.02:
            assert elapsed > loop_stop
        assert accounting.counts()["success_in_window"] >= 1
        assert time.perf_counter() - start >= config.sample_seconds - 0.05

    asyncio.run(exercise())


def _producer_contracts():
    support = str(SUPPORT_ROOT)
    if support not in sys.path:
        sys.path.insert(0, support)
    import contracts

    return contracts


def _load_producer_app():
    support = str(SUPPORT_ROOT)
    if support not in sys.path:
        sys.path.insert(0, support)
    spec = importlib.util.spec_from_file_location(
        "performance_producer_app",
        SUPPORT_ROOT / "producer_app.py",
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_producer_run_sample_wires_submit_and_track() -> None:
    from superjobs import JobCompleted, JobStarted, JobSucceeded

    producer_app = _load_producer_app()

    class FakeEvent:
        def __init__(self, data: object) -> None:
            self.data = data

    class FakeHandle:
        def __init__(self, *, delay: float = 0.0) -> None:
            self.delay = delay

        async def outcome(self, wait_timeout: float):
            if self.delay:
                await asyncio.sleep(self.delay)
            return JobSucceeded(result=None)

        async def result(self, wait_timeout: float):
            return None

        def events(self):
            async def _gen():
                yield FakeEvent(JobStarted())
                yield FakeEvent(JobCompleted())

            return _gen()

    class FakeClient:
        def __init__(self) -> None:
            self.calls = 0

        async def submit(self, _request):
            self.calls += 1
            if self.calls == 2:
                raise RuntimeError("submit failed")
            if self.calls >= 3:
                return FakeHandle(delay=5.0)
            return FakeHandle()

    class FakeJobs:
        def client(self, _job):
            return FakeClient()

    async def exercise() -> None:
        request_bytes: dict[str, int] = {}
        accounting, _seq = await producer_app._run_sample(
            FakeJobs(),
            workload="telemetry",
            inflight=1,
            sample_seconds=0.2,
            drain_seconds=0.05,
            sequence_start=0,
            request_bytes_seen=request_bytes,
        )
        counts = accounting.counts()
        assert counts["success_in_window"] >= 1
        assert counts["submit_failed"] >= 1
        assert counts["incomplete_drain"] >= 1
        assert request_bytes["min"] == request_bytes["max"]

    asyncio.run(exercise())


def test_probe_role_dependency_versions_reads_installed_packages() -> None:
    versions = probe_role_dependency_versions(Path(sys.executable))
    assert "python" in versions
    assert "superjobs" in versions
    assert versions["superjobs"] not in ("unknown", "missing")


def test_broker_storage_unknown_off_c_drive() -> None:
    hardware = {"c_drive_storage_medium": "ssd", "c_drive_storage_evidence": "C: maps"}
    medium, evidence = broker_storage_for_path(Path("D:/nats/store"), hardware)
    assert medium == "unknown"
    assert "C: inference only" in evidence


def _synthetic_baseline_sample(
    *,
    failures: int = 0,
    diagnostics: list[dict] | None = None,
) -> dict:
    counts = {
        "success_in_window": 10,
        "success_late_drain": 0,
        "failure": failures,
        "validation_error": 0,
        "incomplete_drain": 0,
        "submit_failed": 0,
        "submit_uncertain": 0,
    }
    sample = {
        "counts": counts,
        "throughput_per_second": 10.0,
        "throughput_interval_seconds": 20.0,
        "window_seconds": 20.0,
        "submit_latency_seconds": {
            "median": 0.001,
            "p95": 0.002,
            "n": 10,
        },
        "failure_diagnostics": diagnostics or [],
    }
    return sample


def _synthetic_baseline_combo(
    workload: str,
    concurrency: int,
    *,
    producer_exit: int = 0,
    error: str | None = None,
    sample_diagnostics: list[dict] | None = None,
) -> dict:
    samples = [
        _synthetic_baseline_sample(
            failures=1 if sample_diagnostics else 0,
            diagnostics=sample_diagnostics,
        )
        for _ in range(BASELINE_SAMPLE_COUNT)
    ]
    return {
        "workload": workload,
        "concurrency": concurrency,
        "inflight": concurrency,
        "producer_exit": producer_exit,
        "baseline_eligible": producer_exit == 0,
        "effective_timing": {
            "warmup_seconds": 3.0,
            "sample_seconds": 20.0,
            "sample_count": 3,
            "drain_seconds": 60.0,
        },
        "observed_request_bytes": {"min": 1041, "max": 1043},
        "samples": samples,
        "error": error,
    }


def _synthetic_environment_snapshot() -> dict:
    return {
        "revision": "deadbeef",
        "revision_clean": True,
        "platform": {"system": "Windows", "release": "11", "machine": "AMD64"},
        "hardware": {"cpu_model": "test", "cpu_cores": 1, "cpu_logical": 1},
        "storage_medium": "ssd",
        "storage_evidence": "test",
        "broker_jetstream_store_path": "C:/temp/store",
        "harness_python": sys.version,
        "child_runtime": {
            "producer": {"python": "3.12"},
            "worker": {"python": "3.12"},
        },
        "dependencies_note": "synthetic",
        "nats_server_version": "2.15.0",
        "fixture_request_bytes": {"telemetry": 1041, "manifest": 65547},
        "measurement_clock": {
            "function": "perf_counter",
            "implementation": "QueryPerformanceCounter()",
            "resolution": 1e-07,
            "monotonic": True,
            "adjustable": False,
        },
    }


def test_main_failed_baseline_retains_four_combos_and_writes_report(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    combos = [
        _synthetic_baseline_combo("telemetry", 1),
        _synthetic_baseline_combo("telemetry", 8),
        _synthetic_baseline_combo("manifest", 1),
        _synthetic_baseline_combo(
            "manifest",
            8,
            producer_exit=EXIT_VALIDATION,
            error="producer exit 4",
            sample_diagnostics=[
                {
                    "sequence": 1,
                    "execution_id": "exec-1",
                    "phase": "validation_events",
                    "bucket": "validation_error",
                    "exception_type": "RuntimeError",
                    "exception_message": "replay gap",
                },
            ],
        ),
    ]

    def fake_verify_python_version(**_kwargs):
        return {
            "combinations": combos,
            "controls": [],
            "origins": {},
            "environment_snapshot": _synthetic_environment_snapshot(),
            "broker_store_path": None,
            "error": "manifest@8: producer exit 4",
        }

    monkeypatch.setattr(vp, "verify_python_version", fake_verify_python_version)
    monkeypatch.setattr(vp, "build_wheels", lambda _work: (tmp_path / "lib.whl", tmp_path / "c.whl"))
    monkeypatch.setattr(vp, "run_performance_typing_gate", lambda: None)
    monkeypatch.setattr(vp, "assert_work_parent_outside_repo", lambda _parent: None)

    baseline_dir = tmp_path / "baselines"
    exit_code = vp.main(
        [
            "--python",
            "3.12",
            "--skip-contract-typing",
            "--work-dir",
            str(tmp_path / "work"),
            "--artifact-dir",
            str(tmp_path / "artifacts"),
            "--baseline-dir",
            str(baseline_dir),
            "--report-date",
            "2026-10-03",
        ],
    )
    assert exit_code == EXIT_VALIDATION
    failed_json = baseline_dir / "2026-10-03-failed-baseline.json"
    assert failed_json.is_file()
    payload = json.loads(failed_json.read_text(encoding="utf-8"))
    assert len(payload["combinations"]) == 4
    assert payload["run_success"] is False
    assert payload["harness_exit_code"] == EXIT_VALIDATION
    assert payload["baseline_eligible"] is False
    assert payload["error"]
    assert payload["follow_up_issues"] == [MANIFEST_RELIABILITY_ISSUE_URL]
    clock = payload["environment"]["measurement_clock"]
    assert clock["function"] == "perf_counter"
    manifest = payload["combinations"][-1]
    assert manifest["producer_exit"] == EXIT_VALIDATION
    assert manifest["samples"][0]["failure_diagnostics"]


def test_main_recovers_partial_summary_on_setup_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    def failing_verify(**kwargs):
        artifact_dir = kwargs["artifact_dir"]
        tag = runtime_evidence_tag(kwargs["python_version"])
        evidence_dir = artifact_dir / tag
        evidence_dir.mkdir(parents=True, exist_ok=True)
        summary = {
            "python_version": kwargs["python_version"],
            "combinations": [_synthetic_baseline_combo("telemetry", 1)],
            "controls": [],
            "origins": {},
            "environment_snapshot": _synthetic_environment_snapshot(),
            "broker_store_path": None,
            "error": "broker setup failed",
        }
        _write_evidence(evidence_dir / "summary.json", summary)
        raise vp.PerformanceError("broker setup failed")

    monkeypatch.setattr(vp, "verify_python_version", failing_verify)
    monkeypatch.setattr(vp, "build_wheels", lambda _work: (tmp_path / "lib.whl", tmp_path / "c.whl"))
    monkeypatch.setattr(vp, "run_performance_typing_gate", lambda: None)
    monkeypatch.setattr(vp, "assert_work_parent_outside_repo", lambda _parent: None)

    baseline_dir = tmp_path / "baselines"
    exit_code = vp.main(
        [
            "--python",
            "3.12",
            "--skip-contract-typing",
            "--work-dir",
            str(tmp_path / "work"),
            "--artifact-dir",
            str(tmp_path / "artifacts"),
            "--baseline-dir",
            str(baseline_dir),
            "--report-date",
            "2026-10-03",
        ],
    )
    assert exit_code == 1
    failed_json = baseline_dir / "2026-10-03-failed-baseline.json"
    assert failed_json.is_file()
    payload = json.loads(failed_json.read_text(encoding="utf-8"))
    assert len(payload["combinations"]) == 1
    assert payload["run_success"] is False
    assert payload["harness_exit_code"] == 1
    assert "broker setup failed" in payload["error"]
    summary = json.loads(
        next((tmp_path / "artifacts").glob("run-*/py312/summary.json")).read_text(
            encoding="utf-8",
        ),
    )
    assert len(summary["combinations"]) == 1


def test_main_early_failure_does_not_recover_previous_invocation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    artifact_parent = tmp_path / "artifacts"
    stale_path = artifact_parent / runtime_evidence_tag("3.12") / "summary.json"
    stale = {
        "combinations": [_synthetic_baseline_combo("manifest", 8)],
        "environment_snapshot": _synthetic_environment_snapshot(),
        "error": "stale invocation failure",
    }
    _write_evidence(stale_path, stale)
    prior_contents = stale_path.read_bytes()

    def fail_build(_work):
        raise vp.PerformanceError("current invocation build failed")

    monkeypatch.setattr(vp, "build_wheels", fail_build)
    monkeypatch.setattr(vp, "run_performance_typing_gate", lambda: None)
    monkeypatch.setattr(vp, "assert_work_parent_outside_repo", lambda _parent: None)
    baseline_dir = tmp_path / "baselines"
    assert vp.main([
        "--python", "3.12", "--skip-contract-typing",
        "--work-dir", str(tmp_path / "work"),
        "--artifact-dir", str(artifact_parent),
        "--baseline-dir", str(baseline_dir),
        "--report-date", "2026-10-03",
    ]) == 1
    report = json.loads(
        (baseline_dir / "2026-10-03-failed-baseline.json").read_text(encoding="utf-8"),
    )
    assert report["combinations"] == []
    assert report["run_success"] is False
    assert report["error"] == "current invocation build failed"
    assert stale_path.read_bytes() == prior_contents
    errors = list(artifact_parent.glob("run-*/run_error.json"))
    assert len(errors) == 1
    assert "current invocation build failed" in errors[0].read_text(encoding="utf-8")


def test_finalize_run_outcome_links_manifest_reliability_issue() -> None:
    base = build_report_payload(
        profile="baseline",
        baseline_eligible=False,
        combinations=[],
        controls=[],
        repo_root=REPO_ROOT,
    )
    failed = finalize_run_outcome(
        base,
        run_success=False,
        harness_exit_code=EXIT_VALIDATION,
        error="producer exit 4",
    )
    assert failed["run_outcome"] == "failed"
    assert failed["follow_up_issues"] == [MANIFEST_RELIABILITY_ISSUE_URL]


def test_runner_rejects_mixed_runtimes_before_setup(capsys) -> None:
    with pytest.raises(SystemExit) as exc:
        vp.main(["--python", "3.12", "--python", "3.14"])
    assert exc.value.code == 2
    assert "exactly one Python runtime" in capsys.readouterr().err


def test_smoke_profile_cannot_publish_baseline_artifacts() -> None:
    payload = build_report_payload(
        profile="smoke",
        baseline_eligible=False,
        combinations=[{"workload": "telemetry", "concurrency": 1, "samples": []}],
        controls=[],
        repo_root=REPO_ROOT,
    )
    assert payload["baseline_eligible"] is False
    assert payload["profile"] == "smoke"


def test_run_sample_records_submit_failure_without_aborting() -> None:
    async def exercise() -> None:
        async def submit_fn(seq: int) -> object:
            if seq == 0:
                raise RuntimeError("boom")
            return object()

        async def track_fn(*_args, **_kwargs) -> None:
            return None

        config = SampleRunConfig(
            inflight=2,
            sample_seconds=0.2,
            drain_seconds=0.05,
            sequence_start=0,
        )
        accounting, next_seq = await run_sample(submit_fn, track_fn, config=config)
        assert next_seq >= 2
        assert accounting.counts()["submit_failed"] == 1

    asyncio.run(exercise())


def test_materialize_role_dir_keeps_worker_handlers_off_producer(tmp_path: Path) -> None:
    producer_dir = vp.materialize_role_dir(tmp_path, "producer", vp.PRODUCER_SUPPORT_FILES)
    worker_dir = vp.materialize_role_dir(tmp_path, "worker", vp.WORKER_SUPPORT_FILES)
    assert (producer_dir / "producer_app.py").is_file()
    assert not (producer_dir / "worker_handlers.py").exists()
    assert (worker_dir / "worker_handlers.py").is_file()
    assert (producer_dir / "scheduling.py").is_file()


def test_failure_diagnostics_include_phase_and_exception_fields() -> None:
    window = SampleWindow(submission_start_mono=0.0, submission_end_mono=1.0)
    accounting = SampleAccounting(window=window)
    accounting.add(
        CompletionRecord(
            sequence=7,
            submit_started_mono=0.0,
            submit_finished_mono=0.1,
            terminal_mono=0.5,
            bucket="failure",
            phase="outcome",
            execution_id="exec-7",
            job_error_code="handler_error",
            job_error_message="boom",
            detail="handler_error",
        ),
    )
    diagnostics = accounting.failure_diagnostics()
    assert len(diagnostics) == 1
    assert diagnostics[0]["phase"] == "outcome"
    assert diagnostics[0]["job_error_code"] == "handler_error"
    summary = accounting.summarize()
    assert summary["failure_diagnostics"] == diagnostics
    assert summary["counts"]["failure"] == 1


def test_exception_fields_capture_qualified_type() -> None:
    from tools.performance_support.accounting import exception_fields

    try:
        raise RuntimeError("submit failed")
    except RuntimeError as exc:
        fields = exception_fields(exc)
    assert fields["exception_type"] == "RuntimeError" or fields["exception_type"].endswith(
        "RuntimeError",
    )
    assert fields["exception_message"] == "submit failed"


def test_diagnostic_profile_is_not_baseline_eligible() -> None:
    from tools.performance_support.config import profile_for

    profile = profile_for("diagnostic")
    assert profile.baseline_eligible is False
    assert profile.sample_count == 3
    assert profile.sample_seconds == 20.0


def test_build_report_payload_carries_observed_request_bytes() -> None:
    payload = build_report_payload(
        profile="diagnostic",
        baseline_eligible=False,
        combinations=[
            {
                "workload": "manifest",
                "concurrency": 8,
                "inflight": 8,
                "observed_request_bytes": {"min": 65547, "max": 65547},
                "samples": [],
            },
        ],
        controls=[],
        repo_root=REPO_ROOT,
    )
    combo = payload["combinations"][0]
    assert combo["observed_request_bytes"] == {"min": 65547, "max": 65547}


def test_runner_help_lists_diagnostic_flag() -> None:
    completed = subprocess.run(
        [sys.executable, str(RUNNER), "--help"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
        creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0,
    )
    assert completed.returncode == 0
    assert "--smoke" in completed.stdout
    assert "--diagnostic" in completed.stdout
    assert "--diagnostic-full-order" in completed.stdout


def _manifest_track_fake_jobs(handle_factory):
    class FakeClient:
        def __init__(self) -> None:
            self.calls = 0

        async def submit(self, _request):
            self.calls += 1
            return handle_factory(self.calls)

    class FakeJobs:
        def client(self, _job):
            return FakeClient()

    return FakeJobs()


def _single_validation_failure_row(accounting: SampleAccounting) -> dict:
    rows = accounting.failure_diagnostics()
    assert len(rows) >= 1
    return rows[0]


def test_producer_track_manifest_result_exception_records_validation_phase() -> None:
    from superjobs import JobSucceeded

    producer_app = _load_producer_app()

    class FakeHandle:
        job_id = "exec-result-exc"

        async def outcome(self, wait_timeout: float):
            return JobSucceeded(result=None)

        async def result(self, wait_timeout: float):
            raise ValueError("result read failed")

        def events(self):
            async def _gen():
                if False:
                    yield None

            return _gen()

    async def exercise() -> None:
        jobs = _manifest_track_fake_jobs(lambda _call: FakeHandle())
        accounting, _seq = await producer_app._run_sample(
            jobs,
            workload="manifest",
            inflight=1,
            sample_seconds=0.15,
            drain_seconds=0.05,
            sequence_start=0,
            request_bytes_seen={},
        )
        row = _single_validation_failure_row(accounting)
        assert row["phase"] == "validation_result"
        assert row["bucket"] == "validation_error"
        assert row["execution_id"] == "exec-result-exc"
        assert "ValueError" in (row.get("exception_type") or "")
        assert row.get("exception_message") == "result read failed"
        assert "result read failed" in (row.get("exception_repr") or "")

    asyncio.run(exercise())


def test_producer_track_manifest_events_exception_records_validation_events_phase() -> None:
    from superjobs import JobSucceeded

    producer_app = _load_producer_app()
    contracts = _producer_contracts()
    PerformanceManifestResult = contracts.PerformanceManifestResult

    class FakeHandle:
        job_id = "exec-events-exc"

        async def outcome(self, wait_timeout: float):
            return JobSucceeded(result=None)

        async def result(self, wait_timeout: float):
            return PerformanceManifestResult(revision="perf-baseline-rev")

        def events(self):
            async def _gen():
                raise RuntimeError("events stream failed")
                yield None

            return _gen()

    async def exercise() -> None:
        jobs = _manifest_track_fake_jobs(lambda _call: FakeHandle())
        accounting, _seq = await producer_app._run_sample(
            jobs,
            workload="manifest",
            inflight=1,
            sample_seconds=0.15,
            drain_seconds=0.05,
            sequence_start=0,
            request_bytes_seen={},
        )
        row = _single_validation_failure_row(accounting)
        assert row["phase"] == "validation_events"
        assert row["bucket"] == "validation_error"
        assert row["execution_id"] == "exec-events-exc"
        assert "RuntimeError" in (row.get("exception_type") or "")
        assert row.get("exception_message") == "events stream failed"

    asyncio.run(exercise())


def test_producer_track_manifest_validation_outcome_failure_records_phase() -> None:
    from superjobs import JobCompleted, JobError, JobFailedOutcome, JobStarted, JobSucceeded

    producer_app = _load_producer_app()
    contracts = _producer_contracts()
    PerformanceManifestEvent = contracts.PerformanceManifestEvent
    PerformanceManifestResult = contracts.PerformanceManifestResult

    class FakeEvent:
        def __init__(self, data: object) -> None:
            self.data = data

    class FakeHandle:
        job_id = "exec-outcome-fail"

        def __init__(self) -> None:
            self.outcome_reads = 0

        async def outcome(self, wait_timeout: float):
            self.outcome_reads += 1
            if self.outcome_reads == 1:
                return JobSucceeded(result=None)
            return JobFailedOutcome(error=JobError(code="bad", message="nope"))

        async def result(self, wait_timeout: float):
            return PerformanceManifestResult(revision="perf-baseline-rev")

        def events(self):
            async def _gen():
                yield FakeEvent(JobStarted())
                for stage in ("received", "validated", "published"):
                    yield FakeEvent(PerformanceManifestEvent(stage=stage))
                yield FakeEvent(JobCompleted())

            return _gen()

    async def exercise() -> None:
        jobs = _manifest_track_fake_jobs(lambda _call: FakeHandle())
        accounting, _seq = await producer_app._run_sample(
            jobs,
            workload="manifest",
            inflight=1,
            sample_seconds=0.15,
            drain_seconds=0.05,
            sequence_start=0,
            request_bytes_seen={},
        )
        row = _single_validation_failure_row(accounting)
        assert row["phase"] == "validation_outcome"
        assert row["bucket"] == "validation_error"
        assert row["execution_id"] == "exec-outcome-fail"
        assert "JobFailedOutcome" in (row.get("detail") or "")

    asyncio.run(exercise())


def test_producer_track_telemetry_validation_outcome_failure_records_phase() -> None:
    from superjobs import JobError, JobFailedOutcome, JobSucceeded

    producer_app = _load_producer_app()

    class FakeHandle:
        job_id = "exec-telemetry-outcome"

        def __init__(self) -> None:
            self.outcome_reads = 0

        async def outcome(self, wait_timeout: float):
            self.outcome_reads += 1
            if self.outcome_reads == 1:
                return JobSucceeded(result=None)
            return JobFailedOutcome(error=JobError(code="telemetry_bad", message="x"))

        async def result(self, wait_timeout: float):
            return None

        def events(self):
            async def _gen():
                if False:
                    yield None

            return _gen()

    class FakeClient:
        async def submit(self, _request):
            return FakeHandle()

    class FakeJobs:
        def client(self, _job):
            return FakeClient()

    async def exercise() -> None:
        accounting, _seq = await producer_app._run_sample(
            FakeJobs(),
            workload="telemetry",
            inflight=1,
            sample_seconds=0.15,
            drain_seconds=0.05,
            sequence_start=0,
            request_bytes_seen={},
        )
        row = _single_validation_failure_row(accounting)
        assert row["phase"] == "validation_outcome"
        assert row["bucket"] == "validation_error"
        assert row["execution_id"] == "exec-telemetry-outcome"

    asyncio.run(exercise())


def test_measurement_clock_metadata_uses_perf_counter() -> None:
    meta = measurement_clock_metadata()
    assert meta["function"] == "perf_counter"
    assert meta["monotonic"] is True
    assert meta["resolution"] > 0


def test_probe_measurement_clocks_from_installed_python() -> None:
    payload = probe_measurement_clocks(Path(sys.executable))
    measurement = payload["measurement_clock"]
    legacy = payload["legacy_monotonic_clock"]
    assert measurement["function"] == "perf_counter"
    assert measurement["resolution"] <= 1e-6
    assert legacy["function"] == "monotonic"
    assert legacy["resolution"] > 0


def test_run_sample_records_nonzero_submit_latency() -> None:
    async def exercise() -> None:
        sample_accounting = SampleAccounting(window=SampleWindow(0.0, 0.0))

        async def submit_fn(_seq: int) -> object:
            await asyncio.sleep(0.004)
            return object()

        async def track_fn(_handle, **kwargs) -> None:
            boundary_mono = kwargs["boundary_mono"]
            submit_started = kwargs["submit_started"]
            submit_finished = kwargs.get("submit_finished")
            terminal_mono = time.perf_counter()
            bucket = classify_terminal_time(terminal_mono, boundary_mono)
            sample_accounting.add(
                CompletionRecord(
                    sequence=kwargs["seq"],
                    submit_started_mono=submit_started,
                    submit_finished_mono=submit_finished,
                    terminal_mono=terminal_mono,
                    bucket=bucket,
                ),
            )

        config = SampleRunConfig(
            inflight=1,
            sample_seconds=0.25,
            drain_seconds=0.05,
            sequence_start=0,
        )
        accounting, _next_seq = await run_sample(
            submit_fn,
            track_fn,
            config=config,
            accounting=sample_accounting,
        )
        latencies = accounting.submit_latencies_seconds()
        assert latencies
        assert max(latencies) > 0.0
        summary = accounting.summarize()
        assert summary["submit_latency_seconds"]["n"] == len(latencies)

    asyncio.run(exercise())
