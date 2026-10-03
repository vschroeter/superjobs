#!/usr/bin/env python3
"""Manual telemetry/manifest performance baseline harness (installed producer and worker)."""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import tempfile
import time
import uuid
from datetime import date
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, TextIO

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
SUPPORT_ROOT = Path(__file__).resolve().parent / "performance_support"
ORIGIN_PROBE = Path(__file__).resolve().parent / "wheel_origin_probe.py"

from scripts.dev_check.process_tree import kill_process_tree  # noqa: E402
from tests.support.nats_harness.server import OwnedNatsServer  # noqa: E402

from tools.cross_program_support.child_env import (  # noqa: E402
    isolated_child_env,
    python_command,
    subprocess_creationflags,
)
from tools.performance_support.config import (  # noqa: E402
    BASELINE_DRAIN_SECONDS,
    BASELINE_SAMPLE_COUNT,
    BASELINE_SAMPLE_SECONDS,
    CONCURRENCY_LEVELS,
    EXIT_INCOMPLETE_DRAIN,
    EXIT_OK,
    EXIT_VALIDATION,
    SMOKE_SAMPLE_COUNT,
    WORKLOADS,
)
from tools.performance_support.fixtures import measure_fixture_sizes  # noqa: E402
from tools.performance_support.metadata import environment_block  # noqa: E402
from tools.performance_support.protocol import (  # noqa: E402
    read_producer_results,
    wait_for_ready,
    write_worker_stop,
)
from tools.performance_support.reporting import (  # noqa: E402
    build_report_payload,
    finalize_run_outcome,
    write_json_report,
    write_markdown_report,
)
from tools.verify_contract_typing import (  # noqa: E402
    DEFAULT_RUNTIMES,
    REPO_ROOT as TYPING_REPO_ROOT,
    VerificationError,
    assert_final_runtime_python,
    assert_work_parent_outside_repo,
    build_wheels,
    compare_negative,
    compare_positive,
    create_wheel_venv,
    load_suite_expectations,
    run_cmd,
    run_pyright,
    runtime_evidence_tag,
    validate_pyright_payload,
    venv_python,
)
from tools.verify_cross_program import (  # noqa: E402
    ChildRecord,
    _close_child_streams_safe,
    _finalize_child_after_kill,
    _reap_child,
    _spawn_child,
    _write_evidence,
)

assert TYPING_REPO_ROOT == REPO_ROOT

CHILD_TIMEOUT_SECONDS = 30
COMBINATION_TIMEOUT_SECONDS = 600
KILL_REAP_SECONDS = 5
WORKER_EXIT_OK = 0

ROLE_SHARED_FILES = (
    "protocol.py",
    "contracts.py",
    "scenario_jobs.py",
    "fixtures.py",
    "accounting.py",
    "scheduling.py",
    "measurement_clock.py",
    "config.py",
    "runtime_isolation.py",
)
WORKER_SUPPORT_FILES = ROLE_SHARED_FILES + ("worker_handlers.py", "worker_app.py")
PRODUCER_SUPPORT_FILES = ROLE_SHARED_FILES + ("producer_app.py",)

CONTROL_SCENARIOS = (
    ("control_failure", "failure", EXIT_VALIDATION),
    ("control_delay", "delay", EXIT_OK),
    ("control_incomplete", "incomplete", EXIT_INCOMPLETE_DRAIN),
)


def materialize_role_dir(work_dir: Path, role: str, files: tuple[str, ...]) -> Path:
    dest = work_dir / role
    dest.mkdir(parents=True, exist_ok=True)
    for name in files:
        shutil.copy2(SUPPORT_ROOT / name, dest / name)
    return dest


class PerformanceError(Exception):
    """Raised when performance verification fails."""


def _combination_work_deadline(combination_deadline: float) -> float:
    return combination_deadline - KILL_REAP_SECONDS


def _sample_measurable_work(sample: dict[str, Any]) -> int:
    counts = sample.get("counts") or {}
    return int(
        counts.get("success_in_window", 0)
        + counts.get("success_late_drain", 0)
        + counts.get("failure", 0)
        + counts.get("validation_error", 0)
        + counts.get("incomplete_drain", 0)
        + counts.get("submit_failed", 0)
        + counts.get("submit_uncertain", 0)
    )


def _validate_producer_results(
    results: dict[str, Any],
    *,
    profile: str,
    workload: str,
    concurrency: int,
    producer_exit: int = EXIT_OK,
) -> None:
    if results.get("workload") != workload:
        raise PerformanceError(
            f"producer workload {results.get('workload')!r} != expected {workload!r}",
        )
    if int(results.get("concurrency") or 0) != concurrency:
        raise PerformanceError(
            f"producer concurrency {results.get('concurrency')!r} != expected {concurrency}",
        )
    inflight = results.get("inflight")
    if inflight is None:
        raise PerformanceError("producer results missing inflight")
    if int(inflight) != concurrency:
        raise PerformanceError(
            f"producer inflight {inflight} must match concurrency {concurrency}",
        )
    effective = results.get("effective_timing") or {}
    samples = list(results.get("samples") or [])
    benchmark_succeeded = producer_exit == EXIT_OK
    if profile == "baseline":
        if benchmark_succeeded and not results.get("baseline_eligible"):
            raise PerformanceError("producer marked baseline run non-eligible")
        if float(effective.get("sample_seconds", -1)) != BASELINE_SAMPLE_SECONDS:
            raise PerformanceError("producer sample_seconds != baseline configuration")
        if int(effective.get("sample_count", -1)) != BASELINE_SAMPLE_COUNT:
            raise PerformanceError("producer sample_count != baseline configuration")
        if float(effective.get("drain_seconds", -1)) != BASELINE_DRAIN_SECONDS:
            raise PerformanceError("producer drain_seconds != baseline configuration")
        if len(samples) != BASELINE_SAMPLE_COUNT:
            raise PerformanceError(
                f"expected {BASELINE_SAMPLE_COUNT} baseline samples, got {len(samples)}",
            )
    elif profile == "smoke":
        if results.get("baseline_eligible"):
            raise PerformanceError("smoke producer must not be baseline eligible")
        if len(samples) != SMOKE_SAMPLE_COUNT:
            raise PerformanceError(
                f"expected {SMOKE_SAMPLE_COUNT} smoke samples, got {len(samples)}",
            )
    elif profile == "diagnostic":
        if results.get("baseline_eligible"):
            raise PerformanceError("diagnostic producer must not be baseline eligible")
        if float(effective.get("sample_seconds", -1)) != BASELINE_SAMPLE_SECONDS:
            raise PerformanceError("diagnostic sample_seconds != baseline configuration")
        if int(effective.get("sample_count", -1)) != BASELINE_SAMPLE_COUNT:
            raise PerformanceError("diagnostic sample_count != baseline configuration")
        if len(samples) != BASELINE_SAMPLE_COUNT:
            raise PerformanceError(
                f"expected {BASELINE_SAMPLE_COUNT} diagnostic samples, got {len(samples)}",
            )
    clock = results.get("measurement_clock") or {}
    if clock.get("function") != "perf_counter":
        raise PerformanceError(
            f"producer measurement_clock must use perf_counter, got {clock!r}",
        )
    for index, sample in enumerate(samples, start=1):
        if _sample_measurable_work(sample) <= 0:
            raise PerformanceError(f"sample {index} recorded no measurable work")
        if (
            benchmark_succeeded
            and profile == "baseline"
            and float(sample.get("throughput_per_second") or 0.0) <= 0.0
        ):
            raise PerformanceError(f"sample {index} has zero in-window throughput")


@dataclass
class CombinationRecord:
    workload: str
    concurrency: int
    inflight: int | None
    producer_exit: int
    samples: list[dict[str, Any]]
    baseline_eligible: bool | None = None
    effective_timing: dict[str, Any] | None = None
    observed_request_bytes: dict[str, int] | None = None
    producer_results_evidence: str | None = None
    error: str | None = None


@dataclass
class ControlRecord:
    name: str
    worker_mode: str
    expected_exit: int
    producer_exit: int | None
    samples: list[dict[str, Any]] = field(default_factory=list)
    error: str | None = None


def _print_diagnostic_failure_summary(
    combinations: list[dict[str, Any]],
    *,
    file: TextIO,
) -> None:
    for combo in combinations:
        workload = combo.get("workload")
        concurrency = combo.get("concurrency")
        for sample_index, sample in enumerate(combo.get("samples") or [], start=1):
            for row in sample.get("failure_diagnostics") or []:
                print(
                    "diagnostic failure "
                    f"workload={workload} concurrency={concurrency} "
                    f"sample={sample_index} sequence={row.get('sequence')} "
                    f"execution_id={row.get('execution_id')} phase={row.get('phase')} "
                    f"bucket={row.get('bucket')} "
                    f"job_error_code={row.get('job_error_code')} "
                    f"job_error_message={row.get('job_error_message')} "
                    f"exception_type={row.get('exception_type')} "
                    f"exception_message={row.get('exception_message')} "
                    f"exception_repr={row.get('exception_repr')} "
                    f"detail={row.get('detail')}",
                    file=file,
                )


def probe_role_origins(
    python: Path,
    role_dir: Path,
    *,
    role: str,
    evidence_dir: Path | None,
    python_version: str,
) -> dict[str, Any]:
    probe_dir = role_dir / f"origin_probe_{role}"
    probe_dir.mkdir(parents=True, exist_ok=True)
    files = PRODUCER_SUPPORT_FILES if role == "producer" else WORKER_SUPPORT_FILES
    for name in files:
        shutil.copy2(role_dir / name, probe_dir / name)
    if role == "producer":
        shutil.copy2(probe_dir / "producer_app.py", probe_dir / "producer.py")
    shutil.copy2(ORIGIN_PROBE, probe_dir / "wheel_origin_probe.py")
    command = python_command(
        str(python),
        probe_dir,
        "wheel_origin_probe.py",
        str(probe_dir),
        str(REPO_ROOT),
        "--role",
        role,
    )
    completed = run_cmd(command, cwd=probe_dir, env=isolated_child_env())
    evidence = json.loads(completed.stdout)
    if evidence_dir is not None:
        evidence_dir.mkdir(parents=True, exist_ok=True)
        tag = runtime_evidence_tag(python_version)
        (evidence_dir / f"{tag}-{role}-origin.json").write_text(
            json.dumps(evidence, indent=2),
            encoding="utf-8",
        )
    return evidence


def _shutdown_worker(
    *,
    worker_process: subprocess.Popen[str],
    record: ChildRecord,
    state_dir: Path,
    run_id: str,
    stdout_io: TextIO,
    stderr_io: TextIO,
    deadline: float,
    expect_ok: bool = True,
) -> None:
    existing = worker_process.poll()
    if existing is not None:
        record.exit_code = existing
        record.ended_at = time.monotonic()
        _close_child_streams_safe(stdout_io, stderr_io)
        if expect_ok and existing != WORKER_EXIT_OK:
            raise PerformanceError(f"worker exit {existing} != expected {WORKER_EXIT_OK}")
        return
    if not expect_ok:
        if worker_process.poll() is None and worker_process.pid:
            kill_process_tree(worker_process.pid)
        record.exit_code = _reap_child(
            worker_process,
            label="worker",
            deadline=deadline,
            expected_code=None,
        )
        record.ended_at = time.monotonic()
        _close_child_streams_safe(stdout_io, stderr_io)
        return
    write_worker_stop(state_dir, run_id)
    record.exit_code = _reap_child(
        worker_process,
        label="worker",
        deadline=deadline,
        expected_code=WORKER_EXIT_OK,
    )
    record.ended_at = time.monotonic()
    _close_child_streams_safe(stdout_io, stderr_io)


def _run_benchmark_pass(
    *,
    nats_url: str,
    producer_python: Path,
    worker_python: Path,
    producer_dir: Path,
    worker_dir: Path,
    workload: str,
    concurrency: int,
    profile: str,
    worker_mode: str,
    log_dir: Path,
    expect_worker_ok: bool = True,
) -> tuple[int, list[dict[str, Any]], dict[str, Any]]:
    run_id = uuid.uuid4().hex
    state_dir = producer_dir / "state" / workload / str(concurrency) / run_id
    state_dir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)
    combination_deadline = time.monotonic() + COMBINATION_TIMEOUT_SECONDS
    work_deadline = _combination_work_deadline(combination_deadline)
    base_env = {
        "NATS_URL": nats_url,
        "SUPERJOBS_PERF_RUN_ID": run_id,
        "SUPERJOBS_PERF_STATE_DIR": str(state_dir),
        "SUPERJOBS_PERF_WORKLOAD": workload,
        "SUPERJOBS_PERF_CONCURRENCY": str(concurrency),
        "SUPERJOBS_PERF_INFLIGHT": str(concurrency),
        "SUPERJOBS_PERF_PROFILE": profile,
        "SUPERJOBS_PERF_WORKER_MODE": worker_mode,
    }
    worker_process: subprocess.Popen[str] | None = None
    producer_process: subprocess.Popen[str] | None = None
    worker_streams: tuple[TextIO, TextIO] | None = None
    producer_streams: tuple[TextIO, TextIO] | None = None
    worker_record: ChildRecord | None = None
    producer_record: ChildRecord | None = None
    try:
        worker_env = isolated_child_env(base_env)
        worker_process, worker_record, w_out, w_err = _spawn_child(
            name="worker",
            python=worker_python,
            role_dir=worker_dir,
            script="worker_app.py",
            cwd=worker_dir,
            env=worker_env,
            log_dir=log_dir,
        )
        worker_streams = (w_out, w_err)
        wait_for_ready(
            state_dir,
            expected_run_id=run_id,
            deadline=max(
                0.0,
                min(CHILD_TIMEOUT_SECONDS, work_deadline - time.monotonic()),
            ),
            child_process=worker_process,
        )

        producer_env = isolated_child_env(base_env)
        producer_process, producer_record, p_out, p_err = _spawn_child(
            name="producer",
            python=producer_python,
            role_dir=producer_dir,
            script="producer_app.py",
            cwd=producer_dir,
            env=producer_env,
            log_dir=log_dir,
        )
        producer_streams = (p_out, p_err)
        producer_code = _reap_child(
            producer_process,
            label="producer",
            deadline=work_deadline,
            expected_code=None,
        )
        producer_record.exit_code = producer_code
        producer_record.ended_at = time.monotonic()
        _close_child_streams_safe(p_out, p_err)

        results_path = state_dir / "producer_results.json"
        if not results_path.is_file():
            raise PerformanceError(
                f"producer exit {producer_code} without results at {results_path}",
            )
        results = read_producer_results(state_dir)
        _validate_producer_results(
            results,
            profile=profile,
            workload=workload,
            concurrency=concurrency,
            producer_exit=producer_code,
        )
        samples = list(results.get("samples") or [])
        return producer_code, samples, results
    finally:
        if worker_process is not None and worker_record is not None:
            w_out, w_err = worker_streams or (None, None)
            try:
                if w_out is not None and w_err is not None:
                    _shutdown_worker(
                        worker_process=worker_process,
                        record=worker_record,
                        state_dir=state_dir,
                        run_id=run_id,
                        stdout_io=w_out,
                        stderr_io=w_err,
                        deadline=combination_deadline,
                        expect_ok=expect_worker_ok,
                    )
            except BaseException:
                if worker_process.poll() is None:
                    kill_process_tree(worker_process.pid or 0)
                _finalize_child_after_kill(
                    worker_process,
                    worker_record,
                    deadline=combination_deadline,
                )
                raise
        if producer_process is not None and producer_record is not None:
            if producer_process.poll() is None:
                kill_process_tree(producer_process.pid or 0)
                _finalize_child_after_kill(
                    producer_process,
                    producer_record,
                    deadline=combination_deadline,
                )


def run_performance_typing_gate() -> None:
    typing_dir = SUPPORT_ROOT / "typing"
    positive_payload = run_pyright(typing_dir)
    positive_errors, positive_warnings = validate_pyright_payload(positive_payload, typing_dir)
    compare_positive(
        positive_errors,
        positive_warnings,
        suite="performance_positive",
        mode="source",
    )

    negative_dir = typing_dir / "negative"
    negative_payload = run_pyright(negative_dir)
    negative_errors, negative_warnings = validate_pyright_payload(negative_payload, negative_dir)
    expected = load_suite_expectations(negative_dir, ("check_types.py",))
    compare_negative(
        expected,
        negative_errors,
        negative_warnings,
        suite="performance_negative",
        mode="source",
    )


def run_contract_typing_gate(python_version: str) -> None:
    command = [
        "uv",
        "run",
        "--no-project",
        "--python",
        python_version,
        "--with",
        ".",
        "python",
        str(REPO_ROOT / "tools" / "verify_contract_typing.py"),
        "--mode",
        "both",
        "--python",
        python_version,
    ]
    run_cmd(command, cwd=REPO_ROOT)


def verify_python_version(
    *,
    python_version: str,
    work_dir: Path,
    library: Path,
    contract: Path,
    artifact_dir: Path,
    profile: str,
    run_controls: bool,
    combinations: list[tuple[str, int]],
) -> dict[str, Any]:
    tag = runtime_evidence_tag(python_version)
    evidence_dir = artifact_dir / tag
    evidence_dir.mkdir(parents=True, exist_ok=True)
    owner: OwnedNatsServer | None = None
    broker_store_path: Path | None = None
    combo_records: list[CombinationRecord] = []
    control_records: list[ControlRecord] = []
    error: str | None = None
    origins: dict[str, Any] | None = None
    environment_snapshot: dict[str, Any] | None = None
    raised: BaseException | None = None
    try:
        producer_venv = create_wheel_venv(
            work_dir,
            python_version,
            library,
            contract,
            name=f"venv-producer-{tag}",
        )
        worker_venv = create_wheel_venv(
            work_dir,
            python_version,
            library,
            contract,
            name=f"venv-worker-{tag}",
        )
        producer_py = venv_python(producer_venv)
        worker_py = venv_python(worker_venv)
        assert_final_runtime_python(producer_py)
        assert_final_runtime_python(worker_py)

        producer_dir = materialize_role_dir(work_dir / tag, "producer", PRODUCER_SUPPORT_FILES)
        worker_dir = materialize_role_dir(work_dir / tag, "worker", WORKER_SUPPORT_FILES)
        origins = {
            "producer": probe_role_origins(
                producer_py,
                producer_dir,
                role="producer",
                evidence_dir=evidence_dir,
                python_version=python_version,
            ),
            "worker": probe_role_origins(
                worker_py,
                worker_dir,
                role="worker",
                evidence_dir=evidence_dir,
                python_version=python_version,
            ),
        }
        owner = OwnedNatsServer()
        target = owner.start()
        nats_url = target.url
        if target.work_dir is not None:
            broker_store_path = target.work_dir / "store"

        fixture_sizes = measure_fixture_sizes()
        environment_snapshot = environment_block(
            repo_root=REPO_ROOT,
            origins=origins,
            broker_store_path=broker_store_path,
            fixture_bytes={
                "telemetry": fixture_sizes.telemetry_request_bytes,
                "manifest": fixture_sizes.manifest_request_bytes,
            },
            producer_python=producer_py,
            worker_python=worker_py,
        )

        for workload, concurrency in combinations:
            log_dir = evidence_dir / "logs" / workload / str(concurrency)
            code, samples, results = _run_benchmark_pass(
                nats_url=nats_url,
                producer_python=producer_py,
                worker_python=worker_py,
                producer_dir=producer_dir,
                worker_dir=worker_dir,
                workload=workload,
                concurrency=concurrency,
                profile=profile,
                worker_mode="success",
                log_dir=log_dir,
            )
            evidence_rel = f"evidence/{workload}/{concurrency}/producer_results.json"
            evidence_path = evidence_dir / evidence_rel
            evidence_path.parent.mkdir(parents=True, exist_ok=True)
            _write_evidence(evidence_path, results)
            observed_bytes = results.get("observed_request_bytes")
            record = CombinationRecord(
                workload=workload,
                concurrency=concurrency,
                inflight=int(results.get("inflight") or concurrency),
                producer_exit=code,
                samples=samples,
                baseline_eligible=bool(results.get("baseline_eligible")),
                effective_timing=dict(results.get("effective_timing") or {}),
                observed_request_bytes=(
                    dict(observed_bytes) if isinstance(observed_bytes, dict) else None
                ),
                producer_results_evidence=evidence_rel,
            )
            if code != EXIT_OK:
                record.error = f"producer exit {code}"
                combo_records.append(record)
                if profile == "diagnostic":
                    continue
                if profile == "baseline":
                    continue
                raise PerformanceError(record.error)
            combo_records.append(record)

        if profile == "baseline":
            seen = {(item.workload, item.concurrency) for item in combo_records}
            expected = {(workload, level) for workload in WORKLOADS for level in CONCURRENCY_LEVELS}
            if seen != expected:
                raise PerformanceError(
                    f"baseline requires {len(expected)} unique combinations, saw {len(seen)}",
                )
            failed = [item for item in combo_records if item.error]
            if failed:
                error = "; ".join(
                    f"{item.workload}@{item.concurrency}: {item.error}" for item in failed
                )

        if run_controls:
            for name, mode, expected in CONTROL_SCENARIOS:
                log_dir = evidence_dir / "logs" / "controls" / name
                code, control_samples, control_results = _run_benchmark_pass(
                    nats_url=nats_url,
                    producer_python=producer_py,
                    worker_python=worker_py,
                    producer_dir=producer_dir,
                    worker_dir=worker_dir,
                    workload="telemetry",
                    concurrency=1,
                    profile=profile,
                    worker_mode=mode,
                    log_dir=log_dir,
                    expect_worker_ok=mode != "incomplete",
                )
                control = ControlRecord(
                    name=name,
                    worker_mode=mode,
                    expected_exit=expected,
                    producer_exit=code,
                    samples=control_samples,
                )
                if code != expected:
                    control.error = f"exit {code} != expected {expected}"
                    control_records.append(control)
                    raise PerformanceError(f"{name}: {control.error}")
                control_records.append(control)
    except BaseException as exc:
        raised = exc
        error = str(exc)
        raise
    finally:
        broker_cleanup_error: BaseException | None = None
        broker_log = ""
        if owner is not None and owner.target is not None:
            try:
                owner.stop(owner.target)
            except BaseException as exc:
                broker_cleanup_error = exc
                if error is None:
                    error = f"Broker cleanup failed: {exc}"
            log_path = owner.target.log_path
            if log_path is not None and log_path.is_file():
                broker_log = log_path.read_text(encoding="utf-8")
        payload = {
            "python_version": python_version,
            "origins": origins,
            "environment_snapshot": environment_snapshot,
            "broker_store_path": str(broker_store_path) if broker_store_path else None,
            "error": error,
            "broker_log": broker_log,
            "combinations": [
                {
                    "workload": item.workload,
                    "concurrency": item.concurrency,
                    "inflight": item.inflight,
                    "producer_exit": item.producer_exit,
                    "baseline_eligible": item.baseline_eligible,
                    "effective_timing": item.effective_timing,
                    "observed_request_bytes": item.observed_request_bytes,
                    "producer_results_evidence": item.producer_results_evidence,
                    "samples": item.samples,
                    "error": item.error,
                }
                for item in combo_records
            ],
            "controls": [
                {
                    "name": item.name,
                    "worker_mode": item.worker_mode,
                    "expected_exit": item.expected_exit,
                    "producer_exit": item.producer_exit,
                    "samples": item.samples,
                    "error": item.error,
                }
                for item in control_records
            ],
        }
        _write_evidence(evidence_dir / "summary.json", payload)
        if broker_cleanup_error is not None and raised is None:
            raise PerformanceError(
                f"Broker cleanup failed: {broker_cleanup_error}",
            ) from broker_cleanup_error
    return payload


def collect_python_versions(args: argparse.Namespace) -> tuple[str, ...]:
    versions: list[str] = list(args.python_versions or [])
    if not versions:
        return (DEFAULT_RUNTIMES[0],)
    return tuple(dict.fromkeys(versions))


def _benchmark_outcome_error(combinations: list[dict[str, Any]]) -> str | None:
    messages: list[str] = []
    for combo in combinations:
        exit_code = int(combo.get("producer_exit") or 0)
        combo_error = combo.get("error")
        if exit_code != EXIT_OK or combo_error:
            label = f"{combo.get('workload')}@{combo.get('concurrency')}"
            if combo_error:
                messages.append(f"{label}: {combo_error}")
            elif exit_code != EXIT_OK:
                messages.append(f"{label}: producer exit {exit_code}")
    if not messages:
        return None
    return "; ".join(messages)


def _runtime_summary_path(artifact_dir: Path, python_version: str) -> Path:
    tag = runtime_evidence_tag(python_version)
    return artifact_dir / tag / "summary.json"


def _load_runtime_summary(artifact_dir: Path, python_version: str) -> dict[str, Any] | None:
    path = _runtime_summary_path(artifact_dir, python_version)
    if not path.is_file():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _absorb_runtime_payload(
    payload: dict[str, Any],
    *,
    report_combinations: list[dict[str, Any]],
    report_controls: list[dict[str, Any]],
    report_origins: dict[str, Any] | None,
    report_environment: dict[str, Any] | None,
    broker_store_path: Path | None,
    report_date: str,
) -> tuple[
    list[dict[str, Any]],
    list[dict[str, Any]],
    dict[str, Any] | None,
    dict[str, Any] | None,
    Path | None,
]:
    combinations = list(payload.get("combinations") or [])
    if combinations:
        report_combinations.extend(combinations)
    controls = list(payload.get("controls") or [])
    if controls:
        report_controls.extend(controls)
    origins = payload.get("origins")
    if origins:
        report_origins = origins
    snapshot = payload.get("environment_snapshot")
    if snapshot:
        enriched = dict(snapshot)
        enriched["report_date"] = report_date
        report_environment = enriched
    store_raw = payload.get("broker_store_path")
    if store_raw:
        broker_store_path = Path(store_raw)
    return (
        report_combinations,
        report_controls,
        report_origins,
        report_environment,
        broker_store_path,
    )


def _write_profile_report(
    path: Path,
    *,
    profile: str,
    baseline_eligible: bool,
    combinations: list[dict[str, Any]],
    controls: list[dict[str, Any]],
    repo_root: Path,
    origins: dict[str, Any] | None,
    broker_store_path: Path | None,
    report_date: str,
    environment: dict[str, Any] | None,
    run_success: bool,
    harness_exit_code: int,
    error: str | None,
) -> dict[str, Any]:
    report = build_report_payload(
        profile=profile,
        baseline_eligible=baseline_eligible,
        combinations=combinations,
        controls=controls,
        repo_root=repo_root,
        origins=origins,
        broker_store_path=broker_store_path,
        report_date=report_date,
        environment=environment,
    )
    report = finalize_run_outcome(
        report,
        run_success=run_success,
        harness_exit_code=harness_exit_code,
        error=error,
    )
    write_json_report(path, report)
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--python", action="append", dest="python_versions")
    parser.add_argument("--smoke", action="store_true", help="Short non-baseline profile.")
    parser.add_argument(
        "--diagnostic",
        action="store_true",
        help="Focused non-baseline profile (3x20s windows); ineligible for dated baselines.",
    )
    parser.add_argument(
        "--diagnostic-full-order",
        action="store_true",
        help=(
            "With --diagnostic, run telemetry/manifest at concurrency 1 and 8 "
            "in original baseline order on one broker (non-baseline)."
        ),
    )
    parser.add_argument(
        "--workload",
        choices=WORKLOADS,
        help="Single workload for --diagnostic (default manifest).",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        choices=CONCURRENCY_LEVELS,
        help="Single concurrency for --diagnostic (default 8).",
    )
    parser.add_argument(
        "--skip-contract-typing",
        action="store_true",
        help="Skip verify_contract_typing gate (not recommended).",
    )
    parser.add_argument("--artifact-dir", type=Path)
    parser.add_argument(
        "--baseline-dir",
        type=Path,
        default=REPO_ROOT / "docs" / "performance-baselines",
    )
    parser.add_argument("--keep-work", action="store_true")
    parser.add_argument("--work-dir", type=Path)
    parser.add_argument(
        "--report-date",
        default=date.today().isoformat(),
        help="Report date (YYYY-MM-DD) for baseline artifacts and environment metadata",
    )
    args = parser.parse_args(argv)

    if args.smoke and args.diagnostic:
        parser.error("--smoke and --diagnostic are mutually exclusive.")
    if not args.diagnostic and args.diagnostic_full_order:
        parser.error("--diagnostic-full-order requires --diagnostic.")
    if args.diagnostic_full_order and (
        args.workload is not None or args.concurrency is not None
    ):
        parser.error(
            "--diagnostic-full-order is incompatible with --workload and --concurrency.",
        )
    if (args.workload is not None or args.concurrency is not None) and not args.diagnostic:
        parser.error("--workload and --concurrency require --diagnostic.")

    profile_name = "diagnostic" if args.diagnostic else ("smoke" if args.smoke else "baseline")
    python_versions = collect_python_versions(args)
    if len(python_versions) != 1:
        parser.error("Select exactly one Python runtime per performance run.")
    parent = args.work_dir or Path(tempfile.gettempdir())
    parent.mkdir(parents=True, exist_ok=True)
    assert_work_parent_outside_repo(parent)
    work_dir = Path(tempfile.mkdtemp(prefix="superjobs-performance-", dir=parent))
    artifact_dir = (
        args.artifact_dir / f"run-{uuid.uuid4().hex}"
        if args.artifact_dir is not None
        else Path(tempfile.mkdtemp(prefix="superjobs-performance-artifacts-", dir=parent))
    )
    artifact_dir.mkdir(parents=True, exist_ok=True)
    owns_default_artifacts = args.artifact_dir is None

    if profile_name == "smoke":
        combinations = [("telemetry", 1)]
    elif profile_name == "diagnostic":
        if args.diagnostic_full_order:
            combinations = [
                ("telemetry", 1),
                ("telemetry", 8),
                ("manifest", 1),
                ("manifest", 8),
            ]
        else:
            workload = args.workload or "manifest"
            concurrency = args.concurrency or 8
            combinations = [(workload, concurrency)]
    else:
        combinations = [(workload, level) for workload in WORKLOADS for level in CONCURRENCY_LEVELS]

    exit_code = 0
    run_error: BaseException | None = None
    run_error_message: str | None = None
    report_combinations: list[dict[str, Any]] = []
    report_controls: list[dict[str, Any]] = []
    report_origins: dict[str, Any] | None = None
    report_environment: dict[str, Any] | None = None
    broker_store_path: Path | None = None
    report_baseline_eligible = profile_name == "baseline"
    diagnostic_failure = False
    benchmark_outcome_failed = False
    try:
        if not args.skip_contract_typing:
            run_performance_typing_gate()
            for py_version in python_versions:
                run_contract_typing_gate(py_version)
        library, contract = build_wheels(work_dir)
        for py_version in python_versions:
            payload = verify_python_version(
                python_version=py_version,
                work_dir=work_dir,
                library=library,
                contract=contract,
                artifact_dir=artifact_dir,
                profile=profile_name,
                run_controls=args.smoke,
                combinations=combinations,
            )
            report_combinations.extend(payload.get("combinations") or [])
            report_controls.extend(payload.get("controls") or [])
            if payload.get("origins"):
                report_origins = payload["origins"]
            if payload.get("environment_snapshot"):
                snapshot = dict(payload["environment_snapshot"])
                snapshot["report_date"] = args.report_date
                report_environment = snapshot
            for combo in payload.get("combinations") or []:
                if combo.get("baseline_eligible") is False:
                    report_baseline_eligible = False
                if profile_name == "diagnostic" and int(combo.get("producer_exit") or 0) != EXIT_OK:
                    diagnostic_failure = True
            store_raw = payload.get("broker_store_path")
            if store_raw:
                broker_store_path = Path(store_raw)
            if profile_name == "baseline" and payload.get("error"):
                benchmark_outcome_failed = True
        if profile_name == "baseline":
            outcome_error = _benchmark_outcome_error(report_combinations)
            if outcome_error:
                benchmark_outcome_failed = True
                run_error_message = outcome_error
                exit_code = EXIT_VALIDATION
                print(f"verify_performance: {outcome_error}", file=sys.stderr)
        if profile_name == "diagnostic" and diagnostic_failure:
            exit_code = EXIT_VALIDATION
            run_error_message = (
                "diagnostic run recorded producer validation failures; see failure_diagnostics"
            )
            run_error = PerformanceError(run_error_message)
    except (PerformanceError, VerificationError, KeyboardInterrupt) as exc:
        exit_code = 1
        run_error = exc
        run_error_message = str(exc)
        print(f"verify_performance: {exc}", file=sys.stderr)
        for py_version in python_versions:
            recovered = _load_runtime_summary(artifact_dir, py_version)
            if recovered is None:
                continue
            (
                report_combinations,
                report_controls,
                report_origins,
                report_environment,
                broker_store_path,
            ) = _absorb_runtime_payload(
                recovered,
                report_combinations=report_combinations,
                report_controls=report_controls,
                report_origins=report_origins,
                report_environment=report_environment,
                broker_store_path=broker_store_path,
                report_date=args.report_date,
            )
            if recovered.get("error") and run_error_message is None:
                run_error_message = str(recovered["error"])
    except BaseException as exc:
        exit_code = 1
        run_error = exc
        run_error_message = str(exc) if str(exc) else type(exc).__name__
        print(f"verify_performance: {exc}", file=sys.stderr)
        for py_version in python_versions:
            recovered = _load_runtime_summary(artifact_dir, py_version)
            if recovered is None:
                continue
            (
                report_combinations,
                report_controls,
                report_origins,
                report_environment,
                broker_store_path,
            ) = _absorb_runtime_payload(
                recovered,
                report_combinations=report_combinations,
                report_controls=report_controls,
                report_origins=report_origins,
                report_environment=report_environment,
                broker_store_path=broker_store_path,
                report_date=args.report_date,
            )
            if recovered.get("error") and run_error_message is None:
                run_error_message = str(recovered["error"])
    finally:
        print(f"Artifact directory: {artifact_dir}", file=sys.stderr)
        if run_error is not None:
            _write_evidence(artifact_dir / "run_error.json", {"error": str(run_error)})
        if args.keep_work:
            print(f"Work directory (inspection): {work_dir}", file=sys.stderr)
        elif work_dir.exists():
            shutil.rmtree(work_dir, ignore_errors=True)

    run_success = exit_code == EXIT_OK
    report_error = run_error_message
    if benchmark_outcome_failed and run_error_message:
        report_error = run_error_message

    if exit_code == 0 and report_baseline_eligible:
        stamp = args.report_date
        report = _write_profile_report(
            args.baseline_dir / f"{stamp}-baseline.json",
            profile=profile_name,
            baseline_eligible=True,
            combinations=report_combinations,
            controls=report_controls,
            repo_root=REPO_ROOT,
            origins=report_origins,
            broker_store_path=broker_store_path,
            report_date=stamp,
            environment=report_environment,
            run_success=True,
            harness_exit_code=EXIT_OK,
            error=None,
        )
        write_markdown_report(args.baseline_dir / f"{stamp}-baseline.md", report)
        print(f"Baseline written to {args.baseline_dir}")
    elif profile_name == "baseline" and not run_success:
        stamp = args.report_date
        report = _write_profile_report(
            args.baseline_dir / f"{stamp}-failed-baseline.json",
            profile=profile_name,
            baseline_eligible=False,
            combinations=report_combinations,
            controls=report_controls,
            repo_root=REPO_ROOT,
            origins=report_origins,
            broker_store_path=broker_store_path,
            report_date=stamp,
            environment=report_environment,
            run_success=False,
            harness_exit_code=exit_code,
            error=report_error,
        )
        write_markdown_report(args.baseline_dir / f"{stamp}-failed-baseline.md", report)
        print(
            f"Failed baseline attempt (ineligible) written to "
            f"{args.baseline_dir / f'{stamp}-failed-baseline.json'}",
            file=sys.stderr,
        )
    elif profile_name == "smoke":
        smoke_path = artifact_dir / "smoke-report.json"
        report = _write_profile_report(
            smoke_path,
            profile=profile_name,
            baseline_eligible=False,
            combinations=report_combinations,
            controls=report_controls,
            repo_root=REPO_ROOT,
            origins=report_origins,
            broker_store_path=broker_store_path,
            report_date=args.report_date,
            environment=report_environment,
            run_success=run_success,
            harness_exit_code=exit_code,
            error=report_error,
        )
        if run_success:
            print(f"Smoke report (non-baseline): {smoke_path}")
        else:
            print(f"Smoke report with harness failure (non-baseline): {smoke_path}", file=sys.stderr)
    elif profile_name == "diagnostic":
        diagnostic_path = artifact_dir / "diagnostic-report.json"
        report = _write_profile_report(
            diagnostic_path,
            profile=profile_name,
            baseline_eligible=False,
            combinations=report_combinations,
            controls=report_controls,
            repo_root=REPO_ROOT,
            origins=report_origins,
            broker_store_path=broker_store_path,
            report_date=args.report_date,
            environment=report_environment,
            run_success=run_success,
            harness_exit_code=exit_code,
            error=report_error,
        )
        _print_diagnostic_failure_summary(report_combinations, file=sys.stderr)
        if run_success:
            print(f"Diagnostic report (non-baseline): {diagnostic_path}")
        else:
            print(
                f"Diagnostic report with failures (non-baseline): {diagnostic_path}",
                file=sys.stderr,
            )

    if exit_code != 0:
        return exit_code
    if owns_default_artifacts and profile_name == "smoke":
        pass
    print("verify_performance: OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
