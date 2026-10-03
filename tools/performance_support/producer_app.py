"""Installed-wheel producer for performance baselines."""

from __future__ import annotations

import asyncio
import os
import sys
import time
from pathlib import Path
from typing import Any

from faststream.nats import NatsBroker

from superjobs import (
    JobCancelled,
    JobCompleted,
    JobFailed,
    JobFailedOutcome,
    JobLog,
    JobProgress,
    JobRetryScheduled,
    JobStarted,
    JobSucceeded,
    SuperJobs,
)

from accounting import (
    CompletionRecord,
    OutcomeBucket,
    SampleAccounting,
    SampleWindow,
    aggregate_exit_code,
    classify_terminal_time,
)
from config import (
    EXIT_ASSERTION,
    EXIT_ENV,
    EXIT_VALIDATION,
    MANIFEST_EVENT_STAGES,
    MANIFEST_EXPECTED_REVISION,
    WorkloadName,
    concurrency_from_env,
    inflight_from_env,
    timing_from_env,
    worker_mode_from_env,
    workload_from_env,
)
from contracts import (
    PERFORMANCE_MANIFEST_JOB,
    PERFORMANCE_TELEMETRY_JOB,
    PerformanceManifestEvent,
    PerformanceManifestResult,
)
from fixtures import (
    build_manifest_request,
    build_telemetry_request,
    measure_fixture_byte_range,
    measure_fixture_sizes,
)
from protocol import read_ready, ready_path, write_producer_results
from runtime_isolation import assert_producer_layout
from scheduling import SampleRunConfig, run_sample

FLOW_TIMEOUT = 120.0
READY_POLL_INTERVAL = 0.05
TERMINAL_WAIT = 30.0

_KNOWN_SYSTEM_EVENT_TYPES = (
    JobStarted,
    JobCompleted,
    JobFailed,
    JobCancelled,
    JobRetryScheduled,
    JobLog,
    JobProgress,
)


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        print(f"{name} must be set", file=sys.stderr)
        raise SystemExit(EXIT_ENV)
    return value


async def _wait_for_worker_ready(state_dir: Path, run_id: str, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        path = ready_path(state_dir)
        if path.is_file():
            marker = read_ready(state_dir)
            if marker.run_id != run_id:
                raise AssertionError(
                    f"ready run_id {marker.run_id!r} != expected {run_id!r}",
                )
            return
        await asyncio.sleep(READY_POLL_INTERVAL)
    raise TimeoutError(f"worker did not become ready within {timeout}s")


def _unexpected_event_error(event: object) -> str:
    return f"unexpected observation {event!r}"


async def _validate_telemetry(handle) -> str | None:
    outcome = await handle.outcome(wait_timeout=TERMINAL_WAIT)
    if not isinstance(outcome, JobSucceeded):
        return f"telemetry outcome {outcome!r}"
    try:
        result = await handle.result(wait_timeout=TERMINAL_WAIT)
    except Exception as exc:
        return f"telemetry result raised {exc!r}"
    if result is not None:
        return f"telemetry unexpected result payload {result!r}"
    events = [event.data async for event in handle.events()]
    if len(events) != 2:
        return f"telemetry expected 2 system events, got {len(events)}"
    if not isinstance(events[0], JobStarted):
        return _unexpected_event_error(events[0])
    if not isinstance(events[1], JobCompleted):
        return _unexpected_event_error(events[1])
    return None


async def _validate_manifest(handle) -> str | None:
    result = await handle.result(wait_timeout=TERMINAL_WAIT)
    if result != PerformanceManifestResult(revision=MANIFEST_EXPECTED_REVISION):
        return f"manifest unexpected result {result!r}"
    events = [event.data async for event in handle.events()]
    expected_types: tuple[type, ...] = (
        JobStarted,
        PerformanceManifestEvent,
        PerformanceManifestEvent,
        PerformanceManifestEvent,
        JobCompleted,
    )
    if len(events) != len(expected_types):
        return f"manifest expected {len(expected_types)} events, got {len(events)}"
    app_events: list[PerformanceManifestEvent] = []
    for index, (event, expected_type) in enumerate(zip(events, expected_types, strict=True)):
        if not isinstance(event, expected_type):
            return f"manifest event {index} type {type(event)!r} != {expected_type!r}"
        if isinstance(event, PerformanceManifestEvent):
            app_events.append(event)
    stages = [event.stage for event in app_events]
    if stages != list(MANIFEST_EVENT_STAGES):
        return f"manifest application event order {stages!r}"
    for event in events:
        if isinstance(event, PerformanceManifestEvent):
            continue
        if isinstance(event, _KNOWN_SYSTEM_EVENT_TYPES):
            continue
        return _unexpected_event_error(event)
    outcome = await handle.outcome(wait_timeout=TERMINAL_WAIT)
    if not isinstance(outcome, JobSucceeded):
        return f"manifest outcome {outcome!r}"
    return None


async def _run_sample(
    jobs: SuperJobs,
    *,
    workload: WorkloadName,
    inflight: int,
    sample_seconds: float,
    drain_seconds: float,
    sequence_start: int,
    request_bytes_seen: dict[str, int],
) -> tuple[SampleAccounting, int]:
    client = jobs.client(
        PERFORMANCE_TELEMETRY_JOB if workload == "telemetry" else PERFORMANCE_MANIFEST_JOB,
    )
    sample_accounting = SampleAccounting(window=SampleWindow(0.0, 0.0))
    workload_key = "telemetry" if workload == "telemetry" else "manifest"

    def _record_fixture_bytes(sequences: range) -> None:
        minimum, maximum = measure_fixture_byte_range(workload_key, sequences=sequences)
        request_bytes_seen["min"] = min(request_bytes_seen.get("min", minimum), minimum)
        request_bytes_seen["max"] = max(request_bytes_seen.get("max", maximum), maximum)

    async def submit_one(seq: int):
        if workload == "telemetry":
            request = build_telemetry_request(seq)
        else:
            request = build_manifest_request(seq)
        return await client.submit(request)

    async def track(
        handle,
        *,
        submit_started: float,
        submit_finished: float | None,
        seq: int,
        boundary_mono: float,
        terminal_wait: float,
        sem: asyncio.Semaphore,
        release_sem: bool,
    ) -> None:
        bucket: OutcomeBucket | None = None
        terminal_mono: float | None = None
        detail: str | None = None
        try:
            outcome = await handle.outcome(wait_timeout=terminal_wait)
            terminal_mono = time.monotonic()
            if isinstance(outcome, JobFailedOutcome):
                bucket = "failure"
                detail = outcome.error.code
            else:
                if workload == "telemetry":
                    validation_error = await _validate_telemetry(handle)
                else:
                    validation_error = await _validate_manifest(handle)
                if validation_error:
                    bucket = "validation_error"
                    detail = validation_error
                else:
                    bucket = classify_terminal_time(terminal_mono, boundary_mono)
        except TimeoutError:
            if bucket is None:
                bucket = "incomplete_drain"
                detail = "terminal wait timed out"
        except asyncio.CancelledError:
            if bucket is None:
                bucket = "incomplete_drain"
                detail = "cancelled during drain"
        except Exception as exc:
            bucket = "failure"
            detail = str(exc)
        finally:
            if release_sem:
                sem.release()
            if bucket is None:
                bucket = "incomplete_drain"
                detail = detail or "unfinished"
            sample_accounting.add(
                CompletionRecord(
                        sequence=seq,
                        submit_started_mono=submit_started,
                        submit_finished_mono=submit_finished,
                        terminal_mono=terminal_mono,
                        bucket=bucket,
                        detail=detail,
                ),
            )

    config = SampleRunConfig(
        inflight=inflight,
        sample_seconds=sample_seconds,
        drain_seconds=drain_seconds,
        sequence_start=sequence_start,
        submit_timeout_seconds=FLOW_TIMEOUT,
        terminal_wait_seconds=TERMINAL_WAIT,
    )

    _record_fixture_bytes(range(sequence_start, sequence_start + 1))

    accounting, sequence = await run_sample(
        submit_one,
        track,
        config=config,
        accounting=sample_accounting,
    )
    _record_fixture_bytes(range(sequence_start, sequence))
    return accounting, sequence


async def _run() -> None:
    assert_producer_layout(Path(__file__).resolve().parent)

    nats_url = _require_env("NATS_URL")
    run_id = _require_env("SUPERJOBS_PERF_RUN_ID")
    state_dir = Path(_require_env("SUPERJOBS_PERF_STATE_DIR"))
    await _wait_for_worker_ready(state_dir, run_id, FLOW_TIMEOUT)

    timing = timing_from_env()
    workload = workload_from_env()
    concurrency = concurrency_from_env()
    inflight = inflight_from_env(concurrency)
    if inflight != concurrency:
        raise AssertionError(
            f"inflight {inflight} must match worker concurrency {concurrency} for this baseline",
        )
    worker_mode = worker_mode_from_env()
    expect_success = worker_mode in ("success", "delay")

    jobs = SuperJobs(broker=NatsBroker(nats_url, connect_timeout=5))
    payload: dict[str, Any] | None = None
    exit_code = 1
    lifecycle_cleanup_error: str | None = None
    await jobs.start()
    try:
        sequence = 0
        client = jobs.client(
            PERFORMANCE_TELEMETRY_JOB if workload == "telemetry" else PERFORMANCE_MANIFEST_JOB,
        )
        if worker_mode in ("success", "delay"):
            warmup_deadline = time.monotonic() + timing.warmup_seconds
            while time.monotonic() < warmup_deadline:
                if workload == "telemetry":
                    handle = await client.submit(build_telemetry_request(sequence))
                else:
                    handle = await client.submit(build_manifest_request(sequence))
                sequence += 1
                error = (
                    await _validate_telemetry(handle)
                    if workload == "telemetry"
                    else await _validate_manifest(handle)
                )
                if error:
                    raise AssertionError(f"warmup validation failed: {error}")

        samples: list[SampleAccounting] = []
        request_bytes_seen: dict[str, int] = {}
        for _ in range(timing.sample_count):
            sample, sequence = await _run_sample(
                jobs,
                workload=workload,
                inflight=inflight,
                sample_seconds=timing.sample_seconds,
                drain_seconds=timing.drain_seconds,
                sequence_start=sequence,
                request_bytes_seen=request_bytes_seen,
            )
            samples.append(sample)

        exit_code = aggregate_exit_code(
            samples,
            expect_success=expect_success,
            allow_incomplete_drain=worker_mode == "incomplete",
        )
        fixture_sizes = measure_fixture_sizes()
        payload = {
            "run_id": run_id,
            "workload": workload,
            "concurrency": concurrency,
            "inflight": inflight,
            "worker_mode": worker_mode,
            "profile": timing.name,
            "baseline_eligible": timing.baseline_eligible,
            "effective_timing": {
                "warmup_seconds": timing.warmup_seconds,
                "sample_seconds": timing.sample_seconds,
                "sample_count": timing.sample_count,
                "drain_seconds": timing.drain_seconds,
            },
            "fixture_bytes": {
                "telemetry_request_bytes": fixture_sizes.telemetry_request_bytes,
                "manifest_request_bytes": fixture_sizes.manifest_request_bytes,
            },
            "observed_request_bytes": request_bytes_seen or None,
            "samples": [sample.summarize() for sample in samples],
            "exit_code": exit_code,
        }
    finally:
        try:
            await asyncio.wait_for(jobs.stop(), timeout=30.0)
        except TimeoutError:
            lifecycle_cleanup_error = "jobs.stop timed out after 30s"
        except Exception as exc:
            lifecycle_cleanup_error = f"jobs.stop failed: {exc!r}"
        if payload is not None:
            if lifecycle_cleanup_error is not None:
                payload["lifecycle_cleanup_error"] = lifecycle_cleanup_error
                payload["exit_code"] = EXIT_VALIDATION
            write_producer_results(state_dir, payload)
    if lifecycle_cleanup_error is not None:
        print(lifecycle_cleanup_error, file=sys.stderr)
        raise SystemExit(EXIT_VALIDATION)
    raise SystemExit(exit_code)


def main() -> None:
    try:
        asyncio.run(_run())
    except AssertionError as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(EXIT_ASSERTION) from exc
    except SystemExit:
        raise
    except Exception as exc:
        print(f"performance producer failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
