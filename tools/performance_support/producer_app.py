"""Installed-wheel producer for performance baselines."""

from __future__ import annotations

import asyncio
import os
import sys
import time
from pathlib import Path
from typing import Any, NamedTuple

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
    FailurePhase,
    OutcomeBucket,
    SampleAccounting,
    SampleWindow,
    aggregate_exit_code,
    classify_terminal_time,
    exception_fields,
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
from measurement_clock import metadata as measurement_clock_metadata
from measurement_clock import now as measurement_now
from protocol import read_ready, ready_path, write_producer_results
from runtime_isolation import assert_producer_layout
from scheduling import SampleRunConfig, run_sample

FLOW_TIMEOUT = 120.0
READY_POLL_INTERVAL = 0.05
TERMINAL_WAIT = 30.0
CAPTURE_REPLAY_EVIDENCE_ENV = "SUPERJOBS_PERF_CAPTURE_REPLAY_EVIDENCE"
REPLAY_EVIDENCE_TIMEOUT_SECONDS = 15.0
REPLAY_EVIDENCE_MAX_EVENTS = 64


def _capture_replay_evidence_enabled() -> bool:
    return os.environ.get(CAPTURE_REPLAY_EVIDENCE_ENV) == "1"

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


class ValidationFailure(NamedTuple):
    detail: str
    phase: FailurePhase
    source_exc: BaseException | None = None


def _validation_ok() -> ValidationFailure | None:
    return None


def _validation_fail(
    detail: str,
    phase: FailurePhase,
    *,
    source_exc: BaseException | None = None,
) -> ValidationFailure:
    if source_exc is not None:
        fields = exception_fields(source_exc)
        detail = fields["detail"] or detail
    return ValidationFailure(detail=detail, phase=phase, source_exc=source_exc)


def _execution_id(handle) -> str | None:
    job_id = getattr(handle, "job_id", None)
    if job_id is None:
        return None
    return str(job_id)


async def _validate_telemetry_after_outcome(handle) -> ValidationFailure | None:
    try:
        result = await handle.result(wait_timeout=TERMINAL_WAIT)
    except Exception as exc:
        return _validation_fail(
            f"telemetry result raised {exc!r}",
            "validation_result",
            source_exc=exc,
        )
    if result is not None:
        return _validation_fail(
            f"telemetry unexpected result payload {result!r}",
            "validation_result",
        )
    try:
        events = [event.data async for event in handle.events()]
    except Exception as exc:
        return _validation_fail(
            f"telemetry events raised {exc!r}",
            "validation_events",
            source_exc=exc,
        )
    if len(events) != 2:
        return _validation_fail(
            f"telemetry expected 2 system events, got {len(events)}",
            "validation_events",
        )
    if not isinstance(events[0], JobStarted):
        return _validation_fail(_unexpected_event_error(events[0]), "validation_events")
    if not isinstance(events[1], JobCompleted):
        return _validation_fail(_unexpected_event_error(events[1]), "validation_events")
    return _validation_ok()


async def _validate_telemetry(handle) -> ValidationFailure | None:
    outcome = await handle.outcome(wait_timeout=TERMINAL_WAIT)
    if not isinstance(outcome, JobSucceeded):
        return _validation_fail(f"telemetry outcome {outcome!r}", "validation_outcome")
    return await _validate_telemetry_after_outcome(handle)


async def _collect_replay_evidence_inner(handle) -> dict[str, Any]:
    evidence: dict[str, Any] = {}
    for delay in (0.0, 0.1, 0.5):
        if delay:
            await asyncio.sleep(delay)
        key = f"events_retry_after_{delay}s"
        collected: list[dict[str, Any]] = []
        try:
            async for event in handle.events():
                if len(collected) >= REPLAY_EVIDENCE_MAX_EVENTS:
                    evidence[key] = {
                        "ok": False,
                        "truncated": True,
                        "events": collected,
                    }
                    break
                collected.append(
                    {
                        "sequence": event.sequence,
                        "type": type(event.data).__qualname__,
                    },
                )
            else:
                evidence[key] = {"ok": True, "events": collected}
        except Exception as exc:
            evidence[key] = {"ok": False, **exception_fields(exc)}
    for after in (0, 1):
        key = f"events_after_cursor_{after}"
        sequences: list[int] = []
        try:
            async for event in handle.events(after=after):
                if len(sequences) >= REPLAY_EVIDENCE_MAX_EVENTS:
                    evidence[key] = {
                        "ok": False,
                        "truncated": True,
                        "sequences": sequences,
                    }
                    break
                sequences.append(event.sequence)
            else:
                evidence[key] = {"ok": True, "sequences": sequences}
        except Exception as exc:
            evidence[key] = {"ok": False, **exception_fields(exc)}
    return evidence


async def _collect_replay_evidence(handle) -> dict[str, Any]:
    try:
        return await asyncio.wait_for(
            _collect_replay_evidence_inner(handle),
            timeout=REPLAY_EVIDENCE_TIMEOUT_SECONDS,
        )
    except TimeoutError:
        return {"ok": False, "detail": "replay evidence collection timed out"}


async def _observation_consumer_at_failure(
    jobs: SuperJobs,
    handle,
    *,
    job,
) -> dict[str, Any]:
    from nats.js.errors import NotFoundError

    from superjobs.transport.nats_backend import NatsJobBackend
    from superjobs.transport.transport import Source

    execution_id = _execution_id(handle)
    if execution_id is None:
        return {"available": False, "reason": "missing execution_id"}
    transport = jobs.transport
    if not isinstance(transport, NatsJobBackend):
        return {"available": False, "reason": "not a NATS backend"}
    subject = transport._observation_subject(job.identity, execution_id)
    source = Source(
        name=subject,
        stream=transport.queue_config.observation_stream,
    )
    consumer_name = transport._transport.consumer_name(source)
    payload: dict[str, Any] = {
        "observation_subject": subject,
        "observation_stream": transport.queue_config.observation_stream,
        "expected_durable_consumer": consumer_name,
    }
    if consumer_name is None:
        payload["note"] = "non-durable observation consumer; broker may not list it after close"
        return payload
    jetstream = transport.broker.connection.jetstream()
    try:
        info = await jetstream.consumer_info(
            transport.queue_config.observation_stream,
            consumer_name,
        )
    except NotFoundError:
        payload["consumer_info"] = {"present": False}
        payload["note"] = "durable consumer not found at failure point (may be ephemeral)"
        return payload
    except Exception as exc:
        payload["consumer_info_error"] = repr(exc)
        return payload
    config = info.config
    payload["consumer_info"] = {
        "present": True,
        "name": info.name,
        "filter_subject": config.filter_subject,
        "filter_subjects": list(config.filter_subjects or ()),
        "ack_policy": str(config.ack_policy),
        "deliver_policy": str(config.deliver_policy),
        "num_pending": info.num_pending,
        "num_ack_pending": info.num_ack_pending,
    }
    return payload


async def _bounded_observation_consumer_at_failure(
    jobs: SuperJobs,
    handle,
    *,
    job,
) -> dict[str, Any]:
    try:
        return await asyncio.wait_for(
            _observation_consumer_at_failure(jobs, handle, job=job),
            timeout=REPLAY_EVIDENCE_TIMEOUT_SECONDS,
        )
    except TimeoutError:
        return {"ok": False, "detail": "observation consumer probe timed out"}
    except Exception as exc:
        return {"ok": False, **exception_fields(exc)}


async def _collect_validation_failure_replay_evidence(
    jobs: SuperJobs,
    handle,
    *,
    job,
) -> dict[str, Any]:
    evidence: dict[str, Any] = {}
    try:
        evidence["observation_consumer"] = await _bounded_observation_consumer_at_failure(
            jobs,
            handle,
            job=job,
        )
    except Exception as exc:
        evidence["observation_consumer"] = {"ok": False, **exception_fields(exc)}
    try:
        evidence.update(await _collect_replay_evidence(handle))
    except Exception as exc:
        evidence["replay_collection_error"] = exception_fields(exc)
    return evidence


async def _validate_manifest(handle) -> ValidationFailure | None:
    try:
        result = await handle.result(wait_timeout=TERMINAL_WAIT)
    except Exception as exc:
        return _validation_fail(
            f"manifest result raised {exc!r}",
            "validation_result",
            source_exc=exc,
        )
    if result != PerformanceManifestResult(revision=MANIFEST_EXPECTED_REVISION):
        return _validation_fail(
            f"manifest unexpected result {result!r}",
            "validation_result",
        )
    try:
        events = [event.data async for event in handle.events()]
    except Exception as exc:
        return _validation_fail(
            f"manifest events raised {exc!r}",
            "validation_events",
            source_exc=exc,
        )
    expected_types: tuple[type, ...] = (
        JobStarted,
        PerformanceManifestEvent,
        PerformanceManifestEvent,
        PerformanceManifestEvent,
        JobCompleted,
    )
    if len(events) != len(expected_types):
        return _validation_fail(
            f"manifest expected {len(expected_types)} events, got {len(events)}",
            "validation_events",
        )
    app_events: list[PerformanceManifestEvent] = []
    for index, (event, expected_type) in enumerate(zip(events, expected_types, strict=True)):
        if not isinstance(event, expected_type):
            return _validation_fail(
                f"manifest event {index} type {type(event)!r} != {expected_type!r}",
                "validation_events",
            )
        if isinstance(event, PerformanceManifestEvent):
            app_events.append(event)
    stages = [event.stage for event in app_events]
    if stages != list(MANIFEST_EVENT_STAGES):
        return _validation_fail(
            f"manifest application event order {stages!r}",
            "validation_events",
        )
    for event in events:
        if isinstance(event, PerformanceManifestEvent):
            continue
        if isinstance(event, _KNOWN_SYSTEM_EVENT_TYPES):
            continue
        return _validation_fail(_unexpected_event_error(event), "validation_events")
    outcome = await handle.outcome(wait_timeout=TERMINAL_WAIT)
    if not isinstance(outcome, JobSucceeded):
        return _validation_fail(f"manifest outcome {outcome!r}", "validation_outcome")
    return _validation_ok()


def _completion_from_failure(
    *,
    handle,
    seq: int,
    submit_started: float,
    submit_finished: float | None,
    terminal_mono: float | None,
    bucket: OutcomeBucket,
    phase: FailurePhase,
    detail: str | None,
    outcome: JobFailedOutcome | None = None,
    exc: BaseException | None = None,
) -> CompletionRecord:
    record_kwargs: dict[str, Any] = {
        "sequence": seq,
        "submit_started_mono": submit_started,
        "submit_finished_mono": submit_finished,
        "terminal_mono": terminal_mono,
        "bucket": bucket,
        "phase": phase,
        "execution_id": _execution_id(handle) if handle is not None else None,
        "detail": detail,
    }
    if outcome is not None:
        record_kwargs["job_error_code"] = outcome.error.code
        record_kwargs["job_error_message"] = outcome.error.message
    if exc is not None:
        record_kwargs.update(exception_fields(exc))
    return CompletionRecord(**record_kwargs)


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
        phase: FailurePhase | None = None
        detail: str | None = None
        failed_outcome: JobFailedOutcome | None = None
        track_exc: BaseException | None = None
        validation_exc: BaseException | None = None
        replay_evidence: dict[str, Any] | None = None
        try:
            outcome = await handle.outcome(wait_timeout=terminal_wait)
            terminal_mono = measurement_now()
            if isinstance(outcome, JobFailedOutcome):
                bucket = "failure"
                phase = "outcome"
                detail = outcome.error.code
                failed_outcome = outcome
            else:
                validation_failure = (
                    await _validate_telemetry(handle)
                    if workload == "telemetry"
                    else await _validate_manifest(handle)
                )
                if validation_failure is not None:
                    bucket = "validation_error"
                    detail = validation_failure.detail
                    phase = validation_failure.phase
                    validation_exc = validation_failure.source_exc
                    if (
                        phase == "validation_events"
                        and handle is not None
                        and _capture_replay_evidence_enabled()
                    ):
                        job = (
                            PERFORMANCE_TELEMETRY_JOB
                            if workload == "telemetry"
                            else PERFORMANCE_MANIFEST_JOB
                        )
                        replay_evidence = await _collect_validation_failure_replay_evidence(
                            jobs,
                            handle,
                            job=job,
                        )
                else:
                    bucket = classify_terminal_time(terminal_mono, boundary_mono)
        except TimeoutError:
            if bucket is None:
                bucket = "incomplete_drain"
                phase = "outcome"
                detail = "terminal wait timed out"
        except asyncio.CancelledError:
            if bucket is None:
                bucket = "incomplete_drain"
                phase = "cleanup"
                detail = "cancelled during drain"
        except Exception as exc:
            bucket = "failure"
            phase = "outcome"
            track_exc = exc
            detail = str(exc)
        finally:
            if release_sem:
                sem.release()
            if bucket is None:
                bucket = "incomplete_drain"
                phase = "cleanup"
                detail = detail or "unfinished"
            if bucket in ("success_in_window", "success_late_drain"):
                sample_accounting.add(
                    CompletionRecord(
                        sequence=seq,
                        submit_started_mono=submit_started,
                        submit_finished_mono=submit_finished,
                        terminal_mono=terminal_mono,
                        bucket=bucket,
                    ),
                )
            else:
                record_exc = validation_exc if bucket == "validation_error" else track_exc
                failure_record = _completion_from_failure(
                    handle=handle,
                    seq=seq,
                    submit_started=submit_started,
                    submit_finished=submit_finished,
                    terminal_mono=terminal_mono,
                    bucket=bucket,
                    phase=(
                        phase
                        if phase is not None
                        else (
                            "submit"
                            if bucket in ("submit_failed", "submit_uncertain")
                            else "outcome"
                        )
                    ),
                    detail=detail,
                    outcome=failed_outcome,
                    exc=record_exc,
                )
                if bucket == "validation_error" and phase == "validation_events":
                    failure_record.replay_evidence = replay_evidence
                sample_accounting.add(failure_record)

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
            warmup_deadline = measurement_now() + timing.warmup_seconds
            while measurement_now() < warmup_deadline:
                if workload == "telemetry":
                    handle = await client.submit(build_telemetry_request(sequence))
                else:
                    handle = await client.submit(build_manifest_request(sequence))
                sequence += 1
                failure = (
                    await _validate_telemetry(handle)
                    if workload == "telemetry"
                    else await _validate_manifest(handle)
                )
                if failure is not None:
                    raise AssertionError(f"warmup validation failed: {failure.detail}")

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
            "measurement_clock": measurement_clock_metadata(),
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
                cleanup_sample = samples[-1] if samples else None
                if cleanup_sample is not None:
                    cleanup_sample.add(
                        CompletionRecord(
                            sequence=-1,
                            submit_started_mono=measurement_now(),
                            submit_finished_mono=None,
                            terminal_mono=measurement_now(),
                            bucket="validation_error",
                            phase="cleanup",
                            detail=lifecycle_cleanup_error,
                        ),
                    )
                    payload["samples"] = [sample.summarize() for sample in samples]
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
