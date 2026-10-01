"""Installed-wheel producer for idle outage verification (long-lived runtime)."""

from __future__ import annotations

import asyncio
import os
import sys
import time
from pathlib import Path

from nats.errors import ConnectionClosedError, NoServersError
from nats.errors import TimeoutError as NatsTimeoutError
from nats.js.errors import ServiceUnavailableError

from superjobs import JobCompleted, JobSucceeded, SuperJobs
from superjobs.exceptions.jobs import JobNotFoundError
from superjobs_contract_example import ManifestRequest, ManifestResult

from broker_callbacks import make_broker_callbacks
from broker_connect import nats_broker, reconnect_settings_record
from protocol import (
    BASELINE_DONE,
    COMMAND_BASELINE,
    COMMAND_POST_OUTAGE,
    COMMAND_READ_AFTER_RECONNECT,
    COMMAND_READ_DURING_OUTAGE,
    HANDLE_READ_FAILED,
    HANDLE_READ_SUCCEEDED,
    POST_OUTAGE_DONE,
    BROKER_RECONNECT_SETTINGS,
    PRODUCER_RUNTIME_READY,
    ProducerRequest,
    checkpoint_path,
    producer_request_path,
    read_producer_request,
    read_worker_stop,
    stop_path,
    is_allowed_outage_read_error,
    write_checkpoint,
)
from queue_config import queue_config_for_run
from runtime_isolation import assert_producer_layout
from scenario_jobs import baseline_job, post_outage_job

FLOW_TIMEOUT = 30.0
RECOVER_TIMEOUT = 45.0
OUTAGE_READ_TIMEOUT = 2.0
_OUTAGE_TRANSPORT_ERRORS = (
    asyncio.TimeoutError,
    TimeoutError,
    NatsTimeoutError,
    ConnectionClosedError,
    NoServersError,
    ServiceUnavailableError,
)
READY_POLL_INTERVAL = 0.05
REQUEST_POLL_INTERVAL = 0.05

EXIT_OK = 0
EXIT_ASSERTION = 3


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        print(f"{name} must be set", file=sys.stderr)
        raise SystemExit(11)
    return value


async def _wait_for_worker_ready(state_dir: Path, run_id: str, timeout: float) -> None:
    from protocol import read_ready, ready_path

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


async def _poll_request(
    state_dir: Path,
    run_id: str,
    *,
    last_request_id: str | None,
) -> ProducerRequest | None:
    if stop_path(state_dir).is_file():
        marker = read_worker_stop(state_dir)
        if marker.run_id == run_id:
            return None
    request = read_producer_request(state_dir)
    if request is None:
        return None
    if request.run_id != run_id:
        raise AssertionError(f"producer request run_id {request.run_id!r} != {run_id!r}")
    if request.request_id == last_request_id:
        return None
    return request


async def _command_checkpoint_done(request: ProducerRequest) -> bool:
    name = {
        COMMAND_BASELINE: BASELINE_DONE,
        COMMAND_READ_DURING_OUTAGE: HANDLE_READ_FAILED,
        COMMAND_READ_AFTER_RECONNECT: HANDLE_READ_SUCCEEDED,
        COMMAND_POST_OUTAGE: POST_OUTAGE_DONE,
    }.get(request.command)
    if name is None:
        return False
    return checkpoint_path(
        Path(os.environ["SUPERJOBS_CROSS_STATE_DIR"]),
        name,
    ).is_file()


async def _wait_for_request(
    state_dir: Path,
    run_id: str,
    *,
    last_request_id: str | None,
) -> ProducerRequest | None:
    while True:
        if stop_path(state_dir).is_file():
            marker = read_worker_stop(state_dir)
            if marker.run_id == run_id:
                return None
        request = await _poll_request(state_dir, run_id, last_request_id=last_request_id)
        if request is not None:
            return request
        await asyncio.sleep(REQUEST_POLL_INTERVAL)


async def _run_baseline(
    jobs: SuperJobs,
    run_id: str,
    state_dir: Path,
    request: ProducerRequest,
) -> tuple[object, int]:
    job = baseline_job(run_id)
    handle = await jobs.client(job).submit(ManifestRequest(device_id="baseline-device"))
    result = await handle.result(wait_timeout=RECOVER_TIMEOUT)
    expected = ManifestResult(revision="idle-baseline-baseline-device")
    if result != expected:
        raise AssertionError(f"unexpected baseline result {result!r}")
    outcome = await handle.outcome(wait_timeout=RECOVER_TIMEOUT)
    if not isinstance(outcome, JobSucceeded):
        raise AssertionError(f"expected JobSucceeded, got {outcome!r}")
    write_checkpoint(
        state_dir,
        run_id=run_id,
        checkpoint=BASELINE_DONE,
        job=job.name,
        execution_id=handle.id,
        request_id=request.request_id,
        pid=os.getpid(),
        runtime_object_id=id(jobs),
        handle_object_id=id(handle),
    )
    print(f"producer baseline execution_id={handle.id}", flush=True)
    return handle, id(handle)


async def _run_read_during_outage(
    baseline_handle: object,
    run_id: str,
    state_dir: Path,
    request: ProducerRequest,
    *,
    jobs: SuperJobs,
    handle_object_id: int,
) -> None:
    started = time.monotonic()
    handle_id = baseline_handle.id
    try:
        async with asyncio.timeout(OUTAGE_READ_TIMEOUT):
            await baseline_handle.status()
    except JobNotFoundError as exc:
        raise AssertionError(
            "transport failure must not be reported as a missing Job",
        ) from exc
    except _OUTAGE_TRANSPORT_ERRORS as exc:
        error_type = type(exc).__name__
        if not is_allowed_outage_read_error(error_type):
            raise AssertionError(
                f"unexpected outage read error type {error_type!r}",
            ) from exc
        elapsed = time.monotonic() - started
        write_checkpoint(
            state_dir,
            run_id=run_id,
            checkpoint=HANDLE_READ_FAILED,
            execution_id=handle_id,
            handle_id=handle_id,
            request_id=request.request_id,
            error_type=error_type,
            elapsed_seconds=elapsed,
            pid=os.getpid(),
            runtime_object_id=id(jobs),
            handle_object_id=handle_object_id,
        )
        print(
            f"producer baseline read failed during outage: {error_type} "
            f"elapsed={elapsed:.3f}s handle_id={handle_id}",
            flush=True,
        )
        return
    except Exception as exc:
        raise AssertionError(
            f"unexpected outage read error {type(exc).__name__!r}",
        ) from exc
    raise AssertionError("baseline read during outage must not succeed")


async def _run_read_after_reconnect(
    baseline_handle: object,
    run_id: str,
    state_dir: Path,
    request: ProducerRequest,
    *,
    jobs: SuperJobs,
    handle_object_id: int,
) -> None:
    result = await baseline_handle.result(wait_timeout=RECOVER_TIMEOUT)
    expected = ManifestResult(revision="idle-baseline-baseline-device")
    if result != expected:
        raise AssertionError(f"unexpected recovered baseline result {result!r}")
    outcome = await baseline_handle.outcome(wait_timeout=RECOVER_TIMEOUT)
    if not isinstance(outcome, JobSucceeded):
        raise AssertionError(f"expected JobSucceeded outcome, got {outcome!r}")
    events = [event.data async for event in baseline_handle.events()]
    if not events or not isinstance(events[-1], JobCompleted):
        raise AssertionError("missing JobCompleted terminal observation after reconnect")
    write_checkpoint(
        state_dir,
        run_id=run_id,
        checkpoint=HANDLE_READ_SUCCEEDED,
        execution_id=baseline_handle.id,
        request_id=request.request_id,
        pid=os.getpid(),
        runtime_object_id=id(jobs),
        handle_object_id=handle_object_id,
    )
    print(f"producer recovered baseline execution_id={baseline_handle.id}", flush=True)


async def _run_post_outage(
    jobs: SuperJobs,
    run_id: str,
    state_dir: Path,
    request: ProducerRequest,
) -> None:
    job = post_outage_job(run_id)
    handle = await jobs.client(job).submit(ManifestRequest(device_id="post-device"))
    result = await handle.result(wait_timeout=RECOVER_TIMEOUT)
    expected = ManifestResult(revision="idle-post-post-device")
    if result != expected:
        raise AssertionError(f"unexpected post-outage result {result!r}")
    outcome = await handle.outcome(wait_timeout=RECOVER_TIMEOUT)
    if not isinstance(outcome, JobSucceeded):
        raise AssertionError(f"expected JobSucceeded, got {outcome!r}")
    write_checkpoint(
        state_dir,
        run_id=run_id,
        checkpoint=POST_OUTAGE_DONE,
        job=job.name,
        execution_id=handle.id,
        request_id=request.request_id,
        pid=os.getpid(),
    )
    print(f"producer post-outage execution_id={handle.id}", flush=True)


async def _run() -> None:
    nats_url = _require_env("NATS_URL")
    run_id = _require_env("SUPERJOBS_CROSS_RUN_ID")
    state_dir = Path(_require_env("SUPERJOBS_CROSS_STATE_DIR"))
    assert_producer_layout(Path(__file__).resolve().parent)
    await _wait_for_worker_ready(state_dir, run_id, FLOW_TIMEOUT)

    jobs_ref: dict[str, SuperJobs | None] = {"jobs": None}
    disconnected_cb, reconnected_cb = make_broker_callbacks(
        state_dir,
        run_id,
        role="producer",
        runtime_object_id=lambda: id(jobs_ref["jobs"]),
    )
    jobs = SuperJobs(
        broker=nats_broker(
            nats_url,
            disconnected_cb=disconnected_cb,
            reconnected_cb=reconnected_cb,
        ),
        queue_config=queue_config_for_run(run_id),
    )
    jobs_ref["jobs"] = jobs
    await asyncio.wait_for(jobs.start(), timeout=FLOW_TIMEOUT)
    baseline_handle: object | None = None
    baseline_handle_object_id: int | None = None
    last_request_id: str | None = None
    try:
        write_checkpoint(
            state_dir,
            run_id=run_id,
            checkpoint=PRODUCER_RUNTIME_READY,
            pid=os.getpid(),
            runtime_object_id=id(jobs),
        )
        write_checkpoint(
            state_dir,
            run_id=run_id,
            checkpoint=BROKER_RECONNECT_SETTINGS,
            pid=os.getpid(),
            status_state=str(reconnect_settings_record()),
        )
        print(f"producer runtime ready run_id={run_id}", flush=True)
        while True:
            request = await _wait_for_request(
                state_dir,
                run_id,
                last_request_id=last_request_id,
            )
            if request is None:
                break
            if await _command_checkpoint_done(request):
                last_request_id = request.request_id
                continue
            if request.command == COMMAND_BASELINE:
                baseline_handle, baseline_handle_object_id = await _run_baseline(
                    jobs,
                    run_id,
                    state_dir,
                    request,
                )
            elif request.command == COMMAND_READ_DURING_OUTAGE:
                if baseline_handle is None or baseline_handle_object_id is None:
                    raise AssertionError("baseline handle missing before outage read")
                await _run_read_during_outage(
                    baseline_handle,
                    run_id,
                    state_dir,
                    request,
                    jobs=jobs,
                    handle_object_id=baseline_handle_object_id,
                )
            elif request.command == COMMAND_READ_AFTER_RECONNECT:
                if baseline_handle is None or baseline_handle_object_id is None:
                    raise AssertionError("baseline handle missing before reconnect read")
                await _run_read_after_reconnect(
                    baseline_handle,
                    run_id,
                    state_dir,
                    request,
                    jobs=jobs,
                    handle_object_id=baseline_handle_object_id,
                )
            elif request.command == COMMAND_POST_OUTAGE:
                await _run_post_outage(jobs, run_id, state_dir, request)
            else:
                raise AssertionError(f"unknown producer command {request.command!r}")
            last_request_id = request.request_id
            producer_request_path(state_dir).unlink(missing_ok=True)
        print(f"producer ok run_id={run_id}", flush=True)
    finally:
        await asyncio.wait_for(jobs.stop(), timeout=FLOW_TIMEOUT)


def main() -> None:
    try:
        asyncio.run(_run())
    except AssertionError as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(EXIT_ASSERTION) from exc
    except Exception as exc:
        print(f"producer failed: {exc!r}", file=sys.stderr)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
