"""NATS worker handlers for the contract-interface CLI example."""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path

from superjobs import JobContext, SuperJobs
from superjobs_contract_example import (
    CLI_FAIL_JOB,
    CLI_GATE_JOB,
    HEARTBEAT_JOB,
    MANIFEST_NO_EVENTS_JOB,
    MANIFEST_WITH_EVENTS_JOB,
    TELEMETRY_INGEST_JOB,
    CliFailRequest,
    CliGateRequest,
    CliGateResult,
    HeartbeatResult,
    ManifestEvent,
    ManifestNoEventsRequest,
    ManifestNoEventsResult,
    ManifestRequest,
    ManifestResult,
    TelemetrySample,
)


def _sync_dir() -> Path:
    raw = os.environ.get("SUPERJOBS_CLI_SYNC_DIR") or os.environ.get("SUPERJOBS_CLI_STATE_DIR")
    if not raw:
        raise RuntimeError("SUPERJOBS_CLI_SYNC_DIR or SUPERJOBS_CLI_STATE_DIR must be set")
    return Path(raw)


def _write_json_atomic(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def _write_checkpoint(
    name: str,
    *,
    gate: str | None = None,
    execution_id: str | None = None,
    run_id: str | None = None,
) -> None:
    payload: dict[str, object] = {"checkpoint": name}
    if gate is not None:
        payload["gate"] = gate
    if execution_id is not None:
        payload["execution_id"] = execution_id
    if run_id is not None:
        payload["run_id"] = run_id
    if gate is not None:
        path = _sync_dir() / f"checkpoint_{name}_{gate}.json"
    else:
        path = _sync_dir() / f"checkpoint_{name}.json"
    _write_json_atomic(path, payload)


async def _wait_for_release(gate: str, *, poll_interval: float = 0.05) -> None:
    path = _sync_dir() / f"release_{gate}.json"
    while not path.is_file():
        await asyncio.sleep(poll_interval)


def register_contract_handlers(jobs: SuperJobs) -> None:
    @jobs.handler(MANIFEST_WITH_EVENTS_JOB)
    async def manifest_with_events(
        request: ManifestRequest,
        context: JobContext[ManifestEvent],
    ) -> ManifestResult:
        await context.log("phase", extra={"worker_pid": os.getpid()})
        await context.emit(ManifestEvent(stage="mid"))
        await context.progress(completed=1, total=1)
        return ManifestResult(revision=f"{request.device_id}-r1")

    async def manifest_no_events_impl(
        request: ManifestNoEventsRequest,
        context: JobContext[None],
    ) -> ManifestNoEventsResult:
        await context.log(f"remote bundle {request.bundle_id}")
        return ManifestNoEventsResult(accepted=True)

    jobs.register(MANIFEST_NO_EVENTS_JOB, manifest_no_events_impl)

    @TELEMETRY_INGEST_JOB.handler
    async def telemetry_ingest(
        request: TelemetrySample,
        context: JobContext[None],
    ) -> None:
        await context.log(f"telemetry {request.metric}={request.value}")
        return None

    jobs.register(telemetry_ingest)

    @HEARTBEAT_JOB.handler
    async def heartbeat(context: JobContext[None]) -> HeartbeatResult:
        return HeartbeatResult(ok=True)

    jobs.register(heartbeat)

    @jobs.handler(CLI_GATE_JOB)
    async def cli_gate(request: CliGateRequest, context: JobContext[None]) -> CliGateResult:
        await context.log("gate", extra={"worker_pid": os.getpid()})
        await context.progress(completed=0, total=1)
        run_id = os.environ.get("SUPERJOBS_CLI_RUN_ID")
        _write_checkpoint(
            "handler_entered",
            gate=request.gate,
            execution_id=context.id,
            run_id=run_id,
        )
        await _wait_for_release(request.gate)
        await context.progress(completed=1, total=1)
        return CliGateResult(gate=request.gate)

    @jobs.handler(CLI_FAIL_JOB)
    async def cli_fail(request: CliFailRequest, context: JobContext[None]) -> None:
        raise RuntimeError(request.reason)
