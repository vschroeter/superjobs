"""Backend-free handler catalog shared by CLI and worker example packages."""

from __future__ import annotations

import os
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager

from superjobs import CLIField, Command, HandlerCatalog, JobContext, RetryPolicy
from superjobs_contract_example import (
    CLI_FAIL_JOB,
    CLI_GATE_JOB,
    CLI_LOCAL_PROBE_JOB,
    HEARTBEAT_JOB,
    MANIFEST_NO_EVENTS_JOB,
    MANIFEST_WITH_EVENTS_JOB,
    TELEMETRY_INGEST_JOB,
    CliFailRequest,
    CliGateRequest,
    CliGateResult,
    CliLocalProbeRequest,
    CliLocalProbeResult,
    HeartbeatResult,
    ManifestEvent,
    ManifestNoEventsRequest,
    ManifestNoEventsResult,
    ManifestRequest,
    ManifestResult,
    TelemetrySample,
)

from superjobs_contract_handlers.sync import (
    wait_for_gate_release,
    write_handler_entered,
    write_provider_closed,
    write_provider_entered,
)

CONTRACT_CATALOG = HandlerCatalog()

_BUNDLE_COMMAND = Command(
    name="bundle",
    aliases=("accept-bundle",),
    positional_fields=("bundle_id",),
    field_options={"bundle_id": CLIField(help="Bundle identifier.")},
)

_OBSERVE_COMMAND = Command(
    name="observe-local",
    field_options={"device_id": CLIField(help="Device identifier for manifest revision.")},
)

_PROBE_COMMAND = Command(
    name="probe-local",
    aliases=("probe",),
    positional_fields=("note",),
    field_options={"note": CLIField(help="Probe note written to logs.")},
)

_GATE_COMMAND = Command(name="gate-local")
_FAIL_COMMAND = Command(name="fail-local")


def _manifest_revision(device_id: str) -> str:
    if os.environ.get("SUPERJOBS_WORKER_PROCESS") == "1":
        return f"{device_id}-r1"
    return device_id


@CONTRACT_CATALOG.handler(
    MANIFEST_NO_EVENTS_JOB,
    retry=RetryPolicy(max_attempts=2),
    cli=_BUNDLE_COMMAND,
)
async def manifest_no_events(
    request: ManifestNoEventsRequest,
    context: JobContext[None],
) -> ManifestNoEventsResult:
    await context.log(f"bundle {request.bundle_id}")
    return ManifestNoEventsResult(accepted=True)


@CONTRACT_CATALOG.handler(MANIFEST_WITH_EVENTS_JOB, cli=_OBSERVE_COMMAND)
async def manifest_with_events(
    request: ManifestRequest,
    context: JobContext[ManifestEvent],
) -> ManifestResult:
    await context.log("phase", extra={"worker_pid": os.getpid()})
    await context.emit(ManifestEvent(stage="mid"))
    await context.progress(completed=1, total=1)
    return ManifestResult(revision=_manifest_revision(request.device_id))


@asynccontextmanager
async def _probe_provider() -> AsyncIterator[
    Callable[[CliLocalProbeRequest, JobContext[None]], Awaitable[CliLocalProbeResult]]
]:
    write_provider_entered("probe-local")
    try:

        async def probe_local(
            request: CliLocalProbeRequest,
            context: JobContext[None],
        ) -> CliLocalProbeResult:
            await context.log(f"probe note={request.note}")
            return CliLocalProbeResult(pid=os.getpid())

        yield probe_local
    finally:
        write_provider_closed("probe-local")


CONTRACT_CATALOG.bind(
    CLI_LOCAL_PROBE_JOB,
    provider=_probe_provider,
    cli=_PROBE_COMMAND,
)


@CONTRACT_CATALOG.handler(CLI_GATE_JOB, cli=_GATE_COMMAND)
async def cli_gate(request: CliGateRequest, context: JobContext[None]) -> CliGateResult:
    await context.log("gate", extra={"worker_pid": os.getpid()})
    await context.progress(completed=0, total=1)
    write_handler_entered(
        request.gate,
        execution_id=context.id,
        run_id=os.environ.get("SUPERJOBS_CLI_RUN_ID"),
    )
    await wait_for_gate_release(request.gate, check_cancelled=context.check_cancelled)
    await context.progress(completed=1, total=1)
    return CliGateResult(gate=request.gate)


@CONTRACT_CATALOG.handler(CLI_FAIL_JOB, cli=_FAIL_COMMAND)
async def cli_fail(request: CliFailRequest, context: JobContext[None]) -> None:
    raise RuntimeError(request.reason)


@TELEMETRY_INGEST_JOB.handler
async def telemetry_ingest(
    request: TelemetrySample,
    context: JobContext[None],
) -> None:
    await context.log(f"telemetry {request.metric}={request.value}")
    return None


CONTRACT_CATALOG.register(telemetry_ingest)


@HEARTBEAT_JOB.handler
async def heartbeat(context: JobContext[None]) -> HeartbeatResult:
    return HeartbeatResult(ok=True)


CONTRACT_CATALOG.register(heartbeat)
