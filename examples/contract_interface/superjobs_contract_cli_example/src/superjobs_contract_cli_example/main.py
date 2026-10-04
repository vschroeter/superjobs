"""Contract-interface CLI with local ``run`` and NATS ``submit`` (issue #42)."""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from faststream.nats import NatsBroker
from superjobs import InMemoryTransport, JobContext, SuperJobs
from superjobs.cli import CLIField, JobCLI
from superjobs_contract_example import (
    CLI_FAIL_JOB,
    CLI_GATE_JOB,
    CLI_LOCAL_PROBE_JOB,
    MANIFEST_NO_EVENTS_JOB,
    MANIFEST_WITH_EVENTS_JOB,
    CliFailRequest,
    CliGateRequest,
    CliGateResult,
    CliLocalProbeRequest,
    CliLocalProbeResult,
    ManifestEvent,
    ManifestNoEventsRequest,
    ManifestNoEventsResult,
    ManifestRequest,
    ManifestResult,
)


@asynccontextmanager
async def local_runtime() -> AsyncIterator[SuperJobs]:
    from superjobs_contract_cli_example.sync import write_runtime_factory_closed, write_runtime_factory_entered

    write_runtime_factory_entered("local")
    jobs = SuperJobs(transport=InMemoryTransport())
    try:
        async with jobs:
            yield jobs
    finally:
        write_runtime_factory_closed("local")


@asynccontextmanager
async def remote_runtime() -> AsyncIterator[SuperJobs]:
    from superjobs_contract_cli_example.sync import write_runtime_factory_entered

    write_runtime_factory_entered("remote")
    nats_url = os.environ.get("SUPERJOBS_NATS_URL", "nats://localhost:4222")
    jobs = SuperJobs(
        broker=NatsBroker(
            nats_url,
            connect_timeout=2,
            allow_reconnect=False,
            max_reconnect_attempts=0,
        ),
    )
    try:
        yield jobs
    finally:
        if jobs.started:
            await jobs.stop(graceful=False)


def build_cli() -> JobCLI:
    cli = JobCLI(
        local_runtime_factory=local_runtime,
        remote_runtime_factory=remote_runtime,
    )

    async def bundle_local(
        request: ManifestNoEventsRequest,
        context: JobContext[None],
    ) -> ManifestNoEventsResult:
        await context.log(f"local bundle {request.bundle_id}")
        return ManifestNoEventsResult(accepted=True)

    cli.add(
        "bundle",
        MANIFEST_NO_EVENTS_JOB,
        handler=bundle_local,
        positional_fields=("bundle_id",),
        field_options={"bundle_id": CLIField(help="Bundle identifier.")},
    )

    async def observe_local(
        request: ManifestRequest,
        context: JobContext[ManifestEvent],
    ) -> ManifestResult:
        await context.log("phase")
        await context.emit(ManifestEvent(stage="mid"))
        await context.progress(completed=1, total=1)
        return ManifestResult(revision=request.device_id)

    cli.add("observe-local", MANIFEST_WITH_EVENTS_JOB, handler=observe_local)

    async def probe_local(
        request: CliLocalProbeRequest,
        context: JobContext[None],
    ) -> CliLocalProbeResult:
        return CliLocalProbeResult(pid=os.getpid())

    cli.add(
        "probe-local",
        CLI_LOCAL_PROBE_JOB,
        handler=probe_local,
        positional_fields=("note",),
        field_options={"note": CLIField(help="Probe note.")},
    )

    async def gate_local(
        request: CliGateRequest,
        context: JobContext[None],
    ) -> CliGateResult:
        from superjobs_contract_cli_example.sync import wait_for_gate_release, write_handler_entered

        write_handler_entered(
            request.gate,
            execution_id=context.id,
            run_id=os.environ.get("SUPERJOBS_CLI_RUN_ID"),
        )
        await wait_for_gate_release(request.gate, check_cancelled=context.check_cancelled)
        return CliGateResult(gate=request.gate)

    cli.add("gate-local", CLI_GATE_JOB, handler=gate_local)

    cli.add("gate-remote", CLI_GATE_JOB, remote_only=True)
    cli.add("fail-remote", CLI_FAIL_JOB, remote_only=True)
    cli.add(
        "fail-local",
        CLI_FAIL_JOB,
        handler=_fail_local_handler,
    )
    return cli


async def _fail_local_handler(
    request: CliFailRequest,
    context: JobContext[None],
) -> None:
    raise RuntimeError(request.reason)


def main(argv: list[str] | None = None) -> int:
    import sys

    if os.environ.get("SUPERJOBS_CLI_EMIT_PID") == "1":
        print(f"cli-pid:{os.getpid()}", file=sys.stderr, flush=True)
    return build_cli().main(argv)


if __name__ == "__main__":
    raise SystemExit(main())
