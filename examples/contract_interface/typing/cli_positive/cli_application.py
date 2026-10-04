"""Installed application handler and factory consumer examples (issue #42)."""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from superjobs import JobContext
from superjobs.cli import JobCLI
from superjobs_contract_example import (
    CLI_FAIL_JOB,
    CLI_GATE_JOB,
    CLI_LOCAL_PROBE_JOB,
    CliFailRequest,
    CliGateRequest,
    CliGateResult,
    CliLocalProbeRequest,
    CliLocalProbeResult,
)
from superjobs_contract_cli_example import build_cli


def _gate_handler(cli: JobCLI) -> None:
    async def handler(request: CliGateRequest, context: JobContext[None]) -> CliGateResult:
        return CliGateResult(gate=request.gate)

    cli.add("gate", CLI_GATE_JOB, handler=handler)


def _gate_factory(cli: JobCLI) -> None:
    def factory() -> Callable[[CliGateRequest, JobContext[None]], Awaitable[CliGateResult]]:
        async def handler(request: CliGateRequest, context: JobContext[None]) -> CliGateResult:
            return CliGateResult(gate=request.gate)

        return handler

    cli.add("gate-factory", CLI_GATE_JOB, handler_factory=factory)


def _probe_handler(cli: JobCLI) -> None:
    async def handler(
        request: CliLocalProbeRequest,
        context: JobContext[None],
    ) -> CliLocalProbeResult:
        return CliLocalProbeResult(pid=0)

    cli.add("probe", CLI_LOCAL_PROBE_JOB, handler=handler)


def _fail_handler(cli: JobCLI) -> None:
    async def handler(request: CliFailRequest, context: JobContext[None]) -> None:
        raise RuntimeError(request.reason)

    cli.add("fail", CLI_FAIL_JOB, handler=handler)


def _installed_example_surface() -> None:
    build_cli()
