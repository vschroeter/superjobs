"""Local-run provider stack shutdown bounds (issue #46 review)."""

from __future__ import annotations

import asyncio
import io
from contextlib import asynccontextmanager
from dataclasses import dataclass

import pytest

from superjobs import HandlerCatalog, InMemoryTransport, Job, JobContext, SuperJobs
from superjobs.cli import EXIT_RUNTIME_FAILURE, JobCLI
from superjobs.cli.input_prepare import prepare_command_input
from superjobs.cli.local_run import _execute_local_run_async
from superjobs.cli.schema_plan import build_command_input_plan


@dataclass
class EchoRequest:
    value: int


@dataclass
class EchoResult:
    text: str


def _echo_job() -> Job[EchoRequest, EchoResult, None]:
    return Job(
        "tests.cli.provider.cleanup",
        version="v1",
        request=EchoRequest,
        result=EchoResult,
    )


@pytest.mark.asyncio
async def test_hanging_provider_release_times_out_and_fails_exit() -> None:
    catalog = HandlerCatalog()
    job = _echo_job()
    release_started = asyncio.Event()

    @asynccontextmanager
    async def provider():
        async def handler(request: EchoRequest, context: JobContext[None]) -> EchoResult:
            return EchoResult(text=str(request.value))

        yield handler
        release_started.set()
        await asyncio.Event().wait()

    catalog.bind(job, provider=provider, cli="prov")
    cli_reg = next(
        r for r in JobCLI(handlers=catalog).registrations() if r.command_name == "prov"
    )
    plan = build_command_input_plan(job)
    prepared = prepare_command_input(plan, {"json_text": '{"value": 3}'}, None)

    @asynccontextmanager
    async def factory():
        async with SuperJobs(transport=InMemoryTransport()) as jobs:
            yield jobs

    stderr = io.StringIO()
    result = await _execute_local_run_async(
        cli_reg,
        prepared,
        factory,
        stderr=stderr,
        shutdown_timeout=0.15,
    )
    assert result.exit_code == EXIT_RUNTIME_FAILURE
    err = stderr.getvalue()
    assert "provider cleanup" in err
    assert release_started.is_set()
