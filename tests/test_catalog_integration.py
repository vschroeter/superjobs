"""Catalog, runtime, and CLI integration (issue #46)."""

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass

import pytest
from typer.testing import CliRunner

from superjobs import HandlerCatalog, InMemoryTransport, Job, JobContext, RetryPolicy, SuperJobs
from superjobs.cli import JobCLI


@dataclass
class Payload:
    value: int


@dataclass
class Answer:
    text: str


@pytest.mark.asyncio
async def test_runtime_consumes_shared_catalog() -> None:
    catalog = HandlerCatalog()
    job = Job("tests.catalog.runtime", request=Payload, result=Answer)

    @catalog.handler(job, retry=RetryPolicy(max_attempts=3))
    async def handler(request: Payload, context: JobContext[None]) -> Answer:
        return Answer(text=str(request.value))

    jobs = SuperJobs(transport=InMemoryTransport(), handlers=catalog)
    async with jobs:
        assert await jobs.client(job).run(Payload(value=9)) == Answer(text="9")


@pytest.mark.asyncio
async def test_runtime_decorator_updates_supplied_catalog() -> None:
    catalog = HandlerCatalog()
    jobs = SuperJobs(transport=InMemoryTransport(), handlers=catalog)
    job = Job("tests.catalog.runtime.decorator", request=Payload, result=Answer)

    @jobs.handler(job, cli="echo")
    async def handler(request: Payload, context: JobContext[None]) -> Answer:
        return Answer(text="ok")

    binding = catalog.get(job)
    assert binding is not None
    assert binding.cli is not None and binding.cli.name == "echo"


def test_jobcli_snapshots_catalog_commands() -> None:
    catalog = HandlerCatalog()
    job = Job("tests.catalog.cli", request=Payload, result=Answer)

    @catalog.handler(job, cli="echo")
    async def handler(request: Payload, context: JobContext[None]) -> Answer:
        return Answer(text=str(request.value))

    cli = JobCLI(handlers=catalog)
    names = {registration.command_name for registration in cli.registrations()}
    assert names == {"echo"}

    @catalog.handler(Job("tests.catalog.cli.late", request=Payload, result=Answer), cli="late")
    async def late(request: Payload, context: JobContext[None]) -> Answer:
        return Answer(text="late")

    assert {r.command_name for r in cli.registrations()} == {"echo"}


def test_catalog_local_run_inherits_retry_policy() -> None:
    catalog = HandlerCatalog()
    job = Job("tests.catalog.local.retry", request=Payload, result=Answer)
    attempts = 0

    @catalog.handler(job, cli="retry", retry=RetryPolicy(max_attempts=2))
    async def flaky(request: Payload, context: JobContext[None]) -> Answer:
        nonlocal attempts
        attempts += 1
        if attempts < 2:
            raise RuntimeError("retry me")
        return Answer(text="ok")

    cli = JobCLI(handlers=catalog)
    result = CliRunner().invoke(cli.build_typer(), ["run", "retry", "--value", "1"])
    assert result.exit_code == 0
    assert attempts == 2


def test_provider_only_enters_on_local_run() -> None:
    catalog = HandlerCatalog()
    job = Job("tests.catalog.provider", request=Payload, result=Answer)
    entered = False

    @asynccontextmanager
    async def provider():
        nonlocal entered
        entered = True

        async def inner(request: Payload, context: JobContext[None]) -> Answer:
            return Answer(text=str(request.value))

        yield inner

    catalog.bind(job, provider=provider, cli="prov")
    cli = JobCLI(handlers=catalog)
    assert not entered
    CliRunner().invoke(cli.build_typer(), ["--help"])
    assert not entered
    result = CliRunner().invoke(cli.build_typer(), ["run", "prov", "--value", "3"])
    assert result.exit_code == 0
    assert entered
