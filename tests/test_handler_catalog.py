from __future__ import annotations

import math
from contextlib import asynccontextmanager
from dataclasses import dataclass

import pytest

from superjobs import (
    CLIField,
    Command,
    HandlerCatalog,
    Job,
    JobContext,
    RetryPolicy,
)
from superjobs.jobs.handler_binding import get_marked_job


@dataclass
class Request:
    value: int


@dataclass
class Result:
    value: str


def test_catalog_decorator_attaches_job_marker_and_binding() -> None:
    catalog = HandlerCatalog()
    job = Job("tests.catalog.decorator", version="v1", request=Request, result=Result)

    @catalog.handler(job, concurrency=2, retry=RetryPolicy(max_attempts=3))
    async def decorated(request: Request, context: JobContext[None]) -> Result:
        return Result(value=str(request.value))

    binding = catalog.get(job)
    assert binding is not None
    assert binding.callback is decorated
    assert binding.concurrency == 2
    assert binding.retry is not None and binding.retry.max_attempts == 3
    assert get_marked_job(decorated) is job


def test_catalog_decorator_preserves_original_callable() -> None:
    catalog = HandlerCatalog()
    job = Job("tests.catalog.identity", request=Request, result=Result)
    default_context = JobContext(job)

    def original(
        payload: Request = Request(value=1),
        ctx: JobContext[None] = default_context,
        *,
        trace: bool = False,
    ) -> Result:
        return Result(value=f"{payload.value}:{trace}")

    marked = catalog.handler(job)(original)
    assert marked is original
    assert marked() == Result(value="1:False")


@pytest.mark.asyncio
async def test_sync_and_no_request_catalog_handlers_validate() -> None:
    catalog = HandlerCatalog()
    request_job = Job("tests.catalog.sync", version="v1", request=Request, result=Result)
    noop = Job("tests.catalog.noop", version="v1", result=None)

    @catalog.handler(request_job)
    def sync_handler(request: Request, context: JobContext[None]) -> Result:
        return Result(value="sync")

    @catalog.handler(noop)
    async def heartbeat(context: JobContext[None]) -> None:
        return None

    assert catalog.get(request_job) is not None
    assert catalog.get(noop) is not None


def test_explicit_register_and_marked_register() -> None:
    catalog = HandlerCatalog()
    job = Job("tests.catalog.register", request=Request, result=Result)

    async def explicit(request: Request, context: JobContext[None]) -> Result:
        return Result(value="explicit")

    catalog.register(job, explicit)
    assert catalog.get(job) is not None
    assert catalog.get(job).callback is explicit

    marked_job = Job("tests.catalog.register.marked", request=Request, result=Result)

    @marked_job.handler
    async def marked(request: Request, context: JobContext[None]) -> Result:
        return Result(value="marked")

    catalog.register(marked)
    assert catalog.get(marked_job) is not None
    assert catalog.get(marked_job).callback is marked


def test_catalog_stores_cli_metadata() -> None:
    catalog = HandlerCatalog()
    job = Job("tests.catalog.cli", request=Request, result=Result)
    field_options = {"value": CLIField(option="v", help="payload value")}
    command = Command(
        name="run-me",
        aliases=("alias",),
        positional_fields=("value",),
        field_options=field_options,
    )

    @catalog.handler(job, cli="shortcut")
    async def shortcut_handler(request: Request, context: JobContext[None]) -> Result:
        return Result(value="a")

    assert catalog.get(job) is not None
    assert catalog.get(job).cli == Command(name="shortcut")

    catalog2 = HandlerCatalog()
    job2 = Job("tests.catalog.cli.rich", request=Request, result=Result)

    @catalog2.handler(job2, cli=command)
    async def rich_handler(request: Request, context: JobContext[None]) -> Result:
        return Result(value="b")

    binding = catalog2.get(job2)
    assert binding is not None
    assert binding.cli is not None
    assert binding.cli.name == command.name
    assert binding.cli.aliases == command.aliases
    assert dict(binding.cli.field_options) == field_options
    field_options["value"] = CLIField(option="changed")
    assert dict(binding.cli.field_options) == {
        "value": CLIField(option="v", help="payload value")
    }


def test_snapshot_and_command_metadata_are_immutable() -> None:
    catalog = HandlerCatalog()
    first_job = Job("tests.catalog.snapshot.a", request=Request, result=Result)
    second_job = Job("tests.catalog.snapshot.b", request=Request, result=Result)

    @catalog.handler(first_job)
    async def first(request: Request, context: JobContext[None]) -> Result:
        return Result(value="first")

    snapshot = catalog.snapshot()
    assert len(snapshot) == 1
    assert snapshot.get(first_job) is not None
    assert snapshot.get(second_job) is None

    @catalog.handler(second_job)
    async def second(request: Request, context: JobContext[None]) -> Result:
        return Result(value="second")

    assert len(catalog.snapshot()) == 2
    assert len(snapshot) == 1
    assert snapshot.get(second_job) is None

    bindings = snapshot.bindings()
    with pytest.raises(TypeError):
        bindings["tests.catalog.snapshot.a"] = snapshot.get(first_job)  # type: ignore[index]


def test_duplicate_catalog_registration_rejected() -> None:
    catalog = HandlerCatalog()
    job = Job("tests.catalog.duplicate", request=Request, result=Result)

    async def first(request: Request, context: JobContext[None]) -> Result:
        return Result(value="first")

    async def second(request: Request, context: JobContext[None]) -> Result:
        return Result(value="second")

    catalog.handler(job)(first)
    with pytest.raises(ValueError, match="already registered"):
        catalog.handler(job)(second)


def test_invalid_catalog_signatures_rejected() -> None:
    catalog = HandlerCatalog()
    job = Job("tests.catalog.signature", request=Request, result=Result)
    no_request_job = Job("tests.catalog.signature.noreq", result=Result)

    async def missing_context(request: Request) -> Result:
        return Result(value="x")

    async def wrong_no_request(request: Request, context: JobContext[None]) -> Result:
        return Result(value="x")

    with pytest.raises(TypeError):
        catalog.handler(job)(missing_context)
    assert catalog.get(job) is None

    with pytest.raises(TypeError):
        catalog.handler(no_request_job)(wrong_no_request)
    assert catalog.get(no_request_job) is None


def test_invalid_catalog_options_rejected() -> None:
    catalog = HandlerCatalog()
    job = Job("tests.catalog.options", request=Request, result=Result)

    async def valid_handler(request: Request, context: JobContext[None]) -> Result:
        return Result(value="ok")

    with pytest.raises(ValueError, match="concurrency"):
        catalog.handler(job, concurrency=0)(valid_handler)
    assert catalog.get(job) is None

    with pytest.raises(ValueError, match="heartbeat_interval"):
        catalog.handler(job, heartbeat_interval=0)(valid_handler)
    assert catalog.get(job) is None

    with pytest.raises(ValueError, match="heartbeat_interval"):
        catalog.handler(job, heartbeat_interval=math.nan)(valid_handler)
    assert catalog.get(job) is None

    with pytest.raises(TypeError, match="concurrency"):
        catalog.handler(job, concurrency=True)(valid_handler)
    assert catalog.get(job) is None

    with pytest.raises(TypeError, match="concurrency"):
        catalog.handler(job, concurrency=1.5)(valid_handler)
    assert catalog.get(job) is None


def test_failed_registration_leaves_marker_and_catalog_unchanged() -> None:
    catalog = HandlerCatalog()
    first_job = Job("tests.catalog.tx.a", request=Request, result=Result)
    second_job = Job("tests.catalog.tx.b", request=Request, result=Result)

    async def reusable(request: Request, context: JobContext[None]) -> Result:
        return Result(value="ok")

    with pytest.raises(ValueError, match="non-empty"):
        catalog.handler(first_job, cli="")(reusable)
    assert catalog.get(first_job) is None
    assert get_marked_job(reusable) is None

    catalog.handler(second_job)(reusable)
    assert catalog.get(second_job) is not None
    assert get_marked_job(reusable) is second_job


def test_conflicting_job_metadata_rejected_in_catalog() -> None:
    catalog = HandlerCatalog()
    first_job = Job("tests.catalog.conflict.a", request=Request, result=Result)
    second_job = Job("tests.catalog.conflict.b", request=Request, result=Result)

    @catalog.handler(first_job)
    async def marked(request: Request, context: JobContext[None]) -> Result:
        return Result(value="a")

    with pytest.raises(ValueError, match="already associated"):
        catalog.handler(second_job)(marked)


def test_provider_binding_without_callback() -> None:
    catalog = HandlerCatalog()
    job = Job("tests.catalog.provider", request=Request, result=Result)
    invoked = False

    @asynccontextmanager
    async def provider() -> object:
        nonlocal invoked
        invoked = True

        async def handler(request: Request, context: JobContext[None]) -> Result:
            return Result(value=str(request.value))

        yield handler

    catalog.bind(job, provider=provider, concurrency=2, cli="from-provider")
    assert not invoked
    binding = catalog.get(job)
    assert binding is not None
    assert binding.callback is None
    assert binding.provider is provider
    assert binding.cli == Command(name="from-provider")


def test_handler_command_validation() -> None:
    with pytest.raises(ValueError, match="non-empty"):
        Command(name="")

    with pytest.raises(ValueError, match="aliases"):
        Command(name="ok", aliases=("",))

    with pytest.raises(ValueError, match="duplicates"):
        Command(name="ok", aliases=("a", "a"))

    with pytest.raises(ValueError, match="duplicates"):
        Command(name="ok", positional_fields=("field", "field"))

    with pytest.raises(TypeError, match="CLIField"):
        Command(name="ok", field_options={"x": object()})  # type: ignore[dict-item]

    from superjobs.cli import CLIField as CliCLIField

    assert CliCLIField is CLIField
