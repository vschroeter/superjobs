from __future__ import annotations

import asyncio
from dataclasses import dataclass

import pytest

from superjobs import InMemoryTransport, Job, JobContext, SuperJobs


@dataclass
class Request:
    value: int


@dataclass
class Result:
    value: str


class SpyTransport(InMemoryTransport):
    def __init__(self) -> None:
        super().__init__()
        self.register_job_calls: list[Job[Request, Result, None]] = []

    def register_job(self, job: Job[Request, Result, None]) -> None:
        self.register_job_calls.append(job)


@pytest.mark.asyncio
async def test_runtime_decorator_executes() -> None:
    jobs = SuperJobs(transport=InMemoryTransport())
    job = Job("tests.handlers.decorator", version="v1", request=Request, result=Result)

    @jobs.handler(job)
    async def decorated(request: Request, context: JobContext[None]) -> Result:
        await context.log("decorated")
        return Result(value=str(request.value))

    async with jobs:
        assert await jobs.client(job).run(Request(value=7)) == Result(value="7")


@pytest.mark.asyncio
async def test_explicit_register_executes() -> None:
    jobs = SuperJobs(transport=InMemoryTransport())
    job = Job("tests.handlers.explicit", version="v1", request=Request, result=Result)

    async def explicit(request: Request, context: JobContext[None]) -> Result:
        return Result(value=str(request.value + 1))

    jobs.register(job, explicit)

    async with jobs:
        assert await jobs.client(job).run(Request(value=2)) == Result(value="3")


@pytest.mark.asyncio
async def test_marked_handler_register_executes() -> None:
    jobs = SuperJobs(transport=InMemoryTransport())
    job = Job("tests.handlers.marked", version="v1", request=Request, result=Result)

    @job.handler
    async def marked(request: Request, context: JobContext[None]) -> Result:
        return Result(value=f"marked-{request.value}")

    jobs.register(marked)

    async with jobs:
        assert await jobs.client(job).run(Request(value=4)) == Result(value="marked-4")


@pytest.mark.asyncio
async def test_sync_runtime_decorator_executes() -> None:
    jobs = SuperJobs(transport=InMemoryTransport())
    job = Job("tests.handlers.sync_decorator", version="v1", request=Request, result=Result)

    @jobs.handler(job)
    def decorated_sync(request: Request, context: JobContext[None]) -> Result:
        return Result(value=str(request.value))

    async with jobs:
        assert await jobs.client(job).run(Request(value=6)) == Result(value="6")


@pytest.mark.asyncio
async def test_sync_handlers_execute() -> None:
    jobs = SuperJobs(transport=InMemoryTransport())
    job = Job("tests.handlers.sync", version="v1", request=Request, result=Result)

    def sync_handler(request: Request, context: JobContext[None]) -> Result:
        return Result(value=str(request.value))

    jobs.register(job, sync_handler)

    async with jobs:
        assert await jobs.client(job).run(Request(value=5)) == Result(value="5")


@pytest.mark.asyncio
async def test_no_request_and_no_result_handlers() -> None:
    jobs = SuperJobs(transport=InMemoryTransport())
    noop = Job("tests.handlers.noop", version="v1")
    ingest = Job("tests.handlers.ingest", version="v1", request=Request, result=None)

    @jobs.handler(noop)
    async def heartbeat(context: JobContext[None]) -> None:
        await context.log("beat")
        return None

    @ingest.handler
    async def ingest_handler(request: Request, context: JobContext[None]) -> None:
        await context.log(str(request.value))
        return None

    jobs.register(ingest_handler)

    async with jobs:
        assert await jobs.client(noop).run(None) is None
        assert await jobs.client(ingest).run(Request(value=1)) is None


@pytest.mark.asyncio
async def test_metadata_decorator_has_no_registry_side_effects() -> None:
    jobs = SuperJobs(transport=InMemoryTransport())
    job = Job("tests.handlers.metadata_only", version="v1", request=Request, result=Result)

    @job.handler
    async def marked_only(request: Request, context: JobContext[None]) -> Result:
        return Result(value="idle")

    assert jobs._handlers == {}
    other = SuperJobs(transport=InMemoryTransport())
    other.register(marked_only)
    assert other._handlers


@pytest.mark.asyncio
async def test_marked_handler_reused_in_separate_runtimes() -> None:
    job = Job("tests.handlers.reuse", version="v1", request=Request, result=Result)

    @job.handler
    async def shared(request: Request, context: JobContext[None]) -> Result:
        return Result(value=str(request.value))

    first = SuperJobs(transport=InMemoryTransport())
    second = SuperJobs(transport=InMemoryTransport())
    first.register(shared)
    second.register(shared)

    async with first, second:
        assert await first.client(job).run(Request(value=1)) == Result(value="1")
        assert await second.client(job).run(Request(value=2)) == Result(value="2")


@pytest.mark.asyncio
async def test_duplicate_registration_preserves_active_handler() -> None:
    transport = SpyTransport()
    jobs = SuperJobs(transport=transport)
    job = Job("tests.handlers.duplicate", version="v1", request=Request, result=Result)

    async def first(request: Request, context: JobContext[None]) -> Result:
        return Result(value="first")

    async def second(request: Request, context: JobContext[None]) -> Result:
        return Result(value="second")

    jobs.register(job, first)
    with pytest.raises(ValueError, match="already registered"):
        jobs.register(job, second)

    assert jobs._handlers[job.canonical_name].callback is first
    assert transport.register_job_calls == [job]

    async with jobs:
        assert await jobs.client(job).run(Request(value=0)) == Result(value="first")


@pytest.mark.asyncio
async def test_unmarked_register_form_fails() -> None:
    jobs = SuperJobs(transport=InMemoryTransport())
    job = Job("tests.handlers.unmarked", version="v1", request=Request, result=Result)

    async def plain(request: Request, context: JobContext[None]) -> Result:
        return Result(value="x")

    with pytest.raises(TypeError, match="@job.handler"):
        jobs.register(plain)

    with pytest.raises(TypeError, match="@job.handler"):
        jobs.register(job)


@pytest.mark.asyncio
async def test_conflicting_job_metadata_rejected() -> None:
    jobs = SuperJobs(transport=InMemoryTransport())
    first_job = Job("tests.handlers.conflict.a", version="v1", request=Request, result=Result)
    second_job = Job("tests.handlers.conflict.b", version="v1", request=Request, result=Result)

    @first_job.handler
    async def marked(request: Request, context: JobContext[None]) -> Result:
        return Result(value="a")

    with pytest.raises(ValueError, match="already associated"):
        second_job.handler(marked)

    with pytest.raises(ValueError, match="already associated"):
        jobs.register(second_job, marked)


@pytest.mark.asyncio
async def test_invalid_signatures_rejected_before_backend_registration() -> None:
    transport = SpyTransport()
    jobs = SuperJobs(transport=transport)
    job = Job("tests.handlers.signature", version="v1", request=Request, result=Result)
    no_request_job = Job("tests.handlers.signature.noreq", version="v1", result=Result)

    async def missing_context(request: Request) -> Result:
        return Result(value="x")

    async def extra_required(
        request: Request,
        context: JobContext[None],
        extra: str,
    ) -> Result:
        return Result(value=extra)

    async def extra_keyword_required(
        request: Request, context: JobContext[None], *, extra: str
    ) -> Result:
        return Result(value=extra)

    async def wrong_no_request_shape(request: Request, context: JobContext[None]) -> Result:
        return Result(value="x")

    for invalid in (missing_context, extra_required, extra_keyword_required):
        with pytest.raises(TypeError):
            jobs.register(job, invalid)
        assert transport.register_job_calls == []
        assert jobs._handlers == {}

    with pytest.raises(TypeError):
        jobs.register(no_request_job, wrong_no_request_shape)
    assert transport.register_job_calls == []
    assert jobs._handlers == {}


@pytest.mark.asyncio
async def test_invalid_handler_options_rejected_before_backend_registration() -> None:
    transport = SpyTransport()
    jobs = SuperJobs(transport=transport)
    job = Job("tests.handlers.invalid_options", version="v1", request=Request, result=Result)

    async def valid_handler(request: Request, context: JobContext[None]) -> Result:
        return Result(value="ok")

    with pytest.raises(ValueError, match="concurrency"):
        jobs.register(job, valid_handler, concurrency=0)
    assert transport.register_job_calls == []
    assert jobs._handlers == {}

    with pytest.raises(ValueError, match="heartbeat_interval"):
        jobs.register(job, valid_handler, heartbeat_interval=0)
    assert transport.register_job_calls == []
    assert jobs._handlers == {}


@pytest.mark.asyncio
async def test_sync_no_request_runtime_decorator_executes() -> None:
    jobs = SuperJobs(transport=InMemoryTransport())
    job = Job("tests.handlers.sync_no_request_runtime", version="v1", result=Result)

    @jobs.handler(job)
    def heartbeat(context: JobContext[None]) -> Result:
        return Result(value="runtime-beat")

    async with jobs:
        assert await jobs.client(job).run(None) == Result(value="runtime-beat")


@pytest.mark.asyncio
async def test_sync_no_request_contract_decorator_executes() -> None:
    jobs = SuperJobs(transport=InMemoryTransport())
    job = Job("tests.handlers.sync_no_request", version="v1", result=Result)

    @job.handler
    def heartbeat(context: JobContext[None]) -> Result:
        return Result(value="beat")

    jobs.register(heartbeat)

    async with jobs:
        assert await jobs.client(job).run(None) == Result(value="beat")


@pytest.mark.asyncio
async def test_async_marked_handler_is_not_treated_as_sync() -> None:
    jobs = SuperJobs(transport=InMemoryTransport())
    job = Job("tests.handlers.asyncmeta", version="v1", request=Request, result=Result)
    observed = asyncio.Event()
    started = asyncio.Event()

    @job.handler
    async def marked(request: Request, context: JobContext[None]) -> Result:
        started.set()
        await observed.wait()
        return Result(value="ok")

    jobs.register(marked)

    async with jobs:
        handle = await jobs.client(job).submit(Request(value=1))
        await asyncio.wait_for(started.wait(), timeout=1)
        assert not observed.is_set()
        observed.set()
        assert await handle.result() == Result(value="ok")


@pytest.mark.asyncio
async def test_contract_decorator_preserves_original_callable_and_defaults() -> None:
    jobs = SuperJobs(transport=InMemoryTransport())
    job = Job("tests.handlers.signature_identity", request=Request, result=Result)
    default_context = JobContext(job)

    def original(
        payload: Request = Request(value=1),
        ctx: JobContext[None] = default_context,
        *,
        trace: bool = False,
    ) -> Result:
        return Result(value=f"{payload.value}:{trace}")

    marked = job.handler(original)
    assert marked is original
    assert marked() == Result(value="1:False")
    assert marked(payload=Request(value=2), ctx=default_context, trace=True) == Result(value="2:True")
    jobs.register(marked)
    async with jobs:
        assert await jobs.client(job).run(Request(value=3)) == Result(value="3:False")
