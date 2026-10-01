from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest
from pydantic import BaseModel, field_validator

from pydantic import ValidationError

from superjobs import Job, NoRequestJob, PayloadValidationError, RequestJob, SubmitOptions, SuperJobs
from superjobs.payload import construct_payload
from superjobs.payload.adapter.implementations.dataclass import DataclassPayloadAdapter
from superjobs.jobs.submission import SubmitShapeError
from superjobs.transport.in_memory import InMemoryTransport


@dataclass(frozen=True, slots=True)
class DcRequest:
    device_id: str


@dataclass(frozen=True, slots=True)
class DcResult:
    value: str


@dataclass(kw_only=True, frozen=True, slots=True)
class KeywordOnlyRequest:
    bundle_id: str
    options: str = "business"


@dataclass(kw_only=True, frozen=True, slots=True)
class DefaultOnlyRequest:
    count: int = 7


@dataclass(kw_only=True, frozen=True, slots=True)
class BusinessTimeoutRequest:
    timeout: int


class PydanticRequest(BaseModel):
    metric: str
    count: int = 1


class ConvertedRequest(BaseModel):
    value: int

    @field_validator("value", mode="before")
    @classmethod
    def parse(cls, value: object) -> object:
        return int(value)


def test_presence_aware_job_classes_reject_opposite_request_shape() -> None:
    with pytest.raises(TypeError, match="requires a request type"):
        RequestJob("tests.producer.invalid_request_job", result=DcResult)
    with pytest.raises(TypeError, match="cannot declare a request type"):
        NoRequestJob("tests.producer.invalid_no_request_job", request=DcRequest)


@pytest.mark.asyncio
async def test_keyword_submit_constructs_request_before_backend() -> None:
    transport = InMemoryTransport()
    jobs = SuperJobs(transport=transport)
    job = Job("tests.producer.keywords", version="v1", request=DcRequest, result=DcResult)
    transport.submit = AsyncMock(side_effect=transport.submit)

    @jobs.handler(job)
    async def handler(request: DcRequest, context) -> DcResult:
        return DcResult(value=request.device_id)

    async with jobs:
        handle = await jobs.client(job).submit(device_id="sensor-17")
        assert await handle.result() == DcResult(value="sensor-17")
    transport.submit.assert_awaited()


@pytest.mark.asyncio
async def test_invalid_keyword_submit_rejects_before_backend() -> None:
    transport = InMemoryTransport()
    jobs = SuperJobs(transport=transport)
    job = Job("tests.producer.invalid", version="v1", request=DcRequest, result=DcResult)
    transport.submit = AsyncMock(side_effect=transport.submit)

    @jobs.handler(job)
    async def handler(request: DcRequest, context) -> DcResult:
        return DcResult(value=request.device_id)

    async with jobs:
        with pytest.raises((SubmitShapeError, TypeError, ValueError)):
            await jobs.client(job).submit(device_id=17)
    transport.submit.assert_not_awaited()


_coercing_init_calls = 0


@dataclass
class CoercingDataclass:
    value: int

    def __init__(self, value: int) -> None:
        global _coercing_init_calls
        _coercing_init_calls += 1
        self.value = int(value)


@pytest.mark.asyncio
async def test_invalid_keyword_submit_rejects_before_coercing_dataclass_init() -> None:
    global _coercing_init_calls
    _coercing_init_calls = 0
    transport = InMemoryTransport()
    jobs = SuperJobs(transport=transport)
    job = Job("tests.producer.coercing", version="v1", request=CoercingDataclass, result=DcResult)
    transport.submit = AsyncMock(side_effect=transport.submit)

    @jobs.handler(job)
    async def handler(request: CoercingDataclass, context) -> DcResult:
        return DcResult(value=str(request.value))

    async with jobs:
        with pytest.raises(ValidationError):
            await jobs.client(job).submit(value="not-an-int")
    transport.submit.assert_not_awaited()
    assert _coercing_init_calls == 0
    adapter = DataclassPayloadAdapter(CoercingDataclass)
    with pytest.raises(ValidationError):
        construct_payload(adapter, {"value": "not-an-int"})
    assert _coercing_init_calls == 0


@pytest.mark.asyncio
async def test_non_convertible_keyword_rejected_before_backend() -> None:
    transport = InMemoryTransport()
    jobs = SuperJobs(transport=transport)
    job = Job("tests.producer.coerce", version="v1", request=ConvertedRequest, result=ConvertedRequest)
    transport.submit = AsyncMock(side_effect=transport.submit)

    @jobs.handler(job)
    async def handler(request: ConvertedRequest, context) -> ConvertedRequest:
        return request

    async with jobs:
        with pytest.raises((ValidationError, PayloadValidationError, ValueError)):
            await jobs.client(job).submit(value="not-a-number")
    transport.submit.assert_not_awaited()


@pytest.mark.asyncio
async def test_before_validator_conversion_on_keyword_submit() -> None:
    jobs = SuperJobs(transport=InMemoryTransport())
    job = Job("tests.producer.converted", version="v1", request=ConvertedRequest, result=ConvertedRequest)

    @jobs.handler(job)
    async def handler(request: ConvertedRequest, context) -> ConvertedRequest:
        return request

    async with jobs:
        handle = await jobs.client(job).submit(value="7")
        assert (await handle.result()).value == 7


@pytest.mark.asyncio
async def test_no_request_submit_shapes() -> None:
    jobs = SuperJobs(transport=InMemoryTransport())
    job = Job("tests.producer.heartbeat", version="v1", result=DcResult)

    @jobs.handler(job)
    async def heartbeat(context) -> DcResult:
        return DcResult(value="ok")

    async with jobs:
        assert await (await jobs.client(job).submit()).result() == DcResult(value="ok")
        assert await (await jobs.client(job).submit(None)).result() == DcResult(value="ok")
        assert await (await jobs.client(job).submit(SubmitOptions())).result() == DcResult(value="ok")


@pytest.mark.asyncio
async def test_submit_rejects_mixed_object_and_keywords() -> None:
    jobs = SuperJobs(transport=InMemoryTransport())
    job = Job("tests.producer.mixed", version="v1", request=DcRequest, result=DcResult)

    @jobs.handler(job)
    async def handler(request: DcRequest, context) -> DcResult:
        return DcResult(value=request.device_id)

    async with jobs:
        with pytest.raises(SubmitShapeError):
            await jobs.client(job).submit(DcRequest(device_id="a"), device_id="b")


@pytest.mark.asyncio
async def test_submit_rejects_positional_constructor_keywords() -> None:
    jobs = SuperJobs(transport=InMemoryTransport())
    job = Job("tests.producer.positional", version="v1", request=DcRequest, result=DcResult)

    @jobs.handler(job)
    async def handler(request: DcRequest, context) -> DcResult:
        return DcResult(value=request.device_id)

    async with jobs:
        with pytest.raises(SubmitShapeError):
            await jobs.client(job).submit("sensor-17")


@pytest.mark.asyncio
async def test_submit_options_and_legacy_keywords() -> None:
    jobs = SuperJobs(transport=InMemoryTransport())
    job = Job("tests.producer.options", version="v1", request=DcRequest, result=DcResult)
    fixed = datetime.now(tz=UTC) + timedelta(hours=1)

    @jobs.handler(job)
    async def handler(request: DcRequest, context) -> DcResult:
        return DcResult(value=request.device_id)

    async with jobs:
        handle = await jobs.client(job).submit(
            SubmitOptions(timeout=5),
            device_id="sensor-17",
        )
        assert await handle.result() == DcResult(value="sensor-17")

        handle = await jobs.client(job).submit(
            DcRequest(device_id="legacy"),
            options=SubmitOptions(timeout=3),
        )
        assert await handle.result() == DcResult(value="legacy")

        handle = await jobs.client(job).submit(DcRequest(device_id="kw"), timeout=4)
        assert await handle.result() == DcResult(value="kw")

        handle = await jobs.client(job).submit(
            DcRequest(device_id="deadline"),
            deadline=fixed,
        )
        assert await handle.result() == DcResult(value="deadline")


@pytest.mark.asyncio
async def test_submit_rejects_conflicting_option_sources() -> None:
    jobs = SuperJobs(transport=InMemoryTransport())
    job = Job("tests.producer.conflict", version="v1", request=DcRequest, result=DcResult)

    @jobs.handler(job)
    async def handler(request: DcRequest, context) -> DcResult:
        return DcResult(value=request.device_id)

    async with jobs:
        with pytest.raises(SubmitShapeError):
            await jobs.client(job).submit(
                DcRequest(device_id="a"),
                options=SubmitOptions(timeout=1),
                timeout=2,
            )


@pytest.mark.asyncio
async def test_business_field_named_options_in_keyword_submit() -> None:
    jobs = SuperJobs(transport=InMemoryTransport())
    job = Job("tests.producer.business_options", version="v1", request=KeywordOnlyRequest, result=DcResult)

    @jobs.handler(job)
    async def handler(request: KeywordOnlyRequest, context) -> DcResult:
        return DcResult(value=f"{request.bundle_id}:{request.options}")

    async with jobs:
        handle = await jobs.client(job).submit(bundle_id="b", options="trace")
        assert await handle.result() == DcResult(value="b:trace")


@pytest.mark.asyncio
async def test_default_only_request_constructs_without_fields() -> None:
    jobs = SuperJobs(transport=InMemoryTransport())
    job = Job("tests.producer.default_only", request=DefaultOnlyRequest, result=DcResult)

    @jobs.handler(job)
    async def handler(request: DefaultOnlyRequest, context) -> DcResult:
        return DcResult(value=str(request.count))

    async with jobs:
        handle = await jobs.client(job).submit()
        assert await handle.result() == DcResult(value="7")


@pytest.mark.asyncio
async def test_constructor_keywords_never_reserve_business_timeout() -> None:
    transport = InMemoryTransport()
    jobs = SuperJobs(transport=transport)
    job = Job("tests.producer.business_timeout", request=BusinessTimeoutRequest, result=DcResult)

    @jobs.handler(job)
    async def handler(request: BusinessTimeoutRequest, context) -> DcResult:
        return DcResult(value=str(request.timeout))

    async with jobs:
        constructed = await jobs.client(job).submit(timeout=3)
        explicit = await jobs.client(job).submit(BusinessTimeoutRequest(timeout=3), timeout=5)
        assert await constructed.result() == DcResult(value="3")
        assert await explicit.result() == DcResult(value="3")
        constructed_record = await transport.get_execution(job.identity, constructed.id)
        explicit_record = await transport.get_execution(job.identity, explicit.id)
        assert constructed_record is not None and constructed_record.timeout is None
        assert explicit_record is not None and explicit_record.timeout == 5


@pytest.mark.asyncio
async def test_submit_options_are_scoped_to_one_execution() -> None:
    transport = InMemoryTransport()
    jobs = SuperJobs(transport=transport)
    job = Job("tests.producer.option_scope", request=DcRequest, result=DcResult)

    @jobs.handler(job)
    async def handler(request: DcRequest, context) -> DcResult:
        return DcResult(value=request.device_id)

    async with jobs:
        client = jobs.client(job)
        first = await client.submit(device_id="a")
        override = await client.submit(
            SubmitOptions(timeout=5, caller_scope="tenant", idempotency_key="stable"),
            device_id="b",
        )
        last = await client.submit(device_id="a")
        records = [await transport.get_execution(job.identity, handle.id) for handle in (first, override, last)]
        assert all(record is not None for record in records)
        assert records[0].timeout is None and records[0].deadline is None
        assert records[1].timeout == 5 and records[1].caller_scope == "tenant"
        assert records[1].idempotency_key == "stable"
        assert records[2].timeout is None and records[2].caller_scope == "default"
        assert records[0].idempotency_key != records[2].idempotency_key
        assert records[0].job_id != records[2].job_id


@pytest.mark.asyncio
async def test_invalid_submit_options_fail_before_backend_submission() -> None:
    transport = InMemoryTransport()
    jobs = SuperJobs(transport=transport)
    job = Job("tests.producer.invalid_options", request=DcRequest, result=DcResult)
    transport.submit = AsyncMock(side_effect=transport.submit)

    @jobs.handler(job)
    async def handler(request: DcRequest, context) -> DcResult:
        return DcResult(value=request.device_id)

    invalid = (
        SubmitOptions(timeout=0),
        SubmitOptions(timeout=float("nan")),
        SubmitOptions(timeout=True),
        SubmitOptions(deadline=datetime.now()),
        SubmitOptions(idempotency_key=""),
        SubmitOptions(job_id=""),
        SubmitOptions(caller_scope=""),
    )
    async with jobs:
        for options in invalid:
            with pytest.raises(ValueError):
                await jobs.client(job).submit(options, device_id="sensor")
    transport.submit.assert_not_awaited()


@pytest.mark.asyncio
async def test_run_rejects_conflicting_option_sources() -> None:
    jobs = SuperJobs(transport=InMemoryTransport())
    job = Job("tests.producer.run_options", request=DcRequest, result=DcResult)
    async with jobs:
        with pytest.raises(SubmitShapeError):
            await jobs.client(job).run(DcRequest(device_id="a"), options=SubmitOptions(timeout=1), timeout=2)


@pytest.mark.asyncio
async def test_four_payload_shapes_keyword_or_explicit() -> None:
    jobs = SuperJobs(transport=InMemoryTransport())
    with_events = Job(
        "tests.producer.with_events",
        version="v1",
        request=DcRequest,
        result=DcResult,
        event=DcResult,
    )
    no_events = Job("tests.producer.no_events", version="v1", request=DcRequest, result=DcResult)
    ingest = Job("tests.producer.ingest", version="v1", request=PydanticRequest, result=None)
    heartbeat = Job("tests.producer.beat", version="v1", result=DcResult)

    @jobs.handler(with_events)
    async def with_events_handler(request: DcRequest, context) -> DcResult:
        return DcResult(value=request.device_id)

    @jobs.handler(no_events)
    async def no_events_handler(request: DcRequest, context) -> DcResult:
        return DcResult(value=request.device_id)

    @jobs.handler(ingest)
    async def ingest_handler(request: PydanticRequest, context) -> None:
        return None

    @jobs.handler(heartbeat)
    async def heartbeat_handler(context) -> DcResult:
        return DcResult(value="beat")

    async with jobs:
        assert await (await jobs.client(with_events).submit(device_id="a")).result() == DcResult(value="a")
        assert await (await jobs.client(no_events).submit(DcRequest(device_id="b"))).result() == DcResult(
            value="b",
        )
        assert await (await jobs.client(ingest).submit(metric="m")).result() is None
        assert await (await jobs.client(heartbeat).submit()).result() == DcResult(value="beat")


def test_no_request_register_rejects_request_none_shape() -> None:
    jobs = SuperJobs(transport=InMemoryTransport())
    job = Job("tests.producer.register", version="v1", result=DcResult)

    async def invalid(request: None, context) -> DcResult:
        return DcResult(value="nope")

    with pytest.raises(TypeError, match="only the job context"):
        jobs.register(job, invalid)
