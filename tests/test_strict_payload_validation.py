from __future__ import annotations

import enum
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock

import pytest
from pydantic import BaseModel, Field, ValidationError, field_validator

from superjobs.exceptions.jobs import JobFailedError
from superjobs.jobs.job import Job
from superjobs.jobs.retry_policy import RetryPolicy
from superjobs.payload import (
    PayloadValidationError,
    construct_payload,
    construct_payload_for_type,
    validate_payload,
)
from superjobs.payload.adapter.implementations.dataclass import DataclassPayloadAdapter
from superjobs.payload.adapter.implementations.pydantic import PydanticPayloadAdapter
from superjobs.payload.adapter.implementations.python import PlainPythonPayloadAdapter
from superjobs.payload.adapter.protocol import PayloadAdapter, WireValue
from superjobs.payload.codec.implementations.json import JsonCodec
from superjobs.payload.codec.implementations.msgpack import MsgpackCodec
from superjobs.payload.codec.payloadcodec import PayloadCodec
from superjobs.superjobs import SuperJobs
from superjobs.transport.in_memory import InMemoryTransport


@dataclass
class NestedDc:
    count: int


@dataclass(frozen=True)
class RequestDc:
    name: str
    nested: NestedDc
    tags: list[str] = field(default_factory=list)


class NestedModel(BaseModel):
    count: int


class RequestModel(BaseModel):
    name: str
    nested: NestedModel
    tags: list[str] = Field(default_factory=list)


class AliasRequest(BaseModel):
    device_id: str = Field(alias="deviceId")

    model_config = {"populate_by_name": True}


class ConvertedRequest(BaseModel):
    value: int

    @field_validator("value", mode="before")
    @classmethod
    def parse_value(cls, raw: object) -> object:
        if isinstance(raw, str) and raw.isdigit():
            return int(raw)
        return raw


class Stage(enum.Enum):
    ALPHA = "alpha"


class RichWireModel(BaseModel):
    when: datetime
    token: uuid.UUID
    stage: Stage
    payload: bytes


_coercing_init_calls = 0


@dataclass
class CoercingDataclass:
    value: int

    def __init__(self, value: int) -> None:
        global _coercing_init_calls
        _coercing_init_calls += 1
        self.value = int(value)


class CustomAdapter(PayloadAdapter[dict[str, str]]):
    def dump(self, value: dict[str, str]) -> WireValue:
        return dict(value)

    def load(self, value: WireValue) -> dict[str, str]:
        if not isinstance(value, dict):
            raise ValueError("expected object")
        return {str(key): str(item) for key, item in value.items()}

    def schema(self) -> Any:
        return {"type": "object"}


def test_construct_payload_rejects_unknown_fields_for_pydantic() -> None:
    adapter = PydanticPayloadAdapter(RequestModel)
    with pytest.raises(ValidationError):
        construct_payload(adapter, {"name": "a", "nested": {"count": 1}, "extra": True})


def test_construct_payload_rejects_wrong_scalar_types() -> None:
    adapter = DataclassPayloadAdapter(RequestDc)
    with pytest.raises(ValidationError):
        construct_payload(adapter, {"name": 1, "nested": {"count": 1}})


def test_construct_payload_preserves_defaults_and_aliases() -> None:
    adapter = PydanticPayloadAdapter(AliasRequest)
    built = construct_payload(adapter, {"deviceId": "sensor-1"})
    assert built.device_id == "sensor-1"


def test_construct_payload_honors_before_validator() -> None:
    adapter = PydanticPayloadAdapter(ConvertedRequest)
    built = construct_payload(adapter, {"value": "42"})
    assert built.value == 42


def test_construct_payload_rejects_invalid_before_coercing_dataclass_init() -> None:
    global _coercing_init_calls
    _coercing_init_calls = 0
    adapter = DataclassPayloadAdapter(CoercingDataclass)
    with pytest.raises(ValidationError):
        construct_payload(adapter, {"value": "42"})
    assert _coercing_init_calls == 0


def test_validate_payload_rejects_model_construct_bypass() -> None:
    adapter = PydanticPayloadAdapter(RequestModel)
    invalid = RequestModel.model_construct(name="x", nested={"count": "bad"})
    with pytest.raises(ValidationError):
        validate_payload(adapter, invalid)


def test_validate_payload_rejects_mutated_nested_dataclass() -> None:
    adapter = DataclassPayloadAdapter(RequestDc)
    value = RequestDc(name="ok", nested=NestedDc(count=1))
    object.__setattr__(value.nested, "count", "bad")
    with pytest.raises(ValidationError):
        validate_payload(adapter, value)


def test_registry_construct_payload_for_type_preserves_type() -> None:
    built = construct_payload_for_type(RequestModel, name="job", nested={"count": 2})
    assert isinstance(built, RequestModel)
    assert built.nested.count == 2


def test_custom_adapter_construct_fields_is_unsupported() -> None:
    adapter = CustomAdapter()
    with pytest.raises(TypeError, match="does not support keyword field construction"):
        construct_payload(adapter, {"key": "value"})


def test_custom_adapter_requires_explicit_instance_validation_capability() -> None:
    with pytest.raises(TypeError, match="explicit instance validation"):
        validate_payload(CustomAdapter(), {"key": "value"})


def test_custom_adapter_event_preparation_preserves_legacy_dump_policy() -> None:
    codec = PayloadCodec(CustomAdapter(), JsonCodec())
    value = {"key": "value"}
    assert codec.prepare(value) is value


@pytest.mark.asyncio
async def test_invalid_client_request_is_rejected_before_backend_submit() -> None:
    transport = InMemoryTransport()
    jobs = SuperJobs(transport=transport)
    job = Job("tests.strict.submit", version="v1", request=RequestModel, result=RequestModel)

    @jobs.handler(job)
    async def handler(request: RequestModel, context) -> RequestModel:
        return request

    transport.submit = AsyncMock(side_effect=transport.submit)

    async with jobs:
        with pytest.raises((ValidationError, PayloadValidationError, ValueError)):
            await jobs.client(job).submit(
                RequestModel.model_construct(name="x", nested={"count": "bad"}),
            )
    transport.submit.assert_not_awaited()


@pytest.mark.asyncio
async def test_invalid_worker_result_is_permanent_invalid_result() -> None:
    jobs = SuperJobs(transport=InMemoryTransport())
    job = Job("tests.strict.result", version="v1", request=RequestModel, result=RequestModel)

    @jobs.handler(job)
    async def handler(request: RequestModel, context) -> RequestModel:
        return RequestModel.model_construct(name="x", nested={"count": "bad"})

    async with jobs:
        handle = await jobs.client(job).submit(
            RequestModel(name="ok", nested=NestedModel(count=1)),
        )
        with pytest.raises(JobFailedError) as raised:
            await handle.result()
    assert raised.value.error.code == "invalid_result"


@pytest.mark.asyncio
async def test_invalid_application_event_rejected_before_publish() -> None:
    jobs = SuperJobs(transport=InMemoryTransport())
    job = Job(
        "tests.strict.event",
        version="v1",
        request=RequestModel,
        result=RequestModel,
        event=RequestModel,
    )

    @jobs.handler(job)
    async def handler(request: RequestModel, context) -> RequestModel:
        await context.emit(
            RequestModel.model_construct(name="bad", nested={"count": "nope"}),
        )
        return request

    async with jobs:
        handle = await jobs.client(job).submit(
            RequestModel(name="ok", nested=NestedModel(count=1)),
        )
        with pytest.raises(JobFailedError):
            await handle.result()
        events = [event async for event in handle.events()]
    assert not any(
        isinstance(getattr(event, "data", None), RequestModel)
        and getattr(event.data, "name", None) == "bad"
        for event in events
    )


@pytest.mark.asyncio
async def test_explicit_event_conversion_is_preserved_in_memory() -> None:
    jobs = SuperJobs(transport=InMemoryTransport())
    job = Job(
        "tests.strict.converted.event",
        request=ConvertedRequest,
        result=ConvertedRequest,
        event=ConvertedRequest,
    )

    @jobs.handler(job)
    async def handler(request: ConvertedRequest, context) -> ConvertedRequest:
        await context.emit(ConvertedRequest.model_construct(value="42"))
        return request

    async with jobs:
        handle = await jobs.client(job).submit(ConvertedRequest(value=1))
        await handle.result()
        events = [item.data async for item in handle.events() if isinstance(item.data, ConvertedRequest)]
    assert len(events) == 1
    assert events[0].value == 42
    assert type(events[0].value) is int


def test_construct_payload_allows_nullable_defaults() -> None:
    class NullableRequest(BaseModel):
        value: int | None = None

    adapter = PydanticPayloadAdapter(NullableRequest)
    built = construct_payload(adapter, {})
    assert built.value is None


@pytest.mark.parametrize("codec", [JsonCodec(), MsgpackCodec()])
def test_rich_wire_values_round_trip_through_payload_codec(codec: JsonCodec | MsgpackCodec) -> None:
    when = datetime(2024, 5, 1, 12, 0, tzinfo=UTC)
    token = uuid.uuid4()
    value = RichWireModel(
        when=when,
        token=token,
        stage=Stage.ALPHA,
        payload=b"\x01\x02",
    )
    payload_codec = PayloadCodec(PydanticPayloadAdapter(RichWireModel), codec)
    decoded = payload_codec.decode(payload_codec.encode(value))
    assert decoded == value


@pytest.mark.parametrize("codec", [JsonCodec(), MsgpackCodec()])
def test_plain_python_scalar_validation_on_encode(codec: JsonCodec | MsgpackCodec) -> None:
    payload_codec = PayloadCodec(PlainPythonPayloadAdapter(int), codec)
    with pytest.raises(ValidationError):
        payload_codec.encode("not-an-int")  # type: ignore[arg-type]


def test_instance_validation_is_stricter_than_wire_load_for_typed_fields() -> None:
    when = datetime(2024, 5, 1, 12, 0, tzinfo=UTC)
    adapter = PydanticPayloadAdapter(RichWireModel)
    wire = adapter.dump(
        RichWireModel(
            when=when,
            token=uuid.uuid4(),
            stage=Stage.ALPHA,
            payload=b"\x00",
        ),
    )
    loaded = adapter.load(wire)
    assert isinstance(loaded.when, datetime)
    invalid_instance = RichWireModel.model_construct(
        when=when.isoformat(),
        token=loaded.token,
        stage=Stage.ALPHA,
        payload=b"\x00",
    )
    with pytest.raises(ValidationError):
        validate_payload(adapter, invalid_instance)


def test_payload_codec_rejects_unknown_wire_fields() -> None:
    payload_codec = PayloadCodec(PydanticPayloadAdapter(RequestModel), JsonCodec())
    with pytest.raises(ValidationError):
        payload_codec.decode(
            b'{"name":"x","nested":{"count":1},"unexpected":true}',
        )


def test_builtin_adapter_dump_revalidates_explicit_instance() -> None:
    adapter = PydanticPayloadAdapter(RequestModel)
    invalid = RequestModel.model_construct(name="x", nested={"count": "bad"})
    with pytest.raises(ValidationError):
        adapter.dump(invalid)


class SplitAliasRequest(BaseModel):
    device_id: str = Field(
        validation_alias="inputDevice",
        serialization_alias="outputDevice",
    )

    model_config = {"populate_by_name": True}


@pytest.mark.parametrize("codec", [JsonCodec(), MsgpackCodec()])
def test_split_validation_and_serialization_aliases_round_trip(
    codec: JsonCodec | MsgpackCodec,
) -> None:
    adapter = PydanticPayloadAdapter(SplitAliasRequest)
    built = construct_payload(adapter, {"inputDevice": "sensor-7"})
    assert built.device_id == "sensor-7"
    assert built.model_dump(mode="json", by_alias=True) == {"outputDevice": "sensor-7"}
    payload_codec = PayloadCodec(adapter, codec)
    decoded = payload_codec.decode(payload_codec.encode(built))
    assert decoded.device_id == "sensor-7"


@pytest.mark.parametrize("codec", [JsonCodec(), MsgpackCodec()])
def test_wire_rejects_coercible_integer_strings(codec: JsonCodec | MsgpackCodec) -> None:
    payload_codec = PayloadCodec(PydanticPayloadAdapter(RequestModel), codec)
    wire: WireValue = {"name": "x", "nested": {"count": "1"}}
    with pytest.raises(ValidationError):
        payload_codec.decode(codec.encode(wire))


_default_factory_calls = 0


def _default_tags_factory() -> list[str]:
    global _default_factory_calls
    _default_factory_calls += 1
    return ["a"]


@dataclass
class DefaultOnceDc:
    tags: list[str] = field(default_factory=_default_tags_factory)


def test_default_factory_runs_once_for_construct_fields() -> None:
    global _default_factory_calls
    _default_factory_calls = 0
    adapter = DataclassPayloadAdapter(DefaultOnceDc)
    built = construct_payload(adapter, {})
    assert built.tags == ["a"]
    assert _default_factory_calls == 1


@dataclass
class PostInitCorruptDc:
    value: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "value", "bad")


def test_post_init_corruption_is_rejected_on_validate() -> None:
    adapter = DataclassPayloadAdapter(PostInitCorruptDc)
    corrupt = PostInitCorruptDc(value=1)
    with pytest.raises(ValidationError):
        validate_payload(adapter, corrupt)


_after_validator_calls = 0


class AfterOnceModel(BaseModel):
    value: int

    @field_validator("value", mode="after")
    @classmethod
    def bump(cls, value: int) -> int:
        global _after_validator_calls
        _after_validator_calls += 1
        return value + 1


def test_after_validator_runs_once_per_construct_boundary() -> None:
    global _after_validator_calls
    _after_validator_calls = 0
    adapter = PydanticPayloadAdapter(AfterOnceModel)
    built = construct_payload(adapter, {"value": 1})
    assert built.value == 2
    assert _after_validator_calls == 1


@pytest.mark.asyncio
async def test_invalid_worker_result_stays_permanent_with_retries() -> None:
    jobs = SuperJobs(transport=InMemoryTransport())
    job = Job("tests.strict.result.retry", version="v1", request=RequestModel, result=RequestModel)

    @jobs.handler(job, retry=RetryPolicy(max_attempts=3))
    async def handler(request: RequestModel, context) -> RequestModel:
        return RequestModel.model_construct(name="x", nested={"count": "bad"})

    async with jobs:
        handle = await jobs.client(job).submit(
            RequestModel(name="ok", nested=NestedModel(count=1)),
        )
        with pytest.raises(JobFailedError) as raised:
            await handle.result()
    assert raised.value.error.code == "invalid_result"
