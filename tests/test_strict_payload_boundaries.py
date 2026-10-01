from dataclasses import InitVar, dataclass, field
from typing import ClassVar

import pytest
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from superjobs import Job, construct_payload_for_type
from superjobs.payload.adapter.implementations.pydantic import PydanticPayloadAdapter
from superjobs.payload.codec.implementations.json import JsonCodec
from superjobs.payload.codec.implementations.msgpack import MsgpackCodec
from superjobs.payload.codec.payloadcodec import PayloadCodec


class Inner(BaseModel):
    count: int


class Outer(BaseModel):
    inner: Inner


@dataclass
class InnerDc:
    count: int


@dataclass
class OuterDc:
    inner: InnerDc


@pytest.mark.parametrize("bad", ["42", True])
def test_nested_prebuilt_model_is_revalidated(bad: object) -> None:
    value = Outer.model_construct(inner=Inner.model_construct(count=bad))
    job = Job("independent.model", request=Outer)
    with pytest.raises(ValueError):
        job.encode_request(value)


@pytest.mark.parametrize("bad", ["42", True])
def test_nested_dataclass_is_revalidated(bad: object) -> None:
    value = OuterDc(InnerDc(1))
    value.inner.count = bad
    job = Job("independent.dataclass", request=OuterDc)
    with pytest.raises(ValueError):
        job.encode_request(value)


class AliasModel(BaseModel):
    model_config = ConfigDict(serialize_by_alias=True)
    count: int = Field(validation_alias="inputCount", serialization_alias="outputCount")


@pytest.mark.parametrize("wire", [JsonCodec(), MsgpackCodec()])
def test_different_validation_and_serialization_alias_round_trip(wire: object) -> None:
    codec = PayloadCodec(PydanticPayloadAdapter(AliasModel), wire)
    original = construct_payload_for_type(AliasModel, inputCount=7)
    encoded = codec.encode(original)
    decoded = codec.decode(encoded)
    assert isinstance(decoded, AliasModel)
    assert decoded.count == 7


@pytest.mark.parametrize("wire", [JsonCodec(), MsgpackCodec()])
def test_wire_rejects_implicit_int_conversion(wire: object) -> None:
    codec = PayloadCodec(PydanticPayloadAdapter(Inner), wire)
    malformed = wire.encode({"count": "42"})
    with pytest.raises(ValueError):
        codec.decode(malformed)


def test_unknown_nested_fields_in_raw_construction() -> None:
    with pytest.raises(ValueError):
        construct_payload_for_type(Outer, inner={"count": 1, "extra": True})
    with pytest.raises(ValueError):
        construct_payload_for_type(OuterDc, inner={"count": 1, "extra": True})


class CountedConversion(BaseModel):
    calls: ClassVar[int] = 0
    value: int

    @field_validator("value", mode="before")
    @classmethod
    def convert(cls, value: object) -> object:
        cls.calls += 1
        return int(value) if isinstance(value, str) and value.isdigit() else value


def test_converter_not_duplicated_within_construction_or_encode() -> None:
    CountedConversion.calls = 0
    built = construct_payload_for_type(CountedConversion, value="42")
    assert built.value == 42
    assert CountedConversion.calls == 1
    CountedConversion.calls = 0
    Job("independent.converter", request=CountedConversion).encode_request(built)
    assert CountedConversion.calls <= 1


def test_event_preparation_converts_once_and_retains_validated_object() -> None:
    codec = PayloadCodec(PydanticPayloadAdapter(CountedConversion), JsonCodec())
    CountedConversion.calls = 0
    prepared = codec.prepare(CountedConversion.model_construct(value="42"))
    assert prepared.value == 42
    assert type(prepared.value) is int
    assert CountedConversion.calls == 1


def test_event_preparation_checks_wire_encoder_before_buffering() -> None:
    class RejectingCodec(JsonCodec):
        def encode(self, value) -> bytes:
            raise ValueError("wire encoding rejected")

    codec = PayloadCodec(PydanticPayloadAdapter(Inner), RejectingCodec())
    with pytest.raises(ValueError, match="wire encoding rejected"):
        codec.prepare(Inner(count=1))


@dataclass
class CorruptingPostInit:
    value: int

    def __post_init__(self) -> None:
        self.value = "bad"


class CorruptingModelPostInit(BaseModel):
    value: int

    def model_post_init(self, context: object) -> None:
        self.__dict__["value"] = "bad"


@pytest.mark.parametrize("type_", [CorruptingPostInit, CorruptingModelPostInit])
def test_hooks_cannot_corrupt_strict_output(type_: type) -> None:
    with pytest.raises(ValueError):
        construct_payload_for_type(type_, value=1)


class CountedDefaultModel(BaseModel):
    calls: ClassVar[int] = 0

    @staticmethod
    def factory() -> list[int]:
        CountedDefaultModel.calls += 1
        return [1]

    values: list[int] = Field(default_factory=factory)


def test_default_factory_once() -> None:
    CountedDefaultModel.calls = 0
    built = construct_payload_for_type(CountedDefaultModel)
    assert built.values == [1]
    assert CountedDefaultModel.calls == 1


def test_model_configuration_is_unchanged() -> None:
    prior = dict(Outer.model_config)
    validator = Outer.__pydantic_validator__
    construct_payload_for_type(Outer, inner={"count": 1})
    assert Outer.model_config == prior
    assert Outer.__pydantic_validator__ is validator


class BinaryModel(BaseModel):
    model_config = ConfigDict(ser_json_bytes="base64", val_json_bytes="base64")
    data: bytes


@pytest.mark.parametrize("wire", [JsonCodec(), MsgpackCodec()])
def test_declared_binary_representation_is_preserved(wire: object) -> None:
    codec = PayloadCodec(PydanticPayloadAdapter(BinaryModel), wire)
    original = BinaryModel(data=b"\xff\x00")
    assert codec.decode(codec.encode(original)) == original


class OrderedRange(BaseModel):
    lower: int
    upper: int

    @model_validator(mode="after")
    def ordered(self) -> "OrderedRange":
        if self.lower > self.upper:
            raise ValueError("lower must not exceed upper")
        return self


def test_instance_revalidation_runs_model_invariants() -> None:
    job = Job("independent.invariant", request=OrderedRange)
    with pytest.raises(ValueError, match="lower must not exceed upper"):
        job.encode_request(OrderedRange.model_construct(lower=10, upper=1))


class CorruptingConstraint(BaseModel):
    value: int = Field(gt=0)

    @field_validator("value", mode="after")
    @classmethod
    def corrupt(cls, value: int) -> int:
        return -value


def test_output_guard_preserves_field_constraints_without_repeating_converter() -> None:
    with pytest.raises(ValueError):
        construct_payload_for_type(CorruptingConstraint, value=1)


class HookCounter(BaseModel):
    calls: ClassVar[int] = 0
    value: int

    def model_post_init(self, context: object) -> None:
        HookCounter.calls += 1


def test_post_init_runs_once_per_boundary() -> None:
    HookCounter.calls = 0
    value = construct_payload_for_type(HookCounter, value=1)
    assert HookCounter.calls == 1
    HookCounter.calls = 0
    Job("independent.hooks", request=HookCounter).encode_request(value)
    assert HookCounter.calls == 1


def test_explicit_model_boundary_requires_an_instance() -> None:
    job = Job("independent.instance", request=Inner)
    with pytest.raises(ValueError, match="Expected an instance"):
        job.encode_request({"count": 1})


def test_nonfinite_float_rejected_before_transport_encoding() -> None:
    job = Job("independent.finite", request=float)
    with pytest.raises(ValueError):
        job.encode_request(float("nan"))


def test_derived_default_factory_is_not_repeated_and_round_trips() -> None:
    calls = 0

    def default() -> int:
        nonlocal calls
        calls += 1
        return 2

    @dataclass
    class Derived:
        value: int
        derived: int = field(init=False, default_factory=default)

    value = construct_payload_for_type(Derived, value=1)
    assert calls == 1
    job = Job("independent.derived", request=Derived)
    wire = job.encode_request(value)
    assert calls == 1
    assert job.decode_request(wire) == value
    assert calls == 1
    value.derived = "42"
    with pytest.raises(ValueError):
        job.encode_request(value)


def test_unsupported_dataclass_construction_state_fails_setup() -> None:
    @dataclass
    class InitOnly:
        value: int
        temporary: InitVar[int]

    @dataclass
    class UnrepresentedDerived:
        value: int
        derived: int = field(init=False)

        def __post_init__(self) -> None:
            self.derived = self.value

    for type_ in (InitOnly, UnrepresentedDerived):
        with pytest.raises(TypeError, match="custom payload adapter"):
            Job("independent.unsupported", request=type_)
