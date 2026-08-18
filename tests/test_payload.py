from dataclasses import dataclass
from typing import Any

import msgpack
import pytest
from pydantic import BaseModel, ValidationError
from superjobs.payload.adapter.implementations.dataclass import (
    DataclassAdapterFactory,
    DataclassPayloadAdapter,
)
from superjobs.payload.adapter.implementations.pydantic import (
    PydanticAdapterFactory,
    PydanticPayloadAdapter,
)
from superjobs.payload.adapter.implementations.python import (
    PlainPythonAdapterFactory,
    PlainPythonPayloadAdapter,
)
from superjobs.payload.codec.implementations.json import JsonCodec
from superjobs.payload.codec.implementations.msgpack import MsgpackCodec
from superjobs.payload.codec.payloadcodec import PayloadCodec
from superjobs.registry.registry import SuperjobsRegistry


@dataclass(frozen=True)
class Address:
    city: str
    postal_code: int


@dataclass(frozen=True)
class DataclassPayload:
    identifier: int
    title: str
    address: Address
    labels: list[str]


class PydanticAddress(BaseModel):
    city: str
    postal_code: int


class PydanticPayload(BaseModel):
    identifier: int
    title: str
    address: PydanticAddress
    labels: list[str]


@pytest.mark.parametrize(
    ("type_", "expected_factory", "expected_adapter"),
    [
        (str, PlainPythonAdapterFactory, PlainPythonPayloadAdapter),
        (int, PlainPythonAdapterFactory, PlainPythonPayloadAdapter),
        (float, PlainPythonAdapterFactory, PlainPythonPayloadAdapter),
        (bool, PlainPythonAdapterFactory, PlainPythonPayloadAdapter),
        (list, PlainPythonAdapterFactory, PlainPythonPayloadAdapter),
        (dict, PlainPythonAdapterFactory, PlainPythonPayloadAdapter),
        (type(None), PlainPythonAdapterFactory, PlainPythonPayloadAdapter),
        (DataclassPayload, DataclassAdapterFactory, DataclassPayloadAdapter),
        (PydanticPayload, PydanticAdapterFactory, PydanticPayloadAdapter),
    ],
)
def test_exactly_one_factory_selects_each_supported_payload_type(
    type_: Any,
    expected_factory: type[Any],
    expected_adapter: type[Any],
) -> None:
    factories = [
        PlainPythonAdapterFactory(),
        DataclassAdapterFactory(),
        PydanticAdapterFactory(),
    ]

    matching_factories = [factory for factory in factories if factory.supports(type_)]

    assert len(matching_factories) == 1
    assert isinstance(matching_factories[0], expected_factory)
    assert isinstance(matching_factories[0].create(type_), expected_adapter)


@pytest.mark.parametrize(
    "type_",
    [
        None,
        object(),
        list[str],
        tuple,
        set,
        PydanticPayload | None,
        DataclassPayload(
            identifier=1,
            title="example",
            address=Address(city="Berlin", postal_code=10115),
            labels=[],
        ),
        PydanticPayload(
            identifier=1,
            title="example",
            address=PydanticAddress(city="Berlin", postal_code=10115),
            labels=[],
        ),
    ],
    ids=[
        "none",
        "instance",
        "generic-list",
        "tuple",
        "set",
        "union",
        "dataclass-instance",
        "pydantic-instance",
    ],
)
@pytest.mark.parametrize(
    "factory",
    [
        PlainPythonAdapterFactory(),
        DataclassAdapterFactory(),
        PydanticAdapterFactory(),
    ],
    ids=["plain-python", "dataclass", "pydantic"],
)
def test_factories_reject_non_supported_type_descriptors(
    factory: Any,
    type_: Any,
) -> None:
    assert factory.supports(type_) is False


@pytest.mark.parametrize(
    ("type_", "expected_factory"),
    [
        (str, PlainPythonAdapterFactory),
        (dict, PlainPythonAdapterFactory),
        (DataclassPayload, DataclassAdapterFactory),
        (PydanticPayload, PydanticAdapterFactory),
    ],
)
def test_registry_selects_and_caches_the_factory_for_a_payload_type(
    type_: type[Any],
    expected_factory: type[Any],
) -> None:
    registry = SuperjobsRegistry()
    registry.register_adapter(PydanticAdapterFactory())
    registry.register_adapter(DataclassAdapterFactory())
    registry.register_adapter(PlainPythonAdapterFactory())

    selected = registry.get_adapter(type_)

    assert isinstance(selected, expected_factory)
    assert registry.get_adapter(type_) is selected


def test_registry_rejects_types_without_an_adapter() -> None:
    registry = SuperjobsRegistry()
    registry.register_adapter(PlainPythonAdapterFactory())

    with pytest.raises(ValueError, match="No adapter found"):
        registry.get_adapter(tuple)


@pytest.mark.parametrize(
    ("type_", "value"),
    [
        (str, "a plain string"),
        (int, 42),
        (float, 3.5),
        (bool, True),
        (list, ["one", 2, False, None]),
        (dict, {"name": "Ada", "scores": [10, 9], "active": True}),
        (type(None), None),
    ],
    ids=["str", "int", "float", "bool", "list", "dict", "none"],
)
def test_plain_python_adapter_round_trips_wire_values(
    type_: type[Any],
    value: Any,
) -> None:
    adapter = PlainPythonPayloadAdapter(type_)

    dumped = adapter.dump(value)

    assert dumped == value
    assert adapter.load(dumped) == value
    assert adapter.schema() is not None


def test_plain_python_factory_creates_a_working_adapter() -> None:
    adapter = PlainPythonAdapterFactory().create(dict)

    assert adapter.dump({"answer": 42}) == {"answer": 42}
    assert adapter.load({"answer": 42}) == {"answer": 42}


def test_dataclass_adapter_dumps_nested_dataclasses_to_wire_values() -> None:
    value = DataclassPayload(
        identifier=7,
        title="Generate report",
        address=Address(city="Berlin", postal_code=10115),
        labels=["urgent", "batch"],
    )
    adapter = DataclassPayloadAdapter(DataclassPayload)

    assert adapter.dump(value) == {
        "identifier": 7,
        "title": "Generate report",
        "address": {"city": "Berlin", "postal_code": 10115},
        "labels": ["urgent", "batch"],
    }


def test_dataclass_adapter_loads_nested_wire_values_to_dataclasses() -> None:
    adapter = DataclassPayloadAdapter(DataclassPayload)
    wire_value = {
        "identifier": 7,
        "title": "Generate report",
        "address": {"city": "Berlin", "postal_code": 10115},
        "labels": ["urgent", "batch"],
    }

    loaded = adapter.load(wire_value)

    assert loaded == DataclassPayload(
        identifier=7,
        title="Generate report",
        address=Address(city="Berlin", postal_code=10115),
        labels=["urgent", "batch"],
    )
    assert isinstance(loaded, DataclassPayload)
    assert isinstance(loaded.address, Address)


def test_dataclass_adapter_exposes_a_schema_and_validates_input() -> None:
    adapter = DataclassPayloadAdapter(DataclassPayload)
    schema = adapter.schema()

    assert schema["type"] == "object"
    assert schema["properties"]["identifier"]["type"] == "integer"
    assert set(schema["required"]) == {"identifier", "title", "address", "labels"}

    with pytest.raises(ValidationError):
        adapter.load({"identifier": 7})


def test_pydantic_adapter_dumps_nested_models_to_wire_values() -> None:
    value = PydanticPayload(
        identifier=7,
        title="Generate report",
        address=PydanticAddress(city="Berlin", postal_code=10115),
        labels=["urgent", "batch"],
    )
    adapter = PydanticPayloadAdapter(PydanticPayload)

    assert adapter.dump(value) == {
        "identifier": 7,
        "title": "Generate report",
        "address": {"city": "Berlin", "postal_code": 10115},
        "labels": ["urgent", "batch"],
    }


def test_pydantic_adapter_loads_nested_wire_values_to_models() -> None:
    adapter = PydanticPayloadAdapter(PydanticPayload)
    wire_value = {
        "identifier": 7,
        "title": "Generate report",
        "address": {"city": "Berlin", "postal_code": 10115},
        "labels": ["urgent", "batch"],
    }

    loaded = adapter.load(wire_value)

    assert loaded == PydanticPayload(
        identifier=7,
        title="Generate report",
        address=PydanticAddress(city="Berlin", postal_code=10115),
        labels=["urgent", "batch"],
    )
    assert isinstance(loaded, PydanticPayload)
    assert isinstance(loaded.address, PydanticAddress)


def test_pydantic_adapter_exposes_a_schema_and_validates_input() -> None:
    adapter = PydanticPayloadAdapter(PydanticPayload)
    schema = adapter.schema()

    assert schema["type"] == "object"
    assert schema["properties"]["identifier"]["type"] == "integer"
    assert set(schema["required"]) == {"identifier", "title", "address", "labels"}

    with pytest.raises(ValidationError):
        adapter.load({"identifier": "not-an-integer"})


@pytest.mark.parametrize(
    ("codec_type", "media_type"),
    [
        (JsonCodec, "application/json"),
        (MsgpackCodec, "application/msgpack"),
    ],
)
def test_wire_codec_exposes_its_media_type(
    codec_type: type[Any],
    media_type: str,
) -> None:
    assert codec_type().media_type == media_type


@pytest.mark.parametrize(
    "codec_type",
    [JsonCodec, MsgpackCodec],
    ids=["json", "msgpack"],
)
@pytest.mark.parametrize(
    "value",
    [
        None,
        False,
        17,
        -2.5,
        "Grüße",
        [],
        [1, "two", True, None],
        {"message": "hello", "nested": {"count": 2}},
    ],
    ids=["none", "bool", "int", "float", "text", "empty-list", "list", "dict"],
)
def test_wire_codecs_round_trip_every_wire_value_shape(
    codec_type: type[Any],
    value: Any,
) -> None:
    codec = codec_type()

    encoded = codec.encode(value)

    assert isinstance(encoded, bytes)
    assert codec.decode(encoded) == value


def test_json_codec_uses_compact_json_bytes() -> None:
    codec = JsonCodec()

    assert codec.encode({"message": "hello", "items": [1, True, None]}) == (b'{"message":"hello","items":[1,true,null]}')
    assert codec.decode(b'{"message":"hello","items":[1,true,null]}') == {
        "message": "hello",
        "items": [1, True, None],
    }


def test_msgpack_codec_produces_data_decodable_by_msgpack() -> None:
    value = {"message": "hello", "items": [1, True, None]}
    codec = MsgpackCodec()

    encoded = codec.encode(value)

    assert msgpack.unpackb(encoded) == value


@pytest.mark.parametrize(
    "wire_codec_type",
    [JsonCodec, MsgpackCodec],
    ids=["json", "msgpack"],
)
@pytest.mark.parametrize(
    ("adapter_factory", "type_", "value"),
    [
        (
            PlainPythonAdapterFactory,
            dict,
            {"message": "hello", "attempts": [1, 2], "successful": True},
        ),
        (
            DataclassAdapterFactory,
            DataclassPayload,
            DataclassPayload(
                identifier=7,
                title="Generate report",
                address=Address(city="Berlin", postal_code=10115),
                labels=["urgent", "batch"],
            ),
        ),
        (
            PydanticAdapterFactory,
            PydanticPayload,
            PydanticPayload(
                identifier=7,
                title="Generate report",
                address=PydanticAddress(city="Berlin", postal_code=10115),
                labels=["urgent", "batch"],
            ),
        ),
    ],
    ids=["plain-python", "dataclass", "pydantic"],
)
def test_payload_codec_composes_adapter_and_wire_codec(
    wire_codec_type: type[Any],
    adapter_factory: type[Any],
    type_: type[Any],
    value: Any,
) -> None:
    adapter = adapter_factory().create(type_)
    codec = PayloadCodec(adapter, wire_codec_type())

    encoded = codec.encode(value)
    decoded = codec.decode(encoded)

    assert decoded == value
    assert codec.media_type == wire_codec_type().media_type
    assert codec.schema() == adapter.schema()


def test_registry_builds_a_payload_codec_from_the_selected_adapter() -> None:
    registry = SuperjobsRegistry()
    registry.register_adapter(PydanticAdapterFactory())
    value = PydanticPayload(
        identifier=7,
        title="Generate report",
        address=PydanticAddress(city="Berlin", postal_code=10115),
        labels=["urgent", "batch"],
    )

    codec = registry.get_payload_codec(PydanticPayload, JsonCodec())

    assert codec.decode(codec.encode(value)) == value
    assert codec.media_type == "application/json"


def test_payload_codec_validates_values_after_wire_decoding() -> None:
    payload_codec = PayloadCodec(
        PydanticAdapterFactory().create(PydanticPayload),
        JsonCodec(),
    )

    with pytest.raises(ValidationError):
        payload_codec.decode(b'{"identifier":"not-an-integer"}')
