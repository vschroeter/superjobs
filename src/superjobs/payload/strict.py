from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import fields as dataclass_fields
from dataclasses import is_dataclass
from functools import lru_cache
from typing import Any, NoReturn, Protocol, get_type_hints, runtime_checkable

from pydantic import BaseModel, ValidationError
from pydantic import TypeAdapter as PydanticTypeAdapter
from pydantic_core import CoreSchema, SchemaValidator

from superjobs.payload.adapter.protocol import PayloadAdapter, WireValue

_REBUILD_SUPPORTED: bool | None = None

_SCHEMA_CHILD_KEYS = (
    "schema",
    "fields",
    "keys_schema",
    "values_schema",
    "items_schema",
    "arguments_schema",
    "steps",
    "choices",
    "left",
    "right",
    "then_schema",
    "else_schema",
    "first_schema",
    "second_schema",
    "js_schema",
    "metadata_schema",
    "python_schema",
    "json_schema",
    "lax_schema",
    "strict_schema",
)


class PayloadValidationError(ValueError):
    """Raised when a payload fails strict validation at a SuperJobs boundary."""


def _ensure_rebuild_supported() -> None:
    global _REBUILD_SUPPORTED
    if _REBUILD_SUPPORTED is True:
        return
    if _REBUILD_SUPPORTED is False:
        raise RuntimeError(_rebuild_unavailable_message())
    try:
        SchemaValidator({"type": "int"}, _use_prebuilt=False)
    except TypeError as exc:
        _REBUILD_SUPPORTED = False
        raise RuntimeError(_rebuild_unavailable_message()) from exc
    _REBUILD_SUPPORTED = True


def _rebuild_unavailable_message() -> str:
    return (
        "Strict payload validation requires pydantic-core rebuild support "
        "(_use_prebuilt=False). Install pydantic >= 2.13 with a matching "
        "pydantic-core (measured: 2.13.4 / core 2.46.4)."
    )


def _make_validator(schema: CoreSchema) -> SchemaValidator:
    _ensure_rebuild_supported()
    return SchemaValidator(schema, config={"strict": True}, _use_prebuilt=False)


def _clone_schema(schema: Any) -> Any:
    # Preserve opaque defaults, callable descriptors and classes by identity.
    # Only the schema's containers are copied; no application objects are patched.
    if isinstance(schema, dict):
        return {key: _clone_schema(value) for key, value in schema.items()}
    if isinstance(schema, list):
        return [_clone_schema(value) for value in schema]
    if isinstance(schema, tuple):
        return tuple(_clone_schema(value) for value in schema)
    return schema


def _set_config_extra_forbid(node: dict[str, Any]) -> None:
    config = dict(node.get("config") or {})
    config["extra_fields_behavior"] = "forbid"
    config["strict"] = True
    node["config"] = config


def _strip_application_hooks(schema: Any) -> Any:
    if isinstance(schema, (list, tuple)):
        return type(schema)(_strip_application_hooks(item) for item in schema)
    if not isinstance(schema, dict):
        return schema

    node_type = schema.get("type")
    if node_type in {"function-before", "function-after", "function-wrap"}:
        return _strip_application_hooks(schema["schema"])
    if node_type == "function-plain":
        raise TypeError("Plain validators without an underlying schema require a custom payload adapter")

    node = dict(schema)
    if node_type == "model":
        node.pop("post_init", None)
    if node_type == "dataclass":
        node["post_init"] = False

    for key in _SCHEMA_CHILD_KEYS:
        if key not in node:
            continue
        child = node[key]
        if key == "fields" and isinstance(child, dict):
            node[key] = {
                field_name: _strip_application_hooks(field_schema)
                for field_name, field_schema in child.items()
            }
        else:
            node[key] = _strip_application_hooks(child)

    if node_type == "definitions":
        node["definitions"] = _strip_application_hooks(node["definitions"])

    return node


def _patch_schema_node(schema: Any, *, mode: str) -> Any:
    if isinstance(schema, (list, tuple)):
        return type(schema)(_patch_schema_node(item, mode=mode) for item in schema)
    if not isinstance(schema, dict):
        return schema

    node = dict(schema)
    node_type = node.get("type")

    if node_type == "default":
        node["validate_default"] = True

    if node_type == "model-fields":
        node["strict"] = True
        node["extra_behavior"] = "forbid"

    if node_type == "dataclass-field":
        if node.get("init_only"):
            raise TypeError("Dataclass InitVar fields require a custom payload adapter")
        # Existing derived values are input during instance revalidation and wire
        # decoding. Raw constructor fields keep the original init=False policy.
        if mode != "raw" and node.get("init") is False:
            node["init"] = True

    if node_type == "dataclass-args":
        node["extra_behavior"] = "forbid"

    if node_type == "model":
        node["revalidate_instances"] = "always"
        node["custom_init"] = False
        _set_config_extra_forbid(node)
        if mode == "instance":
            node["strict"] = True
        elif mode in {"raw", "wire"}:
            node["strict"] = False

    if node_type == "dataclass":
        args_schema = node["schema"]
        while args_schema.get("type") in {"function-before", "function-after", "function-wrap"}:
            args_schema = args_schema["schema"]
        represented = {field["name"] for field in args_schema.get("fields", [])}
        if any(not field.init and field.name not in represented for field in dataclass_fields(node["cls"])):
            raise TypeError("Dataclass init=False fields without schema-backed defaults require a custom payload adapter")
        _set_config_extra_forbid(node)
        node["revalidate_instances"] = "always"
        if mode == "instance":
            node["strict"] = True
        else:
            node["strict"] = False

    if node_type == "definitions":
        node["definitions"] = [
            _patch_schema_node(definition, mode=mode)
            for definition in node.get("definitions", [])
        ]

    for key in _SCHEMA_CHILD_KEYS:
        if key not in node:
            continue
        child = node[key]
        if key == "fields" and isinstance(child, dict):
            node[key] = {
                field_name: _patch_schema_node(field_schema, mode=mode)
                for field_name, field_schema in child.items()
            }
        else:
            node[key] = _patch_schema_node(child, mode=mode)

    return node


def _schema_for_mode(base_schema: CoreSchema, mode: str) -> CoreSchema:
    return _patch_schema_node(_clone_schema(base_schema), mode=mode)


def _validate_output_structure(type_: Any, value: Any) -> None:
    if isinstance(type_, type) and issubclass(type_, BaseModel):
        for name, field in type_.model_fields.items():
            _validate_output_structure(field.rebuild_annotation(), getattr(value, name))
        return

    if isinstance(type_, type) and is_dataclass(type_):
        annotations = get_type_hints(type_, include_extras=True)
        for field in dataclass_fields(type_):
            nested_value = getattr(value, field.name)
            _validate_output_structure(annotations[field.name], nested_value)
        return

    if type_ in (Any,):
        return

    _output_validator_for(type_).validate_python(value, strict=True, by_name=True, by_alias=False)


@lru_cache(maxsize=None)
def _output_validator_for(type_: Any) -> SchemaValidator:
    schema = _schema_for_mode(PydanticTypeAdapter(type_).core_schema, "instance")
    return _make_validator(_strip_application_hooks(schema))


class StrictPayloadBoundary:
    __slots__ = ("_raw_fields", "_serializer", "_shape", "_structure", "_type", "_wire")

    def __init__(self, type_: Any):
        _ensure_rebuild_supported()
        self._type = type_
        adapter = PydanticTypeAdapter(type_)
        base_schema = adapter.core_schema
        self._serializer = adapter
        self._wire = _make_validator(_schema_for_mode(base_schema, "wire"))
        self._raw_fields = _make_validator(_schema_for_mode(base_schema, "raw"))
        instance_schema = _schema_for_mode(base_schema, "instance")
        self._shape = _make_validator(instance_schema)
        self._structure = _make_validator(_strip_application_hooks(instance_schema))

    def _check_output(self, value: Any) -> None:
        self._structure.validate_python(value, strict=True, by_name=True, by_alias=False)
        # Also check derived dataclass fields, which are absent from constructor schemas.
        _validate_output_structure(self._type, value)

    def validate_wire(self, value: WireValue) -> Any:
        payload = json.dumps(value, allow_nan=False).encode("utf-8")
        validated = self._wire.validate_json(payload, strict=True, by_name=True, by_alias=False)
        self._check_output(validated)
        return validated

    def construct_fields(self, fields: Mapping[str, Any]) -> Any:
        built = self._raw_fields.validate_python(dict(fields), by_alias=True)
        self._check_output(built)
        return built

    def validate_instance(self, value: Any) -> Any:
        if isinstance(self._type, type) and (issubclass(self._type, BaseModel) or is_dataclass(self._type)):
            if not isinstance(value, self._type):
                raise PayloadValidationError(f"Expected an instance of {self._type.__name__}")
        validated = self._shape.validate_python(value, strict=True, by_name=True, by_alias=False)
        self._check_output(validated)
        return validated

    def dump_wire(self, value: Any) -> WireValue:
        wire = self._serializer.dump_python(value, mode="json", by_alias=False)
        json.dumps(wire, allow_nan=False)
        if not isinstance(wire, (dict, list, str, int, float, bool)) and wire is not None:
            msg = f"Unexpected wire value type {type(wire)!r} for {self._type!r}"
            raise PayloadValidationError(msg)
        return wire  # type: ignore[return-value]

    def prepare_instance(self, value: Any) -> tuple[Any, WireValue]:
        validated = self.validate_instance(value)
        # Prove wire compatibility before buffering; retain the converted object.
        return validated, self.dump_wire(validated)

    def json_schema(self) -> Any:
        return self._serializer.json_schema(mode="serialization", by_alias=False)


@lru_cache(maxsize=None)
def strict_boundary_for(type_: Any) -> StrictPayloadBoundary:
    return StrictPayloadBoundary(type_)


def _raise_payload_validation(exc: BaseException) -> NoReturn:
    if isinstance(exc, ValidationError):
        raise exc
    if isinstance(exc, PayloadValidationError):
        raise exc
    raise PayloadValidationError(str(exc)) from exc


@runtime_checkable
class FieldConstructiblePayloadAdapter[T](PayloadAdapter[T], Protocol):
    def validate_instance(self, value: T) -> T: ...

    def construct_fields(self, fields: Mapping[str, Any]) -> T: ...


def validate_payload[T](adapter: PayloadAdapter[T], value: T) -> T:
    """Revalidate a Python payload before encoding or handler-side emission."""
    validate = getattr(adapter, "validate_instance", None)
    if validate is None:
        raise TypeError("This payload adapter does not support explicit instance validation; use its dump method")
    try:
        return validate(value)
    except ValidationError:
        raise
    except Exception as exc:
        _raise_payload_validation(exc)


def construct_payload[T](adapter: PayloadAdapter[T], fields: Mapping[str, Any]) -> T:
    """Validate raw keyword fields and build a typed payload (for future keyword submit)."""
    construct = getattr(adapter, "construct_fields", None)
    if construct is None:
        msg = (
            "This payload adapter does not support keyword field construction. "
            "Built-in dataclass, Pydantic, and plain-Python adapters implement it; "
            "custom adapters must provide construct_fields explicitly."
        )
        raise TypeError(msg)
    try:
        return construct(fields)
    except TypeError:
        raise
    except ValidationError:
        raise
    except Exception as exc:
        _raise_payload_validation(exc)


def load_wire_value[T](adapter: PayloadAdapter[T], value: WireValue) -> T:
    try:
        return adapter.load(value)
    except ValidationError:
        raise
    except Exception as exc:
        _raise_payload_validation(exc)


def dump_python_value[T](adapter: PayloadAdapter[T], value: T) -> WireValue:
    try:
        return adapter.dump(value)
    except ValidationError:
        raise
    except PayloadValidationError:
        raise
    except Exception as exc:
        _raise_payload_validation(exc)


def prepare_python_value[T](adapter: PayloadAdapter[T], value: T) -> tuple[T, WireValue]:
    prepare = getattr(adapter, "prepare_instance", None)
    if prepare is None:
        # Legacy custom adapters own their validation and normalization policy.
        return value, dump_python_value(adapter, value)
    try:
        return prepare(value)
    except ValidationError:
        raise
    except Exception as exc:
        _raise_payload_validation(exc)
