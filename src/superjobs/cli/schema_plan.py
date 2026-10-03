"""Derive CLI request field plans from adapter JSON Schema (issue #39)."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from typing import Any

from superjobs.cli.field_config import CLIField, normalize_option_name
from superjobs.jobs.job import Job
from superjobs.payload.codec.payloadcodec import PayloadCodec


class ScalarKind(str, Enum):
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"


@dataclass(frozen=True, slots=True)
class RequestFieldPlan:
    canonical_name: str
    option_name: str
    kind: ScalarKind
    required_in_field_mode: bool
    default_repr: str | None
    description: str | None
    enum_values: tuple[str | int | float | bool, ...] | None
    is_positional: bool


@dataclass(frozen=True, slots=True)
class CommandInputPlan:
    has_request_payload: bool
    fields: tuple[RequestFieldPlan, ...]
    field_mode_available: bool
    field_mode_unavailable_reason: str | None
    nullable_root: bool
    request_codec: PayloadCodec[Any] | None


def _resolve_schema_node(schema: Any, root: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(schema, dict):
        return None
    resolved = dict(schema)
    seen: set[str] = set()
    while "$ref" in resolved:
        ref = resolved["$ref"]
        if not isinstance(ref, str) or not ref.startswith("#/") or ref in seen:
            return None
        seen.add(ref)
        node: Any = root
        for segment in ref.removeprefix("#/").split("/"):
            if not isinstance(node, dict):
                return None
            node = node.get(segment.replace("~1", "/").replace("~0", "~"))
        if not isinstance(node, dict):
            return None
        # JSON Schema permits metadata beside $ref: don't lose field defaults or
        # descriptions when resolving an enum/type definition.
        resolved = {**node, **{key: value for key, value in resolved.items() if key != "$ref"}}
    return resolved


def _unwrap_nullable(schema: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    resolved = _resolve_schema_node(schema, schema) or schema
    if resolved.get("type") == "null":
        return resolved, True
    for key in ("anyOf", "oneOf"):
        variants = resolved.get(key)
        if not isinstance(variants, list):
            continue
        non_null = [
            item for item in variants if isinstance(item, dict) and item.get("type") != "null"
        ]
        if len(non_null) == 1 and len(variants) >= 2:
            inner = _resolve_schema_node(non_null[0], schema) or non_null[0]
            return inner, True
    return resolved, False


def _is_read_only(prop: dict[str, Any]) -> bool:
    return prop.get("readOnly") is True


def _classify_property(
    prop: dict[str, Any],
    *,
    root: dict[str, Any],
) -> tuple[ScalarKind | None, tuple[str | int | float | bool, ...] | None, str | None]:
    resolved = _resolve_schema_node(prop, root) or prop
    inner, nullable = _unwrap_nullable(resolved)
    if nullable:
        return (
            None,
            None,
            "nullable fields are only accepted via --json/--input",
        )
    if inner.get("type") == "array" or "items" in inner:
        return None, None, "list fields are only accepted via --json/--input"
    if inner.get("type") == "object" or "properties" in inner:
        return None, None, "nested fields are only accepted via --json/--input"
    for union_key in ("anyOf", "oneOf", "allOf"):
        if union_key in inner:
            return None, None, "union fields are only accepted via --json/--input"

    enum_values = inner.get("enum")
    if isinstance(enum_values, list) and enum_values:
        if all(isinstance(v, str) for v in enum_values):
            return ScalarKind.STRING, tuple(enum_values), None
        if all(isinstance(v, int) and not isinstance(v, bool) for v in enum_values):
            return ScalarKind.INTEGER, tuple(enum_values), None
        if all(isinstance(v, bool) for v in enum_values):
            return ScalarKind.BOOLEAN, tuple(enum_values), None
        if all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in enum_values):
            return ScalarKind.FLOAT, tuple(enum_values), None
        return None, None, "enum values are not homogeneous scalars; use --json/--input"

    schema_type = inner.get("type")
    if schema_type == "string":
        return ScalarKind.STRING, None, None
    if schema_type == "integer":
        return ScalarKind.INTEGER, None, None
    if schema_type == "number":
        return ScalarKind.FLOAT, None, None
    if schema_type == "boolean":
        return ScalarKind.BOOLEAN, None, None

    return None, None, "unsupported schema for CLI field options; use --json/--input"


def _default_repr(prop: dict[str, Any]) -> str | None:
    if "default" not in prop:
        return None
    default = prop["default"]
    if isinstance(default, bool):
        return "true" if default else "false"
    return repr(default)


def _reject_unknown_configuration(
    *,
    property_names: frozenset[str],
    positional_fields: tuple[str, ...],
    field_options: Mapping[str, CLIField],
) -> None:
    unknown_positional = sorted(name for name in positional_fields if name not in property_names)
    if unknown_positional:
        joined = ", ".join(unknown_positional)
        raise ValueError(f"positional_fields references unknown request fields: {joined}")
    unknown_overrides = sorted(set(field_options) - property_names)
    if unknown_overrides:
        joined = ", ".join(unknown_overrides)
        raise ValueError(f"field_options references unknown request fields: {joined}")
    if len(positional_fields) != len(set(positional_fields)):
        raise ValueError("positional_fields must not repeat field names")


def build_command_input_plan(
    job: Job[Any, Any, Any],
    *,
    positional_fields: tuple[str, ...] = (),
    field_options: Mapping[str, CLIField] | None = None,
) -> CommandInputPlan:
    overrides = field_options or {}

    if job.request_type is None:
        if positional_fields or overrides:
            raise ValueError(
                "positional_fields and field_options are not allowed for no-request Jobs",
            )
        return CommandInputPlan(
            has_request_payload=False,
            fields=(),
            field_mode_available=False,
            field_mode_unavailable_reason=None,
            nullable_root=False,
            request_codec=None,
        )

    codec = job.request_codec
    if codec is None:
        if positional_fields or overrides:
            raise ValueError(
                "positional_fields and field_options require a schema-backed request",
            )
        return CommandInputPlan(
            has_request_payload=True,
            fields=(),
            field_mode_available=False,
            field_mode_unavailable_reason=(
                "request uses a custom adapter without schema; use --json/--input"
            ),
            nullable_root=False,
            request_codec=None,
        )

    raw_schema = codec.schema()
    if not isinstance(raw_schema, dict):
        if positional_fields or overrides:
            raise ValueError(
                "positional_fields and field_options require a schema-backed request",
            )
        return CommandInputPlan(
            has_request_payload=True,
            fields=(),
            field_mode_available=False,
            field_mode_unavailable_reason=(
                "request schema is unavailable; use --json/--input"
            ),
            nullable_root=False,
            request_codec=codec,
        )

    root_schema = _resolve_schema_node(raw_schema, raw_schema)
    if root_schema is None:
        if positional_fields or overrides:
            raise ValueError(
                "positional_fields and field_options require a flat object request schema",
            )
        return CommandInputPlan(
            has_request_payload=True,
            fields=(),
            field_mode_available=False,
            field_mode_unavailable_reason="request schema is not an object; use --json/--input",
            nullable_root=False,
            request_codec=codec,
        )

    root_schema, nullable_root = _unwrap_nullable(root_schema)
    schema_type = root_schema.get("type")

    if schema_type != "object" or nullable_root or any(
        key in root_schema for key in ("anyOf", "oneOf", "allOf")
    ):
        if positional_fields or overrides:
            raise ValueError(
                "positional_fields and field_options require a flat object request schema",
            )
        return CommandInputPlan(
            has_request_payload=True,
            fields=(),
            field_mode_available=False,
            field_mode_unavailable_reason=(
                "request is not a flat object; use --json/--input"
            ),
            nullable_root=nullable_root,
            request_codec=codec,
        )

    properties = root_schema.get("properties")
    if not isinstance(properties, dict):
        if positional_fields or overrides:
            raise ValueError("field configuration requires a flat object with declared properties")
        return CommandInputPlan(
            has_request_payload=True,
            fields=(),
            field_mode_available=False,
            field_mode_unavailable_reason="request has no declared properties; use --json/--input",
            nullable_root=False,
            request_codec=codec,
        )

    input_properties: dict[str, dict[str, Any]] = {}
    for name, prop_schema in properties.items():
        if not isinstance(prop_schema, dict):
            input_properties[name] = {}
            continue
        resolved = _resolve_schema_node(prop_schema, raw_schema) or prop_schema
        if _is_read_only(resolved):
            continue
        input_properties[name] = resolved

    _reject_unknown_configuration(
        property_names=frozenset(input_properties),
        positional_fields=positional_fields,
        field_options=overrides,
    )

    schema_required = frozenset(root_schema.get("required") or [])
    positional_set = frozenset(positional_fields)

    field_plans: list[RequestFieldPlan] = []
    unsupported_reasons: list[str] = []

    for canonical_name, prop_resolved in input_properties.items():
        kind, enum_values, skip_reason = _classify_property(
            prop_resolved,
            root=raw_schema,
        )
        override = overrides.get(canonical_name)
        if override is not None and override.option is not None:
            option_name = normalize_option_name(override.option)
        else:
            option_name = normalize_option_name(canonical_name)

        if override is not None and override.help is not None:
            description = override.help
        else:
            desc = prop_resolved.get("description")
            description = desc if isinstance(desc, str) else None

        is_positional = canonical_name in positional_set
        if is_positional and kind is None:
            raise ValueError(
                f"positional_fields includes {canonical_name!r} which does not support field mode",
            )

        if kind is None:
            unsupported_reasons.append(f"field {canonical_name!r}: {skip_reason}")

        required = canonical_name in schema_required and kind is not None
        field_plans.append(
            RequestFieldPlan(
                canonical_name=canonical_name,
                option_name=option_name,
                kind=kind or ScalarKind.STRING,
                required_in_field_mode=required,
                default_repr=_default_repr(prop_resolved),
                description=description,
                enum_values=enum_values,
                is_positional=is_positional,
            ),
        )

    field_mode_available = not unsupported_reasons
    unavailable: str | None = None
    if not field_mode_available:
        if unsupported_reasons:
            unavailable = "; ".join(dict.fromkeys(unsupported_reasons))
        elif not field_plans:
            unavailable = "request has no input properties; use --json/--input when needed"
        if positional_fields or overrides:
            raise ValueError(
                "positional_fields and field_options require a flat field-mode-capable request",
            )

    if positional_fields:
        by_name = {plan.canonical_name: plan for plan in field_plans}
        ordered = [by_name[name] for name in positional_fields]
        ordered.extend(
            plan for plan in field_plans if plan.canonical_name not in positional_set
        )
    else:
        ordered = field_plans

    return CommandInputPlan(
        has_request_payload=True,
        fields=tuple(ordered),
        field_mode_available=field_mode_available,
        field_mode_unavailable_reason=unavailable,
        nullable_root=nullable_root,
        request_codec=codec,
    )
