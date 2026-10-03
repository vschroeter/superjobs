"""Shared typed request preparation for CLI run/submit (issue #39)."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import typer
import math
from pydantic import ValidationError

from superjobs.cli.schema_plan import CommandInputPlan, RequestFieldPlan, ScalarKind
from superjobs.cli.strict_json import CLIInputError, parse_strict_json, read_utf8_text
from superjobs.payload import PayloadValidationError
from superjobs.payload.adapter.protocol import WireValue
from superjobs.payload.strict import load_wire_value

__all__ = ["CLIInputError", "PreparedCommandInput", "prepare_command_input", "internal_param_name"]


@dataclass(frozen=True, slots=True)
class PreparedCommandInput:
    """Typed request ready for future execution slices."""

    request: Any | None
    has_request: bool


def _field_supplied(ctx: typer.Context | None, param_name: str) -> bool:
    if ctx is None:
        return False
    try:
        source = ctx.get_parameter_source(param_name)
    except (KeyError, AttributeError):
        return False
    return source is not None and source.name in {"COMMANDLINE", "ENVIRONMENT", "PROMPT"}


def _format_validation_error(exc: ValidationError) -> str:
    parts: list[str] = []
    for error in exc.errors():
        loc = ".".join(str(item) for item in error.get("loc", ()))
        msg = error.get("msg", "invalid value")
        if loc:
            parts.append(f"{loc}: {msg}")
        else:
            parts.append(str(msg))
    return "; ".join(parts) if parts else str(exc)


def _load_request(adapter: Any, wire_tree: WireValue) -> Any:
    try:
        return load_wire_value(adapter, wire_tree)
    except ValidationError as exc:
        raise CLIInputError(_format_validation_error(exc)) from exc
    except PayloadValidationError as exc:
        raise CLIInputError(str(exc)) from exc
    except TypeError as exc:
        raise CLIInputError(str(exc)) from exc


def _convert_field_text(plan: RequestFieldPlan, raw: str) -> Any:
    if plan.kind == ScalarKind.STRING:
        return raw
    if plan.kind == ScalarKind.INTEGER:
        try:
            return int(raw, 10)
        except ValueError as exc:
            raise CLIInputError(
                f"field {plan.canonical_name!r}: expected integer, got {raw!r}",
            ) from exc
    if plan.kind == ScalarKind.FLOAT:
        try:
            value = float(raw)
        except ValueError as exc:
            raise CLIInputError(
                f"field {plan.canonical_name!r}: expected number, got {raw!r}",
            ) from exc
        if not math.isfinite(value):
            raise CLIInputError(f"field {plan.canonical_name!r}: non-finite number")
        return value
    if plan.kind == ScalarKind.BOOLEAN:
        lowered = raw.lower()
        if lowered in {"true", "1", "yes"}:
            return True
        if lowered in {"false", "0", "no"}:
            return False
        raise CLIInputError(
            f"field {plan.canonical_name!r}: expected boolean, got {raw!r}",
        )
    raise CLIInputError(f"field {plan.canonical_name!r}: unsupported CLI field type")


def _collect_field_values(
    plan: CommandInputPlan,
    params: Mapping[str, Any],
    ctx: typer.Context | None,
) -> dict[str, Any]:
    fields: dict[str, Any] = {}
    for field_plan in plan.fields:
        internal_name = _internal_param_name(field_plan)
        if not _field_supplied(ctx, internal_name):
            continue
        raw = params.get(internal_name)
        if raw is None:
            continue
        value = _convert_field_text(field_plan, str(raw))
        if field_plan.enum_values is not None and value not in field_plan.enum_values:
            choices = ", ".join(str(choice) for choice in field_plan.enum_values)
            raise CLIInputError(f"field {field_plan.canonical_name!r}: expected one of {choices}")
        fields[field_plan.canonical_name] = value

    missing = [
        field_plan.canonical_name
        for field_plan in plan.fields
        if field_plan.required_in_field_mode
        and field_plan.canonical_name not in fields
    ]
    if missing:
        joined = ", ".join(missing)
        raise CLIInputError(f"missing required request fields in field mode: {joined}")
    return fields


def _internal_param_name(field_plan: RequestFieldPlan) -> str:
    prefix = "arg_" if field_plan.is_positional else "opt_"
    # Canonical custom-adapter names need not be Python identifiers. Encoding also
    # keeps renamed foo_bar/foo-bar fields distinct in Typer's signature.
    return f"{prefix}{field_plan.canonical_name.encode('utf-8').hex()}"


def internal_param_name(field_plan: RequestFieldPlan) -> str:
    return _internal_param_name(field_plan)


def prepare_command_input(
    plan: CommandInputPlan,
    params: Mapping[str, Any],
    ctx: typer.Context | None,
) -> PreparedCommandInput:
    if not plan.has_request_payload:
        if params.get("json_text") is not None or params.get("input_path") is not None:
            raise CLIInputError("this command accepts no request payload")
        for field_plan in plan.fields:
            if _field_supplied(ctx, _internal_param_name(field_plan)):
                raise CLIInputError("this command accepts no request payload")
        return PreparedCommandInput(request=None, has_request=False)

    json_text = params.get("json_text")
    input_path = params.get("input_path")
    whole_sources = sum(1 for value in (json_text, input_path) if value is not None)
    if whole_sources > 1:
        raise CLIInputError("--json and --input are mutually exclusive")

    field_supplied = plan.field_mode_available and any(
        _field_supplied(ctx, _internal_param_name(field_plan))
        for field_plan in plan.fields
    )
    if whole_sources and field_supplied:
        raise CLIInputError(
            "request-field options cannot be combined with --json or --input",
        )

    codec = plan.request_codec
    if codec is None:
        if not whole_sources:
            raise CLIInputError(
                plan.field_mode_unavailable_reason or "use --json/--input",
            )
        adapter = None
    else:
        adapter = codec.adapter

    if whole_sources:
        if json_text is not None:
            text = json_text
        else:
            text = read_utf8_text(str(input_path))
        wire_tree: WireValue = parse_strict_json(text)
        if adapter is None:
            raise CLIInputError(
                plan.field_mode_unavailable_reason or "use --json/--input",
            )
        request = _load_request(adapter, wire_tree)
        return PreparedCommandInput(request=request, has_request=True)

    if field_supplied:
        if adapter is None:
            raise CLIInputError(
                plan.field_mode_unavailable_reason
                or "request fields are only accepted via --json/--input",
            )
        fields = _collect_field_values(plan, params, ctx)
        request = _load_request(adapter, fields)
        return PreparedCommandInput(request=request, has_request=True)

    if plan.nullable_root:
        raise CLIInputError(
            "request is nullable; provide --json null or --input with JSON null",
        )

    if plan.field_mode_available and any(
        field.required_in_field_mode for field in plan.fields
    ):
        raise CLIInputError(
            "missing request input; use --json/--input or required field options",
        )

    if plan.field_mode_available:
        if adapter is None:
            raise CLIInputError("missing request input; use --json or --input")
        request = _load_request(adapter, {})
        return PreparedCommandInput(request=request, has_request=True)

    raise CLIInputError(
        plan.field_mode_unavailable_reason or "missing request input; use --json or --input",
    )
