"""Build Typer command callbacks with dynamic request parameters (issue #39)."""

from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Any

import typer

from superjobs.cli.constants import (
    EXIT_RUNTIME_FAILURE,
    EXIT_USAGE,
    REMOTE_ONLY_RUN_MESSAGE,
    UNAVAILABLE_EXECUTION_MESSAGE,
)
from superjobs.cli.input_prepare import internal_param_name, prepare_command_input
from superjobs.cli.registration import CommandRegistration
from superjobs.cli.schema_plan import CommandInputPlan, RequestFieldPlan, ScalarKind
from superjobs.cli.strict_json import CLIInputError


def _help_for_field(field: RequestFieldPlan) -> str:
    parts: list[str] = []
    if field.description:
        parts.append(field.description)
    if field.required_in_field_mode:
        parts.append("required in field mode")
    if field.default_repr is not None:
        parts.append(f"default: {field.default_repr}")
    if field.enum_values:
        parts.append(f"choices: {', '.join(str(value) for value in field.enum_values)}")
    return "; ".join(parts) if parts else ""


def _annotation_for_field(field: RequestFieldPlan) -> Any:
    if field.kind == ScalarKind.BOOLEAN:
        return bool | None
    if field.kind == ScalarKind.INTEGER:
        return int | None
    if field.kind == ScalarKind.FLOAT:
        return float | None
    return str | None


def build_job_command_callback(
    registration: CommandRegistration,
    plan: CommandInputPlan,
    *,
    mode: str,
) -> Callable[..., None]:
    annotations: dict[str, Any] = {"ctx": typer.Context, "return": None}
    kwdefaults: dict[str, Any] = {}

    if plan.has_request_payload:
        annotations["json_text"] = str | None
        kwdefaults["json_text"] = typer.Option(
            None,
            "--json",
            help="Whole request as inline JSON text.",
            metavar="TEXT",
        )
        annotations["input_path"] = str | None
        kwdefaults["input_path"] = typer.Option(
            None,
            "--input",
            help="Whole request JSON from a UTF-8 file path or '-' for stdin.",
            metavar="PATH",
        )

    if plan.field_mode_available:
        for field in plan.fields:
            internal = internal_param_name(field)
            help_text = _help_for_field(field)
            annotations[internal] = _annotation_for_field(field)
            if field.is_positional:
                kwdefaults[internal] = typer.Argument(
                    None,
                    metavar=field.option_name.upper(),
                    help=help_text or None,
                )
            elif field.kind == ScalarKind.BOOLEAN:
                kwdefaults[internal] = typer.Option(
                    None,
                    f"--{field.option_name}/--no-{field.option_name}",
                    help=help_text or None,
                    show_default=False,
                )
            else:
                kwdefaults[internal] = typer.Option(
                    None,
                    f"--{field.option_name}",
                    help=help_text or None,
                    show_default=field.default_repr is not None,
                )

    def callback(**kwargs: Any) -> None:
        ctx = kwargs.pop("ctx")
        if mode == "run" and registration.remote_only:
            typer.echo(REMOTE_ONLY_RUN_MESSAGE, err=True)
            raise typer.Exit(code=EXIT_USAGE)

        try:
            prepare_command_input(plan, ctx.params, ctx)
        except CLIInputError as exc:
            typer.echo(f"Error: {exc}", err=True)
            raise typer.Exit(code=EXIT_USAGE) from exc
        typer.echo(UNAVAILABLE_EXECUTION_MESSAGE, err=True)
        raise typer.Exit(code=EXIT_RUNTIME_FAILURE)

    callback.__name__ = f"{mode}_{registration.command_name.replace('-', '_')}"
    callback.__annotations__ = annotations
    callback.__kwdefaults__ = kwdefaults
    callback.__signature__ = inspect.Signature(
        parameters=[
            inspect.Parameter(
                "ctx",
                inspect.Parameter.KEYWORD_ONLY,
                annotation=typer.Context,
            ),
            *[
                inspect.Parameter(
                    name,
                    inspect.Parameter.KEYWORD_ONLY,
                    default=default,
                    annotation=annotations[name],
                )
                for name, default in kwdefaults.items()
            ],
        ],
    )
    return callback


def command_help_text(
    registration: CommandRegistration,
    plan: CommandInputPlan,
    *,
    mode: str,
) -> str:
    parts = [f"Job {registration.job.canonical_name} ({mode}; execution unavailable)."]
    if plan.field_mode_unavailable_reason:
        parts.append(plan.field_mode_unavailable_reason)
    return " ".join(parts)
