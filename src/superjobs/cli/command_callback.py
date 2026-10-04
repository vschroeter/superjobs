"""Build Typer command callbacks with dynamic request parameters (issue #39)."""

from __future__ import annotations

import inspect
import math
from collections.abc import Callable
from typing import Any

import typer

from superjobs.cli.runtime_factory import LocalRuntimeFactory, RemoteRuntimeFactory
from superjobs.cli.constants import (
    DEFAULT_CLI_WAIT_TIMEOUT_SECONDS,
    EXIT_RUNTIME_FAILURE,
    EXIT_USAGE,
    REMOTE_ONLY_RUN_MESSAGE,
)
from superjobs.cli.local_run import run_local_command
from superjobs.cli.remote_submit import run_remote_command
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
    local_runtime_factory: LocalRuntimeFactory | None = None,
    remote_runtime_factory: RemoteRuntimeFactory | None = None,
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

    if mode == "submit":
        annotations["wait"] = bool
        kwdefaults["wait"] = typer.Option(
            False,
            "--wait",
            help="Wait for the authoritative final result on stdout.",
        )
        annotations["wait_timeout"] = float | None
        kwdefaults["wait_timeout"] = typer.Option(
            None,
            "--wait-timeout",
            help=f"Client wait bound with --wait (default {DEFAULT_CLI_WAIT_TIMEOUT_SECONDS:g}s).",
        )

    def callback(**kwargs: Any) -> None:
        ctx = kwargs.pop("ctx")
        if mode == "run" and registration.remote_only:
            typer.echo(REMOTE_ONLY_RUN_MESSAGE, err=True)
            raise typer.Exit(code=EXIT_USAGE)

        try:
            prepared = prepare_command_input(plan, ctx.params, ctx)
        except CLIInputError as exc:
            typer.echo(f"Error: {exc}", err=True)
            raise typer.Exit(code=EXIT_USAGE) from exc

        if mode == "run":
            if local_runtime_factory is None:
                raise RuntimeError("run command callback missing local_runtime_factory snapshot")
            result = run_local_command(registration, prepared, local_runtime_factory)
            if result.stdout is not None:
                typer.echo(result.stdout)
            raise typer.Exit(code=result.exit_code)

        wait = bool(kwargs.pop("wait", False))
        wait_timeout = kwargs.pop("wait_timeout", None)
        if wait_timeout is not None and not wait:
            typer.echo("Error: --wait-timeout requires --wait", err=True)
            raise typer.Exit(code=EXIT_USAGE)
        if wait and wait_timeout is not None and (
            not math.isfinite(wait_timeout) or wait_timeout <= 0
        ):
            typer.echo(
                "Error: --wait-timeout must be a finite positive number",
                err=True,
            )
            raise typer.Exit(code=EXIT_USAGE)
        result = run_remote_command(
            registration,
            prepared,
            remote_runtime_factory,
            wait=wait,
            wait_timeout=wait_timeout,
        )
        if result.stdout is not None:
            typer.echo(result.stdout)
        raise typer.Exit(code=result.exit_code)

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
    if mode == "run":
        execution_note = (
            "local in-process execution"
            if registration.supports_local_run
            else "remote-only; cannot run locally"
        )
    else:
        execution_note = "remote NATS submission through remote_runtime_factory"
    parts = [f"Job {registration.job.canonical_name} ({mode}; {execution_note})."]
    if plan.field_mode_unavailable_reason:
        parts.append(plan.field_mode_unavailable_reason)
    return " ".join(parts)
