"""Registration-time validation for JobCLI command names and option collisions."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

from superjobs.cli.constants import RESERVED_CLI_OPTION_NAMES
from superjobs.cli.field_config import CLIField
from superjobs.cli.schema_plan import CommandInputPlan, ScalarKind, build_command_input_plan
from superjobs.jobs.handler_binding import validate_handler_job_association, validate_handler_signature
from superjobs.jobs.job import Job

_COMMAND_NAME_RE = re.compile(r"^[a-z][a-z0-9-]*$")
_RESERVED_COMMAND_NAMES = frozenset({"run", "submit", "help"})


class CLIRegistrationError(ValueError):
    """Raised when a JobCLI registration is invalid."""


def validate_command_name(command_name: str) -> None:
    if not command_name:
        raise CLIRegistrationError("command_name must not be empty")
    if command_name in _RESERVED_COMMAND_NAMES:
        raise CLIRegistrationError(
            f"command_name {command_name!r} is reserved; choose another name",
        )
    if not _COMMAND_NAME_RE.fullmatch(command_name):
        raise CLIRegistrationError(
            "command_name must use lowercase letters, digits, and hyphens "
            f"(got {command_name!r})",
        )


def _validate_option_token(option_name: str, command_name: str, field_name: str) -> None:
    if not option_name:
        raise CLIRegistrationError(
            f"command {command_name!r} field {field_name!r} has empty CLI option name",
        )
    if not re.fullmatch(r"[a-z][a-z0-9-]*", option_name):
        raise CLIRegistrationError(
            f"command {command_name!r} field {field_name!r} option {option_name!r} "
            "must use lowercase letters, digits, and hyphens",
        )


def _validate_plan_collisions(
    plan: CommandInputPlan,
    command_name: str,
) -> None:
    if not plan.field_mode_available:
        return

    effective: dict[str, str] = {}
    for field in plan.fields:
        _validate_option_token(field.option_name, command_name, field.canonical_name)
        if field.is_positional:
            continue
        if field.option_name in effective and effective[field.option_name] != field.canonical_name:
            raise CLIRegistrationError(
                f"command {command_name!r} request fields {effective[field.option_name]!r} and "
                f"{field.canonical_name!r} have duplicate normalized CLI option --{field.option_name}",
            )
        effective[field.option_name] = field.canonical_name

    positive_names = set(effective)
    collisions = sorted(positive_names & RESERVED_CLI_OPTION_NAMES)
    if collisions:
        joined = ", ".join(f"--{name}" for name in collisions)
        raise CLIRegistrationError(
            f"command {command_name!r} request fields collide with reserved CLI options: "
            f"{joined}",
        )

    for field in plan.fields:
        if field.is_positional or field.kind is not ScalarKind.BOOLEAN:
            continue
        negative = f"no-{field.option_name}"
        if negative in positive_names:
            raise CLIRegistrationError(
                f"command {command_name!r} boolean negative name --{negative} "
                f"collides with field option --{negative}",
            )


def validate_command_input_configuration(
    job: Job[Any, Any, Any],
    command_name: str,
    *,
    positional_fields: tuple[str, ...],
    field_options: Mapping[str, CLIField],
) -> CommandInputPlan:
    if not isinstance(positional_fields, tuple):
        raise CLIRegistrationError("positional_fields must be a tuple of field names")
    if field_options is not None and not isinstance(field_options, Mapping):
        raise CLIRegistrationError("field_options must be a mapping of field names to CLIField")

    if not all(isinstance(name, str) for name in positional_fields):
        raise CLIRegistrationError("positional_fields must contain field names as strings")
    for name, config in field_options.items():
        if not isinstance(name, str) or not isinstance(config, CLIField):
            raise CLIRegistrationError("field_options must map string field names to CLIField")
        if config.option is not None and not isinstance(config.option, str):
            raise CLIRegistrationError("CLIField.option must be a string")
        if config.help is not None and not isinstance(config.help, str):
            raise CLIRegistrationError("CLIField.help must be a string")

    try:
        plan = build_command_input_plan(
            job,
            positional_fields=positional_fields,
            field_options=field_options,
        )
    except (ValueError, TypeError, AttributeError) as exc:
        raise CLIRegistrationError(str(exc)) from exc

    _validate_plan_collisions(plan, command_name)
    return plan


def validate_local_handler(job: Job[Any, Any, Any], handler: Any) -> None:
    validate_handler_job_association(job, handler)
    validate_handler_signature(job, handler)
