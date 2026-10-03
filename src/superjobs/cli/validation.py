"""Registration-time validation for JobCLI command names and option collisions."""

from __future__ import annotations

import re
from typing import Any

from superjobs.cli.constants import RESERVED_CLI_OPTION_NAMES
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


def normalize_option_name(field_name: str) -> str:
    return field_name.replace("_", "-")


def _resolve_schema_node(schema: Any, root: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(schema, dict):
        return None
    ref = schema.get("$ref")
    if isinstance(ref, str) and ref.startswith("#/"):
        node: Any = root
        for segment in ref.removeprefix("#/").split("/"):
            if not isinstance(node, dict):
                return None
            node = node.get(segment.replace("~1", "/").replace("~0", "~"))
        return node if isinstance(node, dict) else None
    return schema


def _normalized_field_sources(properties: dict[str, Any]) -> dict[str, list[str]]:
    sources: dict[str, list[str]] = {}
    for key in properties:
        normalized = normalize_option_name(str(key))
        sources.setdefault(normalized, []).append(str(key))
    return sources


def validate_reserved_option_collisions(job: Job[Any, Any, Any], command_name: str) -> None:
    if job.request_type is None:
        return
    codec = job.request_codec
    if codec is None:
        return
    raw_schema = codec.schema()
    if not isinstance(raw_schema, dict):
        return
    object_schema = _resolve_schema_node(raw_schema, raw_schema)
    if object_schema is None:
        return
    properties = object_schema.get("properties")
    if not isinstance(properties, dict):
        return

    normalized_sources = _normalized_field_sources(properties)
    duplicate_norms = sorted(
        name for name, keys in normalized_sources.items() if len(keys) > 1
    )
    if duplicate_norms:
        details = ", ".join(
            f"{norm!r} from {normalized_sources[norm]!r}" for norm in duplicate_norms
        )
        raise CLIRegistrationError(
            f"command {command_name!r} request schema has duplicate normalized "
            f"CLI option names: {details}",
        )

    declared = frozenset(normalized_sources)
    collisions = sorted(declared & RESERVED_CLI_OPTION_NAMES)
    if collisions:
        joined = ", ".join(f"--{name}" for name in collisions)
        raise CLIRegistrationError(
            f"command {command_name!r} request fields collide with reserved CLI options: "
            f"{joined}",
        )


def validate_local_handler(job: Job[Any, Any, Any], handler: Any) -> None:
    validate_handler_job_association(job, handler)
    validate_handler_signature(job, handler)
