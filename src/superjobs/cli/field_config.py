"""Public field customization for JobCLI request options (issue #39)."""

from __future__ import annotations

from dataclasses import dataclass


def normalize_option_name(field_name: str) -> str:
    return field_name.replace("_", "-")


@dataclass(frozen=True, slots=True)
class CLIField:
    """Override CLI presentation for one canonical request field name.

    ``option`` is the long option name without leading dashes (underscores become
    hyphens). When omitted, the canonical field name is normalized for CLI use.
    """

    option: str | None = None
    help: str | None = None
