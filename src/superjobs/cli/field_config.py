"""Public field customization for JobCLI request options (issue #39)."""

from __future__ import annotations

from superjobs.jobs.handler_command import CLIField, normalize_cli_option_name

normalize_option_name = normalize_cli_option_name

__all__ = ["CLIField", "normalize_option_name"]
