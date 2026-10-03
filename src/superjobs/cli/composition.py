"""Validate Typer composition before modifying an application."""
from __future__ import annotations

from typing import TYPE_CHECKING
from typer.main import get_group

from superjobs.cli.validation import CLIRegistrationError

if TYPE_CHECKING:
    import typer


def assert_mount_names_available(app: typer.Typer, names: tuple[str, ...]) -> None:
    # Typer resolves inferred names and flattens unnamed groups when compiling.
    # Building the command tree does not invoke application callbacks.
    existing = get_group(app).commands
    for name in names:
        if name in existing:
            raise CLIRegistrationError(
                f"cannot mount JobCLI: application Typer already has command or group {name!r}"
            )
