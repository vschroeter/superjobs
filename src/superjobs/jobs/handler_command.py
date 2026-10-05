"""Dependency-free CLI presentation metadata for handler catalog bindings."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType


def normalize_cli_option_name(field_name: str) -> str:
    return field_name.replace("_", "-")


@dataclass(frozen=True, slots=True)
class CLIField:
    """Override CLI presentation for one canonical request field name.

    ``option`` is the long option name without leading dashes (underscores become
    hyphens). When omitted, the canonical field name is normalized for CLI use.
    """

    option: str | None = None
    help: str | None = None


@dataclass(frozen=True, slots=True)
class Command:
    """CLI exposure metadata stored on a catalog binding (no Typer dependency)."""

    name: str
    aliases: tuple[str, ...] = ()
    positional_fields: tuple[str, ...] = ()
    field_options: Mapping[str, CLIField] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("Command name must be non-empty")
        if any(not alias for alias in self.aliases):
            raise ValueError("Command aliases must be non-empty strings")
        _validate_command_names(self.aliases, label="aliases")
        _validate_command_names(self.positional_fields, label="positional_fields")
        if not isinstance(self.field_options, Mapping):
            raise TypeError("field_options must be a mapping")
        for key, value in self.field_options.items():
            if not key:
                raise ValueError("field_options keys must be non-empty strings")
            if not isinstance(value, CLIField):
                raise TypeError("field_options values must be CLIField instances")
        object.__setattr__(
            self,
            "field_options",
            MappingProxyType(dict(self.field_options)),
        )


def normalize_command(cli: str | Command | None) -> Command | None:
    if cli is None:
        return None
    if isinstance(cli, Command):
        return freeze_command(cli)
    if isinstance(cli, str):
        if not cli:
            raise ValueError("cli command name must be non-empty when provided as a string")
        return Command(name=cli)
    raise TypeError("cli must be a str, Command, or None")


def freeze_command(command: Command) -> Command:
    """Return a catalog-safe command with copied immutable metadata containers."""
    field_options = {
        key: value for key, value in command.field_options.items()
    }
    return Command(
        name=command.name,
        aliases=tuple(command.aliases),
        positional_fields=tuple(command.positional_fields),
        field_options=MappingProxyType(field_options),
    )


def _validate_command_names(names: tuple[str, ...], *, label: str) -> None:
    seen: set[str] = set()
    for name in names:
        if not name:
            raise ValueError(f"Command {label} must be non-empty strings")
        if name in seen:
            raise ValueError(f"Command {label} must not contain duplicates")
        seen.add(name)
