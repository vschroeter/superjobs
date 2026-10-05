"""Catalog snapshot to JobCLI command registrations (issue #46)."""

from __future__ import annotations

from collections.abc import Mapping

from superjobs.cli.field_config import CLIField
from superjobs.cli.registration import CommandRegistration
from superjobs.cli.validation import (
    CLIRegistrationError,
    validate_command_input_configuration,
    validate_command_name,
)
from superjobs.jobs.handler_catalog import HandlerBinding, HandlerCatalogSnapshot
from superjobs.jobs.handler_command import Command
from superjobs.jobs.job import Job


def _field_options_from_command(command: Command) -> dict[str, CLIField]:
    return {
        key: CLIField(option=value.option, help=value.help)
        for key, value in command.field_options.items()
    }


def command_registration_from_binding(
    command_name: str,
    binding: HandlerBinding,
    *,
    positional_fields: tuple[str, ...] | None = None,
    field_options: Mapping[str, CLIField] | None = None,
    aliases: tuple[str, ...] = (),
) -> CommandRegistration:
    validate_command_name(command_name)
    base = binding.cli
    merged_positional = (
        positional_fields
        if positional_fields is not None
        else (base.positional_fields if base is not None else ())
    )
    merged_options: dict[str, CLIField] = {}
    if base is not None:
        merged_options.update(_field_options_from_command(base))
    if field_options is not None:
        merged_options.update(dict(field_options))
    input_plan = validate_command_input_configuration(
        binding.job,
        command_name,
        positional_fields=merged_positional,
        field_options=merged_options,
    )
    supports_local = binding.callback is not None or binding.provider is not None
    return CommandRegistration(
        command_name=command_name,
        job=binding.job,
        remote_only=not supports_local,
        handler=None,
        handler_factory=None,
        positional_fields=merged_positional,
        field_options=merged_options,
        input_plan=input_plan,
        catalog_binding=binding,
        command_aliases=aliases,
    )


def auto_expose_catalog_bindings(
    snapshot: HandlerCatalogSnapshot,
    commands: dict[str, CommandRegistration],
) -> None:
    for binding in snapshot:
        if binding.cli is None:
            continue
        command = binding.cli
        names = (command.name, *command.aliases)
        for index, name in enumerate(names):
            if name in commands:
                raise CLIRegistrationError(f"duplicate command_name {name!r}")
            registration = command_registration_from_binding(
                name,
                binding,
                aliases=() if index == 0 else (),
            )
            commands[name] = registration
