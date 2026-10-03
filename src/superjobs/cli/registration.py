"""Internal command registration records for JobCLI."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

from superjobs.cli.field_config import CLIField
from superjobs.cli.schema_plan import CommandInputPlan
from superjobs.jobs.job import Job


@dataclass(frozen=True)
class CommandRegistration:
    command_name: str
    job: Job[Any, Any, Any]
    remote_only: bool
    handler: Callable[..., Any] | None
    handler_factory: Callable[[], Callable[..., Any]] | None
    positional_fields: tuple[str, ...] = ()
    field_options: Mapping[str, CLIField] = field(default_factory=dict)
    input_plan: CommandInputPlan | None = None

    @property
    def supports_local_run(self) -> bool:
        return not self.remote_only and (
            self.handler is not None or self.handler_factory is not None
        )
