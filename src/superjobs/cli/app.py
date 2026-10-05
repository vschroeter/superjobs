"""Typed JobCLI builder and Typer command shell (issue #38)."""

from __future__ import annotations

import asyncio
import sys
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, Literal, ParamSpec, TypeVar, overload

from superjobs.cli.builtin_remote_runtime import builtin_remote_runtime
from superjobs.cli.catalog_cli import auto_expose_catalog_bindings, command_registration_from_binding
from superjobs.cli.command_callback import build_job_command_callback, command_help_text
from superjobs.cli.default_local_runtime import default_local_runtime_factory
from superjobs.cli.runtime_factory import LocalRuntimeFactory, RemoteRuntimeFactory
from superjobs.jobs.handler_catalog import HandlerCatalog, HandlerCatalogSnapshot
from superjobs.cli.composition import assert_mount_names_available
from superjobs.cli.field_config import CLIField
from superjobs.cli.constants import (
    ACTIVE_EVENT_LOOP_MESSAGE,
    EXIT_INTERRUPTED,
    EXIT_RUNTIME_FAILURE,
    EXIT_SUCCESS,
    EXIT_USAGE,
)
from superjobs.cli.registration import CommandRegistration
from superjobs.cli.schema_plan import build_command_input_plan
from superjobs.cli.validation import (
    CLIRegistrationError,
    validate_command_input_configuration,
    validate_command_name,
    validate_local_handler,
)
from superjobs.jobs.handler_binding import get_marked_job
from superjobs.jobs.job import Job, NoRequestJob, RequestJob
from superjobs.jobs.job_context import JobContext
from superjobs.superjobs import SuperJobs

try:
    import typer
except ModuleNotFoundError as error:
    if error.name != "typer":
        raise
    raise ImportError(
        "SuperJobs CLI requires the optional 'cli' extra. "
        "Install with: pip install 'superjobs[cli]'"
    ) from error

ReqT = TypeVar("ReqT")
FinalT = TypeVar("FinalT")
InterT = TypeVar("InterT")
ConstructorP = ParamSpec("ConstructorP")

_MOUNT_GROUP_NAMES = ("run", "submit")


@dataclass
class _SubmitTransportState:
    nats_url_override: str | None = None


@dataclass
class JobCLI:
    """Register contract Jobs as CLI commands backed by Typer.

    Application code owns runtime lifetime: supply ``local_runtime_factory`` for
    custom in-process ``run`` resources, or rely on the built-in isolated
    in-memory runtime when it is omitted. ``remote_runtime_factory`` is reserved
    for NATS ``submit``. Factories must not be invoked for help
    or when a command is not selected. Lazy ``handler_factory`` callables run
    only when a local ``run`` command is selected.

    When ``handlers`` is supplied, a snapshot of catalog bindings with CLI
    metadata is exposed automatically. ``expose()`` projects additional aliases
    without duplicating worker subscriptions.

    ``build_typer()`` and ``mount()`` snapshot the current registration table at
    call time. Later ``add()`` calls do not alter Typer objects already built or
    mounted.
    """

    handlers: HandlerCatalog | None = None
    nats_url: str | None = None
    local_runtime_factory: LocalRuntimeFactory | None = None
    remote_runtime_factory: RemoteRuntimeFactory | None = None
    _commands: dict[str, CommandRegistration] = field(
        default_factory=dict,
        init=False,
        repr=False,
    )
    _catalog_snapshot: HandlerCatalogSnapshot | None = field(
        default=None,
        init=False,
        repr=False,
    )

    def __post_init__(self) -> None:
        if self.remote_runtime_factory is not None and self.nats_url is not None:
            raise CLIRegistrationError(
                "remote_runtime_factory cannot be combined with constructor nats_url",
            )
        if self.handlers is not None:
            self._catalog_snapshot = self.handlers.snapshot()
            auto_expose_catalog_bindings(self._catalog_snapshot, self._commands)

    def expose(
        self,
        command_name: str,
        job: Job[Any, Any, Any],
        *,
        aliases: tuple[str, ...] = (),
        positional_fields: tuple[str, ...] = (),
        field_options: Mapping[str, CLIField] | None = None,
    ) -> None:
        """Expose an existing catalog binding under ``command_name`` and optional aliases."""
        if self._catalog_snapshot is None:
            raise CLIRegistrationError("expose() requires JobCLI(handlers=...)")
        binding = self._catalog_snapshot.get(job)
        if binding is None:
            raise CLIRegistrationError(
                f"no catalog binding for {job.canonical_name!r} in the CLI snapshot",
            )
        names = (command_name, *aliases)
        for name in names:
            if name in self._commands:
                raise CLIRegistrationError(f"duplicate command_name {name!r}")
            self._commands[name] = command_registration_from_binding(
                name,
                binding,
                positional_fields=positional_fields or None,
                field_options=field_options,
            )

    @overload
    def add(
        self,
        command_name: str,
        job: NoRequestJob[FinalT, InterT],
        *,
        handler: Callable[[JobContext[InterT]], Awaitable[FinalT]],
        handler_factory: None = None,
        remote_only: Literal[False] = False,
        positional_fields: tuple[str, ...] = (),
        field_options: Mapping[str, CLIField] | None = None,
    ) -> None: ...

    @overload
    def add(
        self,
        command_name: str,
        job: NoRequestJob[FinalT, InterT],
        *,
        handler: Callable[[JobContext[InterT]], FinalT],
        handler_factory: None = None,
        remote_only: Literal[False] = False,
        positional_fields: tuple[str, ...] = (),
        field_options: Mapping[str, CLIField] | None = None,
    ) -> None: ...

    @overload
    def add(
        self,
        command_name: str,
        job: RequestJob[ReqT, FinalT, InterT, ConstructorP],
        *,
        handler: Callable[[ReqT, JobContext[InterT]], Awaitable[FinalT]],
        handler_factory: None = None,
        remote_only: Literal[False] = False,
        positional_fields: tuple[str, ...] = (),
        field_options: Mapping[str, CLIField] | None = None,
    ) -> None: ...

    @overload
    def add(
        self,
        command_name: str,
        job: RequestJob[ReqT, FinalT, InterT, ConstructorP],
        *,
        handler: Callable[[ReqT, JobContext[InterT]], FinalT],
        handler_factory: None = None,
        remote_only: Literal[False] = False,
        positional_fields: tuple[str, ...] = (),
        field_options: Mapping[str, CLIField] | None = None,
    ) -> None: ...

    @overload
    def add(
        self,
        command_name: str,
        job: NoRequestJob[FinalT, InterT],
        *,
        handler: None = None,
        handler_factory: Callable[
            [],
            Callable[[JobContext[InterT]], Awaitable[FinalT]],
        ],
        remote_only: Literal[False] = False,
        positional_fields: tuple[str, ...] = (),
        field_options: Mapping[str, CLIField] | None = None,
    ) -> None: ...

    @overload
    def add(
        self,
        command_name: str,
        job: NoRequestJob[FinalT, InterT],
        *,
        handler: None = None,
        handler_factory: Callable[[], Callable[[JobContext[InterT]], FinalT]],
        remote_only: Literal[False] = False,
        positional_fields: tuple[str, ...] = (),
        field_options: Mapping[str, CLIField] | None = None,
    ) -> None: ...

    @overload
    def add(
        self,
        command_name: str,
        job: RequestJob[ReqT, FinalT, InterT, ConstructorP],
        *,
        handler: None = None,
        handler_factory: Callable[
            [],
            Callable[[ReqT, JobContext[InterT]], Awaitable[FinalT]],
        ],
        remote_only: Literal[False] = False,
        positional_fields: tuple[str, ...] = (),
        field_options: Mapping[str, CLIField] | None = None,
    ) -> None: ...

    @overload
    def add(
        self,
        command_name: str,
        job: RequestJob[ReqT, FinalT, InterT, ConstructorP],
        *,
        handler: None = None,
        handler_factory: Callable[
            [],
            Callable[[ReqT, JobContext[InterT]], FinalT],
        ],
        remote_only: Literal[False] = False,
        positional_fields: tuple[str, ...] = (),
        field_options: Mapping[str, CLIField] | None = None,
    ) -> None: ...

    @overload
    def add(
        self,
        command_name: str,
        job: Job[ReqT, FinalT, InterT],
        *,
        handler: None = None,
        handler_factory: None = None,
        remote_only: Literal[True],
        positional_fields: tuple[str, ...] = (),
        field_options: Mapping[str, CLIField] | None = None,
    ) -> None: ...

    def add(
        self,
        command_name: str,
        job: Job[Any, Any, Any],
        *,
        handler: Callable[..., Any] | None = None,
        handler_factory: Callable[[], Callable[..., Any]] | None = None,
        remote_only: bool = False,
        positional_fields: tuple[str, ...] = (),
        field_options: Mapping[str, CLIField] | None = None,
    ) -> None:
        validate_command_name(command_name)
        if command_name in self._commands:
            raise CLIRegistrationError(
                f"duplicate command_name {command_name!r}",
            )
        if remote_only and (handler is not None or handler_factory is not None):
            raise CLIRegistrationError(
                "remote_only commands cannot register a local handler or handler_factory",
            )
        if not remote_only and handler is None and handler_factory is None:
            raise CLIRegistrationError(
                "local commands require handler or handler_factory "
                "(or set remote_only=True)",
            )
        if handler is not None and handler_factory is not None:
            raise CLIRegistrationError(
                "pass only one of handler or handler_factory",
            )
        if handler is not None:
            if not callable(handler):
                raise CLIRegistrationError("handler must be callable")
            marked = get_marked_job(handler)
            if marked is not None:
                if marked is not job:
                    raise CLIRegistrationError(
                        f"handler is marked for {marked}, cannot register for {job}",
                    )
            validate_local_handler(job, handler)
        if handler_factory is not None and not callable(handler_factory):
            raise CLIRegistrationError("handler_factory must be callable")
        if field_options is not None and not isinstance(field_options, Mapping):
            raise CLIRegistrationError("field_options must be a mapping of field names to CLIField")
        options = dict(field_options) if field_options is not None else {}
        input_plan = validate_command_input_configuration(
            job,
            command_name,
            positional_fields=positional_fields,
            field_options=options,
        )
        self._commands[command_name] = CommandRegistration(
            command_name=command_name,
            job=job,
            remote_only=remote_only,
            handler=handler if callable(handler) else None,
            handler_factory=handler_factory if callable(handler_factory) else None,
            positional_fields=positional_fields,
            field_options=options,
            input_plan=input_plan,
        )

    def registrations(self) -> tuple[CommandRegistration, ...]:
        return tuple(self._commands.values())

    def _effective_local_runtime_factory(self) -> LocalRuntimeFactory:
        if self.local_runtime_factory is not None:
            return self.local_runtime_factory
        return default_local_runtime_factory

    def _builtin_remote_runtime_factory(
        self,
        *,
        constructor_url: str | None,
        submit_state: _SubmitTransportState,
    ) -> RemoteRuntimeFactory:
        @asynccontextmanager
        async def _builtin() -> AsyncIterator[SuperJobs]:
            async with builtin_remote_runtime(
                constructor_url=constructor_url,
                cli_override=submit_state.nats_url_override,
            ) as runtime:
                yield runtime

        return _builtin

    def _build_run_submit_groups(self) -> tuple[typer.Typer, typer.Typer]:
        run_group = typer.Typer(help="Run a Job locally in-process.")
        submit_group = typer.Typer(help="Submit a Job for remote execution.")
        local_factory = self._effective_local_runtime_factory()
        submit_state = _SubmitTransportState()
        captured_remote_factory = self.remote_runtime_factory
        captured_constructor_url = self.nats_url
        if captured_remote_factory is not None:
            remote_factory = captured_remote_factory
        else:
            remote_factory = self._builtin_remote_runtime_factory(
                constructor_url=captured_constructor_url,
                submit_state=submit_state,
            )

        @submit_group.callback()
        def _submit_group_options(
            ctx: typer.Context,
            nats_url: str | None = typer.Option(
                None,
                "--nats-url",
                help="NATS broker URL for remote submission (overrides env and constructor).",
            ),
        ) -> None:
            if captured_remote_factory is not None and nats_url is not None:
                raise typer.BadParameter(
                    "--nats-url cannot be used with remote_runtime_factory",
                )
            submit_state.nats_url_override = nats_url

        for registration in self._commands.values():
            self._register_command(
                run_group,
                registration,
                mode="run",
                local_runtime_factory=local_factory,
            )
            self._register_command(
                submit_group,
                registration,
                mode="submit",
                remote_runtime_factory=remote_factory,
            )
        return run_group, submit_group

    def _register_command(
        self,
        group: typer.Typer,
        registration: CommandRegistration,
        *,
        mode: str,
        local_runtime_factory: LocalRuntimeFactory | None = None,
        remote_runtime_factory: RemoteRuntimeFactory | None = None,
    ) -> None:
        plan = registration.input_plan
        if plan is None:
            plan = build_command_input_plan(
                registration.job,
                positional_fields=registration.positional_fields,
                field_options=registration.field_options,
            )
        callback = build_job_command_callback(
            registration,
            plan,
            mode=mode,
            local_runtime_factory=local_runtime_factory,
            remote_runtime_factory=remote_runtime_factory,
        )
        group.command(
            name=registration.command_name,
            help=command_help_text(registration, plan, mode=mode),
            context_settings={"max_content_width": 120},
        )(callback)

    def build_typer(self) -> typer.Typer:
        root = typer.Typer(
            help="SuperJobs contract commands (local run and remote submit).",
            no_args_is_help=True,
            pretty_exceptions_enable=False,
        )
        run_group, submit_group = self._build_run_submit_groups()
        root.add_typer(run_group, name="run")
        root.add_typer(submit_group, name="submit")
        return root

    def mount(self, app: typer.Typer) -> None:
        """Attach ``run`` and ``submit`` command groups to an application-owned Typer app."""
        assert_mount_names_available(app, _MOUNT_GROUP_NAMES)
        run_group, submit_group = self._build_run_submit_groups()
        app.add_typer(run_group, name="run")
        app.add_typer(submit_group, name="submit")

    def main(self, argv: list[str] | None = None) -> int:
        """Synchronous process entry point for ``run`` / ``submit`` commands."""
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            pass
        else:
            print(ACTIVE_EVENT_LOOP_MESSAGE, file=sys.stderr)
            return EXIT_USAGE
        try:
            return self._invoke_typer(argv)
        except (KeyboardInterrupt, typer.Abort):
            return EXIT_INTERRUPTED

    def _invoke_typer(self, argv: list[str] | None) -> int:
        app = self.build_typer()
        args = argv if argv is not None else sys.argv[1:]
        try:
            exit_code = app(args=args, standalone_mode=False, prog_name="jobcli")
        except typer.TyperException as error:
            typer.echo(f"Error: {error.format_message()}", err=True)
            return error.exit_code
        except (KeyboardInterrupt, typer.Abort):
            return EXIT_INTERRUPTED
        return EXIT_SUCCESS if exit_code is None else int(exit_code)
