"""JobCLI catalog demo: shared handlers, built-in local run, NATS submit (#39–#46)."""

from __future__ import annotations

import os
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import Enum

from superjobs import CLIField, Command, HandlerCatalog, Job, JobContext
from superjobs.cli import JobCLI


@dataclass
class GreetRequest:
    name: str


@dataclass
class GreetResult:
    message: str


class Mood(str, Enum):
    CHEERFUL = "cheerful"
    PLAIN = "plain"


@dataclass
class MoodRequest:
    name: str
    mood: Mood = Mood.PLAIN
    excited: bool = False


GREET_JOB = Job(
    "examples.cli.greet",
    version="v1",
    request=GreetRequest,
    result=GreetResult,
)

MOOD_JOB = Job(
    "examples.cli.mood",
    version="v1",
    request=MoodRequest,
    result=GreetResult,
)

HANDLERS = HandlerCatalog()


@HANDLERS.handler(
    GREET_JOB,
    cli=Command(
        name="greet",
        positional_fields=("name",),
        field_options={"name": CLIField(help="Name to greet.")},
    ),
)
async def greet_local(
    request: GreetRequest,
    context: JobContext[None],
) -> GreetResult:
    return GreetResult(message=f"hello, {request.name}")


@HANDLERS.handler(
    MOOD_JOB,
    cli=Command(
        name="greet-mood",
        field_options={"mood": CLIField(option="tone", help="Greeting tone.")},
    ),
)
async def mood_handler(request: MoodRequest, context: JobContext[None]) -> GreetResult:
    prefix = "hi" if request.mood is Mood.CHEERFUL else "hello"
    message = f"{prefix}, {request.name}"
    return GreetResult(message=message.upper() if request.excited else message)


def _greet_lazy_factory() -> Callable[
    [GreetRequest, JobContext[None]],
    Awaitable[GreetResult],
]:
    async def lazy(
        request: GreetRequest,
        context: JobContext[None],
    ) -> GreetResult:
        return GreetResult(message=f"lazy hello, {request.name}")

    return lazy


def build_cli() -> JobCLI:
    configured = os.environ.get("SUPERJOBS_CLI_CONFIGURED_NATS_URL")
    cli = JobCLI(handlers=HANDLERS, nats_url=configured)
    cli.add("greet-lazy", GREET_JOB, handler_factory=_greet_lazy_factory)
    cli.add("greet-remote", GREET_JOB, remote_only=True)
    return cli


def main() -> int:
    return build_cli().main()


if __name__ == "__main__":
    raise SystemExit(main())
