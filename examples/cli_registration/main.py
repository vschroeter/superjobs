"""JobCLI registration demo with JSON and field input (issue #39; execution unavailable)."""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from enum import Enum

from superjobs import InMemoryTransport, Job, JobContext, SuperJobs
from superjobs.cli import CLIField, JobCLI


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


@asynccontextmanager
async def local_runtime() -> AsyncIterator[SuperJobs]:
    """Application-owned SuperJobs lifetime for future local ``run`` execution."""
    jobs = SuperJobs(transport=InMemoryTransport())
    async with jobs:
        yield jobs


@asynccontextmanager
async def remote_runtime() -> AsyncIterator[SuperJobs]:
    """Application-owned SuperJobs lifetime for future NATS ``submit`` execution."""
    from faststream.nats import NatsBroker

    jobs = SuperJobs(broker=NatsBroker("nats://localhost:4222"))
    async with jobs:
        yield jobs


def build_cli() -> JobCLI:
    cli = JobCLI(
        local_runtime_factory=local_runtime,
        remote_runtime_factory=remote_runtime,
    )

    async def greet_local(
        request: GreetRequest,
        context: JobContext[None],
    ) -> GreetResult:
        return GreetResult(message=f"hello, {request.name}")

    cli.add(
        "greet",
        GREET_JOB,
        handler=greet_local,
        positional_fields=("name",),
        field_options={"name": CLIField(help="Name to greet.")},
    )

    def greet_handler_factory() -> Callable[[GreetRequest, JobContext[None]], Awaitable[GreetResult]]:
        async def lazy(
            request: GreetRequest,
            context: JobContext[None],
        ) -> GreetResult:
            return GreetResult(message=f"lazy hello, {request.name}")

        return lazy

    cli.add("greet-lazy", GREET_JOB, handler_factory=greet_handler_factory)
    cli.add("greet-remote", GREET_JOB, remote_only=True)

    async def mood_handler(request: MoodRequest, context: JobContext[None]) -> GreetResult:
        prefix = "hi" if request.mood is Mood.CHEERFUL else "hello"
        message = f"{prefix}, {request.name}"
        return GreetResult(message=message.upper() if request.excited else message)

    cli.add(
        "greet-mood", MOOD_JOB, handler=mood_handler,
        field_options={"mood": CLIField(option="tone", help="Greeting tone.")},
    )
    return cli


def main() -> int:
    return build_cli().main()


if __name__ == "__main__":
    raise SystemExit(main())
