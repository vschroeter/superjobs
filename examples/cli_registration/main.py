"""Minimal JobCLI registration and help demo (issue #38; execution unavailable)."""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass

from superjobs import InMemoryTransport, Job, JobContext, SuperJobs
from superjobs.cli import JobCLI


@dataclass
class GreetRequest:
    name: str


@dataclass
class GreetResult:
    message: str


GREET_JOB = Job(
    "examples.cli.greet",
    version="v1",
    request=GreetRequest,
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

    cli.add("greet", GREET_JOB, handler=greet_local)

    def greet_handler_factory() -> Callable[[GreetRequest, JobContext[None]], Awaitable[GreetResult]]:
        async def lazy(
            request: GreetRequest,
            context: JobContext[None],
        ) -> GreetResult:
            return GreetResult(message=f"lazy hello, {request.name}")

        return lazy

    cli.add("greet-lazy", GREET_JOB, handler_factory=greet_handler_factory)
    cli.add("greet-remote", GREET_JOB, remote_only=True)
    return cli


def main() -> int:
    return build_cli().main()


if __name__ == "__main__":
    raise SystemExit(main())
