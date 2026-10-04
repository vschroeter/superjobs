"""Default owned in-memory runtime for local CLI ``run`` (issue #40)."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from superjobs.superjobs import SuperJobs
from superjobs.transport.in_memory import InMemoryTransport

__all__ = ["default_local_runtime_factory"]


@asynccontextmanager
async def default_local_runtime_factory() -> AsyncIterator[SuperJobs]:
    """Yield a fresh started ``SuperJobs`` instance with an isolated in-memory transport."""
    jobs = SuperJobs(transport=InMemoryTransport())
    async with jobs:
        yield jobs
