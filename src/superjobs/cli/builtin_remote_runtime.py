"""Built-in producer-only NATS runtime for CLI submit (issue #46)."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from faststream.nats import NatsBroker

from superjobs.cli.nats_url import resolve_nats_url
from superjobs.superjobs import SuperJobs


@asynccontextmanager
async def builtin_remote_runtime(
    *,
    constructor_url: str | None,
    cli_override: str | None = None,
) -> AsyncIterator[SuperJobs]:
    url = resolve_nats_url(
        cli_override=cli_override,
        constructor_url=constructor_url,
        use_ambient_env=True,
    )
    broker = NatsBroker(url, connect_timeout=5, allow_reconnect=False)
    jobs = SuperJobs(broker=broker)
    async with jobs:
        yield jobs
