"""NATS URL resolution for built-in remote CLI runtimes (issue #46)."""

from __future__ import annotations

import os
from typing import Final

DEFAULT_NATS_URL: Final = "nats://localhost:4222"
ENV_NATS_URL: Final = "SUPERJOBS_NATS_URL"


def resolve_nats_url(
    *,
    cli_override: str | None = None,
    constructor_url: str | None = None,
    use_ambient_env: bool = True,
) -> str:
    if cli_override is not None:
        if not cli_override:
            raise ValueError("nats-url must be non-empty when provided")
        return cli_override
    if use_ambient_env:
        env = os.environ.get(ENV_NATS_URL)
        if env:
            return env
    if constructor_url is not None:
        return constructor_url
    return DEFAULT_NATS_URL
