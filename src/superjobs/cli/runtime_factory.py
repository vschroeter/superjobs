"""Async context-manager factories for CLI-owned SuperJobs lifetimes."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractAsyncContextManager

from superjobs.superjobs import SuperJobs

LocalRuntimeFactory = Callable[[], AbstractAsyncContextManager[SuperJobs]]
RemoteRuntimeFactory = Callable[[], AbstractAsyncContextManager[SuperJobs]]

__all__ = ["LocalRuntimeFactory", "RemoteRuntimeFactory"]
