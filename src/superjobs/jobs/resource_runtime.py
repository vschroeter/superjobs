"""Runtime coordination for resource-bound handler admission."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from superjobs.jobs.job_handler import JobHandler
    from superjobs.superjobs import SuperJobs


class ResourceRuntimeCoordinator:
    """Withdraws readiness and blocks new admission without cancelling live work."""

    def __init__(self, runtime: SuperJobs) -> None:
        self._runtime = runtime
        self._blocked_keys: set[str] = set()
        self._lock = asyncio.Lock()

    def is_admission_blocked(self, resource_key: str) -> bool:
        return resource_key in self._blocked_keys

    def begin_serving(self) -> None:
        self._blocked_keys.clear()

    async def withdraw_for_resource(self, resource_key: str) -> None:
        async with self._lock:
            if resource_key in self._blocked_keys:
                return
            self._blocked_keys.add(resource_key)
        for handler in self._handlers_for_key(resource_key):
            handler.block_new_resource_admission()
        presence = self._runtime._presence
        for handler in self._handlers_for_key(resource_key):
            job = handler.job
            if presence.has_handler(job):
                try:
                    await presence.withdraw_readiness(job)
                except Exception:
                    pass

    def _handlers_for_key(self, resource_key: str) -> list[JobHandler[Any, Any, Any]]:
        return [
            handler
            for handler in self._runtime._handlers.values()
            if handler._resource_config is not None
            and handler._resource_config.requirement.resource_key == resource_key
        ]
