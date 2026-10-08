from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generic, TypeVar

from superjobs.presence.errors import LocalWorkerHandleError

if TYPE_CHECKING:
    from superjobs.jobs.job import Job
    from superjobs.presence.supervisor import PresenceSupervisor

CapT = TypeVar("CapT")


class LocalWorkerHandle(Generic[CapT]):
    """Runtime-local handle for capability updates on one registered handler."""

    __slots__ = ("_job", "_supervisor")

    def __init__(
        self,
        supervisor: PresenceSupervisor,
        job: Job[Any, Any, Any],
    ) -> None:
        self._supervisor = supervisor
        self._job = job

    async def update_capabilities(self, value: CapT | None) -> None:
        await self._supervisor.update_capabilities(self._job, value)

    async def refresh_capabilities(self) -> None:
        await self._supervisor.refresh_capabilities(self._job)

    def _require_entry(self) -> None:
        if not self._supervisor.has_handler(self._job):
            raise LocalWorkerHandleError(
                "No handler is registered in this runtime",
                job=self._job,
            )
