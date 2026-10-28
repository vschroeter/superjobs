from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generic, TypeVar

from superjobs.discovery.capabilities import materialize_registration
from superjobs.discovery.models import WorkerRegistration
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

    @property
    def readiness_withdrawn(self) -> bool:
        """Whether this handler's readiness has been withdrawn locally."""
        return self._supervisor.is_readiness_withdrawn(self._job)

    @property
    def own_registration_active(self) -> bool:
        """Whether this runtime's registry row is actively offered right now."""
        return self._supervisor.is_own_registration_active(self._job)

    async def wait_until_readiness_withdrawn(self, *, timeout: float | None = None) -> None:
        """Wait until withdrawn readiness is published or locally recorded."""
        await self._supervisor.wait_until_readiness_withdrawn(self._job, timeout=timeout)

    async def wait_until_own_registration_inactive(
        self,
        *,
        timeout: float | None = None,
    ) -> None:
        """Wait until the registry row is missing, draining, expired, or unreadable."""
        await self._supervisor.wait_until_own_registration_inactive(
            self._job,
            timeout=timeout,
        )

    async def read_own_registration(
        self,
        *,
        include_stale: bool = False,
    ) -> WorkerRegistration[CapT] | None:
        """Read this runtime's registration for the handler from the discovery store."""
        self._require_entry()
        stored = await self._supervisor.read_own_registration(
            self._job,
            include_stale=include_stale,
        )
        if stored is None:
            return None
        return materialize_registration(
            stored,
            capability_type=self._job.capability_type,
            capability_codec=self._job.capability_codec,
            decode_typed=True,
        )

    async def reconcile_own_registration(
        self,
    ) -> WorkerRegistration[CapT] | None:
        """Re-read the registry row and refresh local observation state."""
        self._require_entry()
        stored = await self._supervisor.reconcile_own_registration(self._job)
        if stored is None:
            return None
        return materialize_registration(
            stored,
            capability_type=self._job.capability_type,
            capability_codec=self._job.capability_codec,
            decode_typed=True,
        )

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
