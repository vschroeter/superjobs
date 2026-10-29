"""Runtime-facing resource admission helpers."""

from __future__ import annotations

from superjobs.resources.admission import ResourceAdmission
from superjobs.resources.models import (
    AdmissionSnapshot,
    AdmissionOwner,
    RecoveryResult,
    ReleaseResult,
    TerminationConfirmation,
    WaitingExecutionView,
)
from superjobs.resources.waiting_execution import read_waiting_executions
from superjobs.transport.backend import JobBackend


class RuntimeResources:
    """Ownership snapshot and confirmed recovery for the active runtime."""

    def __init__(
        self,
        admission: ResourceAdmission,
        *,
        backend: JobBackend,
    ) -> None:
        self._admission = admission
        self._backend = backend

    async def snapshot(self) -> AdmissionSnapshot:
        return await self._admission.snapshot()

    async def waiting_executions(
        self,
        *,
        max_entries: int | None = None,
    ) -> WaitingExecutionView:
        """Read-only view of persisted resource-waiting completion records.

        Requires the runtime transport to have started already; normal startup may
        provision JetStream buckets. Entries reflect durable
        ``waiting_for_resource`` records with a requested resource key; they are
        historical evidence only and do not prove live blocking. Any scanned
        ``waiting_for_resource`` or ``pending`` record marks the view ``partial``
        because liveness cannot be verified from observation alone.
        ``evaluated_at`` is when the scan started; stored evidence may be older;
        concurrent writers may race the view during and after the scan.
        Completeness reflects bounded scan coverage, not live contention.
        The read path does not mutate admission or write broker completion data.
        """
        return await read_waiting_executions(
            self._backend,
            max_entries=max_entries,
        )

    async def release_resource_after_cleanup(
        self, owner: AdmissionOwner
    ) -> ReleaseResult:
        """Acknowledge completed application cleanup using the exact owner token."""
        return await self._admission.release_resource_after_cleanup(owner)

    async def resolve_terminated_incarnation(
        self,
        confirmation: TerminationConfirmation,
    ) -> RecoveryResult:
        return await self._admission.resolve_terminated_incarnation(confirmation)


def resource_admission_for_backend(backend: JobBackend) -> ResourceAdmission | None:
    store = getattr(backend, "resource_admission_store", None)
    if store is None:
        return None
    return ResourceAdmission(store)
