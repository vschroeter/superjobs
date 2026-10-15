"""Runtime-facing resource admission helpers."""

from __future__ import annotations

from superjobs.resources.admission import ResourceAdmission
from superjobs.resources.models import (
    AdmissionSnapshot,
    AdmissionOwner,
    RecoveryResult,
    ReleaseResult,
    TerminationConfirmation,
)
from superjobs.transport.backend import JobBackend


class RuntimeResources:
    """Ownership snapshot and confirmed recovery for the active runtime."""

    def __init__(self, admission: ResourceAdmission) -> None:
        self._admission = admission

    async def snapshot(self) -> AdmissionSnapshot:
        return await self._admission.snapshot()

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
