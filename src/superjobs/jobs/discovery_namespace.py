from __future__ import annotations

from typing import TYPE_CHECKING, Any, TypeVar, overload

CapT = TypeVar("CapT")
ReqT = TypeVar("ReqT")
FinalT = TypeVar("FinalT")
InterT = TypeVar("InterT")

from superjobs.discovery.reader import list_offered_jobs, list_worker_registrations
from superjobs.jobs.job import (
    CapabilityNoRequestJob,
    CapabilityRequestJob,
    Job,
    NoRequestJob,
    RequestJob,
)
from superjobs.jobs.job_identity import JobIdentity

if TYPE_CHECKING:
    from superjobs.discovery.models import RawCapabilities, WorkerRegistration
    from superjobs.superjobs import SuperJobs


class JobsDiscovery:
    def __init__(self, runtime: SuperJobs) -> None:
        self._runtime = runtime

    async def jobs(self) -> list[JobIdentity]:
        return await list_offered_jobs(self._runtime.transport)

    @overload
    async def workers(
        self,
        job: CapabilityRequestJob[Any, Any, Any, Any, CapT]
        | CapabilityNoRequestJob[Any, Any, CapT],
        *,
        include_stale: bool = False,
    ) -> list[WorkerRegistration[CapT]]: ...

    @overload
    async def workers(
        self,
        job: RequestJob[Any, Any, Any, Any] | NoRequestJob[Any, Any],
        *,
        include_stale: bool = False,
    ) -> list[WorkerRegistration[object]]: ...

    @overload
    async def workers(
        self,
        job: Job[ReqT, FinalT, InterT],
        *,
        include_stale: bool = False,
    ) -> list[WorkerRegistration[object]]: ...

    @overload
    async def workers(
        self,
        job: JobIdentity,
        *,
        include_stale: bool = False,
    ) -> list[WorkerRegistration[RawCapabilities]]: ...

    async def workers(
        self,
        job: Job[Any, Any, Any] | JobIdentity,
        *,
        include_stale: bool = False,
    ) -> list[Any]:
        return await list_worker_registrations(
            self._runtime.transport,
            job,
            include_stale=include_stale,
        )
