from __future__ import annotations

import hashlib
import uuid
from datetime import UTC, datetime
from typing import Any

from superjobs.exceptions.jobs import JobNotFoundError
from superjobs.jobs.job import Job
from superjobs.jobs.job_handle import JobHandle
from superjobs.transport.backend import JobBackend, SubmissionOptions


class JobClient[ReqT: Any | None, FinalT: Any | None, InterT: Any | None]:
    def __init__(self, job: Job[ReqT, FinalT, InterT], backend: JobBackend):
        self.job = job
        self.backend = backend
        register_job = getattr(backend, "register_job", None)
        if register_job is not None:
            register_job(job)

    async def submit(
        self,
        request: ReqT,
        *,
        idempotency_key: str | None = None,
        job_id: str | None = None,
        timeout: float | None = None,
        deadline: datetime | None = None,
        caller_scope: str = "default",
    ) -> JobHandle[ReqT, FinalT, InterT]:
        if timeout is not None and timeout <= 0:
            raise ValueError("timeout must be positive")
        if idempotency_key is not None and not idempotency_key:
            raise ValueError("idempotency_key must not be empty")
        if job_id is not None and not job_id:
            raise ValueError("job_id must not be empty")
        if not caller_scope:
            raise ValueError("caller_scope must not be empty")
        deadline = _normalise_deadline(deadline)
        idempotency_key = idempotency_key or str(uuid.uuid4())
        job_id = job_id or str(uuid.uuid4())
        payload = self.job.encode_request(request)
        fingerprint = submission_fingerprint(
            self.job,
            payload,
            timeout=timeout,
            deadline=deadline,
        )
        options = SubmissionOptions(
            idempotency_key=idempotency_key,
            job_id=job_id,
            caller_scope=caller_scope,
            timeout=timeout,
            deadline=deadline,
        )
        execution = await self.backend.submit(
            self.job.identity,
            request_payload=payload,
            request_media_type=(
                self.job.request_codec.media_type
                if self.job.request_codec is not None
                else None
            ),
            fingerprint=fingerprint,
            options=options,
        )
        return JobHandle(self.job, self.backend, execution.job_id)

    async def run(
        self,
        request: ReqT,
        *,
        idempotency_key: str | None = None,
        job_id: str | None = None,
        timeout: float | None = None,
        deadline: datetime | None = None,
        caller_scope: str = "default",
        wait_timeout: float | None = None,
    ) -> FinalT:
        handle = await self.submit(
            request,
            idempotency_key=idempotency_key,
            job_id=job_id,
            timeout=timeout,
            deadline=deadline,
            caller_scope=caller_scope,
        )
        return await handle.result(wait_timeout=wait_timeout)

    async def get(self, job_id: str) -> JobHandle[ReqT, FinalT, InterT]:
        execution = await self.backend.get_execution(self.job.identity, job_id)
        if execution is None:
            raise JobNotFoundError(job_id)
        return JobHandle(self.job, self.backend, job_id)

    async def handle(self, job_id: str) -> JobHandle[ReqT, FinalT, InterT]:
        return await self.get(job_id)

    async def start(self) -> None:
        if not getattr(self.backend, "started", False):
            await self.backend.start()


def _normalise_deadline(deadline: datetime | None) -> datetime | None:
    if deadline is None:
        return None
    if deadline.tzinfo is None:
        raise ValueError("deadline must be timezone-aware")
    return deadline.astimezone(UTC)


def submission_fingerprint(
    job: Job[Any, Any, Any],
    payload: bytes,
    *,
    timeout: float | None,
    deadline: datetime | None,
) -> str:
    digest = hashlib.sha256()
    for value in (
        job.canonical_name,
        str(timeout),
        deadline.isoformat() if deadline is not None else "",
    ):
        encoded = value.encode("utf-8")
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
    digest.update(len(payload).to_bytes(8, "big"))
    digest.update(payload)
    return digest.hexdigest()


_submission_fingerprint = submission_fingerprint
