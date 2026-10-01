from __future__ import annotations

import hashlib
import uuid
from datetime import datetime
from typing import Any, ParamSpec, TypeVar, overload

from superjobs.exceptions.jobs import JobNotFoundError
from superjobs.jobs.job import Job, NoRequestJob, RequestJob
from superjobs.jobs.job_handle import JobHandle
from superjobs.jobs.submit_options import SubmitOptions
from superjobs.jobs.submission import (
    SubmitShapeError,
    normalized_submit_options,
    resolve_request_submission,
)
from superjobs.transport.backend import JobBackend, SubmissionOptions

_ReqT = TypeVar("_ReqT")
_FinalT = TypeVar("_FinalT")
_InterT = TypeVar("_InterT")
_ConstructorP = ParamSpec("_ConstructorP")


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
        /,
        *,
        options: SubmitOptions | None = None,
        idempotency_key: str | None = None,
        job_id: str | None = None,
        timeout: float | None = None,
        deadline: datetime | None = None,
        caller_scope: str = "default",
    ) -> JobHandle[ReqT, FinalT, InterT]:
        keywords: dict[str, Any] = {}
        if options is not None:
            keywords["options"] = options
        for name, value in (
            ("idempotency_key", idempotency_key),
            ("job_id", job_id),
            ("timeout", timeout),
            ("deadline", deadline),
        ):
            if value is not None:
                keywords[name] = value
        if caller_scope != "default":
            keywords["caller_scope"] = caller_scope
        return await self._submit_call((request,), keywords)

    async def _submit_call(
        self,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> JobHandle[ReqT, FinalT, InterT]:
        request, options_obj, legacy = resolve_request_submission(self.job, args, kwargs)
        return await self._submit_prepared(
            request,
            options_obj=options_obj,
            legacy=legacy,
        )

    async def _submit_prepared(
        self,
        request: ReqT,
        *,
        options_obj: SubmitOptions | None,
        legacy: dict[str, Any],
    ) -> JobHandle[ReqT, FinalT, InterT]:
        idempotency_key, job_id, timeout, deadline, caller_scope = normalized_submit_options(
            options_obj=options_obj,
            legacy=legacy,
        )
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
        options: SubmitOptions | None = None,
    ) -> FinalT:
        if options is not None:
            if (
                idempotency_key is not None
                or job_id is not None
                or timeout is not None
                or deadline is not None
                or caller_scope != "default"
            ):
                raise SubmitShapeError("Pass execution options through SubmitOptions or legacy keywords, not both")
            handle = await self.submit(request, options=options)
        else:
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


class RequestJobClient[ReqT, FinalT, InterT, **ConstructorP](JobClient[ReqT, FinalT, InterT]):
    @overload
    async def submit(
        self,
        request: ReqT,
        /,
        *,
        options: SubmitOptions | None = None,
        idempotency_key: str | None = None,
        job_id: str | None = None,
        timeout: float | None = None,
        deadline: datetime | None = None,
        caller_scope: str = "default",
    ) -> JobHandle[ReqT, FinalT, InterT]: ...

    @overload
    async def submit(
        self,
        options: SubmitOptions,
        /,
        *args: ConstructorP.args,
        **kwargs: ConstructorP.kwargs,
    ) -> JobHandle[ReqT, FinalT, InterT]: ...

    @overload
    async def submit(
        self,
        /,
        *args: ConstructorP.args,
        **kwargs: ConstructorP.kwargs,
    ) -> JobHandle[ReqT, FinalT, InterT]: ...

    async def submit(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> JobHandle[ReqT, FinalT, InterT]:
        return await self._submit_call(args, kwargs)


class NoRequestJobClient[FinalT, InterT](JobClient[None, FinalT, InterT]):
    @overload
    async def submit(
        self,
        /,
        *,
        options: SubmitOptions | None = None,
        idempotency_key: str | None = None,
        job_id: str | None = None,
        timeout: float | None = None,
        deadline: datetime | None = None,
        caller_scope: str = "default",
    ) -> JobHandle[None, FinalT, InterT]: ...

    @overload
    async def submit(
        self,
        legacy_none: None,
        /,
        *,
        options: SubmitOptions | None = None,
        idempotency_key: str | None = None,
        job_id: str | None = None,
        timeout: float | None = None,
        deadline: datetime | None = None,
        caller_scope: str = "default",
    ) -> JobHandle[None, FinalT, InterT]: ...

    @overload
    async def submit(
        self,
        options: SubmitOptions,
        /,
        *,
        idempotency_key: str | None = None,
        job_id: str | None = None,
        timeout: float | None = None,
        deadline: datetime | None = None,
        caller_scope: str = "default",
    ) -> JobHandle[None, FinalT, InterT]: ...

    async def submit(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> JobHandle[None, FinalT, InterT]:
        return await self._submit_call(args, kwargs)


@overload
def client_for_job(
    job: NoRequestJob[_FinalT, _InterT],
    backend: JobBackend,
) -> NoRequestJobClient[_FinalT, _InterT]: ...


@overload
def client_for_job(
    job: RequestJob[_ReqT, _FinalT, _InterT, _ConstructorP],
    backend: JobBackend,
) -> RequestJobClient[_ReqT, _FinalT, _InterT, _ConstructorP]: ...


@overload
def client_for_job(
    job: Job[_ReqT, _FinalT, _InterT],
    backend: JobBackend,
) -> JobClient[_ReqT, _FinalT, _InterT]: ...


def client_for_job(
    job: Job[Any, Any, Any],
    backend: JobBackend,
) -> JobClient[Any, Any, Any]:
    if isinstance(job, NoRequestJob):
        return NoRequestJobClient(job, backend)
    if isinstance(job, RequestJob):
        return RequestJobClient(job, backend)
    return JobClient(job, backend)


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
