from __future__ import annotations

import asyncio
from collections.abc import Callable, Coroutine
from typing import Any, ParamSpec, TypeVar, overload

from faststream.nats import NatsBroker

from superjobs.jobs.handler_binding import (
    ensure_marked_job,
    validate_handler_job_association,
    validate_handler_signature,
)
from superjobs.jobs.handler_decorators import (
    RuntimeHandlerDecorator,
    RuntimeNoRequestHandlerDecorator,
)
from superjobs.jobs.job import Job, NoRequestJob, RequestJob
from superjobs.jobs.job_client import (
    JobClient,
    NoRequestJobClient,
    RequestJobClient,
    client_for_job,
)
from superjobs.jobs.job_context import JobContext, ObservationPolicy
from superjobs.jobs.job_handler import JobHandler
from superjobs.jobs.retention import ObservationRetention, ResultRetention
from superjobs.jobs.retry_policy import RetryPolicy
from superjobs.transport.backend import JobBackend
from superjobs.transport.in_memory import InMemoryTransport


ReqT = TypeVar("ReqT")
FinalT = TypeVar("FinalT")
InterT = TypeVar("InterT")
ConstructorP = ParamSpec("ConstructorP")


class SuperJobs:
    def __init__(
        self,
        broker: NatsBroker | None = None,
        queue_config=None,
        *,
        transport: JobBackend | None = None,
        observation_policy: ObservationPolicy | None = None,
        observation_retention: ObservationRetention | None = None,
        result_retention: ResultRetention | None = None,
        max_result_bytes: int | None = None,
        graceful_shutdown_timeout: float | None = None,
    ):
        if broker is not None and transport is not None:
            raise ValueError("Pass either broker or transport, not both")
        if graceful_shutdown_timeout is not None and graceful_shutdown_timeout <= 0:
            raise ValueError("graceful_shutdown_timeout must be positive")
        if max_result_bytes is not None and max_result_bytes < 1:
            raise ValueError("max_result_bytes must be positive")

        if transport is not None:
            self.transport = transport
        elif broker is not None:
            from superjobs.transport.nats_backend import NatsJobBackend

            self.transport = NatsJobBackend(broker, queue_config)
        else:
            self.transport = InMemoryTransport(
                observation_retention=observation_retention,
                result_retention=result_retention,
                max_result_bytes=max_result_bytes,
            )

        if observation_retention is not None and hasattr(
            self.transport,
            "observation_retention",
        ):
            setattr(self.transport, "observation_retention", observation_retention)
        if result_retention is not None and hasattr(self.transport, "result_retention"):
            setattr(self.transport, "result_retention", result_retention)
        if max_result_bytes is not None and hasattr(self.transport, "max_result_bytes"):
            setattr(self.transport, "max_result_bytes", max_result_bytes)

        self.observation_policy = observation_policy or ObservationPolicy()
        self.graceful_shutdown_timeout = graceful_shutdown_timeout
        self._handlers: dict[str, JobHandler[Any, Any, Any]] = {}
        self._started = False
        self._lifecycle_lock = asyncio.Lock()

    @property
    def started(self) -> bool:
        return self._started

    async def start(self) -> None:
        async with self._lifecycle_lock:
            if self._started:
                return

            await self.transport.start()
            self._started = True
            try:
                await asyncio.gather(
                    *(handler.start() for handler in self._handlers.values()),
                )
            except BaseException:
                self._started = False
                await asyncio.gather(
                    *(
                        handler.stop(graceful=False)
                        for handler in self._handlers.values()
                    ),
                    return_exceptions=True,
                )
                await self.transport.stop()
                raise

    async def stop(
        self,
        *,
        graceful: bool = True,
        timeout: float | None = None,
    ) -> None:
        if timeout is not None and timeout <= 0:
            raise ValueError("timeout must be positive")
        async with self._lifecycle_lock:
            if not self._started:
                return
            self._started = False
            drain_timeout = (
                timeout
                if timeout is not None
                else self.graceful_shutdown_timeout
            )
            await asyncio.gather(
                *(
                    handler.stop(
                        graceful=graceful,
                        timeout=drain_timeout,
                    )
                    for handler in self._handlers.values()
                ),
                return_exceptions=True,
            )
            await self.transport.stop()

    async def __aenter__(self) -> SuperJobs:
        await self.start()
        return self

    async def __aexit__(self, exception_type, exception, traceback) -> None:
        await self.stop()

    @overload
    def handler(
        self,
        job: NoRequestJob[FinalT, InterT],
        *,
        concurrency: int = 1,
        retry: RetryPolicy | None = None,
        observation_policy: ObservationPolicy | None = None,
        heartbeat_interval: float | None = None,
    ) -> RuntimeNoRequestHandlerDecorator[FinalT, InterT]: ...

    @overload
    def handler(
        self,
        job: RequestJob[ReqT, FinalT, InterT, ConstructorP],
        *,
        concurrency: int = 1,
        retry: RetryPolicy | None = None,
        observation_policy: ObservationPolicy | None = None,
        heartbeat_interval: float | None = None,
    ) -> RuntimeHandlerDecorator[ReqT, FinalT, InterT]: ...

    def handler(
        self,
        job: Job[Any, Any, Any],
        *,
        concurrency: int = 1,
        retry: RetryPolicy | None = None,
        observation_policy: ObservationPolicy | None = None,
        heartbeat_interval: float | None = None,
    ) -> RuntimeHandlerDecorator[Any, Any, Any] | RuntimeNoRequestHandlerDecorator[Any, Any]:
        if job.request_type is None:
            return RuntimeNoRequestHandlerDecorator(
                self,
                job,
                concurrency=concurrency,
                retry=retry,
                observation_policy=observation_policy,
                heartbeat_interval=heartbeat_interval,
            )
        return RuntimeHandlerDecorator(
            self,
            job,
            concurrency=concurrency,
            retry=retry,
            observation_policy=observation_policy,
            heartbeat_interval=heartbeat_interval,
        )

    # Compatibility alias for the original sketch while callers migrate.
    handle = handler

    @overload
    def register(
        self,
        job: NoRequestJob[FinalT, InterT],
        callback: Callable[[JobContext[InterT]], Coroutine[Any, Any, FinalT]],
        /,
        *,
        concurrency: int = 1,
        retry: RetryPolicy | None = None,
        observation_policy: ObservationPolicy | None = None,
        heartbeat_interval: float | None = None,
    ) -> None: ...

    @overload
    def register(
        self,
        job: NoRequestJob[FinalT, InterT],
        callback: Callable[[JobContext[InterT]], FinalT],
        /,
        *,
        concurrency: int = 1,
        retry: RetryPolicy | None = None,
        observation_policy: ObservationPolicy | None = None,
        heartbeat_interval: float | None = None,
    ) -> None: ...

    @overload
    def register(
        self,
        job: RequestJob[ReqT, FinalT, InterT, ConstructorP],
        callback: Callable[[ReqT, JobContext[InterT]], Coroutine[Any, Any, FinalT]],
        /,
        *,
        concurrency: int = 1,
        retry: RetryPolicy | None = None,
        observation_policy: ObservationPolicy | None = None,
        heartbeat_interval: float | None = None,
    ) -> None: ...

    @overload
    def register(
        self,
        job: RequestJob[ReqT, FinalT, InterT, ConstructorP],
        callback: Callable[[ReqT, JobContext[InterT]], FinalT],
        /,
        *,
        concurrency: int = 1,
        retry: RetryPolicy | None = None,
        observation_policy: ObservationPolicy | None = None,
        heartbeat_interval: float | None = None,
    ) -> None: ...

    @overload
    def register(
        self,
        marked_callback: Callable[[JobContext[InterT]], Coroutine[Any, Any, FinalT]],
        /,
        *,
        concurrency: int = 1,
        retry: RetryPolicy | None = None,
        observation_policy: ObservationPolicy | None = None,
        heartbeat_interval: float | None = None,
    ) -> None: ...

    @overload
    def register(
        self,
        marked_callback: Callable[[JobContext[InterT]], FinalT],
        /,
        *,
        concurrency: int = 1,
        retry: RetryPolicy | None = None,
        observation_policy: ObservationPolicy | None = None,
        heartbeat_interval: float | None = None,
    ) -> None: ...

    @overload
    def register(
        self,
        marked_callback: Callable[[ReqT, JobContext[InterT]], Coroutine[Any, Any, FinalT]],
        /,
        *,
        concurrency: int = 1,
        retry: RetryPolicy | None = None,
        observation_policy: ObservationPolicy | None = None,
        heartbeat_interval: float | None = None,
    ) -> None: ...

    @overload
    def register(
        self,
        marked_callback: Callable[[ReqT, JobContext[InterT]], FinalT],
        /,
        *,
        concurrency: int = 1,
        retry: RetryPolicy | None = None,
        observation_policy: ObservationPolicy | None = None,
        heartbeat_interval: float | None = None,
    ) -> None: ...

    def register(
        self,
        job_or_marked: Job[Any, Any, Any] | Callable[..., Any],
        callback: Callable[..., Any] | None = None,
        /,
        *,
        concurrency: int = 1,
        retry: RetryPolicy | None = None,
        observation_policy: ObservationPolicy | None = None,
        heartbeat_interval: float | None = None,
    ) -> None:
        if callback is not None:
            if not isinstance(job_or_marked, Job):
                raise TypeError("First argument must be a Job when registering a handler")
            self._register_handler(
                job_or_marked,
                callback,
                concurrency=concurrency,
                retry=retry,
                observation_policy=observation_policy,
                heartbeat_interval=heartbeat_interval,
            )
            return

        marked = job_or_marked
        if isinstance(marked, Job):
            raise TypeError(
                "Pass a handler callable decorated with @job.handler, or call "
                "register(job, handler)",
            )
        job = ensure_marked_job(marked)
        self._register_handler(
            job,
            marked,
            concurrency=concurrency,
            retry=retry,
            observation_policy=observation_policy,
            heartbeat_interval=heartbeat_interval,
        )

    @overload
    def client(
        self,
        job: RequestJob[ReqT, FinalT, InterT, ConstructorP],
    ) -> RequestJobClient[ReqT, FinalT, InterT, ConstructorP]: ...

    @overload
    def client(self, job: NoRequestJob[FinalT, InterT]) -> NoRequestJobClient[FinalT, InterT]: ...

    @overload
    def client(self, job: Job[ReqT, FinalT, InterT]) -> JobClient[ReqT, FinalT, InterT]: ...

    def client(self, job: Job[ReqT, FinalT, InterT]) -> JobClient[ReqT, FinalT, InterT]:
        return client_for_job(job, self.transport)

    def _register_handler(
        self,
        job: Job[Any, Any, Any],
        callback: Callable[..., Any],
        *,
        concurrency: int = 1,
        retry: RetryPolicy | None = None,
        observation_policy: ObservationPolicy | None = None,
        heartbeat_interval: float | None = None,
    ) -> None:
        validate_handler_job_association(job, callback)
        validate_handler_signature(job, callback)
        key = job.canonical_name
        if key in self._handlers:
            raise ValueError(f"A handler is already registered for {job}")

        handler = JobHandler(
            job,
            callback,
            backend=self.transport,
            concurrency=concurrency,
            retry=retry,
            observation_policy=observation_policy or self.observation_policy,
            heartbeat_interval=heartbeat_interval,
        )

        register_job = getattr(self.transport, "register_job", None)
        if register_job is not None:
            register_job(job)

        self._handlers[key] = handler
        if self._started:
            handler.schedule_start()
