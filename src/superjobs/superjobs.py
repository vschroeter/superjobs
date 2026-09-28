from __future__ import annotations

import asyncio
import inspect
from collections.abc import Callable
from typing import Any, TypeVar

from faststream.nats import NatsBroker

from superjobs.jobs.job import Job
from superjobs.jobs.job_client import JobClient
from superjobs.jobs.job_context import ObservationPolicy
from superjobs.jobs.job_handler import JobHandler
from superjobs.jobs.retention import ObservationRetention, ResultRetention
from superjobs.jobs.retry_policy import RetryPolicy
from superjobs.transport.backend import JobBackend
from superjobs.transport.in_memory import InMemoryTransport


ReqT = TypeVar("ReqT")
FinalT = TypeVar("FinalT")
InterT = TypeVar("InterT")


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

    def handler(
        self,
        job: Job[Any, Any, Any],
        *,
        concurrency: int = 1,
        retry: RetryPolicy | None = None,
        observation_policy: ObservationPolicy | None = None,
        heartbeat_interval: float | None = None,
    ):
        def decorator(callback: Callable[..., Any]) -> Callable[..., Any]:
            self._validate_callback(job, callback)
            register_job = getattr(self.transport, "register_job", None)
            if register_job is not None:
                register_job(job)
            handler = JobHandler(
                job,
                callback,
                backend=self.transport,
                concurrency=concurrency,
                retry=retry,
                observation_policy=observation_policy or self.observation_policy,
                heartbeat_interval=heartbeat_interval,
            )
            key = job.canonical_name
            if key in self._handlers:
                raise ValueError(f"A handler is already registered for {job}")
            self._handlers[key] = handler
            if self._started:
                handler.schedule_start()
            return callback

        return decorator

    # Compatibility alias for the original sketch while callers migrate.
    handle = handler

    def client(self, job: Job[ReqT, FinalT, InterT]) -> JobClient[ReqT, FinalT, InterT]:
        return JobClient(job, self.transport)

    @staticmethod
    def _validate_callback(job: Job[Any, Any, Any], callback: Callable[..., Any]) -> None:
        signature = inspect.signature(callback)
        positional = [
            parameter
            for parameter in signature.parameters.values()
            if parameter.kind
            in (
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
            )
        ]
        expected = 1 if job.request_type is None else 2
        if len(positional) < expected:
            raise TypeError(
                f"Handler for {job} must accept {expected} positional arguments",
            )
