from __future__ import annotations

import asyncio
import uuid
from contextlib import AsyncExitStack
from collections.abc import Callable, Coroutine
from typing import Any, ParamSpec, TypeVar, overload

from faststream.nats import NatsBroker

from superjobs.discovery.config import PresenceConfig
from superjobs.discovery.memory import Clock
from superjobs.presence import LocalWorkerHandle, PresenceSupervisor

from superjobs.jobs.catalog_runtime import (
    close_provider_stack,
    create_job_handler,
    enter_provider_callback,
    register_job_on_backend,
)
from superjobs.jobs.handler_binding import (
    ensure_marked_job,
    get_marked_job,
    validate_handler_job_association,
    validate_handler_signature,
)
from superjobs.jobs.handler_catalog import HandlerBinding, HandlerCatalog, HandlerCatalogSnapshot
from superjobs.jobs.handler_command import Command
from superjobs.jobs.handler_decorators import (
    RuntimeHandlerDecorator,
    RuntimeNoRequestHandlerDecorator,
)
from superjobs.jobs.capability_input import CapabilityInput
from superjobs.jobs.job import (
    CapabilityNoRequestJob,
    CapabilityRequestJob,
    Job,
    NoRequestJob,
    RequestJob,
)
from superjobs.jobs.job_client import (
    CapabilityNoRequestJobClient,
    CapabilityRequestJobClient,
    JobClient,
    NoRequestJobClient,
    RequestJobClient,
    client_for_job,
)
from superjobs.jobs.job_context import JobContext, ObservationPolicy
from superjobs.jobs.job_handler import JobHandler
from superjobs.jobs.retention import ObservationRetention, ResultRetention
from superjobs.jobs.retry_policy import RetryPolicy
from superjobs.runtime_lifecycle import ShutdownCompletion, finish_cleanup
from superjobs.jobs.discovery_namespace import JobsDiscovery
from superjobs.transport.backend import JobBackend
from superjobs.transport.in_memory import InMemoryTransport


ReqT = TypeVar("ReqT")
FinalT = TypeVar("FinalT")
InterT = TypeVar("InterT")
ConstructorP = ParamSpec("ConstructorP")
CapT = TypeVar("CapT")


class SuperJobs:
    def __init__(
        self,
        broker: NatsBroker | None = None,
        queue_config=None,
        *,
        transport: JobBackend | None = None,
        handlers: HandlerCatalog | None = None,
        observation_policy: ObservationPolicy | None = None,
        observation_retention: ObservationRetention | None = None,
        result_retention: ResultRetention | None = None,
        max_result_bytes: int | None = None,
        graceful_shutdown_timeout: float | None = None,
        discovery: bool = True,
        presence_config: PresenceConfig | None = None,
        presence_clock: Clock | None = None,
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

            self.transport = NatsJobBackend(
                broker,
                queue_config,
                presence_config=presence_config,
            )
        else:
            self.transport = InMemoryTransport(
                observation_retention=observation_retention,
                result_retention=result_retention,
                max_result_bytes=max_result_bytes,
                presence_config=presence_config,
                discovery_clock=presence_clock,
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
        self._catalog = handlers if handlers is not None else HandlerCatalog()
        self._provider_bindings: dict[str, HandlerBinding] = {}
        self._provider_stack: AsyncExitStack | None = None
        self._started = False
        self._lifecycle_lock = asyncio.Lock()
        self._lifecycle_owner: asyncio.Task[Any] | None = None
        self._active_lifecycle_generation = 0
        self._completion: ShutdownCompletion | None = None
        self._stop_task: asyncio.Task[None] | None = None
        self._starting = False
        self._worker_id = uuid.uuid4().hex
        self._discovery_enabled = discovery
        self._presence = PresenceSupervisor(
            worker_id=self._worker_id,
            transport=self.transport,
            enabled=discovery,
            presence_config=presence_config,
            presence_clock=presence_clock,
        )
        self._serving_generation = 0
        self.discovery = JobsDiscovery(self)
        if handlers is not None:
            self._consume_catalog_snapshot(handlers.snapshot())

    @property
    def started(self) -> bool:
        return self._started

    @property
    def worker_id(self) -> str:
        return self._worker_id

    @property
    def catalog(self) -> HandlerCatalog:
        return self._catalog

    def _consume_catalog_snapshot(self, snapshot: HandlerCatalogSnapshot) -> None:
        for binding in snapshot:
            key = binding.job.canonical_name
            if binding.provider is not None:
                self._provider_bindings[key] = binding
            elif binding.callback is not None:
                self._install_catalog_callback(binding)

    def _install_catalog_callback(self, binding: HandlerBinding) -> None:
        if binding.callback is None:
            raise RuntimeError("catalog callback binding is missing a callback")
        self._register_handler(
            binding.job,
            binding.callback,
            concurrency=binding.concurrency,
            retry=binding.retry,
            observation_policy=binding.observation_policy,
            heartbeat_interval=binding.heartbeat_interval,
            capabilities=binding.capabilities,
            catalog_source=True,
        )

    async def _activate_provider_bindings(self) -> None:
        if not self._provider_bindings:
            return
        stack = AsyncExitStack()
        self._provider_stack = stack
        for binding in self._provider_bindings.values():
            callback = await enter_provider_callback(binding, stack)
            handler = create_job_handler(
                binding,
                callback,
                backend=self.transport,
                default_observation_policy=self.observation_policy,
            )
            register_job_on_backend(self.transport, binding.job)
            self._presence.register_handler(binding.job, binding.capabilities)
            self._handlers[binding.job.canonical_name] = handler

    async def _release_provider_bindings(self, errors: list[BaseException]) -> None:
        stack = self._provider_stack
        self._provider_stack = None
        if stack is None:
            return
        release_error = await close_provider_stack(stack)
        if release_error is not None:
            errors.append(release_error)

    async def _rollback_start(
        self,
        start_tasks: list[asyncio.Task[None]],
        *,
        serving_generation: int,
    ) -> None:
        for task in start_tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*start_tasks, return_exceptions=True)
        presence_errors: list[BaseException] = []
        try:
            await self._presence.rollback_generation(serving_generation)
        except BaseException as error:
            presence_errors.append(error)
        errors = [
            result for result in await asyncio.gather(
                *(handler.stop(graceful=False) for handler in self._handlers.values()),
                return_exceptions=True,
            ) if isinstance(result, BaseException)
        ]
        errors.extend(presence_errors)
        await self._presence.stop_renewal()
        await self._release_provider_bindings(errors)
        try:
            await self.transport.stop()
        except BaseException as error:
            errors.append(error)
        if errors:
            raise BaseExceptionGroup("Runtime startup rollback failed", errors)

    async def _start_locked(self) -> ShutdownCompletion:
        if self._stop_task is not None and not self._stop_task.done():
            await finish_cleanup(self._stop_task)
        self._starting = True
        start_tasks: list[asyncio.Task[None]] = []
        serving_generation = 0
        try:
            serving_generation = await self._presence.begin_serving_generation()
            self._serving_generation = serving_generation
            await self.transport.start()
            await self._activate_provider_bindings()
            if self._handlers:
                await self._presence.publish_all_initial(generation=serving_generation)
            start_tasks = [
                asyncio.create_task(handler.start(), name=f"superjobs-handler-start-{key}")
                for key, handler in self._handlers.items()
            ]
            if start_tasks:
                await asyncio.gather(*start_tasks)
            self._presence.start_renewal()
        except BaseException as error:
            rollback = asyncio.create_task(
                self._rollback_start(start_tasks, serving_generation=serving_generation),
            )
            try:
                await finish_cleanup(rollback)
            except BaseException as cleanup_error:
                error.add_note(f"Startup rollback also failed: {cleanup_error!r}")
            raise
        finally:
            self._starting = False
        self._active_lifecycle_generation += 1
        self._completion = ShutdownCompletion()
        self._stop_task = None
        self._started = True
        return self._completion

    async def start(self) -> None:
        async with self._lifecycle_lock:
            if self._started:
                return
            if self._lifecycle_owner is not None:
                raise RuntimeError("A SuperJobs lifecycle owner is already active")
            await self._start_locked()

    async def _stop_generation(
        self,
        completion: ShutdownCompletion,
        *,
        graceful: bool,
        timeout: float | None,
    ) -> None:
        try:
            async def _stop_admission() -> None:
                results = await asyncio.gather(
                    *(
                        handler.stop_admission()
                        for handler in self._handlers.values()
                    ),
                    return_exceptions=True,
                )
                admission_errors = [
                    result for result in results if isinstance(result, BaseException)
                ]
                if admission_errors:
                    raise BaseExceptionGroup(
                        "Handler admission stop failed",
                        admission_errors,
                    )

            presence_errors = await self._presence.shutdown(
                stop_admission=_stop_admission,
            )
            results = await asyncio.gather(
                *(handler.stop(graceful=graceful, timeout=timeout)
                  for handler in self._handlers.values()),
                return_exceptions=True,
            )
            errors = [result for result in results if isinstance(result, BaseException)]
            errors.extend(presence_errors)
            if errors:
                completion.error = errors[0]
                for error in errors[1:]:
                    completion.error.add_note(f"Additional handler shutdown failure: {error!r}")
        except BaseException as error:
            completion.error = error
        finally:
            release_errors: list[BaseException] = []
            await self._release_provider_bindings(release_errors)
            if release_errors and completion.error is None:
                completion.error = release_errors[0]
                for error in release_errors[1:]:
                    completion.error.add_note(f"Additional provider release failure: {error!r}")
            try:
                await self.transport.stop()
            except BaseException as error:
                if completion.error is None:
                    completion.error = error
                else:
                    completion.error.add_note(f"Transport shutdown also failed: {error!r}")
            finally:
                completion.done.set()

    async def stop(
        self,
        *,
        graceful: bool = True,
        timeout: float | None = None,
    ) -> None:
        if timeout is not None and timeout <= 0:
            raise ValueError("timeout must be positive")
        async with self._lifecycle_lock:
            completion = self._completion
            if self._stop_task is not None and not self._stop_task.done():
                task = self._stop_task
            elif not self._started:
                return
            else:
                assert completion is not None
                self._started = False
                task = self._stop_task = asyncio.create_task(
                    self._stop_generation(
                        completion,
                        graceful=graceful,
                        timeout=timeout if timeout is not None else self.graceful_shutdown_timeout,
                    ),
                    name="superjobs-shutdown",
                )
        await finish_cleanup(task)
        if completion is not None and completion.error is not None:
            raise completion.error

    async def serve(self) -> None:
        """Own startup, waiting, and graceful shutdown of an unstarted runtime.

        Cancellation propagates after cleanup. No process signal handlers are
        installed. An external stop releases this owner after draining completes.
        """
        async with self._lifecycle_lock:
            if self._lifecycle_owner is not None:
                raise RuntimeError("A SuperJobs lifecycle owner is already active")
            if self._started or (self._stop_task is not None and not self._stop_task.done()):
                raise RuntimeError("SuperJobs runtime is already started or stopping")
            self._lifecycle_owner = asyncio.current_task()
            try:
                completion = await self._start_locked()
            except BaseException:
                self._lifecycle_owner = None
                raise
        try:
            await completion.wait()
        finally:
            try:
                await self.stop()
            finally:
                async with self._lifecycle_lock:
                    self._lifecycle_owner = None

    async def wait_until_stopped(self) -> None:
        """Observe the active lifetime, including drain, without owning it.

        Independent waiter cancellation never requests runtime shutdown. A
        captured generation retains its outcome even if the runtime restarts.
        """
        completion = self._completion
        if completion is None or completion.done.is_set():
            raise RuntimeError("SuperJobs runtime is not started")
        await completion.wait()

    async def __aenter__(self) -> SuperJobs:
        await self.start()
        return self

    async def __aexit__(self, exception_type, exception, traceback) -> None:
        await self.stop()

    @overload
    def handler(
        self,
        job: CapabilityNoRequestJob[FinalT, InterT, CapT],
        *,
        concurrency: int = 1,
        retry: RetryPolicy | None = None,
        observation_policy: ObservationPolicy | None = None,
        heartbeat_interval: float | None = None,
        cli: str | Command | None = None,
        capabilities: CapabilityInput[CapT] = None,
    ) -> RuntimeNoRequestHandlerDecorator[FinalT, InterT]: ...

    @overload
    def handler(
        self,
        job: NoRequestJob[FinalT, InterT],
        *,
        concurrency: int = 1,
        retry: RetryPolicy | None = None,
        observation_policy: ObservationPolicy | None = None,
        heartbeat_interval: float | None = None,
        cli: str | Command | None = None,
        capabilities: None = None,
    ) -> RuntimeNoRequestHandlerDecorator[FinalT, InterT]: ...

    @overload
    def handler(
        self,
        job: CapabilityRequestJob[ReqT, FinalT, InterT, ConstructorP, CapT],
        *,
        concurrency: int = 1,
        retry: RetryPolicy | None = None,
        observation_policy: ObservationPolicy | None = None,
        heartbeat_interval: float | None = None,
        cli: str | Command | None = None,
        capabilities: CapabilityInput[CapT] = None,
    ) -> RuntimeHandlerDecorator[ReqT, FinalT, InterT]: ...

    @overload
    def handler(
        self,
        job: RequestJob[ReqT, FinalT, InterT, ConstructorP],
        *,
        concurrency: int = 1,
        retry: RetryPolicy | None = None,
        observation_policy: ObservationPolicy | None = None,
        heartbeat_interval: float | None = None,
        cli: str | Command | None = None,
        capabilities: None = None,
    ) -> RuntimeHandlerDecorator[ReqT, FinalT, InterT]: ...

    def handler(
        self,
        job: Job[Any, Any, Any],
        *,
        concurrency: int = 1,
        retry: RetryPolicy | None = None,
        observation_policy: ObservationPolicy | None = None,
        heartbeat_interval: float | None = None,
        cli: str | Command | None = None,
        capabilities: object = None,
    ) -> RuntimeHandlerDecorator[Any, Any, Any] | RuntimeNoRequestHandlerDecorator[Any, Any]:
        if job.request_type is None:
            return RuntimeNoRequestHandlerDecorator(
                self,
                job,
                concurrency=concurrency,
                retry=retry,
                observation_policy=observation_policy,
                heartbeat_interval=heartbeat_interval,
                cli=cli,
                capabilities=capabilities,
            )
        return RuntimeHandlerDecorator(
            self,
            job,
            concurrency=concurrency,
            retry=retry,
            observation_policy=observation_policy,
            heartbeat_interval=heartbeat_interval,
            cli=cli,
            capabilities=capabilities,
        )

    # Compatibility alias for the original sketch while callers migrate.
    handle = handler

    @overload
    def register(
        self,
        job: CapabilityNoRequestJob[FinalT, InterT, CapT],
        callback: Callable[[JobContext[InterT]], Coroutine[Any, Any, FinalT]],
        /,
        *,
        concurrency: int = 1,
        retry: RetryPolicy | None = None,
        observation_policy: ObservationPolicy | None = None,
        heartbeat_interval: float | None = None,
        capabilities: CapabilityInput[CapT] = None,
    ) -> None: ...

    @overload
    def register(
        self,
        job: CapabilityNoRequestJob[FinalT, InterT, CapT],
        callback: Callable[[JobContext[InterT]], FinalT],
        /,
        *,
        concurrency: int = 1,
        retry: RetryPolicy | None = None,
        observation_policy: ObservationPolicy | None = None,
        heartbeat_interval: float | None = None,
        capabilities: CapabilityInput[CapT] = None,
    ) -> None: ...

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
        capabilities: None = None,
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
        capabilities: None = None,
    ) -> None: ...

    @overload
    def register(
        self,
        job: CapabilityRequestJob[ReqT, FinalT, InterT, ConstructorP, CapT],
        callback: Callable[[ReqT, JobContext[InterT]], Coroutine[Any, Any, FinalT]],
        /,
        *,
        concurrency: int = 1,
        retry: RetryPolicy | None = None,
        observation_policy: ObservationPolicy | None = None,
        heartbeat_interval: float | None = None,
        capabilities: CapabilityInput[CapT] = None,
    ) -> None: ...

    @overload
    def register(
        self,
        job: CapabilityRequestJob[ReqT, FinalT, InterT, ConstructorP, CapT],
        callback: Callable[[ReqT, JobContext[InterT]], FinalT],
        /,
        *,
        concurrency: int = 1,
        retry: RetryPolicy | None = None,
        observation_policy: ObservationPolicy | None = None,
        heartbeat_interval: float | None = None,
        capabilities: CapabilityInput[CapT] = None,
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
        capabilities: None = None,
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
        capabilities: None = None,
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
        cli: str | Command | None = None,
        capabilities: Any | None = None,
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
                cli=cli,
                capabilities=capabilities,
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
            cli=cli,
            capabilities=capabilities,
        )

    @overload
    def worker(self, job: CapabilityRequestJob[Any, Any, Any, Any, CapT]) -> LocalWorkerHandle[CapT]: ...

    @overload
    def worker(self, job: CapabilityNoRequestJob[Any, Any, CapT]) -> LocalWorkerHandle[CapT]: ...

    @overload
    def worker(self, job: RequestJob[Any, Any, Any, Any]) -> LocalWorkerHandle[None]: ...

    @overload
    def worker(self, job: NoRequestJob[Any, Any]) -> LocalWorkerHandle[None]: ...

    @overload
    def worker(self, job: Job[Any, Any, Any]) -> LocalWorkerHandle[None]: ...

    def worker(self, job: Job[Any, Any, Any]) -> LocalWorkerHandle[Any]:
        return self._presence.worker(job)

    @overload
    def client(
        self,
        job: CapabilityRequestJob[ReqT, FinalT, InterT, ConstructorP, CapT],
    ) -> CapabilityRequestJobClient[ReqT, FinalT, InterT, ConstructorP, CapT]: ...

    @overload
    def client(
        self,
        job: RequestJob[ReqT, FinalT, InterT, ConstructorP],
    ) -> RequestJobClient[ReqT, FinalT, InterT, ConstructorP]: ...

    @overload
    def client(
        self,
        job: CapabilityNoRequestJob[FinalT, InterT, CapT],
    ) -> CapabilityNoRequestJobClient[FinalT, InterT, CapT]: ...

    @overload
    def client(
        self,
        job: NoRequestJob[FinalT, InterT],
    ) -> NoRequestJobClient[FinalT, InterT]: ...

    @overload
    def client(
        self,
        job: Job[ReqT, FinalT, InterT],
    ) -> JobClient[ReqT, FinalT, InterT]: ...

    def client(
        self,
        job: Job[ReqT, FinalT, InterT],
    ) -> JobClient[ReqT, FinalT, InterT]:
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
        cli: str | Command | None = None,
        capabilities: Any | None = None,
        catalog_source: bool = False,
    ) -> None:
        if self._starting:
            raise RuntimeError("Cannot register handlers while the runtime is starting")
        if self._stop_task is not None and not self._stop_task.done():
            raise RuntimeError(
                "Cannot register handlers while the runtime is shutting down",
            )
        key = job.canonical_name
        if key in self._handlers or key in self._provider_bindings:
            raise ValueError(f"A handler is already registered for {job}")

        if not catalog_source:
            existing = self._catalog.get(job)
            if existing is not None:
                if existing.provider is not None:
                    raise ValueError(
                        f"A catalog provider binding already exists for {job}",
                    )
                if existing.callback is not None and existing.callback is not callback:
                    raise ValueError(
                        f"Catalog binding for {job} does not match the runtime handler",
                    )
                if cli is not None:
                    raise ValueError(
                        f"A catalog binding already exists for {job}; "
                        "cannot attach cli metadata through the runtime",
                    )

        validate_handler_job_association(job, callback)
        validate_handler_signature(job, callback)

        if not catalog_source:
            existing = self._catalog.get(job)
            if existing is None:
                marked = get_marked_job(callback)
                if marked is None or marked is job:
                    self._catalog._register_callback(
                        job,
                        callback,
                        concurrency=concurrency,
                        retry=retry,
                        observation_policy=observation_policy,
                        heartbeat_interval=heartbeat_interval,
                        capabilities=capabilities,
                        cli=cli,
                        attach_marker=False,
                    )

        handler = JobHandler(
            job,
            callback,
            backend=self.transport,
            concurrency=concurrency,
            retry=retry,
            observation_policy=observation_policy or self.observation_policy,
            heartbeat_interval=heartbeat_interval,
        )

        register_job_on_backend(self.transport, job)
        self._presence.register_handler(job, capabilities)

        self._handlers[key] = handler
        if self._started:
            self._schedule_handler_start(handler, job)

    def _schedule_handler_start(
        self,
        handler: JobHandler[Any, Any, Any],
        job: Job[Any, Any, Any],
    ) -> None:
        generation = self._serving_generation

        async def _publish_then_start() -> None:
            try:
                if self._presence.enabled:
                    await self._presence.publish_initial(job, generation=generation)
                if not self._started or self._serving_generation != generation:
                    raise RuntimeError("The runtime stopped before the handler became ready")
                await handler.start()
            except BaseException as error:
                try:
                    await self._presence.rollback_handler(job, generation=generation)
                except BaseException as cleanup_error:
                    error.add_note(f"Handler presence rollback also failed: {cleanup_error!r}")
                raise

        if handler._start_task is None or handler._start_task.done():
            handler._start_task = asyncio.create_task(
                _publish_then_start(),
                name=f"superjobs-start-{job.canonical_name}",
            )
