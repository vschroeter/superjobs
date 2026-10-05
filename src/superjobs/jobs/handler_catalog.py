"""Backend-free declarative handler catalog (issue #46, Variant C foundation)."""

from __future__ import annotations

import math
from collections.abc import Awaitable, Callable, Coroutine, Iterator, Mapping
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, ParamSpec, Protocol, TypeVar, overload

from superjobs.jobs.handler_binding import (
    attach_job_marker,
    ensure_marked_job,
    validate_handler_job_association,
    validate_handler_signature,
)
from superjobs.jobs.handler_command import Command, normalize_command
from superjobs.jobs.job import Job, NoRequestJob, RequestJob
from superjobs.jobs.job_context import JobContext, ObservationPolicy
from superjobs.jobs.retry_policy import RetryPolicy

ReqT = TypeVar("ReqT")
FinalT = TypeVar("FinalT")
InterT = TypeVar("InterT")
ConstructorP = ParamSpec("ConstructorP")

_ProviderCallback = Callable[..., Any]
_ProviderFactory = Callable[[], AbstractAsyncContextManager[_ProviderCallback]]


class _RequestCallback[Request, Result, Event, **Parameters, Return](Protocol):
    @overload
    def __call__(
        self, request: Request, context: JobContext[Event], /
    ) -> Result | Awaitable[Result]: ...

    @overload
    def __call__(self, *args: Parameters.args, **kwargs: Parameters.kwargs) -> Return: ...


class _ContextCallback[Result, Event, **Parameters, Return](Protocol):
    @overload
    def __call__(self, context: JobContext[Event], /) -> Result | Awaitable[Result]: ...

    @overload
    def __call__(self, *args: Parameters.args, **kwargs: Parameters.kwargs) -> Return: ...


class _RequestDecorator[Request, Result, Event]:
    def __call__[**Parameters, Return](
        self, callback: _RequestCallback[Request, Result, Event, Parameters, Return]
    ) -> Callable[Parameters, Return]:
        self._bind(callback)
        return callback

    def _bind(self, callback: Callable[..., Any]) -> None:
        raise NotImplementedError


class _ContextDecorator[Result, Event]:
    def __call__[**Parameters, Return](
        self, callback: _ContextCallback[Result, Event, Parameters, Return]
    ) -> Callable[Parameters, Return]:
        self._bind(callback)
        return callback

    def _bind(self, callback: Callable[..., Any]) -> None:
        raise NotImplementedError


@dataclass(frozen=True, slots=True)
class HandlerBinding:
    """Immutable validated catalog entry for one Job."""

    job: Job[Any, Any, Any]
    callback: Callable[..., Any] | None
    provider: _ProviderFactory | None
    concurrency: int
    retry: RetryPolicy | None
    observation_policy: ObservationPolicy | None
    heartbeat_interval: float | None
    cli: Command | None

    def __post_init__(self) -> None:
        if self.callback is None and self.provider is None:
            raise ValueError("HandlerBinding requires a callback or provider")
        if self.callback is not None and self.provider is not None:
            raise ValueError("HandlerBinding cannot combine callback and provider")
        if self.concurrency < 1:
            raise ValueError("concurrency must be at least one")
        if self.heartbeat_interval is not None and self.heartbeat_interval <= 0:
            raise ValueError("heartbeat_interval must be positive")


class HandlerCatalogSnapshot:
    """Point-in-time view of catalog bindings; unaffected by later catalog mutations."""

    __slots__ = ("_bindings",)

    def __init__(self, bindings: Mapping[str, HandlerBinding]) -> None:
        self._bindings: Mapping[str, HandlerBinding] = MappingProxyType(dict(bindings))

    def get(self, job: Job[Any, Any, Any]) -> HandlerBinding | None:
        return self._bindings.get(job.canonical_name)

    def bindings(self) -> Mapping[str, HandlerBinding]:
        return self._bindings

    def __iter__(self) -> Iterator[HandlerBinding]:
        return iter(self._bindings.values())

    def __len__(self) -> int:
        return len(self._bindings)


class _CatalogBinding:
    def __init__(
        self,
        catalog: HandlerCatalog,
        job: Job[Any, Any, Any],
        *,
        concurrency: int = 1,
        retry: RetryPolicy | None = None,
        observation_policy: ObservationPolicy | None = None,
        heartbeat_interval: float | None = None,
        cli: str | Command | None = None,
    ) -> None:
        self._catalog = catalog
        self._job = job
        self._concurrency = concurrency
        self._retry = retry
        self._observation_policy = observation_policy
        self._heartbeat_interval = heartbeat_interval
        self._cli = cli

    def _bind(self, callback: Callable[..., Any]) -> None:
        self._catalog._register_callback(
            self._job,
            callback,
            concurrency=self._concurrency,
            retry=self._retry,
            observation_policy=self._observation_policy,
            heartbeat_interval=self._heartbeat_interval,
            cli=self._cli,
        )


class CatalogHandlerDecorator[Request, Result, Event](
    _CatalogBinding, _RequestDecorator[Request, Result, Event]
):
    pass


class CatalogNoRequestHandlerDecorator[Result, Event](
    _CatalogBinding, _ContextDecorator[Result, Event]
):
    pass


class HandlerCatalog:
    """Collects backend-free handler definitions for runtime and CLI consumers."""

    def __init__(self) -> None:
        self._bindings: dict[str, HandlerBinding] = {}

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
    ) -> CatalogNoRequestHandlerDecorator[FinalT, InterT]: ...

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
    ) -> CatalogHandlerDecorator[ReqT, FinalT, InterT]: ...

    def handler(
        self,
        job: Job[Any, Any, Any],
        *,
        concurrency: int = 1,
        retry: RetryPolicy | None = None,
        observation_policy: ObservationPolicy | None = None,
        heartbeat_interval: float | None = None,
        cli: str | Command | None = None,
    ) -> CatalogHandlerDecorator[Any, Any, Any] | CatalogNoRequestHandlerDecorator[Any, Any]:
        if job.request_type is None:
            return CatalogNoRequestHandlerDecorator(
                self,
                job,
                concurrency=concurrency,
                retry=retry,
                observation_policy=observation_policy,
                heartbeat_interval=heartbeat_interval,
                cli=cli,
            )
        return CatalogHandlerDecorator(
            self,
            job,
            concurrency=concurrency,
            retry=retry,
            observation_policy=observation_policy,
            heartbeat_interval=heartbeat_interval,
            cli=cli,
        )

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
        cli: str | Command | None = None,
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
        cli: str | Command | None = None,
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
        cli: str | Command | None = None,
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
        cli: str | Command | None = None,
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
        cli: str | Command | None = None,
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
        cli: str | Command | None = None,
    ) -> None: ...

    @overload
    def register(
        self,
        marked_callback: Callable[
            [ReqT, JobContext[InterT]], Coroutine[Any, Any, FinalT]
        ],
        /,
        *,
        concurrency: int = 1,
        retry: RetryPolicy | None = None,
        observation_policy: ObservationPolicy | None = None,
        heartbeat_interval: float | None = None,
        cli: str | Command | None = None,
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
        cli: str | Command | None = None,
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
    ) -> None:
        if callback is not None:
            if not isinstance(job_or_marked, Job):
                raise TypeError("First argument must be a Job when registering a handler")
            self._register_callback(
                job_or_marked,
                callback,
                concurrency=concurrency,
                retry=retry,
                observation_policy=observation_policy,
                heartbeat_interval=heartbeat_interval,
                cli=cli,
            )
            return

        marked = job_or_marked
        if isinstance(marked, Job):
            raise TypeError(
                "Pass a handler callable decorated with @job.handler, or call "
                "register(job, handler)",
            )
        job = ensure_marked_job(marked)
        self._register_callback(
            job,
            marked,
            concurrency=concurrency,
            retry=retry,
            observation_policy=observation_policy,
            heartbeat_interval=heartbeat_interval,
            cli=cli,
        )

    @overload
    def bind(
        self,
        job: NoRequestJob[FinalT, InterT],
        /,
        *,
        provider: Callable[
            [],
            AbstractAsyncContextManager[
                Callable[[JobContext[InterT]], FinalT]
                | Callable[[JobContext[InterT]], Coroutine[Any, Any, FinalT]]
            ],
        ],
        concurrency: int = 1,
        retry: RetryPolicy | None = None,
        observation_policy: ObservationPolicy | None = None,
        heartbeat_interval: float | None = None,
        cli: str | Command | None = None,
    ) -> None: ...

    @overload
    def bind(
        self,
        job: RequestJob[ReqT, FinalT, InterT, ConstructorP],
        /,
        *,
        provider: Callable[
            [],
            AbstractAsyncContextManager[
                Callable[[ReqT, JobContext[InterT]], FinalT]
                | Callable[[ReqT, JobContext[InterT]], Coroutine[Any, Any, FinalT]]
            ],
        ],
        concurrency: int = 1,
        retry: RetryPolicy | None = None,
        observation_policy: ObservationPolicy | None = None,
        heartbeat_interval: float | None = None,
        cli: str | Command | None = None,
    ) -> None: ...

    def bind(
        self,
        job: Job[Any, Any, Any],
        /,
        *,
        provider: _ProviderFactory,
        concurrency: int = 1,
        retry: RetryPolicy | None = None,
        observation_policy: ObservationPolicy | None = None,
        heartbeat_interval: float | None = None,
        cli: str | Command | None = None,
    ) -> None:
        self._register_provider(
            job,
            provider,
            concurrency=concurrency,
            retry=retry,
            observation_policy=observation_policy,
            heartbeat_interval=heartbeat_interval,
            cli=cli,
        )

    @overload
    def bind_provider(
        self,
        job: NoRequestJob[FinalT, InterT],
        provider: Callable[
            [],
            AbstractAsyncContextManager[
                Callable[[JobContext[InterT]], FinalT]
                | Callable[[JobContext[InterT]], Coroutine[Any, Any, FinalT]]
            ],
        ],
        /,
        *,
        concurrency: int = 1,
        retry: RetryPolicy | None = None,
        observation_policy: ObservationPolicy | None = None,
        heartbeat_interval: float | None = None,
        cli: str | Command | None = None,
    ) -> None: ...

    @overload
    def bind_provider(
        self,
        job: RequestJob[ReqT, FinalT, InterT, ConstructorP],
        provider: Callable[
            [],
            AbstractAsyncContextManager[
                Callable[[ReqT, JobContext[InterT]], FinalT]
                | Callable[[ReqT, JobContext[InterT]], Coroutine[Any, Any, FinalT]]
            ],
        ],
        /,
        *,
        concurrency: int = 1,
        retry: RetryPolicy | None = None,
        observation_policy: ObservationPolicy | None = None,
        heartbeat_interval: float | None = None,
        cli: str | Command | None = None,
    ) -> None: ...

    def bind_provider(
        self,
        job: Job[Any, Any, Any],
        provider: _ProviderFactory,
        /,
        *,
        concurrency: int = 1,
        retry: RetryPolicy | None = None,
        observation_policy: ObservationPolicy | None = None,
        heartbeat_interval: float | None = None,
        cli: str | Command | None = None,
    ) -> None:
        """Compatibility alias for :meth:`bind`."""
        self._register_provider(
            job,
            provider,
            concurrency=concurrency,
            retry=retry,
            observation_policy=observation_policy,
            heartbeat_interval=heartbeat_interval,
            cli=cli,
        )

    def get(self, job: Job[Any, Any, Any]) -> HandlerBinding | None:
        return self._bindings.get(job.canonical_name)

    def snapshot(self) -> HandlerCatalogSnapshot:
        return HandlerCatalogSnapshot(self._bindings)

    def _register_callback(
        self,
        job: Job[Any, Any, Any],
        callback: Callable[..., Any],
        *,
        concurrency: int,
        retry: RetryPolicy | None,
        observation_policy: ObservationPolicy | None,
        heartbeat_interval: float | None,
        cli: str | Command | None,
        attach_marker: bool = True,
    ) -> None:
        validate_handler_job_association(job, callback)
        validate_handler_signature(job, callback)
        normalized_cli = _prepare_registration(
            job,
            catalog_keys=self._bindings,
            concurrency=concurrency,
            heartbeat_interval=heartbeat_interval,
            cli=cli,
        )
        binding = HandlerBinding(
            job=job,
            callback=callback,
            provider=None,
            concurrency=concurrency,
            retry=retry,
            observation_policy=observation_policy,
            heartbeat_interval=heartbeat_interval,
            cli=normalized_cli,
        )
        self._store_binding(job, binding, callback=callback, attach_marker=attach_marker)

    def _register_provider(
        self,
        job: Job[Any, Any, Any],
        provider: _ProviderFactory,
        *,
        concurrency: int,
        retry: RetryPolicy | None,
        observation_policy: ObservationPolicy | None,
        heartbeat_interval: float | None,
        cli: str | Command | None,
    ) -> None:
        _validate_provider_factory(provider)
        normalized_cli = _prepare_registration(
            job,
            catalog_keys=self._bindings,
            concurrency=concurrency,
            heartbeat_interval=heartbeat_interval,
            cli=cli,
        )
        binding = HandlerBinding(
            job=job,
            callback=None,
            provider=provider,
            concurrency=concurrency,
            retry=retry,
            observation_policy=observation_policy,
            heartbeat_interval=heartbeat_interval,
            cli=normalized_cli,
        )
        self._store_binding(job, binding, callback=None)

    def _store_binding(
        self,
        job: Job[Any, Any, Any],
        binding: HandlerBinding,
        *,
        callback: Callable[..., Any] | None,
        attach_marker: bool = True,
    ) -> None:
        if callback is not None and attach_marker:
            attach_job_marker(job, callback)
        self._bindings[job.canonical_name] = binding


def _prepare_registration(
    job: Job[Any, Any, Any],
    *,
    catalog_keys: Mapping[str, HandlerBinding],
    concurrency: int,
    heartbeat_interval: float | None,
    cli: str | Command | None,
) -> Command | None:
    _validate_handler_options(
        concurrency=concurrency,
        heartbeat_interval=heartbeat_interval,
    )
    if job.canonical_name in catalog_keys:
        raise ValueError(f"A handler is already registered for {job}")
    return normalize_command(cli)


def _validate_handler_options(
    *,
    concurrency: int,
    heartbeat_interval: float | None,
) -> None:
    if isinstance(concurrency, bool) or not isinstance(concurrency, int):
        raise TypeError("concurrency must be a positive integer")
    if concurrency < 1:
        raise ValueError("concurrency must be at least one")
    if heartbeat_interval is not None:
        if isinstance(heartbeat_interval, bool) or not isinstance(
            heartbeat_interval, (int, float)
        ):
            raise TypeError("heartbeat_interval must be a positive finite number")
        if not math.isfinite(heartbeat_interval) or heartbeat_interval <= 0:
            raise ValueError("heartbeat_interval must be a positive finite number")


def _validate_provider_factory(provider: _ProviderFactory) -> None:
    if not callable(provider):
        raise TypeError("provider must be callable")
