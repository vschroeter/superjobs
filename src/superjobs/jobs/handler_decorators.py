"""Job-shaped decorators that retain callback parameters and result types."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any, ParamSpec, Protocol, TypeVar, overload

from superjobs.jobs.handler_binding import attach_job_marker
from superjobs.jobs.job_context import JobContext, ObservationPolicy
from superjobs.jobs.retry_policy import RetryPolicy

if TYPE_CHECKING:
    from superjobs.jobs.job import Job, NoRequestJob, RequestJob
    from superjobs.superjobs import SuperJobs

ReqT = TypeVar("ReqT")
FinalT = TypeVar("FinalT")
InterT = TypeVar("InterT")
ConstructorP = ParamSpec("ConstructorP")


class _RequestCallback[Request, Result, Event, **Parameters, Return](Protocol):
    # The first overload proves that the runtime can supply exactly these
    # arguments. The second captures the original complete callable signature.
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


class JobHandlerDecorator[Request, Result, Event](
    _RequestDecorator[Request, Result, Event]
):
    def __init__(self, job: Job[Request, Result, Event]) -> None:
        self._job = job

    def _bind(self, callback: Callable[..., Any]) -> None:
        attach_job_marker(self._job, callback)


class JobNoRequestHandlerDecorator[Result, Event](_ContextDecorator[Result, Event]):
    def __init__(self, job: Job[Any, Result, Event]) -> None:
        self._job = job

    def _bind(self, callback: Callable[..., Any]) -> None:
        attach_job_marker(self._job, callback)


class _RuntimeBinding:
    def __init__(
        self,
        superjobs: SuperJobs,
        job: Job[Any, Any, Any],
        *,
        concurrency: int = 1,
        retry: RetryPolicy | None = None,
        observation_policy: ObservationPolicy | None = None,
        heartbeat_interval: float | None = None,
        cli: str | Any | None = None,
    ) -> None:
        self._superjobs = superjobs
        self._job = job
        self._concurrency = concurrency
        self._retry = retry
        self._observation_policy = observation_policy
        self._heartbeat_interval = heartbeat_interval
        self._cli = cli

    def _bind(self, callback: Callable[..., Any]) -> None:
        self._superjobs._register_handler(
            self._job,
            callback,
            concurrency=self._concurrency,
            retry=self._retry,
            observation_policy=self._observation_policy,
            heartbeat_interval=self._heartbeat_interval,
            cli=self._cli,
        )


class RuntimeHandlerDecorator[Request, Result, Event](
    _RuntimeBinding, _RequestDecorator[Request, Result, Event]
):
    pass


class RuntimeNoRequestHandlerDecorator[Result, Event](
    _RuntimeBinding, _ContextDecorator[Result, Event]
):
    pass


class JobHandlerDescriptor:
    @overload
    def __get__(
        self, obj: NoRequestJob[FinalT, InterT], owner: type[object] | None
    ) -> JobNoRequestHandlerDecorator[FinalT, InterT]: ...

    @overload
    def __get__(
        self, obj: RequestJob[ReqT, FinalT, InterT, ConstructorP], owner: type[object] | None
    ) -> JobHandlerDecorator[ReqT, FinalT, InterT]: ...

    def __get__(
        self, obj: Job[Any, Any, Any] | None, owner: type[object] | None
    ) -> JobHandlerDecorator[Any, Any, Any] | JobNoRequestHandlerDecorator[Any, Any]:
        if obj is None:
            raise AttributeError("Job.handler must be accessed on a Job instance")
        if obj.request_type is None:
            return JobNoRequestHandlerDecorator(obj)
        return JobHandlerDecorator(obj)


job_handler_descriptor = JobHandlerDescriptor()
