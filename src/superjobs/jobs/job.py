from __future__ import annotations

from collections.abc import Callable
from typing import Any, TypeVar, cast, overload

CapT = TypeVar("CapT")

from superjobs.jobs.handler_decorators import job_handler_descriptor
from superjobs.jobs.job_identity import JobIdentity
from superjobs.payload.codec.payloadcodec import PayloadCodec
from superjobs.registry import registry


class Job[ReqT: Any | None, FinalT: Any | None, InterT: Any | None]:
    @overload
    def __new__[**P](
        cls,
        name: str,
        *,
        request: Callable[P, ReqT],
        result: None = None,
        event: None = None,
        version: str | None = None,
        job_identity: JobIdentity | None = None,
        request_codec: PayloadCodec[ReqT] | None = None,
        result_codec: PayloadCodec[FinalT] | None = None,
        event_codec: PayloadCodec[InterT] | None = None,
    ) -> RequestJob[ReqT, None, None, P]: ...

    @overload
    def __new__[**P](
        cls,
        name: str,
        *,
        request: Callable[P, ReqT],
        result: None,
        event: None = None,
        version: str | None = None,
        job_identity: JobIdentity | None = None,
        request_codec: PayloadCodec[ReqT] | None = None,
        result_codec: PayloadCodec[FinalT] | None = None,
        event_codec: PayloadCodec[InterT] | None = None,
    ) -> RequestJob[ReqT, None, None, P]: ...

    @overload
    def __new__[**P](
        cls,
        name: str,
        *,
        request: Callable[P, ReqT],
        result: None = None,
        event: None = None,
        capabilities: type[CapT],
        capability_codec: PayloadCodec[CapT] | None = None,
        version: str | None = None,
        job_identity: JobIdentity | None = None,
        request_codec: PayloadCodec[ReqT] | None = None,
        result_codec: PayloadCodec[FinalT] | None = None,
        event_codec: PayloadCodec[InterT] | None = None,
    ) -> CapabilityRequestJob[ReqT, None, None, P, CapT]: ...

    @overload
    def __new__[**P](
        cls,
        name: str,
        request: Callable[P, ReqT],
        result: type[FinalT],
        event: type[InterT],
        *,
        version: str | None = None,
        job_identity: JobIdentity | None = None,
        request_codec: PayloadCodec[ReqT] | None = None,
        result_codec: PayloadCodec[FinalT] | None = None,
        event_codec: PayloadCodec[InterT] | None = None,
    ) -> RequestJob[ReqT, FinalT, InterT, P]: ...

    @overload
    def __new__[**P](
        cls,
        name: str,
        request: Callable[P, ReqT],
        result: type[FinalT],
        event: type[InterT],
        *,
        version: str | None = None,
        job_identity: JobIdentity | None = None,
        request_codec: PayloadCodec[ReqT] | None = None,
        result_codec: PayloadCodec[FinalT] | None = None,
        event_codec: PayloadCodec[InterT] | None = None,
        capabilities: type[CapT],
        capability_codec: PayloadCodec[CapT] | None = None,
    ) -> CapabilityRequestJob[ReqT, FinalT, InterT, P, CapT]: ...

    @overload
    def __new__[**P](
        cls,
        name: str,
        request: Callable[P, ReqT],
        result: type[FinalT],
        event: None = None,
        *,
        version: str | None = None,
        job_identity: JobIdentity | None = None,
        request_codec: PayloadCodec[ReqT] | None = None,
        result_codec: PayloadCodec[FinalT] | None = None,
        event_codec: PayloadCodec[InterT] | None = None,
    ) -> RequestJob[ReqT, FinalT, None, P]: ...

    @overload
    def __new__[**P](
        cls,
        name: str,
        request: Callable[P, ReqT],
        result: type[FinalT],
        event: None = None,
        *,
        version: str | None = None,
        job_identity: JobIdentity | None = None,
        request_codec: PayloadCodec[ReqT] | None = None,
        result_codec: PayloadCodec[FinalT] | None = None,
        event_codec: PayloadCodec[InterT] | None = None,
        capabilities: type[CapT],
        capability_codec: PayloadCodec[CapT] | None = None,
    ) -> CapabilityRequestJob[ReqT, FinalT, None, P, CapT]: ...

    @overload
    def __new__[**P](
        cls,
        name: str,
        request: Callable[P, ReqT],
        result: None = None,
        event: type[InterT] = ...,
        *,
        version: str | None = None,
        job_identity: JobIdentity | None = None,
        request_codec: PayloadCodec[ReqT] | None = None,
        result_codec: PayloadCodec[FinalT] | None = None,
        event_codec: PayloadCodec[InterT] | None = None,
    ) -> RequestJob[ReqT, None, InterT, P]: ...

    @overload
    def __new__[**P](
        cls,
        name: str,
        request: Callable[P, ReqT],
        result: None = None,
        event: type[InterT] = ...,
        *,
        version: str | None = None,
        job_identity: JobIdentity | None = None,
        request_codec: PayloadCodec[ReqT] | None = None,
        result_codec: PayloadCodec[FinalT] | None = None,
        event_codec: PayloadCodec[InterT] | None = None,
        capabilities: type[CapT],
        capability_codec: PayloadCodec[CapT] | None = None,
    ) -> CapabilityRequestJob[ReqT, None, InterT, P, CapT]: ...

    @overload
    def __new__[**P](
        cls,
        name: str,
        request: Callable[P, ReqT],
        result: None = None,
        event: None = None,
        *,
        version: str | None = None,
        job_identity: JobIdentity | None = None,
        request_codec: PayloadCodec[ReqT] | None = None,
        result_codec: PayloadCodec[FinalT] | None = None,
        event_codec: PayloadCodec[InterT] | None = None,
    ) -> RequestJob[ReqT, None, None, P]: ...

    @overload
    def __new__[**P](
        cls,
        name: str,
        request: Callable[P, ReqT],
        result: None = None,
        event: None = None,
        *,
        version: str | None = None,
        job_identity: JobIdentity | None = None,
        request_codec: PayloadCodec[ReqT] | None = None,
        result_codec: PayloadCodec[FinalT] | None = None,
        event_codec: PayloadCodec[InterT] | None = None,
        capabilities: type[CapT],
        capability_codec: PayloadCodec[CapT] | None = None,
    ) -> CapabilityRequestJob[ReqT, None, None, P, CapT]: ...

    @overload
    def __new__(
        cls,
        name: str,
        request: None = None,
        *,
        result: type[FinalT],
        event: type[InterT],
        version: str | None = None,
        job_identity: JobIdentity | None = None,
        request_codec: PayloadCodec[ReqT] | None = None,
        result_codec: PayloadCodec[FinalT] | None = None,
        event_codec: PayloadCodec[InterT] | None = None,
    ) -> NoRequestJob[FinalT, InterT]: ...

    @overload
    def __new__(
        cls,
        name: str,
        request: None = None,
        *,
        result: type[FinalT],
        event: type[InterT],
        version: str | None = None,
        job_identity: JobIdentity | None = None,
        request_codec: PayloadCodec[ReqT] | None = None,
        result_codec: PayloadCodec[FinalT] | None = None,
        event_codec: PayloadCodec[InterT] | None = None,
        capabilities: type[CapT],
        capability_codec: PayloadCodec[CapT] | None = None,
    ) -> CapabilityNoRequestJob[FinalT, InterT, CapT]: ...

    @overload
    def __new__(
        cls,
        name: str,
        request: None = None,
        *,
        result: type[FinalT],
        event: None = None,
        version: str | None = None,
        job_identity: JobIdentity | None = None,
        request_codec: PayloadCodec[ReqT] | None = None,
        result_codec: PayloadCodec[FinalT] | None = None,
        event_codec: PayloadCodec[InterT] | None = None,
    ) -> NoRequestJob[FinalT, None]: ...

    @overload
    def __new__(
        cls,
        name: str,
        request: None = None,
        *,
        result: type[FinalT],
        event: None = None,
        version: str | None = None,
        job_identity: JobIdentity | None = None,
        request_codec: PayloadCodec[ReqT] | None = None,
        result_codec: PayloadCodec[FinalT] | None = None,
        event_codec: PayloadCodec[InterT] | None = None,
        capabilities: type[CapT],
        capability_codec: PayloadCodec[CapT] | None = None,
    ) -> CapabilityNoRequestJob[FinalT, None, CapT]: ...

    @overload
    def __new__(
        cls,
        name: str,
        request: None = None,
        *,
        result: None = None,
        event: type[InterT],
        version: str | None = None,
        job_identity: JobIdentity | None = None,
        request_codec: PayloadCodec[ReqT] | None = None,
        result_codec: PayloadCodec[FinalT] | None = None,
        event_codec: PayloadCodec[InterT] | None = None,
    ) -> NoRequestJob[None, InterT]: ...

    @overload
    def __new__(
        cls,
        name: str,
        request: None = None,
        *,
        result: None = None,
        event: type[InterT],
        version: str | None = None,
        job_identity: JobIdentity | None = None,
        request_codec: PayloadCodec[ReqT] | None = None,
        result_codec: PayloadCodec[FinalT] | None = None,
        event_codec: PayloadCodec[InterT] | None = None,
        capabilities: type[CapT],
        capability_codec: PayloadCodec[CapT] | None = None,
    ) -> CapabilityNoRequestJob[None, InterT, CapT]: ...

    @overload
    def __new__(
        cls,
        name: str,
        request: None = None,
        *,
        result: None = None,
        event: None = None,
        version: str | None = None,
        job_identity: JobIdentity | None = None,
        request_codec: PayloadCodec[ReqT] | None = None,
        result_codec: PayloadCodec[FinalT] | None = None,
        event_codec: PayloadCodec[InterT] | None = None,
    ) -> NoRequestJob[None, None]: ...

    @overload
    def __new__(
        cls,
        name: str,
        request: None = None,
        *,
        result: None = None,
        event: None = None,
        version: str | None = None,
        job_identity: JobIdentity | None = None,
        request_codec: PayloadCodec[ReqT] | None = None,
        result_codec: PayloadCodec[FinalT] | None = None,
        event_codec: PayloadCodec[InterT] | None = None,
        capabilities: type[CapT],
        capability_codec: PayloadCodec[CapT] | None = None,
    ) -> CapabilityNoRequestJob[None, None, CapT]: ...

    def __new__(
        cls,
        name: str,
        request: Any = None,
        result: Any = None,
        event: Any = None,
        *,
        version: str | None = None,
        job_identity: JobIdentity | None = None,
        request_codec: Any = None,
        result_codec: Any = None,
        event_codec: Any = None,
        capabilities: Any = None,
        capability_codec: Any = None,
    ) -> Any:
        if cls is Job:
            if request is None:
                target: type[Job[Any, Any, Any]] = (
                    CapabilityNoRequestJob
                    if capabilities is not None
                    else NoRequestJob
                )
            else:
                target = (
                    CapabilityRequestJob if capabilities is not None else RequestJob
                )
            return object.__new__(target)
        return super().__new__(cls)

    def __init__(
        self,
        name: str,
        request: type[ReqT] | None = None,
        result: type[FinalT] | None = None,
        event: type[InterT] | None = None,
        *,
        version: str | None = None,
        job_identity: JobIdentity | None = None,
        request_codec: PayloadCodec[ReqT] | None = None,
        result_codec: PayloadCodec[FinalT] | None = None,
        event_codec: PayloadCodec[InterT] | None = None,
        capabilities: type[Any] | None = None,
        capability_codec: PayloadCodec[Any] | None = None,
    ):
        _validate_request_presence(self, request)
        self.identity = job_identity or JobIdentity(name=name, version=version)

        self.request_type = request
        self.result_type = result
        self.event_type = event
        self.capability_type = capabilities
        self.capability_codec: PayloadCodec[Any] | None = capability_codec
        if self.capability_codec is None and capabilities is not None:
            self.capability_codec = cast(
                PayloadCodec[Any],
                registry.get_payload_codec(capabilities),
            )

        self.request_codec: PayloadCodec[ReqT] | None = request_codec
        if self.request_codec is None and request is not None:
            self.request_codec = cast(PayloadCodec[ReqT], registry.get_payload_codec(request))
        self.result_codec: PayloadCodec[FinalT] | None = result_codec
        if self.result_codec is None and result is not None:
            self.result_codec = cast(PayloadCodec[FinalT], registry.get_payload_codec(result))
        self.event_codec: PayloadCodec[InterT] | None = event_codec
        if self.event_codec is None and event is not None:
            self.event_codec = cast(PayloadCodec[InterT], registry.get_payload_codec(event))

        self._frozen = True

    def __setattr__(self, name: str, value: Any) -> None:
        if getattr(self, "_frozen", False):
            raise AttributeError("Job definitions are immutable")
        object.__setattr__(self, name, value)

    @property
    def name(self) -> str:
        return self.identity.name

    @property
    def version(self) -> str | None:
        return self.identity.version

    @property
    def canonical_name(self) -> str:
        return self.identity.canonical_name

    def encode_request(self, request: ReqT) -> bytes:
        if self.request_codec is None:
            if request is not None:
                raise TypeError(f"Job {self} does not accept a request")
            return b""
        return self.request_codec.encode(request)

    def decode_request(self, data: bytes) -> ReqT:
        if self.request_codec is None:
            if data:
                raise ValueError(f"Job {self} does not accept a request payload")
            return None  # type: ignore[return-value]
        return self.request_codec.decode(data)

    def encode_result(self, result: FinalT) -> bytes:
        if self.result_codec is None:
            if result is not None:
                raise TypeError(f"Job {self} does not return a result")
            return b""
        return self.result_codec.encode(result)

    def decode_result(self, data: bytes) -> FinalT:
        if self.result_codec is None:
            if data:
                raise ValueError(f"Job {self} does not have a result payload")
            return None  # type: ignore[return-value]
        return self.result_codec.decode(data)

    def encode_event(self, event: InterT) -> bytes:
        if self.event_codec is None:
            raise TypeError(f"Job {self} does not declare intermediate events")
        return self.event_codec.encode(event)

    def decode_event(self, data: bytes) -> InterT:
        if self.event_codec is None:
            raise TypeError(f"Job {self} does not declare intermediate events")
        return self.event_codec.decode(data)

    def __call__(self, request: ReqT) -> FinalT:
        raise TypeError("Jobs must be invoked through a JobClient")

    def __str__(self) -> str:
        return self.canonical_name

    def __repr__(self) -> str:
        return f"Job({self.canonical_name})"

    handler = job_handler_descriptor


class RequestJob[ReqT, FinalT, InterT, **ConstructorP](
    Job[ReqT, FinalT, InterT],
):
    """Job specialization that retains the request constructor parameter specification."""


class CapabilityRequestJob[ReqT, FinalT, InterT, **ConstructorP, CapT](
    RequestJob[ReqT, FinalT, InterT, ConstructorP],
):
    """Request job with a declared application capability type."""

    capability_type: type[CapT] | None
    capability_codec: PayloadCodec[CapT] | None


class NoRequestJob[FinalT, InterT](Job[None, FinalT, InterT]):
    """Job specialization for contracts without a request payload."""


class CapabilityNoRequestJob[FinalT, InterT, CapT](NoRequestJob[FinalT, InterT]):
    """No-request job with a declared application capability type."""

    capability_type: type[CapT] | None
    capability_codec: PayloadCodec[CapT] | None


def _validate_request_presence(job: Job[Any, Any, Any], request: Any) -> None:
    if isinstance(job, RequestJob) and request is None:
        raise TypeError("RequestJob requires a request type")
    if isinstance(job, NoRequestJob) and request is not None:
        raise TypeError("NoRequestJob cannot declare a request type")
