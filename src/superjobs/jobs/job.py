from __future__ import annotations

from typing import Any

from superjobs.jobs.handler_decorators import job_handler_descriptor
from superjobs.jobs.job_identity import JobIdentity
from superjobs.payload.codec.payloadcodec import PayloadCodec
from superjobs.registry import registry


class Job[ReqT: Any | None, FinalT: Any | None, InterT: Any | None]:
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
    ):
        self.identity = job_identity or JobIdentity(name=name, version=version)

        self.request_type = request
        self.result_type = result
        self.event_type = event

        self.request_codec = request_codec or (
            registry.get_payload_codec(request) if request is not None else None
        )
        self.result_codec = result_codec or (
            registry.get_payload_codec(result) if result is not None else None
        )
        self.event_codec = event_codec or (
            registry.get_payload_codec(event) if event is not None else None
        )

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
