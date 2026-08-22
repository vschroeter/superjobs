from __future__ import annotations

import functools
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

from superjobs.jobs.job_context import JobContext
from superjobs.jobs.job_handler import JobHandler
from superjobs.jobs.job_identity import JobIdentity
from superjobs.payload.codec.payloadcodec import PayloadCodec
from superjobs.registry import registry

if TYPE_CHECKING:
    from superjobs.superjobs import SuperJobs

# TODO: Split up Job (only definition), JobHandler and JobClient


class Job[ReqT: Any | None, FinalT: Any | None, InterT: Any | None]:
    def __init__(
        self,
        name: str,
        request: type[ReqT] | None = None,
        result: type[FinalT] | None = None,
        event: type[InterT] | None = None,
        *,
        version: str | None = None,
        namespace: str | None = None,
        job_identity: JobIdentity | None = None,
        request_codec: PayloadCodec[ReqT] | None = None,
        result_codec: PayloadCodec[FinalT] | None = None,
        event_codec: PayloadCodec[InterT] | None = None,
    ):
        self.identity = job_identity or JobIdentity(name=name, namespace=namespace, version=version)

        self.request_codec = request_codec or registry.get_payload_codec(request)
        self.result_codec = result_codec or registry.get_payload_codec(result)
        self.event_codec = event_codec or registry.get_payload_codec(event)

        # self._handler: Callable[[ReqT, JobContext], FinalT] | None = None
        self._handler: JobHandler[ReqT, FinalT, InterT] | None = None

        self._jobs: SuperJobs | None = None

    # region PROPERTIES

    @property
    def name(self) -> str:
        return self.identity.name

    @property
    def namespace(self) -> str | None:
        return self.identity.namespace

    @property
    def version(self) -> str | None:
        return self.identity.version

    @property
    def canonical_name(self) -> str:
        return self.identity.canonical_name

    # region DATA HANDLING
    def encode_request(self, request: ReqT) -> bytes:
        return self.request_codec.encode(request)

    # def create_out_message()

    # def decode_request(self, data: bytes) -> ReqT:

    # region DECORATORS

    def handler(
        self,
        jobs: SuperJobs,
        fct: Callable[
            [ReqT, JobContext[ReqT, FinalT, InterT]],
            FinalT | Awaitable[FinalT],
        ],
    ) -> Callable[
        [ReqT, JobContext[ReqT, FinalT, InterT]],
        FinalT | Awaitable[FinalT],
    ]:
        self._jobs = jobs

        @functools.wraps(fct)
        def wrapper(*args, **kwargs):
            return fct(*args, **kwargs)

        self._handler = JobHandler(self, wrapper, transport=jobs.transport)

        jobs._register_handler(self._handler)

        return wrapper

    # region HELPERS

    def __call__(self, request: ReqT) -> FinalT:
        pass

    def __str__(self) -> str:
        return self.canonical_name

    def __repr__(self) -> str:
        return f"Job({self.canonical_name})"
