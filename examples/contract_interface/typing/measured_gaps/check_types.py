"""Documented typing gaps: compare EXPECTED targets with MEASURED Pyright output."""

from __future__ import annotations

from typing import reveal_type

from superjobs import Job, JobContext, SuperJobs
from superjobs_contract_example import (
    HEARTBEAT_JOB,
    MANIFEST_WITH_EVENTS_JOB,
    HeartbeatResult,
    ManifestEvent,
    ManifestRequest,
    ManifestResult,
)


def _omitted_event_parameter_inference() -> None:
    reveal_type(
        Job(
            "probe.noevent",
            request=ManifestRequest,
            result=ManifestResult,
        ),
    )


def _explicit_register_no_request_shape_static_gap(jobs: SuperJobs) -> None:
    """EXPECTED: ``register`` on ``Job[None, ...]`` rejects a request-bearing callback.

    MEASURED (Pyright 1.1.414, basic): no ``reportArgumentType`` on this call because
    ``Job[ReqT, ...]`` overloads still match when ``ReqT`` is inferred as ``None``.
    Runtime rejects the shape via ``validate_handler_signature`` (see
    ``tests/test_handler_registration.py``).
    """

    async def needs_context_only(
        request: None,
        context: JobContext[None],
    ) -> HeartbeatResult:
        return HeartbeatResult(ok=True)

    jobs.register(HEARTBEAT_JOB, needs_context_only)
