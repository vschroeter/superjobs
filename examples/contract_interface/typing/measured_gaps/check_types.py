"""Documented typing limits after the producer-interface slice."""

from __future__ import annotations

from typing import reveal_type

from superjobs import Job, SuperJobs
from superjobs_contract_example import (
    MANIFEST_WITH_EVENTS_JOB,
    ManifestEvent,
    ManifestRequest,
    ManifestResult,
)


def _widened_job_erases_constructor_keywords(jobs: SuperJobs) -> None:
    """Widening removes constructor keywords; handler registration is rejected in negative fixtures."""

    def accept(job: Job[ManifestRequest, ManifestResult, ManifestEvent]) -> None:
        reveal_type(jobs.client(job))
        # A client of this base Job accepts only an explicit request object.
        # The specialized RequestJob also carries the constructor ParamSpec.

    accept(MANIFEST_WITH_EVENTS_JOB)
