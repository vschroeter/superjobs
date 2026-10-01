"""Idle-outage scenario Job identities (producer-importable, no handler code)."""

from __future__ import annotations

from superjobs import Job
from superjobs_contract_example import ManifestRequest, ManifestResult

from protocol import JOB_VERSION


def baseline_job(run_id: str) -> Job:
    return Job(
        f"idle_outage.baseline.{run_id}",
        version=JOB_VERSION,
        request=ManifestRequest,
        result=ManifestResult,
    )


def post_outage_job(run_id: str) -> Job:
    return Job(
        f"idle_outage.post_outage.{run_id}",
        version=JOB_VERSION,
        request=ManifestRequest,
        result=ManifestResult,
    )
