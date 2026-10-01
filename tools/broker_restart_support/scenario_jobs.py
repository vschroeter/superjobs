"""Broker-restart scenario Job identities (producer-importable, no handler code)."""

from __future__ import annotations

from superjobs import Job
from superjobs_contract_example import ManifestRequest, ManifestResult
from protocol import JOB_VERSION


def completed_job(run_id: str) -> Job:
    return Job(
        f"broker_restart.completed.{run_id}",
        version=JOB_VERSION,
        request=ManifestRequest,
        result=ManifestResult,
    )


def pending_job(run_id: str) -> Job:
    return Job(
        f"broker_restart.pending.{run_id}",
        version=JOB_VERSION,
        request=ManifestRequest,
        result=ManifestResult,
    )
