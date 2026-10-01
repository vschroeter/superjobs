"""Scenario Job identities shared by producer and worker (no handler code)."""

from __future__ import annotations

from superjobs import Job
from superjobs_contract_example import ManifestRequest, ManifestResult


def failure_job(run_id: str) -> Job:
    return Job(
        f"cross_program.verify.failure.{run_id}",
        version="v1",
        request=ManifestRequest,
        result=ManifestResult,
    )


def cancel_job(run_id: str) -> Job:
    return Job(
        f"cross_program.verify.cancel.{run_id}",
        version="v1",
        request=ManifestRequest,
        result=ManifestResult,
    )


def retry_job(run_id: str) -> Job:
    return Job(
        f"cross_program.verify.retry.{run_id}",
        version="v1",
        request=ManifestRequest,
        result=ManifestResult,
    )


def crash_job(run_id: str) -> Job:
    return Job(
        f"cross_program.verify.crash.{run_id}",
        version="v1",
        request=ManifestRequest,
        result=ManifestResult,
    )
