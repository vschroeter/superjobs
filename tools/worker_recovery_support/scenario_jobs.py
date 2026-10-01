"""Recovery scenario Job identities (producer-importable, no handler code)."""

from __future__ import annotations

from superjobs import Job
from superjobs_contract_example import ManifestRequest, ManifestResult


def before_completion_job(run_id: str) -> Job:
    return Job(
        f"worker_recovery.before_completion.{run_id}",
        version="v1",
        request=ManifestRequest,
        result=ManifestResult,
    )


def after_completion_job(run_id: str) -> Job:
    return Job(
        f"worker_recovery.after_completion.{run_id}",
        version="v1",
        request=ManifestRequest,
        result=ManifestResult,
    )


def after_retry_publication_job(run_id: str) -> Job:
    return Job(
        f"worker_recovery.after_retry_publication.{run_id}",
        version="v1",
        request=ManifestRequest,
        result=ManifestResult,
    )
