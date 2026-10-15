"""Resource admission scenario Job identities (producer/controller-importable)."""

from __future__ import annotations

from superjobs import Job
from superjobs_contract_example import ManifestRequest, ManifestResult

from constants import JOB_VERSION


def _job(name: str) -> Job:
    return Job(
        name,
        version=JOB_VERSION,
        request=ManifestRequest,
        result=ManifestResult,
    )


def job_a(run_id: str) -> Job:
    return _job(f"resource_admission.job_a.{run_id}")


def job_b(run_id: str) -> Job:
    return _job(f"resource_admission.job_b.{run_id}")


def job_redelivery(run_id: str) -> Job:
    return _job(f"resource_admission.redelivery.{run_id}")


def job_crash_successor(run_id: str) -> Job:
    return _job(f"resource_admission.crash_successor.{run_id}")


def job_broker_restart(run_id: str) -> Job:
    return _job(f"resource_admission.broker_restart.{run_id}")
