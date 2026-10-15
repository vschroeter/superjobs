"""Controller-side helpers for jobs.resources snapshot and termination resolution."""

from __future__ import annotations

from faststream.nats import NatsBroker

from superjobs import HandlerCatalog, SuperJobs
from superjobs.resources import TerminationConfirmation
from superjobs.resources.models import AdmissionOwner

from constants import RESOURCE_KEY
from queue_config import queue_config_for_run


def superjobs_for_admission(nats_url: str, run_id: str) -> SuperJobs:
    queue_config = queue_config_for_run(run_id)
    broker = NatsBroker(nats_url, connect_timeout=5)
    catalog = HandlerCatalog()
    jobs = SuperJobs(
        broker=broker,
        queue_config=queue_config,
        handlers=catalog,
        discovery=False,
        resource_capacities={RESOURCE_KEY: 1},
    )
    _ = jobs.resources
    return jobs


async def snapshot_owners(jobs: SuperJobs) -> list[AdmissionOwner]:
    admission = jobs.resources
    snap = await admission.snapshot()
    owners: list[AdmissionOwner] = []
    for resource in snap.resources:
        if resource.owner is not None:
            owners.append(resource.owner)
    return owners


async def resolve_terminated_owner(
    jobs: SuperJobs,
    owner: AdmissionOwner,
    *,
    evidence: str,
) -> str:
    admission = jobs.resources
    confirmation = TerminationConfirmation(
        incarnation=owner.incarnation,
        confirmed=True,
        evidence=evidence,
    )
    result = await admission.resolve_terminated_incarnation(confirmation)
    return result.outcome.value


async def stale_release_attempt(jobs: SuperJobs, owner: AdmissionOwner) -> str:
    admission = jobs.resources
    result = await admission.release_resource_after_cleanup(owner)
    return result.outcome.value
