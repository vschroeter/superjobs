from __future__ import annotations

import pytest
from faststream.nats import NatsBroker

from superjobs.resources import (
    AcquisitionOutcome,
    NatsResourceAdmissionStore,
    ReleaseOutcome,
    ResourceAdmission,
    ResourceRequirement,
    TerminationConfirmation,
    WorkerIncarnation,
)
from tests.test_resource_admission_memory import _intent

pytestmark = pytest.mark.nats


@pytest.mark.asyncio
async def test_nats_cas_contention_and_recovery(nats_url: str, nats_queue_config) -> None:
    prefix = nats_queue_config.subject_prefix
    broker_a = NatsBroker(nats_url, connect_timeout=2, max_reconnect_attempts=0)
    broker_b = NatsBroker(nats_url, connect_timeout=2, max_reconnect_attempts=0)
    await broker_a.start()
    await broker_b.start()
    try:
        store_a = NatsResourceAdmissionStore(broker_a, subject_prefix=prefix)
        store_b = NatsResourceAdmissionStore(broker_b, subject_prefix=prefix)
        left = ResourceAdmission(store_a)
        right = ResourceAdmission(store_b)
        key = "host-nats.gpu0"
        await left.declare_capacity(ResourceRequirement(resource_key=key, units=1))

        first = await left.try_acquire(
            _intent(worker_id="worker-a", execution_id="exec-nats-1", resource_key=key),
        )
        second = await right.try_acquire(
            _intent(worker_id="worker-b", execution_id="exec-nats-2", resource_key=key),
        )
        assert first.outcome is AcquisitionOutcome.ACQUIRED
        assert second.outcome is AcquisitionOutcome.CONTENTION
        assert first.owner is not None

        recovery = await right.resolve_terminated_incarnation(
            TerminationConfirmation(
                incarnation=WorkerIncarnation(worker_id="worker-a", incarnation_id="inc-1"),
                confirmed=True,
                evidence="supervisor:worker-a",
            ),
        )
        assert recovery.outcome.value == "resolved"

        successor = await right.try_acquire(
            _intent(worker_id="worker-b", execution_id="exec-nats-3", resource_key=key),
        )
        assert successor.outcome is AcquisitionOutcome.ACQUIRED

        stale = await left.release_acknowledged_cleanup(first.owner)
        assert stale.outcome is ReleaseOutcome.STALE
    finally:
        await broker_a.stop()
        await broker_b.stop()
