"""A revoked RUNNING invocation must terminate without a waiting-only CAS spin."""

from __future__ import annotations

import asyncio
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest


def _run_child(mode: str, terminated: bool, url: str = "") -> None:
    result = subprocess.run(
        [sys.executable, str(Path(__file__).resolve()), mode, str(int(terminated)), url],
        capture_output=True,
        text=True,
        timeout=12,
    )
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize("mode", ["cancel", "deadline"])
@pytest.mark.parametrize("terminated", [True, False])
def test_memory_running_owner_recovery(mode: str, terminated: bool) -> None:
    _run_child(mode, terminated)


@pytest.mark.nats
@pytest.mark.parametrize("mode", ["cancel", "deadline"])
@pytest.mark.parametrize("terminated", [True, False])
def test_nats_running_owner_recovery(mode: str, terminated: bool, nats_url: str) -> None:
    _run_child(mode, terminated, nats_url)


async def _scenario(mode: str, terminated: bool, url: str) -> None:
    from superjobs import HandlerCatalog, InMemoryTransport, Job, SubmitOptions, SuperJobs
    from superjobs.jobs.execution import JobState
    from superjobs.resources import (
        AdmissionAcquireIntent,
        ResourceAdmission,
        ResourceRequirement,
        TerminationConfirmation,
        WorkerIncarnation,
    )

    broker = None
    if url:
        from faststream.nats import NatsBroker
        from superjobs.transport.implementations.nats import NatsQueueConfig
        from superjobs.transport.nats_backend import NatsJobBackend

        broker = NatsBroker(url)
        await broker.start()
        backend = NatsJobBackend(broker, NatsQueueConfig())
    else:
        backend = InMemoryTransport()
    controller = SuperJobs(transport=backend)
    await controller.start()
    job = Job(f"tests.confirmed-recovery.{uuid4().hex}", result=str)
    options = SubmitOptions(
        deadline=datetime.now(UTC) + timedelta(seconds=2)
        if mode == "deadline" else None
    )
    handle = await controller.client(job).submit(options=options)
    record = await backend.get_execution(job.identity, handle.id)
    assert record is not None
    admission = ResourceAdmission(backend.resource_admission_store)
    key = f"test.gpu.{uuid4().hex}"
    await admission.declare_capacity(ResourceRequirement(resource_key=key, units=1))
    incarnation = WorkerIncarnation(worker_id=f"original-{uuid4().hex}", incarnation_id="1")
    acquired = await admission.try_acquire(
        AdmissionAcquireIntent(
            incarnation=incarnation, invocation_id=uuid4(),
            execution_id=handle.id, resource_key=key,
        )
    )
    assert acquired.owner is not None
    await backend.mark_started(record, 1, admission_owner=acquired.owner)
    if mode == "cancel":
        await handle.cancel()
    else:
        await asyncio.sleep(max(0, (record.deadline - datetime.now(UTC)).total_seconds()) + 0.05)
    if terminated:
        await admission.resolve_terminated_incarnation(
            TerminationConfirmation(
                incarnation=incarnation, confirmed=True,
                evidence="test supervisor confirmed original process-tree exit",
            )
        )
    catalog = HandlerCatalog()

    @catalog.handler(job, resources={key: 1})
    async def forbidden(context):
        raise AssertionError("Cancelled/expired work entered the callback")

    worker = SuperJobs(transport=backend, handlers=catalog, resource_capacities={key: 1})
    await worker.start()
    try:
        # Broken code spins synchronously here, so only the parent process's hard
        # timeout can catch it. An asyncio timeout alone cannot detect starvation.
        await asyncio.sleep(0.3)
        status = await handle.status()
        if terminated:
            expected = JobState.CANCELLED if mode == "cancel" else JobState.FAILED
            assert status.state is expected, status
            assert await admission.execution_owner(handle.id) is None
        else:
            assert status.state is JobState.RUNNING, status
            assert await admission.execution_owner(handle.id) == acquired.owner
    finally:
        await worker.stop(graceful=False)
        await controller.stop(graceful=False)
        if broker:
            await broker.stop()


if __name__ == "__main__":
    asyncio.run(_scenario(sys.argv[1], bool(int(sys.argv[2])), sys.argv[3]))
