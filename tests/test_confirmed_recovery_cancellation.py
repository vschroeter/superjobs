"""A revoked RUNNING invocation must terminate without a waiting-only CAS spin."""

from __future__ import annotations

import asyncio
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest

if TYPE_CHECKING:
    from superjobs import JobState, JobStatus

_TERMINAL_POLL_TIMEOUT_S = 4.0
_PROTECTED_OBSERVATION_S = 1.5
_POLL_INTERVAL_S = 0.02


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


async def _ownership_released(admission, execution_id: str, resource_key: str) -> bool:
    from superjobs.resources import ResourceAvailability

    if await admission.execution_owner(execution_id) is not None:
        return False
    snapshot = await admission.snapshot()
    if any(c.execution_id == execution_id for c in snapshot.execution_claims):
        return False
    resource = next(
        (r for r in snapshot.resources if r.resource_key == resource_key),
        None,
    )
    return (
        resource is not None
        and resource.availability is ResourceAvailability.AVAILABLE
        and resource.owner is None
    )


async def _wait_until_settled(
    handle,
    admission,
    *,
    expected_state: JobState,
    expected_code: str,
    execution_id: str,
    resource_key: str,
    timeout: float,
) -> JobStatus:
    from superjobs import JobState

    terminal = frozenset({JobState.COMPLETED, JobState.FAILED, JobState.CANCELLED})
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    last = None
    while loop.time() < deadline:
        last = await handle.status()
        if last.state is expected_state:
            if last.error is None or last.error.code != expected_code:
                raise AssertionError(
                    f"unexpected error for {expected_state!r}: {last}"
                )
            if await _ownership_released(admission, execution_id, resource_key):
                return last
        elif last.state in terminal:
            raise AssertionError(
                f"reached unexpected terminal state {last.state!r} "
                f"(expected {expected_state!r}): {last}"
            )
        await asyncio.sleep(_POLL_INTERVAL_S)
    exec_owner = await admission.execution_owner(execution_id)
    released = await _ownership_released(admission, execution_id, resource_key)
    raise AssertionError(
        f"timed out after {timeout}s waiting for settled {expected_state!r}; "
        f"last status: {last}; execution_owner={exec_owner!r}; "
        f"ownership_released={released}"
    )


async def _scenario(mode: str, terminated: bool, url: str) -> None:
    from superjobs import HandlerCatalog, InMemoryTransport, Job, JobState, SubmitOptions, SuperJobs
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
    callback_entries = 0

    @catalog.handler(job, resources={key: 1})
    async def forbidden(context):
        nonlocal callback_entries
        callback_entries += 1
        raise AssertionError("Cancelled/expired work entered the callback")

    worker = SuperJobs(transport=backend, handlers=catalog, resource_capacities={key: 1})
    await worker.start()
    try:
        # Broken code spins synchronously here, so only the parent process's hard
        # timeout can catch it. An asyncio timeout alone cannot detect starvation.
        expected_code = "cancelled" if mode == "cancel" else "deadline_exceeded"
        if terminated:
            expected_state = JobState.CANCELLED if mode == "cancel" else JobState.FAILED
            await _wait_until_settled(
                handle,
                admission,
                expected_state=expected_state,
                expected_code=expected_code,
                execution_id=handle.id,
                resource_key=key,
                timeout=_TERMINAL_POLL_TIMEOUT_S,
            )
        else:
            terminal = frozenset(
                {JobState.COMPLETED, JobState.FAILED, JobState.CANCELLED}
            )
            loop = asyncio.get_running_loop()
            deadline = loop.time() + _PROTECTED_OBSERVATION_S
            while loop.time() < deadline:
                status = await handle.status()
                if status.state is not JobState.RUNNING:
                    if status.state in terminal:
                        raise AssertionError(
                            f"job became terminal during protected observation: {status}"
                        )
                    raise AssertionError(
                        f"expected RUNNING during observation, got "
                        f"{status.state!r}: {status}"
                    )
                assert await admission.execution_owner(handle.id) == acquired.owner
                snapshot = await admission.snapshot()
                resource = next(
                    (r for r in snapshot.resources if r.resource_key == key),
                    None,
                )
                assert resource is not None and resource.owner == acquired.owner
                await asyncio.sleep(_POLL_INTERVAL_S)
            assert (await handle.status()).state is JobState.RUNNING
            assert await admission.execution_owner(handle.id) == acquired.owner
            snapshot = await admission.snapshot()
            claims = [
                c for c in snapshot.execution_claims if c.execution_id == handle.id
            ]
            assert len(claims) == 1 and claims[0].owner == acquired.owner
        assert callback_entries == 0
    finally:
        await worker.stop(graceful=False)
        await controller.stop(graceful=False)
        if broker:
            await broker.stop()


if __name__ == "__main__":
    asyncio.run(_scenario(sys.argv[1], bool(int(sys.argv[2])), sys.argv[3]))
