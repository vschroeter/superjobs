from __future__ import annotations

import asyncio

from uuid import uuid4

import pytest

from superjobs import HandlerCatalog, JobState, NoRequestJob, SuperJobs
from superjobs.jobs.resource_cleanup import acknowledge_resource_cleanup
from superjobs.resources import WaitingExecutionCompleteness

RESOURCE_KEY = "device.gpu0"

job_a = NoRequestJob("tests.nats.waiting.job_a", version="v1", result=str)
job_b = NoRequestJob("tests.nats.waiting.job_b", version="v1", result=str)

pytestmark = pytest.mark.nats


@pytest.mark.asyncio
async def test_nats_waiting_execution_view_during_contention(
    nats_broker_factory,
    nats_queue_config,
) -> None:
    entered = asyncio.Event()
    release = asyncio.Event()

    catalog_a = HandlerCatalog()
    catalog_b = HandlerCatalog()

    @catalog_a.handler(job_a, resources={RESOURCE_KEY: 1})
    async def handle_a(context) -> str:
        entered.set()
        await asyncio.wait_for(release.wait(), timeout=10.0)
        acknowledge_resource_cleanup(context)
        return "a"

    @catalog_b.handler(job_b, resources={RESOURCE_KEY: 1})
    async def handle_b(context) -> str:
        await asyncio.sleep(0.05)
        acknowledge_resource_cleanup(context)
        return "b"

    runtime_a = SuperJobs(
        broker=nats_broker_factory(),
        queue_config=nats_queue_config,
        handlers=catalog_a,
        resource_capacities={RESOURCE_KEY: 1},
        discovery=False,
    )
    runtime_b = SuperJobs(
        broker=nats_broker_factory(),
        queue_config=nats_queue_config,
        handlers=catalog_b,
        resource_capacities={RESOURCE_KEY: 1},
        discovery=False,
    )
    await runtime_a.start()
    await runtime_b.start()
    try:
        first = await runtime_a.client(job_a).submit()
        second = await runtime_b.client(job_b).submit()
        await asyncio.wait_for(entered.wait(), timeout=10.0)

        view = None
        for _ in range(100):
            view = await runtime_a.resources.waiting_executions()
            if view.entries:
                break
            await asyncio.sleep(0.05)
        assert view is not None
        assert view.completeness is WaitingExecutionCompleteness.PARTIAL
        assert {entry.execution_id for entry in view.entries} == {second.id}
        assert view.entries[0].resource_key == RESOURCE_KEY

        record = await runtime_a.transport.get_execution(job_b.identity, second.id)
        assert record is not None
        assert record.state is JobState.WAITING_FOR_RESOURCE

        release.set()
        await asyncio.wait_for(first.result(), timeout=15.0)
        await asyncio.wait_for(second.result(), timeout=15.0)
        assert (await runtime_a.resources.waiting_executions()).entries == ()
    finally:
        await runtime_a.stop(graceful=False)
        await runtime_b.stop(graceful=False)

@pytest.mark.asyncio
async def test_observation_does_not_change_persistent_state_or_leave_consumers(
    nats_broker_factory, nats_queue_config,
) -> None:
    runtime = SuperJobs(
        broker=nats_broker_factory(), queue_config=nats_queue_config, discovery=False,
    )
    await runtime.start()
    try:
        handle = await runtime.client(job_a).submit()
        record = await runtime.transport.get_execution(job_a.identity, handle.id)
        assert record is not None
        await runtime.transport.mark_started(
            record,
            1,
            target_state=JobState.WAITING_FOR_RESOURCE,
            waiting_resource_key=RESOURCE_KEY,
            waiting_resource_invocation_id=uuid4(),
        )
        js = runtime.transport.broker.connection.jetstream()
        stream_name = "KV_" + nats_queue_config.completion_bucket

        async def broker_state():
            info = await js.stream_info(stream_name)
            consumers = await js.consumers_info(stream_name)
            return (info.state.messages, info.state.last_seq,
                    sorted(consumer.name for consumer in consumers))

        before = await broker_state()
        result = await runtime.resources.waiting_executions()
        after = await broker_state()
        assert [item.execution_id for item in result.entries] == [handle.id]
        assert before == after
    finally:
        await runtime.stop(graceful=False)

@pytest.mark.asyncio
async def test_waiting_view_across_producer_and_worker_processes(
    nats_url, nats_queue_config, nats_broker_factory, tmp_path,
) -> None:
    import json
    import os
    import sys

    worker_script = tmp_path / "worker.py"
    ready = tmp_path / "ready"
    entered = tmp_path / "entered"
    worker_script.write_text('''
import asyncio, json, os
from pathlib import Path
from faststream.nats import NatsBroker
from superjobs import HandlerCatalog, NoRequestJob, SuperJobs
from superjobs.transport.implementations.nats import NatsQueueConfig
job_a = NoRequestJob("tests.nats.waiting.job_a", version="v1", result=str)
job_b = NoRequestJob("tests.nats.waiting.job_b", version="v1", result=str)
catalog = HandlerCatalog()
@catalog.handler(job_a, resources={"device.gpu0": 1})
async def first(context):
    Path(os.environ["ENTERED"]).write_text("entered")
    await asyncio.sleep(3)
    context.acknowledge_resource_cleanup()
    return "a"
@catalog.handler(job_b, resources={"device.gpu0": 1})
async def second(context):
    context.acknowledge_resource_cleanup()
    return "b"
async def main():
    jobs = SuperJobs(broker=NatsBroker(os.environ["BROKER"]),
        queue_config=NatsQueueConfig(**json.loads(os.environ["QUEUE"])),
        handlers=catalog, resource_capacities={"device.gpu0": 1}, discovery=False)
    await jobs.start()
    Path(os.environ["READY"]).write_text("ready")
    try:
        await asyncio.Event().wait()
    finally:
        await jobs.stop()
asyncio.run(main())
''', encoding="utf-8")
    queue = {name: getattr(nats_queue_config, name) for name in (
        "stream", "observation_stream", "subject_prefix", "completion_bucket", "idempotency_bucket"
    )}
    env = dict(os.environ, BROKER=nats_url, QUEUE=json.dumps(queue), READY=str(ready), ENTERED=str(entered))
    worker = await asyncio.create_subprocess_exec(sys.executable, str(worker_script), env=env)
    runtime = SuperJobs(broker=nats_broker_factory(), queue_config=nats_queue_config, discovery=False)
    try:
        for _ in range(100):
            if ready.exists():
                break
            if worker.returncode is not None:
                pytest.fail(f"worker exited early: {worker.returncode}")
            await asyncio.sleep(0.05)
        assert ready.exists()
        await runtime.start()
        first = await runtime.client(job_a).submit()
        for _ in range(100):
            if entered.exists(): break
            await asyncio.sleep(0.05)
        assert entered.exists()
        second = await runtime.client(job_b).submit()
        for _ in range(100):
            view = await runtime.resources.waiting_executions()
            if any(item.execution_id == second.id for item in view.entries): break
            await asyncio.sleep(0.05)
        assert any(item.execution_id == second.id for item in view.entries)
        assert view.completeness is WaitingExecutionCompleteness.PARTIAL
        assert await asyncio.wait_for(first.result(), 8) == "a"
        assert await asyncio.wait_for(second.result(), 8) == "b"
    finally:
        await runtime.stop(graceful=False)
        if worker.returncode is None:
            worker.terminate()
        await asyncio.wait_for(worker.wait(), timeout=5)


@pytest.mark.asyncio
async def test_nats_pending_without_key_marks_partial(
    nats_broker_factory,
    nats_queue_config,
) -> None:
    runtime = SuperJobs(
        broker=nats_broker_factory(),
        queue_config=nats_queue_config,
        discovery=False,
    )
    await runtime.start()
    try:
        handle = await runtime.client(job_a).submit()
        record = await runtime.transport.get_execution(job_a.identity, handle.id)
        assert record is not None
        assert record.state is JobState.PENDING
        view = await runtime.resources.waiting_executions()
        assert view.entries == ()
        assert view.completeness is WaitingExecutionCompleteness.PARTIAL
    finally:
        await runtime.stop(graceful=False)


@pytest.mark.asyncio
async def test_nats_writer_gone_same_attempt_marks_partial(
    nats_url,
    nats_queue_config,
    nats_broker_factory,
    tmp_path,
) -> None:
    import json
    import os
    import sys

    from superjobs.resources import WaitingExecutionEvidence

    worker_script = tmp_path / "writer_gone_worker.py"
    ready = tmp_path / "ready"
    entered = tmp_path / "entered"
    worker_script.write_text(
        '''
import asyncio, json, os
from pathlib import Path
from faststream.nats import NatsBroker
from superjobs import HandlerCatalog, NoRequestJob, SuperJobs
from superjobs.transport.implementations.nats import NatsQueueConfig
job_a = NoRequestJob("tests.nats.waiting.job_a", version="v1", result=str)
job_b = NoRequestJob("tests.nats.waiting.job_b", version="v1", result=str)
catalog = HandlerCatalog()
@catalog.handler(job_a, resources={"device.gpu0": 1})
async def first(context):
    Path(os.environ["ENTERED"]).write_text("entered")
    await asyncio.Event().wait()
@catalog.handler(job_b, resources={"device.gpu0": 1})
async def second(context):
    await asyncio.sleep(3600)
    context.acknowledge_resource_cleanup()
    return "b"
async def main():
    jobs = SuperJobs(broker=NatsBroker(os.environ["BROKER"]),
        queue_config=NatsQueueConfig(**json.loads(os.environ["QUEUE"])),
        handlers=catalog, resource_capacities={"device.gpu0": 1}, discovery=False)
    await jobs.start()
    Path(os.environ["READY"]).write_text("ready")
    try:
        await asyncio.Event().wait()
    finally:
        await jobs.stop()
asyncio.run(main())
''',
        encoding="utf-8",
    )
    queue = {
        name: getattr(nats_queue_config, name)
        for name in (
            "stream",
            "observation_stream",
            "subject_prefix",
            "completion_bucket",
            "idempotency_bucket",
        )
    }
    env = dict(
        os.environ,
        BROKER=nats_url,
        QUEUE=json.dumps(queue),
        READY=str(ready),
        ENTERED=str(entered),
    )
    worker = await asyncio.create_subprocess_exec(sys.executable, str(worker_script), env=env)
    runtime = SuperJobs(
        broker=nats_broker_factory(),
        queue_config=nats_queue_config,
        discovery=False,
    )
    await runtime.start()
    try:
        for _ in range(100):
            if ready.exists():
                break
            if worker.returncode is not None:
                pytest.fail(f"worker exited early: {worker.returncode}")
            await asyncio.sleep(0.05)
        assert ready.exists()
        await runtime.client(job_a).submit()
        for _ in range(100):
            if entered.exists():
                break
            await asyncio.sleep(0.05)
        assert entered.exists()
        second = await runtime.client(job_b).submit()
        view = None
        for _ in range(100):
            view = await runtime.resources.waiting_executions()
            if any(item.execution_id == second.id for item in view.entries):
                break
            await asyncio.sleep(0.05)
        assert view is not None
        worker.terminate()
        await asyncio.wait_for(worker.wait(), timeout=5)
        record = await runtime.transport.get_execution(job_b.identity, second.id)
        assert record is not None
        assert record.state is JobState.WAITING_FOR_RESOURCE
        assert record.waiting_resource_attempt == record.attempt
        after_kill = await runtime.resources.waiting_executions()
        assert any(item.execution_id == second.id for item in after_kill.entries)
        assert after_kill.entries[0].evidence is WaitingExecutionEvidence.PERSISTED_HISTORICAL
        assert after_kill.completeness is WaitingExecutionCompleteness.PARTIAL
    finally:
        await runtime.stop(graceful=False)
        if worker.returncode is None:
            worker.terminate()
        await asyncio.wait_for(worker.wait(), timeout=5)


@pytest.mark.asyncio
async def test_fresh_broker_read_without_provisioning_missing_store_unavailable(
    nats_url: str,
    nats_queue_config,
) -> None:
    from dataclasses import replace
    from uuid import uuid4

    from faststream.nats import NatsBroker
    from nats.js.errors import NotFoundError

    from superjobs.resources import WaitingExecutionUnavailableError, read_waiting_executions_from_broker

    prefix = f"tests.fresh.wait.{uuid4().hex}"
    queue_config = replace(nats_queue_config, subject_prefix=prefix)
    broker = NatsBroker(nats_url, connect_timeout=2, max_reconnect_attempts=0)
    await broker.start()
    try:
        js = broker.connection.jetstream()
        with pytest.raises(NotFoundError):
            await js.stream_info(f"KV_{queue_config.completion_bucket}")
        with pytest.raises(WaitingExecutionUnavailableError, match="missing"):
            await read_waiting_executions_from_broker(
                broker,
                queue_config=queue_config,
            )
        with pytest.raises(NotFoundError):
            await js.stream_info(f"KV_{queue_config.completion_bucket}")
    finally:
        await broker.stop()


@pytest.mark.asyncio
async def test_fresh_broker_read_verified_empty_completion_bucket_without_mutation(
    nats_broker_factory,
    nats_queue_config,
) -> None:
    from superjobs.resources import WaitingExecutionCompleteness, read_waiting_executions_from_broker

    runtime = SuperJobs(
        broker=nats_broker_factory(),
        queue_config=nats_queue_config,
        discovery=False,
    )
    observer = nats_broker_factory()
    await runtime.start()
    await observer.start()
    try:
        js = runtime.transport.broker.connection.jetstream()
        stream_name = f"KV_{nats_queue_config.completion_bucket}"

        async def completion_stream_state() -> tuple[int, int]:
            info = await js.stream_info(stream_name)
            consumers = await js.consumers_info(stream_name)
            return info.state.messages, len(tuple(consumers))

        before = await completion_stream_state()
        view = await read_waiting_executions_from_broker(
            observer,
            queue_config=nats_queue_config,
        )
        after = await completion_stream_state()
        assert view.entries == ()
        assert view.completeness is WaitingExecutionCompleteness.COMPLETE
        assert before == after
    finally:
        await observer.stop()
        await runtime.stop(graceful=False)


@pytest.mark.asyncio
async def test_fresh_broker_read_observes_persisted_waiting_entry(
    nats_broker_factory,
    nats_queue_config,
) -> None:
    from superjobs.resources import read_waiting_executions_from_broker

    runtime = SuperJobs(
        broker=nats_broker_factory(),
        queue_config=nats_queue_config,
        discovery=False,
    )
    observer = nats_broker_factory()
    await runtime.start()
    await observer.start()
    try:
        handle = await runtime.client(job_a).submit()
        record = await runtime.transport.get_execution(job_a.identity, handle.id)
        assert record is not None
        await runtime.transport.mark_started(
            record,
            1,
            target_state=JobState.WAITING_FOR_RESOURCE,
            waiting_resource_key=RESOURCE_KEY,
            waiting_resource_invocation_id=uuid4(),
        )
        view = await read_waiting_executions_from_broker(
            observer,
            queue_config=nats_queue_config,
        )
        assert [item.execution_id for item in view.entries] == [handle.id]
        assert view.entries[0].resource_key == RESOURCE_KEY
        assert view.completeness is WaitingExecutionCompleteness.PARTIAL
    finally:
        await observer.stop()
        await runtime.stop(graceful=False)
