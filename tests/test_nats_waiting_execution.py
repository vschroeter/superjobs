from __future__ import annotations

import asyncio

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
        assert view.completeness is WaitingExecutionCompleteness.COMPLETE
        assert {entry.execution_id for entry in view.entries} == {second.id}

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
        await runtime.transport.mark_started(record, 1, target_state=JobState.WAITING_FOR_RESOURCE)
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
        assert await asyncio.wait_for(first.result(), 8) == "a"
        assert await asyncio.wait_for(second.result(), 8) == "b"
    finally:
        await runtime.stop(graceful=False)
        if worker.returncode is None:
            worker.terminate()
        await asyncio.wait_for(worker.wait(), timeout=5)
