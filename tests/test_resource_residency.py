"""A completed job can retain a fenced resource for consecutive work."""

import asyncio
from uuid import uuid4

import pytest
from superjobs import HandlerCatalog, Job, JobContext, SuperJobs
from superjobs.resources import (
    AcquisitionOutcome,
    AdmissionAcquireIntent,
    InMemoryResourceAdmissionStore,
    ResourceAdmission,
    ResourceRequirement,
    WorkerIncarnation,
)


async def test_retained_owner_handoff_never_exposes_available_device():
    admission = ResourceAdmission(InMemoryResourceAdmissionStore())
    await admission.declare_capacity(ResourceRequirement(resource_key="gpu.test", units=1))
    worker = WorkerIncarnation(worker_id="worker", incarnation_id="one")
    first = AdmissionAcquireIntent(
        incarnation=worker, invocation_id=uuid4(), execution_id="first", resource_key="gpu.test"
    )
    owner = (await admission.try_acquire(first)).owner
    assert owner is not None
    await admission.retain_resource(owner, "model.one")
    await admission.finalize_execution_claim(owner)
    snapshot = await admission.snapshot()
    assert not snapshot.execution_claims
    assert snapshot.resources[0].owner == owner
    assert snapshot.resources[0].retained
    second = first.model_copy(update={"execution_id": "second", "invocation_id": uuid4()})
    result = await admission.acquire_retained(second, owner)
    assert result.outcome is AcquisitionOutcome.ACQUIRED
    assert result.owner.fencing.generation > owner.fencing.generation
    assert (await admission.release_retained_after_cleanup(owner)).outcome.value == "stale"
    await admission.release_acknowledged_cleanup(result.owner)


async def test_competing_acquisition_requests_release_without_taking_ownership():
    admission = ResourceAdmission(InMemoryResourceAdmissionStore())
    await admission.declare_capacity(ResourceRequirement(resource_key="gpu.test", units=1))
    first = AdmissionAcquireIntent(
        incarnation=WorkerIncarnation(worker_id="a", incarnation_id="one"),
        invocation_id=uuid4(),
        execution_id="first",
        resource_key="gpu.test",
    )
    owner = (await admission.try_acquire(first)).owner
    await admission.retain_resource(owner, "model.one")
    await admission.finalize_execution_claim(owner)
    other = first.model_copy(
        update={
            "incarnation": WorkerIncarnation(worker_id="b", incarnation_id="one"),
            "execution_id": "other",
            "invocation_id": uuid4(),
        }
    )
    assert (await admission.try_acquire(other)).outcome is AcquisitionOutcome.CONTENTION
    record = (await admission.snapshot()).resources[0]
    assert record.owner == owner and record.release_requested
    assert (
        await admission.acquire_retained(other, owner)
    ).outcome is not AcquisitionOutcome.ACQUIRED
    await admission.release_retained_after_cleanup(owner)
    assert (await admission.try_acquire(other)).outcome is AcquisitionOutcome.ACQUIRED


async def test_runtime_reuses_model_until_idle_timeout_then_releases():
    job = Job("test.residency.reuse", result=bool)
    catalog = HandlerCatalog()
    loaded = False
    loads = 0
    unloads = 0

    async def unload():
        nonlocal loaded, unloads
        loaded = False
        unloads += 1

    @catalog.handler(job, resources={"gpu.test": 1})
    async def run(context: JobContext[None]) -> bool:
        nonlocal loaded, loads
        reused = context.resource_reused("model.one")
        if not reused:
            assert not loaded
            loaded = True
            loads += 1
        context.retain_resource("model.one", unload, idle_timeout=0.15)
        return reused

    runtime = SuperJobs(handlers=catalog, resource_capacities={"gpu.test": 1})
    async with runtime:
        assert not await asyncio.wait_for((await runtime.client(job).submit()).result(), 2)
        assert await asyncio.wait_for((await runtime.client(job).submit()).result(), 2)
        assert loads == 1 and unloads == 0 and loaded
        snapshot = await runtime.resources.snapshot()
        assert snapshot.resources[0].retained
        assert not snapshot.execution_claims
        for _ in range(100):
            if not loaded:
                break
            await asyncio.sleep(0.01)
        assert not loaded and unloads == 1
        assert (await runtime.resources.snapshot()).resources[0].owner is None


async def test_shutdown_unloads_retained_model_before_transport_stops():
    job = Job("test.residency.shutdown", result=str)
    catalog = HandlerCatalog()
    unloaded = asyncio.Event()

    async def unload():
        unloaded.set()

    @catalog.handler(job, resources={"gpu.test": 1})
    async def run(context: JobContext[None]) -> str:
        context.retain_resource("model.one", unload, idle_timeout=30)
        return "done"

    runtime = SuperJobs(handlers=catalog, resource_capacities={"gpu.test": 1})
    await runtime.start()
    assert await asyncio.wait_for((await runtime.client(job).submit()).result(), 2) == "done"
    await asyncio.wait_for(runtime.stop(), 2)
    assert unloaded.is_set()


async def test_unload_failure_keeps_unavailable_owner():
    job = Job("test.residency.failure", result=str)
    catalog = HandlerCatalog()

    async def unload():
        raise RuntimeError("native model still resident")

    @catalog.handler(job, resources={"gpu.test": 1})
    async def run(context: JobContext[None]) -> str:
        context.retain_resource("model.one", unload, idle_timeout=0.03)
        return "done"

    runtime = SuperJobs(handlers=catalog, resource_capacities={"gpu.test": 1})
    await runtime.start()
    try:
        assert await asyncio.wait_for((await runtime.client(job).submit()).result(), 2) == "done"
        for _ in range(100):
            record = (await runtime.resources.snapshot()).resources[0]
            if record.availability.value == "unavailable":
                break
            await asyncio.sleep(0.01)
        assert record.availability.value == "unavailable"
        assert record.owner is not None
    finally:
        with pytest.raises(RuntimeError, match="native model still resident"):
            await runtime.stop()


async def test_prepare_resource_switches_model_with_cleanup_under_same_owner():
    job = Job("test.residency.switch", request=str, result=bool)
    catalog = HandlerCatalog()
    events = []
    current = None

    async def unload():
        nonlocal current
        events.append(("unload", current))
        current = None

    @catalog.handler(job, resources={"gpu.test": 1})
    async def run(model: str, context: JobContext[str]) -> bool:
        async def load():
            nonlocal current
            assert current is None
            current = model
            events.append(("load", model))

        return await context.prepare_resource(model, load, unload, idle_timeout=30)

    async with SuperJobs(handlers=catalog, resource_capacities={"gpu.test": 1}) as runtime:
        client = runtime.client(job)
        assert not await (await client.submit("a")).result(wait_timeout=2)
        assert await (await client.submit("a")).result(wait_timeout=2)
        assert not await (await client.submit("b")).result(wait_timeout=2)
        assert events == [("load", "a"), ("unload", "a"), ("load", "b")]
    assert events[-1] == ("unload", "b")


async def test_competing_job_gets_resource_before_residency_timeout():
    first = Job("test.residency.holder", result=str)
    second = Job("test.residency.contender", result=str)
    catalog = HandlerCatalog()
    loaded = False

    async def unload():
        nonlocal loaded
        loaded = False

    @catalog.handler(first, resources={"gpu.test": 1})
    async def holder(context: JobContext[None]) -> str:
        nonlocal loaded
        loaded = True
        context.retain_resource("model", unload, idle_timeout=30)
        return "held"

    @catalog.handler(second, resources={"gpu.test": 1})
    async def contender(context: JobContext[None]) -> str:
        assert not loaded
        return "exclusive"

    async with SuperJobs(handlers=catalog, resource_capacities={"gpu.test": 1}) as runtime:
        assert await (await runtime.client(first).submit()).result(wait_timeout=2) == "held"
        assert await (await runtime.client(second).submit()).result(wait_timeout=2) == "exclusive"


async def test_failed_call_unloads_residency_without_losing_exclusive_cleanup():
    from superjobs.exceptions.jobs import JobFailedError

    job = Job("test.residency.callerror", result=str)
    catalog = HandlerCatalog()
    loaded = False

    async def load():
        nonlocal loaded
        loaded = True

    async def unload():
        nonlocal loaded
        loaded = False

    @catalog.handler(job, resources={"gpu.test": 1})
    async def run(context: JobContext[None]) -> str:
        await context.prepare_resource("model", load, unload)
        raise ValueError("call failed")

    async with SuperJobs(handlers=catalog, resource_capacities={"gpu.test": 1}) as runtime:
        with pytest.raises(JobFailedError, match="call failed"):
            await (await runtime.client(job).submit()).result(wait_timeout=2)
        assert not loaded
        assert (await runtime.resources.snapshot()).resources[0].owner is None


@pytest.mark.nats
async def test_nats_separate_runtimes_release_idle_owner_to_competing_job(
    nats_broker_factory, nats_queue_config
):
    first = Job("test.residency.nats.holder", result=bool)
    second = Job("test.residency.nats.contender", result=str)
    a, b = HandlerCatalog(), HandlerCatalog()
    loaded = False

    async def unload():
        nonlocal loaded
        loaded = False

    @a.handler(first, resources={"gpu.residency-test": 1})
    async def holder(context: JobContext[None]) -> bool:
        nonlocal loaded
        reused = context.resource_reused("model")
        loaded = True
        context.retain_resource("model", unload, idle_timeout=30)
        return reused

    @b.handler(second, resources={"gpu.residency-test": 1})
    async def contender(context: JobContext[None]) -> str:
        assert not loaded
        return "exclusive"

    left = SuperJobs(
        broker=nats_broker_factory(),
        queue_config=nats_queue_config,
        handlers=a,
        resource_capacities={"gpu.residency-test": 1},
        discovery=False,
    )
    right = SuperJobs(
        broker=nats_broker_factory(), queue_config=nats_queue_config, handlers=b, discovery=False
    )
    async with left, right:
        assert not await (await left.client(first).submit()).result(wait_timeout=10)
        assert await (await left.client(first).submit()).result(wait_timeout=10)
        snapshot = await right.resources.snapshot()
        assert snapshot.resources[0].retained
        assert not snapshot.execution_claims
        assert await (await right.client(second).submit()).result(wait_timeout=10) == "exclusive"


async def test_handoff_lost_cas_reply_reconciles_same_intent():
    from superjobs.resources import ResourcePartialAcquisitionError

    class LostReply(InMemoryResourceAdmissionStore):
        lose = True

        async def compare_and_set_resource(self, record, **kwargs):
            result = await super().compare_and_set_resource(record, **kwargs)
            if self.lose and record.owner and record.owner.execution_id == "second":
                self.lose = False
                raise ConnectionError("CAS committed, reply lost")
            return result

    admission = ResourceAdmission(LostReply())
    await admission.declare_capacity(ResourceRequirement(resource_key="gpu.test", units=1))
    intent = AdmissionAcquireIntent(
        incarnation=WorkerIncarnation(worker_id="a", incarnation_id="one"),
        invocation_id=uuid4(),
        execution_id="first",
        resource_key="gpu.test",
    )
    owner = (await admission.try_acquire(intent)).owner
    await admission.retain_resource(owner, "model")
    await admission.finalize_execution_claim(owner)
    next_intent = intent.model_copy(update={"execution_id": "second", "invocation_id": uuid4()})
    with pytest.raises(ResourcePartialAcquisitionError):
        await admission.acquire_retained(next_intent, owner)
    result = await admission.acquire_retained(next_intent, owner)
    assert result.outcome is AcquisitionOutcome.ACQUIRED
    assert (await admission.snapshot()).resources[0].owner == result.owner


async def test_cancel_drains_live_call_before_unloading_resident_state():
    from superjobs.exceptions.jobs import JobCancelledError

    job = Job("test.residency.cancel", result=str)
    catalog = HandlerCatalog()
    entered, finish, unloaded = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def prepare():
        pass

    async def cleanup():
        assert finish.is_set()
        unloaded.set()

    @catalog.handler(job, resources={"gpu.test": 1})
    async def run(context: JobContext[None]) -> str:
        await context.prepare_resource("model", prepare, cleanup)
        entered.set()
        await finish.wait()
        return "drained"

    async with SuperJobs(handlers=catalog, resource_capacities={"gpu.test": 1}) as runtime:
        handle = await runtime.client(job).submit()
        await asyncio.wait_for(entered.wait(), 2)
        await handle.cancel()
        await asyncio.sleep(0.05)
        assert not unloaded.is_set()
        assert (await runtime.resources.snapshot()).resources[0].owner is not None
        finish.set()
        with pytest.raises(JobCancelledError):
            await handle.result(wait_timeout=2)
        assert unloaded.is_set()
        assert (await runtime.resources.snapshot()).resources[0].owner is None
