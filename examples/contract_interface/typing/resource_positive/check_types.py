"""Pyright-positive checks for resource admission consumer surfaces (issue #61)."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import assert_type
from uuid import UUID, uuid4

from superjobs import HandlerCatalog, JobContext, SuperJobs
from superjobs.jobs.execution import JobState
from superjobs.resources import (
    AdmissionFencing,
    AdmissionOwner,
    AdmissionSnapshot,
    CapacityDeclaration,
    ResourceRequirement,
    TerminationConfirmation,
    V1_EXCLUSIVE_CAPACITY,
    WaitingExecutionCompleteness,
    WaitingExecutionEntry,
    WaitingExecutionEvidence,
    WaitingExecutionView,
    WorkerIncarnation,
    parse_exclusive_resource_requirement,
    read_waiting_executions_from_broker,
)
from superjobs_contract_example import (
    HEARTBEAT_JOB,
    MANIFEST_WITH_EVENTS_JOB,
    HeartbeatResult,
    ManifestEvent,
    ManifestRequest,
    ManifestResult,
)

DEVICE_KEY = "sample-host.gpu0"
_RESOURCES = {DEVICE_KEY: V1_EXCLUSIVE_CAPACITY}


def _exclusive_requirement() -> None:
    requirement = parse_exclusive_resource_requirement(_RESOURCES)
    assert_type(requirement, ResourceRequirement)
    declaration = CapacityDeclaration(
        resource_key=requirement.resource_key,
        capacity=requirement.units,
    )
    assert_type(declaration, CapacityDeclaration)


def _catalog_decorators_with_resources(catalog: HandlerCatalog) -> None:
    @catalog.handler(MANIFEST_WITH_EVENTS_JOB, resources=_RESOURCES)
    async def async_request(
        request: ManifestRequest,
        context: JobContext[ManifestEvent],
    ) -> ManifestResult:
        context.acknowledge_resource_cleanup()
        return ManifestResult(revision=request.device_id)

    @catalog.handler(MANIFEST_WITH_EVENTS_JOB, resources=_RESOURCES)
    def sync_request(
        request: ManifestRequest,
        context: JobContext[ManifestEvent],
    ) -> ManifestResult:
        context.acknowledge_resource_cleanup()
        return ManifestResult(revision=request.device_id)

    @catalog.handler(HEARTBEAT_JOB, resources=_RESOURCES)
    async def async_no_request(context: JobContext[None]) -> HeartbeatResult:
        context.acknowledge_resource_cleanup()
        return HeartbeatResult(ok=True)


def _catalog_register_with_resources(catalog: HandlerCatalog) -> None:
    async def async_request(
        request: ManifestRequest,
        context: JobContext[ManifestEvent],
    ) -> ManifestResult:
        context.acknowledge_resource_cleanup()
        return ManifestResult(revision=request.device_id)

    def sync_request(
        request: ManifestRequest,
        context: JobContext[ManifestEvent],
    ) -> ManifestResult:
        context.acknowledge_resource_cleanup()
        return ManifestResult(revision=request.device_id)

    catalog.register(MANIFEST_WITH_EVENTS_JOB, async_request, resources=_RESOURCES)
    catalog.register(MANIFEST_WITH_EVENTS_JOB, sync_request, resources=_RESOURCES)


def _runtime_decorators_and_register(jobs: SuperJobs) -> None:
    @jobs.handler(MANIFEST_WITH_EVENTS_JOB, resources=_RESOURCES)
    async def async_runtime(
        request: ManifestRequest,
        context: JobContext[ManifestEvent],
    ) -> ManifestResult:
        context.acknowledge_resource_cleanup()
        return ManifestResult(revision=request.device_id)

    @jobs.handler(MANIFEST_WITH_EVENTS_JOB, resources=_RESOURCES)
    def sync_runtime(
        request: ManifestRequest,
        context: JobContext[ManifestEvent],
    ) -> ManifestResult:
        context.acknowledge_resource_cleanup()
        return ManifestResult(revision=request.device_id)

    async def registered(
        request: ManifestRequest,
        context: JobContext[ManifestEvent],
    ) -> ManifestResult:
        context.acknowledge_resource_cleanup()
        return ManifestResult(revision=request.device_id)

    jobs.register(MANIFEST_WITH_EVENTS_JOB, registered, resources=_RESOURCES)


def _runtime_capacity_and_controller_surface() -> None:
    jobs = SuperJobs(resource_capacities=_RESOURCES)
    _ = jobs.resources
    assert_type(jobs.worker_incarnation, WorkerIncarnation)
    _runtime_decorators_and_register(jobs)


def _waiting_execution_observation_types() -> None:
    async def observe(jobs: SuperJobs) -> None:
        view = await jobs.resources.waiting_executions(max_entries=10)
        assert_type(view, WaitingExecutionView)
        assert_type(view.completeness, WaitingExecutionCompleteness)
        if view.entries:
            entry = view.entries[0]
            assert_type(entry, WaitingExecutionEntry)
            assert_type(entry.resource_key, str)
            assert_type(entry.record_state, JobState)
            assert_type(entry.evidence, WaitingExecutionEvidence)
            assert_type(entry.waiting_invocation_id, UUID | None)

    _ = observe


def _fresh_broker_waiting_observation_types(broker: object, queue_config: object) -> None:
    async def observe() -> None:
        view = await read_waiting_executions_from_broker(
            broker,  # type: ignore[arg-type]
            queue_config=queue_config,  # type: ignore[arg-type]
        )
        assert_type(view, WaitingExecutionView)

    _ = observe


def _context_cleanup_api(context: JobContext[ManifestEvent]) -> None:
    context.acknowledge_resource_cleanup()
    context.resource_cleanup_failed("cleanup could not finish")


def _snapshot_and_termination_evidence() -> None:
    now = datetime.now(tz=UTC)
    incarnation = WorkerIncarnation(worker_id="worker-a", incarnation_id="gen-1")
    owner = AdmissionOwner(
        resource_key=DEVICE_KEY,
        incarnation=incarnation,
        invocation_id=uuid4(),
        execution_id="execution-1",
        fencing=AdmissionFencing(generation=1, token="fence-token"),
    )
    confirmation = TerminationConfirmation(
        incarnation=incarnation,
        confirmed=True,
        evidence="supervisor:pid=4242 container=engine-a exited code=0",
    )
    assert_type(owner, AdmissionOwner)
    assert_type(confirmation, TerminationConfirmation)
    snapshot = AdmissionSnapshot(evaluated_at=now, capacities=(), resources=(), execution_claims=())
    assert_type(snapshot, AdmissionSnapshot)
