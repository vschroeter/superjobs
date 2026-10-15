"""Pyright-negative checks: deliberate resource admission type errors."""

from __future__ import annotations

from uuid import uuid4

from superjobs import HandlerCatalog, JobContext, SuperJobs
from superjobs.resources import (
    AdmissionFencing,
    AdmissionOwner,
    CapacityDeclaration,
    ResourceRequirement,
    WorkerIncarnation,
    parse_exclusive_resource_requirement,
)
from superjobs_contract_example import (
    MANIFEST_WITH_EVENTS_JOB,
    ManifestEvent,
    ManifestRequest,
    ManifestResult,
)

DEVICE_KEY = "sample-host.gpu0"
_VALID_RESOURCES = {DEVICE_KEY: 1}


def _wrong_exclusive_requirement_mapping() -> None:
    parse_exclusive_resource_requirement({"host.gpu0": "1"})  # expect: reportArgumentType


def _wrong_requirement_units_type() -> None:
    ResourceRequirement(resource_key=DEVICE_KEY, units="1")  # expect: reportArgumentType


def _wrong_requirement_key_type() -> None:
    ResourceRequirement(resource_key=1, units=1)  # expect: reportArgumentType


def _wrong_capacity_units_type() -> None:
    CapacityDeclaration(resource_key=DEVICE_KEY, capacity="1")  # expect: reportArgumentType


def _wrong_owner_resource_key() -> None:
    AdmissionOwner(
        resource_key=1,  # expect: reportArgumentType
        incarnation=WorkerIncarnation(worker_id="w", incarnation_id="i"),
        invocation_id=uuid4(),
        execution_id="execution-1",
        fencing=AdmissionFencing(generation=1, token="t"),
    )


def _wrong_catalog_decorator_resource_units(catalog: HandlerCatalog) -> None:
    @catalog.handler(MANIFEST_WITH_EVENTS_JOB, resources={DEVICE_KEY: "1"})  # expect: reportCallIssue, reportArgumentType
    async def handler(
        request: ManifestRequest,
        context: JobContext[ManifestEvent],
    ) -> ManifestResult:
        return ManifestResult(revision=request.device_id)


def _wrong_catalog_decorator_callback(catalog: HandlerCatalog) -> None:
    @catalog.handler(MANIFEST_WITH_EVENTS_JOB, resources=_VALID_RESOURCES)  # expect: reportArgumentType
    async def incompatible_request(
        request: str,
        context: JobContext[ManifestEvent],
    ) -> ManifestResult:
        return ManifestResult(revision=request)


def _wrong_catalog_register_callback(catalog: HandlerCatalog) -> None:
    async def incompatible_request(
        request: str,
        context: JobContext[ManifestEvent],
    ) -> ManifestResult:
        return ManifestResult(revision=request)

    catalog.register(MANIFEST_WITH_EVENTS_JOB, incompatible_request, resources=_VALID_RESOURCES)  # expect: reportCallIssue, reportArgumentType


def _wrong_runtime_decorator_callback(jobs: SuperJobs) -> None:
    @jobs.handler(MANIFEST_WITH_EVENTS_JOB, resources=_VALID_RESOURCES)  # expect: reportArgumentType
    async def incompatible_result(
        request: ManifestRequest,
        context: JobContext[ManifestEvent],
    ) -> str:
        return "wrong"


def _wrong_context_cleanup_reason(
    context: JobContext[ManifestEvent],
) -> None:
    context.resource_cleanup_failed(1)  # expect: reportArgumentType
