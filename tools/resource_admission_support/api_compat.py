"""Probe installed SuperJobs for resource-admission integration surface."""

from __future__ import annotations

import inspect
import json
from typing import Any

from superjobs import HandlerCatalog, JobState, SuperJobs
from superjobs.jobs.handler_catalog import HandlerCatalog as HandlerCatalogType
from superjobs.transport.in_memory import InMemoryTransport

try:
    from constants import RESOURCE_KEY
except ModuleNotFoundError:  # imported from repository tests
    from tools.resource_admission_support.constants import RESOURCE_KEY


class ApiCompatError(Exception):
    """Installed distribution lacks required resource admission integration."""


def _check_handler_resources(catalog: HandlerCatalogType) -> None:
    sig = inspect.signature(catalog.handler)
    if "resources" not in sig.parameters:
        raise ApiCompatError(
            "HandlerCatalog.handler missing resources= parameter "
            "(issue #61 library integration)",
        )


def _check_superjobs_capacities() -> None:
    sig = inspect.signature(SuperJobs.__init__)
    if "resource_capacities" not in sig.parameters:
        raise ApiCompatError(
            "SuperJobs.__init__ missing resource_capacities= parameter",
        )


def _check_job_state() -> None:
    if not hasattr(JobState, "WAITING_FOR_RESOURCE"):
        raise ApiCompatError("JobState.WAITING_FOR_RESOURCE missing")
    if JobState.WAITING_FOR_RESOURCE.value != "waiting_for_resource":
        raise ApiCompatError(
            "JobState.WAITING_FOR_RESOURCE.value must be 'waiting_for_resource'",
        )


def _check_resources_admission_type() -> list[str]:
    missing: list[str] = []
    try:
        from superjobs.resources import (  # noqa: PLC0415
            AdmissionOwner,
            ResourceAdmission,
            TerminationConfirmation,
            WorkerIncarnation,
        )
    except ImportError as exc:
        raise ApiCompatError(
            f"superjobs.resources public imports missing: {exc}"
        ) from exc
    _ = (AdmissionOwner, ResourceAdmission, TerminationConfirmation, WorkerIncarnation)
    if not hasattr(ResourceAdmission, "snapshot"):
        missing.append("ResourceAdmission.snapshot")
    if not hasattr(ResourceAdmission, "resolve_terminated_incarnation"):
        missing.append("ResourceAdmission.resolve_terminated_incarnation")
    return missing


def _check_instance_resources() -> None:
    jobs = SuperJobs(
        transport=InMemoryTransport(),
        resource_capacities={RESOURCE_KEY: 1},
        discovery=False,
    )
    _ = jobs.resources


def collect_requirements() -> dict[str, Any]:
    errors: list[str] = []
    try:
        _check_handler_resources(HandlerCatalog())
    except ApiCompatError as exc:
        errors.append(str(exc))
    try:
        _check_superjobs_capacities()
    except ApiCompatError as exc:
        errors.append(str(exc))
    try:
        _check_job_state()
    except ApiCompatError as exc:
        errors.append(str(exc))
    try:
        missing = _check_resources_admission_type()
        errors.extend(missing)
    except ApiCompatError as exc:
        errors.append(str(exc))
    try:
        _check_instance_resources()
    except Exception as exc:
        errors.append(f"SuperJobs.resources missing on instance: {exc}")

    return {
        "ok": not errors,
        "errors": errors,
    }


def main() -> int:
    payload = collect_requirements()
    print(json.dumps(payload, indent=2))
    return 0 if payload["ok"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
