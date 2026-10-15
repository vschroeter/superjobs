"""Shared contract for resource-admission examples (producer-safe imports)."""

from __future__ import annotations

from dataclasses import dataclass

from superjobs import Job

# Stable host + physical device identity (caller-selected, engine-independent).
DEVICE_RESOURCE_KEY = "inference-host.gpu0"


@dataclass(frozen=True, slots=True, kw_only=True)
class DeviceWorkRequest:
    device_id: str


@dataclass(frozen=True, slots=True)
class DeviceWorkResult:
    revision: str


DEVICE_WORK_JOB = Job(
    "examples.resource_admission.device_work",
    version="v1",
    request=DeviceWorkRequest,
    result=DeviceWorkResult,
    event=None,
)
