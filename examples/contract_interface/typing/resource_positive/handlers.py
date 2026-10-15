"""Worker handler catalog with exclusive device admission (issue #61)."""

from __future__ import annotations

from superjobs import HandlerCatalog, JobContext
from superjobs.resources import V1_EXCLUSIVE_CAPACITY

from contract import (
    DEVICE_RESOURCE_KEY,
    DEVICE_WORK_JOB,
    DeviceWorkRequest,
    DeviceWorkResult,
)

catalog = HandlerCatalog()


@catalog.handler(
    DEVICE_WORK_JOB,
    resources={DEVICE_RESOURCE_KEY: V1_EXCLUSIVE_CAPACITY},
)
async def run_device_work(
    request: DeviceWorkRequest,
    context: JobContext[None],
) -> DeviceWorkResult:
    # Model load and inference must run only inside this admitted callback.
    model = await _load_model()
    try:
        try:
            revision = await _infer(model, request.device_id)
            return DeviceWorkResult(revision=revision)
        except Exception:
            # Inference failed; still tear down local state, then acknowledge below.
            raise
    finally:
        try:
            await _unload_model(model)
        except Exception as exc:
            context.resource_cleanup_failed(f"model unload failed: {exc}")
            raise
        context.acknowledge_resource_cleanup()


async def _load_model() -> object:
    return object()


async def _infer(model: object, device_id: str) -> str:
    _ = model
    return f"rev-{device_id}"


async def _unload_model(model: object) -> None:
    _ = model
