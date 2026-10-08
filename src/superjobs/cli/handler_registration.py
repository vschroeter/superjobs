"""Shared handler registration for catalog-backed CLI and runtime paths."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import AsyncExitStack
from typing import Any

from superjobs.jobs.catalog_runtime import (
    close_provider_stack,
    create_job_handler,
    enter_provider_callback,
    register_job_on_backend,
)
from superjobs.jobs.handler_catalog import HandlerBinding
from superjobs.jobs.job_handler import JobHandler
from superjobs.superjobs import SuperJobs


def register_callback_binding(
    runtime: SuperJobs,
    binding: HandlerBinding,
    callback: Callable[..., Any],
) -> JobHandler[Any, Any, Any]:
    key = binding.job.canonical_name
    if key in runtime._handlers:
        raise ValueError(f"A handler is already registered for {binding.job}")
    handler = create_job_handler(
        binding,
        callback,
        backend=runtime.transport,
        default_observation_policy=runtime.observation_policy,
    )
    register_job_on_backend(runtime.transport, binding.job)
    runtime._presence.register_handler(binding.job, binding.capabilities)
    runtime._handlers[key] = handler
    if runtime._started:
        runtime._schedule_handler_start(handler, binding.job)
    return handler


async def register_selected_catalog_binding(
    runtime: SuperJobs,
    binding: HandlerBinding,
    *,
    provider_stack: AsyncExitStack | None = None,
) -> AsyncExitStack:
    """Register one catalog binding on an isolated local runtime."""
    stack = provider_stack or AsyncExitStack()
    if binding.callback is not None:
        register_callback_binding(runtime, binding, binding.callback)
        return stack
    if binding.provider is None:
        raise RuntimeError("catalog binding has no callback or provider")
    try:
        callback = await enter_provider_callback(binding, stack)
        register_callback_binding(runtime, binding, callback)
    except BaseException:
        release_error = await close_provider_stack(stack)
        if release_error is not None:
            raise release_error
        raise
    return stack
