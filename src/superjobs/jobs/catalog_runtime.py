"""Materialize catalog bindings onto a SuperJobs runtime (issue #46)."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractAsyncContextManager, AsyncExitStack
from typing import Any

from superjobs.jobs.handler_binding import validate_handler_job_association, validate_handler_signature
from superjobs.jobs.handler_catalog import HandlerBinding
from superjobs.jobs.job import Job
from superjobs.jobs.job_context import ObservationPolicy
from superjobs.jobs.job_handler import JobHandler
from superjobs.jobs.retry_policy import RetryPolicy
from superjobs.transport.backend import JobBackend


class InvalidProviderHandlerError(TypeError):
    """Raised when a provider yields a non-callable handler."""


def binding_handler_options(
    binding: HandlerBinding,
    *,
    default_observation_policy: ObservationPolicy,
) -> dict[str, Any]:
    return {
        "concurrency": binding.concurrency,
        "retry": binding.retry,
        "observation_policy": binding.observation_policy or default_observation_policy,
        "heartbeat_interval": binding.heartbeat_interval,
    }


def create_job_handler(
    binding: HandlerBinding,
    callback: Callable[..., Any],
    *,
    backend: JobBackend,
    default_observation_policy: ObservationPolicy,
) -> JobHandler[Any, Any, Any]:
    validate_handler_job_association(binding.job, callback)
    validate_handler_signature(binding.job, callback)
    options = binding_handler_options(
        binding,
        default_observation_policy=default_observation_policy,
    )
    return JobHandler(
        binding.job,
        callback,
        backend=backend,
        concurrency=options["concurrency"],
        retry=options["retry"],
        observation_policy=options["observation_policy"],
        heartbeat_interval=options["heartbeat_interval"],
    )


def ensure_provider_callback(
    job: Job[Any, Any, Any],
    yielded: object,
) -> Callable[..., Any]:
    if not callable(yielded):
        raise InvalidProviderHandlerError(f"provider for {job} must yield a callable handler")
    callback = yielded
    validate_handler_job_association(job, callback)
    validate_handler_signature(job, callback)
    return callback


async def enter_provider_callback(
    binding: HandlerBinding,
    stack: AsyncExitStack,
) -> Callable[..., Any]:
    if binding.provider is None:
        raise RuntimeError("enter_provider_callback requires a provider binding")
    entered = await stack.enter_async_context(binding.provider())
    return ensure_provider_callback(binding.job, entered)


def register_job_on_backend(transport: JobBackend, job: Job[Any, Any, Any]) -> None:
    register_job = getattr(transport, "register_job", None)
    if register_job is not None:
        register_job(job)


async def close_provider_stack(stack: AsyncExitStack | None) -> BaseException | None:
    if stack is None:
        return None
    try:
        await stack.aclose()
    except BaseException as error:
        return error
    return None
