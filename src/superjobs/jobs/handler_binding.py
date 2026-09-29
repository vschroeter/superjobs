from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Any, TypeVar

SUPERJOBS_HANDLER_JOB_ATTR = "__superjobs_handler_job__"

HandlerCallbackT = TypeVar("HandlerCallbackT", bound=Callable[..., Any])


def get_marked_job(callback: Callable[..., Any]) -> Any | None:
    return getattr(callback, SUPERJOBS_HANDLER_JOB_ATTR, None)


def attach_job_marker(job: Any, callback: HandlerCallbackT) -> HandlerCallbackT:
    existing = get_marked_job(callback)
    if existing is not None and existing is not job:
        raise ValueError(
            f"Handler is already associated with {existing}, cannot associate with {job}",
        )
    setattr(callback, SUPERJOBS_HANDLER_JOB_ATTR, job)
    return callback


def ensure_marked_job(callback: Callable[..., Any]) -> Any:
    job = get_marked_job(callback)
    if job is None:
        raise TypeError(
            "Handler must be decorated with @job.handler before jobs.register(handler)",
        )
    return job


def validate_handler_job_association(job: Any, callback: Callable[..., Any]) -> None:
    marked = get_marked_job(callback)
    if marked is not None and marked is not job:
        raise ValueError(
            f"Handler is already associated with {marked}, cannot register for {job}",
        )


def validate_handler_signature(job: Any, callback: Callable[..., Any]) -> None:
    signature = inspect.signature(callback)
    try:
        if job.request_type is not None:
            signature.bind(object(), object())
        else:
            signature.bind(object())
    except TypeError as error:
        raise TypeError(
            f"Handler signature for {job} cannot be invoked with the mandatory "
            f"context arguments: {error}",
        ) from error
