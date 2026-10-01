from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
import math
from typing import Any

from superjobs.jobs.submit_options import SubmitOptions
from superjobs.registry.registry import construct_payload_for_type

_LEGACY_SUBMIT_OPTION_KEYS = frozenset(
    {
        "idempotency_key",
        "job_id",
        "timeout",
        "deadline",
        "caller_scope",
    },
)


class SubmitShapeError(ValueError):
    """Invalid combination of submit arguments."""


def _matches_request_type(value: Any, request_type: Any) -> bool:
    return isinstance(value, request_type)


def normalise_deadline(deadline: datetime | None) -> datetime | None:
    if deadline is None:
        return None
    if not isinstance(deadline, datetime):
        raise ValueError("deadline must be a datetime")
    if deadline.tzinfo is None:
        raise ValueError("deadline must be timezone-aware")
    return deadline.astimezone(UTC)


def _field_from_options(
    name: str,
    *,
    options_obj: SubmitOptions | None,
    legacy: Mapping[str, Any],
) -> Any:
    legacy_value = legacy.get(name)
    options_value = getattr(options_obj, name) if options_obj is not None else None
    if legacy_value is not None and options_value is not None and legacy_value != options_value:
        raise SubmitShapeError(
            f"Conflicting submission values for {name!r} from SubmitOptions and legacy keywords",
        )
    if legacy_value is not None:
        return legacy_value
    if options_value is not None:
        return options_value
    if options_obj is not None and name == "caller_scope":
        return options_obj.caller_scope
    if name == "caller_scope":
        return "default"
    return None


def normalized_submit_options(
    *,
    options_obj: SubmitOptions | None,
    legacy: Mapping[str, Any],
) -> tuple[str | None, str | None, float | None, datetime | None, str]:
    if options_obj is not None and legacy:
        overlap = _LEGACY_SUBMIT_OPTION_KEYS.intersection(legacy)
        if overlap:
            raise SubmitShapeError(
                "Pass execution options through SubmitOptions or legacy keywords, not both",
            )

    timeout = _field_from_options("timeout", options_obj=options_obj, legacy=legacy)
    if timeout is not None and (
        isinstance(timeout, bool)
        or not isinstance(timeout, (int, float))
        or not math.isfinite(timeout)
        or timeout <= 0
    ):
        raise ValueError("timeout must be a positive finite number")

    idempotency_key = _field_from_options("idempotency_key", options_obj=options_obj, legacy=legacy)
    if idempotency_key is not None and (
        not isinstance(idempotency_key, str) or not idempotency_key
    ):
        raise ValueError("idempotency_key must be a non-empty string")

    job_id = _field_from_options("job_id", options_obj=options_obj, legacy=legacy)
    if job_id is not None and (not isinstance(job_id, str) or not job_id):
        raise ValueError("job_id must be a non-empty string")

    caller_scope = _field_from_options("caller_scope", options_obj=options_obj, legacy=legacy)
    if not isinstance(caller_scope, str) or not caller_scope:
        raise ValueError("caller_scope must be a non-empty string")

    deadline = normalise_deadline(
        _field_from_options("deadline", options_obj=options_obj, legacy=legacy),
    )
    return idempotency_key, job_id, timeout, deadline, caller_scope


def _partition_kwargs(
    kwargs: Mapping[str, Any],
    *,
    explicit_object_form: bool,
) -> tuple[SubmitOptions | None, dict[str, Any], dict[str, Any]]:
    remaining = dict(kwargs)
    options_obj: SubmitOptions | None = None
    if explicit_object_form:
        options_obj = remaining.pop("options", None)
        if options_obj is not None and not isinstance(options_obj, SubmitOptions):
            raise SubmitShapeError("options must be a SubmitOptions instance")
    # Constructor keywords belong to the request contract. Execution options
    # are positional in this form, so no business field name is reserved.
    if not explicit_object_form:
        return options_obj, {}, remaining

    legacy: dict[str, Any] = {}
    request_fields: dict[str, Any] = {}
    for key, value in remaining.items():
        if key in _LEGACY_SUBMIT_OPTION_KEYS:
            legacy[key] = value
        else:
            request_fields[key] = value
    return options_obj, legacy, request_fields


def resolve_request_submission(
    job: Any,
    args: tuple[Any, ...],
    kwargs: Mapping[str, Any],
) -> tuple[Any, SubmitOptions | None, dict[str, Any]]:
    if job.request_type is None:
        options_obj, legacy, request_fields = _partition_kwargs(
            kwargs,
            explicit_object_form=True,
        )
        if request_fields:
            raise SubmitShapeError("No-request jobs do not accept request fields")
        if not args:
            return None, options_obj, legacy
        if len(args) == 1 and args[0] is None:
            return None, options_obj, legacy
        if len(args) == 1 and isinstance(args[0], SubmitOptions):
            if options_obj is not None and options_obj is not args[0]:
                raise SubmitShapeError("Conflicting SubmitOptions positional and keyword values")
            return None, args[0], legacy
        raise SubmitShapeError("No-request jobs accept submit() or an optional positional SubmitOptions only")

    request_type = job.request_type
    positional_options: SubmitOptions | None = None
    explicit_request: Any | None = None
    remaining_args = args
    if args:
        if isinstance(args[0], SubmitOptions):
            positional_options = args[0]
            remaining_args = args[1:]
        elif args[0] is None:
            raise SubmitShapeError("Request-bearing jobs require a request payload")
        elif _matches_request_type(args[0], request_type):
            explicit_request = args[0]
            remaining_args = args[1:]
        else:
            raise SubmitShapeError(
                "Positional request construction is not supported; pass a request object or use keywords",
            )

    if positional_options is not None:
        options_obj, legacy, request_fields = _partition_kwargs(
            kwargs,
            explicit_object_form=explicit_request is not None,
        )
        if options_obj is not None and options_obj is not positional_options:
            raise SubmitShapeError("Conflicting SubmitOptions positional and keyword values")
        options_obj = positional_options
    else:
        options_obj, legacy, request_fields = _partition_kwargs(
            kwargs,
            explicit_object_form=explicit_request is not None,
        )

    if remaining_args:
        raise SubmitShapeError("Unexpected positional arguments after the request or SubmitOptions prefix")

    if explicit_request is not None and request_fields:
        raise SubmitShapeError("Cannot mix an explicit request object with constructor keywords")

    if explicit_request is not None:
        return explicit_request, options_obj, legacy

    built = construct_payload_for_type(request_type, **request_fields)
    return built, options_obj, legacy
