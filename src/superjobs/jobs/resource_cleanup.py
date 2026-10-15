"""Resource cleanup acknowledgement for resource-bound handlers."""

from __future__ import annotations

from superjobs.exceptions.jobs import NonRetryableError


class ResourceCleanupError(NonRetryableError):
    """Raised when a resource-bound handler exits without acknowledging cleanup."""

    code = "resource_cleanup_failed"


class _ResourceCleanupState:
    __slots__ = ("acknowledged", "failed", "failure_reason")

    def __init__(self) -> None:
        self.acknowledged = False
        self.failed = False
        self.failure_reason: str | None = None


_CLEANUP_STATE_ATTR = "_superjobs_resource_cleanup_state"


def init_resource_cleanup_state(context: object) -> None:
    setattr(context, _CLEANUP_STATE_ATTR, _ResourceCleanupState())


def acknowledge_resource_cleanup(context: object) -> None:
    """Confirm that resource-consuming work and cleanup have finished."""
    state = getattr(context, _CLEANUP_STATE_ATTR, None)
    if state is None:
        raise RuntimeError("resource cleanup is not active for this JobContext")
    if state.failed:
        raise ResourceCleanupError(
            "resource cleanup was already marked failed; cannot acknowledge afterward",
        )
    state.acknowledged = True


def resource_cleanup_failed(context: object, reason: str) -> None:
    """Declare that cleanup could not complete; retains resource ownership."""
    state = getattr(context, _CLEANUP_STATE_ATTR, None)
    if state is None:
        raise RuntimeError("resource cleanup is not active for this JobContext")
    if state.acknowledged:
        raise ResourceCleanupError(
            "resource cleanup was already acknowledged; cannot mark failed afterward",
        )
    state.failed = True
    state.failure_reason = reason


def resource_cleanup_acknowledged(context: object) -> bool:
    state = getattr(context, _CLEANUP_STATE_ATTR, None)
    return bool(state is not None and state.acknowledged)


def resource_cleanup_failed_reason(context: object) -> str | None:
    state = getattr(context, _CLEANUP_STATE_ATTR, None)
    if state is None or not state.failed:
        return None
    return state.failure_reason
