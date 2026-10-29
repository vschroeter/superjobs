"""Read-only, bounded broker-wide observation of resource-waiting executions.

Scan limits bound time, returned entries, and how many completion records are
examined during observation. NATS backends enumerate completion KV keys through a
short-lived ``watchall`` snapshot capped by ``max_scan_keys``; in-memory backends
iterate stored execution records without materializing the full map first.
"""

from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from superjobs.jobs.execution import JobState
from superjobs.resources.errors import (
    UnsupportedWaitingExecutionBackendError,
    WaitingExecutionObservationError,
    WaitingExecutionUnavailableError,
)
from superjobs.resources.models import (
    WaitingExecutionCompleteness,
    WaitingExecutionEntry,
    WaitingExecutionView,
)

if TYPE_CHECKING:
    from superjobs.transport.backend import ExecutionRecord

DEFAULT_MAX_WAITING_EXECUTION_ENTRIES = 256
DEFAULT_MAX_WAITING_EXECUTION_SCAN_KEYS = 4096
DEFAULT_WAITING_EXECUTION_READ_TIMEOUT = timedelta(seconds=15)


def supports_waiting_execution_observation(backend: Any) -> bool:
    """Return whether the backend can serve read-only waiting-execution views."""
    if getattr(backend, "resource_admission_store", None) is None:
        return False
    return callable(getattr(backend, "read_waiting_execution_view", None))


def _configured_read_timeout(backend: Any) -> timedelta:
    timeout = getattr(backend, "waiting_execution_read_timeout", None)
    if isinstance(timeout, timedelta):
        return timeout
    return DEFAULT_WAITING_EXECUTION_READ_TIMEOUT


def _configured_max_entries(backend: Any, *, max_entries: int | None) -> int:
    if max_entries is not None:
        if type(max_entries) is not int or isinstance(max_entries, bool):
            raise ValueError("max_entries must be a positive integer")
        if max_entries < 1:
            raise ValueError("max_entries must be positive")
        return max_entries
    configured = getattr(backend, "max_waiting_execution_entries", None)
    if isinstance(configured, int) and not isinstance(configured, bool) and configured > 0:
        return configured
    return DEFAULT_MAX_WAITING_EXECUTION_ENTRIES


def _configured_max_scan_keys(backend: Any) -> int:
    configured = getattr(backend, "max_waiting_execution_scan_keys", None)
    if isinstance(configured, int) and not isinstance(configured, bool) and configured > 0:
        return configured
    return DEFAULT_MAX_WAITING_EXECUTION_SCAN_KEYS


def _entry_from_record(record: ExecutionRecord) -> WaitingExecutionEntry:
    return WaitingExecutionEntry(
        execution_id=record.job_id,
        job=record.identity,
        created_at=record.created_at,
    )


def _sort_key(entry: WaitingExecutionEntry) -> tuple[datetime, str, str, str | None]:
    created = entry.created_at or datetime.min.replace(tzinfo=UTC)
    return (created, entry.job.name, entry.job.version or "", entry.execution_id)


def build_waiting_execution_view(
    *,
    evaluated_at: datetime,
    records: list[ExecutionRecord],
    max_entries: int,
    partial: bool,
    scan_truncated: bool = False,
) -> WaitingExecutionView:
    waiting = [
        _entry_from_record(record)
        for record in records
        if record.state is JobState.WAITING_FOR_RESOURCE
    ]
    waiting.sort(key=_sort_key)
    completeness = WaitingExecutionCompleteness.COMPLETE
    if scan_truncated or len(waiting) > max_entries:
        if len(waiting) > max_entries:
            waiting = waiting[:max_entries]
        completeness = WaitingExecutionCompleteness.TRUNCATED
    if partial:
        # A read gap is more informative than truncation alone; both mean incomplete.
        completeness = WaitingExecutionCompleteness.PARTIAL
    return WaitingExecutionView(
        evaluated_at=evaluated_at,
        completeness=completeness,
        entries=tuple(waiting),
    )


async def read_waiting_executions(
    backend: Any,
    *,
    max_entries: int | None = None,
) -> WaitingExecutionView:
    """Return a bounded view of persisted ``waiting_for_resource`` completion records.

    Callers must use a transport that has already completed normal ``start()``;
    that startup path may provision JetStream buckets. This API performs read-only
    observation against an already-provisioned completion store and does not offer
    non-provisioning inspection of a fresh broker on its own.

    Entries come only from durable completion records in ``waiting_for_resource``
    state. Blocked deliveries still in ``pending`` without that persisted state,
    and admission execution claims alone are excluded. The requested resource key is
    not stored in these waiting records. The scan is not atomic and may race writes.

    ``WaitingExecutionCompleteness`` describes coverage of matching records within
    the bounded scan, not every execution actually waiting for a resource.

    Observation is read-only: it does not provision JetStream buckets, mutate
    admission state, or write completion records.
    """
    if not supports_waiting_execution_observation(backend):
        raise UnsupportedWaitingExecutionBackendError(
            f"{type(backend).__name__} does not support waiting-execution observation",
        )
    limit = _configured_max_entries(backend, max_entries=max_entries)
    evaluated_at = datetime.now(tz=UTC)
    timeout = _configured_read_timeout(backend)
    scan_deadline_monotonic = time.monotonic() + timeout.total_seconds()
    max_scan_keys = _configured_max_scan_keys(backend)
    try:
        return await asyncio.wait_for(
            backend.read_waiting_execution_view(
                evaluated_at=evaluated_at,
                max_entries=limit,
                max_scan_keys=max_scan_keys,
                scan_deadline_monotonic=scan_deadline_monotonic,
            ),
            timeout=timeout.total_seconds(),
        )
    except asyncio.CancelledError:
        raise
    except asyncio.TimeoutError as error:
        raise WaitingExecutionUnavailableError(
            "Waiting-execution observation timed out",
        ) from error
    except WaitingExecutionObservationError:
        raise
    except Exception as error:
        raise WaitingExecutionUnavailableError(
            "Waiting-execution observation failed",
        ) from error
