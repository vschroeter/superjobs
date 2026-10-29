"""Read-only, bounded broker-wide observation of persisted resource-waiting records.

Scan limits bound time, returned entries, and how many completion records are
examined during observation. Listed entries are historical completion evidence
only; observation cannot prove that a writing worker is still alive.
"""

from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from nats.js.errors import APIError, BucketNotFoundError, NotFoundError
from nats.js.kv import KeyValue

from superjobs.discovery.errors import DiscoveryUnavailableError
from superjobs.discovery.nats import NatsDiscoveryBackend
from superjobs.jobs.execution import JobState
from superjobs.resources.errors import (
    ResourceStoreUnavailableError,
    UnsupportedWaitingExecutionBackendError,
    WaitingExecutionObservationError,
    WaitingExecutionUnavailableError,
)
from superjobs.resources.models import (
    WaitingExecutionCompleteness,
    WaitingExecutionEntry,
    WaitingExecutionEvidence,
    WaitingExecutionView,
)
from superjobs.resources.nats import NatsResourceAdmissionStore, kv_bucket_verified_empty
from superjobs.resources.keys import validate_resource_key

if TYPE_CHECKING:
    from faststream.nats import NatsBroker

    from superjobs.transport.backend import ExecutionRecord
    from superjobs.transport.implementations.nats import NatsQueueConfig

DEFAULT_MAX_WAITING_EXECUTION_ENTRIES = 256
DEFAULT_MAX_WAITING_EXECUTION_SCAN_KEYS = 4096
DEFAULT_WAITING_EXECUTION_READ_TIMEOUT = timedelta(seconds=15)

_WAITING_OBSERVATION_STATES = frozenset({JobState.WAITING_FOR_RESOURCE})


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


def clear_waiting_resource_publication(record: ExecutionRecord) -> None:
    record.waiting_resource_key = None
    record.waiting_resource_invocation_id = None
    record.waiting_resource_attempt = None


def waiting_pin_blocks_new_publication(
    record: ExecutionRecord,
    *,
    attempt: int,
    waiting_resource_invocation_id: UUID | None,
) -> bool:
    """Return whether an active waiting pin rejects a conflicting publication."""
    if waiting_resource_invocation_id is None:
        return record.waiting_resource_invocation_id is not None
    pinned_invocation = record.waiting_resource_invocation_id
    if pinned_invocation is None:
        return False
    if pinned_invocation == waiting_resource_invocation_id:
        return False
    pinned_attempt = record.waiting_resource_attempt
    if pinned_attempt is None:
        pinned_attempt = record.attempt
    return attempt <= pinned_attempt


def record_matches_waiting_observation(record: ExecutionRecord) -> bool:
    if record.state is not JobState.WAITING_FOR_RESOURCE:
        return False
    if record.waiting_resource_key is None:
        return False
    try:
        validate_resource_key(record.waiting_resource_key)
    except Exception:
        return False
    return True


def _entry_from_record(record: ExecutionRecord) -> WaitingExecutionEntry | None:
    key = record.waiting_resource_key
    if key is None or record.state not in _WAITING_OBSERVATION_STATES:
        return None
    try:
        validate_resource_key(key)
    except Exception:
        return None
    return WaitingExecutionEntry(
        execution_id=record.job_id,
        job=record.identity,
        resource_key=key,
        evidence=WaitingExecutionEvidence.PERSISTED_HISTORICAL,
        waiting_invocation_id=record.waiting_resource_invocation_id,
        record_state=record.state,
        created_at=record.created_at,
    )


def _sort_key(entry: WaitingExecutionEntry) -> tuple[datetime, str, str, str, str]:
    created = entry.created_at or datetime.min.replace(tzinfo=UTC)
    return (
        created,
        entry.resource_key,
        entry.job.name,
        entry.job.version or "",
        entry.execution_id,
    )


def build_waiting_execution_view(
    *,
    evaluated_at: datetime,
    records: list[ExecutionRecord],
    max_entries: int,
    partial: bool,
    scan_truncated: bool = False,
    legacy_waiting_without_key: bool = False,
    unverified_liveness: bool = False,
) -> WaitingExecutionView:
    waiting: list[WaitingExecutionEntry] = []
    for record in records:
        entry = _entry_from_record(record)
        if entry is not None:
            waiting.append(entry)
    waiting.sort(key=_sort_key)
    completeness = WaitingExecutionCompleteness.COMPLETE
    if scan_truncated or len(waiting) > max_entries:
        if len(waiting) > max_entries:
            waiting = waiting[:max_entries]
        completeness = WaitingExecutionCompleteness.TRUNCATED
    if partial or legacy_waiting_without_key or unverified_liveness:
        completeness = WaitingExecutionCompleteness.PARTIAL
    return WaitingExecutionView(
        evaluated_at=evaluated_at,
        completeness=completeness,
        entries=tuple(waiting),
    )


async def scan_completion_kv_for_waiting_view(
    *,
    completion_kv: KeyValue | None,
    discovery_backend: NatsDiscoveryBackend,
    evaluated_at: datetime,
    max_entries: int,
    max_scan_keys: int,
    scan_deadline_monotonic: float,
    record_from_wire: Any,
    unpack: Any,
) -> WaitingExecutionView:
    if completion_kv is None:
        raise WaitingExecutionUnavailableError(
            "Completion store is missing for waiting-execution observation",
        )
    try:
        verified_empty = await kv_bucket_verified_empty(completion_kv)
    except ResourceStoreUnavailableError as error:
        raise WaitingExecutionUnavailableError(str(error)) from error

    if verified_empty:
        return build_waiting_execution_view(
            evaluated_at=evaluated_at,
            records=[],
            max_entries=max_entries,
            partial=False,
            scan_truncated=False,
        )

    try:
        scanned_keys, scan_truncated = await discovery_backend.bounded_kv_meta_key_scan(
            completion_kv,
            max_keys=max_scan_keys,
            deadline_monotonic=scan_deadline_monotonic,
        )
    except DiscoveryUnavailableError as error:
        raise WaitingExecutionUnavailableError(str(error)) from error
    except APIError as error:
        raise WaitingExecutionUnavailableError(
            "Unable to list completion records for waiting-execution observation",
        ) from error

    if not scanned_keys and not scan_truncated:
        try:
            if not await kv_bucket_verified_empty(completion_kv):
                raise WaitingExecutionUnavailableError(
                    "Completion key scan was empty while the bucket retains records",
                )
        except ResourceStoreUnavailableError as error:
            raise WaitingExecutionUnavailableError(str(error)) from error

    matched_records: list[ExecutionRecord] = []
    partial = False
    legacy_waiting_without_key = False
    unverified_liveness = False
    for key in sorted(scanned_keys):
        if time.monotonic() >= scan_deadline_monotonic:
            scan_truncated = True
            break
        try:
            entry = await completion_kv.get(key)
        except NotFoundError:
            partial = True
            continue
        except APIError as error:
            raise WaitingExecutionUnavailableError(
                "Unable to read completion records for waiting-execution observation",
            ) from error
        if entry.value is None:
            partial = True
            continue
        try:
            record = record_from_wire(unpack(entry.value))
        except Exception:
            partial = True
            continue
        if record.state in {JobState.WAITING_FOR_RESOURCE, JobState.PENDING}:
            unverified_liveness = True
        if record.state is JobState.WAITING_FOR_RESOURCE and record.waiting_resource_key is None:
            legacy_waiting_without_key = True
            continue
        if record_matches_waiting_observation(record):
            matched_records.append(record)

    return build_waiting_execution_view(
        evaluated_at=evaluated_at,
        records=matched_records,
        max_entries=max_entries,
        partial=partial,
        scan_truncated=scan_truncated,
        legacy_waiting_without_key=legacy_waiting_without_key,
        unverified_liveness=unverified_liveness,
    )


async def _open_completion_kv_readonly(
    broker: NatsBroker,
    bucket: str,
) -> KeyValue | None:
    try:
        connection = broker.connection
    except Exception as error:
        raise WaitingExecutionUnavailableError(
            "NATS connection is not available",
        ) from error
    if connection is None:
        raise WaitingExecutionUnavailableError("NATS connection is not available")
    js = connection.jetstream()
    try:
        return await js.key_value(bucket)
    except (BucketNotFoundError, NotFoundError):
        return None


class NatsFreshWaitingExecutionBackend:
    """Read-only waiting observation against existing JetStream buckets only."""

    def __init__(
        self,
        broker: NatsBroker,
        *,
        queue_config: NatsQueueConfig,
    ) -> None:
        self.broker = broker
        self.queue_config = queue_config
        self.resource_admission_store = NatsResourceAdmissionStore(
            broker,
            subject_prefix=queue_config.subject_prefix,
            provision=False,
        )
        self.discovery_backend = NatsDiscoveryBackend(
            broker,
            subject_prefix=queue_config.subject_prefix,
            discovery_provision=False,
        )
        self.started = True

    async def read_waiting_execution_view(
        self,
        *,
        evaluated_at: datetime,
        max_entries: int,
        max_scan_keys: int,
        scan_deadline_monotonic: float,
    ) -> WaitingExecutionView:
        completion_kv = await _open_completion_kv_readonly(
            self.broker,
            self.queue_config.completion_bucket,
        )
        from superjobs.transport import nats_backend as backend_module

        return await scan_completion_kv_for_waiting_view(
            completion_kv=completion_kv,
            discovery_backend=self.discovery_backend,
            evaluated_at=evaluated_at,
            max_entries=max_entries,
            max_scan_keys=max_scan_keys,
            scan_deadline_monotonic=scan_deadline_monotonic,
            record_from_wire=backend_module._record_from_wire,
            unpack=backend_module._unpack,
        )


async def read_waiting_executions(
    backend: Any,
    *,
    max_entries: int | None = None,
) -> WaitingExecutionView:
    """Return a bounded view of persisted resource-waiting completion records.

    Entries come from durable ``waiting_for_resource`` completion records that
    carry a requested resource key. Observation cannot prove that the writing
    worker is still alive; matching attempt metadata is historical evidence only.
    Any scanned ``waiting_for_resource`` or ``pending`` record marks the view
    ``partial`` because liveness is unverified. Legacy waiting records without a
    persisted key are omitted and also mark ``partial``.

    For started runtimes, normal ``start()`` may have provisioned JetStream
    buckets already. Use :func:`read_waiting_executions_from_broker` for a fresh
    broker connection that opens existing buckets read-only and never provisions
    admission or completion stores. When the completion bucket is absent,
    :class:`WaitingExecutionUnavailableError` is raised (a missing bucket does
    not prove an unused namespace).

    ``evaluated_at`` is when the scan started; stored evidence may be older;
    concurrent writers can add, remove, or change records during and after
    the scan before callers consume the view.
    ``WaitingExecutionCompleteness`` describes bounded scan coverage, not live
    resource contention. Observation is read-only: it does not mutate admission
    state or write completion records.
    """
    if not supports_waiting_execution_observation(backend):
        raise UnsupportedWaitingExecutionBackendError(
            f"{type(backend).__name__} does not support waiting-execution observation",
        )
    if hasattr(backend, "started") and not getattr(backend, "started", False):
        raise WaitingExecutionUnavailableError(
            "Waiting-execution observation requires a started transport",
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


async def read_waiting_executions_from_broker(
    broker: NatsBroker,
    *,
    queue_config: NatsQueueConfig,
    max_entries: int | None = None,
) -> WaitingExecutionView:
    """Observe waiting work through a fresh broker connection without provisioning."""
    backend = NatsFreshWaitingExecutionBackend(broker, queue_config=queue_config)
    return await read_waiting_executions(backend, max_entries=max_entries)
