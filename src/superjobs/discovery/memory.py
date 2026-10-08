from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol

from superjobs.discovery.config import PresenceConfig
from superjobs.discovery.envelope import (
    StoredWorkerRegistration,
    encode_envelope,
    validate_envelope_write,
)
from superjobs.discovery.errors import DiscoveryEnvelopeError, DiscoveryWriteError
from superjobs.discovery.models import RawCapabilities, WorkerRegistrationMetadata, WorkerRegistrationState
from superjobs.job_identity import JobIdentity

RegistrationKey = tuple[str, str, str | None]


def registration_key(worker_id: str, job: JobIdentity) -> RegistrationKey:
    return (worker_id, job.name, job.version)


def job_identity_from_registration_key(key: RegistrationKey) -> JobIdentity:
    return JobIdentity(name=key[1], version=key[2])


@dataclass(frozen=True, slots=True)
class DiscoverySnapshot:
    evaluated_at: datetime
    entries: tuple[tuple[RegistrationKey, bytes], ...]


@dataclass(frozen=True, slots=True)
class _LiveEntry:
    data: bytes
    last_acknowledged_at: datetime


class DiscoveryBackend(Protocol):
    async def read_snapshot(self, *, include_stale: bool = False) -> DiscoverySnapshot: ...

    async def write_registration(
        self,
        registration: StoredWorkerRegistration,
        *,
        max_envelope_bytes: int,
    ) -> None: ...

    async def delete_registration(self, worker_id: str, job: JobIdentity) -> None: ...


Clock = Callable[[], datetime]


def utc_now(clock: Clock | None = None) -> datetime:
    if clock is None:
        return datetime.now(tz=UTC)
    value = clock()
    if value.tzinfo is None:
        raise ValueError("Discovery clock must return timezone-aware UTC datetimes")
    return value.astimezone(UTC)


def _require_timezone_aware_utc(value: datetime, *, field: str) -> datetime:
    if value.tzinfo is None:
        raise ValueError(f"{field} must be timezone-aware UTC datetimes")
    return value.astimezone(UTC)


def _write_error_with_key_context(
    key: RegistrationKey,
    error: DiscoveryWriteError,
) -> DiscoveryWriteError:
    return error.add_context(
        worker_id=key[0],
        job=job_identity_from_registration_key(key),
    )


def _assert_key_matches_registration(
    key: RegistrationKey,
    registration: StoredWorkerRegistration,
) -> None:
    expected = registration_key(registration.worker_id, registration.job)
    if key != expected:
        raise DiscoveryWriteError(
            "Registration key does not match envelope identity",
            worker_id=registration.worker_id,
            job=registration.job,
        )


class InMemoryDiscoveryBackend:
    """Process-local discovery store shared by transports attached to the same instance."""

    def __init__(
        self,
        *,
        config: PresenceConfig | None = None,
        clock: Clock | None = None,
    ) -> None:
        self._config = config or PresenceConfig()
        self._clock = clock or utc_now
        self._lock = asyncio.Lock()
        self._live: dict[RegistrationKey, _LiveEntry] = {}

    @property
    def config(self) -> PresenceConfig:
        return self._config

    def _effective_max_bytes(self, max_envelope_bytes: int) -> int:
        if type(max_envelope_bytes) is not int or isinstance(max_envelope_bytes, bool):
            raise DiscoveryWriteError("max_envelope_bytes must be a positive integer")
        if max_envelope_bytes < 1:
            raise DiscoveryWriteError("max_envelope_bytes must be positive")
        return min(max_envelope_bytes, self._config.max_envelope_bytes)

    def _snapshot_at(self, evaluated_at: datetime) -> DiscoverySnapshot:
        self._purge_retention_locked(evaluated_at)
        live_items = tuple((key, entry.data) for key, entry in self._live.items())
        return DiscoverySnapshot(evaluated_at=evaluated_at, entries=live_items)

    async def read_snapshot(self, *, include_stale: bool = False) -> DiscoverySnapshot:
        async with self._lock:
            evaluated_at = utc_now(self._clock)
            return self._snapshot_at(evaluated_at)

    def _store_validated_bytes(
        self,
        key: RegistrationKey,
        data: bytes,
        registration: StoredWorkerRegistration,
    ) -> None:
        last_ack = registration.last_seen_at.astimezone(UTC)
        previous = self._live.get(key)
        if previous is not None and previous.data == data:
            return
        self._live[key] = _LiveEntry(data=data, last_acknowledged_at=last_ack)

    async def write_registration(
        self,
        registration: StoredWorkerRegistration,
        *,
        max_envelope_bytes: int,
    ) -> None:
        limit = self._effective_max_bytes(max_envelope_bytes)
        encoded = encode_envelope(registration, max_bytes=limit)
        validated = validate_envelope_write(encoded, max_bytes=limit)
        key = registration_key(registration.worker_id, registration.job)
        async with self._lock:
            self._store_validated_bytes(key, encoded, validated)

    async def write_registration_bytes(
        self,
        key: RegistrationKey,
        data: bytes,
        *,
        max_envelope_bytes: int,
    ) -> None:
        limit = self._effective_max_bytes(max_envelope_bytes)
        try:
            registration = validate_envelope_write(data, max_bytes=limit)
        except DiscoveryWriteError as error:
            cause = error.__cause__
            _write_error_with_key_context(key, error)
            raise error from cause
        except DiscoveryEnvelopeError as error:
            enriched = error.add_context(
                worker_id=key[0],
                job=job_identity_from_registration_key(key),
            )
            raise DiscoveryWriteError(
                str(enriched),
                worker_id=enriched.worker_id,
                job=enriched.job,
            ) from enriched
        _assert_key_matches_registration(key, registration)
        async with self._lock:
            self._store_validated_bytes(key, data, registration)

    async def replace_registration_bytes(
        self,
        key: RegistrationKey,
        data: bytes,
        *,
        max_envelope_bytes: int,
    ) -> None:
        limit = self._effective_max_bytes(max_envelope_bytes)
        try:
            registration = validate_envelope_write(data, max_bytes=limit)
        except DiscoveryWriteError as error:
            cause = error.__cause__
            _write_error_with_key_context(key, error)
            raise error from cause
        except DiscoveryEnvelopeError as error:
            enriched = error.add_context(
                worker_id=key[0],
                job=job_identity_from_registration_key(key),
            )
            raise DiscoveryWriteError(
                str(enriched),
                worker_id=enriched.worker_id,
                job=enriched.job,
            ) from enriched
        _assert_key_matches_registration(key, registration)
        async with self._lock:
            if key not in self._live:
                raise DiscoveryWriteError("Cannot replace a missing worker registration")
            self._store_validated_bytes(key, data, registration)

    async def delete_registration(self, worker_id: str, job: JobIdentity) -> None:
        key = registration_key(worker_id, job)
        async with self._lock:
            self._live.pop(key, None)

    def _purge_retention_locked(self, evaluated_at: datetime) -> None:
        retention = self._config.stale_retention
        expired = [
            key
            for key, entry in self._live.items()
            if evaluated_at - entry.last_acknowledged_at >= retention
        ]
        for key in expired:
            del self._live[key]

    def build_registration(
        self,
        *,
        worker_id: str,
        job: JobIdentity,
        state: WorkerRegistrationState,
        registered_at: datetime,
        last_seen_at: datetime,
        expires_at: datetime,
        capabilities: RawCapabilities | None,
        metadata: WorkerRegistrationMetadata | None = None,
    ) -> StoredWorkerRegistration:
        return StoredWorkerRegistration(
            worker_id=worker_id,
            job=job,
            state=state,
            registered_at=_require_timezone_aware_utc(registered_at, field="registered_at"),
            last_seen_at=_require_timezone_aware_utc(last_seen_at, field="last_seen_at"),
            expires_at=_require_timezone_aware_utc(expires_at, field="expires_at"),
            capabilities=capabilities,
            metadata=metadata or WorkerRegistrationMetadata(),
        )

    def lease_expires_at(self, *, last_seen_at: datetime) -> datetime:
        return last_seen_at + self._config.lease_timeout

    def renewal_interval(self) -> timedelta:
        return self._config.renewal_interval
