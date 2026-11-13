from __future__ import annotations

import asyncio
from datetime import datetime
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

import nats.errors
from nats.js.api import KeyValueConfig, StorageType
from nats.js.errors import (
    APIError,
    BucketNotFoundError,
    KeyNotFoundError,
    KeyWrongLastSequenceError,
    NoKeysError,
)
from nats.js.kv import KeyValue

if TYPE_CHECKING:
    from faststream.nats import NatsBroker

from superjobs.discovery.nats import NatsDiscoveryBackend
from superjobs.resources.config import ResourceAdmissionConfig
from superjobs.resources.errors import (
    ResourceConfigurationError,
    ResourceStoreUnavailableError,
)
from superjobs.resources.keys import (
    admission_bucket_name,
    capacity_kv_key,
    execution_kv_key,
    resource_kv_key,
    revoked_incarnation_kv_key,
)
from superjobs.resources.models import (
    AdmissionOwner,
    AdmissionSnapshot,
    CapacityDeclaration,
    StoredCapacityRecord,
    StoredExecutionClaim,
    StoredResourceRecord,
    StoredRevokedIncarnation,
    WorkerIncarnation,
)
from superjobs.resources.store import CasResult, CasStatus
from superjobs.resources.wire import (
    decode_capacity,
    decode_execution_claim,
    decode_resource,
    decode_revoked_incarnation,
    encode_capacity,
    encode_execution_claim,
    encode_resource,
    encode_revoked_incarnation,
)

_STREAM_ALREADY_EXISTS_CODE = 10058
_KV_CACHE_RETRY_ATTEMPTS = 2
_CAS_RETRY_ATTEMPTS = 8
KV_KEYS_LIST_RETRY_ATTEMPTS = 3
KV_KEYS_LIST_RETRY_DELAY_S = 0.05
_KV_KEYS_LIST_RETRY_ATTEMPTS = KV_KEYS_LIST_RETRY_ATTEMPTS
_KV_KEYS_LIST_RETRY_DELAY_S = KV_KEYS_LIST_RETRY_DELAY_S


async def kv_bucket_verified_empty(kv: KeyValue) -> bool:
    try:
        status = await kv.status()
    except (APIError, nats.errors.Error) as error:
        raise ResourceStoreUnavailableError(
            "Unable to read JetStream KV bucket state",
        ) from error
    return status.values == 0


async def list_kv_keys_with_verified_empty(
    kv: KeyValue,
    *,
    ambiguous_empty_message: str,
    list_failure_message: str = "Unable to list JetStream KV keys",
    status_failure_message: str = "Unable to read JetStream KV bucket state",
    list_keys: Callable[[KeyValue], Awaitable[list[str]]],
) -> tuple[str, ...]:
    """List KV keys, treating ``NoKeysError`` as empty only when the bucket is verified empty.

    The listing returns the full key set; callers apply scan limits afterward.
    This is not a constant-memory iterator over bucket contents.
    """
    for attempt in range(_KV_KEYS_LIST_RETRY_ATTEMPTS):
        try:
            keys = await list_keys(kv)
            if not keys:
                raise NoKeysError
            return tuple(keys)
        except NoKeysError as error:
            try:
                verified_empty = await kv_bucket_verified_empty(kv)
            except ResourceStoreUnavailableError as status_error:
                raise ResourceStoreUnavailableError(
                    status_failure_message,
                ) from status_error
            if verified_empty:
                return ()
            if attempt + 1 >= _KV_KEYS_LIST_RETRY_ATTEMPTS:
                raise ResourceStoreUnavailableError(ambiguous_empty_message) from error
            await asyncio.sleep(_KV_KEYS_LIST_RETRY_DELAY_S)
        except Exception as error:
            raise ResourceStoreUnavailableError(list_failure_message) from error
    raise ResourceStoreUnavailableError(list_failure_message)


def _raise_store_unavailable(operation: str, error: BaseException) -> None:
    raise ResourceStoreUnavailableError(
        f"Resource admission store could not complete {operation}",
    ) from error


def _expected_kv_config(bucket: str, config: ResourceAdmissionConfig) -> KeyValueConfig:
    return KeyValueConfig(
        bucket=bucket,
        history=1,
        storage=StorageType.FILE,
        max_value_size=config.max_record_bytes,
    )


def _is_bucket_already_exists_error(error: APIError) -> bool:
    if error.err_code == _STREAM_ALREADY_EXISTS_CODE:
        return True
    description = (error.description or "").lower()
    return (
        "already in use" in description or "stream name already in use" in description
    )


def _is_missing_bucket_error(error: BaseException) -> bool:
    if isinstance(error, BucketNotFoundError):
        return True
    if isinstance(error, APIError):
        description = (error.description or "").lower()
        return "stream not found" in description or "bucket not found" in description
    return False


def _validate_existing_bucket(
    status: KeyValue.BucketStatus, expected: KeyValueConfig
) -> None:
    stream_config = status.stream_info.config
    if status.history != expected.history:
        raise ResourceConfigurationError(
            f"Resource admission KV bucket {expected.bucket!r} has history={status.history}, "
            f"expected {expected.history}",
        )
    if stream_config.storage != expected.storage:
        raise ResourceConfigurationError(
            f"Resource admission KV bucket {expected.bucket!r} has storage={stream_config.storage!r}, "
            f"expected {expected.storage!r}",
        )
    max_age = getattr(stream_config, "max_age", None)
    if max_age is not None and float(max_age) > 0:
        raise ResourceConfigurationError(
            f"Resource admission KV bucket {expected.bucket!r} must not use TTL retention",
        )
    if stream_config.max_msg_size != expected.max_value_size:
        raise ResourceConfigurationError(
            f"Resource admission KV bucket {expected.bucket!r} has max_value_size="
            f"{stream_config.max_msg_size}, expected {expected.max_value_size}",
        )
    max_msgs = getattr(stream_config, "max_msgs", -1)
    if max_msgs is not None and max_msgs > 0:
        raise ResourceConfigurationError(
            f"Resource admission KV bucket {expected.bucket!r} must not cap messages "
            f"(max_msgs={max_msgs})",
        )
    max_bytes = getattr(stream_config, "max_bytes", -1)
    if max_bytes is not None and max_bytes > 0:
        raise ResourceConfigurationError(
            f"Resource admission KV bucket {expected.bucket!r} must not cap bytes "
            f"(max_bytes={max_bytes})",
        )


class NatsResourceAdmissionStore:
    """JetStream KV persistence for resource admission with revision CAS updates."""

    def __init__(
        self,
        broker: NatsBroker,
        *,
        subject_prefix: str,
        config: ResourceAdmissionConfig | None = None,
        provision: bool = True,
    ) -> None:
        self._broker = broker
        self._bucket_name = admission_bucket_name(subject_prefix)
        self._config = config or ResourceAdmissionConfig()
        self._provision = provision
        self._kv: KeyValue | None = None
        self._kv_init_lock = asyncio.Lock()
        self._key_scanner = NatsDiscoveryBackend(
            broker, subject_prefix=subject_prefix, discovery_provision=False,
        )

    @property
    def bucket_name(self) -> str:
        return self._bucket_name

    def invalidate(self) -> None:
        self._kv = None

    async def _jetstream(self):
        try:
            connection = self._broker.connection
        except Exception as error:
            raise ResourceStoreUnavailableError(
                "NATS connection is not available"
            ) from error
        if connection is None:
            raise ResourceStoreUnavailableError("NATS connection is not available")
        return connection.jetstream()

    async def _validate_cached_kv(self, kv: KeyValue) -> None:
        expected = _expected_kv_config(self._bucket_name, self._config)
        try:
            _validate_existing_bucket(await kv.status(), expected)
        except ResourceConfigurationError:
            raise
        except (APIError, nats.errors.Error) as error:
            if _is_missing_bucket_error(error):
                raise BucketNotFoundError from error
            raise ResourceStoreUnavailableError(
                "Resource admission KV bucket configuration could not be read",
            ) from error

    async def _open_kv(self) -> KeyValue:
        expected = _expected_kv_config(self._bucket_name, self._config)
        js = await self._jetstream()
        try:
            kv = await js.key_value(self._bucket_name)
        except BucketNotFoundError:
            if not self._provision:
                raise ResourceConfigurationError(
                    f"Resource admission KV bucket {self._bucket_name!r} is absent and "
                    "provision is disabled",
                ) from None
            try:
                await js.create_key_value(expected)
            except APIError as error:
                if not _is_bucket_already_exists_error(error):
                    raise ResourceStoreUnavailableError(
                        "Resource admission KV bucket provisioning failed",
                    ) from error
            kv = await js.key_value(self._bucket_name)
        await self._validate_cached_kv(kv)
        return kv

    async def _ensure_kv(self) -> KeyValue:
        async with self._kv_init_lock:
            last_error: BaseException | None = None
            for _ in range(_KV_CACHE_RETRY_ATTEMPTS):
                if self._kv is not None:
                    try:
                        await self._validate_cached_kv(self._kv)
                        return self._kv
                    except BucketNotFoundError as error:
                        last_error = error
                        self._kv = None
                    except ResourceConfigurationError:
                        raise
                try:
                    self._kv = await self._open_kv()
                    return self._kv
                except BucketNotFoundError as error:
                    last_error = error
                    self._kv = None
            if last_error is not None:
                raise ResourceStoreUnavailableError(
                    "Resource admission store is unavailable",
                ) from last_error
            raise ResourceStoreUnavailableError(
                "Resource admission store is unavailable"
            )

    async def _fetch(self, key: str) -> tuple[bytes | None, int]:
        kv = await self._ensure_kv()
        try:
            entry = await kv.get(key)
        except KeyNotFoundError:
            return None, 0
        if entry.value is None:
            return None, 0
        revision: int = entry.revision if entry.revision is not None else 0
        return entry.value, revision

    async def _list_admission_kv_keys(self, kv: KeyValue) -> tuple[str, ...]:
        return await list_kv_keys_with_verified_empty(
            kv,
            ambiguous_empty_message=(
                "Admission KV key listing reported no keys while the bucket "
                "retains JetStream messages"
            ),
            list_failure_message="Unable to list admission KV keys",
            status_failure_message="Unable to read admission KV bucket state",
            list_keys=self._key_scanner._list_kv_keys,
        )

    async def read_snapshot(self, *, evaluated_at: datetime) -> AdmissionSnapshot:
        kv = await self._ensure_kv()
        keys = await self._list_admission_kv_keys(kv)
        capacities: list[StoredCapacityRecord] = []
        resources: list[StoredResourceRecord] = []
        claims: list[StoredExecutionClaim] = []
        for key in sorted(keys):
            if key.startswith("cap.v1."):
                payload, _ = await self._fetch(key)
                if payload is not None:
                    capacities.append(decode_capacity(payload))
            elif key.startswith("res.v1."):
                payload, _ = await self._fetch(key)
                if payload is not None:
                    resources.append(decode_resource(payload))
            elif key.startswith("exe.v1."):
                payload, _ = await self._fetch(key)
                if payload is not None:
                    claims.append(decode_execution_claim(payload))
        return AdmissionSnapshot(
            evaluated_at=evaluated_at,
            capacities=tuple(capacities),
            resources=tuple(resources),
            execution_claims=tuple(claims),
        )

    async def read_capacity(self, resource_key: str) -> StoredCapacityRecord | None:
        payload, _ = await self._fetch(capacity_kv_key(resource_key))
        if payload is None:
            return None
        return decode_capacity(payload)

    async def compare_and_set_capacity(
        self,
        declaration: CapacityDeclaration,
        *,
        updated_at: datetime,
        create_only: bool,
    ) -> CasResult[StoredCapacityRecord]:
        key = capacity_kv_key(declaration.resource_key)
        record = StoredCapacityRecord(declaration=declaration, updated_at=updated_at)
        encoded = encode_capacity(record, self._config)
        kv = await self._ensure_kv()
        for _ in range(_CAS_RETRY_ATTEMPTS):
            current_payload, revision = await self._fetch(key)
            if create_only and current_payload is not None:
                return CasResult(CasStatus.CONFLICT, decode_capacity(current_payload))
            if current_payload is not None and not create_only:
                existing = decode_capacity(current_payload)
                if existing.declaration != declaration:
                    return CasResult(CasStatus.CONFLICT, existing)
            try:
                if revision == 0:
                    await kv.create(key, encoded)
                else:
                    await kv.update(key, encoded, last=revision)
            except KeyWrongLastSequenceError:
                continue
            except APIError as error:
                _raise_store_unavailable("capacity compare-and-set", error)
            return CasResult(CasStatus.APPLIED, record)
        raise ResourceStoreUnavailableError(
            "capacity compare-and-set retries exhausted"
        )

    async def read_resource(
        self, resource_key: str
    ) -> tuple[StoredResourceRecord | None, int]:
        payload, revision = await self._fetch(resource_kv_key(resource_key))
        if payload is None:
            return None, 0
        return decode_resource(payload), revision

    async def compare_and_set_resource(
        self,
        record: StoredResourceRecord,
        *,
        last_revision: int | None,
    ) -> CasResult[StoredResourceRecord]:
        key = resource_kv_key(record.resource_key)
        encoded = encode_resource(record, self._config)
        kv = await self._ensure_kv()
        for _ in range(_CAS_RETRY_ATTEMPTS):
            current_payload, revision = await self._fetch(key)
            if last_revision is None:
                if current_payload is not None:
                    return CasResult(
                        CasStatus.CONFLICT, decode_resource(current_payload)
                    )
                try:
                    await kv.create(key, encoded)
                except KeyWrongLastSequenceError:
                    continue
                except APIError as error:
                    _raise_store_unavailable("resource compare-and-set", error)
                return CasResult(CasStatus.APPLIED, record)
            if current_payload is None or revision != last_revision:
                if current_payload is None:
                    return CasResult(CasStatus.MISSING, None)
                return CasResult(CasStatus.CONFLICT, decode_resource(current_payload))
            try:
                await kv.update(key, encoded, last=revision)
            except KeyWrongLastSequenceError:
                continue
            except APIError as error:
                _raise_store_unavailable("resource compare-and-set", error)
            return CasResult(CasStatus.APPLIED, record)
        raise ResourceStoreUnavailableError(
            "resource compare-and-set retries exhausted"
        )

    async def read_execution_claim(
        self,
        execution_id: str,
    ) -> tuple[StoredExecutionClaim | None, int]:
        payload, revision = await self._fetch(execution_kv_key(execution_id))
        if payload is None:
            return None, 0
        return decode_execution_claim(payload), revision

    async def compare_and_set_execution_claim(
        self,
        record: StoredExecutionClaim,
        *,
        last_revision: int | None,
    ) -> CasResult[StoredExecutionClaim]:
        key = execution_kv_key(record.execution_id)
        encoded = encode_execution_claim(record, self._config)
        kv = await self._ensure_kv()
        for _ in range(_CAS_RETRY_ATTEMPTS):
            current_payload, revision = await self._fetch(key)
            if last_revision is None:
                if current_payload is not None:
                    return CasResult(
                        CasStatus.CONFLICT, decode_execution_claim(current_payload)
                    )
                try:
                    await kv.create(key, encoded)
                except KeyWrongLastSequenceError:
                    continue
                except APIError as error:
                    _raise_store_unavailable("execution claim compare-and-set", error)
                return CasResult(CasStatus.APPLIED, record)
            if current_payload is None or revision != last_revision:
                if current_payload is None:
                    return CasResult(CasStatus.MISSING, None)
                return CasResult(
                    CasStatus.CONFLICT,
                    decode_execution_claim(current_payload),
                )
            try:
                await kv.update(key, encoded, last=revision)
            except KeyWrongLastSequenceError:
                continue
            except APIError as error:
                _raise_store_unavailable("execution claim compare-and-set", error)
            return CasResult(CasStatus.APPLIED, record)
        raise ResourceStoreUnavailableError(
            "execution claim compare-and-set retries exhausted"
        )

    async def delete_execution_claim_if_owner(
        self,
        execution_id: str,
        owner: AdmissionOwner,
        *,
        last_revision: int,
    ) -> CasResult[None]:
        key = execution_kv_key(execution_id)
        kv = await self._ensure_kv()
        for _ in range(_CAS_RETRY_ATTEMPTS):
            current_payload, revision = await self._fetch(key)
            if current_payload is None or revision != last_revision:
                if current_payload is None:
                    return CasResult(CasStatus.MISSING, None)
                return CasResult(CasStatus.CONFLICT, None)
            claim = decode_execution_claim(current_payload)
            if claim.owner != owner:
                return CasResult(CasStatus.CONFLICT, None)
            try:
                await kv.delete(key, last=revision)
            except KeyWrongLastSequenceError:
                continue
            except APIError as error:
                _raise_store_unavailable("execution claim delete", error)
            return CasResult(CasStatus.APPLIED, None)
        raise ResourceStoreUnavailableError("execution claim delete retries exhausted")

    async def read_revoked_incarnation(
        self,
        incarnation: WorkerIncarnation,
    ) -> StoredRevokedIncarnation | None:
        key = revoked_incarnation_kv_key(
            incarnation.worker_id, incarnation.incarnation_id
        )
        payload, _ = await self._fetch(key)
        if payload is None:
            return None
        return decode_revoked_incarnation(payload)

    async def compare_and_set_revoked_incarnation(
        self,
        record: StoredRevokedIncarnation,
        *,
        create_only: bool,
    ) -> CasResult[StoredRevokedIncarnation]:
        key = revoked_incarnation_kv_key(
            record.incarnation.worker_id,
            record.incarnation.incarnation_id,
        )
        encoded = encode_revoked_incarnation(record, self._config)
        kv = await self._ensure_kv()
        for _ in range(_CAS_RETRY_ATTEMPTS):
            current_payload, revision = await self._fetch(key)
            if create_only and current_payload is not None:
                return CasResult(
                    CasStatus.CONFLICT, decode_revoked_incarnation(current_payload)
                )
            try:
                if revision == 0:
                    await kv.create(key, encoded)
                else:
                    await kv.update(key, encoded, last=revision)
            except KeyWrongLastSequenceError:
                continue
            except APIError as error:
                _raise_store_unavailable("revoked incarnation compare-and-set", error)
            return CasResult(CasStatus.APPLIED, record)
        raise ResourceStoreUnavailableError(
            "revoked incarnation compare-and-set retries exhausted"
        )
