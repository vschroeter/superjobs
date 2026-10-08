from __future__ import annotations

import asyncio
import base64
import binascii
import hashlib
import re
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import nats.errors
from nats.js.api import KeyValueConfig, StorageType
from nats.js.errors import APIError, BucketNotFoundError, KeyDeletedError, KeyNotFoundError
from nats.js.kv import KeyValue

from superjobs.discovery.config import PresenceConfig
from superjobs.discovery.envelope import (
    StoredWorkerRegistration,
    encode_envelope,
    validate_envelope_write,
)
from superjobs.discovery.errors import (
    DiscoveryConfigurationError,
    DiscoveryEnvelopeError,
    DiscoveryUnavailableError,
    DiscoveryWriteError,
)
from superjobs.discovery.memory import DiscoverySnapshot
from superjobs.job_identity import JobIdentity
from superjobs.runtime_lifecycle import finish_cleanup

if TYPE_CHECKING:
    from faststream.nats import NatsBroker

_KV_KEY_PREFIX = "wr.v1"
_BUCKET_SUFFIX = "-worker-discovery"
_VALID_BUCKET_RE = re.compile(r"^[a-zA-Z0-9_-]+$")
_KV_SAFE_KEY_RE = re.compile(r"^[-/_=\.a-zA-Z0-9]+$")
_STREAM_ALREADY_EXISTS_CODE = 10058
_JS_REQUEST_TIMEOUT_CAP_SECONDS = 1.0
_KV_CACHE_RETRY_ATTEMPTS = 2

# Budget to let a shielded nats-py KeyValue.watchall initializer finish after caller
# cancellation so consumer subscriptions can be torn down via watcher.stop().
_WATCHALL_INIT_CLEANUP_BUDGET_FACTOR = 2.0


def _b64url_encode(text: str) -> str:
    return base64.urlsafe_b64encode(text.encode("utf-8")).rstrip(b"=").decode("ascii")


def _b64url_decode_strict(segment: str, *, field: str) -> str:
    if not segment:
        raise DiscoveryEnvelopeError(f"Malformed worker registration KV key: missing {field}")
    padding = "=" * (-len(segment) % 4)
    try:
        raw = base64.urlsafe_b64decode(segment + padding)
        text = raw.decode("utf-8")
    except (binascii.Error, ValueError, UnicodeDecodeError) as error:
        raise DiscoveryEnvelopeError(
            f"Malformed worker registration KV key segment for {field}",
        ) from error
    if _b64url_encode(text) != segment:
        raise DiscoveryEnvelopeError(
            f"Malformed worker registration KV key segment for {field}",
        )
    return text


def _is_kv_key_valid(key: str) -> bool:
    if not key or key[0] == "." or key[-1] == ".":
        return False
    return _KV_SAFE_KEY_RE.fullmatch(key) is not None


def discovery_bucket_name(subject_prefix: str) -> str:
    candidate = f"{subject_prefix}{_BUCKET_SUFFIX}"
    if _VALID_BUCKET_RE.fullmatch(candidate):
        return candidate
    digest = hashlib.sha256(subject_prefix.encode("utf-8")).hexdigest()[:22]
    hashed = f"sj-{digest}-wdisc"
    if _VALID_BUCKET_RE.fullmatch(hashed):
        return hashed
    raise DiscoveryConfigurationError(
        f"Cannot derive a valid JetStream KV bucket name from subject prefix {subject_prefix!r}",
    )


def registration_kv_key(worker_id: str, job: JobIdentity) -> str:
    if not worker_id:
        raise DiscoveryConfigurationError("worker_id must be non-empty")
    if job.version is None:
        version_segment = "_uv"
    else:
        version_segment = f"_v{_b64url_encode(job.version)}"
    key = (
        f"{_KV_KEY_PREFIX}.{_b64url_encode(worker_id)}."
        f"{_b64url_encode(job.name)}.{version_segment}"
    )
    if not _is_kv_key_valid(key):
        raise DiscoveryConfigurationError(
            "Worker registration identity cannot be encoded as a KV-safe key",
        )
    return key


def parse_registration_kv_key(key: str) -> tuple[str, str, str | None]:
    if not key.startswith(f"{_KV_KEY_PREFIX}."):
        raise DiscoveryEnvelopeError(f"Unknown worker registration KV key prefix: {key!r}")
    parts = key.split(".")
    if len(parts) != 5 or parts[0] != "wr" or parts[1] != "v1":
        raise DiscoveryEnvelopeError(f"Malformed worker registration KV key: {key!r}")
    worker_id = _b64url_decode_strict(parts[2], field="worker_id")
    if not worker_id:
        raise DiscoveryEnvelopeError("worker_id must be a non-empty string")
    job_name = _b64url_decode_strict(parts[3], field="job.name")
    version_segment = parts[4]
    if version_segment == "_uv":
        job_version: str | None = None
    elif version_segment.startswith("_v"):
        job_version = _b64url_decode_strict(version_segment[2:], field="job.version")
    else:
        raise DiscoveryEnvelopeError(f"Malformed worker registration KV key: {key!r}")
    try:
        job = JobIdentity(name=job_name, version=job_version)
    except ValueError as error:
        raise DiscoveryEnvelopeError(str(error)) from error
    return (worker_id, job.name, job.version)


def _expected_kv_config(bucket: str, config: PresenceConfig) -> KeyValueConfig:
    return KeyValueConfig(
        bucket=bucket,
        history=1,
        storage=StorageType.FILE,
        ttl=float(config.stale_retention.total_seconds()),
        max_value_size=config.max_envelope_bytes,
    )


def _stream_ttl_seconds(stream_info: object) -> float | None:
    max_age = getattr(getattr(stream_info, "config", None), "max_age", None)
    if max_age is None:
        return None
    return float(max_age)


def _validate_existing_bucket(status: KeyValue.BucketStatus, expected: KeyValueConfig) -> None:
    stream_config = status.stream_info.config
    if status.history != expected.history:
        raise DiscoveryConfigurationError(
            f"Discovery KV bucket {expected.bucket!r} has history={status.history}, "
            f"expected {expected.history}",
        )
    if stream_config.storage != expected.storage:
        raise DiscoveryConfigurationError(
            f"Discovery KV bucket {expected.bucket!r} has storage={stream_config.storage!r}, "
            f"expected {expected.storage!r}",
        )
    expected_ttl = expected.ttl
    actual_ttl = _stream_ttl_seconds(status.stream_info)
    if expected_ttl is not None:
        if actual_ttl is None:
            raise DiscoveryConfigurationError(
                f"Discovery KV bucket {expected.bucket!r} is missing a retention TTL",
            )
        if abs(actual_ttl - expected_ttl) > 0.001:
            raise DiscoveryConfigurationError(
                f"Discovery KV bucket {expected.bucket!r} has ttl={actual_ttl}, "
                f"expected {expected_ttl}",
            )
    if stream_config.max_msg_size != expected.max_value_size:
        raise DiscoveryConfigurationError(
            f"Discovery KV bucket {expected.bucket!r} has max_value_size="
            f"{stream_config.max_msg_size}, expected {expected.max_value_size}",
        )


def _is_bucket_already_exists_error(error: APIError) -> bool:
    if error.err_code == _STREAM_ALREADY_EXISTS_CODE:
        return True
    description = (error.description or "").lower()
    return "already in use" in description or "stream name already in use" in description


def _is_missing_bucket_error(error: BaseException) -> bool:
    if isinstance(error, BucketNotFoundError):
        return True
    if isinstance(error, APIError):
        description = (error.description or "").lower()
        return "stream not found" in description or "bucket not found" in description
    return False


class NatsDiscoveryBackend:
    """JetStream KV persistence for worker registration snapshots."""

    def __init__(
        self,
        broker: NatsBroker,
        *,
        subject_prefix: str,
        config: PresenceConfig | None = None,
        discovery_provision: bool = True,
    ) -> None:
        self._broker = broker
        self._config = config or PresenceConfig()
        self._discovery_provision = discovery_provision
        self._bucket_name = discovery_bucket_name(subject_prefix)
        self._kv: KeyValue | None = None
        self._kv_init_lock = asyncio.Lock()
        self._mutation_lock = asyncio.Lock()

    @property
    def config(self) -> PresenceConfig:
        return self._config

    @property
    def bucket_name(self) -> str:
        return self._bucket_name

    def invalidate(self) -> None:
        self._kv = None

    def _timeout_seconds(self) -> float:
        return self._config.read_timeout.total_seconds()

    def _js_request_timeout_seconds(self) -> float:
        return min(_JS_REQUEST_TIMEOUT_CAP_SECONDS, self._timeout_seconds())

    def _watchall_init_cleanup_budget_seconds(self) -> float:
        return min(
            self._timeout_seconds() * _WATCHALL_INIT_CLEANUP_BUDGET_FACTOR,
            self._timeout_seconds() + _JS_REQUEST_TIMEOUT_CAP_SECONDS,
        )

    async def _run_bounded(self, coro, *, operation: str):
        try:
            return await asyncio.wait_for(coro, timeout=self._timeout_seconds())
        except asyncio.CancelledError:
            raise
        except asyncio.TimeoutError as error:
            raise DiscoveryUnavailableError(f"Discovery registry {operation} timed out") from error

    def _effective_max_bytes(self, max_envelope_bytes: int) -> int:
        if type(max_envelope_bytes) is not int or isinstance(max_envelope_bytes, bool):
            raise DiscoveryWriteError("max_envelope_bytes must be a positive integer")
        if max_envelope_bytes < 1:
            raise DiscoveryWriteError("max_envelope_bytes must be positive")
        return min(max_envelope_bytes, self._config.max_envelope_bytes)

    async def _jetstream(self):
        try:
            connection = self._broker.connection
        except Exception as error:
            raise DiscoveryUnavailableError("NATS connection is not available") from error
        if connection is None:
            raise DiscoveryUnavailableError("NATS connection is not available")
        return connection.jetstream(timeout=self._js_request_timeout_seconds())

    async def _validate_cached_kv(self, kv: KeyValue) -> None:
        expected = _expected_kv_config(self._bucket_name, self._config)
        try:
            _validate_existing_bucket(await kv.status(), expected)
        except DiscoveryConfigurationError:
            raise
        except (APIError, nats.errors.Error) as error:
            if _is_missing_bucket_error(error):
                raise BucketNotFoundError from error
            raise DiscoveryUnavailableError(
                "Discovery KV bucket configuration could not be read",
            ) from error
        except Exception as error:
            raise DiscoveryUnavailableError(
                "Discovery KV bucket configuration could not be read",
            ) from error

    async def _open_kv(self) -> KeyValue:
        expected = _expected_kv_config(self._bucket_name, self._config)
        try:
            js = await self._jetstream()
        except DiscoveryUnavailableError:
            raise
        except Exception as error:
            raise DiscoveryUnavailableError(
                "Discovery registry connection failed",
            ) from error
        try:
            kv = await js.key_value(self._bucket_name)
        except BucketNotFoundError:
            if not self._discovery_provision:
                raise DiscoveryConfigurationError(
                    f"Discovery KV bucket {self._bucket_name!r} is absent and "
                    "discovery_provision is disabled",
                ) from None
            try:
                await js.create_key_value(expected)
            except APIError as error:
                if not _is_bucket_already_exists_error(error):
                    raise DiscoveryUnavailableError(
                        "Discovery KV bucket provisioning failed",
                    ) from error
                kv = await js.key_value(self._bucket_name)
            else:
                kv = await js.key_value(self._bucket_name)
        except DiscoveryConfigurationError:
            raise
        except (APIError, nats.errors.Error) as error:
            raise DiscoveryUnavailableError(
                "Discovery registry is unavailable",
            ) from error
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
                    except DiscoveryConfigurationError:
                        raise
                try:
                    self._kv = await self._open_kv()
                    return self._kv
                except BucketNotFoundError as error:
                    last_error = error
                    self._kv = None
            if last_error is not None:
                raise DiscoveryUnavailableError(
                    "Discovery registry is unavailable",
                ) from last_error
            raise DiscoveryUnavailableError("Discovery registry is unavailable")

    async def _stop_watcher(self, watcher: KeyValue.KeyWatcher, *, strict: bool) -> None:
        try:
            await asyncio.wait_for(
                watcher.stop(),
                timeout=min(_JS_REQUEST_TIMEOUT_CAP_SECONDS, self._timeout_seconds()),
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            if strict:
                raise

    async def _await_init_task_quiet(
        self,
        init_task: asyncio.Task[KeyValue.KeyWatcher],
    ) -> KeyValue.KeyWatcher | None:
        """Wait for a shielded watchall initializer without surfacing its errors."""
        if not init_task.done():
            try:
                await asyncio.wait_for(
                    asyncio.shield(init_task),
                    timeout=self._watchall_init_cleanup_budget_seconds(),
                )
            except asyncio.TimeoutError:
                init_task.cancel()
            except asyncio.CancelledError:
                raise
            except Exception:
                pass
            try:
                await init_task
            except (asyncio.CancelledError, Exception):
                pass
        if init_task.cancelled() or not init_task.done():
            return None
        if init_task.exception() is not None:
            return None
        return init_task.result()

    async def _release_kv_watchall(
        self,
        watcher: KeyValue.KeyWatcher,
        init_task: asyncio.Task[KeyValue.KeyWatcher],
        *,
        on_cancel: bool,
    ) -> None:
        if not init_task.done():
            extra = await self._await_init_task_quiet(init_task)
            if extra is not None and extra is not watcher:
                try:
                    await self._stop_watcher(extra, strict=not on_cancel)
                except Exception:
                    if not on_cancel:
                        raise
        try:
            await self._stop_watcher(watcher, strict=not on_cancel)
        except Exception:
            if not on_cancel:
                raise

    async def _start_kv_watchall(
        self,
        kv: KeyValue,
        **kwargs: object,
    ) -> tuple[KeyValue.KeyWatcher, asyncio.Task[KeyValue.KeyWatcher]]:
        init_task = asyncio.create_task(
            kv.watchall(**kwargs),
            name="superjobs-discovery-kv-watchall-init",
        )
        try:
            watcher = await asyncio.shield(init_task)
        except asyncio.CancelledError:
            async def _cleanup_initializer() -> None:
                initialized = await self._await_init_task_quiet(init_task)
                if initialized is not None:
                    await self._stop_watcher(initialized, strict=False)

            cleanup = asyncio.create_task(
                _cleanup_initializer(),
                name="superjobs-discovery-kv-watchall-cleanup",
            )
            try:
                await finish_cleanup(cleanup)
            except (asyncio.CancelledError, Exception):
                pass
            raise
        return watcher, init_task

    async def _list_kv_keys(self, kv: KeyValue) -> list[str]:
        watcher, init_task = await self._start_kv_watchall(
            kv,
            ignore_deletes=True,
            meta_only=True,
        )
        try:
            keys: list[str] = []
            async for entry in watcher:
                if entry is None:
                    break
                keys.append(entry.key)
        except asyncio.CancelledError:
            cleanup = asyncio.create_task(
                self._release_kv_watchall(watcher, init_task, on_cancel=True),
                name="superjobs-discovery-kv-watchall-cleanup",
            )
            try:
                await finish_cleanup(cleanup)
            except asyncio.CancelledError:
                pass
            raise
        except Exception:
            await finish_cleanup(asyncio.create_task(
                self._release_kv_watchall(watcher, init_task, on_cancel=False),
                name="superjobs-discovery-kv-watchall-cleanup",
            ))
            raise
        else:
            await finish_cleanup(asyncio.create_task(
                self._release_kv_watchall(watcher, init_task, on_cancel=False),
                name="superjobs-discovery-kv-watchall-cleanup",
            ))
            return keys

    async def _fetch_kv_entry(self, kv: KeyValue, key: str) -> KeyValue.Entry:
        return await kv.get(key)

    async def read_snapshot(self, *, include_stale: bool = False) -> DiscoverySnapshot:
        async def _read() -> DiscoverySnapshot:
            evaluated_at = datetime.now(tz=UTC)
            kv = await self._ensure_kv()
            key_names = await self._list_kv_keys(kv)
            entries: list[tuple[tuple[str, str, str | None], bytes]] = []
            for key_name in key_names:
                if not key_name.startswith(f"{_KV_KEY_PREFIX}."):
                    raise DiscoveryEnvelopeError(
                        f"Unexpected key in discovery KV bucket: {key_name!r}",
                    )
                logical_key = parse_registration_kv_key(key_name)
                try:
                    entry = await self._fetch_kv_entry(kv, key_name)
                except (KeyNotFoundError, KeyDeletedError):
                    continue
                if entry.value is None:
                    continue
                entries.append((logical_key, entry.value))
            return DiscoverySnapshot(evaluated_at=evaluated_at, entries=tuple(entries))

        try:
            return await self._run_bounded(_read(), operation="read")
        except DiscoveryConfigurationError:
            raise
        except DiscoveryEnvelopeError:
            raise
        except DiscoveryUnavailableError:
            raise
        except asyncio.CancelledError:
            raise
        except Exception as error:
            raise DiscoveryUnavailableError(
                "Discovery registry read failed",
            ) from error

    async def write_registration(
        self,
        registration: StoredWorkerRegistration,
        *,
        max_envelope_bytes: int,
    ) -> None:
        limit = self._effective_max_bytes(max_envelope_bytes)
        encoded = encode_envelope(registration, max_bytes=limit)
        validated = validate_envelope_write(encoded, max_bytes=limit)
        kv_key = registration_kv_key(validated.worker_id, validated.job)

        async def _write() -> None:
            kv = await self._ensure_kv()
            async with self._mutation_lock:
                await kv.put(kv_key, encoded)

        try:
            await self._run_bounded(_write(), operation="write")
        except DiscoveryWriteError:
            raise
        except DiscoveryConfigurationError:
            raise
        except DiscoveryUnavailableError as error:
            raise DiscoveryWriteError(
                str(error),
                worker_id=validated.worker_id,
                job=validated.job,
            ) from error
        except asyncio.CancelledError:
            raise
        except Exception as error:
            raise DiscoveryWriteError(
                "Failed to write worker registration",
                worker_id=validated.worker_id,
                job=validated.job,
            ) from error

    async def delete_registration(self, worker_id: str, job: JobIdentity) -> None:
        kv_key = registration_kv_key(worker_id, job)

        async def _delete() -> None:
            kv = await self._ensure_kv()
            async with self._mutation_lock:
                try:
                    await kv.delete(kv_key)
                except (KeyNotFoundError, KeyDeletedError):
                    return

        try:
            await self._run_bounded(_delete(), operation="delete")
        except DiscoveryConfigurationError:
            raise
        except asyncio.CancelledError:
            raise
        except DiscoveryUnavailableError as error:
            raise DiscoveryWriteError(
                str(error),
                worker_id=worker_id,
                job=job,
            ) from error
        except Exception as error:
            raise DiscoveryWriteError(
                "Failed to delete worker registration",
                worker_id=worker_id,
                job=job,
            ) from error
