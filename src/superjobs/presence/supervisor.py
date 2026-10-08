from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field, replace
from typing import Any, TypeVar

from superjobs.discovery.config import PresenceConfig
from superjobs.discovery.envelope import StoredWorkerRegistration
from superjobs.discovery.errors import DiscoveryError, DiscoveryUnavailableError, DiscoveryWriteError
from superjobs.discovery.memory import Clock, DiscoveryBackend, utc_now
from superjobs.discovery.models import WorkerRegistrationMetadata, WorkerRegistrationState
from superjobs.discovery.reader import resolve_discovery_backend
from superjobs.jobs.job import Job
from superjobs.presence.capabilities_runtime import (
    encode_validated_capabilities,
    is_capability_factory,
    materialize_capabilities,
    prepare_registration_wire,
    validate_capability_value,
)
from superjobs.presence.errors import LocalWorkerHandleError, WorkerDiscoveryDisabledError
from superjobs.presence.local_handle import LocalWorkerHandle
from superjobs.runtime_lifecycle import finish_cleanup

logger = logging.getLogger(__name__)
T = TypeVar("T")


@dataclass
class PresenceEntry:
    job: Job[Any, Any, Any]
    capabilities_spec: Any
    capabilities_factory: Callable[..., Any] | None
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    generation: int = 0
    fence: int = 0
    accepted: StoredWorkerRegistration | None = None
    attempted_generations: set[int] = field(default_factory=set)


class PresenceSupervisor:
    """Serialize each registration's publication independently of Attempt leases."""

    def __init__(
        self, *, worker_id: str, transport: Any, enabled: bool,
        presence_config: PresenceConfig | None = None,
        presence_clock: Clock | None = None,
    ) -> None:
        self._worker_id = worker_id
        self._transport = transport
        self._enabled = enabled
        self._clock = presence_clock
        self._config_override = presence_config
        self._resolved_config: PresenceConfig | None = None
        self._discovery: DiscoveryBackend | None = None
        self._entries: dict[str, PresenceEntry] = {}
        self._serving_generation = 0
        self._phase = "stopped"
        self._renewal_task: asyncio.Task[None] | None = None
        self._operations: set[asyncio.Task[Any]] = set()
        self._children: set[asyncio.Task[Any]] = set()

    @property
    def worker_id(self) -> str:
        return self._worker_id

    @property
    def enabled(self) -> bool:
        return self._enabled

    def ensure_registry_access(self) -> None:
        if not self._enabled:
            raise WorkerDiscoveryDisabledError("Worker presence is disabled on this runtime")

    def _require_discovery(self) -> DiscoveryBackend:
        self.ensure_registry_access()
        if self._discovery is None:
            self._discovery = resolve_discovery_backend(self._transport)
        return self._discovery

    def ensure_write_backend(self) -> None:
        if self._enabled:
            self._require_discovery()

    def _resolve_config(self) -> PresenceConfig:
        if self._config_override is not None:
            return self._config_override
        if self._resolved_config is None:
            config = getattr(self._discovery, "config", None)
            self._resolved_config = config if isinstance(config, PresenceConfig) else PresenceConfig()
        return self._resolved_config

    def _config_for_publication(self) -> PresenceConfig:
        self._require_discovery()
        return self._resolve_config()

    def register_handler(self, job: Job[Any, Any, Any], capabilities_spec: Any) -> None:
        if self._enabled:
            self._entries[job.canonical_name] = PresenceEntry(
                job=job,
                capabilities_spec=capabilities_spec,
                capabilities_factory=(capabilities_spec if is_capability_factory(job, capabilities_spec) else None),
            )

    def unregister_handler(self, job: Job[Any, Any, Any]) -> None:
        self._entries.pop(job.canonical_name, None)

    def has_handler(self, job: Job[Any, Any, Any]) -> bool:
        return job.canonical_name in self._entries

    def worker(self, job: Job[Any, Any, Any]) -> LocalWorkerHandle[Any]:
        self.ensure_registry_access()
        if not self.has_handler(job):
            raise LocalWorkerHandleError("No handler is registered in this runtime", job=job)
        return LocalWorkerHandle(self, job)

    @staticmethod
    def _consume_task_result(task: asyncio.Task[Any]) -> None:
        if not task.cancelled():
            task.exception()

    async def _bounded(self, operation: Awaitable[T], *, description: str) -> T:
        """A timeout also bounds cancellation of a poorly behaved backend/factory."""
        task = asyncio.create_task(operation)
        self._children.add(task)
        task.add_done_callback(self._children.discard)
        timeout = self._resolve_config().read_timeout.total_seconds()
        try:
            done, _ = await asyncio.wait({task}, timeout=timeout)
            if not done:
                raise DiscoveryUnavailableError(f"{description} timed out")
            return task.result()
        finally:
            if not task.done():
                if not task.cancelling():
                    task.cancel()
                task.add_done_callback(self._consume_task_result)
                # Allow ordinary async factory/backend finally blocks to settle
                # before their provider resources are released.
                async def settle() -> None:
                    await asyncio.wait({task}, timeout=timeout)
                await finish_cleanup(asyncio.create_task(settle()))

    async def _operation(self, operation: Awaitable[T]) -> T:
        task = asyncio.create_task(operation)
        self._operations.add(task)
        task.add_done_callback(self._operations.discard)
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            if not task.done() and not task.cancelling():
                task.cancel()
            async def settle() -> None:
                await asyncio.wait({task}, timeout=self._resolve_config().read_timeout.total_seconds())
            try:
                await finish_cleanup(asyncio.create_task(settle()))
            except asyncio.CancelledError:
                pass
            task.add_done_callback(self._consume_task_result)
            raise

    def _require_ready(self, entry: PresenceEntry, *, generation: int, fence: int) -> None:
        if (
            self._phase != "ready" or generation != self._serving_generation
            or entry.generation != generation or entry.fence != fence
            or entry.accepted is None
        ):
            raise LocalWorkerHandleError("Handler presence is not ready", job=entry.job)

    async def _write(
        self, entry: PresenceEntry, registration: StoredWorkerRegistration,
        *, generation: int, fence: int,
    ) -> None:
        if generation != self._serving_generation or fence != entry.fence:
            raise LocalWorkerHandleError("Stale worker presence operation", job=entry.job)
        if registration.state == WorkerRegistrationState.READY and self._phase not in {"starting", "ready"}:
            raise LocalWorkerHandleError("Handler presence is stopping", job=entry.job)
        config = self._config_for_publication()
        # Validation happens before a backend can change the stored snapshot.
        prepare_registration_wire(entry.job, registration, self._worker_id, max_envelope_bytes=config.max_envelope_bytes)
        entry.attempted_generations.add(generation)
        try:
            await self._bounded(
                self._require_discovery().write_registration(registration, max_envelope_bytes=config.max_envelope_bytes),
                description="Worker presence publication",
            )
        except Exception as error:
            if hasattr(error, "add_context"):
                error.add_context(worker_id=self._worker_id, job=entry.job.identity)
            elif isinstance(error, DiscoveryError):
                error.add_note(f"Worker {self._worker_id!r} on {entry.job.identity}")
            else:
                raise DiscoveryWriteError("Worker presence publication failed", worker_id=self._worker_id, job=entry.job.identity) from error
            raise
        if generation != self._serving_generation or fence != entry.fence:
            raise LocalWorkerHandleError("Stale acknowledged presence operation", job=entry.job)
        entry.accepted = registration
        entry.generation = generation

    def _registration(self, entry: PresenceEntry, raw: Any) -> StoredWorkerRegistration:
        now = utc_now(self._clock)
        return StoredWorkerRegistration(
            worker_id=self._worker_id, job=entry.job.identity,
            state=WorkerRegistrationState.READY,
            registered_at=entry.accepted.registered_at if entry.accepted else now,
            last_seen_at=now,
            expires_at=now + self._config_for_publication().lease_timeout,
            capabilities=raw, metadata=WorkerRegistrationMetadata(),
        )

    async def publish_initial(self, job: Job[Any, Any, Any], *, generation: int) -> None:
        if not self._enabled:
            return
        entry = self._entries[job.canonical_name]
        fence = entry.fence
        self._config_for_publication()

        async def run() -> None:
            async with entry.lock:
                if generation != self._serving_generation or entry.fence != fence or self._phase not in {"starting", "ready"}:
                    raise LocalWorkerHandleError("Stale initial worker presence", job=job)
                _, raw = await self._bounded(materialize_capabilities(job, entry.capabilities_spec), description="Capability factory")
                await self._write(entry, self._registration(entry, raw), generation=generation, fence=fence)

        await self._operation(run())

    async def publish_all_initial(self, *, generation: int) -> None:
        if self._enabled:
            for entry in self._entries.values():
                await self.publish_initial(entry.job, generation=generation)

    async def _delete(self, entry: PresenceEntry) -> None:
        if self._discovery is not None:
            await self._bounded(self._discovery.delete_registration(self._worker_id, entry.job.identity), description="Worker presence deletion")
        entry.accepted = None
        entry.generation = 0
        entry.attempted_generations.clear()

    async def rollback_handler(self, job: Job[Any, Any, Any], *, generation: int) -> None:
        entry = self._entries.get(job.canonical_name)
        if entry is None:
            return
        async with entry.lock:
            if generation in entry.attempted_generations:
                await self._delete(entry)

    async def rollback_generation(self, generation: int) -> None:
        if not self._enabled or self._discovery is None:
            return
        self._phase = "stopping"
        errors: list[BaseException] = []
        try:
            await self._cancel_operations()
        except Exception as error:
            errors.append(error)
        for entry in self._entries.values():
            entry.fence += 1
            try:
                await self._bounded(self.rollback_handler(entry.job, generation=generation), description="Worker startup rollback")
            except Exception as error:
                logger.exception("Worker presence rollback failed for %s", entry.job)
                errors.append(error)
        self._phase = "stopped"
        if errors:
            raise BaseExceptionGroup("Worker presence startup rollback failed", errors)

    async def update_capabilities(self, job: Job[Any, Any, Any], value: Any | None) -> None:
        self.ensure_registry_access()
        entry = self._entries.get(job.canonical_name)
        if entry is None:
            raise LocalWorkerHandleError("No handler is registered in this runtime", job=job)
        generation, fence = self._serving_generation, entry.fence

        async def run() -> None:
            async with entry.lock:
                self._require_ready(entry, generation=generation, fence=fence)
                validated = validate_capability_value(job, value)
                raw = encode_validated_capabilities(job, validated)
                await self._write(entry, self._registration(entry, raw), generation=generation, fence=fence)

        await self._operation(run())

    async def refresh_capabilities(self, job: Job[Any, Any, Any]) -> None:
        self.ensure_registry_access()
        entry = self._entries.get(job.canonical_name)
        if entry is None:
            raise LocalWorkerHandleError("No handler is registered in this runtime", job=job)
        if entry.capabilities_factory is None:
            raise LocalWorkerHandleError("No capability factory is registered for this handler", job=job)
        generation, fence = self._serving_generation, entry.fence

        async def run() -> None:
            async with entry.lock:
                self._require_ready(entry, generation=generation, fence=fence)
                _, raw = await self._bounded(materialize_capabilities(job, entry.capabilities_factory), description="Capability factory")
                self._require_ready(entry, generation=generation, fence=fence)
                await self._write(entry, self._registration(entry, raw), generation=generation, fence=fence)

        await self._operation(run())

    async def _renew_entry(self, entry: PresenceEntry) -> None:
        generation, fence = self._serving_generation, entry.fence

        async def run() -> None:
            async with entry.lock:
                if self._phase != "ready" or entry.accepted is None:
                    return
                self._require_ready(entry, generation=generation, fence=fence)
                # Capture the latest accepted bytes while holding the publication lock.
                await self._write(entry, self._registration(entry, entry.accepted.capabilities), generation=generation, fence=fence)

        try:
            await self._operation(run())
        except Exception:
            logger.exception("Worker presence renewal failed for worker %s on %s", self._worker_id, entry.job)

    async def _renewal_loop(self) -> None:
        interval = self._resolve_config().renewal_interval.total_seconds()
        while self._phase == "ready":
            await asyncio.sleep(interval)
            if self._phase != "ready":
                return
            await asyncio.gather(*(self._renew_entry(entry) for entry in self._entries.values()))

    def start_renewal(self) -> None:
        self._phase = "ready"
        if self._enabled and self._entries and self._renewal_task is None:
            self._renewal_task = asyncio.create_task(self._renewal_loop(), name="superjobs-presence-renewal")

    async def stop_renewal(self) -> None:
        task = self._renewal_task
        self._renewal_task = None
        if task is not None:
            task.cancel()
            await asyncio.wait({task}, timeout=self._resolve_config().read_timeout.total_seconds())
            task.add_done_callback(self._consume_task_result)

    async def _cancel_operations(self) -> None:
        tasks = set(self._operations)
        for task in tasks:
            if not task.cancelling():
                task.cancel()
        if tasks:
            _, pending = await asyncio.wait(tasks, timeout=self._resolve_config().read_timeout.total_seconds())
            if pending:
                raise DiscoveryUnavailableError("In-flight worker presence operations did not stop")
        children = set(self._children)
        for child in children:
            if not child.cancelling():
                child.cancel()
        if children:
            _, pending = await asyncio.wait(children, timeout=self._resolve_config().read_timeout.total_seconds())
            if pending:
                raise DiscoveryUnavailableError("Owned capability or registry tasks did not stop")

    async def shutdown(self, *, stop_admission: Callable[[], Awaitable[None]]) -> list[BaseException]:
        errors: list[BaseException] = []
        self._phase = "stopping"
        for entry in self._entries.values():
            entry.fence += 1
        await self.stop_renewal()
        try:
            await self._cancel_operations()
        except Exception as error:
            errors.append(error)
        if self._enabled:
            for entry in self._entries.values():
                async def drain(entry: PresenceEntry = entry) -> None:
                    async with entry.lock:
                        if entry.accepted is not None:
                            now = utc_now(self._clock)
                            registration = replace(entry.accepted, state=WorkerRegistrationState.DRAINING, last_seen_at=now, expires_at=now + self._resolve_config().lease_timeout)
                            await self._write(entry, registration, generation=entry.generation, fence=entry.fence)
                try:
                    await self._bounded(drain(), description="Worker draining")
                except Exception as error:
                    logger.exception("Worker draining failed for %s", entry.job)
                    errors.append(error)
        try:
            await stop_admission()
        except BaseException as error:
            errors.append(error)
        if self._enabled:
            for entry in self._entries.values():
                async def delete(entry: PresenceEntry = entry) -> None:
                    async with entry.lock:
                        if entry.attempted_generations:
                            await self._delete(entry)
                try:
                    await self._bounded(delete(), description="Worker deregistration")
                except Exception as error:
                    logger.exception("Worker deregistration failed for %s", entry.job)
                    errors.append(error)
        self._phase = "stopped"
        return errors

    async def begin_serving_generation(self) -> int:
        self._serving_generation += 1
        self._phase = "starting"
        return self._serving_generation
