"""Worker-owned residency with bounded idle cleanup and fenced handoff."""

from __future__ import annotations

import asyncio
import math
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from superjobs.resources import AcquisitionOutcome, AdmissionOwner, ReleaseOutcome
from superjobs.resources.errors import ResourceStaleOperationError

if TYPE_CHECKING:
    from superjobs.jobs.admission_execution import ResourceHandlerConfig
    from superjobs.resources import AcquireResult, AdmissionAcquireIntent

Cleanup = Callable[[], Awaitable[None]]


@dataclass
class ResidentResource:
    owner: AdmissionOwner
    config: ResourceHandlerConfig
    key: str
    cleanup: Cleanup
    idle_timeout: float
    idle_since: float | None = None
    ready: bool = True
    failure: BaseException | None = None
    cleaned: bool = False
    next_check: float = 0


class ResourceResidency:
    def __init__(self, coordinator: Any) -> None:
        self.coordinator = coordinator
        self.entries: dict[str, ResidentResource] = {}
        self.lock = asyncio.Lock()
        self.task: asyncio.Task[None] | None = None
        self.closing = False

    def _start(self) -> None:
        self.closing = False
        if self.task is None or self.task.done():
            self.task = asyncio.create_task(self._maintain(), name="superjobs-resource-residency")

    async def acquire(
        self, config: ResourceHandlerConfig, intent: AdmissionAcquireIntent
    ) -> AcquireResult:
        async with self.lock:
            entry = self.entries.get(intent.resource_key)
            if entry is None:
                return await config.admission.try_acquire(intent)
            if entry.failure is not None:
                raise entry.failure
            if entry.idle_since is None:
                # Another local handler still consumes or finalizes this resource.
                from superjobs.resources import AcquireResult

                return AcquireResult(outcome=AcquisitionOutcome.CONTENTION)
            try:
                record = await config.admission.reconcile_retained(entry.owner)
            except ResourceStaleOperationError:
                # A handoff CAS may have committed before its reply was lost.
                result = await config.admission.acquire_retained(intent, entry.owner)
                if result.outcome is not AcquisitionOutcome.ACQUIRED:
                    raise
                assert result.owner is not None
                entry.owner, entry.config, entry.idle_since = result.owner, config, None
                return result
            expired = time.monotonic() - entry.idle_since >= entry.idle_timeout
            if record.release_requested or expired or entry.config is not config:
                await self._release(entry)
                # Give an already waiting contender a turn before reacquiring.
                from superjobs.resources import AcquireResult

                return AcquireResult(outcome=AcquisitionOutcome.CONTENTION)
            result = await config.admission.acquire_retained(intent, entry.owner)
            if result.outcome in {AcquisitionOutcome.ACQUIRED, AcquisitionOutcome.ALREADY_HELD}:
                assert result.owner is not None
                entry.owner = result.owner
                entry.config = config
                entry.idle_since = None
            return result

    def reused(self, owner: AdmissionOwner, key: str) -> bool:
        entry = self.entries.get(owner.resource_key)
        if entry is None:
            return False
        if entry.owner != owner or entry.failure or not entry.ready:
            raise ResourceStaleOperationError(
                "Resident resource is not available to this invocation"
            )
        return entry.key == key

    def offer(
        self,
        config: ResourceHandlerConfig,
        owner: AdmissionOwner,
        key: str,
        cleanup: Cleanup,
        idle_timeout: float,
    ) -> None:
        if not isinstance(key, str) or not key.strip() or len(key) > 512:
            raise ValueError("residency key must contain 1 to 512 characters")
        if (
            isinstance(idle_timeout, bool)
            or not math.isfinite(idle_timeout)
            or not 0 < idle_timeout <= 300
        ):
            raise ValueError("idle_timeout must be finite and within (0, 300] seconds")
        if not callable(cleanup):
            raise TypeError("cleanup must be an asynchronous callable")
        entry = self.entries.get(owner.resource_key)
        if entry is not None and (entry.owner != owner or entry.key != key):
            raise RuntimeError(
                "Use prepare_resource to clean up before changing resident resources"
            )
        self.entries[owner.resource_key] = ResidentResource(
            owner, config, key, cleanup, idle_timeout
        )
        self._start()

    async def prepare(
        self,
        config: ResourceHandlerConfig,
        owner: AdmissionOwner,
        key: str,
        prepare: Cleanup,
        cleanup: Cleanup,
        idle_timeout: float,
    ) -> bool:
        await config.admission.assert_owner_active(owner)
        entry = self.entries.get(owner.resource_key)
        if entry is not None and entry.owner == owner and entry.key != key:
            await entry.cleanup()
            del self.entries[owner.resource_key]
            entry = None
        reused = entry is not None and self.reused(owner, key)
        self.offer(config, owner, key, cleanup, idle_timeout)
        if not reused:
            current = self.entries[owner.resource_key]
            current.ready = False
            await prepare()
            current.ready = True
        return reused

    async def park(self, config: ResourceHandlerConfig, owner: AdmissionOwner) -> None:
        entry = self.entries[owner.resource_key]
        await config.admission.retain_resource(owner, entry.key)

    def completed(self, owner: AdmissionOwner) -> None:
        entry = self.entries.get(owner.resource_key)
        if entry is not None and entry.owner == owner:
            entry.idle_since = time.monotonic()
            self._start()

    def forget(self, owner: AdmissionOwner) -> None:
        entry = self.entries.get(owner.resource_key)
        if entry is not None and entry.owner == owner:
            del self.entries[owner.resource_key]

    async def clean_active(self, owner: AdmissionOwner) -> None:
        entry = self.entries.get(owner.resource_key)
        if entry is not None and entry.owner == owner:
            try:
                await entry.cleanup()
            except BaseException as error:
                entry.failure = error
                await entry.config.admission.retain_cleanup_failed(owner, reason_detail=str(error))
                await self.coordinator.withdraw_for_resource(owner.resource_key)
                raise
            self.forget(owner)

    async def _release(self, entry: ResidentResource) -> None:
        if entry.failure:
            raise entry.failure
        try:
            if not entry.cleaned:
                await entry.cleanup()
                entry.cleaned = True
        except BaseException as error:
            entry.failure = error
            try:
                await entry.config.admission.retain_cleanup_failed(
                    entry.owner, reason_detail=str(error)
                )
            finally:
                await self.coordinator.withdraw_for_resource(entry.owner.resource_key)
            raise
        result = await entry.config.admission.release_retained_after_cleanup(entry.owner)
        if result.outcome is not ReleaseOutcome.RELEASED:
            raise ResourceStaleOperationError("Retained resource release token is stale")
        self.forget(entry.owner)

    async def _maintain(self) -> None:
        while self.entries and not self.closing:
            await asyncio.sleep(0.05)
            async with self.lock:
                for entry in tuple(self.entries.values()):
                    if entry.idle_since is None or entry.failure:
                        continue
                    try:
                        current_time = time.monotonic()
                        if current_time - entry.idle_since >= entry.idle_timeout:
                            await self._release(entry)
                            continue
                        if current_time < entry.next_check:
                            continue
                        record = await entry.config.admission.reconcile_retained(entry.owner)
                        entry.next_check = current_time + 0.5
                        if (
                            record.release_requested
                            or time.monotonic() - entry.idle_since >= entry.idle_timeout
                        ):
                            await self._release(entry)
                    except Exception:
                        # An uncertain broker withdraws reuse immediately. Actual
                        # teardown is still required before any ownership release.
                        await self.coordinator.withdraw_for_resource(entry.owner.resource_key)
                        if entry.failure is None:
                            try:
                                await self._release(entry)
                            except Exception:
                                pass

    async def close(self) -> None:
        self.closing = True
        if self.task:
            # Drain an in-flight native cleanup instead of cancelling its wrapper.
            await asyncio.gather(self.task, return_exceptions=True)
            self.task = None
        async with self.lock:
            for entry in tuple(self.entries.values()):
                await self._release(entry)
