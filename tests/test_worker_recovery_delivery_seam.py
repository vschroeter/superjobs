"""Unit checks for worker recovery delivery ack ordering (no NATS)."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from superjobs.jobs.execution import JobState
from superjobs.jobs.job_identity import JobIdentity
from superjobs.transport.backend import ExecutionRecord
from superjobs.transport.nats_backend import NatsJobBackend

from superjobs.transport.implementations.nats import NatsDelivery

from tools.worker_recovery_support.blocking_backend import (
    BlockingAfterCompletionBackend,
    BlockingAfterRetryPublicationBackend,
)
from tools.worker_recovery_support.delivery_seam import (
    _AckCheckpointDelivery,
    _AckCheckpointWorkSubscription,
)
from tools.worker_recovery_support import protocol as wr_protocol


class _FakeDelivery:
    def __init__(self) -> None:
        self.acked = False

    @property
    def attempt(self) -> int:
        return 1

    async def ack(self) -> None:
        self.acked = True

    async def retry(
        self,
        *,
        delay: timedelta | None = None,
        attempt: int | None = None,
    ) -> None:
        raise NotImplementedError

    async def reject(self, *, reason: str | None = None) -> None:
        raise NotImplementedError

    async def extend_lease(self) -> None:
        raise NotImplementedError


class _FakeBackend:
    def __init__(self, execution: ExecutionRecord) -> None:
        self._execution = execution

    async def get_execution(
        self,
        identity: JobIdentity,
        job_id: str,
    ) -> ExecutionRecord | None:
        return self._execution


@pytest.mark.asyncio
async def test_replacement_ack_written_only_after_delivery_ack(tmp_path: Path) -> None:
    identity = JobIdentity(name="job")
    execution = ExecutionRecord(
        identity=identity,
        job_id="exec-1",
        request_payload=b"{}",
        request_media_type="application/json",
        fingerprint="fp",
        idempotency_key="key",
        caller_scope="default",
        timeout=None,
        deadline=None,
        created_at=datetime.now(UTC),
        state=JobState.COMPLETED,
        terminal_event_published=True,
    )
    inner = _FakeDelivery()
    delivery = _AckCheckpointDelivery(
        inner,
        backend=_FakeBackend(execution),
        identity=identity,
        job_id="exec-1",
        state_dir=tmp_path,
        run_id="run-1",
        worker_generation="2",
        worker_pid=4242,
    )
    assert not (tmp_path / "checkpoint_replacement_ack.json").is_file()
    await delivery.ack()
    assert inner.acked
    marker = wr_protocol.read_checkpoint(tmp_path, wr_protocol.REPLACEMENT_ACK)
    assert marker.execution_id == "exec-1"
    assert marker.pid == 4242
    assert marker.terminal_state == "COMPLETED"
    assert marker.terminal_event_published is True


class _GatedFakeDelivery:
    def __init__(self) -> None:
        self.gate = asyncio.Event()
        self.entered = asyncio.Event()
        self.acked = False

    @property
    def attempt(self) -> int:
        return 1

    async def ack(self) -> None:
        self.entered.set()
        await self.gate.wait()
        self.acked = True

    async def retry(
        self,
        *,
        delay: timedelta | None = None,
        attempt: int | None = None,
    ) -> None:
        raise NotImplementedError

    async def reject(self, *, reason: str | None = None) -> None:
        raise NotImplementedError

    async def extend_lease(self) -> None:
        raise NotImplementedError


class _RaisingFakeDelivery:
    @property
    def attempt(self) -> int:
        return 1

    async def ack(self) -> None:
        raise RuntimeError("ack failed")

    async def retry(
        self,
        *,
        delay: timedelta | None = None,
        attempt: int | None = None,
    ) -> None:
        raise NotImplementedError

    async def reject(self, *, reason: str | None = None) -> None:
        raise NotImplementedError

    async def extend_lease(self) -> None:
        raise NotImplementedError


@pytest.mark.asyncio
async def test_replacement_ack_blocked_until_inner_ack_completes(tmp_path: Path) -> None:
    identity = JobIdentity(name="job")
    execution = ExecutionRecord(
        identity=identity,
        job_id="exec-1",
        request_payload=b"{}",
        request_media_type="application/json",
        fingerprint="fp",
        idempotency_key="key",
        caller_scope="default",
        timeout=None,
        deadline=None,
        created_at=datetime.now(UTC),
        state=JobState.COMPLETED,
        terminal_event_published=True,
    )
    inner = _GatedFakeDelivery()
    delivery = _AckCheckpointDelivery(
        inner,
        backend=_FakeBackend(execution),
        identity=identity,
        job_id="exec-1",
        state_dir=tmp_path,
        run_id="run-1",
        worker_generation="2",
        worker_pid=4242,
    )
    ack_task = asyncio.create_task(delivery.ack())
    await asyncio.wait_for(inner.entered.wait(), 1)
    assert not (tmp_path / "checkpoint_replacement_ack.json").is_file()
    inner.gate.set()
    await ack_task
    assert inner.acked
    assert (tmp_path / "checkpoint_replacement_ack.json").is_file()


@pytest.mark.asyncio
async def test_replacement_ack_absent_when_inner_ack_raises(tmp_path: Path) -> None:
    identity = JobIdentity(name="job")
    execution = ExecutionRecord(
        identity=identity,
        job_id="exec-1",
        request_payload=b"{}",
        request_media_type="application/json",
        fingerprint="fp",
        idempotency_key="key",
        caller_scope="default",
        timeout=None,
        deadline=None,
        created_at=datetime.now(UTC),
        state=JobState.COMPLETED,
        terminal_event_published=True,
    )
    delivery = _AckCheckpointDelivery(
        _RaisingFakeDelivery(),
        backend=_FakeBackend(execution),
        identity=identity,
        job_id="exec-1",
        state_dir=tmp_path,
        run_id="run-1",
        worker_generation="2",
        worker_pid=4242,
    )
    with pytest.raises(RuntimeError, match="ack failed"):
        await delivery.ack()
    assert not (tmp_path / "checkpoint_replacement_ack.json").is_file()


@pytest.mark.asyncio
async def test_ack_checkpoint_subscription_close_delegates(tmp_path: Path) -> None:
    inner = MagicMock()
    inner.close = AsyncMock()
    subscription = _AckCheckpointWorkSubscription(
        inner,
        backend=MagicMock(),
        identity=JobIdentity(name="job"),
        state_dir=tmp_path,
        run_id="run-1",
        worker_generation="2",
        worker_pid=1,
    )
    await subscription.close()
    inner.close.assert_awaited_once()


def _sample_execution() -> ExecutionRecord:
    identity = JobIdentity(name="job")
    return ExecutionRecord(
        identity=identity,
        job_id="exec-1",
        request_payload=b"{}",
        request_media_type="application/json",
        fingerprint="fp",
        idempotency_key="key",
        caller_scope="default",
        timeout=None,
        deadline=None,
        created_at=datetime.now(UTC),
        state=JobState.RUNNING,
        terminal_event_published=False,
    )


@pytest.mark.asyncio
async def test_completion_saved_only_after_delegated_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPERJOBS_CROSS_STATE_DIR", str(tmp_path))
    monkeypatch.setenv("SUPERJOBS_CROSS_RUN_ID", "run-1")
    monkeypatch.setenv("SUPERJOBS_WORKER_GENERATION", "1")
    gate = asyncio.Event()
    entered = asyncio.Event()
    checkpoint_written = asyncio.Event()
    from tools.worker_recovery_support import blocking_backend as blocking_module
    original_checkpoint = blocking_module.write_checkpoint

    def record_checkpoint(*args, **kwargs):
        original_checkpoint(*args, **kwargs)
        checkpoint_written.set()

    monkeypatch.setattr(blocking_module, "write_checkpoint", record_checkpoint)
    execution = _sample_execution()
    updated = ExecutionRecord(
        identity=execution.identity,
        job_id=execution.job_id,
        request_payload=execution.request_payload,
        request_media_type=execution.request_media_type,
        fingerprint=execution.fingerprint,
        idempotency_key=execution.idempotency_key,
        caller_scope=execution.caller_scope,
        timeout=execution.timeout,
        deadline=execution.deadline,
        created_at=execution.created_at,
        state=JobState.COMPLETED,
        terminal_event_published=False,
    )

    async def delegated_write(
        self,
        execution: ExecutionRecord,
        *,
        state: JobState,
        result_payload: bytes = b"",
        result_media_type: str | None = None,
        error=None,
    ) -> ExecutionRecord:
        entered.set()
        await gate.wait()
        return updated

    backend = BlockingAfterCompletionBackend(MagicMock(), None)
    with patch.object(NatsJobBackend, "write_completion", delegated_write):
        task = asyncio.create_task(
            backend.write_completion(execution, state=JobState.COMPLETED),
        )
        await asyncio.wait_for(entered.wait(), 1)
        assert not wr_protocol.checkpoint_path(tmp_path, wr_protocol.COMPLETION_SAVED).is_file()
        gate.set()
        await asyncio.wait_for(checkpoint_written.wait(), 1)
        marker = wr_protocol.read_checkpoint(tmp_path, wr_protocol.COMPLETION_SAVED)
        assert marker.completion_state == "COMPLETED"
        assert marker.terminal_event_published is False
        assert marker.execution_id == "exec-1"
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_retry_published_only_after_delegated_publish_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPERJOBS_CROSS_STATE_DIR", str(tmp_path))
    monkeypatch.setenv("SUPERJOBS_CROSS_RUN_ID", "run-1")
    monkeypatch.setenv("SUPERJOBS_WORKER_GENERATION", "1")
    gate = asyncio.Event()
    entered = asyncio.Event()
    checkpoint_written = asyncio.Event()
    from tools.worker_recovery_support import blocking_backend as blocking_module
    original_checkpoint = blocking_module.write_checkpoint

    def record_checkpoint(*args, **kwargs):
        original_checkpoint(*args, **kwargs)
        checkpoint_written.set()

    monkeypatch.setattr(blocking_module, "write_checkpoint", record_checkpoint)
    execution = _sample_execution()

    async def delegated_publish_retry(
        self,
        execution: ExecutionRecord,
        attempt: int,
        delay: timedelta | None,
    ) -> None:
        entered.set()
        await gate.wait()

    backend = BlockingAfterRetryPublicationBackend(MagicMock(), None)
    backend._active_delivery_attempt = 1
    with patch.object(NatsJobBackend, "_publish_retry", delegated_publish_retry):
        task = asyncio.create_task(
            backend._publish_retry(execution, 2, timedelta(0)),
        )
        await asyncio.wait_for(entered.wait(), 1)
        assert not wr_protocol.checkpoint_path(tmp_path, wr_protocol.RETRY_PUBLISHED).is_file()
        gate.set()
        await asyncio.wait_for(checkpoint_written.wait(), 1)
        marker = wr_protocol.read_checkpoint(tmp_path, wr_protocol.RETRY_PUBLISHED)
        assert marker.execution_id == "exec-1"
        assert marker.delivery_attempt == 1
        assert marker.retry_target_attempt == 2
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_native_delivery_ack_blocked_while_retry_publication_seam_blocks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPERJOBS_CROSS_STATE_DIR", str(tmp_path))
    monkeypatch.setenv("SUPERJOBS_CROSS_RUN_ID", "run-1")
    monkeypatch.setenv("SUPERJOBS_WORKER_GENERATION", "1")
    execution = _sample_execution()
    backend = BlockingAfterRetryPublicationBackend(MagicMock(), None)
    backend._active_delivery_attempt = 1
    publish_entered = asyncio.Event()

    async def immediate_publish_retry(
        self,
        execution: ExecutionRecord,
        attempt: int,
        delay: timedelta | None,
    ) -> None:
        publish_entered.set()

    message = MagicMock()
    message.ack_sync = AsyncMock()
    with patch.object(NatsJobBackend, "_publish_retry", immediate_publish_retry):
        async def publisher(attempt: int, delay: timedelta | None) -> None:
            await backend._publish_retry(execution, attempt, delay)

        delivery = NatsDelivery(message, attempt=1, retry_publisher=publisher)
        retry_task = asyncio.create_task(
            delivery.retry(delay=timedelta(0), attempt=2),
        )
        await asyncio.wait_for(publish_entered.wait(), 1)
        marker = wr_protocol.read_checkpoint(tmp_path, wr_protocol.RETRY_PUBLISHED)
        assert marker.delivery_attempt == 1
        message.ack_sync.assert_not_awaited()
        retry_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await retry_task
        message.ack_sync.assert_not_awaited()


@pytest.mark.asyncio
async def test_retry_published_absent_when_delegated_publish_retry_raises(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPERJOBS_CROSS_STATE_DIR", str(tmp_path))
    monkeypatch.setenv("SUPERJOBS_CROSS_RUN_ID", "run-1")
    monkeypatch.setenv("SUPERJOBS_WORKER_GENERATION", "1")
    execution = _sample_execution()
    backend = BlockingAfterRetryPublicationBackend(MagicMock(), None)
    backend._active_delivery_attempt = 1

    async def failing_publish_retry(
        self,
        execution: ExecutionRecord,
        attempt: int,
        delay: timedelta | None,
    ) -> None:
        raise RuntimeError("publish retry failed")

    with patch.object(NatsJobBackend, "_publish_retry", failing_publish_retry):
        with pytest.raises(RuntimeError, match="publish retry failed"):
            await backend._publish_retry(execution, 2, timedelta(0))
    assert not wr_protocol.checkpoint_path(tmp_path, wr_protocol.RETRY_PUBLISHED).is_file()
