"""Deterministic checks for tools/verify_resource_admission.py support modules."""

from __future__ import annotations

import time
import uuid
from pathlib import Path

import pytest

from tools.resource_admission_support import protocol as ra_protocol
from tools.resource_admission_support.api_compat import collect_requirements
from tools.resource_admission_support.evidence import (
    append_event,
    read_events,
    summarize_holder_overlap,
)
from tools.resource_admission_support.observation_collector import assert_holder_evidence
from tools.resource_admission_support.orchestration import phase_deadline, ready_deadline


def test_append_only_holder_evidence_tracks_overlap(tmp_path: Path) -> None:
    run_id = uuid.uuid4().hex
    append_event(
        tmp_path,
        kind="holder_enter",
        run_id=run_id,
        execution_id="exec-a",
        job="a",
    )
    append_event(
        tmp_path,
        kind="holder_enter",
        run_id=run_id,
        execution_id="exec-b",
        job="b",
    )
    summary = summarize_holder_overlap(read_events(tmp_path))
    assert summary.max_concurrent == 2
    append_event(
        tmp_path,
        kind="holder_exit",
        run_id=run_id,
        execution_id="exec-a",
        job="a",
    )
    summary = summarize_holder_overlap(read_events(tmp_path))
    assert summary.max_concurrent == 2
    assert summary.enter_count == 2
    assert summary.exit_count == 1


def test_checkpoint_waits_for_expected_execution_id(tmp_path: Path) -> None:
    run_id = uuid.uuid4().hex
    ra_protocol.write_execution_checkpoint(
        tmp_path,
        run_id=run_id,
        checkpoint=ra_protocol.MODEL_LOAD_ENTERED,
        execution_id="exec-old",
        job="a",
    )
    deadline = time.monotonic() + 0.2
    with pytest.raises(ra_protocol.ProtocolError, match="not observed"):
        ra_protocol.wait_for_checkpoint(
            tmp_path,
            name=ra_protocol.MODEL_LOAD_ENTERED,
            expected_run_id=run_id,
            expected_execution_id="exec-new",
            deadline=deadline,
            poll_interval=0.01,
        )


def test_checkpoint_rejects_wrong_run_id(tmp_path: Path) -> None:
    run_id = uuid.uuid4().hex
    ra_protocol.write_checkpoint(
        tmp_path,
        run_id=run_id,
        checkpoint=ra_protocol.SUBMITTED,
        job="a",
        execution_id="exec-1",
    )
    with pytest.raises(ra_protocol.ProtocolError, match="run_id"):
        ra_protocol.wait_for_checkpoint(
            tmp_path,
            name=ra_protocol.SUBMITTED,
            expected_run_id="other",
            job="a",
            deadline=time.monotonic() + 0.5,
            poll_interval=0.01,
        )


def test_ready_deadline_is_absolute_monotonic() -> None:
    scenario_deadline = time.monotonic() + 60.0
    ready = ready_deadline(scenario_deadline, kill_reap=5.0, cap_seconds=30.0)
    assert ready > time.monotonic()
    assert ready <= scenario_deadline - 5.0


def test_phase_deadline_is_absolute() -> None:
    scenario_deadline = time.monotonic() + 10.0
    assert phase_deadline(scenario_deadline, 2.0) == scenario_deadline - 2.0


def test_per_generation_ready_markers(tmp_path: Path) -> None:
    run_id = uuid.uuid4().hex
    ra_protocol.write_ready(tmp_path, run_id, pid=100, worker_generation="a")
    ra_protocol.write_ready(tmp_path, run_id, pid=200, worker_generation="b")
    marker_a = ra_protocol.read_ready(tmp_path, worker_generation="a")
    marker_b = ra_protocol.read_ready(tmp_path, worker_generation="b")
    assert marker_a.pid == 100
    assert marker_b.pid == 200


def test_observation_collector_asserts_positive_counts() -> None:
    from tools.resource_admission_support.evidence import HolderOverlapSummary

    assert_holder_evidence(
        HolderOverlapSummary(max_concurrent=1, enter_count=2, exit_count=2),
        maximum=1,
        minimum_enters=2,
    )
    with pytest.raises(ValueError, match="overlap"):
        assert_holder_evidence(
            HolderOverlapSummary(max_concurrent=2, enter_count=2, exit_count=2),
            maximum=1,
        )


def test_api_compat_reports_missing_integration() -> None:
    report = collect_requirements()
    assert "errors" in report
    assert "ok" in report
    if not report["ok"]:
        assert report["errors"]


async def test_control_release_waits_for_target_acknowledgement_after_reconnect(monkeypatch) -> None:
    import json
    from types import SimpleNamespace
    from unittest.mock import AsyncMock
    import nats
    from tools.resource_admission_support.nats_control import publish_control

    connection = SimpleNamespace(
        request=AsyncMock(side_effect=[
            nats.errors.NoRespondersError(),
            nats.errors.TimeoutError(),
            SimpleNamespace(data=json.dumps({"released": "wrong-invocation"}).encode()),
            SimpleNamespace(data=json.dumps({"released": "active-invocation"}).encode()),
        ]),
        drain=AsyncMock(),
    )
    monkeypatch.setattr(nats, "connect", AsyncMock(return_value=connection))
    await publish_control("nats://test", "control", {"type": "release_holder", "execution_id": "active-invocation"}, timeout=2)
    assert connection.request.await_count == 4
    connection.drain.assert_awaited_once()
