from __future__ import annotations

import asyncio
import json
import os
import subprocess
import uuid
from pathlib import Path

import pytest

from superjobs import HandlerCatalog, JobState, JobSucceeded, SuperJobs
from superjobs.jobs.execution import JobFailedOutcome
from superjobs.resources import ReleaseOutcome, TerminationConfirmation
from superjobs.resources.models import ResourceAvailability
from tests.support.shared_resource_namespace.constants import HOLD_JOB, RESOURCE_KEY
from tests.support.shared_resource_namespace.orchestration import (
    admission_bucket_for_config,
    assert_snapshots_agree_on_owner,
    claim_for_execution,
    entered_marker_present,
    observer_jobs,
    owner_for_key,
    publish_release,
    resource_for_key,
    spawn_worker,
    stop_jobs,
    store_bucket_name,
    wait_for_entered,
    wait_for_job_state,
    wait_for_ready,
)
from tests.support.shared_resource_namespace.process_proof import (
    ProcessProofError,
    audit_no_worker_entry_processes,
    finalize_owned_worker,
    load_ready_marker,
    ready_marker_from_payload,
)
from tests.support.shared_resource_namespace.queue_config import (
    SHARED_RESOURCE_NAMESPACE,
    queue_config_for_work_prefix,
)

pytestmark = pytest.mark.nats

@pytest.fixture
def process_exit_evidence_dir(request, tmp_path: Path) -> Path:
    # Normal test runs keep generated evidence outside the checkout.
    root = Path(os.environ.get("SJ_SHARED_NAMESPACE_EVIDENCE_DIR", tmp_path / "process-exits"))
    path = root / request.node.name.removeprefix("test_shared_resource_namespace_")
    path.mkdir(parents=True, exist_ok=True)
    return path


def _cleanup_owned_workers(
    *,
    workers: dict[str, subprocess.Popen[str] | None],
    state_dir: Path,
    deadline: float,
    evidence_dir: Path,
    hard_labels: frozenset[str] = frozenset(),
    skip_stop_labels: frozenset[str] = frozenset(),
) -> None:
    errors = []
    for label, process in workers.items():
        if process is None:
            continue
        try:
            marker = load_ready_marker(state_dir, label)
        except ProcessProofError:
            marker = None
        try:
            finalize_owned_worker(
                process, state_dir=state_dir, worker_label=label, deadline=deadline,
                marker=marker, hard=label in hard_labels,
                signal_stop=label not in skip_stop_labels, evidence_dir=evidence_dir,
            )
        except Exception as exc:
            errors.append(exc)
    if errors:
        raise ExceptionGroup("Owned test worker cleanup failed", errors)


def _finalize_test_worker(
    process: subprocess.Popen[str] | None,
    *,
    state_dir: Path,
    label: str,
    deadline: float,
    evidence_dir: Path,
    marker_payload: dict | None = None,
    hard: bool = False,
    signal_stop: bool = True,
):
    if process is None:
        return None
    marker = (
        ready_marker_from_payload(marker_payload) if marker_payload is not None else None
    )
    return finalize_owned_worker(
        process,
        state_dir=state_dir,
        worker_label=label,
        deadline=deadline,
        marker=marker,
        hard=hard,
        signal_stop=signal_stop,
        evidence_dir=evidence_dir,
    )


@pytest.fixture
def shared_ns_run_id() -> str:
    return uuid.uuid4().hex[:12]


@pytest.fixture
def shared_ns_state_dir(tmp_path: Path, shared_ns_run_id: str) -> Path:
    path = tmp_path / f"shared_ns_{shared_ns_run_id}"
    path.mkdir()
    return path


@pytest.mark.asyncio
async def test_shared_resource_namespace_cross_process_exclusion(
    nats_owned_server,
    shared_ns_run_id: str,
    shared_ns_state_dir: Path,
    process_exit_evidence_dir: Path,
) -> None:
    _owner, target = nats_owned_server
    nats_url = target.url
    prefix_a = f"work_a_{shared_ns_run_id}"
    prefix_b = f"work_b_{shared_ns_run_id}"
    config_a = queue_config_for_work_prefix(prefix_a)
    config_b = queue_config_for_work_prefix(prefix_b)
    assert admission_bucket_for_config(config_a) == admission_bucket_for_config(config_b)

    worker_a: subprocess.Popen[str] | None = None
    worker_b: subprocess.Popen[str] | None = None
    observer_a: SuperJobs | None = None
    observer_b: SuperJobs | None = None
    ready_a: dict | None = None
    ready_b: dict | None = None
    deadline = asyncio.get_running_loop().time() + 35.0
    try:
        worker_a = spawn_worker(
            nats_url=nats_url,
            work_prefix=prefix_a,
            worker_label="a",
            state_dir=shared_ns_state_dir,
            resource_namespace=SHARED_RESOURCE_NAMESPACE,
        )
        worker_b = spawn_worker(
            nats_url=nats_url,
            work_prefix=prefix_b,
            worker_label="b",
            state_dir=shared_ns_state_dir,
            resource_namespace=SHARED_RESOURCE_NAMESPACE,
        )
        ready_a = wait_for_ready(shared_ns_state_dir, "a", deadline=deadline)
        ready_b = wait_for_ready(shared_ns_state_dir, "b", deadline=deadline)
        assert ready_a["pid"] != ready_b["pid"]

        observer_a = await observer_jobs(nats_url, config_a)
        observer_b = await observer_jobs(nats_url, config_b)
        assert await store_bucket_name(observer_a) == await store_bucket_name(observer_b)

        handle_a = await observer_a.client(HOLD_JOB).submit()
        entered_a = wait_for_entered(shared_ns_state_dir, "a", deadline=deadline)
        assert entered_a["execution_id"] == handle_a.id

        handle_b = await observer_b.client(HOLD_JOB).submit()
        await wait_for_job_state(
            handle_b,
            JobState.WAITING_FOR_RESOURCE,
            deadline=deadline,
        )
        observe_until = min(deadline, asyncio.get_running_loop().time() + 1.0)
        while asyncio.get_running_loop().time() < observe_until:
            assert not entered_marker_present(shared_ns_state_dir, "b")
            snap_a = await observer_a.resources.snapshot()
            snap_b = await observer_b.resources.snapshot()
            owner_a = assert_snapshots_agree_on_owner(
                snap_a,
                snap_b,
                resource_key=RESOURCE_KEY,
            )
            assert claim_for_execution(snap_a, handle_a.id) is not None
            assert claim_for_execution(snap_b, handle_a.id) is not None
            await asyncio.sleep(0.05)

        await publish_release(nats_url, config_a, handle_a.id)
        outcome_a = await asyncio.wait_for(handle_a.outcome(), timeout=15.0)
        assert isinstance(outcome_a, JobSucceeded)
        assert outcome_a.result == "a"

        entered_b = wait_for_entered(shared_ns_state_dir, "b", deadline=deadline)
        assert entered_b["execution_id"] == handle_b.id
        assert entered_b["pid"] == ready_b["pid"]
        await publish_release(nats_url, config_b, handle_b.id)
        outcome_b = await asyncio.wait_for(handle_b.outcome(), timeout=15.0)
        assert isinstance(outcome_b, JobSucceeded)
        assert outcome_b.result == "b"

        final_a = await observer_a.resources.snapshot()
        final_b = await observer_b.resources.snapshot()
        assert owner_for_key(final_a, RESOURCE_KEY) is None
        assert owner_for_key(final_b, RESOURCE_KEY) is None
        assert not final_a.execution_claims
        assert not final_b.execution_claims
        process_exit_evidence_dir.mkdir(parents=True, exist_ok=True)
        (process_exit_evidence_dir / "coordination.json").write_text(
            json.dumps({
                "work_prefix_a": prefix_a, "work_prefix_b": prefix_b,
                "resource_namespace": SHARED_RESOURCE_NAMESPACE,
                "actual_shared_admission_bucket": await store_bucket_name(observer_a),
                "physical_cpu_key": RESOURCE_KEY,
                "owner_a": owner_a.model_dump(mode="json"),
                "worker_a_pid": ready_a["pid"], "worker_b_pid": ready_b["pid"],
                "result_a": outcome_a.result, "result_b": outcome_b.result,
                "final_owner_count": 0, "final_execution_claim_count": 0,
            }, indent=2), encoding="utf8",
        )
    finally:
        end = asyncio.get_running_loop().time() + 10.0
        _cleanup_owned_workers(
            workers={"a": worker_a, "b": worker_b},
            state_dir=shared_ns_state_dir,
            deadline=end,
            evidence_dir=process_exit_evidence_dir,
        )
        if observer_a is not None:
            try:
                await stop_jobs(observer_a)
            except Exception:
                pass
        if observer_b is not None:
            try:
                await stop_jobs(observer_b)
            except Exception:
                pass


@pytest.mark.asyncio
async def test_shared_resource_namespace_crash_recovery_across_work_prefixes(
    nats_owned_server,
    shared_ns_run_id: str,
    shared_ns_state_dir: Path,
    process_exit_evidence_dir: Path,
) -> None:
    """ReleaseOutcome.STALE vs NOT_OWNER (public contract, not changed here).

    ``release_resource_after_cleanup`` returns ``STALE`` when the resource record
    is owned by a different active ``AdmissionOwner`` (including higher fencing).
    It returns ``NOT_OWNER`` when the resource is already unowned *and* the caller's
    execution claim no longer matchesâ€”typically after confirmed termination removed
    the stale claim while the resource became available. This test requires ``STALE``
    while worker B legitimately holds the device.
    """
    _owner, target = nats_owned_server
    nats_url = target.url
    prefix_a = f"work_a_{shared_ns_run_id}"
    prefix_b = f"work_b_{shared_ns_run_id}"
    config_a = queue_config_for_work_prefix(prefix_a)
    config_b = queue_config_for_work_prefix(prefix_b)

    worker_a: subprocess.Popen[str] | None = None
    worker_b: subprocess.Popen[str] | None = None
    observer_a: SuperJobs | None = None
    observer_b: SuperJobs | None = None
    deadline = asyncio.get_running_loop().time() + 40.0
    try:
        worker_a = spawn_worker(
            nats_url=nats_url,
            work_prefix=prefix_a,
            worker_label="a",
            state_dir=shared_ns_state_dir,
            resource_namespace=SHARED_RESOURCE_NAMESPACE,
        )
        ready_a = wait_for_ready(shared_ns_state_dir, "a", deadline=deadline)
        ready_a_marker = ready_marker_from_payload(ready_a)

        observer_a = await observer_jobs(nats_url, config_a)
        observer_b = await observer_jobs(nats_url, config_b)
        handle_a = await observer_a.client(HOLD_JOB).submit()
        entered_a = wait_for_entered(shared_ns_state_dir, "a", deadline=deadline)
        snap_a_entered = await observer_a.resources.snapshot()
        owner_a = owner_for_key(snap_a_entered, RESOURCE_KEY)
        assert owner_a is not None
        assert owner_a.execution_id == handle_a.id
        assert int(entered_a["pid"]) == ready_a_marker.pid

        terminate_a = finalize_owned_worker(
            worker_a,
            state_dir=shared_ns_state_dir,
            worker_label="a",
            deadline=deadline,
            marker=ready_a_marker,
            hard=True,
            signal_stop=False,
            evidence_dir=process_exit_evidence_dir,
        )
        worker_a = None
        assert terminate_a is not None
        assert terminate_a.interpreter_pid == ready_a_marker.pid
        assert terminate_a.interpreter_pid == int(entered_a["pid"])
        assert terminate_a.incarnation_id == owner_a.incarnation.incarnation_id
        assert terminate_a.worker_id == owner_a.incarnation.worker_id

        snap_blocked_a = await observer_a.resources.snapshot()
        snap_blocked_b = await observer_b.resources.snapshot()
        blocked_owner = assert_snapshots_agree_on_owner(
            snap_blocked_a,
            snap_blocked_b,
            resource_key=RESOURCE_KEY,
        )
        assert blocked_owner.model_dump() == owner_a.model_dump()

        worker_b = spawn_worker(
            nats_url=nats_url,
            work_prefix=prefix_b,
            worker_label="b",
            state_dir=shared_ns_state_dir,
            resource_namespace=SHARED_RESOURCE_NAMESPACE,
        )
        ready_b = wait_for_ready(shared_ns_state_dir, "b", deadline=deadline)
        handle_b = await observer_b.client(HOLD_JOB).submit()
        await wait_for_job_state(
            handle_b,
            JobState.WAITING_FOR_RESOURCE,
            deadline=deadline,
        )

        confirmation = TerminationConfirmation(
            incarnation=owner_a.incarnation,
            confirmed=True,
            evidence=(
                "supervisor:verified-interpreter-exit "
                f"pid={terminate_a.interpreter_pid} "
                f"worker_id={terminate_a.worker_id} "
                f"incarnation_id={terminate_a.incarnation_id}"
            ),
        )
        recovery = await observer_b.resources.resolve_terminated_incarnation(confirmation)
        assert recovery.outcome.value == "resolved"

        entered_b = wait_for_entered(shared_ns_state_dir, "b", deadline=deadline)
        assert entered_b["execution_id"] == handle_b.id
        assert entered_b["pid"] == ready_b["pid"]
        assert entered_b["worker_id"] == ready_b["worker_id"]

        snap_after_b = await observer_a.resources.snapshot()
        snap_after_b_b = await observer_b.resources.snapshot()
        owner_b = assert_snapshots_agree_on_owner(
            snap_after_b,
            snap_after_b_b,
            resource_key=RESOURCE_KEY,
        )
        assert owner_b.execution_id == handle_b.id
        assert owner_b.incarnation.worker_id == ready_b["worker_id"]
        assert owner_b.incarnation.incarnation_id == ready_b["incarnation_id"]
        assert owner_b.fencing.generation > owner_a.fencing.generation
        assert owner_b.fencing.token != owner_a.fencing.token

        claim_a_obs = claim_for_execution(snap_after_b, handle_a.id)
        claim_b_obs = claim_for_execution(snap_after_b_b, handle_b.id)
        assert claim_b_obs is not None
        assert claim_b_obs.owner.execution_id == handle_b.id
        assert claim_b_obs.owner.model_dump() == owner_b.model_dump()

        stale_release = await observer_a.resources.release_resource_after_cleanup(
            owner_a,
        )
        assert stale_release.outcome is ReleaseOutcome.STALE

        snap_stale_attempt = await observer_a.resources.snapshot()
        snap_stale_attempt_b = await observer_b.resources.snapshot()
        owner_after_stale = assert_snapshots_agree_on_owner(
            snap_stale_attempt,
            snap_stale_attempt_b,
            resource_key=RESOURCE_KEY,
        )
        assert owner_after_stale.model_dump() == owner_b.model_dump()
        assert claim_for_execution(snap_stale_attempt, handle_b.id) is not None

        await publish_release(nats_url, config_b, handle_b.id)
        outcome_b = await asyncio.wait_for(handle_b.outcome(), timeout=15.0)
        assert isinstance(outcome_b, JobSucceeded)

        final_a = await observer_a.resources.snapshot()
        final_b = await observer_b.resources.snapshot()
        assert owner_for_key(final_a, RESOURCE_KEY) is None
        assert owner_for_key(final_b, RESOURCE_KEY) is None
        assert not final_a.execution_claims
        assert not final_b.execution_claims
    finally:
        end = asyncio.get_running_loop().time() + 10.0
        _cleanup_owned_workers(
            workers={"a": worker_a, "b": worker_b},
            state_dir=shared_ns_state_dir,
            deadline=end,
            evidence_dir=process_exit_evidence_dir,
        )
        if observer_a is not None:
            try:
                await stop_jobs(observer_a)
            except Exception:
                pass
        if observer_b is not None:
            try:
                await stop_jobs(observer_b)
            except Exception:
                pass


@pytest.mark.asyncio
async def test_shared_resource_namespace_failed_cleanup_blocks_peer_until_confirmed(
    nats_owned_server,
    shared_ns_run_id: str,
    shared_ns_state_dir: Path,
    process_exit_evidence_dir: Path,
) -> None:
    _owner, target = nats_owned_server
    nats_url = target.url
    prefix_a = f"work_a_{shared_ns_run_id}"
    prefix_b = f"work_b_{shared_ns_run_id}"
    config_a = queue_config_for_work_prefix(prefix_a)
    config_b = queue_config_for_work_prefix(prefix_b)

    worker_a: subprocess.Popen[str] | None = None
    worker_b: subprocess.Popen[str] | None = None
    observer_a: SuperJobs | None = None
    observer_b: SuperJobs | None = None
    deadline = asyncio.get_running_loop().time() + 40.0
    try:
        worker_a = spawn_worker(
            nats_url=nats_url,
            work_prefix=prefix_a,
            worker_label="a",
            state_dir=shared_ns_state_dir,
            resource_namespace=SHARED_RESOURCE_NAMESPACE,
            handler_mode="cleanup_failed",
        )
        worker_b = spawn_worker(
            nats_url=nats_url,
            work_prefix=prefix_b,
            worker_label="b",
            state_dir=shared_ns_state_dir,
            resource_namespace=SHARED_RESOURCE_NAMESPACE,
        )
        ready_a_payload = wait_for_ready(shared_ns_state_dir, "a", deadline=deadline)
        wait_for_ready(shared_ns_state_dir, "b", deadline=deadline)

        observer_a = await observer_jobs(nats_url, config_a)
        observer_b = await observer_jobs(nats_url, config_b)

        handle_a = await observer_a.client(HOLD_JOB).submit()
        entered_a = wait_for_entered(shared_ns_state_dir, "a", deadline=deadline)
        outcome_a = await asyncio.wait_for(handle_a.outcome(), timeout=15.0)
        assert isinstance(outcome_a, JobFailedOutcome)

        snap_a = await observer_a.resources.snapshot()
        snap_b = await observer_b.resources.snapshot()
        resource_a = resource_for_key(snap_a, RESOURCE_KEY)
        resource_b = resource_for_key(snap_b, RESOURCE_KEY)
        assert resource_a is not None and resource_b is not None
        assert resource_a.availability is ResourceAvailability.UNAVAILABLE
        assert resource_b.availability is ResourceAvailability.UNAVAILABLE
        owner_a = assert_snapshots_agree_on_owner(snap_a, snap_b, resource_key=RESOURCE_KEY)
        assert owner_a.execution_id == handle_a.id
        assert claim_for_execution(snap_a, handle_a.id) is not None
        assert claim_for_execution(snap_b, handle_a.id) is not None

        handle_b = await observer_b.client(HOLD_JOB).submit()
        await wait_for_job_state(
            handle_b,
            JobState.WAITING_FOR_RESOURCE,
            deadline=deadline,
        )

        terminate_a = _finalize_test_worker(
            worker_a,
            state_dir=shared_ns_state_dir,
            label="a",
            deadline=deadline,
            evidence_dir=process_exit_evidence_dir,
            marker_payload=ready_a_payload,
        )
        worker_a = None
        assert terminate_a is not None
        assert terminate_a.interpreter_terminated
        assert terminate_a.interpreter_pid == int(entered_a["pid"])
        assert terminate_a.incarnation_id == owner_a.incarnation.incarnation_id

        confirmation = TerminationConfirmation(
            incarnation=owner_a.incarnation,
            confirmed=True,
            evidence=(
                "supervisor:failed-cleanup verified-exit "
                f"pid={terminate_a.interpreter_pid}"
            ),
        )
        await observer_b.resources.resolve_terminated_incarnation(confirmation)

        entered_b = wait_for_entered(shared_ns_state_dir, "b", deadline=deadline)
        assert entered_b["execution_id"] == handle_b.id
        await publish_release(nats_url, config_b, handle_b.id)
        outcome_b = await asyncio.wait_for(handle_b.outcome(), timeout=15.0)
        assert isinstance(outcome_b, JobSucceeded)

        final_a = await observer_a.resources.snapshot()
        final_b = await observer_b.resources.snapshot()
        assert owner_for_key(final_a, RESOURCE_KEY) is None
        assert owner_for_key(final_b, RESOURCE_KEY) is None
    finally:
        end = asyncio.get_running_loop().time() + 10.0
        _cleanup_owned_workers(
            workers={"a": worker_a, "b": worker_b},
            state_dir=shared_ns_state_dir,
            deadline=end,
            evidence_dir=process_exit_evidence_dir,
        )
        if observer_a is not None:
            try:
                await stop_jobs(observer_a)
            except Exception:
                pass
        if observer_b is not None:
            try:
                await stop_jobs(observer_b)
            except Exception:
                pass


@pytest.mark.asyncio
async def test_shared_resource_namespace_admission_store_survives_broker_restart(
    nats_owned_server,
    shared_ns_run_id: str,
    shared_ns_state_dir: Path,
    process_exit_evidence_dir: Path,
) -> None:
    owner, target = nats_owned_server
    nats_url = target.url
    prefix_a = f"work_a_{shared_ns_run_id}"
    prefix_b = f"work_b_{shared_ns_run_id}"
    config_a = queue_config_for_work_prefix(prefix_a)
    config_b = queue_config_for_work_prefix(prefix_b)

    worker_a: subprocess.Popen[str] | None = None
    observer_a: SuperJobs | None = None
    observer_b: SuperJobs | None = None
    deadline = asyncio.get_running_loop().time() + 40.0
    try:
        worker_a = spawn_worker(
            nats_url=nats_url,
            work_prefix=prefix_a,
            worker_label="a",
            state_dir=shared_ns_state_dir,
            resource_namespace=SHARED_RESOURCE_NAMESPACE,
        )
        wait_for_ready(shared_ns_state_dir, "a", deadline=deadline)
        observer_a = await observer_jobs(nats_url, config_a)
        observer_b = await observer_jobs(nats_url, config_b)
        bucket_before = await store_bucket_name(observer_a)
        assert bucket_before == await store_bucket_name(observer_b)

        handle_a = await observer_a.client(HOLD_JOB).submit()
        wait_for_entered(shared_ns_state_dir, "a", deadline=deadline)
        before_a = await observer_a.resources.snapshot()
        before_b = await observer_b.resources.snapshot()
        held = assert_snapshots_agree_on_owner(before_a, before_b, resource_key=RESOURCE_KEY)

        try:
            await stop_jobs(observer_a)
        except Exception:
            pass
        try:
            await stop_jobs(observer_b)
        except Exception:
            pass
        observer_a = None
        observer_b = None

        target = await asyncio.to_thread(owner.restart, target)
        nats_url = target.url
        observer_a = await observer_jobs(nats_url, config_a)
        observer_b = await observer_jobs(nats_url, config_b)

        assert await store_bucket_name(observer_a) == bucket_before
        after_a = await observer_a.resources.snapshot()
        after_b = await observer_b.resources.snapshot()
        restored = assert_snapshots_agree_on_owner(after_a, after_b, resource_key=RESOURCE_KEY)
        assert restored.model_dump() == held.model_dump()

        marker_a = load_ready_marker(shared_ns_state_dir, "a")
        terminate_a = finalize_owned_worker(
            worker_a,
            state_dir=shared_ns_state_dir,
            worker_label="a",
            deadline=deadline,
            marker=marker_a,
            evidence_dir=process_exit_evidence_dir,
        )
        worker_a = None
        assert terminate_a is not None and terminate_a.interpreter_terminated
        confirmation = TerminationConfirmation(
            incarnation=held.incarnation,
            confirmed=True,
            evidence=(
                "supervisor:broker-restart verified-exit "
                f"pid={terminate_a.interpreter_pid}"
            ),
        )
        await observer_b.resources.resolve_terminated_incarnation(confirmation)
        final_a = await observer_a.resources.snapshot()
        final_b = await observer_b.resources.snapshot()
        assert owner_for_key(final_a, RESOURCE_KEY) is None
        assert owner_for_key(final_b, RESOURCE_KEY) is None
    finally:
        end = asyncio.get_running_loop().time() + 10.0
        _cleanup_owned_workers(
            workers={"a": worker_a},
            state_dir=shared_ns_state_dir,
            deadline=end,
            evidence_dir=process_exit_evidence_dir,
        )
        if observer_a is not None:
            try:
                await stop_jobs(observer_a)
            except Exception:
                pass
        if observer_b is not None:
            try:
                await stop_jobs(observer_b)
            except Exception:
                pass


@pytest.mark.asyncio
async def test_shared_resource_namespace_explicit_invalidate_keeps_shared_owner(
    nats_owned_server,
    shared_ns_run_id: str,
    shared_ns_state_dir: Path,
    process_exit_evidence_dir: Path,
) -> None:
    _owner, target = nats_owned_server
    nats_url = target.url
    prefix_a = f"work_a_{shared_ns_run_id}"
    prefix_b = f"work_b_{shared_ns_run_id}"
    config_a = queue_config_for_work_prefix(prefix_a)
    config_b = queue_config_for_work_prefix(prefix_b)

    worker_a: subprocess.Popen[str] | None = None
    observer_a: SuperJobs | None = None
    observer_b: SuperJobs | None = None
    deadline = asyncio.get_running_loop().time() + 30.0
    try:
        worker_a = spawn_worker(
            nats_url=nats_url,
            work_prefix=prefix_a,
            worker_label="a",
            state_dir=shared_ns_state_dir,
            resource_namespace=SHARED_RESOURCE_NAMESPACE,
        )
        wait_for_ready(shared_ns_state_dir, "a", deadline=deadline)
        observer_a = await observer_jobs(nats_url, config_a)
        observer_b = await observer_jobs(nats_url, config_b)
        handle_a = await observer_a.client(HOLD_JOB).submit()
        wait_for_entered(shared_ns_state_dir, "a", deadline=deadline)
        before = assert_snapshots_agree_on_owner(
            await observer_a.resources.snapshot(),
            await observer_b.resources.snapshot(),
            resource_key=RESOURCE_KEY,
        )
        observer_a.transport.resource_admission_store.invalidate()
        observer_b.transport.resource_admission_store.invalidate()
        after = assert_snapshots_agree_on_owner(
            await observer_a.resources.snapshot(),
            await observer_b.resources.snapshot(),
            resource_key=RESOURCE_KEY,
        )
        assert after.model_dump() == before.model_dump()
        await publish_release(nats_url, config_a, handle_a.id)
        await asyncio.wait_for(handle_a.outcome(), timeout=15.0)
    finally:
        end = asyncio.get_running_loop().time() + 10.0
        _cleanup_owned_workers(
            workers={"a": worker_a},
            state_dir=shared_ns_state_dir,
            deadline=end,
            evidence_dir=process_exit_evidence_dir,
        )
        if observer_a is not None:
            try:
                await stop_jobs(observer_a)
            except Exception:
                pass
        if observer_b is not None:
            try:
                await stop_jobs(observer_b)
            except Exception:
                pass


def test_no_orphan_worker_entry_processes(process_exit_evidence_dir: Path) -> None:
    orphans = audit_no_worker_entry_processes()
    assert orphans == [], f"leftover worker_entry.py processes: {orphans}"
