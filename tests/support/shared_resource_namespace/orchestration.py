"""Parent-process helpers for shared resource namespace NATS tests."""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import nats
from faststream.nats import NatsBroker

from superjobs import HandlerCatalog, SuperJobs
from superjobs.jobs.execution import JobState
from superjobs.resources import V1_EXCLUSIVE_CAPACITY
from superjobs.resources.keys import admission_bucket_name
from superjobs.resources.models import (
    AdmissionOwner,
    AdmissionSnapshot,
    StoredExecutionClaim,
    StoredResourceRecord,
)
from superjobs.transport.implementations.nats import NatsQueueConfig

from tests.support.shared_resource_namespace.constants import RESOURCE_KEY
from tests.support.shared_resource_namespace.queue_config import (
    control_subject,
    queue_config_for_work_prefix,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
WORKER_ENTRY = Path(__file__).resolve().parent / "worker_entry.py"


def child_env(*, nats_url: str, extra: dict[str, str] | None = None) -> dict[str, str]:
    env = os.environ.copy()
    env.pop("NATS_URL", None)
    env["NATS_URL"] = nats_url
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    src = REPO_ROOT / "src"
    existing = env.get("PYTHONPATH", "")
    prefix = os.pathsep.join((str(src), str(REPO_ROOT)))
    env["PYTHONPATH"] = prefix if not existing else os.pathsep.join((prefix, existing))
    if extra:
        env.update(extra)
    return env


def spawn_worker(
    *,
    nats_url: str,
    work_prefix: str,
    worker_label: str,
    state_dir: Path,
    resource_namespace: str | None,
    handler_mode: str = "hold",
) -> subprocess.Popen[str]:
    env = child_env(
        nats_url=nats_url,
        extra={
            "SJ_WORK_PREFIX": work_prefix,
            "SJ_WORKER_LABEL": worker_label,
            "SJ_STATE_DIR": str(state_dir),
            "SJ_RESOURCE_NAMESPACE": resource_namespace or "",
            "SJ_HANDLER_MODE": handler_mode,
        },
    )
    process = subprocess.Popen(
        [sys.executable, str(WORKER_ENTRY.with_name("worker_bootstrap.py")), str(WORKER_ENTRY)],
        cwd=str(WORKER_ENTRY.parent),
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    from tests.support.shared_resource_namespace.process_proof import register_owned_worker

    register_owned_worker(process, state_dir, worker_label)
    return process


def wait_for_ready(state_dir: Path, worker_label: str, *, deadline: float) -> dict[str, Any]:
    path = state_dir / f"worker_ready_{worker_label}.json"
    while time.monotonic() < deadline:
        if path.is_file():
            payload = json.loads(path.read_text(encoding="utf-8"))
            if payload.get("worker_label") == worker_label:
                return payload
        time.sleep(0.05)
    raise TimeoutError(f"worker {worker_label!r} did not become ready")


def wait_for_entered(
    state_dir: Path,
    worker_label: str,
    *,
    deadline: float,
) -> dict[str, Any]:
    path = state_dir / f"entered_{worker_label}.json"
    while time.monotonic() < deadline:
        if path.is_file():
            return json.loads(path.read_text(encoding="utf-8"))
        time.sleep(0.05)
    raise TimeoutError(f"handler for {worker_label!r} did not enter")


def entered_marker_present(state_dir: Path, worker_label: str) -> bool:
    return (state_dir / f"entered_{worker_label}.json").is_file()


async def wait_for_job_state(
    handle,
    state: JobState,
    *,
    deadline: float,
) -> None:
    while asyncio.get_running_loop().time() < deadline:
        status = await handle.status()
        if status.state is state:
            return
        await asyncio.sleep(0.05)
    raise TimeoutError(f"execution did not reach state {state!r}")


async def publish_release(
    nats_url: str,
    queue_config: NatsQueueConfig,
    execution_id: str,
    *,
    timeout: float = 15.0,
) -> None:
    subject = control_subject(queue_config)
    async with asyncio.timeout(timeout):
        connection = await nats.connect(nats_url, connect_timeout=timeout)
        try:
            deadline = asyncio.get_running_loop().time() + timeout
            message = {"type": "release_holder", "execution_id": execution_id}
            while True:
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    raise TimeoutError("release was not acknowledged")
                try:
                    reply = await connection.request(
                        subject,
                        json.dumps(message).encode("utf-8"),
                        timeout=min(1.0, remaining),
                    )
                except (nats.errors.TimeoutError, nats.errors.NoRespondersError):
                    await asyncio.sleep(0.05)
                    continue
                acknowledged = json.loads(reply.data.decode("utf-8"))
                if acknowledged.get("released") == execution_id:
                    return
        finally:
            await connection.drain()


async def observer_jobs(
    nats_url: str,
    queue_config: NatsQueueConfig,
) -> SuperJobs:
    broker = NatsBroker(nats_url, connect_timeout=5, max_reconnect_attempts=0)
    jobs = SuperJobs(
        broker=broker,
        queue_config=queue_config,
        handlers=HandlerCatalog(),
        discovery=False,
        resource_capacities={RESOURCE_KEY: V1_EXCLUSIVE_CAPACITY},
    )
    await jobs.start()
    return jobs


async def stop_jobs(jobs: SuperJobs) -> None:
    await jobs.stop(graceful=False)


async def reconnect_observer(jobs: SuperJobs) -> None:
    jobs.transport.resource_admission_store.invalidate()
    jobs.transport.discovery_backend.invalidate()
    await jobs.transport.broker.stop()
    await jobs.transport.broker.start()


def admission_bucket_for_config(queue_config: NatsQueueConfig) -> str:
    return admission_bucket_name(queue_config.resolved_resource_admission_namespace())


async def store_bucket_name(jobs: SuperJobs) -> str:
    store = jobs.transport.resource_admission_store
    return store.bucket_name


def owner_for_key(snapshot: AdmissionSnapshot, resource_key: str) -> AdmissionOwner | None:
    for record in snapshot.resources:
        if record.resource_key == resource_key and record.owner is not None:
            return record.owner
    return None


def resource_for_key(
    snapshot: AdmissionSnapshot,
    resource_key: str,
) -> StoredResourceRecord | None:
    for record in snapshot.resources:
        if record.resource_key == resource_key:
            return record
    return None


def claim_for_execution(
    snapshot: AdmissionSnapshot,
    execution_id: str,
) -> StoredExecutionClaim | None:
    for claim in snapshot.execution_claims:
        if claim.owner.execution_id == execution_id:
            return claim
    return None


def assert_snapshots_agree_on_owner(
    snap_a: AdmissionSnapshot,
    snap_b: AdmissionSnapshot,
    *,
    resource_key: str,
) -> AdmissionOwner:
    owner_a = owner_for_key(snap_a, resource_key)
    owner_b = owner_for_key(snap_b, resource_key)
    assert owner_a is not None and owner_b is not None
    assert owner_a.model_dump() == owner_b.model_dump()
    return owner_a
