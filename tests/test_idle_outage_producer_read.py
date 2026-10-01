"""Producer outage-read path tests for idle outage verification."""

from __future__ import annotations

import asyncio
import importlib.util
import shutil
import sys
import types
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from nats.errors import TimeoutError as NatsTimeoutError

REPO_ROOT = Path(__file__).resolve().parents[1]
SUPPORT = REPO_ROOT / "tools" / "idle_outage_support"

from superjobs.exceptions.jobs import JobNotFoundError  # noqa: E402
from tools.idle_outage_support import protocol as io_protocol  # noqa: E402
from tools import verify_idle_outage as vio  # noqa: E402


@pytest.fixture(autouse=True)
def _restore_role_modules():
    aliases = ("protocol", "broker_callbacks", "broker_connect", "queue_config", "runtime_isolation", "scenario_jobs")
    original = {name: sys.modules.get(name) for name in aliases}
    for name in aliases:
        sys.modules.pop(name, None)
    try:
        yield
    finally:
        for name, module in original.items():
            if module is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = module


def _load_producer_module(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    contract = types.ModuleType("superjobs_contract_example")
    contract.ManifestRequest = object
    contract.ManifestResult = object
    monkeypatch.setitem(sys.modules, "superjobs_contract_example", contract)

    role_dir = tmp_path / "producer"
    role_dir.mkdir()
    for name in vio.PRODUCER_SUPPORT_FILES:
        shutil.copy2(SUPPORT / name, role_dir / name)
    monkeypatch.syspath_prepend(str(role_dir))
    module_name = "idle_outage_producer_under_test"
    spec = importlib.util.spec_from_file_location(
        module_name,
        role_dir / "producer_app.py",
    )
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, module_name, module)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


@pytest.mark.asyncio
async def test_run_read_during_outage_success_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    producer = _load_producer_module(tmp_path, monkeypatch)
    handle = SimpleNamespace(id="exec-1", status=AsyncMock(return_value=object()))
    jobs = object()
    request = io_protocol.ProducerRequest(
        run_id="run",
        request_id="req",
        command=io_protocol.COMMAND_READ_DURING_OUTAGE,
    )
    with pytest.raises(AssertionError, match="must not succeed"):
        await producer._run_read_during_outage(
            handle,
            "run",
            tmp_path,
            request,
            jobs=jobs,
            handle_object_id=id(handle),
        )


@pytest.mark.asyncio
async def test_run_read_during_outage_job_not_found_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    producer = _load_producer_module(tmp_path, monkeypatch)
    handle = SimpleNamespace(
        id="exec-1",
        status=AsyncMock(side_effect=JobNotFoundError("exec-1")),
    )
    request = io_protocol.ProducerRequest(
        run_id="run",
        request_id="req",
        command=io_protocol.COMMAND_READ_DURING_OUTAGE,
    )
    with pytest.raises(AssertionError, match="missing Job"):
        await producer._run_read_during_outage(
            handle,
            "run",
            tmp_path,
            request,
            jobs=object(),
            handle_object_id=id(handle),
        )


@pytest.mark.asyncio
async def test_run_read_during_outage_value_error_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    producer = _load_producer_module(tmp_path, monkeypatch)
    handle = SimpleNamespace(
        id="exec-1",
        status=AsyncMock(side_effect=ValueError("bad")),
    )
    request = io_protocol.ProducerRequest(
        run_id="run",
        request_id="req",
        command=io_protocol.COMMAND_READ_DURING_OUTAGE,
    )
    with pytest.raises(AssertionError, match="unexpected outage read error"):
        await producer._run_read_during_outage(
            handle,
            "run",
            tmp_path,
            request,
            jobs=object(),
            handle_object_id=id(handle),
        )


@pytest.mark.asyncio
async def test_run_read_during_outage_timeout_writes_checkpoint(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    producer = _load_producer_module(tmp_path, monkeypatch)
    handle = SimpleNamespace(
        id="exec-1",
        status=AsyncMock(side_effect=NatsTimeoutError()),
    )
    jobs = object()
    run_id = uuid.uuid4().hex
    request = io_protocol.ProducerRequest(
        run_id=run_id,
        request_id="req",
        command=io_protocol.COMMAND_READ_DURING_OUTAGE,
    )
    await producer._run_read_during_outage(
        handle,
        run_id,
        tmp_path,
        request,
        jobs=jobs,
        handle_object_id=id(handle),
    )
    marker = io_protocol.read_checkpoint(tmp_path, io_protocol.HANDLE_READ_FAILED)
    assert marker.error_type == "TimeoutError"
    assert marker.handle_id == "exec-1"
    assert marker.handle_object_id == id(handle)
    assert marker.runtime_object_id == id(jobs)
