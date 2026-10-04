"""Public remote CLI contracts using a deterministic backend and owned NATS."""
from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import pytest
import typer
from faststream.nats import NatsBroker
from typer.testing import CliRunner

from superjobs import InMemoryTransport, Job, JobError, JobState, NatsJobBackend, SuperJobs
from superjobs.cli import JobCLI


class MemoryNatsBackend(NatsJobBackend):
    """Exercise real public clients/handles without connecting a broker."""
    def __init__(self, *, state=JobState.COMPLETED, observation_error=None, fail_start=False):
        super().__init__(NatsBroker(allow_reconnect=False))
        self.memory = InMemoryTransport()
        self.final_state = state
        self.observation_error = observation_error
        self.fail_start = fail_start
        self.submit_calls = 0
        self.stop_calls = 0
        self.close_calls = 0

    async def start(self):
        await self.memory.start()
        self.started = True
        if self.fail_start:
            raise ConnectionError("startup failed after acquisition")

    async def stop(self):
        self.stop_calls += 1
        await self.memory.stop()
        self.started = False

    async def submit(self, identity, **kwargs):
        self.submit_calls += 1
        execution = await self.memory.submit(identity, **kwargs)
        await self.memory.write_completion(
            execution,
            state=self.final_state,
            error=JobError("failed", "worker failed") if self.final_state is JobState.FAILED else None,
        )
        return execution

    async def get_execution(self, identity, job_id):
        return await self.memory.get_execution(identity, job_id)

    async def wait_for_status(self, identity, job_id):
        return await self.memory.wait_for_status(identity, job_id)

    async def observations(self, identity, job_id, *, after=0):
        backend = self
        error = self.observation_error
        class Events:
            def __aiter__(self):
                return self
            async def __anext__(self):
                if error == "read":
                    raise ConnectionError("observation read failed")
                raise StopAsyncIteration
            async def aclose(self):
                backend.close_calls += 1
                if error == "close":
                    raise ConnectionError("observation close failed")
        return Events()

    async def request_cancel(self, *_args, **_kwargs):
        raise AssertionError("the CLI must not cancel remote work")


@asynccontextmanager
async def memory_runtime(backend) -> AsyncIterator[SuperJobs]:
    yield SuperJobs(transport=backend)


def cli_with(backend):
    cli = JobCLI(remote_runtime_factory=lambda: memory_runtime(backend))
    cli.add("probe", Job("tests.cli.remote.contract"), remote_only=True)
    return cli


@pytest.mark.parametrize("wait", [False, True])
def test_public_dispatch_uses_real_client_once_and_closes_owned_resources(wait):
    backend = MemoryNatsBackend()
    result = CliRunner().invoke(cli_with(backend).build_typer(), ["submit", "probe", *(["--wait"] if wait else [])])
    assert result.exit_code == 0, result.stderr
    assert json.loads(result.stdout) is None if wait else json.loads(result.stdout)["job_version"] is None
    assert backend.submit_calls == 1
    assert backend.stop_calls == 1
    assert not backend.started
    assert not backend.memory.started
    if wait:
        assert backend.close_calls == 1


@pytest.mark.parametrize("state", [JobState.FAILED, JobState.CANCELLED])
def test_public_terminal_failures_suppress_stdout_and_preserve_reference(state):
    backend = MemoryNatsBackend(state=state)
    result = CliRunner().invoke(cli_with(backend).build_typer(), ["submit", "probe", "--wait"])
    assert result.exit_code == 1
    assert result.stdout == ""
    assert result.stderr.count("execution reference:") == 1
    assert "failed" in result.stderr if state is JobState.FAILED else "cancelled" in result.stderr
    assert backend.submit_calls == 1
    assert backend.close_calls == 1


@pytest.mark.parametrize("error", ["read", "close"])
def test_public_observation_errors_win_over_simultaneous_success(error):
    backend = MemoryNatsBackend(observation_error=error)
    result = CliRunner().invoke(cli_with(backend).build_typer(), ["submit", "probe", "--wait"])
    assert result.exit_code == 1
    assert result.stdout == ""
    assert f"observation {error} failed" in result.stderr
    assert result.stderr.count("execution reference:") == 1
    assert backend.submit_calls == 1
    assert backend.close_calls == 1
    assert not backend.started


@pytest.mark.parametrize("mounted", [False, True])
def test_remote_factory_snapshot_is_stable_for_built_and_mounted_apps(mounted):
    selected, later = MemoryNatsBackend(), MemoryNatsBackend()
    cli = cli_with(selected)
    if mounted:
        app = typer.Typer()
        cli.mount(app)
    else:
        app = cli.build_typer()
    cli.remote_runtime_factory = lambda: memory_runtime(later)
    result = CliRunner().invoke(app, ["submit", "probe"])
    assert result.exit_code == 0, result.stderr
    assert selected.submit_calls == 1
    assert later.submit_calls == 0


def test_failed_entered_startup_rolls_back_transport_and_factory():
    backend = MemoryNatsBackend(fail_start=True)
    cleaned = []
    @asynccontextmanager
    async def factory():
        try:
            yield SuperJobs(transport=backend)
        finally:
            cleaned.append(True)
    cli = cli_with(backend)
    cli.remote_runtime_factory = factory
    result = CliRunner().invoke(cli.build_typer(), ["submit", "probe"])
    assert result.exit_code == 1
    assert result.stdout == ""
    assert "acceptance unconfirmed" not in result.stderr
    assert backend.submit_calls == 0
    assert backend.stop_calls >= 1
    assert not backend.memory.started
    assert cleaned == [True]


def test_startup_rollback_closes_broker_when_backend_stop_is_noop(monkeypatch):
    backend = MemoryNatsBackend()
    closed = []

    async def partial_start():
        raise ConnectionError("broker acquired before backend startup failed")

    async def broker_stop(*_args, **_kwargs):
        closed.append(True)

    monkeypatch.setattr(backend, "start", partial_start)
    monkeypatch.setattr(backend.broker, "stop", broker_stop)
    result = CliRunner().invoke(cli_with(backend).build_typer(), ["submit", "probe"])
    assert result.exit_code == 1
    assert result.stdout == ""
    assert backend.submit_calls == 0
    assert closed == [True]


def test_consumer_wired_runtime_rejected_before_start(monkeypatch):
    broker = NatsBroker(allow_reconnect=False)
    broker.subscriber("work")(lambda: None)
    started = []
    async def forbidden_start(self):
        started.append(True)
        raise AssertionError("consumer-wired runtime must not start")
    monkeypatch.setattr(SuperJobs, "start", forbidden_start)
    @asynccontextmanager
    async def factory():
        yield SuperJobs(broker=broker)
    cli = JobCLI(remote_runtime_factory=factory)
    cli.add("probe", Job("tests.cli.remote.prewired"), remote_only=True)
    result = CliRunner().invoke(cli.build_typer(), ["submit", "probe"])
    assert result.exit_code == 1
    assert "subscribers" in result.stderr
    assert started == []


def test_native_sigint_racing_acceptance_preserves_reference_and_restores_signal():
    import signal

    class InterruptedBackend(MemoryNatsBackend):
        async def submit(self, identity, **kwargs):
            execution = await super().submit(identity, **kwargs)
            signal.raise_signal(signal.SIGINT)
            return execution

    previous = signal.getsignal(signal.SIGINT)
    backend = InterruptedBackend()
    result = CliRunner().invoke(cli_with(backend).build_typer(), ["submit", "probe"])
    assert result.exit_code == 130
    assert result.stdout == ""
    assert result.stderr.count("execution reference:") == 1
    assert "acceptance unconfirmed" not in result.stderr
    assert signal.getsignal(signal.SIGINT) is previous
    assert backend.submit_calls == 1
    assert not backend.started


@pytest.mark.nats
@pytest.mark.asyncio
@pytest.mark.parametrize("fails", [False, True])
async def test_real_nats_public_wait_no_result_and_terminal_failure(nats_broker_factory, nats_queue_config, fails):
    job = Job("tests.cli.remote.terminal", version="v1")
    worker = SuperJobs(broker=nats_broker_factory(), queue_config=nats_queue_config)
    @worker.handler(job)
    async def handler(context):
        if fails:
            raise RuntimeError("owned NATS handler failed")
        return None
    @asynccontextmanager
    async def producer():
        yield SuperJobs(broker=nats_broker_factory(), queue_config=nats_queue_config)
    cli = JobCLI(remote_runtime_factory=producer)
    cli.add("probe", job, remote_only=True)
    async with worker:
        result = await asyncio.to_thread(CliRunner().invoke, cli.build_typer(), ["submit", "probe", "--wait", "--wait-timeout", "5"])
    if fails:
        assert result.exit_code == 1
        assert result.stdout == ""
        assert "owned NATS handler failed" in result.stderr
        assert result.stderr.count("execution reference:") == 1
    else:
        assert result.exit_code == 0, result.stderr
        assert json.loads(result.stdout) is None
