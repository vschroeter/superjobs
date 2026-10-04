"""Remote NATS CLI submission tests (issue #41)."""

from __future__ import annotations

import asyncio
import json
import sys
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any
from unittest.mock import MagicMock

import pytest
from typer.testing import CliRunner

from superjobs import Job, JobContext, SuperJobs
from superjobs.cli import (
    EXIT_INTERRUPTED,
    EXIT_RUNTIME_FAILURE,
    EXIT_SUCCESS,
    EXIT_USAGE,
    MISSING_REMOTE_FACTORY_MESSAGE,
    JobCLI,
)
from superjobs.cli.input_prepare import prepare_command_input
from superjobs.cli.registration import CommandRegistration
from superjobs.cli.local_run import _InterruptState
from superjobs.cli.constants import SUBMISSION_ACCEPTANCE_UNCONFIRMED_MESSAGE
from superjobs.cli.remote_submit import (
    RemoteRuntimeValidationError,
    _execute_remote_submit_async,
    _wait_for_terminal,
    format_execution_reference,
    run_remote_command,
    validate_remote_runtime,
)
from superjobs.cli.schema_plan import build_command_input_plan
from superjobs.jobs.execution import JobState

@dataclass
class EchoRequest:
    value: int


@dataclass
class EchoResult:
    text: str


def _echo_job(name: str) -> Job[EchoRequest, EchoResult, None]:
    return Job(name, version="v1", request=EchoRequest, result=EchoResult)


def _remote_cli(
    factory: Callable[[], Any],
    job: Job[Any, Any, Any] | None = None,
    *,
    handler: Callable[..., Any] | None = None,
    command_name: str = "cmd",
) -> JobCLI:
    cli = JobCLI(remote_runtime_factory=factory)
    target = job or _echo_job(f"tests.cli.remote.{uuid.uuid4().hex}")
    if handler is not None:
        cli.add(command_name, target, handler=handler)
    else:
        cli.add(command_name, target, remote_only=True)
    return cli


def test_submit_without_factory_reports_missing_configuration() -> None:
    cli = JobCLI()
    cli.add("echo", _echo_job("tests.cli.remote.missing"), remote_only=True)
    result = CliRunner().invoke(cli.build_typer(), ["submit", "echo", "--value", "1"])
    assert result.exit_code == EXIT_RUNTIME_FAILURE
    assert MISSING_REMOTE_FACTORY_MESSAGE in result.stderr
    assert result.stdout == ""


def test_wait_timeout_requires_wait_flag() -> None:
    cli = _remote_cli(lambda: _never_called_factory())  # type: ignore[arg-type]
    result = CliRunner().invoke(
        cli.build_typer(),
        ["submit", "cmd", "--value", "1", "--wait-timeout", "1"],
    )
    assert result.exit_code == EXIT_USAGE
    assert "--wait-timeout requires --wait" in result.stderr


def test_validate_remote_runtime_rejects_in_memory() -> None:
    runtime = SuperJobs()
    with pytest.raises(RemoteRuntimeValidationError, match="NATS"):
        validate_remote_runtime(runtime)


def test_validate_remote_runtime_rejects_preregistered_handlers() -> None:
    job = _echo_job("tests.cli.remote.validate")

    async def handler(request: EchoRequest, context: JobContext[None]) -> EchoResult:
        return EchoResult(text="x")

    runtime = SuperJobs(broker=MagicMock())
    runtime.register(job, handler)
    with pytest.raises(RemoteRuntimeValidationError, match="pre-registered"):
        validate_remote_runtime(runtime)


def test_format_execution_reference_json() -> None:
    job = _echo_job("tests.cli.remote.ref")
    payload = json.loads(format_execution_reference(job, "exec-1"))
    assert payload == {
        "job_id": "exec-1",
        "job_name": job.name,
        "job_version": "v1",
    }


@pytest.mark.parametrize(
    "timeout_arg",
    ["0", "-1", "nan", "inf", "-inf"],
)
def test_wait_timeout_invalid_values_are_usage(timeout_arg: str) -> None:
    cli = _remote_cli(lambda: _never_called_factory())  # type: ignore[arg-type]
    result = CliRunner().invoke(
        cli.build_typer(),
        ["submit", "cmd", "--wait", "--wait-timeout", timeout_arg, "--value", "1"],
    )
    assert result.exit_code == EXIT_USAGE
    assert "finite positive" in result.stderr


def test_run_remote_command_rejects_wait_timeout_without_wait() -> None:
    job = _echo_job("tests.cli.remote.entrypoint")
    cli = JobCLI()
    cli.add("cmd", job, remote_only=True)
    registration = cli.registrations()[0]
    plan = build_command_input_plan(job)
    prepared = prepare_command_input(plan, {"json_text": '{"value": 1}'}, None)

    @asynccontextmanager
    async def remote() -> AsyncIterator[SuperJobs]:
        yield SuperJobs(broker=MagicMock())

    result = run_remote_command(
        registration,
        prepared,
        remote,
        wait=False,
        wait_timeout=1.0,
    )
    assert result.exit_code == EXIT_USAGE


def test_submit_rejects_in_memory_remote_factory() -> None:
    job = _echo_job("tests.cli.remote.reject.memory")

    @asynccontextmanager
    async def localish() -> AsyncIterator[SuperJobs]:
        async with SuperJobs() as jobs:
            yield jobs

    cli = _remote_cli(localish, job)
    result = CliRunner().invoke(cli.build_typer(), ["submit", "cmd", "--value", "1"])
    assert result.exit_code == EXIT_RUNTIME_FAILURE
    assert "NATS" in result.stderr
    assert result.stdout == ""


def test_submit_does_not_load_local_handler_factory() -> None:
    job = _echo_job("tests.cli.remote.lazy")
    calls = 0

    def handler_factory() -> Callable[[EchoRequest, JobContext[None]], Awaitable[EchoResult]]:
        nonlocal calls
        calls += 1
        raise AssertionError("handler factory must not run for submit")

    @asynccontextmanager
    async def remote() -> AsyncIterator[SuperJobs]:
        yield SuperJobs(broker=MagicMock())

    cli = JobCLI(remote_runtime_factory=remote)
    cli.add("cmd", job, handler_factory=handler_factory)
    result = CliRunner().invoke(cli.build_typer(), ["submit", "cmd", "--value", "1"])
    assert calls == 0
    assert result.exit_code == EXIT_RUNTIME_FAILURE


def test_wait_timeout_without_wait_is_usage_before_factory() -> None:
    calls: list[str] = []

    @asynccontextmanager
    async def remote() -> AsyncIterator[SuperJobs]:
        calls.append("factory")
        yield SuperJobs(broker=MagicMock())

    cli = _remote_cli(remote)
    result = CliRunner().invoke(
        cli.build_typer(),
        ["submit", "cmd", "--wait-timeout", "0.1"],
    )
    assert result.exit_code == EXIT_USAGE
    assert calls == []


@pytest.mark.asyncio
async def test_interrupt_during_wait_preserves_execution_reference() -> None:
    job = _echo_job("tests.cli.remote.interrupt")
    registration = None

    @asynccontextmanager
    async def remote() -> AsyncIterator[SuperJobs]:
        yield SuperJobs(broker=MagicMock())

    cli = JobCLI(remote_runtime_factory=remote)
    cli.add("cmd", job, remote_only=True)
    registration = cli.registrations()[0]
    plan = build_command_input_plan(job)
    prepared = prepare_command_input(plan, {"json_text": '{"value": 1}'}, None)

    class FakeHandle:
        job_id = "exec-interrupt"

        async def cancel(self) -> None:
            raise AssertionError("remote CLI must not cancel accepted work on interrupt")

    async def fake_submit(*_args: Any, **_kwargs: Any) -> FakeHandle:
        return FakeHandle()

    async def block_until_terminal(*_args: Any, **_kwargs: Any) -> Any:
        await asyncio.Event().wait()

    import io

    buffer = io.StringIO()
    state = _InterruptState(asyncio.Event())

    async def trigger() -> None:
        await asyncio.sleep(0.05)
        state.event.set()

    trigger_task = asyncio.create_task(trigger())
    with pytest.MonkeyPatch.context() as patcher:
        patcher.setattr(
            "superjobs.cli.remote_submit._submit_prepared",
            fake_submit,
        )
        patcher.setattr(
            "superjobs.cli.remote_submit._wait_for_terminal",
            block_until_terminal,
        )
        patcher.setattr(
            "superjobs.cli.remote_submit.validate_remote_runtime",
            lambda _runtime: None,
        )

        async def noop_start(self: SuperJobs) -> None:
            self._started = True

        async def noop_stop(self: SuperJobs, *, graceful: bool = True) -> None:
            self._started = False

        patcher.setattr(SuperJobs, "start", noop_start)
        patcher.setattr(SuperJobs, "stop", noop_stop)

        result = await _execute_remote_submit_async(
            registration,
            prepared,
            remote,
            wait=True,
            wait_timeout=30.0,
            stderr=buffer,
            shutdown_timeout=1.0,
            interrupt_state=state,
        )
    await trigger_task
    assert result.exit_code == EXIT_INTERRUPTED
    assert result.stdout is None
    assert "execution reference" in buffer.getvalue()
    assert "exec-interrupt" in buffer.getvalue()


@pytest.mark.asyncio
async def test_submit_failure_before_handle_is_unconfirmed() -> None:
    job = _echo_job("tests.cli.remote.unconfirmed")
    registration = CommandRegistration(
        command_name="cmd",
        job=job,
        remote_only=True,
        handler=None,
        handler_factory=None,
        input_plan=build_command_input_plan(job),
    )
    prepared = prepare_command_input(
        registration.input_plan,
        {"json_text": '{"value": 1}'},
        None,
    )

    @asynccontextmanager
    async def remote() -> AsyncIterator[SuperJobs]:
        yield SuperJobs(broker=MagicMock())

    import io

    buffer = io.StringIO()

    async def fail_submit(*_args: Any, **_kwargs: Any) -> Any:
        raise ConnectionError("submit transport error")

    with pytest.MonkeyPatch.context() as patcher:
        patcher.setattr("superjobs.cli.remote_submit.validate_remote_runtime", lambda _r: None)

        async def noop_start(self: SuperJobs) -> None:
            self._started = True

        async def noop_stop(self: SuperJobs, *, graceful: bool = True) -> None:
            self._started = False

        patcher.setattr(SuperJobs, "start", noop_start)
        patcher.setattr(SuperJobs, "stop", noop_stop)
        patcher.setattr("superjobs.cli.remote_submit._submit_prepared", fail_submit)

        result = await _execute_remote_submit_async(
            registration,
            prepared,
            remote,
            wait=False,
            wait_timeout=None,
            stderr=buffer,
            shutdown_timeout=5.0,
        )
    assert result.exit_code == EXIT_RUNTIME_FAILURE
    assert result.stdout is None
    diagnostics = buffer.getvalue()
    assert SUBMISSION_ACCEPTANCE_UNCONFIRMED_MESSAGE in diagnostics
    assert "execution reference" not in diagnostics


@pytest.mark.asyncio
async def test_startup_failure_is_not_unconfirmed() -> None:
    job = _echo_job("tests.cli.remote.startup")
    registration = CommandRegistration(
        command_name="cmd",
        job=job,
        remote_only=True,
        handler=None,
        handler_factory=None,
        input_plan=build_command_input_plan(job),
    )
    prepared = prepare_command_input(
        registration.input_plan,
        {"json_text": '{"value": 1}'},
        None,
    )

    @asynccontextmanager
    async def remote() -> AsyncIterator[SuperJobs]:
        yield SuperJobs(broker=MagicMock())

    import io

    buffer = io.StringIO()

    async def fail_start(self: SuperJobs) -> None:
        raise ConnectionError("broker down")

    with pytest.MonkeyPatch.context() as patcher:
        patcher.setattr(SuperJobs, "start", fail_start)
        patcher.setattr(SuperJobs, "stop", lambda self, **_: None)
        patcher.setattr("superjobs.cli.remote_submit.validate_remote_runtime", lambda _r: None)

        result = await _execute_remote_submit_async(
            registration,
            prepared,
            remote,
            wait=False,
            wait_timeout=None,
            stderr=buffer,
            shutdown_timeout=5.0,
        )
    assert result.exit_code == EXIT_RUNTIME_FAILURE
    assert SUBMISSION_ACCEPTANCE_UNCONFIRMED_MESSAGE not in buffer.getvalue()
    assert "remote submission failed" in buffer.getvalue()


@pytest.mark.asyncio
async def test_wait_for_terminal_does_not_require_observation_drain() -> None:
    from superjobs.jobs.execution import JobSucceeded

    job = _echo_job("tests.cli.remote.outcome-only")
    done = asyncio.Event()

    class OutcomeOnlyHandle:
        job_id = "exec-outcome"

        def events(self) -> Any:
            async def block_forever() -> AsyncIterator[Any]:
                await done.wait()
                if False:  # pragma: no cover
                    yield None

            return block_forever()

        async def outcome(self, *, wait_timeout: float | None = None) -> JobSucceeded[EchoResult]:
            return JobSucceeded(EchoResult(text="ok"))

    import io

    buffer = io.StringIO()
    outcome = await _wait_for_terminal(
        job,
        OutcomeOnlyHandle(),  # type: ignore[arg-type]
        wait_timeout=1.0,
        write_observation=lambda line: buffer.write(line + "\n"),
        write_error=lambda line: buffer.write(line + "\n"),
    )
    assert isinstance(outcome, JobSucceeded)
    assert outcome.result.text == "ok"
    done.set()


@pytest.mark.nats
@pytest.mark.asyncio
async def test_submit_acceptance_prints_execution_reference(
    nats_broker_factory,
    nats_queue_config,
) -> None:
    run_id = uuid.uuid4().hex
    job = _echo_job(f"tests.cli.remote.accept.{run_id}")

    @asynccontextmanager
    async def worker_runtime() -> AsyncIterator[SuperJobs]:
        jobs = SuperJobs(broker=nats_broker_factory(), queue_config=nats_queue_config)

        @jobs.handler(job)
        async def handler(request: EchoRequest, context: JobContext[None]) -> EchoResult:
            return EchoResult(text=str(request.value))

        async with jobs:
            yield jobs

    @asynccontextmanager
    async def producer_runtime() -> AsyncIterator[SuperJobs]:
        jobs = SuperJobs(broker=nats_broker_factory(), queue_config=nats_queue_config)
        async with jobs:
            yield jobs

    async with worker_runtime():
        registration = CommandRegistration(
            command_name="echo",
            job=job,
            remote_only=True,
            handler=None,
            handler_factory=None,
            input_plan=build_command_input_plan(job),
        )
        prepared = prepare_command_input(
            registration.input_plan,
            {"json_text": '{"value": 9}'},
            None,
        )
        result = await _execute_remote_submit_async(
            registration,
            prepared,
            producer_runtime,
            wait=False,
            wait_timeout=None,
            stderr=sys.stderr,
            shutdown_timeout=10.0,
        )
        assert result.exit_code == EXIT_SUCCESS
        payload = json.loads(result.stdout)
        assert payload["job_name"] == job.name
        assert payload["job_version"] == "v1"
        assert payload["job_id"]


@pytest.mark.nats
@pytest.mark.asyncio
async def test_submit_wait_returns_result_and_streams_observations(
    nats_broker_factory,
    nats_queue_config,
) -> None:
    import io

    run_id = uuid.uuid4().hex
    job = _echo_job(f"tests.cli.remote.wait.{run_id}")

    @asynccontextmanager
    async def worker_runtime() -> AsyncIterator[SuperJobs]:
        jobs = SuperJobs(broker=nats_broker_factory(), queue_config=nats_queue_config)

        @jobs.handler(job)
        async def handler(request: EchoRequest, context: JobContext[None]) -> EchoResult:
            await context.log("phase")
            return EchoResult(text=f"ok-{request.value}")

        async with jobs:
            yield jobs

    @asynccontextmanager
    async def producer_runtime() -> AsyncIterator[SuperJobs]:
        jobs = SuperJobs(broker=nats_broker_factory(), queue_config=nats_queue_config)
        async with jobs:
            yield jobs

    async with worker_runtime():
        registration = CommandRegistration(
            command_name="echo",
            job=job,
            remote_only=True,
            handler=None,
            handler_factory=None,
            input_plan=build_command_input_plan(job),
        )
        prepared = prepare_command_input(
            registration.input_plan,
            {"json_text": '{"value": 3}'},
            None,
        )
        buffer = io.StringIO()
        result = await _execute_remote_submit_async(
            registration,
            prepared,
            producer_runtime,
            wait=True,
            wait_timeout=10.0,
            stderr=buffer,
            shutdown_timeout=10.0,
        )
        assert result.exit_code == EXIT_SUCCESS
        assert json.loads(result.stdout) == {"text": "ok-3"}
        assert any('"kind":"log"' in line for line in buffer.getvalue().splitlines())


@pytest.mark.nats
@pytest.mark.asyncio
async def test_submit_wait_timeout_does_not_cancel_remote_execution(
    nats_broker_factory,
    nats_queue_config,
) -> None:
    run_id = uuid.uuid4().hex
    job = _echo_job(f"tests.cli.remote.timeout.{run_id}")
    gate = asyncio.Event()

    @asynccontextmanager
    async def worker_runtime() -> AsyncIterator[SuperJobs]:
        jobs = SuperJobs(broker=nats_broker_factory(), queue_config=nats_queue_config)

        @jobs.handler(job)
        async def handler(request: EchoRequest, context: JobContext[None]) -> EchoResult:
            await gate.wait()
            return EchoResult(text="late")

        async with jobs:
            yield jobs

    @asynccontextmanager
    async def producer_runtime() -> AsyncIterator[SuperJobs]:
        jobs = SuperJobs(broker=nats_broker_factory(), queue_config=nats_queue_config)
        async with jobs:
            yield jobs

    async with worker_runtime():
        registration = CommandRegistration(
            command_name="echo",
            job=job,
            remote_only=True,
            handler=None,
            handler_factory=None,
            input_plan=build_command_input_plan(job),
        )
        prepared = prepare_command_input(
            registration.input_plan,
            {"json_text": '{"value": 1}'},
            None,
        )
        import io

        buffer = io.StringIO()
        result = await _execute_remote_submit_async(
            registration,
            prepared,
            producer_runtime,
            wait=True,
            wait_timeout=0.2,
            stderr=buffer,
            shutdown_timeout=10.0,
        )
        assert result.exit_code == EXIT_RUNTIME_FAILURE
        assert result.stdout is None
        diagnostics = buffer.getvalue()
        assert "timed out waiting" in diagnostics
        ref_line = next(
            line for line in diagnostics.splitlines() if line.startswith("execution reference:")
        )
        job_id = json.loads(ref_line.split(":", 1)[1].strip())["job_id"]
        async with SuperJobs(broker=nats_broker_factory(), queue_config=nats_queue_config) as jobs:
            handle = await jobs.client(job).get(job_id)
            status = await handle.status()
            assert status.state in {JobState.PENDING, JobState.RUNNING}
        gate.set()


@pytest.mark.nats
@pytest.mark.asyncio
async def test_submit_broker_unavailable_fails() -> None:
    from faststream.nats import NatsBroker

    job = _echo_job(f"tests.cli.remote.down.{uuid.uuid4().hex}")
    registration = CommandRegistration(
        command_name="cmd",
        job=job,
        remote_only=True,
        handler=None,
        handler_factory=None,
        input_plan=build_command_input_plan(job),
    )
    prepared = prepare_command_input(
        registration.input_plan,
        {"json_text": '{"value": 1}'},
        None,
    )

    @asynccontextmanager
    async def producer_runtime() -> AsyncIterator[SuperJobs]:
        jobs = SuperJobs(
            broker=NatsBroker(
                "nats://127.0.0.1:1",
                connect_timeout=1,
                allow_reconnect=False,
                max_reconnect_attempts=0,
            ),
        )
        try:
            yield jobs
        finally:
            if jobs.started:
                await jobs.stop(graceful=False)

    import io

    buffer = io.StringIO()
    result = await _execute_remote_submit_async(
        registration,
        prepared,
        producer_runtime,
        wait=False,
        wait_timeout=None,
        stderr=buffer,
        shutdown_timeout=5.0,
    )
    assert result.exit_code == EXIT_RUNTIME_FAILURE
    assert result.stdout is None
    diagnostics = buffer.getvalue()
    assert "remote submission failed" in diagnostics
    assert SUBMISSION_ACCEPTANCE_UNCONFIRMED_MESSAGE not in diagnostics


@pytest.mark.asyncio
async def test_long_successful_wait_gets_fresh_cleanup_budget() -> None:
    """Cleanup budget must not be consumed by --wait before finally runs."""
    from superjobs.jobs.execution import JobSucceeded

    job = _echo_job("tests.cli.remote.cleanup-budget")
    registration = CommandRegistration(
        command_name="cmd",
        job=job,
        remote_only=True,
        handler=None,
        handler_factory=None,
        input_plan=build_command_input_plan(job),
    )
    prepared = prepare_command_input(
        registration.input_plan,
        {"json_text": '{"value": 1}'},
        None,
    )

    @asynccontextmanager
    async def remote() -> AsyncIterator[SuperJobs]:
        yield SuperJobs(broker=MagicMock())

    import io

    buffer = io.StringIO()
    cleanup_started = asyncio.Event()
    cleanup_finished = asyncio.Event()

    async def slow_wait(*_args: Any, **_kwargs: Any) -> JobSucceeded[EchoResult]:
        await asyncio.sleep(0.4)
        return JobSucceeded(EchoResult(text="done"))

    async def slow_stop(self: SuperJobs, *, graceful: bool = True) -> None:
        cleanup_started.set()
        await asyncio.sleep(0.3)
        self._started = False
        cleanup_finished.set()

    with pytest.MonkeyPatch.context() as patcher:
        patcher.setattr("superjobs.cli.remote_submit.validate_remote_runtime", lambda _r: None)
        patcher.setattr("superjobs.cli.remote_submit._wait_for_terminal", slow_wait)
        patcher.setattr("superjobs.cli.remote_submit._submit_prepared", _fake_submit_handle)
        patcher.setattr(SuperJobs, "start", _noop_start)
        patcher.setattr(SuperJobs, "stop", slow_stop)

        result = await _execute_remote_submit_async(
            registration,
            prepared,
            remote,
            wait=True,
            wait_timeout=5.0,
            stderr=buffer,
            shutdown_timeout=0.5,
        )
    await asyncio.wait_for(cleanup_finished.wait(), timeout=2.0)
    assert result.exit_code == EXIT_SUCCESS
    assert json.loads(result.stdout) == {"text": "done"}


@pytest.mark.asyncio
async def test_wait_timeout_sigint_during_cleanup_exits_130() -> None:
    job = _echo_job("tests.cli.remote.cleanup-interrupt")
    registration = CommandRegistration(
        command_name="cmd",
        job=job,
        remote_only=True,
        handler=None,
        handler_factory=None,
        input_plan=build_command_input_plan(job),
    )
    prepared = prepare_command_input(
        registration.input_plan,
        {"json_text": '{"value": 1}'},
        None,
    )

    @asynccontextmanager
    async def remote() -> AsyncIterator[SuperJobs]:
        yield SuperJobs(broker=MagicMock())

    import io

    buffer = io.StringIO()
    state = _InterruptState(asyncio.Event())

    async def timeout_wait(*_args: Any, **_kwargs: Any) -> Any:
        raise asyncio.TimeoutError()

    stop_entered = asyncio.Event()

    async def block_stop(self: SuperJobs, *, graceful: bool = True) -> None:
        stop_entered.set()
        await state.event.wait()

    async def trigger_interrupt() -> None:
        await stop_entered.wait()
        state.event.set()
        state.requested = True

    with pytest.MonkeyPatch.context() as patcher:
        patcher.setattr("superjobs.cli.remote_submit.validate_remote_runtime", lambda _r: None)
        patcher.setattr("superjobs.cli.remote_submit._wait_for_terminal", timeout_wait)
        patcher.setattr("superjobs.cli.remote_submit._submit_prepared", _fake_submit_handle)
        patcher.setattr(SuperJobs, "start", _noop_start)
        patcher.setattr(SuperJobs, "stop", block_stop)

        trigger = asyncio.create_task(trigger_interrupt())
        result = await _execute_remote_submit_async(
            registration,
            prepared,
            remote,
            wait=True,
            wait_timeout=0.05,
            stderr=buffer,
            shutdown_timeout=2.0,
            interrupt_state=state,
        )
        await trigger
    assert result.exit_code == EXIT_INTERRUPTED
    assert result.stdout is None
    assert "execution reference" in buffer.getvalue()


@pytest.mark.asyncio
async def test_observation_stream_failure_is_not_success() -> None:
    from superjobs.jobs.execution import JobSucceeded

    job = _echo_job("tests.cli.remote.obs-fail")

    class FailingEventsHandle:
        job_id = "exec-obs-fail"

        def events(self) -> Any:
            async def broken() -> AsyncIterator[Any]:
                raise RuntimeError("broken observation stream")
                if False:  # pragma: no cover
                    yield None

            return broken()

        async def outcome(self, *, wait_timeout: float | None = None) -> JobSucceeded[EchoResult]:
            await asyncio.sleep(0.2)
            return JobSucceeded(EchoResult(text="should-not-win"))

    import io

    buffer = io.StringIO()
    with pytest.raises(RuntimeError, match="broken observation"):
        await _wait_for_terminal(
            job,
            FailingEventsHandle(),  # type: ignore[arg-type]
            wait_timeout=2.0,
            write_observation=lambda _line: None,
            write_error=lambda line: buffer.write(line + "\n"),
        )
    assert "observation stream failed" in buffer.getvalue()


@pytest.mark.asyncio
async def test_outcome_first_still_drains_intermediate_observations() -> None:
    from datetime import UTC, datetime

    from superjobs.jobs.events import JobEvent, JobLog
    from superjobs.jobs.execution import JobSucceeded

    job = _echo_job("tests.cli.remote.drain")
    gate = asyncio.Event()

    class OutcomeFirstHandle:
        job_id = "exec-drain"

        def events(self) -> Any:
            async def stream() -> AsyncIterator[JobEvent[None]]:
                await gate.wait()
                yield JobEvent(
                    job_id="exec-drain",
                    sequence=1,
                    timestamp=datetime.now(UTC),
                    attempt=1,
                    data=JobLog(message="mid", level="info", extra={}),
                )

            return stream()

        async def outcome(self, *, wait_timeout: float | None = None) -> JobSucceeded[EchoResult]:
            gate.set()
            await asyncio.sleep(0.05)
            return JobSucceeded(EchoResult(text="ok"))

    import io

    lines: list[str] = []
    outcome = await _wait_for_terminal(
        job,
        OutcomeFirstHandle(),  # type: ignore[arg-type]
        wait_timeout=2.0,
        write_observation=lines.append,
        write_error=lambda line: lines.append(line),
    )
    assert isinstance(outcome, JobSucceeded)
    assert any('"kind":"log"' in line for line in lines)


@pytest.mark.asyncio
async def test_serialization_failure_suppresses_stdout() -> None:
    from superjobs.jobs.execution import JobSucceeded

    job = _echo_job("tests.cli.remote.serialize")
    registration = CommandRegistration(
        command_name="cmd",
        job=job,
        remote_only=True,
        handler=None,
        handler_factory=None,
        input_plan=build_command_input_plan(job),
    )
    prepared = prepare_command_input(
        registration.input_plan,
        {"json_text": '{"value": 1}'},
        None,
    )

    @asynccontextmanager
    async def remote() -> AsyncIterator[SuperJobs]:
        yield SuperJobs(broker=MagicMock())

    import io

    buffer = io.StringIO()

    async def succeed(*_args: Any, **_kwargs: Any) -> JobSucceeded[EchoResult]:
        return JobSucceeded(EchoResult(text="x"))

    with pytest.MonkeyPatch.context() as patcher:
        patcher.setattr("superjobs.cli.remote_submit.validate_remote_runtime", lambda _r: None)
        patcher.setattr("superjobs.cli.remote_submit._wait_for_terminal", succeed)
        patcher.setattr("superjobs.cli.remote_submit._submit_prepared", _fake_submit_handle)
        patcher.setattr(SuperJobs, "start", _noop_start)
        patcher.setattr(SuperJobs, "stop", _noop_stop)
        patcher.setattr(
            "superjobs.cli.remote_submit.format_result_json",
            lambda *_a, **_k: (_ for _ in ()).throw(ValueError("bad wire")),
        )

        result = await _execute_remote_submit_async(
            registration,
            prepared,
            remote,
            wait=True,
            wait_timeout=5.0,
            stderr=buffer,
            shutdown_timeout=2.0,
        )
    assert result.exit_code == EXIT_RUNTIME_FAILURE
    assert result.stdout is None
    assert "failed to serialize job result" in buffer.getvalue()
    assert "execution reference" in buffer.getvalue()


@pytest.mark.asyncio
async def test_wrong_factory_yield_does_not_start_runtime() -> None:
    job = _echo_job("tests.cli.remote.wrong-yield")
    registration = CommandRegistration(
        command_name="cmd",
        job=job,
        remote_only=True,
        handler=None,
        handler_factory=None,
        input_plan=build_command_input_plan(job),
    )
    prepared = prepare_command_input(
        registration.input_plan,
        {"json_text": '{"value": 1}'},
        None,
    )
    start_calls = 0

    class NotSuperJobs:
        transport = MagicMock()
        _handlers = {}
        started = False

        async def start(self) -> None:
            nonlocal start_calls
            start_calls += 1

    @asynccontextmanager
    async def bad_factory() -> AsyncIterator[Any]:
        yield NotSuperJobs()

    import io

    buffer = io.StringIO()
    result = await _execute_remote_submit_async(
        registration,
        prepared,
        bad_factory,
        wait=False,
        wait_timeout=None,
        stderr=buffer,
        shutdown_timeout=2.0,
    )
    assert result.exit_code == EXIT_RUNTIME_FAILURE
    assert start_calls == 0
    assert "must yield a SuperJobs instance" in buffer.getvalue()


@pytest.mark.asyncio
async def test_teardown_failure_after_acceptance_emits_reference() -> None:
    job = _echo_job("tests.cli.remote.teardown")
    registration = CommandRegistration(
        command_name="cmd",
        job=job,
        remote_only=True,
        handler=None,
        handler_factory=None,
        input_plan=build_command_input_plan(job),
    )
    prepared = prepare_command_input(
        registration.input_plan,
        {"json_text": '{"value": 1}'},
        None,
    )

    @asynccontextmanager
    async def remote() -> AsyncIterator[SuperJobs]:
        yield SuperJobs(broker=MagicMock())

    import io

    buffer = io.StringIO()

    async def fail_stop(self: SuperJobs, *, graceful: bool = True) -> None:
        raise RuntimeError("stop blew up")

    with pytest.MonkeyPatch.context() as patcher:
        patcher.setattr("superjobs.cli.remote_submit.validate_remote_runtime", lambda _r: None)
        patcher.setattr("superjobs.cli.remote_submit._submit_prepared", _fake_submit_handle)
        patcher.setattr(SuperJobs, "start", _noop_start)
        patcher.setattr(SuperJobs, "stop", fail_stop)

        result = await _execute_remote_submit_async(
            registration,
            prepared,
            remote,
            wait=False,
            wait_timeout=None,
            stderr=buffer,
            shutdown_timeout=2.0,
        )
    assert result.exit_code == EXIT_RUNTIME_FAILURE
    assert result.stdout is None
    assert buffer.getvalue().count("execution reference:") == 1


async def _fake_submit_handle(*_args: Any, **_kwargs: Any) -> Any:
    class Handle:
        job_id = "exec-test"

    return Handle()


async def _noop_start(self: SuperJobs) -> None:
    self._started = True


async def _noop_stop(self: SuperJobs, *, graceful: bool = True) -> None:
    self._started = False


@pytest.mark.nats
@pytest.mark.asyncio
async def test_wait_timeout_worker_completes_and_result_retrieved(
    nats_broker_factory,
    nats_queue_config,
) -> None:
    run_id = uuid.uuid4().hex
    job = _echo_job(f"tests.cli.remote.late.{run_id}")
    gate = asyncio.Event()

    @asynccontextmanager
    async def worker_runtime() -> AsyncIterator[SuperJobs]:
        jobs = SuperJobs(broker=nats_broker_factory(), queue_config=nats_queue_config)

        @jobs.handler(job)
        async def handler(request: EchoRequest, context: JobContext[None]) -> EchoResult:
            await gate.wait()
            return EchoResult(text="late-ok")

        async with jobs:
            yield jobs

    @asynccontextmanager
    async def producer_runtime() -> AsyncIterator[SuperJobs]:
        jobs = SuperJobs(broker=nats_broker_factory(), queue_config=nats_queue_config)
        async with jobs:
            yield jobs

    async with worker_runtime():
        registration = CommandRegistration(
            command_name="echo",
            job=job,
            remote_only=True,
            handler=None,
            handler_factory=None,
            input_plan=build_command_input_plan(job),
        )
        prepared = prepare_command_input(
            registration.input_plan,
            {"json_text": '{"value": 1}'},
            None,
        )
        import io

        buffer = io.StringIO()
        result = await _execute_remote_submit_async(
            registration,
            prepared,
            producer_runtime,
            wait=True,
            wait_timeout=0.25,
            stderr=buffer,
            shutdown_timeout=10.0,
        )
        assert result.exit_code == EXIT_RUNTIME_FAILURE
        ref_line = next(
            line for line in buffer.getvalue().splitlines() if line.startswith("execution reference:")
        )
        job_id = json.loads(ref_line.split(":", 1)[1].strip())["job_id"]
        gate.set()
        async with SuperJobs(broker=nats_broker_factory(), queue_config=nats_queue_config) as jobs:
            handle = await jobs.client(job).get(job_id)
            outcome = await asyncio.wait_for(handle.outcome(), timeout=10.0)
        assert outcome.result.text == "late-ok"  # type: ignore[union-attr]


@pytest.mark.nats
def test_public_cli_dispatch_via_thread(
    nats_broker_factory,
    nats_queue_config,
) -> None:
    run_id = uuid.uuid4().hex
    job = _echo_job(f"tests.cli.remote.thread.{run_id}")

    @asynccontextmanager
    async def worker_runtime() -> AsyncIterator[SuperJobs]:
        jobs = SuperJobs(broker=nats_broker_factory(), queue_config=nats_queue_config)

        @jobs.handler(job)
        async def handler(request: EchoRequest, context: JobContext[None]) -> EchoResult:
            return EchoResult(text=f"thread-{request.value}")

        async with jobs:
            yield jobs

    @asynccontextmanager
    async def producer_runtime() -> AsyncIterator[SuperJobs]:
        jobs = SuperJobs(broker=nats_broker_factory(), queue_config=nats_queue_config)
        async with jobs:
            yield jobs

    async def exercise() -> None:
        async with worker_runtime():
            cli = _remote_cli(producer_runtime, job, command_name="echo")
            runner = CliRunner()
            invoke_result = await asyncio.to_thread(
                runner.invoke,
                cli.build_typer(),
                ["submit", "echo", "--value", "7"],
            )
            assert invoke_result.exit_code == EXIT_SUCCESS
            payload = json.loads(invoke_result.stdout)
            assert payload["job_name"] == job.name

    asyncio.run(exercise())


def _never_called_factory() -> AsyncIterator[SuperJobs]:
    raise AssertionError("factory must not run")
