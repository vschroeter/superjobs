"""Local in-process CLI execution tests (issue #40)."""

from __future__ import annotations

import asyncio
import json
import os
import queue
import signal
import subprocess
import sys
import textwrap
import threading
from collections.abc import AsyncIterator, Awaitable, Callable
from unittest.mock import patch
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

import pytest
from typer.testing import CliRunner

from superjobs import InMemoryTransport, Job, JobContext, SuperJobs
from superjobs.cli import (
    EXIT_INTERRUPTED,
    EXIT_RUNTIME_FAILURE,
    EXIT_SUCCESS,
    EXIT_USAGE,
    JobCLI,
)
from superjobs.cli.input_prepare import prepare_command_input
from superjobs.cli.local_run import (
    LocalRuntimeValidationError,
    _InterruptState,
    _execute_local_run_async,
    validate_local_runtime,
)
from superjobs.cli.schema_plan import build_command_input_plan
from superjobs.transport.nats_backend import NatsJobBackend


@dataclass
class EchoRequest:
    value: int


@dataclass
class EchoResult:
    text: str


@dataclass(frozen=True)
class StageEvent:
    label: str


def _echo_job() -> Job[EchoRequest, EchoResult, None]:
    return Job("tests.cli.local.echo", version="v1", request=EchoRequest, result=EchoResult)


def _event_job() -> Job[EchoRequest, EchoResult, StageEvent]:
    return Job(
        "tests.cli.local.events",
        version="v1",
        request=EchoRequest,
        result=EchoResult,
        event=StageEvent,
    )


@asynccontextmanager
async def _local_runtime() -> AsyncIterator[SuperJobs]:
    jobs = SuperJobs(transport=InMemoryTransport())
    async with jobs:
        yield jobs


def _cli_with_runtime(
    job: Job[Any, Any, Any],
    handler: Callable[..., Any],
    *,
    handler_factory: Callable[[], Callable[..., Any]] | None = None,
) -> JobCLI:
    cli = JobCLI(local_runtime_factory=_local_runtime)
    if handler_factory is not None:
        cli.add("cmd", job, handler_factory=handler_factory)
    else:
        cli.add("cmd", job, handler=handler)
    return cli


def test_run_without_custom_factory_uses_builtin_runtime() -> None:
    job = _echo_job()
    cli = JobCLI()

    async def handler(request: EchoRequest, context: JobContext[None]) -> EchoResult:
        return EchoResult(text="builtin")

    cli.add("cmd", job, handler=handler)
    result = CliRunner().invoke(cli.build_typer(), ["run", "cmd", "--value", "1"])
    assert result.exit_code == EXIT_SUCCESS
    assert json.loads(result.stdout) == {"text": "builtin"}


def test_run_async_handler_success_json_stdout() -> None:
    job = _echo_job()

    async def handler(request: EchoRequest, context: JobContext[None]) -> EchoResult:
        return EchoResult(text=str(request.value * 2))

    cli = _cli_with_runtime(job, handler)
    result = CliRunner().invoke(cli.build_typer(), ["run", "cmd", "--value", "3"])
    assert result.exit_code == EXIT_SUCCESS
    assert json.loads(result.stdout) == {"text": "6"}
    assert '"kind":"completed"' in result.stderr


def test_run_sync_handler_success() -> None:
    job = _echo_job()

    def handler(request: EchoRequest, context: JobContext[None]) -> EchoResult:
        return EchoResult(text="sync")

    cli = _cli_with_runtime(job, handler)
    result = CliRunner().invoke(cli.build_typer(), ["run", "cmd", "--value", "1"])
    assert result.exit_code == EXIT_SUCCESS
    assert json.loads(result.stdout) == {"text": "sync"}


def test_run_no_request_job_null_stdout() -> None:
    job = Job("tests.cli.local.noreq", version="v1")

    async def handler(context: JobContext[None]) -> None:
        await context.log("done")

    cli = _cli_with_runtime(job, handler)
    result = CliRunner().invoke(cli.build_typer(), ["run", "cmd"])
    assert result.exit_code == EXIT_SUCCESS
    assert result.stdout.strip() == "null"
    assert any('"kind":"log"' in line for line in result.stderr.splitlines())


def test_run_streams_events_progress_and_logs() -> None:
    job = _event_job()

    async def handler(request: EchoRequest, context: JobContext[StageEvent]) -> EchoResult:
        await context.log("phase")
        await context.emit(StageEvent(label="mid"))
        await context.progress(completed=1, total=1)
        return EchoResult(text=str(request.value))

    cli = _cli_with_runtime(job, handler)
    result = CliRunner().invoke(cli.build_typer(), ["run", "cmd", "--value", "9"])
    assert result.exit_code == EXIT_SUCCESS
    stderr = result.stderr
    assert '"kind":"log"' in stderr
    assert '"kind":"application"' in stderr
    assert '"kind":"progress"' in stderr
    assert '"kind":"completed"' in stderr


def test_run_invalid_result_exits_one() -> None:
    job = _echo_job()

    async def handler(request: EchoRequest, context: JobContext[None]) -> EchoResult:
        return EchoResult(text=object())  # type: ignore[arg-type]

    cli = _cli_with_runtime(job, handler)
    result = CliRunner().invoke(cli.build_typer(), ["run", "cmd", "--value", "1"])
    assert result.exit_code == EXIT_RUNTIME_FAILURE
    assert "invalid_result" in result.stderr or "code=invalid_result" in result.stderr
    assert result.stdout == ""


def test_run_handler_exception_exits_one() -> None:
    job = _echo_job()

    async def handler(request: EchoRequest, context: JobContext[None]) -> EchoResult:
        raise RuntimeError("boom")

    cli = _cli_with_runtime(job, handler)
    result = CliRunner().invoke(cli.build_typer(), ["run", "cmd", "--value", "1"])
    assert result.exit_code == EXIT_RUNTIME_FAILURE
    assert "boom" in result.stderr
    assert result.stdout == ""


def test_run_invalid_input_still_exits_two_before_runtime() -> None:
    job = _echo_job()
    calls: list[str] = []

    @asynccontextmanager
    async def tracked_runtime() -> AsyncIterator[SuperJobs]:
        calls.append("runtime")
        async with _local_runtime() as jobs:
            yield jobs

    async def handler(request: EchoRequest, context: JobContext[None]) -> EchoResult:
        return EchoResult(text="ok")

    cli = JobCLI(local_runtime_factory=tracked_runtime)
    cli.add("cmd", job, handler=handler)
    result = CliRunner().invoke(cli.build_typer(), ["run", "cmd", "--value", "nope"])
    assert result.exit_code == EXIT_USAGE
    assert calls == []


def test_run_lazy_handler_factory_not_called_on_help() -> None:
    job = _echo_job()
    calls = 0

    def factory() -> Callable[[EchoRequest, JobContext[None]], Awaitable[EchoResult]]:
        nonlocal calls
        calls += 1

        async def handler(request: EchoRequest, context: JobContext[None]) -> EchoResult:
            return EchoResult(text="lazy")

        return handler

    cli = JobCLI(local_runtime_factory=_local_runtime)
    cli.add("cmd", job, handler_factory=factory)
    help_result = CliRunner().invoke(cli.build_typer(), ["run", "cmd", "--help"])
    assert help_result.exit_code == EXIT_SUCCESS
    assert calls == 0
    run_result = CliRunner().invoke(cli.build_typer(), ["run", "cmd", "--value", "2"])
    assert run_result.exit_code == EXIT_SUCCESS
    assert calls == 1
    assert json.loads(run_result.stdout) == {"text": "lazy"}


def test_run_single_attempt_no_retry_observation() -> None:
    job = _echo_job()
    attempts = 0

    async def handler(request: EchoRequest, context: JobContext[None]) -> EchoResult:
        nonlocal attempts
        attempts += 1
        raise RuntimeError("retryable")

    cli = _cli_with_runtime(job, handler)
    result = CliRunner().invoke(cli.build_typer(), ["run", "cmd", "--value", "1"])
    assert result.exit_code == EXIT_RUNTIME_FAILURE
    assert attempts == 1
    assert "retry_scheduled" not in result.stderr


def test_run_startup_failure_is_visible() -> None:
    job = _echo_job()

    @asynccontextmanager
    async def broken_runtime() -> AsyncIterator[SuperJobs]:
        raise RuntimeError("startup failed")
        yield  # pragma: no cover

    async def handler(request: EchoRequest, context: JobContext[None]) -> EchoResult:
        return EchoResult(text="x")

    cli = JobCLI(local_runtime_factory=broken_runtime)
    cli.add("cmd", job, handler=handler)
    result = CliRunner().invoke(cli.build_typer(), ["run", "cmd", "--value", "1"])
    assert result.exit_code == EXIT_RUNTIME_FAILURE
    assert "startup failed" in result.stderr


@pytest.mark.skipif(sys.platform == "win32", reason="SIGINT delivery differs on Windows")
def test_run_process_interrupt_requests_cancellation() -> None:
    script = textwrap.dedent(
        """
        import asyncio
        from contextlib import asynccontextmanager
        from dataclasses import dataclass

        from superjobs import InMemoryTransport, Job, JobContext, SuperJobs
        from superjobs.cli import JobCLI, EXIT_INTERRUPTED

        @dataclass
        class WaitRequest:
            token: str

        @dataclass
        class WaitResult:
            ok: bool

        job = Job("tests.cli.local.interrupt", version="v1", request=WaitRequest, result=WaitResult)
        gate = asyncio.Event()

        @asynccontextmanager
        async def local_runtime():
            jobs = SuperJobs(transport=InMemoryTransport())
            async with jobs:
                yield jobs

        async def handler(request: WaitRequest, context: JobContext[None]) -> WaitResult:
            print("ready", flush=True)
            while True:
                await context.check_cancelled()
                try:
                    await asyncio.wait_for(gate.wait(), timeout=0.05)
                except asyncio.TimeoutError:
                    continue
            return WaitResult(ok=False)

        cli = JobCLI(local_runtime_factory=local_runtime)
        cli.add("wait", job, handler=handler)
        raise SystemExit(cli.main(["run", "wait", "--token", "x"]))
        """
    )
    proc = subprocess.Popen(
        [sys.executable, "-c", script],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env={**os.environ, "PYTHONNOUSERSITE": "1"},
    )
    try:
        ready_lines: queue.Queue[str] = queue.Queue()
        assert proc.stdout is not None
        reader = threading.Thread(
            target=lambda: ready_lines.put(proc.stdout.readline()), daemon=True,
        )
        reader.start()
        ready_line = ready_lines.get(timeout=10)
        assert ready_line.strip() == "ready"
        proc.send_signal(signal.SIGINT)
        stdout, stderr = proc.communicate(timeout=30)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.communicate(timeout=5)
        raise AssertionError("CLI did not exit after SIGINT")
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.communicate(timeout=5)
        reader.join(timeout=5)
    assert proc.returncode == EXIT_INTERRUPTED
    assert stdout == ""
    assert stderr.strip()


def test_run_executes_handler_in_cli_process() -> None:
    import os

    job = Job(
        "tests.cli.local.pid",
        version="v1",
        request=EchoRequest,
        result=EchoResult,
    )

    async def handler(request: EchoRequest, context: JobContext[None]) -> EchoResult:
        return EchoResult(text=str(os.getpid()))

    cli = _cli_with_runtime(job, handler)
    result = CliRunner().invoke(cli.build_typer(), ["run", "cmd", "--value", "1"])
    assert result.exit_code == EXIT_SUCCESS
    assert json.loads(result.stdout) == {"text": str(os.getpid())}


def test_main_process_local_run_entry_point() -> None:
    script = textwrap.dedent(
        """
        import os
        import sys
        from contextlib import asynccontextmanager
        from dataclasses import dataclass

        from superjobs import InMemoryTransport, Job, JobContext, SuperJobs
        from superjobs.cli import JobCLI

        @dataclass
        class PidRequest:
            note: str

        @dataclass
        class PidResult:
            pid: int

        job = Job("tests.cli.local.pid.proc", version="v1", request=PidRequest, result=PidResult)

        @asynccontextmanager
        async def local_runtime():
            jobs = SuperJobs(transport=InMemoryTransport())
            async with jobs:
                yield jobs

        async def handler(request: PidRequest, context: JobContext[None]) -> PidResult:
            return PidResult(pid=os.getpid())

        cli = JobCLI(local_runtime_factory=local_runtime)
        cli.add("pid", job, handler=handler)
        print(f"cli-pid:{os.getpid()}", file=sys.stderr, flush=True)
        raise SystemExit(cli.main(["run", "pid", "--note", "x"]))
        """
    )
    completed = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        env={**os.environ, "PYTHONNOUSERSITE": "1"},
        check=False,
    )
    assert completed.returncode == EXIT_SUCCESS
    marker = next(
        line for line in completed.stderr.splitlines() if line.startswith("cli-pid:")
    )
    cli_pid = int(marker.split(":", 1)[1])
    result_pid = json.loads(completed.stdout)["pid"]
    assert cli_pid == result_pid


def test_validate_local_runtime_rejects_nats_backend() -> None:
    from unittest.mock import MagicMock

    runtime = SuperJobs(broker=MagicMock())
    with pytest.raises(LocalRuntimeValidationError, match="NATS"):
        validate_local_runtime(runtime)


def test_validate_local_runtime_rejects_preregistered_handlers() -> None:
    job = _echo_job()

    async def handler(request: EchoRequest, context: JobContext[None]) -> EchoResult:
        return EchoResult(text="x")

    runtime = SuperJobs(transport=InMemoryTransport())
    runtime.register(job, handler)
    with pytest.raises(LocalRuntimeValidationError, match="pre-registered"):
        validate_local_runtime(runtime)


def test_run_rejects_invalid_runtime_before_handler() -> None:
    from unittest.mock import MagicMock

    job = _echo_job()
    handler_calls = 0

    async def handler(request: EchoRequest, context: JobContext[None]) -> EchoResult:
        nonlocal handler_calls
        handler_calls += 1
        return EchoResult(text="x")

    @asynccontextmanager
    async def nats_runtime() -> AsyncIterator[SuperJobs]:
        yield SuperJobs(broker=MagicMock())

    cli = JobCLI(local_runtime_factory=nats_runtime)
    cli.add("cmd", job, handler=handler)
    result = CliRunner().invoke(cli.build_typer(), ["run", "cmd", "--value", "1"])
    assert result.exit_code == EXIT_RUNTIME_FAILURE
    assert "NATS" in result.stderr
    assert handler_calls == 0


def test_run_unstarted_runtime_from_factory() -> None:
    job = _echo_job()

    @asynccontextmanager
    async def unstarted_runtime() -> AsyncIterator[SuperJobs]:
        yield SuperJobs(transport=InMemoryTransport())

    async def handler(request: EchoRequest, context: JobContext[None]) -> EchoResult:
        return EchoResult(text="fresh")

    cli = JobCLI(local_runtime_factory=unstarted_runtime)
    cli.add("cmd", job, handler=handler)
    result = CliRunner().invoke(cli.build_typer(), ["run", "cmd", "--value", "1"])
    assert result.exit_code == EXIT_SUCCESS
    assert json.loads(result.stdout) == {"text": "fresh"}


def test_run_lazy_invalid_handler_exits_one() -> None:
    job = _echo_job()

    def factory() -> Callable[[EchoRequest, JobContext[None]], Awaitable[EchoResult]]:
        return 42  # type: ignore[return-value]

    cli = JobCLI(local_runtime_factory=_local_runtime)
    cli.add("cmd", job, handler_factory=factory)
    result = CliRunner().invoke(cli.build_typer(), ["run", "cmd", "--value", "1"])
    assert result.exit_code == EXIT_RUNTIME_FAILURE
    assert result.stdout == ""


def test_run_observation_serialization_failure_exits_one() -> None:
    job = _event_job()

    async def handler(request: EchoRequest, context: JobContext[StageEvent]) -> EchoResult:
        await context.emit(StageEvent(label="ok"))
        return EchoResult(text="ok")

    cli = _cli_with_runtime(job, handler)
    with patch.object(
        type(job.event_codec.adapter),  # type: ignore[union-attr]
        "dump",
        side_effect=RuntimeError("bad observation"),
    ):
        result = CliRunner().invoke(cli.build_typer(), ["run", "cmd", "--value", "1"])
    assert result.exit_code == EXIT_RUNTIME_FAILURE
    assert "bad observation" in result.stderr
    assert result.stdout == ""


def test_run_factory_cleanup_failure_is_visible() -> None:
    job = _echo_job()

    @asynccontextmanager
    async def cleanup_fails() -> AsyncIterator[SuperJobs]:
        jobs = SuperJobs(transport=InMemoryTransport())
        async with jobs:
            yield jobs
        raise asyncio.CancelledError("cleanup failed")

    async def handler(request: EchoRequest, context: JobContext[None]) -> EchoResult:
        return EchoResult(text="ok")

    cli = JobCLI(local_runtime_factory=cleanup_fails)
    cli.add("cmd", job, handler=handler)
    result = CliRunner().invoke(cli.build_typer(), ["run", "cmd", "--value", "1"])
    assert result.exit_code == EXIT_RUNTIME_FAILURE
    assert "cleanup failed" in result.stderr


def test_run_context_identity_and_attempt() -> None:
    job = _echo_job()
    seen: list[tuple[str, int]] = []

    async def handler(request: EchoRequest, context: JobContext[None]) -> EchoResult:
        seen.append((context.id, context.attempt))
        return EchoResult(text=context.id)

    cli = _cli_with_runtime(job, handler)
    result = CliRunner().invoke(cli.build_typer(), ["run", "cmd", "--value", "1"])
    assert result.exit_code == EXIT_SUCCESS
    assert len(seen) == 1
    job_id, attempt = seen[0]
    assert job_id
    assert attempt == 1
    assert json.loads(result.stdout)["text"] == job_id


def test_run_active_event_loop_reports_usage() -> None:
    import typer

    job = _echo_job()
    cli = JobCLI()

    async def handler(request: EchoRequest, context: JobContext[None]) -> EchoResult:
        return EchoResult(text="x")

    cli.add("cmd", job, handler=handler)
    app = typer.Typer()
    cli.mount(app)

    async def invoke_from_loop() -> None:
        runner = CliRunner()
        result = runner.invoke(app, ["run", "cmd", "--value", "1"])
        assert result.exit_code == EXIT_USAGE
        assert "event loop is already running" in result.stderr

    asyncio.run(invoke_from_loop())


def test_interrupt_during_run_returns_130() -> None:
    job = _echo_job()
    registration = None
    cli = JobCLI(local_runtime_factory=_local_runtime)

    started = asyncio.Event()
    cancel_requested = asyncio.Event()

    class CancellationTransport(InMemoryTransport):
        async def request_cancel(self, identity: Any, job_id: str) -> Any:
            status = await super().request_cancel(identity, job_id)
            cancel_requested.set()
            return status

    @asynccontextmanager
    async def runtime_factory() -> AsyncIterator[SuperJobs]:
        async with SuperJobs(transport=CancellationTransport()) as jobs:
            yield jobs

    async def handler(request: EchoRequest, context: JobContext[None]) -> EchoResult:
        started.set()
        await cancel_requested.wait()
        await context.check_cancelled()
        return EchoResult(text="late")

    cli.add("cmd", job, handler=handler)
    registration = cli.registrations()[0]
    plan = build_command_input_plan(job)
    prepared = prepare_command_input(plan, {"json_text": '{"value": 1}'}, None)

    async def exercise() -> int:
        state = _InterruptState(asyncio.Event())

        async def trigger() -> None:
            await started.wait()
            state.event.set()

        asyncio.create_task(trigger(), name="test-interrupt-trigger")
        result = await _execute_local_run_async(
            registration,
            prepared,
            runtime_factory,
            stderr=sys.stderr,
            shutdown_timeout=0.2,
            interrupt_state=state,
        )
        return result.exit_code

    assert asyncio.run(exercise()) == EXIT_INTERRUPTED


def test_non_cooperative_handler_reports_shutdown_bound() -> None:
    job = _echo_job()
    registration = None
    cli = JobCLI(local_runtime_factory=_local_runtime)

    started = asyncio.Event()

    async def handler(request: EchoRequest, context: JobContext[None]) -> EchoResult:
        started.set()
        await asyncio.Event().wait()
        return EchoResult(text="never")

    cli.add("cmd", job, handler=handler)
    registration = cli.registrations()[0]
    plan = build_command_input_plan(job)
    prepared = prepare_command_input(plan, {"json_text": '{"value": 1}'}, None)

    async def exercise() -> tuple[int, str]:
        import io

        buffer = io.StringIO()
        state = _InterruptState(asyncio.Event())

        async def trigger() -> None:
            await started.wait()
            state.event.set()

        asyncio.create_task(trigger(), name="test-interrupt-trigger")
        result = await _execute_local_run_async(
            registration,
            prepared,
            _local_runtime,
            stderr=buffer,
            shutdown_timeout=0.05,
            interrupt_state=state,
        )
        return result.exit_code, buffer.getvalue()

    exit_code, diagnostics = asyncio.run(exercise())
    assert exit_code == EXIT_INTERRUPTED
    assert (
        "interrupted local run did not finish" in diagnostics
        or "cancellation did not finish" in diagnostics
        or "shutdown exceeded" in diagnostics
        or "forcing stop" in diagnostics
    )


def test_signal_handler_restored_after_run() -> None:
    job = _echo_job()

    async def handler(request: EchoRequest, context: JobContext[None]) -> EchoResult:
        return EchoResult(text="x")

    cli = _cli_with_runtime(job, handler)
    previous = signal.getsignal(signal.SIGINT)
    CliRunner().invoke(cli.build_typer(), ["run", "cmd", "--value", "1"])
    assert signal.getsignal(signal.SIGINT) == previous


def _registration_without_request(handler: Callable[..., Any]) -> Any:
    cli = JobCLI()
    cli.add("probe", Job("tests.cli.lifecycle.probe"), handler=handler)
    return cli.registrations()[0]


@pytest.mark.parametrize("stage", ["entry", "start", "exit"])
def test_interrupt_covers_application_lifecycle(stage: str) -> None:
    import io
    from superjobs.cli.input_prepare import PreparedCommandInput

    async def exercise() -> None:
        began = asyncio.Event()
        closed = asyncio.Event()
        state = _InterruptState(asyncio.Event())
        handler_called = False

        class StartupTransport(InMemoryTransport):
            async def start(self) -> None:
                if stage == "start":
                    began.set()
                    await asyncio.Event().wait()
                await super().start()

        @asynccontextmanager
        async def factory() -> AsyncIterator[SuperJobs]:
            try:
                if stage == "entry":
                    began.set()
                    await asyncio.Event().wait()
                yield SuperJobs(transport=StartupTransport())
                if stage == "exit":
                    began.set()
                    await asyncio.Event().wait()
            finally:
                closed.set()

        async def handler(context: JobContext[None]) -> None:
            nonlocal handler_called
            handler_called = True

        async def trigger() -> None:
            await began.wait()
            state.event.set()

        before = asyncio.all_tasks()
        interrupt = asyncio.create_task(trigger())
        buffer = io.StringIO()
        async with asyncio.timeout(2):
            result = await _execute_local_run_async(
                _registration_without_request(handler), PreparedCommandInput(None, False),
                factory, stderr=buffer, shutdown_timeout=0.05, interrupt_state=state,
            )
        await interrupt
        assert result.exit_code == EXIT_INTERRUPTED
        assert result.stdout is None
        assert closed.is_set()
        assert handler_called is (stage == "exit")
        assert asyncio.all_tasks() == before

    asyncio.run(exercise())


def test_factory_exit_timeout_fails_without_result_or_task_leak() -> None:
    import io
    from superjobs.cli.input_prepare import PreparedCommandInput

    async def exercise() -> None:
        closed = asyncio.Event()

        @asynccontextmanager
        async def factory() -> AsyncIterator[SuperJobs]:
            try:
                yield SuperJobs()
                await asyncio.Event().wait()
            finally:
                closed.set()

        async def handler(context: JobContext[None]) -> None:
            pass

        buffer = io.StringIO()
        before = asyncio.all_tasks()
        async with asyncio.timeout(2):
            result = await _execute_local_run_async(
                _registration_without_request(handler), PreparedCommandInput(None, False),
                factory, stderr=buffer, shutdown_timeout=0.05,
            )
        assert result.exit_code == EXIT_RUNTIME_FAILURE
        assert result.stdout is None
        assert "application factory cleanup exceeded" in buffer.getvalue()
        assert closed.is_set()
        assert asyncio.all_tasks() == before

    asyncio.run(exercise())


@pytest.mark.parametrize("failure", ["handler-stop", "transport-stop", "handler-start"])
def test_owned_runtime_lifecycle_errors_fail_without_success_stdout(failure: str) -> None:
    from superjobs.jobs.job_handler import JobHandler

    class FailingTransport(InMemoryTransport):
        async def stop(self) -> None:
            await super().stop()
            if failure == "transport-stop":
                raise RuntimeError("transport cleanup failed")

    @asynccontextmanager
    async def factory() -> AsyncIterator[SuperJobs]:
        async with SuperJobs(transport=FailingTransport()) as runtime:
            yield runtime

    async def handler(context: JobContext[None]) -> None:
        pass

    cli = JobCLI(local_runtime_factory=factory)
    cli.add("probe", Job("tests.cli.cleanup.failed"), handler=handler)
    if failure == "transport-stop":
        result = CliRunner().invoke(cli.build_typer(), ["run", "probe"])
    else:
        method = "stop" if failure == "handler-stop" else "start"
        with patch.object(JobHandler, method, side_effect=RuntimeError("handler lifecycle failed")):
            result = CliRunner().invoke(cli.build_typer(), ["run", "probe"])
    assert result.exit_code == EXIT_RUNTIME_FAILURE
    assert result.stdout == ""
    assert "failed" in result.stderr


def test_observation_error_stops_active_execution_without_leaking_tasks() -> None:
    import io
    from superjobs.cli.input_prepare import PreparedCommandInput

    async def exercise() -> None:
        closed = asyncio.Event()
        began = asyncio.Event()

        async def handler(context: JobContext[None]) -> None:
            try:
                began.set()
                await asyncio.Event().wait()
            finally:
                closed.set()

        def broken_observation(*args: Any) -> str:
            raise RuntimeError("observation formatting failed")

        buffer = io.StringIO()
        before = asyncio.all_tasks()
        async with asyncio.timeout(2):
            with patch("superjobs.cli.local_run.format_observation_line", broken_observation):
                result = await _execute_local_run_async(
                    _registration_without_request(handler), PreparedCommandInput(None, False),
                    _local_runtime, stderr=buffer, shutdown_timeout=0.05,
                )
        assert began.is_set()
        assert closed.is_set()
        assert result.exit_code == EXIT_RUNTIME_FAILURE
        assert result.stdout is None
        assert "observation formatting failed" in buffer.getvalue()
        assert asyncio.all_tasks() == before

    asyncio.run(exercise())


def test_selected_handler_and_factory_snapshot_in_mounted_app() -> None:
    import typer

    calls: list[str] = []

    @asynccontextmanager
    async def factory() -> AsyncIterator[SuperJobs]:
        calls.append("runtime")
        yield SuperJobs()

    def unselected() -> Any:
        raise AssertionError("unselected handler initialized")

    async def handler(context: JobContext[None]) -> None:
        calls.append("handler")

    cli = JobCLI(local_runtime_factory=factory)
    cli.add("selected", Job("tests.cli.selection"), handler=handler)
    cli.add("unselected", Job("tests.cli.unselected"), handler_factory=unselected)
    app = typer.Typer()
    cli.mount(app)
    cli.local_runtime_factory = unselected
    result = CliRunner().invoke(app, ["run", "selected"])
    assert result.exit_code == EXIT_SUCCESS
    assert result.stdout.strip() == "null"
    assert calls == ["runtime", "handler"]


def test_local_runtime_rejects_shared_and_used_transports() -> None:
    transport = InMemoryTransport()
    runtime = SuperJobs(transport=transport)
    transport._owners = 2
    with pytest.raises(LocalRuntimeValidationError, match="fresh isolated"):
        validate_local_runtime(runtime)


@pytest.mark.parametrize("has_request", [False, True])
@pytest.mark.parametrize("has_result", [False, True])
@pytest.mark.parametrize("has_event", [False, True])
def test_local_payload_presence_combinations(has_request: bool, has_result: bool, has_event: bool) -> None:
    job = Job(
        "tests.cli.presence", request=EchoRequest if has_request else None,
        result=EchoResult if has_result else None, event=StageEvent if has_event else None,
    )

    async def execute(context: JobContext[Any]) -> Any:
        if has_event:
            await context.emit(StageEvent("stage"))
        return EchoResult("done") if has_result else None

    async def requested(request: EchoRequest, context: JobContext[Any]) -> Any:
        assert request.value == 7
        return await execute(context)

    async def requestless(context: JobContext[Any]) -> Any:
        return await execute(context)

    cli = JobCLI()
    cli.add("probe", job, handler=requested if has_request else requestless)
    argv = ["run", "probe", "--json", '{"value":7}'] if has_request else ["run", "probe"]
    result = CliRunner().invoke(cli.build_typer(), argv)
    assert result.exit_code == EXIT_SUCCESS, result.output
    assert json.loads(result.stdout) == ({"text": "done"} if has_result else None)
    assert ('"kind":"application"' in result.stderr) is has_event


def test_transport_acquired_before_startup_failure_is_closed() -> None:
    transport = InMemoryTransport()

    class StartupFails(InMemoryTransport):
        async def start(self) -> None:
            await super().start()
            raise RuntimeError("partial startup failed")

    transport = StartupFails()

    @asynccontextmanager
    async def factory() -> AsyncIterator[SuperJobs]:
        yield SuperJobs(transport=transport)

    async def handler(context: JobContext[None]) -> None:
        raise AssertionError("handler must not run")

    cli = JobCLI(local_runtime_factory=factory)
    cli.add("probe", Job("tests.cli.partial.startup"), handler=handler)
    result = CliRunner().invoke(cli.build_typer(), ["run", "probe"])
    assert result.exit_code == EXIT_RUNTIME_FAILURE
    assert "partial startup failed" in result.stderr
    assert not transport.started
    assert transport._owners == 0
    assert result.stdout == ""


def test_native_sigint_has_no_success_stdout_and_restores_signal() -> None:
    previous = signal.getsignal(signal.SIGINT)

    async def handler(context: JobContext[None]) -> None:
        signal.raise_signal(signal.SIGINT)

    cli = JobCLI()
    cli.add("probe", Job("tests.cli.native.interrupt"), handler=handler)
    result = CliRunner().invoke(cli.build_typer(), ["run", "probe"])
    assert result.exit_code == EXIT_INTERRUPTED
    assert result.stdout == ""
    assert signal.getsignal(signal.SIGINT) == previous
