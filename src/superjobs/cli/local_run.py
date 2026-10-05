"""In-process local CLI execution through SuperJobs (issue #40)."""

from __future__ import annotations

import asyncio
import json
import signal
import sys
import threading
from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager, AsyncExitStack, suppress
from dataclasses import dataclass
from typing import Any, TextIO, TypeVar, cast

from superjobs.cli.constants import (
    ACTIVE_EVENT_LOOP_MESSAGE,
    EXIT_INTERRUPTED,
    EXIT_RUNTIME_FAILURE,
    EXIT_SUCCESS,
    EXIT_USAGE,
    LOCAL_RUN_SHUTDOWN_TIMEOUT_SECONDS,
)
from superjobs.cli.input_prepare import PreparedCommandInput
from superjobs.cli.handler_registration import register_selected_catalog_binding
from superjobs.cli.registration import CommandRegistration
from superjobs.cli.validation import validate_local_handler
from superjobs.jobs.catalog_runtime import close_provider_stack
from superjobs.jobs.events import (
    JobCancelled,
    JobCompleted,
    JobEvent,
    JobFailed,
    JobLog,
    JobProgress,
    JobRetryScheduled,
    JobStarted,
)
from superjobs.jobs.execution import (
    JobCancelledOutcome,
    JobFailedOutcome,
    JobOutcome,
    JobSucceeded,
)
from superjobs.jobs.job import Job
from superjobs.jobs.job_handle import JobHandle
from superjobs.jobs.retry_policy import RetryPolicy
from superjobs.payload.strict import dump_python_value
from superjobs.superjobs import SuperJobs
from superjobs.transport.in_memory import InMemoryTransport
from superjobs.transport.nats_backend import NatsJobBackend

__all__ = [
    "LocalRunResult",
    "LocalRuntimeValidationError",
    "format_observation_line",
    "format_result_json",
    "run_local_command",
    "validate_local_runtime",
]


class LocalRuntimeValidationError(ValueError):
    """Raised when an application factory yields an unsupported local runtime."""


@dataclass(frozen=True, slots=True)
class LocalRunResult:
    exit_code: int
    stdout: str | None = None


@dataclass
class _InterruptState:
    event: asyncio.Event
    requested: bool = False


def format_result_json(job: Job[Any, Any, Any], result: Any) -> str:
    if job.result_codec is None:
        return "null"
    wire = dump_python_value(job.result_codec.adapter, result)
    return json.dumps(wire, allow_nan=False, separators=(",", ":"))


def _observation_payload(job: Job[Any, Any, Any], data: Any) -> dict[str, Any]:
    if isinstance(data, JobStarted):
        return {"kind": "started"}
    if isinstance(data, JobCompleted):
        return {"kind": "completed"}
    if isinstance(data, JobFailed):
        return {"kind": "failed", "error": data.error.to_wire()}
    if isinstance(data, JobCancelled):
        return {"kind": "cancelled", "reason": data.reason}
    if isinstance(data, JobRetryScheduled):
        return {
            "kind": "retry_scheduled",
            "attempt": data.attempt,
            "delay": data.delay,
        }
    if isinstance(data, JobProgress):
        return {
            "kind": "progress",
            "completed": data.completed,
            "total": data.total,
        }
    if isinstance(data, JobLog):
        return {
            "kind": "log",
            "message": data.message,
            "level": data.level,
            "extra": data.extra,
        }
    if job.event_codec is None:
        raise TypeError("unexpected application observation without an event codec")
    return {
        "kind": "application",
        "payload": dump_python_value(job.event_codec.adapter, data),
    }


def format_observation_line(job: Job[Any, Any, Any], event: JobEvent[Any]) -> str:
    payload = _observation_payload(job, event.data)
    envelope = {
        "job_id": event.job_id,
        "sequence": event.sequence,
        "attempt": event.attempt,
        "timestamp": event.timestamp.isoformat(),
        "observation": payload,
    }
    return json.dumps(envelope, allow_nan=False, separators=(",", ":"))


def validate_local_runtime(runtime: SuperJobs) -> None:
    """Reject remote, shared or pre-wired runtimes before handler registration."""
    if not isinstance(runtime, SuperJobs):
        raise LocalRuntimeValidationError(
            "local_runtime_factory must yield a SuperJobs instance",
        )
    if isinstance(runtime.transport, NatsJobBackend):
        raise LocalRuntimeValidationError(
            "local run cannot use a NATS-backed SuperJobs runtime",
        )
    if not isinstance(runtime.transport, InMemoryTransport):
        raise LocalRuntimeValidationError(
            "local run requires SuperJobs(transport=InMemoryTransport(...))",
        )
    if runtime._handlers:
        raise LocalRuntimeValidationError(
            "local run requires a runtime without pre-registered handlers",
        )
    transport = runtime.transport
    if transport._owners > (1 if runtime.started else 0) or transport._executions:
        raise LocalRuntimeValidationError(
            "local run requires a fresh isolated in-memory transport",
        )
    if any(subscriptions for subscriptions in transport._work_subscriptions.values()):
        raise LocalRuntimeValidationError(
            "local run requires an isolated in-memory transport without active work consumers",
        )


def _resolve_handler(registration: CommandRegistration) -> Callable[..., Any]:
    if registration.handler is not None:
        return registration.handler
    if registration.handler_factory is None:
        raise RuntimeError("local command has no handler")
    handler = registration.handler_factory()
    validate_local_handler(registration.job, handler)
    return handler


async def _register_selected_handler(
    runtime: SuperJobs,
    registration: CommandRegistration,
) -> AsyncExitStack | None:
    if registration.catalog_binding is not None:
        return await register_selected_catalog_binding(runtime, registration.catalog_binding)
    handler = _resolve_handler(registration)
    runtime.register(
        cast(Any, registration.job),
        cast(Any, handler),
        retry=RetryPolicy(max_attempts=1),
    )
    return None


async def _ensure_runtime_ready(runtime: SuperJobs) -> None:
    if not runtime.started:
        await runtime.start()


async def _submit_prepared(
    runtime: SuperJobs,
    registration: CommandRegistration,
    prepared: PreparedCommandInput,
) -> JobHandle[Any, Any, Any]:
    client = runtime.client(registration.job)
    if registration.job.request_type is None:
        return await client.submit()  # type: ignore[call-arg]
    return await client.submit(prepared.request)


async def _stream_observations_and_outcome(
    job: Job[Any, Any, Any],
    handle: JobHandle[Any, Any, Any],
    write_observation: Callable[[str], None],
) -> JobOutcome[Any]:
    # The public observation iterator closes at the terminal event. Reading the
    # authoritative outcome afterward needs no competing background waiter.
    events = handle.events()
    try:
        async for event in events:
            write_observation(format_observation_line(job, event))
        return await handle.outcome()
    finally:
        close = getattr(events, "aclose", None)
        if close is not None:
            await close()


class _LocalInterrupted(Exception):
    pass


PhaseT = TypeVar("PhaseT")


async def _settle_cancelled(task: asyncio.Future[Any], write_error: Callable[[str], None]) -> None:
    task.cancel()
    done, _ = await asyncio.wait({task}, timeout=1.0)
    if not done:
        write_error(
            "Error: owned work is refusing asyncio cancellation; waiting for it "
            "to finish. Python cannot safely terminate arbitrary in-process work."
        )
    # Do not return with owned work still running, including resistant callbacks.
    with suppress(asyncio.CancelledError):
        await task


async def _bounded_call(
    operation: Awaitable[Any],
    *,
    timeout: float,
    label: str,
    write_error: Callable[[str], None],
) -> Any:
    task = asyncio.ensure_future(operation)
    done, _ = await asyncio.wait({task}, timeout=max(0.0, timeout))
    if done:
        return task.result()
    write_error(f"Error: {label} exceeded its shutdown bound; requesting asyncio cancellation")
    await _settle_cancelled(task, write_error)
    raise TimeoutError(f"{label} exceeded its shutdown bound")


class _SignalInterruptRegistration:
    def __init__(self, loop: asyncio.AbstractEventLoop, state: _InterruptState) -> None:
        self._loop = loop
        self._state = state
        self._previous: Any = None

    def install(self) -> None:
        if threading.current_thread() is not threading.main_thread():
            return

        def handler(signum: int, frame: Any) -> None:
            self._state.requested = True
            self._loop.call_soon_threadsafe(self._state.event.set)

        self._previous = signal.getsignal(signal.SIGINT)
        signal.signal(signal.SIGINT, handler)

    def restore(self) -> None:
        if self._previous is not None:
            signal.signal(signal.SIGINT, self._previous)
            self._previous = None


async def _await_phase(
    operation: Awaitable[PhaseT],
    *,
    state: _InterruptState,
    handle: JobHandle[Any, Any, Any] | None,
    timeout: float,
    write_error: Callable[[str], None],
) -> PhaseT:
    task = asyncio.ensure_future(operation)
    interrupted = asyncio.create_task(state.event.wait(), name="superjobs-cli-interrupt")
    try:
        done, _ = await asyncio.wait({task, interrupted}, return_when=asyncio.FIRST_COMPLETED)
        if interrupted not in done and not state.requested:
            return task.result()
        state.requested = True
        # Allow an entered factory to be reclaimed even if entry and SIGINT race.
        if task.done() and handle is None:
            return task.result()
        if handle is not None:
            deadline = asyncio.get_running_loop().time() + timeout
            try:
                await _bounded_call(
                    handle.cancel(), timeout=timeout, label="cancellation request",
                    write_error=write_error,
                )
            except Exception as exc:
                write_error(f"Error: cancellation request failed: {exc}")
            done, _ = await asyncio.wait(
                {task}, timeout=max(0.0, deadline - asyncio.get_running_loop().time()),
            )
            if done:
                # Retrieve an observation failure even if cancellation won the race.
                if not task.cancelled() and task.exception() is not None:
                    write_error(f"Error: interrupted execution: {task.exception()}")
                raise _LocalInterrupted
            write_error(f"Error: interrupted local run did not finish within {timeout:g}s; forcing stop")
        await _settle_cancelled(task, write_error)
        raise _LocalInterrupted
    finally:
        interrupted.cancel()
        with suppress(asyncio.CancelledError):
            await interrupted
        if not task.done():
            await _settle_cancelled(task, write_error)


async def _execute_local_run_async(
    registration: CommandRegistration,
    prepared: PreparedCommandInput,
    runtime_factory: Callable[[], AbstractAsyncContextManager[SuperJobs]],
    *,
    stderr: TextIO,
    shutdown_timeout: float,
    interrupt_state: _InterruptState | None = None,
) -> LocalRunResult:
    write_error = lambda message: print(message, file=stderr, flush=True)
    write_observation = lambda line: print(line, file=stderr, flush=True)
    state = interrupt_state or _InterruptState(asyncio.Event())
    loop = asyncio.get_running_loop()
    signals = _SignalInterruptRegistration(loop, state)
    signals.install()
    manager: AbstractAsyncContextManager[SuperJobs] | None = None
    runtime: SuperJobs | None = None
    provider_stack: AsyncExitStack | None = None
    entered = False
    owned = False
    primary: BaseException | None = None
    result = LocalRunResult(EXIT_RUNTIME_FAILURE)
    cleanup_failed = False
    try:
        manager = runtime_factory()
        runtime = await _await_phase(
            manager.__aenter__(), state=state, handle=None,
            timeout=shutdown_timeout, write_error=write_error,
        )
        entered = True
        validate_local_runtime(runtime)
        owned = True
        if state.requested:
            raise _LocalInterrupted
        provider_stack = await _await_phase(
            _register_selected_handler(runtime, registration),
            state=state, handle=None,
            timeout=shutdown_timeout, write_error=write_error,
        )
        await _await_phase(
            _ensure_runtime_ready(runtime), state=state, handle=None,
            timeout=shutdown_timeout, write_error=write_error,
        )
        await _await_phase(
            runtime._handlers[registration.job.canonical_name].wait_ready(),
            state=state, handle=None, timeout=shutdown_timeout, write_error=write_error,
        )
        if state.requested:
            raise _LocalInterrupted
        handle = await _await_phase(
            _submit_prepared(runtime, registration, prepared), state=state, handle=None,
            timeout=shutdown_timeout, write_error=write_error,
        )
        outcome = await _await_phase(
            _stream_observations_and_outcome(registration.job, handle, write_observation),
            state=state, handle=handle, timeout=shutdown_timeout, write_error=write_error,
        )
        if isinstance(outcome, JobSucceeded):
            result = LocalRunResult(EXIT_SUCCESS, format_result_json(registration.job, outcome.result))
        elif isinstance(outcome, JobFailedOutcome):
            write_error(f"Error: {outcome.error.message} (code={outcome.error.code})")
        elif isinstance(outcome, JobCancelledOutcome):
            write_error(f"Error: {outcome.reason or 'Job cancelled'} (code=cancelled)")
    except _LocalInterrupted as exc:
        primary = exc
        state.requested = True
    except asyncio.CancelledError as exc:
        primary = exc
        state.requested = True
    except Exception as exc:
        primary = exc
        write_error(f"Error: local execution failed: {exc}")
    finally:
        deadline = loop.time() + shutdown_timeout

        async def cleanup(operation: Awaitable[Any], label: str) -> None:
            nonlocal cleanup_failed
            try:
                await _bounded_call(
                    operation, timeout=max(0.0, deadline - loop.time()),
                    label=label, write_error=write_error,
                )
            except (Exception, asyncio.CancelledError) as exc:
                cleanup_failed = True
                write_error(f"Error: {label} failed: {exc}")

        if owned and runtime is not None:
            # SuperJobs.stop deliberately gathers handler failures for ordinary
            # workers. The CLI stops its one owned handler explicitly so those
            # failures remain visible and cannot produce successful stdout.
            handler = runtime._handlers.get(registration.job.canonical_name)
            if handler is not None:
                await cleanup(handler.stop(graceful=False), "selected handler shutdown")
            was_started = runtime.started
            await cleanup(runtime.stop(graceful=False), "runtime shutdown")
            transport = cast(InMemoryTransport, runtime.transport)
            if not was_started or transport.started:
                # start() can fail before SuperJobs marks itself started; stop()
                # would then be a no-op even if the backend acquired resources.
                await cleanup(transport.stop(), "transport startup rollback")
        if provider_stack is not None:

            async def _release_provider_stack() -> None:
                release_error = await close_provider_stack(provider_stack)
                if release_error is not None:
                    raise release_error

            await cleanup(_release_provider_stack(), "provider cleanup")
        if entered and manager is not None:
            await cleanup(
                manager.__aexit__(
                    type(primary) if primary is not None else None,
                    primary, primary.__traceback__ if primary is not None else None,
                ),
                "application factory cleanup",
            )
        signals.restore()
    if state.requested or state.event.is_set():
        return LocalRunResult(EXIT_INTERRUPTED)
    if cleanup_failed:
        return LocalRunResult(EXIT_RUNTIME_FAILURE)
    return result

def run_local_command(
    registration: CommandRegistration,
    prepared: PreparedCommandInput,
    runtime_factory: Callable[[], AbstractAsyncContextManager[SuperJobs]],
    *,
    stderr: TextIO | None = None,
    shutdown_timeout: float | None = None,
    interrupt_state: _InterruptState | None = None,
) -> LocalRunResult:
    """Run one Job locally inside a fresh asyncio event loop in the current thread."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        pass
    else:
        stream = stderr if stderr is not None else sys.stderr
        print(ACTIVE_EVENT_LOOP_MESSAGE, file=stream)
        return LocalRunResult(exit_code=EXIT_USAGE)

    stream = stderr if stderr is not None else sys.stderr
    bound = (
        shutdown_timeout
        if shutdown_timeout is not None
        else LOCAL_RUN_SHUTDOWN_TIMEOUT_SECONDS
    )

    async def runner() -> LocalRunResult:
        return await _execute_local_run_async(
            registration,
            prepared,
            runtime_factory,
            stderr=stream,
            shutdown_timeout=bound,
            interrupt_state=interrupt_state,
        )

    try:
        return asyncio.run(runner())
    except KeyboardInterrupt:
        return LocalRunResult(exit_code=EXIT_INTERRUPTED)
