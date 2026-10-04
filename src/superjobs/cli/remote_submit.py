"""NATS remote CLI submission with optional result waiting (issue #41)."""

from __future__ import annotations

import asyncio
import json
import logging
import math
import sys
from collections.abc import Awaitable, Callable, Iterator
from contextlib import AbstractAsyncContextManager, contextmanager, redirect_stdout, suppress
from dataclasses import dataclass
from typing import Any, TextIO, TypeVar, cast

from superjobs.cli.constants import (
    ACTIVE_EVENT_LOOP_MESSAGE,
    DEFAULT_CLI_WAIT_TIMEOUT_SECONDS,
    EXIT_INTERRUPTED,
    EXIT_RUNTIME_FAILURE,
    EXIT_SUCCESS,
    EXIT_USAGE,
    MISSING_REMOTE_FACTORY_MESSAGE,
    REMOTE_SUBMIT_OBSERVATION_DRAIN_GRACE_SECONDS,
    REMOTE_SUBMIT_SHUTDOWN_TIMEOUT_SECONDS,
    SUBMISSION_ACCEPTANCE_UNCONFIRMED_MESSAGE,
)
from superjobs.cli.input_prepare import PreparedCommandInput
from superjobs.cli.local_run import (
    _InterruptState,
    _SignalInterruptRegistration,
    _bounded_call as _untyped_bounded_call,
    _settle_cancelled,
    _submit_prepared,
    format_observation_line,
    format_result_json,
)
from superjobs.cli.registration import CommandRegistration
from superjobs.jobs.execution import (
    JobCancelledOutcome,
    JobFailedOutcome,
    JobOutcome,
    JobSucceeded,
)
from superjobs.jobs.job import Job
from superjobs.jobs.job_handle import JobHandle
from superjobs.superjobs import SuperJobs
from superjobs.transport.nats_backend import NatsJobBackend

__all__ = [
    "RemoteRuntimeValidationError",
    "RemoteSubmitResult",
    "format_execution_reference",
    "run_remote_command",
    "validate_remote_runtime",
]


class RemoteRuntimeValidationError(ValueError):
    """Raised when an application factory yields an unsupported remote runtime."""


@dataclass(frozen=True, slots=True)
class RemoteSubmitResult:
    exit_code: int
    stdout: str | None = None


def format_execution_reference(job: Job[Any, Any, Any], job_id: str) -> str:
    payload = {
        "job_id": job_id,
        "job_name": job.name,
        "job_version": job.version,
    }
    return json.dumps(payload, allow_nan=False, separators=(",", ":"))


def _invalid_wait_timeout(wait_timeout: float | None) -> bool:
    if wait_timeout is None:
        return False
    return not math.isfinite(wait_timeout) or wait_timeout <= 0


def validate_remote_runtime(runtime: SuperJobs) -> None:
    """Reject local, shared, worker-wired, or consumer-wired runtimes before submit."""
    if not isinstance(runtime, SuperJobs):
        raise RemoteRuntimeValidationError(
            "remote_runtime_factory must yield a SuperJobs instance",
        )
    if not isinstance(runtime.transport, NatsJobBackend):
        raise RemoteRuntimeValidationError(
            "remote submit requires SuperJobs(broker=...) with a NATS transport",
        )
    if runtime._handlers:
        raise RemoteRuntimeValidationError(
            "remote submit requires a runtime without pre-registered handlers",
        )
    broker = runtime.transport.broker
    if broker.subscribers:
        raise RemoteRuntimeValidationError(
            "remote submit requires a NATS broker without active work subscribers",
        )


class _RemoteInterrupted(Exception):
    pass


PhaseT = TypeVar("PhaseT")


async def _bounded_call(
    operation: Awaitable[PhaseT],
    *,
    timeout: float,
    label: str,
    write_error: Callable[[str], None],
) -> PhaseT:
    return cast(PhaseT, await _untyped_bounded_call(
        operation, timeout=timeout, label=label, write_error=write_error,
    ))


@dataclass
class _CleanupBudget:
    seconds: float
    deadline: float | None = None

    def remaining(self) -> float:
        loop = asyncio.get_running_loop()
        if self.deadline is None:
            self.deadline = loop.time() + self.seconds
        return max(0.0, self.deadline - loop.time())


@contextmanager
def _remote_output(stderr: TextIO) -> Iterator[None]:
    """Keep producer startup diagnostics out of the command's JSON stdout."""
    logger = logging.getLogger("faststream.access.nats")
    original_stdout = sys.stdout
    original_handlers = tuple(logger.handlers)
    redirected = []
    for handler in original_handlers:
        if isinstance(handler, logging.StreamHandler) and handler.stream is original_stdout:
            redirected.append(handler)
            handler.stream = stderr
    try:
        # FastStream lazily binds new default access handlers to sys.stdout.
        with redirect_stdout(stderr):
            yield
    finally:
        for handler in tuple(logger.handlers):
            if handler not in original_handlers and isinstance(handler, logging.StreamHandler) and handler.stream is stderr:
                logger.removeHandler(handler)
        for handler in redirected:
            handler.stream = original_stdout


def _write_execution_reference(
    job: Job[Any, Any, Any],
    job_id: str,
    write_error: Callable[[str], None],
) -> None:
    write_error(f"execution reference: {format_execution_reference(job, job_id)}")


def _report_unconfirmed_acceptance(write_error: Callable[[str], None]) -> None:
    write_error(f"Error: {SUBMISSION_ACCEPTANCE_UNCONFIRMED_MESSAGE}")


async def _await_remote_phase(
    operation: Awaitable[PhaseT],
    *,
    state: _InterruptState,
    write_error: Callable[[str], None],
    settle_timeout: float,
    cleanup_budget: _CleanupBudget | None = None,
) -> PhaseT:
    budget = cleanup_budget or _CleanupBudget(settle_timeout)
    task = asyncio.ensure_future(operation)
    interrupted = asyncio.create_task(state.event.wait(), name="superjobs-cli-interrupt")
    try:
        done, _ = await asyncio.wait({task, interrupted}, return_when=asyncio.FIRST_COMPLETED)
        if interrupted not in done and not state.requested:
            return task.result()
        state.requested = True
        if task.done():
            return task.result()
        await _bounded_call(
            _settle_cancelled(task, write_error),
            timeout=budget.remaining(),
            label="remote phase cancellation",
            write_error=write_error,
        )
        raise _RemoteInterrupted
    finally:
        interrupted.cancel()
        with suppress(asyncio.CancelledError):
            await interrupted
        if not task.done():
            await _bounded_call(
                _settle_cancelled(task, write_error),
                timeout=budget.remaining(),
                label="remote phase cleanup",
                write_error=write_error,
            )


async def _close_observation_iterator(events: Any) -> None:
    close = getattr(events, "aclose", None)
    if close is None:
        return
    await close()


async def _wait_for_terminal(
    job: Job[Any, Any, Any],
    handle: JobHandle[Any, Any, Any],
    *,
    wait_timeout: float,
    write_observation: Callable[[str], None],
    write_error: Callable[[str], None],
    cleanup_budget: _CleanupBudget | None = None,
) -> JobOutcome[Any]:
    """Read the durable outcome and observations independently on one deadline."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + wait_timeout
    budget = cleanup_budget or _CleanupBudget(REMOTE_SUBMIT_SHUTDOWN_TIMEOUT_SECONDS)
    events = handle.events()

    async def drain_observations() -> None:
        async for event in events:
            write_observation(format_observation_line(job, event))

    observation_task = asyncio.create_task(drain_observations(), name="superjobs-cli-observations")
    outcome_task = asyncio.create_task(handle.outcome(), name="superjobs-cli-outcome")
    tasks = (observation_task, outcome_task)
    primary: BaseException | None = None
    try:
        pending = set(tasks)
        while pending:
            done, pending = await asyncio.wait(
                pending,
                timeout=max(0.0, deadline - loop.time()),
                return_when=asyncio.FIRST_COMPLETED,
            )
            if not done:
                raise TimeoutError()
            # Inspect observations first even when both tasks complete together.
            if observation_task in done:
                try:
                    observation_task.result()
                except Exception as exc:
                    write_error(f"Error: observation stream failed: {exc}")
                    raise
            if outcome_task in done:
                outcome = outcome_task.result()
                if not observation_task.done():
                    await asyncio.wait(
                        {observation_task},
                        timeout=min(
                            max(0.0, deadline - loop.time()),
                            REMOTE_SUBMIT_OBSERVATION_DRAIN_GRACE_SECONDS,
                        ),
                    )
                if observation_task.done():
                    try:
                        observation_task.result()
                    except Exception as exc:
                        write_error(f"Error: observation stream failed: {exc}")
                        raise
                return outcome
        raise TimeoutError()
    except BaseException as exc:
        primary = exc
        raise
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        cleanup_error: BaseException | None = None
        try:
            results = await _bounded_call(
                asyncio.gather(*tasks, return_exceptions=True),
                timeout=budget.remaining(),
                label="observation and outcome cleanup",
                write_error=write_error,
            )
            for result in results:
                if isinstance(result, Exception) and result is not primary:
                    cleanup_error = result
                    break
        except (Exception, asyncio.CancelledError) as exc:
            cleanup_error = exc
        try:
            await _bounded_call(
                _close_observation_iterator(events),
                timeout=budget.remaining(),
                label="observation iterator close",
                write_error=write_error,
            )
        except (Exception, asyncio.CancelledError) as exc:
            cleanup_error = exc
        if cleanup_error is not None:
            write_error(f"Error: observation cleanup failed: {cleanup_error}")
            if primary is None:
                raise cleanup_error



def _maybe_emit_accepted_reference(
    *,
    submission_accepted: bool,
    handle: JobHandle[Any, Any, Any] | None,
    job: Job[Any, Any, Any],
    write_error: Callable[[str], None],
    reference_emitted: bool,
) -> bool:
    if reference_emitted or not submission_accepted or handle is None:
        return reference_emitted
    _write_execution_reference(job, handle.job_id, write_error)
    return True


async def _execute_remote_submit_async(
    registration: CommandRegistration,
    prepared: PreparedCommandInput,
    runtime_factory: Callable[[], AbstractAsyncContextManager[SuperJobs]] | None,
    *,
    wait: bool,
    wait_timeout: float | None,
    stderr: TextIO,
    shutdown_timeout: float,
    interrupt_state: _InterruptState | None = None,
) -> RemoteSubmitResult:
    write_error = lambda message: print(message, file=stderr, flush=True)
    write_observation = lambda line: print(line, file=stderr, flush=True)
    state = interrupt_state or _InterruptState(asyncio.Event())
    loop = asyncio.get_running_loop()
    signals = _SignalInterruptRegistration(loop, state)
    signals.install()
    manager: AbstractAsyncContextManager[SuperJobs] | None = None
    runtime: SuperJobs | None = None
    entered = False
    reclaim_runtime = False
    handle: JobHandle[Any, Any, Any] | None = None
    submission_accepted = False
    reference_emitted = False
    primary: BaseException | None = None
    exit_code = EXIT_RUNTIME_FAILURE
    stdout_payload: str | None = None
    cleanup_failed = False
    in_submit_phase = False
    cleanup_budget = _CleanupBudget(shutdown_timeout)

    def phase_timeout() -> float:
        return shutdown_timeout

    if runtime_factory is None:
        write_error(f"Error: {MISSING_REMOTE_FACTORY_MESSAGE}")
        signals.restore()
        return RemoteSubmitResult(EXIT_RUNTIME_FAILURE)

    effective_wait_timeout = (
        wait_timeout if wait_timeout is not None else DEFAULT_CLI_WAIT_TIMEOUT_SECONDS
    )

    try:
        manager = runtime_factory()
        runtime = await _bounded_call(
            _await_remote_phase(
                manager.__aenter__(),
                state=state,
                write_error=write_error,
                settle_timeout=phase_timeout(),
                cleanup_budget=cleanup_budget,
            ),
            timeout=phase_timeout(),
            label="application factory entry",
            write_error=write_error,
        )
        entered = True
        validate_remote_runtime(runtime)
        reclaim_runtime = True
        if state.requested:
            raise _RemoteInterrupted
        if not runtime.started:
            await _bounded_call(
                _await_remote_phase(
                    runtime.start(),
                    state=state,
                    write_error=write_error,
                    settle_timeout=phase_timeout(),
                    cleanup_budget=cleanup_budget,
                ),
                timeout=phase_timeout(),
                label="runtime startup",
                write_error=write_error,
            )
        if state.requested:
            raise _RemoteInterrupted
        in_submit_phase = True
        handle = await _bounded_call(
            _await_remote_phase(
                _submit_prepared(runtime, registration, prepared),
                state=state,
                write_error=write_error,
                settle_timeout=phase_timeout(),
                cleanup_budget=cleanup_budget,
            ),
            timeout=phase_timeout(),
            label="remote submission",
            write_error=write_error,
        )
        in_submit_phase = False
        submission_accepted = True
        if not wait:
            exit_code = EXIT_SUCCESS
            stdout_payload = format_execution_reference(registration.job, handle.job_id)
        else:
            try:
                outcome = await _await_remote_phase(
                    _wait_for_terminal(
                        registration.job,
                        handle,
                        wait_timeout=effective_wait_timeout,
                        write_observation=write_observation,
                        write_error=write_error,
                        cleanup_budget=cleanup_budget,
                    ),
                    state=state,
                    write_error=write_error,
                    settle_timeout=shutdown_timeout,
                    cleanup_budget=cleanup_budget,
                )
            except asyncio.TimeoutError:
                write_error(
                    f"Error: timed out waiting for final result after {effective_wait_timeout:g}s",
                )
                reference_emitted = _maybe_emit_accepted_reference(
                    submission_accepted=submission_accepted,
                    handle=handle,
                    job=registration.job,
                    write_error=write_error,
                    reference_emitted=reference_emitted,
                )
            else:
                if isinstance(outcome, JobSucceeded):
                    try:
                        stdout_payload = format_result_json(registration.job, outcome.result)
                    except Exception as exc:
                        write_error(f"Error: failed to serialize job result: {exc}")
                        reference_emitted = _maybe_emit_accepted_reference(
                            submission_accepted=submission_accepted,
                            handle=handle,
                            job=registration.job,
                            write_error=write_error,
                            reference_emitted=reference_emitted,
                        )
                    else:
                        exit_code = EXIT_SUCCESS
                elif isinstance(outcome, JobFailedOutcome):
                    write_error(f"Error: {outcome.error.message} (code={outcome.error.code})")
                    reference_emitted = _maybe_emit_accepted_reference(
                        submission_accepted=submission_accepted,
                        handle=handle,
                        job=registration.job,
                        write_error=write_error,
                        reference_emitted=reference_emitted,
                    )
                elif isinstance(outcome, JobCancelledOutcome):
                    write_error(
                        f"Error: {outcome.reason or 'Job cancelled'} (code=cancelled)",
                    )
                    reference_emitted = _maybe_emit_accepted_reference(
                        submission_accepted=submission_accepted,
                        handle=handle,
                        job=registration.job,
                        write_error=write_error,
                        reference_emitted=reference_emitted,
                    )
    except _RemoteInterrupted as exc:
        primary = exc
        state.requested = True
        if submission_accepted:
            reference_emitted = _maybe_emit_accepted_reference(
                submission_accepted=submission_accepted,
                handle=handle,
                job=registration.job,
                write_error=write_error,
                reference_emitted=reference_emitted,
            )
        elif in_submit_phase:
            _report_unconfirmed_acceptance(write_error)
    except asyncio.CancelledError as exc:
        primary = exc
        state.requested = True
        if submission_accepted:
            reference_emitted = _maybe_emit_accepted_reference(
                submission_accepted=submission_accepted,
                handle=handle,
                job=registration.job,
                write_error=write_error,
                reference_emitted=reference_emitted,
            )
        elif in_submit_phase:
            _report_unconfirmed_acceptance(write_error)
    except RemoteRuntimeValidationError as exc:
        primary = exc
        write_error(f"Error: remote submission rejected runtime: {exc}")
    except Exception as exc:
        primary = exc
        if submission_accepted and handle is not None:
            write_error(f"Error: remote submission failed after acceptance: {exc}")
            reference_emitted = _maybe_emit_accepted_reference(
                submission_accepted=submission_accepted,
                handle=handle,
                job=registration.job,
                write_error=write_error,
                reference_emitted=reference_emitted,
            )
        elif in_submit_phase:
            write_error(f"Error: remote submission failed: {exc}")
            _report_unconfirmed_acceptance(write_error)
        else:
            write_error(f"Error: remote submission failed: {exc}")
    finally:
        async def cleanup(operation: Awaitable[Any], label: str) -> None:
            nonlocal cleanup_failed
            try:
                await _bounded_call(
                    operation,
                    timeout=cleanup_budget.remaining(),
                    label=label,
                    write_error=write_error,
                )
            except (Exception, asyncio.CancelledError) as exc:
                cleanup_failed = True
                write_error(f"Error: {label} failed: {exc}")

        try:
            if reclaim_runtime and runtime is not None:
                was_started = runtime.started
                await cleanup(runtime.stop(graceful=False), "runtime shutdown")
                transport = cast(NatsJobBackend, runtime.transport)
                if not was_started or transport.started:
                    await cleanup(transport.stop(), "transport shutdown")
                if not was_started:
                    # Backend.start can acquire a broker connection before its
                    # started flag is set. Its stop() is then a no-op.
                    await cleanup(transport.broker.stop(), "broker startup rollback")
            if entered and manager is not None:
                await cleanup(
                    manager.__aexit__(
                        type(primary) if primary is not None else None,
                        primary,
                        primary.__traceback__ if primary is not None else None,
                    ),
                    "application factory cleanup",
                )
        finally:
            signals.restore()

    if cleanup_failed and submission_accepted:
        reference_emitted = _maybe_emit_accepted_reference(
            submission_accepted=submission_accepted,
            handle=handle,
            job=registration.job,
            write_error=write_error,
            reference_emitted=reference_emitted,
        )
        exit_code = EXIT_RUNTIME_FAILURE
        stdout_payload = None

    if state.requested or state.event.is_set():
        if submission_accepted:
            reference_emitted = _maybe_emit_accepted_reference(
                submission_accepted=submission_accepted,
                handle=handle,
                job=registration.job,
                write_error=write_error,
                reference_emitted=reference_emitted,
            )
        return RemoteSubmitResult(EXIT_INTERRUPTED)
    if cleanup_failed:
        return RemoteSubmitResult(EXIT_RUNTIME_FAILURE)
    if exit_code == EXIT_SUCCESS:
        return RemoteSubmitResult(EXIT_SUCCESS, stdout_payload)
    if submission_accepted and not reference_emitted:
        _maybe_emit_accepted_reference(
            submission_accepted=submission_accepted,
            handle=handle,
            job=registration.job,
            write_error=write_error,
            reference_emitted=reference_emitted,
        )
    return RemoteSubmitResult(EXIT_RUNTIME_FAILURE)


def run_remote_command(
    registration: CommandRegistration,
    prepared: PreparedCommandInput,
    runtime_factory: Callable[[], AbstractAsyncContextManager[SuperJobs]] | None,
    *,
    wait: bool = False,
    wait_timeout: float | None = None,
    stderr: TextIO | None = None,
    shutdown_timeout: float | None = None,
    interrupt_state: _InterruptState | None = None,
) -> RemoteSubmitResult:
    """Submit one Job remotely inside a fresh asyncio event loop in the current thread."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        pass
    else:
        stream = stderr if stderr is not None else sys.stderr
        print(ACTIVE_EVENT_LOOP_MESSAGE, file=stream)
        return RemoteSubmitResult(exit_code=EXIT_USAGE)

    stream = stderr if stderr is not None else sys.stderr
    if wait_timeout is not None and not wait:
        print("Error: --wait-timeout requires --wait", file=stream)
        return RemoteSubmitResult(exit_code=EXIT_USAGE)
    if _invalid_wait_timeout(wait_timeout):
        print("Error: --wait-timeout must be a finite positive number", file=stream)
        return RemoteSubmitResult(exit_code=EXIT_USAGE)

    bound = (
        shutdown_timeout
        if shutdown_timeout is not None
        else REMOTE_SUBMIT_SHUTDOWN_TIMEOUT_SECONDS
    )

    async def runner() -> RemoteSubmitResult:
        return await _execute_remote_submit_async(
            registration,
            prepared,
            runtime_factory,
            wait=wait,
            wait_timeout=wait_timeout,
            stderr=stream,
            shutdown_timeout=bound,
            interrupt_state=interrupt_state,
        )

    try:
        with _remote_output(stream):
            return asyncio.run(runner())
    except KeyboardInterrupt:
        return RemoteSubmitResult(exit_code=EXIT_INTERRUPTED)
