"""Deadline-bounded sample scheduling (shared by producer and deterministic tests)."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Protocol

try:
    from accounting import CompletionRecord, SampleAccounting, SampleWindow, exception_fields
except ModuleNotFoundError:
    from tools.performance_support.accounting import (
        CompletionRecord,
        SampleAccounting,
        SampleWindow,
        exception_fields,
    )

TrackFn = Callable[..., Awaitable[None]]
SubmitFn = Callable[[int], Awaitable[Any]]


class _MeasurementClock(Protocol):
    def perf_counter(self) -> float: ...


@dataclass(slots=True)
class SampleRunConfig:
    inflight: int
    sample_seconds: float
    drain_seconds: float
    sequence_start: int
    submit_timeout_seconds: float = 120.0
    terminal_wait_seconds: float = 30.0


async def acquire_slot_before(
    sem: asyncio.Semaphore,
    deadline_mono: float,
    *,
    clock: _MeasurementClock | None = None,
) -> bool:
    tick = (clock or time).perf_counter
    while True:
        now = tick()
        if now >= deadline_mono:
            return False
        remaining = deadline_mono - now
        try:
            await asyncio.wait_for(sem.acquire(), timeout=remaining)
            return True
        except TimeoutError:
            return False


def _submit_accept_timeout(
    *,
    now_mono: float,
    fixed_boundary: float,
    configured_cap: float,
) -> float:
    remaining_to_boundary = fixed_boundary - now_mono
    if remaining_to_boundary <= 0:
        return 0.0
    return min(remaining_to_boundary, configured_cap)


MIN_SUBMIT_BUDGET_SECONDS = 0.05


async def run_sample(
    submit_fn: SubmitFn,
    track_fn: TrackFn,
    *,
    config: SampleRunConfig,
    submission_boundary_mono: float | None = None,
    clock: _MeasurementClock | None = None,
    accounting: SampleAccounting | None = None,
) -> tuple[SampleAccounting, int]:
    """Run one sample window with deadline-bounded acquire/submit and bounded drain."""
    tick = (clock or time).perf_counter
    window_start = tick()
    fixed_boundary = (
        submission_boundary_mono
        if submission_boundary_mono is not None
        else window_start + config.sample_seconds
    )
    lifecycle_end = fixed_boundary + config.drain_seconds
    if accounting is None:
        accounting = SampleAccounting(
            window=SampleWindow(
                submission_start_mono=window_start,
                submission_end_mono=fixed_boundary,
                measured_submission_end_mono=window_start,
                lifecycle_end_mono=lifecycle_end,
            ),
        )
    else:
        accounting.window = SampleWindow(
            submission_start_mono=window_start,
            submission_end_mono=fixed_boundary,
            measured_submission_end_mono=window_start,
            lifecycle_end_mono=lifecycle_end,
        )
    sem = asyncio.Semaphore(config.inflight)
    pending: set[asyncio.Task[None]] = set()
    sequence = config.sequence_start
    terminal_wait = min(
        config.terminal_wait_seconds,
        max(1.0, config.drain_seconds + 1.0),
    )

    while tick() < fixed_boundary:
        if not await acquire_slot_before(sem, fixed_boundary, clock=clock):
            break
        if tick() >= fixed_boundary:
            sem.release()
            break
        submit_started = tick()
        accept_timeout = _submit_accept_timeout(
            now_mono=submit_started,
            fixed_boundary=fixed_boundary,
            configured_cap=config.submit_timeout_seconds,
        )
        if accept_timeout < MIN_SUBMIT_BUDGET_SECONDS:
            sem.release()
            break
        seq = sequence
        sequence += 1
        handle: Any | None = None
        submit_finished: float | None = None
        submit_error: str | None = None
        try:
            handle = await asyncio.wait_for(
                submit_fn(seq),
                timeout=accept_timeout,
            )
            submit_finished = tick()
        except TimeoutError:
            submit_error = "submit accept timed out"
            sem.release()
            accounting.add(
                CompletionRecord(
                    sequence=seq,
                    submit_started_mono=submit_started,
                    submit_finished_mono=submit_finished,
                    terminal_mono=None,
                    bucket="submit_uncertain",
                    phase="submit",
                    detail=submit_error,
                ),
            )
            continue
        except Exception as exc:
            fields = exception_fields(exc)
            sem.release()
            accounting.add(
                CompletionRecord(
                    sequence=seq,
                    submit_started_mono=submit_started,
                    submit_finished_mono=submit_finished,
                    terminal_mono=None,
                    bucket="submit_failed",
                    phase="submit",
                    detail=fields["detail"],
                    exception_type=fields["exception_type"],
                    exception_repr=fields["exception_repr"],
                    exception_message=fields["exception_message"],
                ),
            )
            continue

        if tick() >= fixed_boundary:
            sem.release()
            if handle is not None:
                task = asyncio.create_task(
                    track_fn(
                        handle,
                        submit_started=submit_started,
                        submit_finished=submit_finished,
                        seq=seq,
                        boundary_mono=fixed_boundary,
                        terminal_wait=terminal_wait,
                        sem=sem,
                        release_sem=False,
                    ),
                )
                pending.add(task)
                task.add_done_callback(pending.discard)
            break

        task = asyncio.create_task(
            track_fn(
                handle,
                submit_started=submit_started,
                submit_finished=submit_finished,
                seq=seq,
                boundary_mono=fixed_boundary,
                terminal_wait=terminal_wait,
                sem=sem,
                release_sem=True,
            ),
        )
        pending.add(task)
        task.add_done_callback(pending.discard)

    loop_stop_mono = tick()
    while tick() < fixed_boundary:
        if pending:
            await asyncio.wait(
                pending,
                timeout=min(0.05, max(0.0, fixed_boundary - tick())),
            )
        else:
            remaining = fixed_boundary - tick()
            if remaining <= 0:
                break
            await asyncio.sleep(min(0.05, remaining))

    boundary_reached_mono = tick()
    drain_start_mono = boundary_reached_mono
    accounting.window = SampleWindow(
        submission_start_mono=window_start,
        submission_end_mono=fixed_boundary,
        measured_submission_end_mono=boundary_reached_mono,
        submission_loop_stop_mono=loop_stop_mono,
        lifecycle_end_mono=lifecycle_end,
        drain_start_mono=drain_start_mono,
    )

    while pending and tick() < lifecycle_end:
        await asyncio.wait(
            pending,
            timeout=min(0.1, max(0.0, lifecycle_end - tick())),
        )

    for task in list(pending):
        if not task.done():
            task.cancel()
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)

    drain_end_mono = tick()
    accounting.window = SampleWindow(
        submission_start_mono=window_start,
        submission_end_mono=fixed_boundary,
        measured_submission_end_mono=boundary_reached_mono,
        submission_loop_stop_mono=loop_stop_mono,
        lifecycle_end_mono=lifecycle_end,
        drain_start_mono=drain_start_mono,
        drain_end_mono=drain_end_mono,
    )

    return accounting, sequence
