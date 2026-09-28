from dataclasses import dataclass
from datetime import UTC, datetime

from superjobs.jobs.events import (
    JobCompleted,
    JobEvent,
    JobFailed,
    JobLog,
    JobProgress,
    JobStarted,
)
from superjobs.jobs.execution import JobError


@dataclass(frozen=True)
class ApplicationEvent:
    value: int


def test_job_event_wraps_typed_data_and_execution_metadata() -> None:
    timestamp = datetime.now(UTC)
    event = JobEvent(
        job_id="job-1",
        sequence=3,
        timestamp=timestamp,
        attempt=2,
        data=ApplicationEvent(value=7),
    )

    assert event.job_id == "job-1"
    assert event.sequence == 3
    assert event.timestamp == timestamp
    assert event.attempt == 2
    assert event.data == ApplicationEvent(value=7)


def test_system_events_support_keyword_pattern_matching() -> None:
    progress = JobProgress(completed=4, total=10)
    current_progress = JobProgress(current=4, total=10)
    log = JobLog(message="working", level="info", extra={"part": 2})
    failure = JobFailed(
        error=JobError(
            code="failed",
            type_name="RuntimeError",
            message="boom",
        ),
    )

    match progress:
        case JobProgress(completed=completed, total=total):
            assert (completed, total) == (4, 10)
        case _:
            raise AssertionError("progress did not match")

    match progress:
        case JobProgress(current=current, total=total):
            assert (current, total) == (4, 10)
        case _:
            raise AssertionError("progress compatibility alias did not match")

    assert current_progress == progress

    match log:
        case JobLog(message=message, level=level, extra=extra):
            assert (message, level, extra) == ("working", "info", {"part": 2})
        case _:
            raise AssertionError("log did not match")

    match failure:
        case JobFailed(error=error):
            assert error.code == "failed"
        case _:
            raise AssertionError("failure did not match")


def test_terminal_system_events_are_distinct_types() -> None:
    assert type(JobStarted()) is not type(JobCompleted())
    assert type(JobCompleted()) is not type(JobFailed(error=JobError(code="x", message="y")))
