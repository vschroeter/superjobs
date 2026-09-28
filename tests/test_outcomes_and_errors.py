from superjobs.exceptions.jobs import JobFailedError
from superjobs.jobs.execution import (
    JobError,
    JobFailedOutcome,
    JobSucceeded,
)


def test_job_error_is_serializable_without_rehydrating_remote_exception() -> None:
    error = JobError(
        code="timed_out",
        type_name="TimeoutError",
        message="attempt exceeded its limit",
        details={"attempt": 2},
    )

    restored = JobError.from_wire(error.to_wire())

    assert restored == error
    assert isinstance(restored, JobError)


def test_outcome_variants_are_distinct_from_terminal_events() -> None:
    error = JobError(code="failed", message="boom")
    succeeded = JobSucceeded(result=42)
    failed = JobFailedOutcome(error=error)

    assert succeeded.result == 42
    assert failed.error == error
    assert type(succeeded) is not type(failed)


def test_job_failed_error_wraps_stable_job_error() -> None:
    error = JobError(code="failed", message="boom")
    exception = JobFailedError(error)

    assert exception.error == error
    assert str(exception) == "boom"
