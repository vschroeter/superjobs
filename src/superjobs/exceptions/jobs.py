from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from superjobs.jobs.execution import JobError


class JobCancelledError(Exception):
    def __init__(self, reason: str | None = None):
        self.reason = reason
        super().__init__(reason or "Job cancelled")


class JobFailedError(Exception):
    def __init__(self, error: JobError):
        self.error = error
        super().__init__(error.message)


class NonRetryableError(Exception):
    """Marks a handler failure as permanent."""


class ResultTooLargeError(NonRetryableError):
    code = "result_too_large"


class InvalidResultError(NonRetryableError):
    code = "invalid_result"


class JobNotFoundError(LookupError):
    pass


class JobExpiredError(LookupError):
    pass


class ObservationExpiredError(JobExpiredError):
    pass


class ResultExpiredError(JobExpiredError):
    pass


class IdempotencyConflictError(ValueError):
    pass
