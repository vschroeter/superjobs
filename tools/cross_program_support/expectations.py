"""Expected-exception helpers for cross-program producer scenarios."""

from __future__ import annotations

from superjobs import JobCancelledError, JobFailedError


class _ExpectJobFailed:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            raise AssertionError("expected JobFailedError but no exception was raised")
        if exc_type is JobFailedError:
            return True
        return False


class _ExpectJobCancelled:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            raise AssertionError("expected JobCancelledError but no exception was raised")
        if exc_type is JobCancelledError:
            return True
        return False


def expect_job_failed():
    return _ExpectJobFailed()


def expect_job_cancelled():
    return _ExpectJobCancelled()
