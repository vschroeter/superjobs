from superjobs.exceptions.jobs import JobCancelledError, NonRetryableError
from superjobs.jobs.retry_policy import ExponentialBackoff, RetryPolicy


def test_retry_policy_counts_total_attempts() -> None:
    policy = RetryPolicy(max_attempts=3)

    assert policy.max_attempts == 3
    assert policy.should_retry(attempt=1, exception=RuntimeError("temporary"))
    assert policy.should_retry(attempt=2, exception=RuntimeError("temporary"))
    assert not policy.should_retry(attempt=3, exception=RuntimeError("temporary"))


def test_retry_policy_does_not_retry_cancellation_or_explicit_permanent_failures() -> None:
    policy = RetryPolicy(max_attempts=3)

    assert not policy.should_retry(attempt=1, exception=JobCancelledError())
    assert not policy.should_retry(attempt=1, exception=NonRetryableError("bad request"))


def test_exponential_backoff_is_capped() -> None:
    backoff = ExponentialBackoff(initial=1.0, multiplier=2.0, maximum=3.0)

    assert [backoff.delay(attempt) for attempt in range(1, 5)] == [1.0, 2.0, 3.0, 3.0]
