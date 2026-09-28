from __future__ import annotations

import random
from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol

from superjobs.exceptions.jobs import JobCancelledError, NonRetryableError


class Backoff(Protocol):
    def delay(self, attempt: int) -> float: ...


@dataclass(frozen=True, slots=True)
class ExponentialBackoff:
    initial: float = 1.0
    multiplier: float = 2.0
    maximum: float | None = 300.0
    jitter: float = 0.0

    def __post_init__(self) -> None:
        if self.initial < 0:
            raise ValueError("initial backoff must be non-negative")
        if self.multiplier < 1:
            raise ValueError("backoff multiplier must be at least one")
        if self.maximum is not None and self.maximum < 0:
            raise ValueError("maximum backoff must be non-negative")
        if self.jitter < 0 or self.jitter > 1:
            raise ValueError("jitter must be between zero and one")

    def delay(self, attempt: int) -> float:
        if attempt < 1:
            raise ValueError("attempt must be at least one")
        delay = self.initial * self.multiplier ** (attempt - 1)
        if self.maximum is not None:
            delay = min(delay, self.maximum)
        if self.jitter:
            delay *= random.uniform(1 - self.jitter, 1 + self.jitter)
            if self.maximum is not None:
                delay = min(delay, self.maximum)
        return delay


@dataclass(frozen=True, slots=True)
class FixedBackoff:
    value: float = 1.0

    def __post_init__(self) -> None:
        if self.value < 0:
            raise ValueError("backoff must be non-negative")

    def delay(self, attempt: int) -> float:
        if attempt < 1:
            raise ValueError("attempt must be at least one")
        return self.value


ExceptionClassifier = Callable[[BaseException], bool]


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    max_attempts: int = 1
    backoff: Backoff = ExponentialBackoff()
    retry_for: tuple[type[BaseException], ...] = (Exception,)
    classifier: ExceptionClassifier | None = None

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least one")

    def should_retry(self, *, attempt: int, exception: BaseException) -> bool:
        if attempt >= self.max_attempts:
            return False
        if isinstance(exception, (JobCancelledError, NonRetryableError)):
            return False
        if self.classifier is not None:
            return self.classifier(exception)
        return isinstance(exception, self.retry_for)

    def delay(self, *, attempt: int) -> float:
        return self.backoff.delay(attempt)
