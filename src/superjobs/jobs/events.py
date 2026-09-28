from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Generic, TypeAlias, TypeVar

from superjobs.jobs.execution import JobError


@dataclass(frozen=True, slots=True)
class JobStarted:
    pass


@dataclass(frozen=True, slots=True)
class JobCompleted:
    pass


@dataclass(frozen=True, slots=True)
class JobFailed:
    error: JobError


@dataclass(frozen=True, slots=True)
class JobCancelled:
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class JobRetryScheduled:
    attempt: int
    delay: float | None = None


@dataclass(frozen=True, slots=True, init=False)
class JobProgress:
    completed: int
    total: int | None

    def __init__(
        self,
        completed: int | None = None,
        total: int | None = None,
        *,
        current: int | None = None,
    ) -> None:
        if completed is None:
            if current is None:
                raise TypeError("either completed or current must be provided")
            completed = current
        elif current is not None and completed != current:
            raise ValueError("completed and current must have the same value")
        object.__setattr__(self, "completed", completed)
        object.__setattr__(self, "total", total)

    @property
    def current(self) -> int:
        return self.completed


@dataclass(frozen=True, slots=True)
class JobLog:
    message: str
    level: str = "info"
    extra: dict[str, Any] = field(default_factory=dict)


SystemEvent: TypeAlias = (
    JobStarted
    | JobCompleted
    | JobFailed
    | JobCancelled
    | JobRetryScheduled
    | JobProgress
    | JobLog
)

EventT = TypeVar("EventT")


@dataclass(frozen=True, slots=True)
class JobEvent(Generic[EventT]):
    job_id: str
    sequence: int
    timestamp: datetime
    attempt: int
    data: SystemEvent | EventT
