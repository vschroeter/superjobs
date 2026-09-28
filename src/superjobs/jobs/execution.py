from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Generic, TypeVar


class JobState(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass(frozen=True, slots=True)
class ProgressSnapshot:
    completed: int
    total: int | None

    @property
    def current(self) -> int:
        """Compatibility alias for callers that use current/total."""
        return self.completed


@dataclass(frozen=True, slots=True)
class ObservationCursor:
    sequence: int = 0

    def __post_init__(self) -> None:
        if self.sequence < 0:
            raise ValueError("observation sequence must be non-negative")


@dataclass(frozen=True, slots=True)
class JobError:
    code: str
    message: str
    type_name: str | None = None
    details: dict[str, Any] | None = None
    traceback: str | None = None

    def to_wire(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "type_name": self.type_name,
            "details": self.details,
            "traceback": self.traceback,
        }

    @classmethod
    def from_wire(cls, value: dict[str, Any]) -> JobError:
        return cls(
            code=str(value.get("code", "failed")),
            message=str(value.get("message", "")),
            type_name=value.get("type_name"),
            details=value.get("details"),
            traceback=value.get("traceback"),
        )

    @classmethod
    def from_exception(
        cls,
        exception: BaseException,
        *,
        code: str = "failed",
        include_traceback: bool = False,
    ) -> JobError:
        import traceback as traceback_module

        return cls(
            code=code,
            message=str(exception) or exception.__class__.__name__,
            type_name=f"{exception.__class__.__module__}.{exception.__class__.__qualname__}",
            traceback=(
                "".join(traceback_module.format_exception(exception))
                if include_traceback
                else None
            ),
        )


@dataclass(frozen=True, slots=True)
class JobStatus:
    state: JobState
    progress: ProgressSnapshot | None = None
    attempt: int = 0
    created_at: datetime | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    cancellation_requested: bool = False
    next_attempt_at: datetime | None = None
    error: JobError | None = None

    @classmethod
    def pending(cls, *, created_at: datetime | None = None) -> JobStatus:
        return cls(
            state=JobState.PENDING,
            attempt=0,
            created_at=created_at or datetime.now(UTC),
        )


FinalT = TypeVar("FinalT")


@dataclass(frozen=True, slots=True)
class CompletionRecord(Generic[FinalT]):
    job_id: str
    state: JobState
    completed_at: datetime
    result: FinalT | None = None
    error: JobError | None = None

@dataclass(frozen=True, slots=True)
class JobSucceeded(Generic[FinalT]):
    result: FinalT


@dataclass(frozen=True, slots=True)
class JobFailedOutcome:
    error: JobError


@dataclass(frozen=True, slots=True)
class JobCancelledOutcome:
    reason: str | None = None


JobOutcome = JobSucceeded[FinalT] | JobFailedOutcome | JobCancelledOutcome

# Short aliases are useful when matching an outcome in application code.
Succeeded = JobSucceeded
Failed = JobFailedOutcome
Cancelled = JobCancelledOutcome
