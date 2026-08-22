from __future__ import annotations

import uuid
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Any

from superjobs.exceptions.jobs import JobCancelledError

if TYPE_CHECKING:
    from superjobs.jobs.job import Job


class JobStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class JobContext[ReqT: Any | None, FinalT: Any | None, InterT: Any | None]:
    def __init__(
        self,
        job: Job[ReqT, FinalT, InterT],
        message: Any | None = None,
    ):

        self.job = job
        self.message = message
        self.id: str = str(uuid.uuid4())

        self.created_at = datetime.now()

        self.deadline: datetime | None = None

        self.status: JobStatus = JobStatus.PENDING
        self.completed = 0
        self.total = 0

        self.cancelled = False

    def check_cancelled(self) -> None:
        if self.cancelled:
            raise JobCancelledError("Job cancelled")

    async def progress(self, completed: float, total: float) -> None:
        self.completed = completed
        self.total = total

    async def emit_event(self, event: InterT) -> None:
        # self.job.event_codec.encode(event)
        pass

    async def cancel(self) -> None:
        pass
