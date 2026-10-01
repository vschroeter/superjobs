from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, kw_only=True, slots=True)
class SubmitOptions:
    """Execution options for a single job submission."""

    idempotency_key: str | None = None
    job_id: str | None = None
    timeout: float | None = None
    deadline: datetime | None = None
    caller_scope: str = "default"
