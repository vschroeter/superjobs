from __future__ import annotations

from typing import Any

from superjobs.discovery.errors import DiscoveryError


class WorkerPresenceError(DiscoveryError):
    """Base class for local worker presence lifecycle failures."""


class WorkerDiscoveryDisabledError(WorkerPresenceError):
    """Raised when registry access was explicitly disabled on the runtime."""


class WorkerPresenceUnavailableError(WorkerPresenceError):
    """Raised when presence operations require discovery that is not configured."""


class LocalWorkerHandleError(WorkerPresenceError):
    def __init__(
        self,
        message: str,
        *,
        job: Any | None = None,
    ) -> None:
        self.job = job
        suffix = f" for {job}" if job is not None else ""
        super().__init__(f"{message}{suffix}")
