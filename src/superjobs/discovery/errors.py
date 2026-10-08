from __future__ import annotations

from typing import Any, Self


class DiscoveryError(Exception):
    """Base class for worker discovery failures."""


class UnsupportedDiscoveryBackendError(DiscoveryError):
    """Raised when the configured transport does not expose discovery reads."""


class DiscoveryUnavailableError(DiscoveryError):
    """Raised when the registry cannot be read."""


class DiscoveryEnvelopeError(DiscoveryError):
    """Raised when a stored registration envelope is invalid or unsupported."""

    def __init__(
        self,
        message: str,
        *,
        worker_id: str | None = None,
        job: Any | None = None,
    ) -> None:
        self._base_message = message
        self.worker_id = worker_id
        self.job = job
        super().__init__(self._formatted_message())

    def _context_suffix(self) -> str:
        suffix = ""
        if self.worker_id is not None:
            suffix += f" (worker {self.worker_id!r})"
        if self.job is not None:
            suffix += f" on {self.job}"
        return suffix

    def _formatted_message(self) -> str:
        return self._base_message + self._context_suffix()

    def add_context(
        self,
        *,
        worker_id: str | None = None,
        job: Any | None = None,
    ) -> Self:
        resolved_worker = self.worker_id if self.worker_id is not None else worker_id
        resolved_job = self.job if self.job is not None else job
        if resolved_worker == self.worker_id and resolved_job == self.job:
            return self
        self.worker_id = resolved_worker
        self.job = resolved_job
        self.args = (self._formatted_message(),)
        return self


class DiscoveryEnvelopeVersionError(DiscoveryEnvelopeError):
    def __init__(
        self,
        version: Any,
        *,
        worker_id: str | None = None,
        job: Any | None = None,
    ) -> None:
        self.version = version
        super().__init__(
            f"Unsupported worker registration envelope version {version!r}",
            worker_id=worker_id,
            job=job,
        )


class DiscoveryEnvelopeSizeError(DiscoveryEnvelopeError):
    def __init__(
        self,
        size: int,
        *,
        limit: int,
        worker_id: str | None = None,
        job: Any | None = None,
    ) -> None:
        self.size = size
        self.limit = limit
        super().__init__(
            f"Worker registration envelope size {size} exceeds limit {limit}",
            worker_id=worker_id,
            job=job,
        )


class CapabilityDecodeError(DiscoveryError):
    def __init__(
        self,
        message: str,
        *,
        worker_id: str,
        job: Any,
        cause: BaseException | None = None,
    ) -> None:
        self.worker_id = worker_id
        self.job = job
        if cause is not None:
            super().__init__(message)
            self.__cause__ = cause
        else:
            super().__init__(message)


class DiscoveryWriteError(DiscoveryError):
    """Raised when a registration write is rejected."""

    def __init__(
        self,
        message: str,
        *,
        worker_id: str | None = None,
        job: Any | None = None,
    ) -> None:
        self._base_message = message
        self.worker_id = worker_id
        self.job = job
        super().__init__(self._formatted_message())

    def _context_suffix(self) -> str:
        suffix = ""
        if self.worker_id is not None and f"(worker {self.worker_id!r})" not in self._base_message:
            suffix += f" (worker {self.worker_id!r})"
        if self.job is not None and f" on {self.job}" not in self._base_message:
            suffix += f" on {self.job}"
        return suffix

    def _formatted_message(self) -> str:
        return self._base_message + self._context_suffix()

    def add_context(
        self,
        *,
        worker_id: str | None = None,
        job: Any | None = None,
    ) -> Self:
        resolved_worker = self.worker_id if self.worker_id is not None else worker_id
        resolved_job = self.job if self.job is not None else job
        if resolved_worker == self.worker_id and resolved_job == self.job:
            return self
        self.worker_id = resolved_worker
        self.job = resolved_job
        self.args = (self._formatted_message(),)
        return self


class DiscoveryConfigurationError(DiscoveryError):
    """Raised when discovery timing or retention settings are invalid."""
