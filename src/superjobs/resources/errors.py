from __future__ import annotations


class ResourceAdmissionError(Exception):
    """Base error for resource admission operations."""


class ResourceValidationError(ResourceAdmissionError, ValueError):
    """Admission inputs or declarations failed strict validation."""


class ResourceConfigurationError(ResourceAdmissionError, ValueError):
    """Store or bucket configuration is invalid or incompatible."""


class ResourceConflictError(ResourceAdmissionError):
    """A conflicting capacity declaration or occupied resource blocked the operation."""


class ResourceStaleOperationError(ResourceAdmissionError):
    """The operation used a superseded fencing token or incarnation."""


class ResourceStoreUnavailableError(ResourceAdmissionError):
    """The admission store could not complete a durable operation."""


class ResourceContentionError(ResourceAdmissionError):
    """Another owner holds the execution claim or exclusive resource."""


class ResourcePartialAcquisitionError(ResourceAdmissionError):
    """Durable admission state is partially applied and needs reconciliation."""

    def __init__(self, message: str, *, owner: object | None = None) -> None:
        super().__init__(message)
        self.owner = owner
