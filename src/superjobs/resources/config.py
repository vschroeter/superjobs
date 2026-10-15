from __future__ import annotations

from dataclasses import dataclass
import math

DEFAULT_MAX_RECORD_BYTES = 8192


@dataclass(frozen=True, slots=True)
class ResourceAdmissionConfig:
    """Configuration for durable resource admission records."""

    max_record_bytes: int = DEFAULT_MAX_RECORD_BYTES
    heartbeat_timeout: float = 15.0

    def __post_init__(self) -> None:
        if type(self.max_record_bytes) is not int or isinstance(
            self.max_record_bytes, bool
        ):
            raise ValueError("max_record_bytes must be a positive integer")
        if self.max_record_bytes < 1:
            raise ValueError("max_record_bytes must be positive")
        if (
            isinstance(self.heartbeat_timeout, bool)
            or not math.isfinite(self.heartbeat_timeout)
            or self.heartbeat_timeout <= 0
        ):
            raise ValueError("heartbeat_timeout must be finite and positive")
