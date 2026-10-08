from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import timedelta

from superjobs.discovery.errors import DiscoveryConfigurationError

DEFAULT_RENEWAL_INTERVAL = timedelta(seconds=10)
DEFAULT_LEASE_TIMEOUT = timedelta(seconds=30)
DEFAULT_STALE_RETENTION = timedelta(hours=24)
DEFAULT_MAX_ENVELOPE_BYTES = 32 * 1024
DEFAULT_READ_TIMEOUT = timedelta(seconds=30)


def _validate_positive_finite_timedelta(value: timedelta, *, field: str) -> None:
    if not isinstance(value, timedelta):
        raise DiscoveryConfigurationError(f"{field} must be a timedelta")
    seconds = value.total_seconds()
    if not math.isfinite(seconds) or seconds <= 0:
        raise DiscoveryConfigurationError(f"{field} must be a positive finite duration")


def _validate_positive_int(value: int, *, field: str) -> None:
    if type(value) is not int or isinstance(value, bool):
        raise DiscoveryConfigurationError(f"{field} must be a positive integer")
    if value < 1:
        raise DiscoveryConfigurationError(f"{field} must be positive")


@dataclass(frozen=True, slots=True)
class PresenceConfig:
    renewal_interval: timedelta = DEFAULT_RENEWAL_INTERVAL
    lease_timeout: timedelta = DEFAULT_LEASE_TIMEOUT
    stale_retention: timedelta = DEFAULT_STALE_RETENTION
    max_envelope_bytes: int = DEFAULT_MAX_ENVELOPE_BYTES
    read_timeout: timedelta = DEFAULT_READ_TIMEOUT

    def __post_init__(self) -> None:
        _validate_positive_finite_timedelta(self.renewal_interval, field="renewal_interval")
        _validate_positive_finite_timedelta(self.lease_timeout, field="lease_timeout")
        _validate_positive_finite_timedelta(self.stale_retention, field="stale_retention")
        _validate_positive_finite_timedelta(self.read_timeout, field="read_timeout")
        if self.lease_timeout < self.renewal_interval * 3:
            raise DiscoveryConfigurationError(
                "lease_timeout must be at least three renewal intervals",
            )
        _validate_positive_int(self.max_envelope_bytes, field="max_envelope_bytes")
