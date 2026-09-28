from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta


@dataclass(frozen=True, slots=True)
class ObservationRetention:
    max_age: timedelta | None = None
    max_events: int | None = None
    max_bytes: int | None = None

    def __post_init__(self) -> None:
        if self.max_age is not None and self.max_age <= timedelta(0):
            raise ValueError("observation max_age must be positive")
        if self.max_events is not None and self.max_events < 1:
            raise ValueError("observation max_events must be positive")
        if self.max_bytes is not None and self.max_bytes < 1:
            raise ValueError("observation max_bytes must be positive")


@dataclass(frozen=True, slots=True)
class ResultRetention:
    max_age: timedelta | None = None
    max_bytes: int | None = None

    def __post_init__(self) -> None:
        if self.max_age is not None and self.max_age <= timedelta(0):
            raise ValueError("result max_age must be positive")
        if self.max_bytes is not None and self.max_bytes < 1:
            raise ValueError("result max_bytes must be positive")
