from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TypeVar

CapT = TypeVar("CapT")

CapabilityInput = CapT | None | Callable[[], CapT | None | Awaitable[CapT | None]]
