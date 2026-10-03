"""High-resolution monotonic clock for producer performance samples (not harness protocol I/O)."""

from __future__ import annotations

import time
from typing import Any

MEASUREMENT_CLOCK_FUNCTION = "perf_counter"


def now() -> float:
    return time.perf_counter()


def metadata() -> dict[str, Any]:
    info = time.get_clock_info(MEASUREMENT_CLOCK_FUNCTION)
    return {
        "function": MEASUREMENT_CLOCK_FUNCTION,
        "implementation": info.implementation,
        "resolution": info.resolution,
        "monotonic": info.monotonic,
        "adjustable": info.adjustable,
    }
