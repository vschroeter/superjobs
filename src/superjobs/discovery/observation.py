from __future__ import annotations

from typing import Any


def supports_own_registration_observation(discovery: Any) -> bool:
    """Return whether the backend opts into event-driven own-registration observation."""
    if getattr(discovery, "own_registration_observation", None) is False:
        return False
    if not callable(getattr(discovery, "watch_registration", None)):
        return False
    return callable(getattr(discovery, "read_registration", None))
