from __future__ import annotations

from datetime import datetime

from superjobs.discovery.envelope import StoredWorkerRegistration
from superjobs.discovery.models import WorkerRegistrationState


def is_actively_offered(
    stored: StoredWorkerRegistration | None,
    *,
    evaluated_at: datetime,
) -> bool:
    """Whether a stored registration counts as actively offered at one instant."""
    if stored is None:
        return False
    if stored.state != WorkerRegistrationState.READY:
        return False
    return stored.expires_at > evaluated_at
