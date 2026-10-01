"""Shared NatsBroker settings for idle outage verification."""

from __future__ import annotations

from faststream.nats import NatsBroker

CONNECT_TIMEOUT_SECONDS = 5
MAX_RECONNECT_ATTEMPTS = 12
RECONNECT_TIME_WAIT_SECONDS = 1


def nats_broker(
    nats_url: str,
    *,
    disconnected_cb: object | None = None,
    reconnected_cb: object | None = None,
) -> NatsBroker:
    return NatsBroker(
        nats_url,
        connect_timeout=CONNECT_TIMEOUT_SECONDS,
        max_reconnect_attempts=MAX_RECONNECT_ATTEMPTS,
        reconnect_time_wait=RECONNECT_TIME_WAIT_SECONDS,
        disconnected_cb=disconnected_cb,
        reconnected_cb=reconnected_cb,
    )


def reconnect_settings_record() -> dict[str, int]:
    return {
        "connect_timeout_seconds": CONNECT_TIMEOUT_SECONDS,
        "max_reconnect_attempts": MAX_RECONNECT_ATTEMPTS,
        "reconnect_time_wait_seconds": RECONNECT_TIME_WAIT_SECONDS,
    }
