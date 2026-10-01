"""NATS queue settings for idle outage verification."""

from __future__ import annotations

from superjobs.transport.implementations.nats import NatsQueueConfig


def queue_config_for_run(run_id: str) -> NatsQueueConfig:
    prefix = f"io_{run_id[:16]}"
    return NatsQueueConfig(
        stream=prefix,
        observation_stream=f"{prefix}_observations",
        subject_prefix=prefix,
        completion_bucket=f"{prefix}_completions",
        idempotency_bucket=f"{prefix}_idempotency",
        ack_wait=3.0,
    )
