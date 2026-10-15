"""NATS queue settings for resource admission verification."""

from __future__ import annotations

from superjobs.transport.implementations.nats import NatsQueueConfig


def queue_config_for_run(run_id: str, *, ack_wait: float = 3.0) -> NatsQueueConfig:
    prefix = f"ra_{run_id[:16]}"
    return NatsQueueConfig(
        stream=prefix,
        observation_stream=f"{prefix}_observations",
        subject_prefix=prefix,
        completion_bucket=f"{prefix}_completions",
        idempotency_bucket=f"{prefix}_idempotency",
        ack_wait=ack_wait,
    )


def control_subject(queue_config: NatsQueueConfig, run_id: str) -> str:
    return f"{queue_config.subject_prefix}.control.{run_id}"
