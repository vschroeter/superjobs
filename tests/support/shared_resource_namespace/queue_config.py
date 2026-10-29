from __future__ import annotations

from superjobs.transport.implementations.nats import NatsQueueConfig

from tests.support.shared_resource_namespace.constants import (
    RESOURCE_KEY,
    SHARED_RESOURCE_NAMESPACE,
)


def queue_config_for_work_prefix(
    work_prefix: str,
    *,
    resource_namespace: str | None = SHARED_RESOURCE_NAMESPACE,
    ack_wait: float = 3.0,
) -> NatsQueueConfig:
    return NatsQueueConfig(
        stream=f"{work_prefix}_stream",
        observation_stream=f"{work_prefix}_observations",
        subject_prefix=work_prefix,
        completion_bucket=f"{work_prefix}_completions",
        idempotency_bucket=f"{work_prefix}_idempotency",
        ack_wait=ack_wait,
        resource_namespace=resource_namespace,
    )


def control_subject(queue_config: NatsQueueConfig) -> str:
    return f"{queue_config.subject_prefix}.control.hold"
