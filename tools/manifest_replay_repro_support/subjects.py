"""Mirror NatsJobBackend observation routing for harness-side probes."""

from __future__ import annotations

import hashlib

from superjobs.transport.implementations.nats import NatsQueueConfig

MANIFEST_JOB_CANONICAL_NAME = "performance.baseline.manifest:v1"

_DEFAULT_CONFIG = NatsQueueConfig()


def execution_kv_key(job_id: str) -> str:
    return hashlib.sha256(job_id.encode("utf-8")).hexdigest()


def observation_subject(
    job_id: str,
    *,
    job_canonical_name: str = MANIFEST_JOB_CANONICAL_NAME,
    queue_config: NatsQueueConfig | None = None,
) -> str:
    config = queue_config or _DEFAULT_CONFIG
    execution_token = hashlib.sha256(job_id.encode("utf-8")).hexdigest()
    return (
        f"{config.subject_prefix}.obs.{job_canonical_name}.{execution_token}"
    )


def sequence_kv_bucket(queue_config: NatsQueueConfig | None = None) -> str:
    config = queue_config or _DEFAULT_CONFIG
    return f"{config.subject_prefix}-sequences"
