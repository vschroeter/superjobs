from __future__ import annotations

import uuid

import pytest

from superjobs import NatsQueueConfig
from superjobs.resources.errors import ResourceConfigurationError
from superjobs.resources.keys import (
    admission_bucket_name,
    resolve_resource_admission_namespace,
    validate_resource_admission_namespace,
)
from tests.support.shared_resource_namespace.constants import SHARED_RESOURCE_NAMESPACE
from tests.support.shared_resource_namespace.queue_config import queue_config_for_work_prefix


def test_default_resource_namespace_maps_to_subject_prefix() -> None:
    config = NatsQueueConfig()
    assert config.resource_namespace is None
    assert config.resolved_resource_admission_namespace() == config.subject_prefix
    assert admission_bucket_name(config.subject_prefix) == admission_bucket_name(
        config.resolved_resource_admission_namespace(),
    )


def test_two_work_prefixes_share_explicit_resource_namespace_bucket() -> None:
    run = uuid.uuid4().hex[:12]
    config_a = queue_config_for_work_prefix(f"work_a_{run}")
    config_b = queue_config_for_work_prefix(f"work_b_{run}")
    assert config_a.subject_prefix != config_b.subject_prefix
    assert config_a.stream != config_b.stream
    assert config_a.completion_bucket != config_b.completion_bucket
    assert config_a.idempotency_bucket != config_b.idempotency_bucket
    assert config_a.observation_stream != config_b.observation_stream
    bucket_a = admission_bucket_name(config_a.resolved_resource_admission_namespace())
    bucket_b = admission_bucket_name(config_b.resolved_resource_admission_namespace())
    assert bucket_a == bucket_b
    assert bucket_a != admission_bucket_name(config_a.subject_prefix)


def test_dotted_resource_namespace_uses_hashed_bucket_without_alias() -> None:
    dotted = NatsQueueConfig(
        subject_prefix="work_queue",
        resource_namespace="physical.host",
    )
    dashed = NatsQueueConfig(
        subject_prefix="work_queue",
        resource_namespace="physical-host",
    )
    dotted_bucket = admission_bucket_name(dotted.resolved_resource_admission_namespace())
    dashed_bucket = admission_bucket_name(dashed.resolved_resource_admission_namespace())
    assert dotted_bucket.startswith("sj-")
    assert dashed_bucket == "physical-host-resource-admission"
    assert dotted_bucket != dashed_bucket


@pytest.mark.parametrize(
    ("value", "message"),
    [
        ("", "empty"),
        ("   ", "leading or trailing whitespace"),
        ("host a", "whitespace"),
        (".host", "start or end with '.'"),
        ("host.", "start or end with '.'"),
        ("a..b", "empty dot-separated"),
        ("a*b", "wildcard"),
        ("a>b", "wildcard"),
        ("bad!", "segments may contain only"),
    ],
)
def test_invalid_resource_namespace_rejected(value: str, message: str) -> None:
    with pytest.raises(ResourceConfigurationError, match=message):
        validate_resource_admission_namespace(value)
    with pytest.raises(ValueError, match=message):
        NatsQueueConfig(resource_namespace=value)


def test_non_string_resource_namespace_rejected() -> None:
    with pytest.raises(ResourceConfigurationError, match="string"):
        validate_resource_admission_namespace(42)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "invalid_namespace",
    [
        True,
        1,
        b"bytes",  # type: ignore[list-item]
        ["list"],  # type: ignore[list-item]
    ],
)
def test_nats_queue_config_rejects_non_string_resource_namespace(
    invalid_namespace: object,
) -> None:
    with pytest.raises((TypeError, ValueError)):
        NatsQueueConfig(resource_namespace=invalid_namespace)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("value", "message"),
    [
        ("caf\u00e9", "segments may contain only"),
    ],
)
def test_additional_invalid_resource_namespace_tokens(value: str, message: str) -> None:
    with pytest.raises(ResourceConfigurationError, match=message):
        validate_resource_admission_namespace(value)
    with pytest.raises(ValueError, match=message):
        NatsQueueConfig(resource_namespace=value)


def test_control_character_resource_namespace_rejected() -> None:
    with pytest.raises(ResourceConfigurationError, match="control characters"):
        validate_resource_admission_namespace("host\u0001name")
    with pytest.raises(ValueError, match="control characters"):
        NatsQueueConfig(resource_namespace="host\u0001name")


def test_resolve_resource_admission_namespace_preserves_legacy() -> None:
    assert resolve_resource_admission_namespace("legacy_prefix", None) == "legacy_prefix"
    assert (
        resolve_resource_admission_namespace("legacy_prefix", SHARED_RESOURCE_NAMESPACE)
        == SHARED_RESOURCE_NAMESPACE
    )


def test_nats_queue_config_positional_compatibility() -> None:
    config = NatsQueueConfig(
        "stream",
        "observations",
        "prefix",
        "completions",
        "idempotency",
    )
    assert config.resource_namespace is None
    assert config.resolved_resource_admission_namespace() == "prefix"
