from __future__ import annotations

import base64
import hashlib
import re

from superjobs.resources.errors import (
    ResourceConfigurationError,
    ResourceValidationError,
)

_BUCKET_SUFFIX = "-resource-admission"
_VALID_BUCKET_RE = re.compile(r"^[a-zA-Z0-9_-]+$")
_KV_SAFE_KEY_RE = re.compile(r"^[-/_=\.a-zA-Z0-9]+$")
_ADMISSION_NAMESPACE_TOKEN_RE = re.compile(r"^[A-Za-z0-9_-]+$")

_CAPACITY_PREFIX = "cap.v1"
_RESOURCE_PREFIX = "res.v1"
_EXECUTION_PREFIX = "exe.v1"
_REVOKED_PREFIX = "rev.v1"


def _b64url_encode(text: str) -> str:
    return base64.urlsafe_b64encode(text.encode("utf-8")).rstrip(b"=").decode("ascii")


def validate_resource_key(key: str) -> str:
    if not isinstance(key, str) or not key:
        raise ResourceValidationError("resource key must be a non-empty string")
    if key[0] == "." or key[-1] == ".":
        raise ResourceValidationError("resource key must not start or end with '.'")
    if _KV_SAFE_KEY_RE.fullmatch(key) is None:
        raise ResourceValidationError(
            "resource key contains characters that are not KV-safe"
        )
    return key


def validate_resource_admission_namespace(namespace: str) -> str:
    """Validate an explicit resource-admission namespace token (fail-closed, no normalization)."""
    if not isinstance(namespace, str):
        raise ResourceConfigurationError(
            "resource admission namespace must be a string",
        )
    if not namespace:
        raise ResourceConfigurationError(
            "resource admission namespace must not be empty",
        )
    if namespace != namespace.strip():
        raise ResourceConfigurationError(
            "resource admission namespace must not have leading or trailing whitespace",
        )
    if any(ch.isspace() for ch in namespace):
        raise ResourceConfigurationError(
            "resource admission namespace must not contain whitespace",
        )
    if any(ord(ch) < 32 or ord(ch) == 127 for ch in namespace):
        raise ResourceConfigurationError(
            "resource admission namespace must not contain control characters",
        )
    if namespace[0] == "." or namespace[-1] == ".":
        raise ResourceConfigurationError(
            "resource admission namespace must not start or end with '.'",
        )
    if ".." in namespace:
        raise ResourceConfigurationError(
            "resource admission namespace must not contain empty dot-separated segments",
        )
    if "*" in namespace or ">" in namespace:
        raise ResourceConfigurationError(
            "resource admission namespace must not contain NATS wildcard characters",
        )
    for segment in namespace.split("."):
        if _ADMISSION_NAMESPACE_TOKEN_RE.fullmatch(segment) is None:
            raise ResourceConfigurationError(
                "resource admission namespace segments may contain only "
                "ASCII letters, digits, '_', and '-'",
            )
    return namespace


def resolve_resource_admission_namespace(
    subject_prefix: str,
    resource_namespace: str | None,
) -> str:
    if resource_namespace is None:
        return subject_prefix
    return validate_resource_admission_namespace(resource_namespace)


def admission_bucket_name(subject_prefix: str) -> str:
    if not subject_prefix:
        raise ResourceConfigurationError("subject_prefix must be non-empty")
    candidate = f"{subject_prefix}{_BUCKET_SUFFIX}"
    if _VALID_BUCKET_RE.fullmatch(candidate):
        return candidate
    digest = hashlib.sha256(subject_prefix.encode("utf-8")).hexdigest()[:22]
    hashed = f"sj-{digest}-radm"
    if _VALID_BUCKET_RE.fullmatch(hashed):
        return hashed
    raise ResourceConfigurationError(
        f"Cannot derive a valid JetStream KV bucket name from subject prefix {subject_prefix!r}",
    )


def capacity_kv_key(resource_key: str) -> str:
    safe = validate_resource_key(resource_key)
    key = f"{_CAPACITY_PREFIX}.{_b64url_encode(safe)}"
    if _KV_SAFE_KEY_RE.fullmatch(key) is None:
        raise ResourceConfigurationError("capacity record key is not KV-safe")
    return key


def resource_kv_key(resource_key: str) -> str:
    safe = validate_resource_key(resource_key)
    key = f"{_RESOURCE_PREFIX}.{_b64url_encode(safe)}"
    if _KV_SAFE_KEY_RE.fullmatch(key) is None:
        raise ResourceConfigurationError("resource record key is not KV-safe")
    return key


def revoked_incarnation_kv_key(worker_id: str, incarnation_id: str) -> str:
    if not worker_id or not incarnation_id:
        raise ResourceValidationError(
            "revoked incarnation key requires worker_id and incarnation_id"
        )
    token = f"{worker_id}\x00{incarnation_id}"
    key = f"{_REVOKED_PREFIX}.{_b64url_encode(token)}"
    if _KV_SAFE_KEY_RE.fullmatch(key) is None:
        raise ResourceConfigurationError("revoked incarnation key is not KV-safe")
    return key


def execution_kv_key(execution_id: str) -> str:
    if not isinstance(execution_id, str) or not execution_id:
        raise ResourceValidationError("execution_id must be a non-empty string")
    key = f"{_EXECUTION_PREFIX}.{_b64url_encode(execution_id)}"
    if _KV_SAFE_KEY_RE.fullmatch(key) is None:
        raise ResourceConfigurationError("execution claim key is not KV-safe")
    return key
