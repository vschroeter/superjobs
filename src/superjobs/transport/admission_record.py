"""Admission owner persistence on execution completion records."""

from __future__ import annotations

from typing import Any
import json

from superjobs.resources.models import AdmissionOwner


def admission_owner_to_wire(owner: AdmissionOwner) -> dict[str, Any]:
    return owner.model_dump(mode="json")


def admission_owner_from_wire(payload: dict[str, Any] | None) -> AdmissionOwner | None:
    if payload is None:
        return None
    return AdmissionOwner.model_validate_json(json.dumps(payload))


def admission_owners_match(
    left: AdmissionOwner | None,
    right: AdmissionOwner | None,
) -> bool:
    if left is None or right is None:
        return left is right
    return left == right
