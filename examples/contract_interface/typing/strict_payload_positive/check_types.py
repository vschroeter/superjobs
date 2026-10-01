"""Pyright-positive checks for strict payload construction and validation exports."""

from __future__ import annotations

from typing import assert_type

from superjobs import construct_payload_for_type, validate_payload
from superjobs.registry import registry
from superjobs_contract_example import ManifestRequest, ManifestResult


def _construct_request_from_fields() -> None:
    request = construct_payload_for_type(ManifestRequest, device_id="sensor-9")
    assert_type(request, ManifestRequest)


def _validate_explicit_request() -> None:
    codec = registry.get_payload_codec(ManifestRequest)
    request = ManifestRequest(device_id="sensor-9")
    validated = validate_payload(codec.adapter, request)
    assert_type(validated, ManifestRequest)
    prepared = codec.prepare(request)
    assert_type(prepared, ManifestRequest)


def _construct_result_from_fields() -> None:
    result = construct_payload_for_type(ManifestResult, revision="rev-1")
    assert_type(result, ManifestResult)
