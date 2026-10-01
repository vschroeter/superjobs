"""Pyright-negative checks for strict payload typing (deliberate errors)."""

from __future__ import annotations

from superjobs import validate_payload
from superjobs.payload.adapter.implementations.dataclass import DataclassPayloadAdapter
from superjobs_contract_example import ManifestRequest


def _wrong_validate_instance() -> None:
    adapter = DataclassPayloadAdapter(ManifestRequest)
    validate_payload(adapter, "not-a-manifest-request")  # expect: reportArgumentType
