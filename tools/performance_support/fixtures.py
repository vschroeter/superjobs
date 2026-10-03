"""Sized request fixtures with measured msgpack wire bytes."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from superjobs.payload.adapter.implementations.dataclass import DataclassAdapterFactory
from superjobs.payload.codec.implementations.msgpack import MsgpackCodec
from superjobs.payload.codec.payloadcodec import PayloadCodec

try:
    from config import MANIFEST_TARGET_BYTES, TELEMETRY_TARGET_BYTES
    from contracts import PerformanceManifestRequest, PerformanceTelemetryRequest
except ModuleNotFoundError:
    from tools.performance_support.config import MANIFEST_TARGET_BYTES, TELEMETRY_TARGET_BYTES
    from tools.performance_support.contracts import (
        PerformanceManifestRequest,
        PerformanceTelemetryRequest,
    )


@dataclass(frozen=True, slots=True)
class FixtureSizes:
    telemetry_request_bytes: int
    manifest_request_bytes: int


def _codec_for[T](type_: type[T]) -> PayloadCodec[T]:
    adapter = DataclassAdapterFactory().create(type_)
    return PayloadCodec(adapter, MsgpackCodec())


def _wire_bytes(codec: PayloadCodec, value: object) -> int:
    return len(codec.encode(value))


def _resolve_padding_len(
    factory: Callable[[int], object],
    codec: PayloadCodec,
    target: int,
) -> int:
    low = 0
    high = max(target * 2, 256)
    while _wire_bytes(codec, factory(high)) < target:
        high *= 2
    best = 0
    while low <= high:
        mid = (low + high) // 2
        size = _wire_bytes(codec, factory(mid))
        if size <= target:
            best = mid
            low = mid + 1
        else:
            high = mid - 1
    return best


_TELEMETRY_CODEC = _codec_for(PerformanceTelemetryRequest)
_MANIFEST_CODEC = _codec_for(PerformanceManifestRequest)
_TELEMETRY_PADDING = _resolve_padding_len(
    lambda n: PerformanceTelemetryRequest(
        sequence=0,
        metric="pressure",
        value=1.0,
        tags={"source": "performance-baseline", "unit": "kpa"},
        padding="x" * n,
    ),
    _TELEMETRY_CODEC,
    TELEMETRY_TARGET_BYTES,
)
_MANIFEST_PADDING = _resolve_padding_len(
    lambda n: PerformanceManifestRequest(
        device_id="perf-device-1",
        bundle_version=0,
        payload="y" * n,
    ),
    _MANIFEST_CODEC,
    MANIFEST_TARGET_BYTES,
)


def build_telemetry_request(sequence: int) -> PerformanceTelemetryRequest:
    return PerformanceTelemetryRequest(
        sequence=sequence,
        metric="pressure",
        value=1.0,
        tags={
            "source": "performance-baseline",
            "unit": "kpa",
            "seq": f"{sequence:012d}",
        },
        padding="x" * _TELEMETRY_PADDING,
    )


def build_manifest_request(sequence: int) -> PerformanceManifestRequest:
    return PerformanceManifestRequest(
        device_id=f"perf-device-{sequence:012d}",
        bundle_version=sequence,
        payload="y" * _MANIFEST_PADDING,
    )


def wire_bytes_for_sequence(workload: str, sequence: int) -> int:
    if workload == "telemetry":
        payload = build_telemetry_request(sequence)
        return _wire_bytes(_TELEMETRY_CODEC, payload)
    payload = build_manifest_request(sequence)
    return _wire_bytes(_MANIFEST_CODEC, payload)


def measure_fixture_byte_range(workload: str, *, sequences: range) -> tuple[int, int]:
    sizes = [wire_bytes_for_sequence(workload, seq) for seq in sequences]
    return min(sizes), max(sizes)


def measure_fixture_sizes() -> FixtureSizes:
    telemetry = build_telemetry_request(0)
    manifest = build_manifest_request(0)
    return FixtureSizes(
        telemetry_request_bytes=_wire_bytes(_TELEMETRY_CODEC, telemetry),
        manifest_request_bytes=_wire_bytes(_MANIFEST_CODEC, manifest),
    )
