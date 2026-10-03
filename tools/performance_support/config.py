"""Timing and exit-code constants for performance baselines."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Literal

WorkloadName = Literal["telemetry", "manifest"]
WorkerMode = Literal["success", "failure", "delay", "incomplete"]
RunProfile = Literal["baseline", "smoke", "diagnostic"]

EXIT_OK = 0
EXIT_ASSERTION = 3
EXIT_VALIDATION = 4
EXIT_INCOMPLETE_DRAIN = 5
EXIT_ENV = 11

BASELINE_WARMUP_SECONDS = 3.0
BASELINE_SAMPLE_SECONDS = 20.0
BASELINE_SAMPLE_COUNT = 3
BASELINE_DRAIN_SECONDS = 60.0

SMOKE_WARMUP_SECONDS = 0.5
SMOKE_SAMPLE_SECONDS = 1.0
SMOKE_SAMPLE_COUNT = 1
SMOKE_DRAIN_SECONDS = 8.0

CONCURRENCY_LEVELS = (1, 8)
WORKLOADS: tuple[WorkloadName, ...] = ("telemetry", "manifest")

MANIFEST_EVENT_STAGES = ("received", "validated", "published")
MANIFEST_EXPECTED_REVISION = "perf-baseline-rev"

TELEMETRY_TARGET_BYTES = 1024
MANIFEST_TARGET_BYTES = 64 * 1024

MIN_SAMPLES_FOR_PERCENTILES = 20


@dataclass(frozen=True, slots=True)
class TimingProfile:
    name: RunProfile
    warmup_seconds: float
    sample_seconds: float
    sample_count: int
    drain_seconds: float
    baseline_eligible: bool


def profile_for(name: RunProfile) -> TimingProfile:
    if name == "smoke":
        return TimingProfile(
            name="smoke",
            warmup_seconds=SMOKE_WARMUP_SECONDS,
            sample_seconds=SMOKE_SAMPLE_SECONDS,
            sample_count=SMOKE_SAMPLE_COUNT,
            drain_seconds=SMOKE_DRAIN_SECONDS,
            baseline_eligible=False,
        )
    if name == "diagnostic":
        return TimingProfile(
            name="diagnostic",
            warmup_seconds=BASELINE_WARMUP_SECONDS,
            sample_seconds=BASELINE_SAMPLE_SECONDS,
            sample_count=BASELINE_SAMPLE_COUNT,
            drain_seconds=BASELINE_DRAIN_SECONDS,
            baseline_eligible=False,
        )
    return TimingProfile(
        name="baseline",
        warmup_seconds=BASELINE_WARMUP_SECONDS,
        sample_seconds=BASELINE_SAMPLE_SECONDS,
        sample_count=BASELINE_SAMPLE_COUNT,
        drain_seconds=BASELINE_DRAIN_SECONDS,
        baseline_eligible=True,
    )


def _parse_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return float(raw)


def _parse_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return int(raw)


def timing_from_env() -> TimingProfile:
    profile_raw = os.environ.get("SUPERJOBS_PERF_PROFILE", "baseline")
    if profile_raw == "diagnostic":
        base = profile_for("diagnostic")
    elif profile_raw == "smoke":
        base = profile_for("smoke")
    else:
        base = profile_for("baseline")
    warmup = _parse_float("SUPERJOBS_PERF_WARMUP_SECONDS", base.warmup_seconds)
    sample_seconds = _parse_float("SUPERJOBS_PERF_SAMPLE_SECONDS", base.sample_seconds)
    sample_count = _parse_int("SUPERJOBS_PERF_SAMPLE_COUNT", base.sample_count)
    drain_seconds = _parse_float("SUPERJOBS_PERF_DRAIN_SECONDS", base.drain_seconds)
    eligible = base.baseline_eligible and profile_raw not in ("smoke", "diagnostic")
    if eligible:
        eligible = (
            warmup == BASELINE_WARMUP_SECONDS
            and sample_seconds == BASELINE_SAMPLE_SECONDS
            and sample_count == BASELINE_SAMPLE_COUNT
            and drain_seconds == BASELINE_DRAIN_SECONDS
            and not any(
                os.environ.get(name)
                for name in (
                    "SUPERJOBS_PERF_WARMUP_SECONDS",
                    "SUPERJOBS_PERF_SAMPLE_SECONDS",
                    "SUPERJOBS_PERF_SAMPLE_COUNT",
                    "SUPERJOBS_PERF_DRAIN_SECONDS",
                )
            )
        )
    return TimingProfile(
        name=base.name,
        warmup_seconds=warmup,
        sample_seconds=sample_seconds,
        sample_count=sample_count,
        drain_seconds=drain_seconds,
        baseline_eligible=eligible,
    )


def workload_from_env() -> WorkloadName:
    raw = os.environ.get("SUPERJOBS_PERF_WORKLOAD", "telemetry")
    if raw not in WORKLOADS:
        raise ValueError(f"unknown workload {raw!r}")
    return raw


def worker_mode_from_env() -> WorkerMode:
    raw = os.environ.get("SUPERJOBS_PERF_WORKER_MODE", "success")
    if raw not in ("success", "failure", "delay", "incomplete"):
        raise ValueError(f"unknown worker mode {raw!r}")
    return raw


def concurrency_from_env() -> int:
    value = _parse_int("SUPERJOBS_PERF_CONCURRENCY", 1)
    if value < 1:
        raise ValueError("concurrency must be at least 1")
    return value


def inflight_from_env(concurrency: int) -> int:
    value = _parse_int("SUPERJOBS_PERF_INFLIGHT", concurrency)
    if value < 1:
        raise ValueError("inflight must be at least 1")
    return value
