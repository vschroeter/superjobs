"""Deterministic throughput and latency accounting for performance samples."""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Literal

try:
    from config import (
        EXIT_INCOMPLETE_DRAIN,
        EXIT_OK,
        EXIT_VALIDATION,
        MIN_SAMPLES_FOR_PERCENTILES,
    )
except ModuleNotFoundError:
    from tools.performance_support.config import (
        EXIT_INCOMPLETE_DRAIN,
        EXIT_OK,
        EXIT_VALIDATION,
        MIN_SAMPLES_FOR_PERCENTILES,
    )

OutcomeBucket = Literal[
    "success_in_window",
    "success_late_drain",
    "failure",
    "validation_error",
    "incomplete_drain",
    "submit_failed",
    "submit_uncertain",
]

FailurePhase = Literal[
    "submit",
    "outcome",
    "validation_result",
    "validation_events",
    "validation_outcome",
    "cleanup",
]

_SUCCESS_BUCKETS: frozenset[OutcomeBucket] = frozenset(
    ("success_in_window", "success_late_drain"),
)


@dataclass(slots=True)
class CompletionRecord:
    sequence: int
    submit_started_mono: float
    submit_finished_mono: float | None
    terminal_mono: float | None
    bucket: OutcomeBucket
    detail: str | None = None
    phase: FailurePhase | None = None
    execution_id: str | None = None
    job_error_code: str | None = None
    job_error_message: str | None = None
    exception_type: str | None = None
    exception_repr: str | None = None
    exception_message: str | None = None
    replay_evidence: dict[str, Any] | None = None


@dataclass(slots=True)
class SampleWindow:
    submission_start_mono: float
    submission_end_mono: float
    measured_submission_end_mono: float | None = None
    submission_loop_stop_mono: float | None = None
    lifecycle_end_mono: float | None = None
    drain_start_mono: float | None = None
    drain_end_mono: float | None = None

    @property
    def elapsed_seconds(self) -> float:
        measured_end = self.measured_submission_end_mono
        if measured_end is None:
            measured_end = self.submission_loop_stop_mono
        if measured_end is None:
            measured_end = self.submission_end_mono
        return max(0.0, measured_end - self.submission_start_mono)

    @property
    def fixed_boundary_mono(self) -> float:
        return self.submission_end_mono

    @property
    def intended_submission_window_seconds(self) -> float:
        return max(0.0, self.submission_end_mono - self.submission_start_mono)

    @property
    def submission_boundary_reached_seconds(self) -> float:
        measured_end = self.measured_submission_end_mono
        if measured_end is None:
            measured_end = self.submission_end_mono
        return max(0.0, measured_end - self.submission_start_mono)

    @property
    def submission_loop_stop_seconds(self) -> float:
        if self.submission_loop_stop_mono is None:
            return self.elapsed_seconds
        return max(0.0, self.submission_loop_stop_mono - self.submission_start_mono)

    @property
    def drain_elapsed_seconds(self) -> float:
        if self.drain_start_mono is None or self.drain_end_mono is None:
            return 0.0
        return max(0.0, self.drain_end_mono - self.drain_start_mono)

    @property
    def drain_overrun_seconds(self) -> float:
        if self.lifecycle_end_mono is None or self.drain_end_mono is None:
            return 0.0
        return max(0.0, self.drain_end_mono - self.lifecycle_end_mono)


@dataclass
class SampleAccounting:
    window: SampleWindow
    completions: list[CompletionRecord] = field(default_factory=list)

    def add(self, record: CompletionRecord) -> None:
        self.completions.append(record)

    def counts(self) -> dict[str, int]:
        totals: dict[str, int] = {
            "success_in_window": 0,
            "success_late_drain": 0,
            "failure": 0,
            "validation_error": 0,
            "incomplete_drain": 0,
            "submit_failed": 0,
            "submit_uncertain": 0,
        }
        for item in self.completions:
            totals[item.bucket] += 1
        return totals

    def throughput_per_second(self) -> float:
        elapsed = self.window.intended_submission_window_seconds
        if elapsed <= 0:
            return 0.0
        return self.counts()["success_in_window"] / elapsed

    def _successful_buckets(self) -> tuple[str, str]:
        return ("success_in_window", "success_late_drain")

    def submit_latencies_seconds(self) -> list[float]:
        values: list[float] = []
        for item in self.completions:
            if item.bucket not in self._successful_buckets():
                continue
            if item.submit_finished_mono is None:
                continue
            values.append(item.submit_finished_mono - item.submit_started_mono)
        return values

    def submit_to_terminal_latencies_seconds(self) -> list[float]:
        values: list[float] = []
        for item in self.completions:
            if item.terminal_mono is None:
                continue
            if item.bucket not in self._successful_buckets():
                continue
            values.append(item.terminal_mono - item.submit_started_mono)
        return values

    def failure_diagnostics(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for item in self.completions:
            if item.bucket in _SUCCESS_BUCKETS:
                continue
            rows.append(
                {
                    "sequence": item.sequence,
                    "execution_id": item.execution_id,
                    "phase": item.phase or _default_failure_phase(item.bucket),
                    "bucket": item.bucket,
                    "detail": item.detail,
                    "job_error_code": item.job_error_code,
                    "job_error_message": item.job_error_message,
                    "exception_type": item.exception_type,
                    "exception_repr": item.exception_repr,
                    "exception_message": item.exception_message,
                    "replay_evidence": item.replay_evidence,
                },
            )
        return rows

    def summarize(self) -> dict[str, Any]:
        counts = self.counts()
        successful_scope = (
            "successful_completions_including_late_drain_for_latency_only"
        )
        failure_rows = self.failure_diagnostics()
        return {
            "window_seconds": self.window.elapsed_seconds,
            "submission_boundary_seconds": self.window.intended_submission_window_seconds,
            "submission_boundary_reached_seconds": (
                self.window.submission_boundary_reached_seconds
            ),
            "submission_loop_stop_seconds": self.window.submission_loop_stop_seconds,
            "drain_elapsed_seconds": self.window.drain_elapsed_seconds,
            "drain_overrun_seconds": self.window.drain_overrun_seconds,
            "throughput_per_second": self.throughput_per_second(),
            "throughput_interval_seconds": self.window.intended_submission_window_seconds,
            "counts": counts,
            "submit_latency_seconds": {
                "scope": successful_scope,
                **percentile_summary(self.submit_latencies_seconds()),
            },
            "submit_to_terminal_seconds": {
                "scope": successful_scope,
                **percentile_summary(self.submit_to_terminal_latencies_seconds()),
            },
            "failure_diagnostics": failure_rows,
        }


def _default_failure_phase(bucket: OutcomeBucket) -> FailurePhase:
    if bucket in ("submit_failed", "submit_uncertain"):
        return "submit"
    return "outcome"


def exception_fields(exc: BaseException) -> dict[str, str | None]:
    exc_type = type(exc)
    qualified = f"{exc_type.__module__}.{exc_type.__qualname__}"
    message = str(exc)
    return {
        "exception_type": qualified,
        "exception_repr": repr(exc),
        "exception_message": message,
        "detail": message,
    }


def classify_terminal_time(
    terminal_mono: float,
    window_end_mono: float,
) -> Literal["success_in_window", "success_late_drain"]:
    if terminal_mono <= window_end_mono:
        return "success_in_window"
    return "success_late_drain"


def percentile_summary(values: list[float]) -> dict[str, float | int | None]:
    if len(values) < MIN_SAMPLES_FOR_PERCENTILES:
        return {"median": None, "p95": None, "n": len(values)}
    ordered = sorted(values)
    return {
        "median": _percentile(ordered, 0.5),
        "p95": _percentile(ordered, 0.95),
        "n": len(values),
    }


def throughput_variation(samples: list[dict[str, Any]]) -> dict[str, float | None]:
    rates = [
        float(item.get("throughput_per_second") or 0.0)
        for item in samples
        if item.get("throughput_per_second") is not None
    ]
    if not rates:
        return {"min": None, "max": None, "spread": None}
    return {
        "min": min(rates),
        "max": max(rates),
        "spread": max(rates) - min(rates),
    }


def _percentile(ordered: list[float], quantile: float) -> float:
    if not ordered:
        return math.nan
    if len(ordered) == 1:
        return ordered[0]
    index = quantile * (len(ordered) - 1)
    lower = math.floor(index)
    upper = math.ceil(index)
    if lower == upper:
        return ordered[lower]
    weight = index - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def _sample_has_measurable_work(accounting: SampleAccounting) -> bool:
    counts = accounting.counts()
    return (
        counts["success_in_window"]
        + counts["success_late_drain"]
        + counts["failure"]
        + counts["validation_error"]
        + counts["incomplete_drain"]
    ) > 0


def producer_exit_code(accounting: SampleAccounting, *, expect_success: bool) -> int:
    counts = accounting.counts()
    successes = counts["success_in_window"] + counts["success_late_drain"]
    if counts["incomplete_drain"] > 0 or counts["submit_uncertain"] > 0:
        return EXIT_INCOMPLETE_DRAIN
    if counts["submit_failed"] > 0:
        return EXIT_VALIDATION
    if not _sample_has_measurable_work(accounting):
        return EXIT_VALIDATION
    if expect_success:
        if counts["validation_error"] > 0 or counts["failure"] > 0:
            return EXIT_VALIDATION
        if counts["success_in_window"] == 0:
            return EXIT_VALIDATION
        return EXIT_OK
    if successes > 0:
        return EXIT_VALIDATION
    if counts["failure"] + counts["validation_error"] > 0:
        return EXIT_VALIDATION
    return EXIT_VALIDATION


def aggregate_exit_code(
    samples: list[SampleAccounting],
    *,
    expect_success: bool,
    allow_incomplete_drain: bool = False,
) -> int:
    code = EXIT_OK
    for sample in samples:
        sample_code = producer_exit_code(sample, expect_success=expect_success)
        if allow_incomplete_drain and sample_code == EXIT_INCOMPLETE_DRAIN:
            code = sample_code
            continue
        if sample_code != EXIT_OK:
            code = sample_code
    return code
