"""Harness-side JSON and Markdown reports for performance baselines."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

MANIFEST_RELIABILITY_ISSUE_URL = "https://github.com/vschroeter/superjobs/issues/27"

from tools.performance_support.accounting import throughput_variation
from tools.performance_support.fixtures import measure_fixture_sizes
from tools.performance_support.metadata import environment_block

_HARDWARE_COMPARE_KEYS = (
    "cpu_model",
    "cpu_cores",
    "cpu_logical",
    "ram_bytes",
    "os_caption",
    "c_drive_disk_number",
    "c_drive_model",
    "c_drive_storage_medium",
)


def _broker_volume_key(store_path: str | None) -> str | None:
    if not store_path or store_path == "unknown":
        return store_path
    drive = Path(store_path).drive.upper()
    return drive or "no-drive"


def measurement_clock_comparison_key(clock: dict[str, Any] | None) -> dict[str, Any] | None:
    if not clock:
        return None
    return {
        "function": clock.get("function"),
        "implementation": clock.get("implementation"),
        "resolution": clock.get("resolution"),
        "monotonic": clock.get("monotonic"),
        "adjustable": clock.get("adjustable"),
    }


def normalized_comparison_environment(env: dict[str, Any]) -> dict[str, Any]:
    """Stable environment slice for baseline comparison (ignores ephemeral store paths)."""
    hardware = env.get("hardware") or {}
    return {
        "platform": env.get("platform"),
        "hardware": {key: hardware.get(key) for key in _HARDWARE_COMPARE_KEYS},
        "storage_medium": env.get("storage_medium"),
        "storage_evidence": env.get("storage_evidence"),
        "broker_volume": _broker_volume_key(env.get("broker_jetstream_store_path")),
        "nats_server_version": env.get("nats_server_version"),
        "fixture_request_bytes": env.get("fixture_request_bytes"),
        "child_runtime": env.get("child_runtime"),
        "measurement_clock": measurement_clock_comparison_key(
            env.get("measurement_clock"),
        ),
    }


def observed_request_byte_ranges(
    combinations: list[dict[str, Any]],
) -> dict[str, dict[str, int | None]]:
    """Min/max observed request sizes per workload from combination metadata."""
    ranges: dict[str, dict[str, int | None]] = {}
    for combo in combinations:
        workload = combo.get("workload")
        if not workload:
            continue
        observed = combo.get("observed_request_bytes") or {}
        if not isinstance(observed, dict):
            continue
        slot = ranges.setdefault(
            str(workload),
            {"min": None, "max": None},
        )
        for key in ("min", "max"):
            value = observed.get(key)
            if value is None:
                continue
            current = slot[key]
            if current is None:
                slot[key] = int(value)
            elif key == "min":
                slot[key] = min(current, int(value))
            else:
                slot[key] = max(current, int(value))
    return ranges


def count_failure_diagnostics(combinations: list[dict[str, Any]]) -> int:
    total = 0
    for combo in combinations:
        for sample in combo.get("samples") or []:
            total += len(sample.get("failure_diagnostics") or [])
    return total


def finalize_run_outcome(
    payload: dict[str, Any],
    *,
    run_success: bool,
    harness_exit_code: int,
    error: str | None = None,
) -> dict[str, Any]:
    """Attach explicit harness outcome fields for every profile report."""
    enriched = dict(payload)
    enriched["run_success"] = run_success
    enriched["harness_exit_code"] = harness_exit_code
    if error:
        enriched["error"] = error
    if not run_success and payload.get("profile") == "baseline":
        enriched["run_outcome"] = "failed"
        enriched["follow_up_issues"] = [MANIFEST_RELIABILITY_ISSUE_URL]
    return enriched


def combination_comparison_key(item: dict[str, Any]) -> tuple[Any, ...]:
    timing = item.get("effective_timing") or {}
    return (
        item.get("workload"),
        item.get("concurrency"),
        item.get("inflight", item.get("concurrency")),
        timing.get("warmup_seconds"),
        timing.get("sample_seconds"),
        timing.get("sample_count"),
        timing.get("drain_seconds"),
    )


def build_report_payload(
    *,
    profile: str,
    baseline_eligible: bool,
    combinations: list[dict[str, Any]],
    controls: list[dict[str, Any]],
    repo_root: Path,
    origins: dict[str, Any] | None = None,
    broker_store_path: Path | None = None,
    report_date: str | None = None,
    producer_python: Path | None = None,
    worker_python: Path | None = None,
    environment: dict[str, Any] | None = None,
) -> dict[str, Any]:
    sizes = measure_fixture_sizes()
    fixture_bytes = {
        "telemetry": sizes.telemetry_request_bytes,
        "manifest": sizes.manifest_request_bytes,
    }
    enriched: list[dict[str, Any]] = []
    for combo in combinations:
        samples = list(combo.get("samples") or [])
        enriched.append(
            {
                **combo,
                "throughput_variation": throughput_variation(samples),
                "samples": samples,
            },
        )
    env = environment
    if env is None:
        env = environment_block(
            repo_root=repo_root,
            origins=origins,
            broker_store_path=broker_store_path,
            fixture_bytes=fixture_bytes,
            report_date=report_date,
            producer_python=producer_python,
            worker_python=worker_python,
        )
    return {
        "schema": "superjobs-performance-baseline/v1",
        "date": report_date,
        "profile": profile,
        "baseline_eligible": baseline_eligible,
        "environment": env,
        "combinations": enriched,
        "controls": controls,
        "historical_comparison": None,
        "api_friction_notes": [
            "Producer in-flight limiting uses an explicit asyncio semaphore; SuperJobs does not expose a submit concurrency knob.",
            "No-result telemetry jobs: awaiting outcome() is sufficient for success; calling result() is optional and raises on failure.",
            "Manifest throughput validation replays events() to assert three ordered application events plus system terminals.",
            (
                "Producer sample timestamps use time.perf_counter (high resolution, monotonic). "
                "Field names ending in _mono denote a monotonic domain, not time.monotonic(). "
                "Harness readiness and child lifecycle deadlines use time.monotonic(); never subtract across processes or clock domains."
            ),
        ],
    }


def write_json_report(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def write_markdown_report(path: Path, payload: dict[str, Any]) -> None:
    env = payload["environment"]
    hardware = env.get("hardware") or {}
    run_success = payload.get("run_success")
    is_failed_baseline = (
        payload.get("profile") == "baseline"
        and run_success is False
        and payload.get("baseline_eligible") is False
    )
    title_date = payload.get("date") or "undated"
    if is_failed_baseline:
        title = (
            f"# SuperJobs performance baseline attempt (failed, ineligible) — {title_date}"
        )
    else:
        title = f"# SuperJobs performance baseline ({title_date})"
    lines = [title, ""]
    if is_failed_baseline:
        harness_code = payload.get("harness_exit_code")
        err = payload.get("error") or "benchmark outcome validation failed"
        diag_rows = count_failure_diagnostics(payload.get("combinations") or [])
        ranges = observed_request_byte_ranges(payload.get("combinations") or [])
        range_bits: list[str] = []
        for workload in ("telemetry", "manifest"):
            slot = ranges.get(workload) or {}
            if slot.get("min") is not None and slot.get("max") is not None:
                range_bits.append(f"{workload} {slot['min']}–{slot['max']} bytes")
        range_text = "; ".join(range_bits) if range_bits else "unavailable"
        lines.extend(
            [
                (
                    f"This attempt failed (harness exit {harness_code}): {err}. "
                    "Retained measurements are ineligible for dated success baselines. "
                    f"Follow-up: [{MANIFEST_RELIABILITY_ISSUE_URL}]({MANIFEST_RELIABILITY_ISSUE_URL})."
                ),
                (
                    f"Measured revision `{env.get('revision', 'unknown')}` "
                    f"(clean: {env.get('revision_clean')}). "
                    f"Typed failure_diagnostics rows in report: {diag_rows}. "
                    f"Observed request byte ranges: {range_text}."
                ),
                "",
            ],
        )
    lines.extend(
        [
        f"- Profile: **{payload['profile']}** "
        f"(baseline eligible: {payload['baseline_eligible']})",
        f"- Revision: `{env['revision']}` (clean: {env.get('revision_clean')})",
        f"- Child Python (producer): "
        f"{(env.get('child_runtime') or {}).get('producer', {}).get('python', 'unknown')}",
        f"- NATS server: {env['nats_server_version']}",
        f"- Storage medium (broker temp): {env['storage_medium']}",
        f"- CPU: {hardware.get('cpu_model', 'unknown')}",
        f"- Telemetry request bytes: {env['fixture_request_bytes']['telemetry']}",
        f"- Manifest request bytes: {env['fixture_request_bytes']['manifest']}",
        ],
    )
    if run_success is not None:
        lines.append(
            f"- Harness: run_success={run_success}, harness_exit_code={payload.get('harness_exit_code')}",
        )
    measurement_clock = env.get("measurement_clock") or {}
    if measurement_clock:
        lines.extend(
            [
                f"- Measurement clock: `{measurement_clock.get('function', 'unknown')}` "
                f"({measurement_clock.get('implementation', 'unknown')}, "
                f"resolution={measurement_clock.get('resolution')})",
            ],
        )
    lines.extend(
        [
            "",
            "## Combinations",
            "",
        ],
    )
    for item in payload["combinations"]:
        lines.append(
            f"### {item['workload']} @ concurrency {item['concurrency']} "
            f"(in-flight {item.get('inflight', item['concurrency'])})",
        )
        variation = item.get("throughput_variation") or {}
        if variation.get("min") is not None:
            lines.append(
                f"- Throughput variation across samples: "
                f"min={variation['min']:.2f}/s max={variation['max']:.2f}/s "
                f"spread={variation['spread']:.2f}/s",
            )
        for index, sample in enumerate(item.get("samples") or [], start=1):
            counts = sample.get("counts") or {}
            submit_lat = sample.get("submit_latency_seconds") or {}
            e2e = sample.get("submit_to_terminal_seconds") or {}
            lines.append(
                f"- Sample {index}: {sample.get('throughput_per_second', 0):.2f} jobs/s "
                f"(fixed interval {sample.get('throughput_interval_seconds', sample.get('submission_boundary_seconds', 0)):.2f}s, "
                f"observed {sample.get('window_seconds', 0):.3f}s); "
                f"success={counts.get('success_in_window', 0)}, "
                f"late={counts.get('success_late_drain', 0)}, "
                f"failures={counts.get('failure', 0)}, "
                f"validation={counts.get('validation_error', 0)}, "
                f"incomplete={counts.get('incomplete_drain', 0)}",
            )
            if submit_lat.get("median") is not None:
                lines.append(
                    f"  - Submit latency (successful completions, n={submit_lat.get('n')}): "
                    f"median={submit_lat['median']:.4f}s p95={submit_lat['p95']:.4f}s",
                )
            if e2e.get("median") is not None:
                scope = e2e.get("scope")
                scope_note = f" [{scope}]" if scope else ""
                lines.append(
                    f"  - End-to-end (successful completions, n={e2e.get('n')}){scope_note}: "
                    f"median={e2e['median']:.4f}s p95={e2e['p95']:.4f}s",
                )
            for row in sample.get("failure_diagnostics") or []:
                lines.append(
                    "  - failure_diagnostic: "
                    f"sequence={row.get('sequence')} execution_id={row.get('execution_id')} "
                    f"phase={row.get('phase')} bucket={row.get('bucket')} "
                    f"job_error_code={row.get('job_error_code')} "
                    f"job_error_message={row.get('job_error_message')} "
                    f"exception_type={row.get('exception_type')} "
                    f"exception_message={row.get('exception_message')}",
                )
        combo_error = item.get("error")
        if combo_error:
            lines.append(f"- Combination error: {combo_error}")
        lines.append("")
    if payload.get("controls"):
        lines.extend(["## Control smokes", ""])
        for control in payload["controls"]:
            lines.append(
                f"- {control['name']}: producer exit {control.get('producer_exit')} "
                f"(expected {control.get('expected_exit')})",
            )
            samples = control.get("samples") or []
            if samples:
                counts = (samples[0].get("counts") or {}) if samples else {}
                lines.append(
                    f"  - Sample counts: success={counts.get('success_in_window', 0)} "
                    f"failure={counts.get('failure', 0)} "
                    f"incomplete={counts.get('incomplete_drain', 0)}",
                )
        lines.append("")
    lines.extend(["## API friction", ""])
    for note in payload.get("api_friction_notes") or []:
        lines.append(f"- {note}")
    lines.append("")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def comparison_config_keys() -> tuple[str, ...]:
    return tuple(normalized_comparison_environment({}).keys())


def compare_reports(left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
    """Ratio-style comparison when normalized environment and combination timing match."""

    left_env = normalized_comparison_environment(left.get("environment") or {})
    right_env = normalized_comparison_environment(right.get("environment") or {})
    compatible = left_env == right_env
    ratios: list[dict[str, Any]] = []
    if compatible:
        right_by_key = {
            combination_comparison_key(item): item for item in right.get("combinations") or []
        }
        for item in left.get("combinations") or []:
            key = combination_comparison_key(item)
            other = right_by_key.get(key)
            if other is None:
                continue
            left_rates = [
                float(sample.get("throughput_per_second") or 0.0)
                for sample in item.get("samples") or []
            ]
            right_rates = [
                float(sample.get("throughput_per_second") or 0.0)
                for sample in other.get("samples") or []
            ]
            if not left_rates or not right_rates:
                continue
            left_avg = sum(left_rates) / len(left_rates)
            right_avg = sum(right_rates) / len(right_rates)
            ratio = left_avg / right_avg if right_avg else None
            ratios.append(
                {
                    "workload": item["workload"],
                    "concurrency": item["concurrency"],
                    "inflight": item.get("inflight", item["concurrency"]),
                    "left_avg_throughput": left_avg,
                    "right_avg_throughput": right_avg,
                    "throughput_ratio_left_over_right": ratio,
                    "left_variation": throughput_variation(item.get("samples") or []),
                    "right_variation": throughput_variation(other.get("samples") or []),
                },
            )
    return {
        "compatible_environment": compatible,
        "left_environment": left_env,
        "right_environment": right_env,
        "ratios": ratios,
    }
