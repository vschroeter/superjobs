# Manual performance baseline (issue #23)

Implemented manual tooling for workload-based measurements after the reliable
test foundation ([issue #19](https://github.com/vschroeter/superjobs/issues/19),
implementation [#23](https://github.com/vschroeter/superjobs/issues/23)). This
document describes the harness design; numbers are produced only by running
`tools/verify_performance.py` on a prepared host. Dated artifacts are indexed in
[performance-baselines/README.md](../performance-baselines/README.md). Combined
#22/#23 verification: [verification.md](../verification.md).
Documentation index: [docs/README.md](../README.md).

## Topology

- One **installed** producer process and one **installed** worker process (separate virtual environments, wheel-only imports).
- One **harness-owned** disk-backed NATS server (`OwnedNatsServer`) on the same host.
- Producer in-flight limit matches worker handler `concurrency` (1 and 8 for the baseline matrix).

## Workloads

| Workload | Fixture | Worker behavior | Producer validation |
| --- | --- | --- | --- |
| `telemetry` | Structured request padded to ~1 KiB msgpack | No result payload, no application events | `JobSucceeded`, `result()` is `None`, terminal `JobCompleted` |
| `manifest` | Structured request padded to ~64 KiB msgpack | Compact `PerformanceManifestResult`, three ordered `PerformanceManifestEvent` stages | Result revision, event stages `received` → `validated` → `published` |

Actual serialized byte sizes are recorded in every report (`fixture_request_bytes`).

## Sampling

| Profile | Warmup | Sample window | Samples | Drain budget | Baseline eligible |
| --- | --- | --- | --- | --- | --- |
| `baseline` | 3 s | 20 s | 3 per combination | 60 s | yes |
| `smoke` (`--smoke`) | 0.5 s | 1 s | 1 | 8 s | **no** (explicitly labeled non-baseline) |

For each profile the producer stops new submissions at the sample boundary, then drains in-flight work within the drain budget.

## Metrics and accounting

- **Throughput**: count of successful, validated terminal completions before the fixed sample boundary, divided by that same fixed observation interval (`throughput_interval_seconds`, 20 seconds for a baseline). The producer observes the entire interval even when it stops admitting submissions up to 50 ms early to leave time for publish acceptance. Actual boundary wake-up (`window_seconds`), submission loop stop, and drain elapsed are reported separately. Late completions, failures, validation errors, and incomplete drain do not enter the numerator.
- **Submit latency**: producer `time.perf_counter()` from submit start to submit return (`median` / `p95` when at least 20 samples exist). JSON field names ending in `_mono` mean a monotonic clock domain, not `time.monotonic()`.
- **Submit-to-terminal latency**: same clock from submit start to validated terminal (`median` / `p95` with the same minimum N).

### Measurement clock (Windows / Python 3.12)

Producer samples and scheduling deadlines use `time.perf_counter()` (typically `QueryPerformanceCounter()`, sub-microsecond resolution). Harness readiness, child reap deadlines, and cross-process protocol polling stay on `time.monotonic()` (on Windows 3.12 often `GetTickCount64()` at **15.625 ms** resolution). Never subtract timestamps across processes or across these domains.

Reports record `measurement_clock` metadata from the installed producer child (`time.get_clock_info('perf_counter')`). Baseline comparison refuses throughput ratios when `measurement_clock` identity differs (for example legacy `monotonic` / `GetTickCount64()` artifacts vs corrected `perf_counter` runs).

The 2026-10-03 failed baseline at revision `af5fcec` used `time.monotonic()` for samples; six telemetry/manifest submit-latency medians read **0.0** because of quantization, not because submit was free. That artifact is preserved with retrospective `GetTickCount64()` metadata in `docs/performance-baselines/2026-10-03-failed-baseline.{json,md}` (do not overwrite when re-running the harness; use `--baseline-dir` for new attempts). A green diagnostic run does not repair manifest reliability failures; retained failure rows stay evidence. Intermittent manifest observation replay was investigated under [issue #27](https://github.com/vschroeter/superjobs/issues/27) (closed as not currently reproducible); automatic failed-baseline reports may still link that issue and retain typed `failure_diagnostics` when present.

End-to-end timings include application, serialization, broker, and storage costs; they are not labeled as pure queue wait.

## Controls

Short smoke runs (`--smoke`) execute control worker modes after the benchmark combination:

| Control | Worker mode | Expected producer exit |
| --- | --- | --- |
| `control_failure` | `failure` | validation / failure exit (non-zero) |
| `control_delay` | `delay` | success |
| `control_incomplete` | `incomplete` | incomplete drain (non-zero) |

## Verification gates

Before benchmarks (unless `--skip-contract-typing`):

```powershell
uv run --no-project --python 3.12 --with . python tools/verify_contract_typing.py --mode both --python 3.12
```

Wheel origin probes mirror `verify_cross_program.py` (producer must not import worker modules).

## Running

Full baseline (long; do not use for CI):

```powershell
uv run --no-project --python 3.12 --with . python tools/verify_performance.py --python 3.12
```

Short smoke (non-baseline):

```powershell
uv run --no-project --python 3.12 --with . python tools/verify_performance.py --smoke --skip-contract-typing --python 3.12 --artifact-dir dist/performance-smoke
```

Deterministic unit tests (no NATS by default):

```powershell
uv run --no-project --python 3.12 --with . --with pytest python -m pytest tests/test_verify_performance.py -q
```

## Artifacts

- Dated baseline JSON/Markdown: `docs/performance-baselines/YYYY-MM-DD-baseline.{json,md}` (baseline profile only).
- Raw child logs and harness evidence: each explicit `--artifact-dir` gets a fresh `run-<token>` child directory, preserving prior invocations and preventing stale recovery after setup failures. Default evidence is temporary; raw logs are not committed.
- Environment block includes git revision and cleanliness, child-runtime origins (not harness packages), owned broker JetStream store path, optional `--report-date`, and hardware/storage evidence from read-only PowerShell inspection when available (`unknown` otherwise).

## Verification on 2026-10-03

The final high-resolution baseline is recorded in `docs/performance-baselines/high-resolution/2026-10-03-baseline.{json,md}`. It measured clean implementation `2f7797f04107aea6ae7f65b8d65c50cea1dbe912`, one installed Python 3.12.11 runtime, and disk-backed NATS 2.15.0 on the documented Windows SSD host. All four combinations retained three 20-second intervals after a three-second warmup; harness and producers exited zero. Failure, validation, incomplete-drain, failed-submission and uncertain-submission counts were zero. The 21 late successful completions were excluded from throughput. Measured wire sizes were 1041–1043 bytes for telemetry and 65547–65549 bytes for manifest, derived from producer evidence rather than the nominal first fixture.

Sample rates (jobs/s) were telemetry/1: 15.95, 15.85, 15.40; telemetry/8: 98.40, 80.95, 72.95; manifest/1: 7.40, 7.60, 7.40; manifest/8: 75.30, 63.85, 55.25. These show material within-run variation, not universal targets. No compatible historical baseline exists: earlier failed measurements used a different clock. A passing final run does not explain or repair earlier intermittent observation replay errors; retain the failed and diagnostic artifacts. See [verification.md](../verification.md) and [issue #27](https://github.com/vschroeter/superjobs/issues/27) (closed; reopen if symptoms return).

Independent focused checks passed: 39 deterministic tests before the final invocation-isolation correction, then four relevant regression tests covering stale-summary rejection, partial recovery and installed clock metadata. Positive and exact two-negative performance consumer checks passed. The existing source/wheel public typing gate passed, including its negative consumers and 24 installed runtime checks. A separate installed producer/worker smoke exercised failure (exit 4), successful delay (exit 0), and incomplete drain (exit 5); full measurements exclude these controls. Both roles' wheel-origin probes passed in the final run. Raw logs, role origins and producer results remain under `dist/performance-final-high-resolution-2026-10-03/run-a1590aae56e04affaed9521d9b74b8a9/py312/`; only small dated reports are tracked.

## API friction (first slice)

Documented in generated reports and kept in sync with the harness:

- Producer-side in-flight limiting uses an explicit `asyncio.Semaphore`; the library does not expose a submit concurrency knob.
- No-result telemetry jobs: `outcome()` is sufficient for success; `result()` is optional and raises on failure.
- Manifest correctness checks require replaying `events()` to assert ordered intermediate events.

Public API changes suggested by this friction are out of scope for issue #23.

## Deferred

Per-job production instrumentation, pure queue/handler timing, cross-process clock subtraction, CPU/RSS exporters, dashboards, automated baseline services, p99 claims, and PR regression thresholds remain deferred per [ADR 0005](../adr/0005-broker-outage-retry-and-optional-stress.md).
