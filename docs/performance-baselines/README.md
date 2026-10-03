# Performance baseline artifacts

These files are **measurements** from `tools/verify_performance.py` on specific
hosts and revisions. They are not universal SLOs or required CI gates. Harness
design, commands, and accounting rules are in
[performance-baseline.md](../design/performance-baseline.md).

## Tracked reports

| Artifact | Profile | Notes |
| --- | --- | --- |
| [high-resolution/2026-10-03-baseline.md](high-resolution/2026-10-03-baseline.md) | `baseline` | Passing run at `2f7797f`; `perf_counter` clock; baseline-eligible |
| [high-resolution/2026-10-03-baseline.json](high-resolution/2026-10-03-baseline.json) | `baseline` | Machine-readable companion |
| [2026-10-03-failed-baseline.md](2026-10-03-failed-baseline.md) | `baseline` | Retained failure at `af5fcec`; legacy `monotonic` clock artifact |
| [2026-10-03-failed-baseline.json](2026-10-03-failed-baseline.json) | `baseline` | Machine-readable companion |
| [2026-10-03-original-order-diagnostic.json](2026-10-03-original-order-diagnostic.json) | diagnostic | Full-order workload; manifest event-read errors |

Do not overwrite retained failure artifacts when re-running the harness; use a
new `--artifact-dir` and optional `--baseline-dir` for fresh attempts.

Manifest observation replay under accumulated NATS workload was tracked in
[issue #27](https://github.com/vschroeter/superjobs/issues/27) (closed as not
currently reproducible; its discussion records the outcome and retained failure evidence).
Combined #22/#23 context: [verification.md](../verification.md).
Canonical limitations: [verification.md](../verification.md).
