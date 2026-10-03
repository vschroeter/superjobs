# Combined repetition and performance verification

Client report date: **2026-10-03**. Issues [#22](https://github.com/vschroeter/superjobs/issues/22)
and [#23](https://github.com/vschroeter/superjobs/issues/23) were implemented independently
through Cursor Composer 2.5 in separate worktrees, reviewed by Codex, and merged into
`api_design`. Targeted corrections followed independent review and measured failures.

## Required merged checks

| Host/runtime | Result | Local evidence |
| --- | --- | --- |
| Windows, Python 3.12 | All seven stages passed; 461 deterministic tests passed | `dist/verification/issues22-23-final/w312/` |
| WSL Ubuntu 24.04, Python 3.14 | All seven stages passed; 460 deterministic tests passed, two platform skips | `dist/verification/issues22-23-final/l314-corrected/` |

The stages cover deterministic tests, source and installed-wheel positive/negative
contract typing, real NATS tests, installed cross-program behavior, worker crash
recovery, persistent broker restart, and idle outage recovery. Windows full checks
passed before the final test-only portability correction; both corrected platform
cases were then verified on their respective hosts. Linux full checks passed after
the correction at merged revision `6c617c186b5d45af04c83f2dd1abc03002cd9332`.

The original Linux failure is retained under `dist/verification/issues22-23-final/l314/`:
a test constructed `Path("D:/nats/store")` on POSIX and incorrectly expected native
Windows drive evidence. Separate Windows drive and POSIX no-drive tests corrected
that assertion without changing storage or measurement behavior.

## Optional reliability repetition

The four selected reliability families each ran exactly three times with seed
`20261003`: **12/12 passed** on Windows and Linux using Python 3.14.7. Wall times
were 154.2 seconds and 104.0 seconds respectively. Real missing-broker and timeout
controls exited nonzero, stopped the run, and retained evidence. See
[reliability-repetition-verification.md](reliability-repetition-verification.md).

The optional weekly workflow is committed on `api_design`. GitHub scheduled runs
activate only after the workflow exists on the repository default branch, `main`.

## Manual performance measurements

The final measurement used clean implementation revision
`2f7797f04107aea6ae7f65b8d65c50cea1dbe912`, separate installed producer/worker
environments, Python 3.12.11, pinned disk-backed NATS 2.15.0, and an SSD on the same
Windows host. The measurement clock is monotonic `perf_counter` using
`QueryPerformanceCounter()`, with reported resolution 0.0000001 seconds.

All four workload/concurrency combinations ran warmup followed by three fixed
20-second intervals. All twelve samples passed; error and incomplete counts were
zero. **21 late successful completions were excluded from throughput.** Actual
requests ranged from 1,041–1,043 bytes for telemetry and 65,547–65,549 bytes for
manifests. Installed dependency versions, storage/hardware evidence, sample
variation and median/p95 latency counts are retained in the
[dated baseline](../performance-baselines/high-resolution/2026-10-03-baseline.md)
and its [machine-readable report](../performance-baselines/high-resolution/2026-10-03-baseline.json).
These are measurements of this host/configuration, not universal thresholds.

The original measurement recorded six ambiguous failure observations. A subsequent
full-order diagnostic captured manifest event-read errors: NATS `consumer closed`
and SuperJobs expired observation cursors. Both reports are preserved under
`docs/performance-baselines/`. The intermittent errors remain unresolved in
[issue #27](https://github.com/vschroeter/superjobs/issues/27); the passing final
measurement does not establish a runtime repair. No core runtime change was made
for issue #23. See [performance-baseline.md](performance-baseline.md) for commands,
failure controls, public consumer typing checks and evidence provenance.
