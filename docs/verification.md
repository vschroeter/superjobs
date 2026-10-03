# Verification summary

Dated measured results for branch **`api_design`**. Commands and matrix:
[development.md](development.md). This file records **what was run**, not product
SLOs.

## Required CI at `04f24f2`

| Check | Result |
| --- | --- |
| GitHub Actions `checks / fast` + `checks / integration` | **Passed** — [run 37127882378](https://github.com/vschroeter/superjobs/actions/runs/37127882378): 6 fast matrix cells (Windows/Linux × py312/313/314), 4 integration cells (Windows/Linux × py312/314), both aggregates |
| `dev_check full` seven stages | Covered by **fast** matrix jobs (stage 1) **plus** **integration** matrix jobs (stages 2–7); `dev_check integration` alone runs **six** stages |

Library changes between `af5fcec` and `04f24f2` did not alter `src/`; documentation
and local tooling edits may differ on your machine.

## Foundation evidence (issue #15)

Earlier independent runs that established the current gates:

| Check | Result | Notes |
| --- | --- | --- |
| Hosted matrix | **12/12 jobs passed** | [Run 36910809614](https://github.com/vschroeter/superjobs/actions/runs/36910809614), revision `083fe594`, draft PR #26 |
| `dev_check full`, Windows py312 / Linux py314 (local) | **Six stages passed** each | Pre-orchestration-final suite; see historical logs under `dist/verification/issue15-review/` (gitignored) |
| Integration control, missing `NATS_EXECUTABLE` | **Exit 1** | Typing passed; NATS stage failed visibly; no skip |
| Detached-child cleanup control | **Passed** Win/Linux py312 | Timeout termination evidence |

Per-runner platform matrices and checkpoint layouts remain in the specialized
`design/*-verification.md` files linked from [development.md](development.md).

## Deterministic test synchronization (issue #25)

Two scheduling-sensitive tests failed on Windows Python 3.12 before a **test-only**
fix: one redelivery oracle assumed attempt-1 ack timing; one retention test slept
instead of observing completion. The correction cooperatively waits for the expected
ack with a deadline and uses a bounded public result read plus a test-local transport
clock—**no production transport or public API change**.

| Selection | Windows | Linux (WSL) |
| --- | --- | --- |
| Deterministic `not nats and not contract_typing`, py312/313/314 | 334 passed each | 333 passed + one Windows-only skip each |
| Regression including NATS, py312 / py314 | 345 passed each | 344 passed + one Windows-only skip each |

Five dedicated consumer-typing gate tests stay excluded from the regression selection;
CI still runs the installed-wheel typing runner separately.

## Optional #22 / #23 combined record

Client report date **2026-10-03**. Issues [#22](https://github.com/vschroeter/superjobs/issues/22)
and [#23](https://github.com/vschroeter/superjobs/issues/23) merged into `api_design`
after independent review.

| Host/runtime | Result | Local evidence |
| --- | --- | --- |
| Windows, Python 3.12 | All **seven** stages passed; **461** deterministic tests | `dist/verification/issues22-23-final/w312/` |
| WSL Ubuntu 24.04, Python 3.14 | All **seven** stages passed; **460** deterministic tests, two platform skips | `dist/verification/issues22-23-final/l314-corrected/` |

Merged revision for corrected Linux run: `6c617c186b5d45af04c83f2dd1abc03002cd9332`.
Original Linux failure retained at `dist/verification/issues22-23-final/l314/`
(incorrect `D:/nats/store` path assertion on POSIX).

| Optional check | Result |
| --- | --- |
| Reliability repetition seed `20261003`, four families × 3 | **12/12** Windows and Linux py314 (154.2 s / 104.0 s wall) |
| Performance baseline `2026-10-03` high-resolution | **12/12** samples passed at `2f7797f`; **21 late successful completions excluded** from throughput |

Performance artifacts: [performance-baselines/README.md](performance-baselines/README.md).
Harness detail: [design/performance-baseline.md](design/performance-baseline.md).
Repetition detail: [design/reliability-repetition-verification.md](design/reliability-repetition-verification.md).

Issue [#21](https://github.com/vschroeter/superjobs/issues/21) retry-before-ack proof
remains **optional/manual** and outside required PR closure.

## Limitations and open questions

| Topic | State |
| --- | --- |
| Cross-process contract enforcement | **Not implemented** — descriptors/fingerprints/manifest ([#29](https://github.com/vschroeter/superjobs/issues/29), [#30](https://github.com/vschroeter/superjobs/issues/30)) |
| Manifest observation replay under load | [#27](https://github.com/vschroeter/superjobs/issues/27) **closed** as not currently reproducible: no runtime fix, cause, or regression test; retain 15 historical errors (2 consumer closed, 13 expired cursor) in baseline artifacts. The [optional diagnostic runner](design/manifest-replay-investigation.md) preserves evidence for recurrence. |
| Performance baselines | Host-specific measurements only ([#33](https://github.com/vschroeter/superjobs/issues/33)); passing baseline does not disprove intermittent replay symptoms |
| Branch protection / weekly schedule | Documented intent; activation tracked in [#34](https://github.com/vschroeter/superjobs/issues/34) |
| Static typing | Pyright 1.1.414 basic target 3.12 on public fixtures — see [api.md](api.md) |

The original issue #27 tooling review exercised Python 3.12 fast checks plus six
integration stages. It did **not** run the full supported platform/version matrix.

## Diagnostic tooling commit verification — 2026-10-03

The retained issue #27 diagnostic tooling was independently checked on Windows,
CPython 3.12 before committing it:

- Focused replay/performance tests: **60 passed, one POSIX-only skip**, including
  the real-NATS consumer probe.
- `dev_check full`: **all seven stages passed**; deterministic selection:
  **480 passed, one POSIX-only skip, 17 deselected**. Required typing, NATS,
  installed-process and recovery stages passed.
- Local evidence: `dist/verification/issue27-commit-review/` (gitignored).

This verifies the diagnostic tooling and existing gates on that platform/interpreter.
It does not establish a fix for the historical replay failures or a fresh full-order
performance reproduction, and the full supported platform/version matrix was not rerun.

## What required gates do not cover

Optional retry-publication-before-ack ([#21](https://github.com/vschroeter/superjobs/issues/21)),
bounded reliability repetition ([#22](https://github.com/vschroeter/superjobs/issues/22)),
manual performance baselines ([#23](https://github.com/vschroeter/superjobs/issues/23)),
and exhaustive crash-window matrices remain outside required PR closure unless
explicitly promoted in a future decision.
