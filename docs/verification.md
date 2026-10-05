# Verification summary

Dated measured results. Commands and matrix: [development.md](development.md).
This file records **what was run**, not product SLOs.

## Unified catalog, CLI metadata, and worker lifetime (2026-10-05)

Independent local verification of the implementation for issues
[#43](https://github.com/vschroeter/superjobs/issues/43)–
[#47](https://github.com/vschroeter/superjobs/issues/47), on `feature/cli` based
on `23b4419`. Cursor Composer 2.5 implemented bounded slices; Codex reviewed
them and corrected lifecycle failures after repeated unsuccessful Cursor passes.

| Check | Measured result |
| --- | --- |
| `dev_check fast`, Windows × Python 3.12 / 3.13 / 3.14 | All three passed; 772 passed, 2 platform/optional skips per cell |
| `dev_check fast`, Linux × Python 3.12 / 3.13 / 3.14 | All three passed; 771 passed, 3 platform/optional skips per cell |
| `dev_check integration`, Windows/Linux × Python 3.12 / 3.14 | All four passed all seven required stages |
| Public source/wheel typing, pinned Pyright 1.1.414 | Positive suites passed; exact negative diagnostic sets matched (catalog: 9 expected errors) |
| Installed public-import runtime tests | Windows: 300 passed per runtime; Linux: 301 passed per runtime; origins verified |
| Required real NATS pytest | 20 passed per integration cell, no skips |
| Installed CLI / separate worker | 23 scenarios passed per integration cell; catalog providers, URL override, cooperative serving stop, and process interrupt included |
| Changed implementation static check | 25 files, zero errors and warnings |
| Broken required NATS control | Missing `NATS_EXECUTABLE`: exit 1, visible setup error, no skip |

The integration stages also passed installed cross-program execution, worker
recovery, broker persistent-store restart, and idle outage reconnect. Evidence
is under the gitignored `dist/implementation/unified-api/` directory:
`windows-fast-*`, `linux-fast-*`, `windows-integration-*`,
`linux-integration-*`, `implementation-typing-final.json`, and
`broken-nats-control.log`. This is local Windows and WSL Linux evidence;
no new hosted CI run or package publication is claimed.

The subsequent review tightened the metadata mutation test to use its original
dictionary; all 13 catalog tests passed. Documentation and example imports use
the public API. Runtime serving does not install global signal handlers, and
arbitrary cancellation-resistant Python callbacks still require cooperation.

## Gate shape: historical vs current

| Era | Branch / revision | `dev_check full` | `dev_check integration` | Hosted CI |
| --- | --- | --- | --- | --- |
| **Historical** | `api_design` @ `04f24f2` | **Seven** stages (no CLI process proof) | **Six** stages | [Run 37127882378](https://github.com/vschroeter/superjobs/actions/runs/37127882378): fast + integration aggregates passed |
| **Current (CLI)** | `feature/cli` @ `fd9dd0a` | **Eight** stages (adds installed CLI) | **Seven** stages | No CLI PR / hosted run; workflow push trigger remains `main` / `api_design` only |

Counts in older sections below are **historical** unless labeled current CLI.

## Required CI at `04f24f2` (historical)

| Check | Result |
| --- | --- |
| GitHub Actions `checks / fast` + `checks / integration` | **Passed** — [run 37127882378](https://github.com/vschroeter/superjobs/actions/runs/37127882378): 6 fast matrix cells (Windows/Linux × py312/313/314), 4 integration cells (Windows/Linux × py312/314), both aggregates |
| `dev_check full` seven stages | Covered by **fast** matrix jobs (stage 1) **plus** **integration** matrix jobs (stages 2–7); `dev_check integration` alone ran **six** stages (no CLI) |

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

## CLI slices on `feature/cli` (issues #38–#42, closed)

Local verification only unless noted. Issues [#38](https://github.com/vschroeter/superjobs/issues/38)–[#42](https://github.com/vschroeter/superjobs/issues/42) are **closed**; parent [#37](https://github.com/vschroeter/superjobs/issues/37) remains open. Implementation landed at `fd9dd0a`. User-facing behavior: [cli.md](cli.md). Installed proof: [design/cli-application.md](design/cli-application.md).

### #38 — registration shell (2026-10-03)

Typer `JobCLI`, `run`/`submit` groups, mount snapshots, exit codes, 19 CLI negative
diagnostics at `add()` (later slices add more). Evidence: `dist/issue38/` (gitignored).
Representative: `dev_check fast` 523/522 passed (Win/Linux py312/314); 67 installed
public runtime tests per cell; Typer minimum raised to 0.27.2 after 0.15.1 probe failures.

### #39 — request input (2026-10-03)

`--json` / `--input`, generated scalar options, `CLIField` / positionals, JSON-only
fallback. Evidence: `dist/issue39/`. Representative: `dev_check fast` 616/615 passed;
160 installed public runtime tests per cell; 23 CLI misuse diagnostics; 136 focused CLI tests.

### #40 — local `run` (2026-10-04)

In-memory isolated runtime, interrupt/shutdown bounds (`LOCAL_RUN_SHUTDOWN_TIMEOUT_SECONDS` 30s grace + 30s cleanup). Evidence: `dist/issue40/final-*`. Representative: full deterministic gate 664 passed (py312/313/314); 184–185 focused CLI tests; 208–209 installed public runtime tests per platform/interpreter.

### #41 — remote `submit` (2026-10-04)

Owned NATS producer, `--wait` / default 300s client bound, 2s observation drain cap,
30s startup/submission/teardown phase bounds. Evidence: `dist/issue41/final-*`.
Representative (Windows): deterministic py313 700 passed; 45 remote suite tests
(8 real NATS); 20 owned-NATS tests; 245 installed public runtime tests per py312/314.
A full deterministic gate run preceded the final broker **startup rollback**
safeguard; focused remote tests and the installed CLI matrix also cover teardown
when the yielded runtime never reached `started`.

### #42 — installed application matrix (2026-10-04, current)

Independent local matrix on Windows and Linux (WSL), Python 3.12 and 3.14, using
`dev_check integration` with `tools/verify_cli_process.py` (18 scenarios per cell).
Evidence: `dist/issue42/final-matrix-*`, fast console `dist/issue42/final-windows-fast-console/`.

| Check | Windows py312 | Windows py314 | Linux py312 | Linux py314 |
| --- | --- | --- | --- | --- |
| Integration stages | 7 passed | 7 passed | 7 passed | 7 passed |
| CLI scenarios | 18 | 18 | 18 | 18 |
| Installed public runtime | 247 passed | 247 passed | 248 passed | 248 passed |
| Required real NATS pytest | 20 passed | 20 passed | 20 passed | 20 passed |
| Source/wheel typing | **zero errors** on positives; matching negatives incl. **27** CLI diagnostics | same | same | same |

Additional recorded checks (not rerun for documentation): Windows py313 fast gate
**719 passed**, 2 platform skips; missing `NATS_EXECUTABLE` control exits 1; verifier
unit tests 16 passed on Windows. CLI negative typing suite totals **27** diagnostics
on current `feature/cli`.
