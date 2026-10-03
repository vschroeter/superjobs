# Development checks and CI

Canonical commands for local work and required PR gates on branch **`api_design`**
at **`04f24f2`**. Measured outcomes: [verification.md](verification.md).
Architecture context: [adr/](adr/README.md).

## Quick start

Prerequisites: final CPython interpreter, repository checkout with dev dependencies
(`uv sync --dev` or equivalent `pytest` / `pytest-asyncio` from `pyproject.toml`).

| Goal | Command |
| --- | --- |
| Routine fast checks (no broker) | `python -m scripts.dev_check fast` |
| Full verification (typing + NATS + installed processes) | `python -m scripts.dev_check full` |
| NATS pytest only | `uv run pytest -m nats` |
| Contract typing gate | `python tools/verify_contract_typing.py --python 3.12 --python 3.14` |

`dev_check` derives the active **final CPython** minor from `sys.version_info` for
`verify_* --python` arguments. There is no `--python` override on `dev_check`. PyPy,
GraalPython, and prerelease builds are rejected.

### CI-style isolated run (example Python 3.12)

```bash
uv run --isolated --no-project --python 3.12 --with-editable ".[cli]" --with "pytest>=9.1.1" --with "pytest-asyncio>=1.4.0" python -m scripts.dev_check fast
```

```bash
uv run --isolated --no-project --python 3.12 --with-editable ".[cli]" --with "pytest>=9.1.1" --with "pytest-asyncio>=1.4.0" python -m scripts.dev_check full
```

Replace `3.12` with the matrix minor (`3.13`, `3.14`) on other jobs. Each matrix
cell passes **`--python <matrix minor>`** to `uv run` so a repo `.python-version`
pin does not override the job interpreter.

`--with-editable .` binds `superjobs` to the worktree `src/` tree. Runtime and
typing **wheel** checks inside `tools/verify_*` still use their own non-editable
install paths; they are not replaced by this editable orchestration layer.

Orchestration-only regression tests (no broker):

```bash
uv run --isolated --no-project --python 3.12 --with-editable ".[cli]" --with "pytest>=9.1.1" --with "pytest-asyncio>=1.4.0" python -m pytest tests/test_dev_check.py -q
```

Focused iteration without broker or contract typing pytest marker:

```bash
python -m pytest -m "not nats and not contract_typing" -q
```

- **Fast** runs `pytest -m "not nats and not contract_typing"` only. No broker required.
- **Full** runs fast stages plus integration (contract typing runner, real NATS pytest,
  cross-program, worker recovery, broker restart, idle outage).
- **Integration** workflow jobs run heavy stages only. Contract typing uses
  `tools/verify_contract_typing.py` directly; duplicate `pytest -m contract_typing`
  is not part of orchestration.

Each integration tool stage passes `--python <current-minor>` from the active
interpreter. Pyright remains pinned at **1.1.414** inside the typing runner. NATS
**2.15.0** checksums remain in `tests/support/nats_harness/fixtures/`.

`--artifact-dir` (when set) controls `run-summary.json`, per-stage logs, nested tool
evidence, NATS JUnit (`nats-junit.xml`), and `SUPERJOBS_NATS_LOG_DIR` for pytest
broker logs.

## Contract typing gate isolation

`tools/verify_contract_typing.py` exercises **source-tree** and **installed-wheel**
consumer fixtures separately. Positive projects require **zero errors and warnings**.
Negative fixtures declare expected rules at each misuse line; the runner compares
file/line/rule multisets and rejects missing or unexpected diagnostics, warnings,
and malformed checker output. Counts alone are insufficient. Wheel checks use fresh,
non-editable installations without source `extraPaths`, inherited `PYTHONPATH`, or
user-site fallback, and verify package origins and `py.typed` markers. Review fixture
intent whenever expected diagnostics change, including after API changes or a checker
upgrade; preserve the rejection being tested.

## Required matrix

| Gate family | Platforms | CPython minors |
| --- | --- | --- |
| `checks / fast` | `ubuntu-latest`, `windows-latest` | 3.12, 3.13, 3.14 |
| `checks / integration` | `ubuntu-latest`, `windows-latest` | 3.12, 3.14 |

Per-cell jobs: `checks / fast (<os>, py<minor>)` and
`checks / integration (<os>, py<minor>)`. Aggregate jobs **`checks / fast`** and
**`checks / integration`** fail unless every matrix leg succeeds (skipped or
cancelled legs fail the aggregate).

Aggregate required check names (branch protection intent, not yet activated on
`api_design` — [#34](https://github.com/vschroeter/superjobs/issues/34)):

- `checks / fast`
- `checks / integration`

Hosted reference: [Actions run 37127882378](https://github.com/vschroeter/superjobs/actions/runs/37127882378)
at **`04f24f2`** — six fast cells, four integration cells, both aggregates passed.

Stable minors were checked against [Python 3.14.7](https://www.python.org/downloads/release/python-3147/)
and the [2026-08 stable release blog post](https://blog.python.org/2026/08/python-3147-31315/).

## `dev_check full` stages (seven)

1. Deterministic pytest (`not nats and not contract_typing`)
2. Contract typing (`tools/verify_contract_typing.py`)
3. Real NATS pytest (`pytest -m nats`)
4. Installed cross-program (`tools/verify_cross_program.py`)
5. Worker crash recovery (`tools/verify_worker_recovery.py`)
6. Broker persistent-store restart (`tools/verify_broker_restart.py`)
7. Idle outage reconnect (`tools/verify_idle_outage.py`)

Fast mode runs stage 1 only. Integration workflow jobs use a **15 minute** timeout;
orchestrator integration budget **855 s** (job cap minus **45 s** upload reserve).
Recovery scenario caps remain **90 s**; process startup/shutdown **30 s**.

Required stage failures, timeouts, subprocess cleanup errors, launch `OSError`
(including permission denied), user interrupts, and invalid NATS JUnit (missing,
malformed, zero cases, skips, errors, failures) fail the gate. GitHub Actions does
not retry failed jobs.

## Failure evidence and cleanup

On failure or cancellation, CI uploads `dist/verification/dev-check/**` including:

- `run-summary.json` (always persisted, including timeouts and interrupts)
- Per-stage `stdout.txt`, `stderr.txt`, and `stage.json`
- NATS JUnit `nats-junit.xml` under the run directory
- Tool runner artifacts under `contract-typing/`, `cross-program/`,
  `worker-recovery/`, `broker-restart/`, and `idle-outage/`
- NATS broker logs under `nats-logs/` when pytest stages run

Subprocess stages use descendant cleanup (`taskkill /T` on Windows, depth-first
signals on Linux) so owned NATS brokers and detached children do not outlive a timed-out
stage.

Do not weaken assertions in existing verification tools when debugging orchestration.

## Specialized runner documentation

Procedural detail, checkpoints, and failure evidence layouts:

| Stage | Document |
| --- | --- |
| Owned JetStream harness | [design/nats-test-harness.md](design/nats-test-harness.md) |
| Separate producer/worker installs | [design/cross-program-verification.md](design/cross-program-verification.md) |
| Worker kill before/after completion | [design/worker-recovery-verification.md](design/worker-recovery-verification.md) |
| Broker restart with same store | [design/broker-restart-verification.md](design/broker-restart-verification.md) |
| Idle processes after short outage | [design/idle-outage-verification.md](design/idle-outage-verification.md) |

Example installed-process commands (match matrix minors):

```bash
uv run python tools/verify_cross_program.py --python 3.12 --python 3.14 --artifact-dir dist/verification/issue12
uv run python tools/verify_worker_recovery.py --python 3.12 --python 3.14 --artifact-dir dist/verification/issue13
uv run python tools/verify_broker_restart.py --python 3.12 --python 3.14 --artifact-dir dist/verification/issue14
uv run python tools/verify_idle_outage.py --python 3.12 --python 3.14 --artifact-dir dist/verification/issue20
```

Contract typing and the contract-interface example:
[examples/contract_interface/README.md](../examples/contract_interface/README.md).

## Optional manual checks (not PR gates)

| Runner | Document |
| --- | --- |
| `tools/verify_reliability_repetition.py` | [design/reliability-repetition-verification.md](design/reliability-repetition-verification.md) |
| `tools/verify_performance.py` | [design/performance-baseline.md](design/performance-baseline.md) |
| `tools/verify_manifest_replay_repro.py` | [design/manifest-replay-investigation.md](design/manifest-replay-investigation.md) |

Weekly repetition workflow: `.github/workflows/reliability-repetition.yml` on
`api_design`; GitHub `schedule` runs only when that file exists on the default
branch (`main`).

## Guarantee intent (selected)

The matrix proves, among other things: public contracts survive wheel distribution;
installed producer/worker separation; ordinary request/result/event shapes;
worker crash recovery before and after durable completion; broker restart with
persistent store; short idle outage reconnect. It does **not** claim exactly-once
execution, preservation of all intermediate events across worker loss, or
automatic resubmission after outages. See [ADR 0004](adr/0004-test-foundation-and-bounded-recovery.md).
