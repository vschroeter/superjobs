# Developer checks and required CI gates

Implemented for [Add fast and full developer checks with required Linux and Windows CI gates](https://github.com/vschroeter/superjobs/issues/15).

## Exact commands

From the repository root, with a **final CPython** interpreter and (for CI-style runs) [uv](https://docs.astral.sh/uv/) available. PyPy, GraalPython, and prerelease builds are rejected at startup.

### Local (active interpreter, repo `.venv` or equivalent)

Prerequisites: editable or source checkout on `PYTHONPATH`, plus dev dependencies from `pyproject.toml` (`pytest`, `pytest-asyncio`).

```bash
python -m scripts.dev_check fast
python -m scripts.dev_check full
```

The orchestrator always derives the active **final CPython** minor from `sys.version_info` for `verify_* --python` arguments. There is no `--python` override on `dev_check`.

### CI-style isolated environment (matches `.github/workflows/checks.yml`)

Prerequisites on the runner: `actions/setup-python` for the matrix minor, `astral-sh/setup-uv`, repository checkout. No reliance on a committed `.venv`. Each matrix cell passes **`--python <matrix minor>`** to `uv run` so a repo `.python-version` pin does not override the job interpreter.

**Windows and Linux (fast gate), example for Python 3.12:**

```bash
uv run --isolated --no-project --python 3.12 --with-editable . --with "pytest>=9.1.1" --with "pytest-asyncio>=1.4.0" python -m scripts.dev_check fast
```

**Windows and Linux (full check), example for Python 3.12:**

```bash
uv run --isolated --no-project --python 3.12 --with-editable . --with "pytest>=9.1.1" --with "pytest-asyncio>=1.4.0" python -m scripts.dev_check full
```

Replace `3.12` with the matrix minor (`3.13`, `3.14`, …) on other jobs. The documented default example minor is **3.12** (`scripts/dev_check/constants.py`).

`--with-editable .` binds `superjobs` to the worktree `src/` tree. Runtime and typing **wheel** checks inside `tools/verify_*` still use their own non-editable install paths; they are not replaced by this editable orchestration layer.

Orchestration-only regression tests (no broker):

```bash
uv run --isolated --no-project --python 3.12 --with-editable . --with "pytest>=9.1.1" --with "pytest-asyncio>=1.4.0" python -m pytest tests/test_dev_check.py -q
```

- **Fast** runs `pytest -m "not nats and not contract_typing"` only. No broker is required.
- **Full** runs fast stages plus integration stages (contract typing runner, real NATS pytest, cross-program, worker recovery, broker restart). Selecting full makes infrastructure required.
- **Integration** (CI) runs the heavy stages only. Contract typing uses `tools/verify_contract_typing.py` directly; the duplicate `pytest -m contract_typing` gate is not part of this orchestration.

Each integration tool stage passes `--python <current-minor>` from the active interpreter (for example `3.12` on a 3.12 job). Pyright remains pinned at **1.1.414** inside the typing runner. NATS **2.15.0** checksums remain in `tests/support/nats_harness/fixtures/`.

`--artifact-dir` (when set) controls `run-summary.json`, per-stage logs, nested tool evidence, NATS JUnit (`nats-junit.xml`), and `SUPERJOBS_NATS_LOG_DIR` for pytest broker logs.

## Required matrix

| Gate family | Platforms | CPython minors |
| --- | --- | --- |
| `checks / fast` | `ubuntu-latest`, `windows-latest` | 3.12, 3.13, 3.14 |
| `checks / integration` | `ubuntu-latest`, `windows-latest` | 3.12, 3.14 (minimum / latest stable) |

Per-cell jobs are named `checks / fast (<os>, py<minor>)` and `checks / integration (<os>, py<minor>)`. Aggregate jobs **`checks / fast`** and **`checks / integration`** (no matrix suffix) fail unless the corresponding matrix `needs` result is exactly `success` (skipped or cancelled matrix legs fail the aggregate).

Stable minors were checked against [Python 3.14.7](https://www.python.org/downloads/release/python-3147/) and the [2026-08 stable release blog post](https://blog.python.org/2026/08/python-3147-31315/). Python 3.15 is not in required gates.

## Branch protection (documentation only)

Configure required status checks to the **aggregate** names:

- `checks / fast`
- `checks / integration`

Optional: also require individual matrix cells for finer-grained visibility.

Repository protection is not modified by this ticket.

## Safety caps

- Integration workflow job timeout: **15 minutes** (`timeout-minutes: 15`).
- Orchestrator integration stage budget: **855 seconds** (`900` job cap minus **45** seconds reserved for artifact upload/cleanup).
- Existing runners retain **30 second** startup/shutdown and **90 second** recovery scenario caps.

Required stage failures, timeouts, subprocess cleanup errors, launch `OSError` (including permission denied), user interrupts, and invalid NATS JUnit (missing, malformed, zero cases, skips, errors, failures) fail the gate. GitHub Actions does not retry failed jobs.

## Failure evidence

On failure or cancellation, CI uploads `dist/verification/dev-check/**` including:

- `run-summary.json` (always persisted, including timeouts and interrupts)
- Per-stage `stdout.txt`, `stderr.txt`, and `stage.json`
- NATS JUnit `nats-junit.xml` under the run directory
- Tool runner artifacts under `contract-typing/`, `cross-program/`, `worker-recovery/`, and `broker-restart/` (broker logs, checkpoints, typing JSON as produced by existing runners)
- NATS broker logs under `nats-logs/` when pytest stages run

Subprocess stages use descendant cleanup (`taskkill /T` on Windows, depth-first signals on Linux) so owned NATS brokers and detached children do not outlive a timed-out stage.

## Local verification — 2026-10-01

Codex independently ran the committed commands against editable worktree source,
with isolated uv environments. The typing and process runners separately installed
built wheels. The full runs below preceded the final five orchestration regressions;
the final targeted suite was verified separately.

| Check | Result | Notes |
| --- | --- | --- |
| Final `dev_check fast`, Windows CPython 3.12 | **357 passed**, 16 deselected | Includes all 23 orchestration regressions, including actual detached-child termination |
| Final `dev_check fast`, Linux/WSL CPython 3.14 | **356 passed, one Windows-only skip**, 16 deselected | Includes all 23 orchestration regressions; broker-free command passed |
| `dev_check full`, Windows CPython 3.12 | **All six stages passed**, about 81 seconds | Fast tests: 352 passed; typing, NATS, cross-program, worker recovery, broker restart passed |
| `dev_check full`, Linux/WSL CPython 3.14 | **All six stages passed**, about 66 seconds | Fast tests: 351 passed, one Windows-only skip; all required NATS scenarios passed |
| Integration control, Windows CPython 3.12, missing `NATS_EXECUTABLE` | **Exit 1**, required NATS stage failed | Typing passed, then three NATS failures/eight errors; no later stages executed; failure evidence retained |
| External detached-child cleanup control, Windows/Linux CPython 3.12 | **Passed on both platforms** | PID readiness evidence and native process state confirmed child termination after timeout |
| GitHub Actions matrix (six fast/four integration cells plus aggregates) | **Pending** | Local evidence does not establish hosted job results; requires a PR or dispatch |

The scheduling-sensitive execution/retention test oracles exposed during these
checks were fixed separately in [Make completion and retention test synchronization
deterministic on Windows Python 3.12](https://github.com/vschroeter/superjobs/issues/25), and those fixes
were included before the independent full runs. No product guarantee was weakened.

Generated local evidence is under `dist/verification/issue15-review/` in the
verification worktree; it is ignored by Git. CI uploads the selected run directory
on failure. Stress/performance and additional crash windows remain outside the
required PR gates.

Focused iteration without broker or contract typing pytest gate:

```bash
python -m pytest -m "not nats and not contract_typing" -q
```

Do not weaken assertions in existing verification runners when diagnosing orchestration failures.
