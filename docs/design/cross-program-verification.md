# Cross-program installed-wheel verification

Implemented for [Verify installed producer and worker contracts in separate NATS processes](https://github.com/vschroeter/superjobs/issues/12).

## Commands

From the repository root, with Python and uv available:

```bash
uv run python tools/verify_cross_program.py --python 3.12 --python 3.14 --artifact-dir dist/verification/issue12
uv run pytest tests/test_verify_cross_program.py -q
uv run pytest -m "not nats and not contract_typing" -q
```

The runner builds non-editable `superjobs` and `superjobs_contract_example` wheels into a
temporary directory outside the checkout, installs them into **separate** producer and worker
virtual environments per Python version, and provisions an **owned** JetStream broker through
`tests.support.nats_harness.OwnedNatsServer` (never an external-only broker for this gate).

Child processes run with `PYTHONNOUSERSITE=1`, without inherited `PYTHONPATH` / `PYTHONHOME`,
and under `python -I` via `tools/cross_program_support/isolated_bootstrap.py`, which inserts
**only** the ephemeral copied role directory (outside the repository) for helper imports.
There is no `PYTHONPATH` fallback.

Origin probes (`tools/wheel_origin_probe.py`) run in both producer and worker venvs and
assert `site-packages` import origins, `py.typed`, non-editable wheel metadata, and role
layout (producer cannot import `worker_handlers`; worker can). Producer and worker entry
scripts repeat those layout checks at runtime inside the live child.

## Scenarios

| Scenario | Intent |
| --- | --- |
| `contracts_ok` | Four shared contract shapes, ordered application events where applicable, success outcomes, `JobCompleted` terminal closure, no application events on eventless shapes |
| `outcome_failure` | Terminal failure with `invalid_result` code and `JobFailed` closure |
| `outcome_cancel` | Cooperative cancellation after handler-entry checkpoint |
| `outcome_retry` | Retry after handler entry; `JobRetryScheduled`, attempt identity, success outcome |
| `control_missing_readiness` | Producer readiness wait timeout only (exit `2`) |
| `control_worker_crash` | Worker exits `42` after handler-entry checkpoint; producer observes pending/running then result wait timeout (exit `4`) |
| `control_malformed_payload` | Strict payload rejection inside intentional invalid submit only (exit `10`) |

Readiness, cooperative worker stop, and handler-entry checkpoints are JSON files keyed by a
per-run `run_id` under `SUPERJOBS_CROSS_STATE_DIR`. Handler checkpoints also record the
current execution (`execution_id`). Protocol writes are atomic (write-temp-then-replace). Success
is not inferred from arbitrary sleeps.

Normal worker shutdown is cooperative: the runner publishes a per-run stop marker; the worker
exits via `jobs.stop()` in a `finally` block and must return `0`. The runner detects
unexpected worker exits while waiting for readiness and does not accept arbitrary worker exit
codes on success paths. Child stdout/stderr stream to per-scenario log files to avoid pipe
blocking.

Safety caps match the test strategy: **30 seconds** for child startup/shutdown bounds and
**90 seconds** per scenario, with up to **5 seconds** reserved inside each deadline for
kill/reap after cooperative waits expire. Owned broker and child processes are cleaned up on success,
failure, and `KeyboardInterrupt`. Scenario diagnostics (per-child command/cwd/PID/timings, exit codes,
checkpoints including corrupt marker diagnostics, child logs, and the full broker log after scenarios/shutdown) are written even
when `--artifact-dir` is omitted (default artifact directory under the system temp dir, kept
on failure). Run-level failures (including wheel builds) also write `run_error.json` with
parsed subprocess stdout/stderr when available.

Deterministic child-process regressions live in `tests/test_verify_cross_program.py` with tiny
stdlib role scripts under `tests/support/cross_program_regression/`; they call `_run_scenario`
directly without NATS.

Default Python matrix: **3.12** and **3.14** (same stable series as
`tools/verify_contract_typing.py`). This slice does not cover worker crash recovery (#13),
broker restart (#14), or CI orchestration (#15).

## Limitations

- Scenarios reuse one fresh owned broker per Python version pass. Outcome/control Job names
  include a unique run id; the four shared contract names are fixed and run only once per broker.
- `control_worker_crash` expects the producer to observe incomplete execution after the worker
  hard-exits; it does not prove replacement-worker recovery (#13).
- Full cross-program gates require NATS binary provisioning (pinned **2.15.0**) and network on
  first download; missing infrastructure fails the runner rather than skipping.
- Windows child processes use `CREATE_NO_WINDOW`; the gate is validated on Windows and Unix-like
  hosts, but CI matrix coverage may lag local development platforms.

## Independent measurements — 2026-10-01

| Check | Windows | Linux (WSL Ubuntu 24.04) |
| --- | --- | --- |
| Cross-program minimum runtime | Python 3.12.11: seven scenarios passed | Python 3.12.3: seven scenarios passed |
| Cross-program latest runtime | Python 3.14.7: seven scenarios passed | Python 3.14.7: seven scenarios passed |
| Regression suite excluding `contract_typing` | 288 passed, 5 deselected | 288 passed, 5 deselected |
| Source/wheel typing | Matching exact diagnostics; all positives clean | Matching exact diagnostics; all positives clean |
| Installed consumer checks | 24 passed on each runtime | 24 passed on each runtime |
| Explicit typing failure controls and complete gate | 5 passed | 5 passed |

Each cross-program pass asserted producer exits `0` for ordinary behavior, `2` for missing
readiness, `4` for the crash control and `10` for invalid payload rejection. Normal workers
exited `0` after cooperative stop; the intentional crash worker exited `42`. Both role origins
and typing markers were verified. The invalid-payload control exercises public submit
validation, not malformed transport bytes.

The 20 focused helper tests include real child-process failure/cleanup controls, interrupt
handling and a broker cleanup failure that must fail the pass. An independently invoked
runner with a nonexistent `NATS_EXECUTABLE` returned `1` and retained its error and pass summary.

Final cross-program artifacts are under `dist/verification/issue12/windows-final` and
`linux-final`; typing artifacts are under `typing-windows` and `typing-linux`. These generated
files are not tracked. Composer 2.5 implemented the slice with three review/correction passes;
Codex independently reviewed and verified it and directly corrected two remaining failure
paths (empty interrupt errors and suppressed broker cleanup errors). Measurements cover the
working tree, not a required CI gate; CI orchestration remains in #15.
