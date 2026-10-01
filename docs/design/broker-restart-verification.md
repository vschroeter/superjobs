# Broker restart verification

Implemented for [Prove execution and result persistence across a NATS restart](https://github.com/vschroeter/superjobs/issues/14).

## Commands

From the repository root, with Python and uv available:

```bash
uv run python tools/verify_broker_restart.py --python 3.12 --python 3.14 --artifact-dir dist/verification/issue14/windows-smoke
uv run python tools/verify_broker_restart.py --empty-store-control --python 3.12 --artifact-dir dist/verification/issue14/empty-store-control
uv run pytest tests/test_verify_broker_restart.py -q
```

When WSL shares the Windows checkout, use a separate parent environment:

```bash
uv run --no-project --python 3.12 --with . python tools/verify_broker_restart.py --python 3.12 --python 3.14 --artifact-dir dist/verification/issue14/linux-matrix
```

The runner reuses non-editable wheel builds from `tools/verify_contract_typing.py`, separate
producer and worker virtual environments per Python version, `python -I` bootstrap via
`tools/cross_program_support/isolated_bootstrap.py`, and origin probes from
`tools/wheel_origin_probe.py`. Child role scripts live under `tools/broker_restart_support/` and
are copied into ephemeral directories outside the checkout.

Each scenario owns a dedicated persistent JetStream broker (`OwnedNatsServer`). The orchestrator
pauses and restarts the native server while reusing the same on-disk store and client port.
Before restart it records broker PID, port, config path/content fingerprint, store byte/file
fingerprints, and JetStream resource evidence (work and observation streams with **FILE**
storage, completion and idempotency KV backing streams with FILE storage and value counts). Work-stream messages are
sampled non-destructively via JetStream `get_msg` (decoded `job_id`) to prove the pending
execution identity survives restart without resubmission. An empty or memory-only store
substitution fails the proof via resource oracles; `--empty-store-control` exercises the same
post-restart resource oracle after an authorized pause and isolated owned-store rename.

## Scenario

| Step | Intent |
| --- | --- |
| `broker_restart_persistence` | Gen-1 worker completes one execution (exactly one handler invocation). Worker shuts down cooperatively. A fresh producer submits a second execution while no worker is running (public `submit` + non-terminal `status` + checkpoint). All application processes exit before `OwnedNatsServer.restart()`. A fresh producer recovers the completed handle (`get`, typed `ManifestResult`, `JobSucceeded`, `JobCompleted` terminal observation) without resubmission. Gen-2 worker starts; a fresh producer waits for the retained pending execution on its original identity. Completed handler never reruns; pending runs only after restart. |

Readiness markers include `run_id`, worker `pid`, and `worker_generation`. The orchestrator
verifies each marker pid lies in the spawn tree rooted at `Popen.pid` (Windows venv launchers via
`tools/worker_recovery_support/spawn_pid.py`). Handler invocations persist
`run_id`, `job`, `execution_id`, `pid`, and `worker_generation` under
`SUPERJOBS_CROSS_STATE_DIR` and are asserted against the exact ready-marker PIDs after the final
worker shutdown (no unknown generations, no completed gen-2 invocation).

The scenario cap is **90 seconds**, with **30-second** child startup/shutdown/producer-phase caps
and the harness **30-second** broker shutdown reserved inside the cap (same budgeting model as
worker recovery #13). Wheel/environment setup, origin probes, and initial NATS binary
provisioning run outside the scenario timer, which starts immediately before `OwnedNatsServer.start()`.
Broker restart uses a bounded pause+launch budget (30+30 seconds) inside the scenario deadline.

Diagnostics are written under `--artifact-dir` on success and retained on failure by default.
Per-Python `summary.json` includes `scenario_error` when setup or broker startup fails before a
scenario record exists.

Deterministic orchestration regressions live in `tests/test_verify_broker_restart.py` with tiny
stdlib scripts under `tests/support/broker_restart_regression/`. Resource oracles in
`tools/broker_restart_support/broker_resources.py` are covered by unit tests; the native
`test_empty_store_control_native` (`@pytest.mark.nats`) runs the CLI `--empty-store-control`
path end-to-end.

This slice does not cover uninterrupted application reconnection during outage (#20), required CI
orchestration (#15), or power-loss/fsync guarantees. A failing guarantee should remain visible
for a linked product fix rather than being weakened.

## Independent verification (2026-10-01)

Composer 2.5 implemented three bounded passes. Codex independently reviewed and corrected
process-reference retention and original errors after failed cleanup, absolute interpreter-marker paths, the
versioned pending-work subject, short Windows checkpoint paths, and invocation Job names.
No production library change was required.

| Check | Measured result |
| --- | --- |
| Windows Python 3.12.11 and 3.14.7 | Persistence scenario passed for both |
| Linux / WSL Ubuntu Python 3.12.3 and 3.14.7 | Persistence scenario passed for both |
| Real empty-store control, both platforms and both runtimes | Expected missing persisted work stream detected before fresh applications started |
| Focused runner and harness checks, Windows Python 3.12 | 37 passed |
| Focused runner and harness checks, Linux Python 3.12 | 36 passed, one Windows-only process-launcher check skipped |
| Broader regression suite, Windows Python 3.14 | 344 passed, five typing-gate tests deselected |
| Final broader regression suite, restored Windows Python 3.13.5 | 345 passed, five typing-gate tests deselected |
| Final broader regression suite, Linux Python 3.12 | 344 passed, one Windows-only check skipped, five typing-gate tests deselected |
| Static source/wheel consumer gate and failure controls | Five typing-gate tests passed on each platform, including installed consumers on Python 3.12 and 3.14 |
| Missing native server override | Exit 1 with retained `run_error.json` |

The final persistence evidence is in `dist/verification/issue14/windows-final` and
`dist/verification/issue14/linux-final`. Each scenario recorded two invocations: one completed
execution on generation 1 and the original pending execution on generation 2. All application
processes exited successfully; broker PID changed while the client port and store path remained
stable. All four resource configurations reported FILE storage. The empty-store evidence is in
`windows-empty-independent` / `linux-empty-independent` under the same artifact root.

Windows Python 3.12's broader suite had 342 passes and two failures in existing tests using
10 ms sleeps for redelivery acknowledgement and result expiry. Both failed again in isolation,
and both passed on Windows Python 3.14. These are tracked in
[Make completion and retention test synchronization deterministic on Windows Python 3.12](https://github.com/vschroeter/superjobs/issues/25).
This is an unresolved verification gap; it does not weaken the persistence oracles.

The shared `.venv` was changed to a Linux environment during a Composer run. It was preserved
under the ignored artifact directory and the Windows project environment was restored.
Independent Linux checks used `uv run --no-project --python 3.12 --with .` with pytest
and pytest-asyncio added for test runs, avoiding changes to the Windows environment.
Typing contracts and static consumer examples were unchanged in this slice; the existing
typing gate passed independently on both platforms.
