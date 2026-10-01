# Worker crash recovery verification

Implemented for [Prove worker crash recovery before and after durable completion](https://github.com/vschroeter/superjobs/issues/13).

## Commands

From the repository root, with Python and uv available:

```bash
uv run python tools/verify_worker_recovery.py --python 3.12 --python 3.14 --artifact-dir dist/verification/issue13/windows-final-composer
uv run python tools/verify_worker_recovery.py --python 3.12 --scenario recovery_after_retry_publication --artifact-dir dist/verification/issue21/retry-publication
uv run pytest tests/test_verify_worker_recovery.py tests/test_worker_recovery_delivery_seam.py tests/test_worker_recovery_producer_closure.py tests/test_worker_recovery_spawn_pid.py -q
```

The runner reuses non-editable wheel builds from `tools/verify_contract_typing.py`, separate
producer and worker virtual environments per Python version, `python -I` bootstrap via
`tools/cross_program_support/isolated_bootstrap.py`, and origin probes from
`tools/wheel_origin_probe.py`. Child role scripts live under `tools/worker_recovery_support/` and
are copied into ephemeral directories outside the checkout.

Each recovery scenario owns a dedicated persistent JetStream broker (`OwnedNatsServer`); worker
replacement reuses the same on-disk JetStream store (FILE storage via the owned broker config)
without restarting the broker. Store byte/file fingerprints in artifacts are diagnostic; the
orchestrator asserts the store **path** identity across replacement and that the broker process,
config path, and config content fingerprint stay unchanged for the scenario lifetime.

## Scenarios

| Scenario | Intent |
| --- | --- |
| `recovery_before_completion` | Hard-kill the first worker after handler entry on the same execution; replacement worker plus a fresh producer `client.get(id)` recover result, outcome, and `JobCompleted` terminal closure. Requires at least one gen-1 and one gen-2 handler invocation for the same execution id (duplicates allowed). Gen-2 must record `replacement_ack` after the real JetStream delivery `ack`. Handler checkpoint validation includes gen-1 `pid` and `worker_generation`. |
| `recovery_after_completion` | Worker-only `BlockingAfterCompletionBackend` awaits the real `write_completion`, writes a `completion_saved` checkpoint (gen-1 pid/generation, `COMPLETED`, `terminal_event_published: false`), then blocks before terminal publication/ack; hard-kill, replacement worker, fresh producer recovery. Exactly one gen-1 handler invocation and no gen-2 handler re-entry after `replacement_ack` and cooperative stop; redelivery is reconciled via terminal delivery ack only. |
| `recovery_after_retry_publication` *(optional, `--scenario`)* | Gen-1 handler fails attempt 1 under `RetryPolicy(max_attempts=2, backoff=0)`. Gen-1 uses worker-only `BlockingAfterRetryPublicationBackend`, which **awaits the real** `NatsJobBackend._publish_retry`, writes `retry_published` with delivery and scheduled retry attempt evidence (delivery attempt captured on the work subscription), then **blocks** so `NatsDelivery.retry` cannot acknowledge the original JetStream delivery until the seam releases (deterministic tests assert `retry_published` is present while `ack_sync` is still unawaited); gen-2 uses the regular backend. Hard-kill, replacement worker, fresh producer recovery with a **30-second** recover-phase cap (`result`, `outcome`, observation reads, and phase timeout; baseline scenarios keep **45-second** recover waits). Producer recovery awaits `result` before asserting public `COMPLETED` status, then proves **strict** observation closure: monotonically increasing sequences, terminal `JobCompleted`, and cursor replay after the first retained observation (including the terminal-only case, where replay must be empty rather than skipping cursor proof). Requires at least one gen-1 and one gen-2 handler invocation (duplicates allowed). Not part of the default required scenario pair. |

Readiness markers include `run_id`, worker `pid` (from the running worker process), and
`worker_generation` so a stale gen-1 marker cannot satisfy gen-2 startup (generation mismatch and
`forbidden_pid` guard). The orchestrator records PIDs from readiness markers and verifies each
marker pid lies in the spawn tree rooted at `Popen.pid` (Windows venv launchers may differ from
`os.getpid()` via `tools/worker_recovery_support/spawn_pid.py`). Per-generation handler checkpoints
use distinct names (`handler_entered_g1`, `handler_entered_g2`). The parent validates checkpoints
and invocation records against spawned PIDs and the submitted execution id.

Before clearing gen-1 `worker_ready.json`, the orchestrator copies an immutable per-generation
snapshot (ready, `submitted`, and `completion_saved` when present) into scenario JSON under
`generation_snapshots`.

`replacement_ack` proof requires `terminal_state: COMPLETED`, `terminal_event_published: true`, and
matching gen-2 pid / execution id — not merely any terminal marker.

Checkpoints and handler-invocation evidence use atomic JSON writes under
`SUPERJOBS_CROSS_STATE_DIR`, keyed by per-run `run_id`. The parent orchestrator confirms
checkpoints before `process.kill()` and verifies the expected kill exit (`1` on Windows,
`-SIGKILL` on Linux). On Windows, a saved native handle independently confirms that the worker
interpreter terminated, even when its PID differs from the virtual-environment launcher.
The handle is opened only after checking the interpreter belongs to the owned process tree;
an interpreter surviving launcher termination is cleaned up and fails the proof. Kill evidence
records both PIDs and termination timings.

The scenario cap is **90 seconds**, with **30-second** child startup/shutdown caps.
All checkpoint waits reserve **5 seconds** (`KILL_REAP_SECONDS`) for child
cleanup at the scenario deadline and pass the live worker `Popen` into checkpoint waits so early
worker death fails fast.

Wheel/environment setup and initial NATS binary provisioning run before the scenario budgets.
The deadline is taken before native broker startup. The orchestrator reserves the harness's
**30-second** broker shutdown budget inside the **90-second** cap, leaving at most **60 seconds**
for broker startup and scenario work, including the **5-second** child cleanup reserve.

Diagnostics (commands, cwd, PIDs, start/end times, exit codes, checkpoints, invocations, child logs,
broker pid/config fingerprints, broker log) are written under `--artifact-dir` on success and retained on failure
by default. Per-Python `summary.json` and per-scenario `summary-<scenario>.json` files are emitted
from a `finally` block covering setup, origin probes, and scenarios, with a non-empty `error` string
(including bare exception types). Scenario summaries include `scenario_error` when broker startup or
scenario orchestration fails before a scenario record exists; full broker logs are retained on cleanup
failures.

Deterministic orchestration regressions live in `tests/test_verify_worker_recovery.py` with tiny
stdlib scripts under `tests/support/worker_recovery_regression/`. Delivery ack ordering, blocked
ack gates, completion-write ordering, and spawn/ready pid relationship checks live in
`tests/test_worker_recovery_delivery_seam.py` and `tests/test_worker_recovery_spawn_pid.py` without
weakening NATS integration proof.

This slice does not cover broker restart (#14) or required CI orchestration (#15). A failing
oracle against the selected guarantees should remain visible for a linked product fix rather than
being weakened.

## Independent measurements — 2026-10-01

| Check | Windows | Linux (WSL Ubuntu 24.04) |
| --- | --- | --- |
| Minimum runtime | Python 3.12.11: both recovery scenarios passed | Python 3.12.3: both recovery scenarios passed |
| Latest runtime | Python 3.14.7: both recovery scenarios passed | Python 3.14.7: both recovery scenarios passed |
| Regression suite excluding `contract_typing` | 317 passed, 5 deselected | 317 passed, 5 deselected |

Independent artifacts: `dist/verification/issue13/windows-independent` and
`dist/verification/issue13/linux-independent`. The 29 focused recovery tests also passed.
An intentionally missing `NATS_EXECUTABLE` produced runner exit `1` with a retained
`run_error.json` under `dist/verification/issue13/missing-broker-control`.

Commands (exit code **0** on the measured run):

```bash
uv run python tools/verify_worker_recovery.py --python 3.12 --python 3.14 --artifact-dir dist/verification/issue13/windows-independent
uv run pytest tests/test_verify_worker_recovery.py tests/test_worker_recovery_delivery_seam.py tests/test_worker_recovery_spawn_pid.py -q
```

Scenario artifacts include gen-1 `generation_snapshots`, broker config fingerprints unchanged across worker replacement, and gen-2 `checkpoint_replacement_ack.json` with `terminal_state: COMPLETED` and `terminal_event_published: true`.

Scenario A persisted invocations from both worker generations for the same execution;
scenario B persisted exactly one gen-1 invocation and no gen-2 handler entry after replacement
acknowledgement and shutdown. Both fresh producers recovered the expected result, success
outcome and terminal observation closure through public handles.

Composer 2.5 implemented the slice with three review/correction passes. Codex independently
reviewed and verified it, then directly completed the interpreter-termination proof, Windows
API signatures, shutdown-budget reservation and explicit ordering signals in tests. These
measurements concern the working tree. No production defect or library change was needed;
broker restart and required CI remain in #14 and #15.

## Independent measurements — issue21 retry publication (2026-10-01)

| Check | Windows | Linux (WSL Ubuntu 24.04) |
| --- | --- | --- |
| Installed-wheel runner | Python 3.12: optional `recovery_after_retry_publication` passed | Python 3.12: optional scenario passed |
| Scenario / recover caps | 90-second scenario budget; **30-second** fresh-producer recover phase | Same |
| External retry checkpoint | `retry_published` observed before hard-kill | Same |
| Kill evidence | Interpreter **5548** terminated; venv launcher exit **1** | Interpreter **45334** exit **-9** |
| Recovered closure | `result`, public `COMPLETED` status, `JobSucceeded` outcome, strict observation cursor replay | Same |

Duplicate handler invocations across generations are allowed for this scenario; measurements do
not indicate a production defect or library change.

Artifacts: `dist/verification/issue21/windows-final` and
`dist/verification/issue21/linux-final`.

```bash
uv run python tools/verify_worker_recovery.py --python 3.12 --scenario recovery_after_retry_publication --artifact-dir dist/verification/issue21/windows-final
uv run pytest tests/test_verify_worker_recovery.py tests/test_worker_recovery_delivery_seam.py tests/test_worker_recovery_producer_closure.py tests/test_worker_recovery_spawn_pid.py -q
```
