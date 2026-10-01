# Idle outage verification

Implemented for [Prove idle producer and worker recovery after a short NATS outage](https://github.com/vschroeter/superjobs/issues/20).

## Commands

From the repository root, with Python and uv available:

```bash
uv run python tools/verify_idle_outage.py --python 3.12 --python 3.14 --artifact-dir dist/verification/issue20
uv run pytest tests/test_verify_idle_outage.py tests/test_nats_work_pull.py tests/test_idle_outage_producer_read.py -q
```

The runner reuses non-editable wheel builds from `tools/verify_contract_typing.py`, separate
producer and worker virtual environments per Python version, `python -I` bootstrap via
`tools/cross_program_support/isolated_bootstrap.py`, and origin probes from
`tools/wheel_origin_probe.py`. Child role scripts live under `tools/idle_outage_support/` and
are copied into ephemeral directories outside the checkout.

Each scenario owns a dedicated persistent JetStream broker (`OwnedNatsServer`). The orchestrator
pauses and restarts the native server while reusing the same on-disk store and client port.
**Producer and worker OS processes stay alive** across the outage; reconnect is observed through
`NatsBroker` `disconnected_cb` / `reconnected_cb` checkpoints written by each role. Installed
children set an explicit bounded `NatsBroker` reconnect budget (`tools/idle_outage_support/broker_connect.py`)
and record it in `broker_reconnect_settings` checkpoints.

## Scenario

| Step | Intent |
| --- | --- |
| `idle_outage_recovery` | Start one installed worker and one installed producer with long-lived `SuperJobs` runtimes. Complete a baseline execution and retain its in-process handle. Pause the broker (disconnect). Attempt a bounded `handle.status()` on the same handle while disconnected; the failure must be an allowlisted transport/time error, not `JobNotFoundError` or an arbitrary exception. Restart the broker on the same port/store. Observe reconnect callbacks, retry the same handle successfully, then submit and complete a new execution without handler re-registration or OS process restart. |

The scenario cap is **90 seconds**, with **30-second** child startup/shutdown caps and the harness
**30-second** broker shutdown reserved inside the cap (same budgeting model as broker restart #14).

## Baseline reproduction (pre-fix)

The installed scenario with the original production backend completed its baseline execution,
observed both reconnect callbacks, and read the retained result after reconnect. Its new execution
then timed out while the worker process remained alive: the outstanding unbounded
`fetch(..., timeout=None)` did not resume consumption after broker loss.
Evidence: `dist/verification/issue20-pristine/pristine-backend-baseline/py312`.
The one-time baseline measurement did not become a reusable source-mutating verifier option.

## Product fix (bounded work pull)

`NatsJobBackend` work consumption now uses `LogicSubscriber.get_one(timeout=…)` instead of the
unbounded `__aiter__` pull loop. Regression: `tests/test_nats_work_pull.py`.

## Application recovery semantics (documented, not automatic)

- In-flight client calls may raise transport errors or time out during a broker outage. SuperJobs
  does not automatically resubmit application operations.
- A transport failure does not prove a submission was never accepted. Uncertain submit retries must
  reuse the same `job_id` and `idempotency_key`, caller scope, request, and execution-affecting
  options.
- Live event iterators may fail; reopen from the last delivered observation cursor under existing
  retention rules.
- After the configured `NatsBroker` reconnect budget is exhausted, rebuild the connection/runtime.
  Reconnect timing and attempt limits remain on `NatsBroker` / nats-py; `NatsQueueConfig.reconnect_delay`
  is unused and does not control reconnection.

Deterministic orchestration regressions live in `tests/test_verify_idle_outage.py` (including
outage-read error oracles). Full integration includes the `idle-outage` stage in
`scripts/dev_check/stages.py`.

This slice does not cover active handlers crossing outages, uninterrupted live iterators for the
full observation stream, cluster failover, or prolonged outages. Existing at-least-once and
durable-completion semantics remain unchanged.

## Measured results

| Check | Result |
| --- | --- |
| Windows CPython 3.12.11 | `idle_outage_recovery` passed; final reviewed proof completed in 4.28 seconds (`dist/verification/issue20-final-proof`) |
| Windows CPython 3.14.7 | `idle_outage_recovery` passed (`dist/verification/issue20-final-win314`) |
| WSL Ubuntu 24.04 CPython 3.12 / 3.14 | `idle_outage_recovery` passed (`dist/verification/issue20-linux-wsl`, `issue20-linux-wsl-py314`) |
| Original production backend (dist only) | `issue20-pristine/pristine-backend-baseline/py312`: post-outage `TimeoutError` before `post_outage_done` |
| Focused regressions | `test_verify_idle_outage.py`, `test_nats_work_pull.py`, `test_idle_outage_producer_read.py` |
| `dev_check fast` | 379 passed, 16 integration/typing tests deselected on active Windows CPython 3.13 (`dist/verification/issue20-final-fast`) |

Broken-infra control: `tests/test_verify_idle_outage.py::test_missing_nats_executable_cli_writes_run_error`
sets `NATS_EXECUTABLE` to a nonexistent path and runs the verifier CLI (exit `1`, `run_error.json`).
The final standalone control also returned exit `1` with `NatsProvisionError`
(`dist/verification/issue20-final-broken-infra`).
Mocked broker start failure: `test_broker_start_failure_writes_summary`.
