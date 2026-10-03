# Manifest observation replay investigation (issue #27)

## Purpose

Optional bounded real-NATS diagnostics for manifest `validation_events` failures during `events()` replay in the performance harness. Issue [#27](https://github.com/vschroeter/superjobs/issues/27) was closed as **not currently reproducible** at the maintainer's request. This tooling preserves evidence if the symptom recurs; it is not a runtime fix or a required benchmark gate.

## Commands

Targeted unit tests (fast):

```bash
uv run pytest tests/test_manifest_replay_repro.py -m "not nats" -q
```

Broker probe against an owned NATS server:

```bash
uv run pytest tests/test_manifest_replay_repro.py::test_broker_probe_consumers_info_on_real_broker -q
```

Single full-order diagnostic attempt (issue #27 review repro):

```bash
uv run python tools/verify_manifest_replay_repro.py \
  --max-attempts 1 \
  --skip-contract-typing \
  --artifact-dir dist/issue27/reviewed-repro
```

## Artifact layout

Each wrapper invocation creates a fresh `run-<invocation>/` directory under the supplied artifact root (or under `dist/verification/manifest-replay-repro` by default). Repeated runs with the same parent path do not overwrite prior summaries or attempt logs.

| Path | Contents |
|------|----------|
| `<artifact-dir>/run-<invocation>/` | One reproduction runner invocation |
| `<artifact-dir>/run-<invocation>/attempt-NN/` | One `verify_performance` child per attempt |
| `<artifact-dir>/run-<invocation>/attempt-NN/run-<token>/diagnostic-report.json` | **Authoritative** diagnostic report for that attempt |
| `<artifact-dir>/run-<invocation>/attempt-NN/verify_performance.stdout.txt` | Child stdout |
| `<artifact-dir>/run-<invocation>/attempt-NN/verify_performance.stderr.txt` | Child stderr |
| `<artifact-dir>/run-<invocation>/attempt-NN/attempt-meta.json` | Resolution metadata (report path, timeout, cleanup) |
| `<artifact-dir>/run-<invocation>/reproduction-summary.json` | Wrapper outcomes, `fatal_errors`, and exit semantics |

Stale evidence is rejected: a report only under `attempt-NN/py312/diagnostic-report.json` (legacy runtime layout) without a new `run-<token>/` directory is not used.

## Exit codes (wrapper)

| Code | Meaning |
|------|---------|
| 0 | No reproduced `validation_events` failures |
| 1 | Infrastructure error (timeout, missing fresh report, subprocess setup failure) |
| 2 | Reproduced manifest replay validation failure |
| 3 | Harness error (`verify_performance` failed without reproduced validation failures) |

## Evidence capture flags

| Variable | Set by | Effect |
|----------|--------|--------|
| `SUPERJOBS_PERF_CAPTURE_REPLAY_EVIDENCE=1` | `verify_manifest_replay_repro.py` on the `verify_performance` process; forwarded into producer/worker children | Enables producer-side replay probes and orchestrator `collect_broker_snapshots` after the producer exits |

Producer replay probes are **off** unless this variable is set. When enabled, replay reads and the observation-consumer probe are capped (`REPLAY_EVIDENCE_MAX_EVENTS`) and bounded (`REPLAY_EVIDENCE_TIMEOUT_SECONDS`); probe timeouts and auxiliary exceptions are recorded in `replay_evidence` without replacing the original `validation_events` failure phase or exception fields.

Subprocess spawn failure, process-tree `cleanup_errors`, stale report resolution, malformed `diagnostic-report.json`, and non-finite or non-positive `--attempt-timeout-seconds` are fatal infrastructure errors (exit 1) and summarized in `reproduction-summary.json` under `fatal_errors`.

At `validation_events` failure, the producer records `observation_consumer` (expected durable name, filter subject, and `consumer_info` when still present). Post-run broker listing cannot reliably identify a closed ephemeral consumer; filtered durable matches may still be empty.

## Limits

- Per-attempt wall clock defaults to `INTEGRATION_JOB_BUDGET_SECONDS` (15 minutes) via `run_bounded`; on timeout the process tree is killed and streams are retained under the attempt directory.
- Broker probe uses installed `nats-py` `JetStreamContext.consumers_info(stream) -> list` (not an async iterator). Observation batches use msgpack `events[]` wire items with `data.kind`.
- No automatic reruns to green: one attempt per invocation when `--max-attempts 1`.

## Reproduction limits

Reproduction still depends on intermittent manifest `validation_events` behavior in the installed performance producer/worker stack. Harness improvements isolate fresh reports and preserve subprocess evidence; they do not change SuperJobs transport semantics.

## Latest reviewed wrapper run (2026-10-03)

- Command: `--max-attempts 1 --skip-contract-typing --artifact-dir dist/issue27/reviewed-repro`
- Outcome: `clean` (exit 0), `validation_event_failures=0`, ~276 s wall time
- Retained report: `dist/issue27/reviewed-repro/attempt-01/run-2ac6891c1190426c96d6558922103017/diagnostic-report.json`
- Retained summary: `dist/issue27/reviewed-repro/reproduction-summary.json` (invocation `031af3adfcda`)
- This run predates the final invocation-directory isolation correction. Its artifacts remain at their original paths; subsequent runs use the nested layout documented above.

## Independent `dev_check fast` gate note (2026-10-03)

The repository virtual environment was inaccessible when `uv` attempted to select Python 3.12. An isolated non-editable fast gate then imported a stale cached SuperJobs distribution: its `_NatsWorkSubscription` used an iterator, whereas current source uses bounded `get_one` pulls. Three tests in `tests/test_nats_work_pull.py` failed (468 passed, one skipped); evidence remains under `dist/issue27/dev-check-fast`. This was an import-origin mismatch, not evidence of the observation replay defect.

The source gate with `uv run --no-project --python 3.12 --with-editable . --with pytest --with pytest-asyncio python -m scripts.dev_check fast --artifact-dir dist/issue27/dev-check-fast-editable` passed. The corresponding `integration` command with artifact directory `dist/issue27/dev-check-integration` passed all six stages: source/wheel public contract typing (including negative consumers), NATS pytest, cross-program execution, worker recovery, broker restart, and idle outage. These gates exercised Python 3.12 only; the full supported-version matrix was not run in this iteration.

No `validation_events` failure on this attempt; intermittent issue #27 behavior was not observed here.

## Targeted replay stress (fast)

Three exploratory variants (60 s each, one pinned disk broker, inflight 8) were run. Their source is archived under `dist/issue27/diagnostic-targeted-stress/` rather than shipped as supported tooling; archived copies retain their original imports and are not standalone runnable scripts. Recorded original commands and results are in `dist/issue27/targeted-repro/README.md`.

**Diagnostic-only limits** (not installed-distribution verification): `dist/issue27/diagnostic-targeted-stress/HARNESS-LIMITS.md`.

### Targeted stress counts (submitted, not validated)

From `dist/issue27/targeted-repro/run-2c8fcb13841c/targeted-stress-summary.json` (exit 0, `reproduced: false`, ~180 s, `capture_replay_evidence: true`):

| Variant | `executions_attempted` | `validation_event_failures` | `status` |
|---------|------------------------|----------------------------|----------|
| `single_reader_many_executions` | 3427 | 0 | `bounded_complete` |
| `concurrent_readers_same_execution` | 583 | 0 | `bounded_complete` |
| `replay_race_around_terminal` | 2121 | 0 | `bounded_complete` |

These counts do not prove replay health: variants may finish as `bounded_complete` with zero `validation_events` runs (see harness limits above).

## Full-order diagnostics (validated completions)

Installed producer/worker wheel venvs, fresh wheels from current source, `--diagnostic-full-order`, replay evidence capture on wrapper runs.

| Run | Attempts | `validation_event_failures` | Wall time (s) | Summary / report |
|-----|----------|----------------------------|---------------|------------------|
| `dist/issue27/reviewed-repro` (invocation `031af3adfcda`) | 1 | 0 each | ~276 | `reproduction-summary.json` → `attempt-01/.../diagnostic-report.json` |
| `dist/issue27/fix-investigation/run-e0dd8a148a3b` | 3 | 0 each | ~273 / ~275 / ~273 | `reproduction-summary.json` → `attempt-01`…`03` reports |

Core `src/` has no diff between `af5fcec` and `04f24f2`; retained red artifacts remain under `dist/worktrees/issue23/dist/performance-diagnostic-original-order-2026-10-03` and `docs/performance-baselines/2026-10-03-original-order-diagnostic.json` (pinned broker log for that capture: startup only).

## Independent eight-reader replay probe

`dist/issue27/independent_replay_probe.py` on owned NATS (eight concurrent `handle.events()` readers, one manifest execution): `dist/issue27/independent-replay-4cb1c22db99b.json` — all eight readers returned sequences `[1,2,3,4,5]` with no `exception_type`. This is not the full-order harness and does not use accumulated JetStream history from telemetry workloads.

## Historical monotonic clock check (2026-10-03)

Hypothesis: original failure timing domain (`time.monotonic` / `GetTickCount64` sample scheduling) contributed to intermittent `validation_events` errors.

- Command and layout: `dist/issue27/historical-clock-repro/README.md`
- Valid attempt: invocation `31858b356296`, ~272 s, `validation_event_failures=0`, producer `measurement_clock.function=monotonic` (`GetTickCount64()` in `producer_results` evidence); report `environment.measurement_clock` still reflects harness `perf_counter` probe, not role copy.
- In-process only: `_validate_producer_results` perf_counter gate bypassed so monotonic role support could run; `tools/verify_performance.py` unchanged on disk.
- Aborted pilot `run-fc11bf4f4dc0` (~73 s): harness rejected monotonic clock before combinations completed (exit 3); retained as gate evidence, not counted as the historical measurement run.
- `producer_app.py` validation/replay path matches current source (not `af5fcec`); fixtures unchanged since `af5fcec`.

**Outcome:** Clean — cannot attribute retained failures to the clock/scheduling change alone; cannot diagnose the original intermittent defect accurately from this experiment.

## Remaining investigation

No minimized reproduction or diagnosed cause exists yet. Do not infer publication ordering, consumer interference, invalid expiry, resource pressure, or measurement clock domain from clean full-order, targeted-stress, independent probe, or historical-clock runs.

**Next missing evidence:** A red `validation_events` row from any bounded harness on the same machine with preserved broker store and consumer identity at failure time (post-failure broker log beyond startup, or live capture before consumer teardown). Without that, compare retained `af5fcec`-era producer validation code path and broker state to current runs.

Post-failure replay probes are diagnostic observations; their success never removes the original failure or makes a failed verification green.
