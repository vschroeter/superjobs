# Remote CLI submission

Issue [#41](https://github.com/vschroeter/superjobs/issues/41) submits selected
`submit` commands through an application-owned NATS-backed `SuperJobs` producer
lifecycle. The CLI never registers handlers or starts worker consumers.

## Runtime ownership

`JobCLI` snapshots `remote_runtime_factory` when `build_typer()` or `mount()`
runs. The factory must yield a `SuperJobs(broker=...)` instance without
pre-registered handlers. In-memory and worker-wired runtimes are rejected before
submission.

When `remote_runtime_factory` is omitted, `submit` exits `1` with
`MISSING_REMOTE_FACTORY_MESSAGE` after input validation.

## Submission and waiting

Without `--wait`, stdout carries one JSON execution reference
(`job_id`, `job_name`, `job_version`) after confirmed acceptance.
`job_name` is the raw ``Job.name`` string; ``job_version`` is JSON ``null`` when
the Job has no version. The accepted execution can outlive the CLI.

If submission raises or the CLI is interrupted during submission before a handle
is returned,
stderr reports **submission acceptance unconfirmed**. That is distinct from
startup or transport failures before submit, terminal failures after acceptance,
and cleanup failures (stdout stays empty on every non-success path).

With `--wait`, supported observations stream to stderr as JSON lines while the
CLI waits for the authoritative terminal outcome. Stdout carries one
adapter-serialized final result (`null` when the Job has no result). During the
owned producer lifecycle, factory prints and FastStream default access diagnostics
are routed to stderr. Application-supplied loggers should also target stderr.

`--wait-timeout` is allowed only with `--wait` and must be a finite positive
number (NaN and infinities are rejected at usage time). It limits client
waiting, not worker attempt runtime or execution deadlines. The default wait
bound is `DEFAULT_CLI_WAIT_TIMEOUT_SECONDS` (300 seconds, exported from
`superjobs.cli`). The bound covers outcome waiting and observation draining.
Producer teardown uses
a separate shared budget that starts when disconnect begins (after waiting ends,
on timeout, or on interruption), not when the CLI starts.

When the durable outcome arrives before the observation iterator finishes,
the CLI drains normal observations for at most the smaller of the remaining wait
budget and `REMOTE_SUBMIT_OBSERVATION_DRAIN_GRACE_SECONDS` (2 seconds). This lets
retained durable results remain usable when terminal observations are missing.
Observation stream failures are terminal even
when the outcome task would otherwise succeed. Observation resource close failures
also fail the command.

On wait timeout, transport failure after acceptance, terminal failure, cleanup
failure, serialization failure, or CLI interruption, stderr preserves a known
execution reference (once) so another client can reconnect. Timeout and
interruption do not implicitly cancel or resubmit accepted work.

`REMOTE_SUBMIT_SHUTDOWN_TIMEOUT_SECONDS` (30 seconds) bounds factory/runtime
teardown after disconnect. Startup and submission each use the same finite phase
bound independently of waiting. Asyncio cancellation is requested during cleanup;
Python cannot forcibly terminate arbitrary in-process or cancellation-resistant
work. After a cancellation deadline, the CLI diagnoses resistant work and keeps
waiting for it rather than leaving owned tasks running. Blocking event-loop code
prevents timely signal handling. The budgets request cancellation; they do not
guarantee hard termination.

## Related documents

- [CLI registration](cli-registration.md) for factories and exit conventions.
- [CLI input](cli-input.md) for shared request preparation.
- [CLI local execution](cli-local.md) for in-process `run`.

## Verification on 2026-10-04

Cursor Composer 2.5 implemented the submission slice and two fully delivered
correction passes. Codex independently reviewed the results and corrected the
remaining observation-close, simultaneous completion, diagnostic stdout,
shared cleanup-budget and startup-rollback paths. Initial Windows launcher calls
truncated multiline briefs; subsequent passes read the complete briefs from files.

Measured Windows checks:

| Check | Outcome |
| --- | --- |
| Full deterministic suite, Python 3.13 | 700 passed, two documented platform skips |
| Final remote suites, including one additional startup-rollback regression | 45 passed, including eight real NATS cases |
| Full owned-NATS suite | 20 passed; no skips |
| CLI implementation and example, Pyright 1.1.414 | Zero errors and warnings |
| Source and wheel consumer typing | Positive consumers clean; matching negative diagnostics, including 25 CLI misuse errors |
| Exact final installed revision, Python 3.12 and 3.14 | 245 public runtime tests passed per interpreter, one documented platform skip each; wheel origins verified |

The real NATS checks include acceptance, typed and no-result success, terminal
handler failure, an unavailable broker, and successful result retrieval after the
CLI times out and disconnects. Public deterministic tests additionally exercise
factory snapshots, native SIGINT racing acceptance, observation failures and
resource closure without cancellation or resubmission.

Local evidence is retained under `dist/issue41/final-*` (gitignored). The full
deterministic gate preceded the final extra startup-rollback safeguard; the final
remote and installed suites verify that safeguard. Linux and hosted CI were not
run for this slice. Separately installed CLI/worker processes and execution
survival across CLI process exit remain the scope of issue #42.
