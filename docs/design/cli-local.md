# Local CLI execution

Issue [#40](https://github.com/vschroeter/superjobs/issues/40) runs selected
`run` commands in the CLI process through an isolated owned
`SuperJobs(transport=InMemoryTransport(...))` lifecycle. No NATS connection and
no extra worker subprocess are started for local execution.

## Runtime ownership

`JobCLI` snapshots the effective local runtime factory when `build_typer()` or
`mount()` runs. Later changes to `JobCLI.local_runtime_factory` do not affect
Typer objects already built.

When `local_runtime_factory` is omitted, the CLI uses a built-in factory that
yields a fresh started `SuperJobs` instance with a new `InMemoryTransport`.
Applications can still supply a custom async context manager for dependency
startup and teardown.

Before registering the selected handler, local run validates that the yielded
runtime is a `SuperJobs` with an `InMemoryTransport`, without pre-registered
handlers or active in-memory work consumers. NATS-backed runtimes are rejected
without invoking the selected handler. Factory cleanup still runs when validation
fails.

Factories may yield an unstarted `SuperJobs`; the CLI registers the selected
handler, starts the runtime through public `start()`, and waits for that handler
to be ready before submission. Shared transports and transports retaining old
executions are rejected. Factories own their dependency resources and must release
them in `finally`, including when startup is interrupted.

## Execution and I/O

Local run submits exactly one execution through existing public client/handle
paths with `RetryPolicy(max_attempts=1)`. Supported observations stream to
stderr as compact JSON lines. Stdout carries one adapter-serialized final result
JSON value (`null` when the Job has no result). Handler failures, invalid
results, observation serialization failures and startup/shutdown errors exit `1`
with stderr diagnostics. Invalid CLI input still exits `2` before runtime
startup.

This slice always uses one local attempt; it does not expose a local retry
configuration option. Pre-registered handlers are rejected. Local executions have
no durable recovery or restart guarantee.

`JobCLI.main()` refuses to run when the current thread already has an active
asyncio loop (exit `2`). The same rule applies to mounted Typer apps when a
`run` callback would nest `asyncio.run()`.

## Interrupts and shutdown bounds

`LOCAL_RUN_SHUTDOWN_TIMEOUT_SECONDS` (exported from `superjobs.cli`, default
`30`) defines a 30-second cooperative cancellation grace period after interruption
of an execution, followed by a separate 30-second cleanup budget shared by handler,
runtime, transport rollback and application factory teardown. On grace expiration,
the CLI cancels its observation task and stops the selected handler using asyncio
cancellation. Interrupted startup is cancelled immediately. Cleanup has a deadline
on every path, including successful executions; cleanup failure prevents result
stdout and exits `1`. Interruption exits `130`, including during startup or teardown,
while preserving cleanup diagnostics.

Handler side effects are not rolled back when bounds are reached. Python cannot
safely terminate arbitrary threads or handlers that suppress cancellation;
the deadlines trigger cancellation rather than guaranteeing hard termination.
After cancelling an owned task, the CLI allows one second for acknowledgement and
then diagnoses resistant work and continues waiting instead of returning with it
running. A synchronous handler running in a thread must finish before the owned
event loop's executor can close. Blocking event-loop code also prevents timely
signal handling. Applications must use cancellation-aware async handlers and
factories for bounded interruption. No extra process isolation is provided.

SIGINT is registered only on the main thread and restored after each local run.

## Related documents

- [CLI registration](cli-registration.md) for factories, mount snapshots and exit codes.
- [CLI input](cli-input.md) for request preparation shared by `run` and `submit`.

## Verification on 2026-10-04

Cursor Composer 2.5 produced two bounded implementation passes. Codex independently
reviewed both and corrected the remaining lifecycle paths and regression coverage.

Final independent checks on Windows and Linux (WSL):

- Full deterministic gate, Python 3.12, 3.13 and 3.14: 664 passed, two documented
  platform skips in each of six cells.
- CLI implementation and example: Pyright 1.1.414, zero errors or warnings.
- Final focused CLI checks after cleanup-error review: 184 passed on Windows
  (one process-signal skip), 185 passed on Linux.
- Source/wheel consumers on both platforms: matching positive and negative diagnostics,
  including 23 genuine CLI misuse errors. Installed public runtime tests:
  208 passed on Windows and 209 on Linux, on each of Python 3.12 and 3.14, with
  verified wheel origins. Linux installed tests include the process SIGINT proof;
  native SIGINT and signal restoration are also tested on Windows.

Evidence is retained locally under `dist/issue40/final-*` (gitignored). Real NATS and
hosted CI were not run for this in-memory slice. Issue #41 remains remote
submission; issue #42 remains final installed two-mode application integration.
