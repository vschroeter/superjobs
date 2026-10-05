# Unified handler registration, CLI exposure, and worker lifetime

Status: **Variant C (shared `HandlerCatalog`) selected** with runtime `cli=` metadata
shortcut and `await jobs.serve()` / `wait_until_stopped()` lifecycle helpers
([#46](https://github.com/vschroeter/superjobs/issues/46),
[#47](https://github.com/vschroeter/superjobs/issues/47)). Core library behavior,
typing suites, and deterministic tests are implemented on `feature/cli`; installed
example and process verification for catalog + built-in NATS URL are tracked in
the same effort ([#45](https://github.com/vschroeter/superjobs/issues/45)
Wayfinder). Historical Variant B recommendation below remains for comparison only.
Inspection baseline: `feature/cli`, `23b4419`; interface direction frozen
2026-10-05.

Tracking: [Wayfinder: Unify handler registration, CLI exposure, and worker lifetime](https://github.com/vschroeter/superjobs/issues/45),
with native sub-issues [Decide unified ordinary handler registration and CLI exposure](https://github.com/vschroeter/superjobs/issues/43)
and [Decide jobs-level serving and shutdown waiting convenience](https://github.com/vschroeter/superjobs/issues/44).

## Maintainer direction, 2026-10-05

The maintainer prefers Variant C (a shared declarative catalog), together with
the optional CLI metadata shortcut on the runtime decorator, and accepts the
`await jobs.serve()` direction. This supersedes the recommendation of Variant B
later in this historical comparison. The accepted implementation uses typed
managed providers, inherits catalog execution policy for local commands, and
tracks shutdown completion separately for each runtime lifetime.

The catalog remains the source of definitions. A runtime shortcut such as
`@jobs.handler(JOB, cli="manifest")` should add a binding and dependency-free
command metadata to its catalog, rather than maintain an independent CLI table.
The same shortcut on `@handlers.handler(...)` is useful for portable declarations
that do not require constructing a runtime in their definition module. CLI
construction consumes a snapshot; add bindings before constructing the CLI.
An explicit `Command(...)` metadata object can customize field presentation;
Typer remains outside core imports. Runtime-bound resources must not be copied
into another runtime. Remote-only installations still use contract-only command
declarations without importing the handler catalog or workers.

The maintainer also proposed replacing mandatory remote runtime factories with
a NATS URL and a CLI override. There is no fundamental need for a user-written
factory in the ordinary case: a proposed `JobCLI(handlers=catalog,
nats_url="nats://localhost:4222")` can create and clean up a fresh producer-only
runtime internally, only after valid `submit` input. Local `run` uses the
catalog's handlers in an isolated in-memory runtime regardless of this URL.

Proposed option placement: `myapp submit --nats-url nats://broker:4222 manifest
--device-id sensor-17 --wait`. Making the URL a submit-group option separates
transport configuration from request fields. Selected precedence for built-in remote mode is explicit `submit --nats-url`,
then `SUPERJOBS_NATS_URL`, then `JobCLI(nats_url=...)`, then
`nats://localhost:4222`. Custom `remote_runtime_factory` rejects `--nats-url` and
constructor `nats_url`; the factory owns transport configuration and must not
read ambient URL env unless the application chooses to. URL values should not be
resolved by opening a connection during help. Authentication, TLS, and other
connection settings can use a structured configuration; a custom factory may
remain an advanced escape hatch. Factory mode and built-in URL mode must have
explicit exclusivity or override behavior, never silently ignore a supplied URL.

`serve()` is the normal standalone worker entry point. The explicit context and
wait-only form is useful when application work must occur after startup and
before waiting: publishing readiness, opening another service, or coordinating
an existing application lifetime. For example:

```python
async with jobs:
    ready_file.write_text("ready", encoding="utf-8")
    try:
        await jobs.wait_until_stopped()
    finally:
        ready_file.unlink(missing_ok=True)
```

The context owns startup and cleanup; the wait method only suspends until that
lifecycle finishes. It does not trigger shutdown. An external owner can call
`await jobs.stop()`, or cancellation unwinds the enclosing context and invokes
cleanup. The existing worker readiness example is a concrete caller for this
form. Readiness publication is application-defined; successful startup must not
be confused with comprehensive health checks. Most workers need only `serve()`.

## Inspected baseline before implementation (`23b4419`)

- `SuperJobs.handler()` / `register()` bind a contract and callback to a backend
  and accept concurrency, retry, observation policy, and heartbeat settings.
- `JobCLI.add()` separately records the contract and callback or callback factory.
  A callable can already be reused; a second handler implementation is not required.
- Local CLI execution rejects pre-registered runtimes, registers the callback
  itself, and sets `RetryPolicy(max_attempts=1)`. It does not reuse the ordinary
  registration's worker configuration.
- `@job.handler` attaches contract metadata and preserves the callback.
  `@jobs.handler(job)` registers the callback but does not attach that metadata.
- Workers already have `start()`, `stop()`, and an async context manager. Examples
  use an unrelated `asyncio.Event().wait()` to keep the process alive.

Evidence: [runtime](../../src/superjobs/superjobs.py),
[decorators](../../src/superjobs/jobs/handler_decorators.py),
[CLI registration](../../src/superjobs/cli/app.py),
[local execution](../../src/superjobs/cli/local_run.py), and
[worker example](../../examples/contract_interface/worker.py).
Tests explicitly protect the current rejection of pre-registered local runtimes
and the single-attempt local behavior in `tests/test_cli_local_run.py`.

## Constraints and seam

Keep Job definitions and payload types in an independent contract package.
The same Job handler implementation should support ordinary workers and local
CLI execution. Remote submission needs only contracts and connection configuration.
Help and input errors must not acquire runtime resources or resolve lazy handlers.
Typer remains optional and absent from core imports.

Registration and input planning are in-process dependencies. Execution already
has a real seam at the injected `JobBackend`, with in-memory and NATS adapters.
Retain it. The improvement should put registration knowledge in one module and
make CLI commands views of existing definitions, with application-owned names,
aliases, positional fields, help, and option spelling.

All examples below are proposed syntax. Existing contract and payload names are
illustrative; they are imported from the application's contract package.

## Variant A: reuse a contract-marked callable

```python
# handlers.py: ordinary implementation, no CLI import
@MANIFEST_JOB.handler
async def manifest(request: ManifestRequest,
                   context: JobContext[ManifestEvent]) -> ManifestResult:
    ...

# worker.py
jobs.register(manifest, concurrency=4)

# cli.py
cli.add_handler("manifest", manifest, positional_fields=("device_id",))
```

`add_handler()` infers the Job from the existing marker. It removes the repeated
contract argument, needs little migration, and retains the existing handler
typing check. Another possible spelling is an outer `@cli.command(...)`
decorator over `@MANIFEST_JOB.handler`; that makes decorator ordering part of
the interface and imports the CLI in the handler module.

The limitation is depth: this shares the callable, not registration policies or
resource ownership. Importing the callable also loads its implementation during
help. A lazy route remains necessary. Existing `cli.add(name, job, handler=fn)`
can already reuse a function, so this is mostly convenience rather than a full
solution to the split.

## Variant B: project ordinary registrations into commands

```python
# handlers.py: existing ordinary registration, unchanged by CLI exposure
def register_handlers(jobs: SuperJobs) -> None:
    @jobs.handler(MANIFEST_JOB, retry=RetryPolicy(max_attempts=3))
    async def manifest(request: ManifestRequest,
                       context: JobContext[ManifestEvent]) -> ManifestResult:
        ...

# worker.py
jobs = SuperJobs(broker=broker)
register_handlers(jobs)
await jobs.serve()

# cli.py
@asynccontextmanager
async def local_runtime():
    from .handlers import register_handlers

    jobs = SuperJobs()  # fresh, isolated in-memory runtime
    register_handlers(jobs)
    try:
        yield jobs  # configured, unstarted; CLI owns start/stop
    finally:
        await jobs.stop()

cli = JobCLI(local_runtime_factory=local_runtime,
             remote_runtime_factory=remote_runtime)
cli.add("manifest", MANIFEST_JOB, positional_fields=("device_id",))
```

The CLI looks up the selected Job's existing registration. It does not ask for
or register a callback again. `run manifest` uses local registration;
`submit manifest` uses a producer runtime and never enters `local_runtime`.
The command table is contract-only, so help also avoids importing workers.
Application dependencies can be acquired inside the local factory around
`yield`, and remain alive until CLI runtime cleanup finishes.

This is the most comfortable incremental interface: ordinary handler code and
registration stay unchanged, while adding a CLI changes only the CLI module.
There is no new required public catalog concept. Local factory ownership becomes
more precise: fresh and unstarted, with application resources owned by the
factory and runtime execution owned by the CLI.

Implementation must select backend-free registration configuration and use the
existing execution pipeline. Merely removing the empty-runtime guard would still
leave duplicate registration and start unrelated handlers. A private shared
registration module should centralize selection, validation, and handler creation.
Do not make callers or the CLI copy live `JobHandler` instances from `_handlers`.

Its limitation is selective setup: a broad `register_handlers()` function can
import or initialize unrelated dependencies even when only one command runs.
Starting only the selected local handler does not undo those application effects.
Use focused registration helpers first; adopt Variant C if this becomes a common
problem. An already-running NATS worker is never borrowed for local `run`.

## Variant C: share a declarative handler catalog

```python
# handlers.py: backend-free definitions
handlers = HandlerCatalog()

@handlers.handler(MANIFEST_JOB, concurrency=4,
                  retry=RetryPolicy(max_attempts=3))
async def manifest(request: ManifestRequest,
                   context: JobContext[ManifestEvent]) -> ManifestResult:
    ...

# worker.py
jobs = SuperJobs(broker=broker, handlers=handlers)
await jobs.serve()

# cli.py
cli = JobCLI(handlers=handlers, remote_runtime_factory=remote_runtime)
cli.expose("manifest", MANIFEST_JOB, positional_fields=("device_id",))
```

An immutable binding records the Job, callback/provider, and execution policy.
Consumers snapshot the definitions and construct runtime-specific handlers.
Aliases expose one binding with different presentation; they do not create
duplicate subscriptions. Only explicitly exposed Jobs become CLI commands.

For resource-dependent handlers, a lazy provider should be an async context
manager yielding a typed handler. Local execution enters only the selected
provider; worker startup enters its configured providers. Startup rollback and
shutdown release them. An unmanaged callback factory alone does not define
dependency cleanup.

This gives the best locality and selective loading for several entry points and
complex dependencies. It adds a public concept and requires more migration.
Importing a catalog with eager callbacks still imports their implementations;
declarative lazy providers are needed to avoid that. Catalog modules must not
perform resource acquisition at import time.

Producer-only CLIs omit the catalog and expose contracts for submission only.
The current `remote_only=True` spelling can remain available in all variants.

## Attractive shortcut: CLI metadata on the runtime decorator

```python
@jobs.handler(MANIFEST_JOB, cli="manifest")
async def manifest(request, context):
    ...
```

This is compact for one application, but collecting commands from a live runtime
makes help, dependency ownership, aliases, and producer-only installations harder.
Rich presentation settings also spread CLI imports into ordinary worker modules.
Do not use it as the main design. If later added as sugar, collect dependency-free
metadata over the same reusable registration module; never pull Typer into core.
Do not attach application CLI presentation to immutable shared Job contracts.

## Comparison and recommendation

| Variant | Change to ordinary handling | Policy/resource reuse | Main cost |
| --- | --- | --- | --- |
| A: marked callable | Use existing contract decorator if needed | Callable only | Lazy setup and policy remain separate |
| B: registration projection | Existing registration stays unchanged | Existing configuration; factory-owned resources | Fresh factory discipline and broad setup |
| C: catalog | Move registration to a backend-free catalog | Shared configuration and selected managed provider | New public concept and migration |

**Selected direction (2026-10-05):** Variant C as the primary public interface,
with optional runtime `cli=` sugar and `serve()` for standalone workers. Variant B
remains a useful mental model for migration (projection from registrations).
Variant A is optional convenience. Implementation and verification span
[#43](https://github.com/vschroeter/superjobs/issues/43)–[#47](https://github.com/vschroeter/superjobs/issues/47).

## Request fields versus CLI presentation

Flat supported request fields already generate options automatically. Adding a
request field should normally change the request type and application behavior
only. Use `field_options` for an alias/help customization, and `positional_fields`
only when positional presentation is wanted. These remain CLI-local settings:

```python
cli.add("manifest", MANIFEST_JOB,
        field_options={"device_id": CLIField(option="device", help="Device ID")})
```

A CLI-only flag such as `--wait` belongs to command execution. A flag carrying
new domain input must map into the request contract. Preserve JSON/file/stdin
support for shapes without automatic flat options. Remote submission does not
configure the deployed worker's retry or concurrency settings.

## Local execution policy: explicit decision still needed

Sharing a definition does not require identical transport semantics. Keep local
in-memory execution and remote NATS submission explicit, with no fallback.

There are two reasonable retry policies: retain today's single attempt as a
documented local profile, or inherit the shared registration policy with an
explicit local override. Recommend inheritance for a newly selected unified
registration mode, so configured retry behavior is not silently discarded.
Keep legacy registrations' current behavior during migration. Observation policy
and relevant context configuration should carry across; concurrency has little
effect for one isolated submitted execution. Local results remain non-durable.

This recommendation changes an earlier selected single-attempt behavior and must
be reviewed as a design decision. Custom local factories retain runtime-wide
observation settings, retention, and other application choices. Sharing callbacks
alone cannot establish parity with those settings or with a separately deployed
worker revision.

## Runtime lifetime convenience

Propose a standalone lifecycle owner:

```python
async def main():
    jobs = SuperJobs(broker=broker)
    register_handlers(jobs)
    await jobs.serve()

asyncio.run(main())
```

`serve()` starts an unstarted runtime, waits for shutdown, and stops it in
`finally`. An external `jobs.stop()` releases it. Cancellation propagates after
cleanup; startup failures remain failures. Reject an already-started runtime and
concurrent lifecycle owners rather than taking ownership implicitly.

For applications with readiness hooks or their own async lifetime:

```python
async with jobs:
    publish_readiness()
    await jobs.wait_until_stopped()
```

`wait_until_stopped()` waits only, requires an active runtime, and supports
multiple independently cancellable waiters. Cancelling one waiter does not stop
the runtime. Shutdown completion means draining and cleanup have finished;
`_started=False` alone is insufficient because it is currently set before drain.
Completion belongs to a lifecycle generation, so restart cannot strand old
waiters or release new ones through a stale event. Shutdown failure must release
waiters with defined visible failure behavior, not leave them hanging.

Retain existing graceful shutdown configuration. Do not promise hard termination
of arbitrary cancellation-resistant Python code. Keep process-global signal
installation in the entry point or an explicit runner policy. In particular,
embedded async use must not quietly replace its application's signal handlers.
These async helpers are separate from the blocking producer interface in #36.

## Verification required after a design is selected

Keep decision completion separate from implementation acceptance. Future slices
must prove public source and installed imports; positive/negative static examples
for request, result, event, absent payloads, sync/async handlers, and decorator
signature preservation; deterministic registration selection, aliases, policies,
resource cleanup, lazy help, and independent waiters; and existing real-NATS plus
separate installed producer/worker process checks. Missing required infrastructure
must fail. Windows and Linux interrupt behavior and restart/stop races need
targeted lifecycle coverage. No checks were run for the proposed syntax because
it has not been implemented.
