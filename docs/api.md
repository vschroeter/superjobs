# SuperJobs public API

Current public API reference for the SuperJobs library. Domain language:
[GLOSSARY.md](../GLOSSARY.md). Architectural decisions: [adr/](adr/README.md).
Optional application CLI: [cli.md](cli.md). Runnable contracts:
[examples/contract_interface/](../examples/contract_interface/).

## Implemented vs selected / not implemented

| Area | Status |
| --- | --- |
| Shared contract packages, handler registration, async submit/observe | **Implemented** |
| Optional Typer CLI (`JobCLI`, local `run`, remote `submit`) | **Implemented** — [cli.md](cli.md) |
| `SubmitOptions`, keyword request construction (`RequestJob`), strict payload validation | **Implemented** |
| `JobOutcome` narrowing, typed `JobContext` event parameter | **Implemented** |
| Pyright **1.1.414**, `basic`, static target **3.12** on public consumer fixtures | **Measured guarantee** (not strict/mypy proof) |
| Versioned descriptors, deterministic fingerprints, execution/manifest enforcement | **Selected, not implemented** ([#29](https://github.com/vschroeter/superjobs/issues/29), [#30](https://github.com/vschroeter/superjobs/issues/30), [Wayfinder #28](https://github.com/vschroeter/superjobs/issues/28)) |
| Worker runtime `serve()` and `wait_until_stopped()` lifecycle helpers | **Implemented** |
| Worker discovery (`client.workers()`, `jobs.discovery.*`), persistent presence and capability updates | **Implemented** ([#52](https://github.com/vschroeter/superjobs/issues/52), [#53](https://github.com/vschroeter/superjobs/issues/53), [#54](https://github.com/vschroeter/superjobs/issues/54)); installed crash/recovery scenarios tracked in [#55](https://github.com/vschroeter/superjobs/issues/55) |
| Shared `HandlerCatalog`, runtime `handlers=` consumption, catalog-backed CLI | **Implemented** (issue [#46](https://github.com/vschroeter/superjobs/issues/46)) |
| Blocking sync producer helpers with continuous runtime lifecycle | **Direction only** ([#36](https://github.com/vschroeter/superjobs/issues/36)) |
| Dependency-sensitive strict validation guard | **Future** ([#32](https://github.com/vschroeter/superjobs/issues/32)) |
| Presence-aware constructor/handler typing refinements | **Open** ([#31](https://github.com/vschroeter/superjobs/issues/31)) |

## Job contracts

A `Job` binds a stable name and version to optional request, final result, and
intermediate event payload types. Constructor inference on Python **3.12+** fills
`None` for omitted slots.

| Pattern | Request | Result | Events | Producer `submit` |
| --- | --- | --- | --- | --- |
| Full manifest | typed | typed | typed | `submit(ManifestRequest(...))` or keywords |
| No application events | typed | typed | none (`event=None`) | `submit(...)` |
| Telemetry-style | typed | none (`result=None`) | none | `submit(TelemetrySample(...))`; handler returns `None` |
| Heartbeat-style | none (`request=None`) | typed | none | `submit()` or `submit(SubmitOptions(...))` |

Nullable **fields** inside a declared request type are allowed. Omitting the whole
request **slot** is different from a nullable request model.

## Producer submission

```python
from superjobs import SubmitOptions

client = jobs.client(MANIFEST_JOB)

handle = await client.submit(ManifestRequest(device_id="sensor-17"))
handle = await client.submit(device_id="sensor-17")  # RequestJob keyword path
handle = await client.submit(
    SubmitOptions(timeout=30.0, idempotency_key="k1"),
    device_id="sensor-17",
)
handle = await client.submit(
    ManifestRequest(device_id="sensor-17"),
    options=SubmitOptions(job_id="exec-1"),
)
```

`SubmitOptions` fields: `idempotency_key`, `job_id`, `timeout`, `deadline`,
`caller_scope`. Legacy keyword aliases on `submit()` remain for compatibility;
mixing a `SubmitOptions` instance with legacy option values is rejected.

`client.run(...)` is submit plus `result()` in one call. `client.get(job_id)` and
`client.handle(job_id)` reconstruct a public handle for an existing execution.

## Handler rules

- Every handler receives **`JobContext`**; with a request, `(request, context)`.
- One active handler per job in a worker registry.
- Register with `@jobs.handler(job)` or `jobs.register(job, callback)`. Alternatively,
  `@job.handler` marks the callback's contract association, then
  `jobs.register(callback)` activates it in a runtime. Choose one startup form.

### Shared handler catalog

`HandlerCatalog` collects backend-free bindings (callback or lazy async-context-manager
`provider`, concurrency, retry, observation policy, heartbeat, and optional CLI
`Command` metadata). Workers consume a catalog with
`SuperJobs(..., handlers=catalog)`. Runtime decorators still add bindings to the
same catalog when one is supplied (`cli="name"` stores presentation metadata).
Provider bindings enter at runtime startup; startup rollback and shutdown release
entered resources. `catalog.snapshot()` is explicit; CLI construction snapshots
bindings and does not observe later catalog mutations.

```python
from superjobs import Command, HandlerCatalog, SuperJobs

handlers = HandlerCatalog()

@handlers.handler(MANIFEST_JOB, concurrency=4, cli="manifest")
async def manifest(request: ManifestRequest, context: JobContext[ManifestEvent]) -> ManifestResult:
    ...

jobs = SuperJobs(broker=broker, handlers=handlers)
await jobs.serve()
```
- `context.emit` requires a declared event type on the job; forbidden when
  `event=None`.
- Handlers for `result=None` jobs must return `None` explicitly or implicitly.

Typed context example:

```python
@jobs.handler(MANIFEST_JOB)
async def manifest(
    request: ManifestRequest,
    context: JobContext[ManifestEvent],
) -> ManifestResult:
    await context.emit(ManifestEvent(stage="published"))
    return ManifestResult(revision=request.device_id)
```

## Worker runtime lifecycle

Standalone workers usually call `await jobs.serve()`, which starts an unstarted
runtime, waits for shutdown, and stops it in `finally`. An external
`await jobs.stop()` ends the wait; task cancellation propagates after cleanup.
`serve()` rejects runtimes that are already started or already owned by another
lifecycle helper.

Embedded workers can use `async with jobs` for startup and cleanup, run
readiness hooks after enter, then `await jobs.wait_until_stopped()` to suspend
until shutdown without requesting it. Multiple waiters are released when
shutdown finishes; cancelling one waiter does not stop the runtime. Shutdown
cleanup failures are reported by `stop()` and by waiters for that lifetime.

## Observation and durability semantics

- **At-least-once** execution: duplicate attempts before durable completion are
  allowed; design handlers to be idempotent.
- **Progress** is latest-value-wins; logs and intermediate application events keep
  order within retention.
- **Worker loss** may drop in-flight intermediate observations; proved recovery
  scopes preserve authoritative final outcomes and allow handle reconstruction.
- **Retention limits** can yield expired observation cursors on replay (`events(after=...)`).
- **Outages**: transport calls may fail; SuperJobs does not auto-resubmit. Retry
  reads on the same handle; uncertain submits reuse stable identifiers and scope
  (see [ADR 0005](adr/0005-broker-outage-retry-and-optional-stress.md)).
- **Fingerprints / shared manifest**: not enforced at runtime today.

## Strict validation

Request, result, and event payloads are validated at the SuperJobs boundary
(strict by default). Invalid producer input fails before submission. Invalid
handler outputs surface as failed executions. Revalidation applies to explicit
objects passed to `submit`; values already coerced by application code before
construction cannot be recovered. Mixing an explicit request object with
constructor keywords is rejected.

Pydantic `BaseModel` and dataclass payloads are supported; unknown keyword
arguments are rejected when keyword submission is used. Details:
[payload-validation.md](payload-validation.md).

## Static typing limits (Pyright 1.1.414, basic, target 3.12)

Measured on `examples/contract_interface/typing/` and
`tools/verify_contract_typing.py` (source and installed wheels must agree).

### Keyword-only request DTOs and inferred job success

```python
from dataclasses import dataclass

from superjobs import Job, SuperJobs

@dataclass(frozen=True, slots=True, kw_only=True)
class ManifestRequest:
    device_id: str

@dataclass(frozen=True, slots=True)
class ManifestResult:
    revision: str

@dataclass(frozen=True, slots=True)
class ManifestEvent:
    stage: str

MANIFEST_JOB = Job(
    "examples.contract.manifest",
    version="v1",
    request=ManifestRequest,
    result=ManifestResult,
    event=ManifestEvent,
)

async def producer(jobs: SuperJobs) -> None:
    client = jobs.client(MANIFEST_JOB)
    await client.submit(device_id="sensor-17")  # keyword path type-checks
```

### Widening to `Job[Req, Res, Event]` — explicit object only

```python
from superjobs import Job, SuperJobs

# ManifestRequest / ManifestResult / ManifestEvent as above.

async def through_base_annotation(
    jobs: SuperJobs,
    job: Job[ManifestRequest, ManifestResult, ManifestEvent],
) -> None:
    client = jobs.client(job)
    await client.submit(ManifestRequest(device_id="ok"))  # explicit object OK
    await client.submit(device_id="lost")  # intentional negative: reportCallIssue
```

### Explicit `RequestJob[..., ...]` / `NoRequestJob` handler shapes

Payload types (`ManifestRequest`, `ManifestResult`, `ManifestEvent`) are defined in
the preceding example.

```python
from dataclasses import dataclass

from superjobs import Job, JobContext, NoRequestJob, RequestJob, SuperJobs

@dataclass(frozen=True, slots=True)
class HeartbeatResult:
    ok: bool

HEARTBEAT_JOB = Job("examples.contract.heartbeat", version="v1", result=HeartbeatResult)

ExplicitManifest: RequestJob[
    ManifestRequest, ManifestResult, ManifestEvent, ...
] = MANIFEST_JOB  # ellipsis preserves handler checks; loses named submit keywords


def register(jobs: SuperJobs) -> None:
    @jobs.handler(ExplicitManifest)
    async def manifest(
        request: ManifestRequest,
        context: JobContext[ManifestEvent],
    ) -> ManifestResult:
        await context.emit(ManifestEvent(stage="published"))
        return ManifestResult(revision=request.device_id)

    NoRequest: NoRequestJob[HeartbeatResult, None] = HEARTBEAT_JOB

    @jobs.handler(NoRequest)
    async def heartbeat(context: JobContext[None]) -> HeartbeatResult:
        return HeartbeatResult(ok=True)


def register_widened(
    jobs: SuperJobs,
    job: Job[ManifestRequest, ManifestResult, ManifestEvent],
) -> None:
    @jobs.handler(job)  # intentional negative: widened Job cannot select a handler overload
    async def through_base(
        request: ManifestRequest,
        context: JobContext[ManifestEvent],
    ) -> ManifestResult:
        return ManifestResult(revision=request.device_id)
```

### Positional dataclass misuse (static vs runtime)

```python
@dataclass(frozen=True, slots=True)  # not kw_only
class PositionalRequest:
    device_id: str

POSITIONAL_JOB = Job("probe.positional", request=PositionalRequest, result=ManifestResult)

async def misleading_static(jobs: SuperJobs) -> None:
    client = jobs.client(POSITIONAL_JOB)
    await client.submit("device-id")  # intentional negative: may type-check; runtime rejects
    await client.submit(PositionalRequest(device_id="device-id"))  # supported explicit object

@dataclass(frozen=True, slots=True, kw_only=True)
class SafeRequest:
    device_id: str

SAFE_JOB = Job("probe.safe", request=SafeRequest, result=ManifestResult)

async def safe_static(jobs: SuperJobs) -> None:
    await jobs.client(SAFE_JOB).submit("device-id")  # intentional negative: reportArgumentType
```

Upgrade Pyright only by changing the pinned version in `tools/verify_contract_typing.py`
and refreshing expected diagnostics after review.

Historical probe notes: [research/historical-typing-and-validation-notes.md](research/historical-typing-and-validation-notes.md).

## Runtime lifecycle

Standalone workers typically `await jobs.serve()`. Embedded programs can use
`async with jobs:` and `await jobs.wait_until_stopped()` after publishing
readiness. Workers keep the runtime alive while consumers process work (see
[examples/contract_interface/worker.py](../examples/contract_interface/worker.py)).

`SuperJobs(transport=InMemoryTransport())` backs fast deterministic tests without
a broker.

## Worker discovery and presence

Producers inspect **live worker registrations** (ready, unexpired presence
leases) through the same validation path on the client and on
`SuperJobs.discovery`. `InMemoryTransport` provides a shared process-local
registry; `NatsJobBackend` stores complete registration snapshots in a dedicated
persistent JetStream KV bucket. See [worker-discovery.md](worker-discovery.md)
for registration, updates, deployment policy and permissions.

Declare optional application capability typing on the shared Job with
`Job(..., capabilities=LocaleCapability)`, as in the contract-only example:

```python
from superjobs import JobIdentity, SuperJobs
from superjobs_contract_example import LOCALE_DISCOVERY_JOB

async def inspect(jobs: SuperJobs) -> None:
    workers = await jobs.client(LOCALE_DISCOVERY_JOB).workers()
    offered = await jobs.discovery.jobs()
    raw = await jobs.discovery.workers(
        LOCALE_DISCOVERY_JOB.identity,
    )
```

- `WorkerRegistration[CapT].capabilities` is `CapT | None`; `None` means the
  worker published no capability value, not a decode failure.
- Jobs **without** `capabilities=` never fabricate application DTOs; envelopes
  that still carry application payloads fail with `CapabilityDecodeError`.
- `jobs.discovery.workers(JobIdentity)` returns `list[WorkerRegistration[RawCapabilities]]`
  for DTO-free inspection; malformed envelopes and unsupported envelope versions
  fail visibly.
- Snapshot reads use one evaluation time, exclude `expires_at <= now` and
  non-`ready` state unless `include_stale=True` (diagnostics only; stale rows
  are not offered workers).
- Serialized registration envelopes are limited to **32 KiB** by default;
  invalid or oversized writes leave the previous snapshot intact on replace.

Share one `InMemoryDiscoveryBackend` through
`InMemoryTransport(discovery_store=store)` when several runtimes must inspect the
same registry. Its injectable clock makes lease boundaries deterministic. Reads
capture a complete snapshot under the store lock; a concurrent replacement appears
entirely before or after that snapshot. This is process-local consistency, without
a transactional cluster-wide snapshot guarantee.

`PresenceConfig` controls the envelope size, read timeout (default **30 seconds**),
lease timeout (default **30 seconds**), renewal interval (default **10 seconds**),
and stale retention (default **24 hours** since the last acknowledged update).
Durations must be positive and finite; the lease must be at least three renewal
intervals. Stale inspection stops at the retention boundary, and explicit deletion
removes a registration immediately. Configure the shared store once so all runtimes
use the same policy.

The store exposes explicit `write_registration()` and `delete_registration()`
operations for integration and deterministic tests. A runtime publishes its
handlers after backend/provider readiness and capability validation, before
consuming work. Producer-only runtimes publish no registrations. Read failures and
timeouts raise `DiscoveryUnavailableError`; malformed envelopes raise
`DiscoveryEnvelopeError`; invalid application payloads raise `CapabilityDecodeError`
with worker and Job context. A successful read with no matching active entries
returns `[]`.

Keep the inferred capability-specific Job/client type to retain `CapT`. Passing it
through an older `Job[...]` or `JobClient[...]` annotation erases that information:
namespace queries then return `list[WorkerRegistration[object]]`, and legacy
client queries expose `Sequence[WorkerRegistration[object]]`. The runtime still
returns a list and validates the actual Job's declared capability type. This safe
fallback also preserves the existing generic annotation arities.

`jobs.worker(JOB)` returns a typed local handle with
`update_capabilities(value)` and `refresh_capabilities()`. `discovery=False`
disables automatic publication and local updates; explicit discovery reads retain
their ordinary backend semantics. Contract fingerprint enforcement and watches
remain separate follow-ups ([#56](https://github.com/vschroeter/superjobs/issues/56),
[#57](https://github.com/vschroeter/superjobs/issues/57)).
