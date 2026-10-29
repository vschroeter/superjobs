# Resource admission

Broker-backed **exclusive resource admission** (v1): one capacity-one device key plus a
fenced **execution claim** before resource-consuming handler work runs. Tracked in
[issue #61](https://github.com/vschroeter/superjobs/issues/61). Domain terms:
[GLOSSARY.md](../GLOSSARY.md). Accepted decision:
[adr/0009-resource-admission-and-confirmed-recovery.md](adr/0009-resource-admission-and-confirmed-recovery.md).
Measured verification lives only in [verification.md](verification.md).

Generic SuperJobs does **not** import Docker or engine-specific supervisors.
Confirmed termination requires **caller-validated evidence** that identifies the
terminated processes (for example supervisor PID/cgroup/container scope), not a bare
boolean.

## v1 limits

| Topic | Behavior |
| --- | --- |
| Scheduler | No general multi-resource scheduler; exclusive v1 only (`V1_EXCLUSIVE_CAPACITY`). |
| Keys | Caller selects stable **host + physical device** identity; shared across Job identities on the same device. |
| Delivery | **At-least-once**; after an undurable worker crash, handlers and external effects must stay idempotent. |
| Physical checks | No generic GPU/OS memory or process verification; only durable ownership and application cleanup/termination paths. |
| Snapshots | `jobs.resources.snapshot()` is diagnostic; records are not TTL-trimmed into automatic release. NATS key scans clean up their ephemeral consumer on successful reads. If the broker fails during cancellation, cleanup is best effort and immediate consumer removal cannot be confirmed. |
| Waiting | `JobState.WAITING_FOR_RESOURCE` (`waiting_for_resource`); waiting does not spend an inference retry merely because another worker holds the device. |
| Waiting observation | Bounded read-only scan of completion records. Listed entries are durable ``waiting_for_resource`` records with a persisted requested resource key, labeled ``WaitingExecutionEvidence.PERSISTED_HISTORICAL`` (historical evidence only; matching attempt or invocation metadata does not prove the writer is alive). Any scanned ``waiting_for_resource`` or ``pending`` record marks the view ``partial`` because liveness cannot be verified. Legacy waiting records without a key are omitted and also mark ``partial``. ``pending`` records never appear as entries. ``jobs.resources.waiting_executions()`` uses the started runtime transport (normal ``start()`` may provision JetStream buckets). ``read_waiting_executions_from_broker()`` opens existing completion KV read-only without provisioning stores; a missing completion bucket yields ``WaitingExecutionUnavailableError`` (absence does not prove an unused namespace). An existing empty completion bucket can yield an empty complete view. Scans are non-atomic; ``evaluated_at`` is when the scan started; stored evidence may be older and races can yield stale, partial, truncated, or empty views distinct from ``WaitingExecutionUnavailableError``. Observation never acquires, releases, reconciles, or writes admission or completion state. NATS uses a short-lived ephemeral ``watchall`` snapshot capped by ``max_scan_keys``. |
| Heartbeat / TTL | Uncertainty retains ownership; no takeover from elapsed heartbeat or presence expiry alone. |
| Providers | Process-lifetime **provider + `resources=`** is rejected before handler entry (v1). |
| Local CLI | Resource-bound handlers are **rejected** for local `run` paths that skip broker admission; **remote `submit`** remains available. |

Stale owners are fenced with **admission tokens** (generation + token) so delayed
operations cannot release or finalize work for a newer owner.

Use a persisted host identity and physical GPU UUID or PCI identity for keys.
Container-local device ordinals can alias the same GPU; normalize those names in
application configuration so engines sharing a device share the same key.

## Public API

### `superjobs.resources`

Controller and contract packages import admission types without worker handlers:

- Declarations: `ResourceRequirement`, `CapacityDeclaration`,
  `parse_exclusive_resource_requirement`, `V1_EXCLUSIVE_CAPACITY`.
- Ownership: `AdmissionOwner`, `AdmissionSnapshot`, `StoredResourceRecord`,
  `WorkerIncarnation`, `AdmissionFencing`.
- Recovery: `TerminationConfirmation` (`confirmed=True`, non-empty `evidence`).
- Stores: `InMemoryResourceAdmissionStore`, `NatsResourceAdmissionStore`,
  `ResourceAdmission`.
- Observation: `WaitingExecutionView`, `read_waiting_executions()`,
  `read_waiting_executions_from_broker()`, `supports_waiting_execution_observation()`.

### Worker runtime

- `SuperJobs(..., resource_capacities={key: 1})` declares capacities this worker may offer.
- `HandlerCatalog.handler(job, resources={key: 1})` and
  `catalog.register(job, callback, resources={key: 1})` bind requirements.
- Runtime mirrors the same with `@jobs.handler(..., resources=…)` and
  `jobs.register(..., resources=…)`.
- `jobs.resources` exposes `snapshot()`, `waiting_executions()`, and `resolve_terminated_incarnation()` on
  runtimes backed by an admission store.
- `jobs.worker_incarnation` returns the immutable `WorkerIncarnation` used by
  resource admission for the current serving lifetime. It raises `RuntimeError`
  before startup and after shutdown; `worker_id` remains stable across restarts,
  while `incarnation_id` changes for each successful serving lifetime. The
  returned frozen value is detached from the runtime; a captured earlier value
  remains the identity of its original lifetime.

A child process can report the value to its trusted supervisor over an
application-owned channel:

```python
await jobs.start()
try:
    await report_to_parent(jobs.worker_incarnation.model_dump(mode="json"))
    await jobs.wait_until_stopped()
finally:
    await jobs.stop()
```

`report_to_parent` represents application IPC. The supervisor must still validate
which process it controls and confirm termination independently; an incarnation
snapshot, heartbeat timeout, or presence expiry does not prove that work stopped
or authorize resource takeover.

### Handler cleanup (fail-closed)

Resource-bound handlers must confirm cleanup on `JobContext`:

- `context.acknowledge_resource_cleanup()` — call **last** after local teardown
  succeeds (including in `finally` when inference raised, so a safe retry can run).
- `context.resource_cleanup_failed(reason: str)` — optional when teardown fails; do
  **not** acknowledge; re-raise to retain ownership.

Returning normally acknowledges that work and cleanup finished. On an error path,
call the explicit acknowledgement after cleanup; a generic exception without it
retains the reservation until cleanup or confirmed termination. Initialization and
model load belong **inside** the admitted callback, not in import-time or provider
setup.

Timeouts and shutdown request cooperation; they never prove a native call stopped.
SuperJobs drains an admitted callback before release, so an uncooperative native
call can outlive a graceful shutdown timeout. A supervisor must terminate the
process and confirm that termination before administrative recovery.

Broker uncertainty withdraws readiness for new local work while accepted
invocations reconcile. Restart the serving lifetime to offer new work again after
the broker recovers; retained unavailable ownership still requires cleanup or
confirmed termination. The in-memory adapter coordinates runtimes sharing the
same store in one process; use NATS across independently installed programs.

Runnable layout (same directory as typing fixtures):

| File | Role |
| --- | --- |
| [contract.py](../examples/contract_interface/typing/resource_positive/contract.py) | Job + payload types (producer-safe) |
| [handlers.py](../examples/contract_interface/typing/resource_positive/handlers.py) | `HandlerCatalog` + `resources=` + cleanup |
| [worker.py](../examples/contract_interface/typing/resource_positive/worker.py) | `NatsBroker` worker with `resource_capacities` |
| [controller.py](../examples/contract_interface/typing/resource_positive/controller.py) | `submit`, snapshot, termination confirmation |

### Controller recovery

```python
from superjobs.resources import TerminationConfirmation

snapshot = await jobs.resources.snapshot()
# inspect snapshot.resources (diagnostic; not TTL-based release)

confirmation = TerminationConfirmation(
    incarnation=owner.incarnation,
    confirmed=True,
    evidence="supervisor:pid=4242 cgroup=engine-a scope exited",
)
result = await jobs.resources.resolve_terminated_incarnation(confirmation)
```

Validate evidence in application code before trusting it.

## Typing consumers

Pyright **1.1.414**, `basic`, static **3.12**:

- `examples/contract_interface/typing/resource_positive/`
- `examples/contract_interface/typing/resource_negative/`

Gate: `uv run python tools/verify_contract_typing.py` (see [development.md](development.md)).

## Samplegen migration (downstream)

Admission replaces shared-filesystem **device/batch file guards** only. Samplegen
keeps global FIFO scheduling, result validation, atomic publication, and SQLite
ingestion. Controller `control/writer.lock` stays separate from device admission.

Stop active work, drain every old consumer, deploy coordinated resource-bound
workers/controllers, and reconcile existing ownership before removing guards.
Every consumer of a resource-bound Job must follow the same admission policy;
older workers do not acquire these reservations. Keep claims in execution
provenance outside frozen generation inputs. Offline probes need the same NATS
account/domain and resource-admission namespace. Verify the mixed
Windows-controller/Linux-worker application and supervisor lifecycle before
removing its filesystem guards.

## Related checks

| Artifact | Role |
| --- | --- |
| [development.md](development.md) | Contract typing and `dev_check` commands |
| [verification.md](verification.md) | Dated measured results |
| `tools/verify_resource_admission.py` | Cross-process admission scenarios; required in `dev_check integration` / `full` |

## Shared resource-admission namespace (optional)

Work transport settings (`NatsQueueConfig.subject_prefix`, streams, completion/idempotency
buckets, worker discovery subjects) can differ per application queue while **resource
admission** KV state is shared when every participant uses the same broker account,
JetStream domain, **`resource_namespace`**, and **resource key**. Matching resource keys
alone is insufficient when admission buckets still derive from different work prefixes.

```python
from superjobs import NatsQueueConfig

work_a = NatsQueueConfig(
    stream="work_a_stream",
    observation_stream="work_a_observations",
    subject_prefix="work_a",
    completion_bucket="work_a_completions",
    idempotency_bucket="work_a_idempotency",
    resource_namespace="physical_host",
)

work_b = NatsQueueConfig(
    stream="work_b_stream",
    observation_stream="work_b_observations",
    subject_prefix="work_b",
    completion_bucket="work_b_completions",
    idempotency_bucket="work_b_idempotency",
    resource_namespace="physical_host",
)
```

Omit `resource_namespace` (default `None`) to keep the previous behavior: admission
storage derives from each queue's `subject_prefix`. Explicit values must match a
fail-closed token grammar (dot-separated `[A-Za-z0-9_-]` segments; no wildcards,
whitespace, or silent normalization).

Cross-queue controllers can call `jobs.resources.snapshot()` and
`resolve_terminated_incarnation()` against the shared bucket even when their work
prefix differs. Removing a diagnostic work queue must not delete the shared admission
KV bucket or reset its fencing/revocation history. Admission records remain without
TTL; uncertainty never releases ownership based on presence expiry or disconnect alone.
Custom execution IDs must also be unique across the shared admission namespace;
otherwise its execution claims conservatively prevent overlapping invocations.

### Samplegen (external integration)

Samplegen currently has fixed transport/broker-check defaults, a probe using the
`superjobs` admission namespace, and a storage-based physical host identity. A
separately verified diagnostic adapter must consistently supply private work
settings, an explicitly shared resource namespace, and the existing host identity.
Its `serve_worker(..., runtime_factory=...)` and `Orchestrator(..., jobs,
broker_check=...)` seams permit programmatic integration without renaming engine
Jobs. No existing Samplegen CLI namespace override is implied by this library
change. Keep diagnostic databases/output private, models read-only, and termination
confirmation restricted to the supervisor's own verified child incarnation.
