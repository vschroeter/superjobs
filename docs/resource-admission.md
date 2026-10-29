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
| Snapshots | `jobs.resources.snapshot()` is diagnostic; records are not TTL-trimmed into automatic release. |
| Waiting | `JobState.WAITING_FOR_RESOURCE` (`waiting_for_resource`); waiting does not spend an inference retry merely because another worker holds the device. |
| Waiting observation | `jobs.resources.waiting_executions()` requires a started transport (normal `start()` may provision JetStream buckets); it does not inspect a fresh broker without that provisioning path. The call scans durable completion records for `waiting_for_resource` only— not `pending` deliveries, not admission claims alone, and not executions before admission is stored on the record. `WaitingExecutionCompleteness` is coverage of matching records within bounded time/key limits, not every execution actually blocked on a resource; views may be truncated, partial, or stale under broker races. The read path is observation-only (no admission writes or completion KV puts). NATS enumerates completion keys via a short-lived ephemeral `watchall` snapshot capped by `max_scan_keys`; in-memory transport walks stored executions up to the same cap without copying the full map. |
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
  `supports_waiting_execution_observation()`.

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
namespace. Verify the mixed Windows-controller/Linux-worker application and
supervisor lifecycle before removing its filesystem guards.

## Related checks

| Artifact | Role |
| --- | --- |
| [development.md](development.md) | Contract typing and `dev_check` commands |
| [verification.md](verification.md) | Dated measured results |
| `tools/verify_resource_admission.py` | Cross-process admission scenarios; required in `dev_check integration` / `full` |
