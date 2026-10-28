# Worker discovery and presence

Declare the optional capability type beside the Job in the shared contract
package. Producers import the Job and payload classes without importing worker
code. Workers register concrete values or zero-argument sync/async factories:

```python
from superjobs import HandlerCatalog, JobContext, SuperJobs
from superjobs_contract_example import (
    LOCALE_DISCOVERY_JOB, LocaleCapability, ManifestRequest, ManifestResult,
)

handlers = HandlerCatalog()

def capabilities() -> LocaleCapability | None:
    return LocaleCapability(locale="de")

@handlers.handler(LOCALE_DISCOVERY_JOB, capabilities=capabilities)
async def manifest(request: ManifestRequest, context: JobContext[None]) -> ManifestResult:
    return ManifestResult(revision=request.device_id)

async def serve(jobs: SuperJobs) -> None:
    async with jobs:
        local = jobs.worker(LOCALE_DISCOVERY_JOB)
        await local.update_capabilities(LocaleCapability(locale="en"))
        await local.refresh_capabilities()
```

Construct the runtime with `handlers=handlers`. A static value can replace the
factory in the registration. Catalog import and CLI help do not evaluate
factories or enter managed providers. Initial evaluation happens after providers
are ready. The shared contract declaration does not require a concrete value;
`None` is valid absence. The executable contract example is
[`worker_import_probe.py`](../examples/contract_interface/worker_import_probe.py).

`update_capabilities()` replaces the whole optional snapshot and keeps the
original factory. `refresh_capabilities()` evaluates that factory again; a
registration without a factory raises a visible error. Updates require an active
handler in this runtime. Validation and encoding use the declared Job capability
type and codec, including strict decoding of the encoded value. Invalid values
and failed writes do not advance the local accepted snapshot. Renewal reuses the
accepted payload rather than evaluating a factory again.

Inferred Jobs retain the capability type in the local handle. Widening a Job to
the legacy base annotation loses that type; the local handle then accepts only
`None`, rather than allowing unchecked arbitrary replacement values. Existing
request/result/event generic arities remain valid. Discovery snapshot typing is
described in [api.md](api.md#worker-discovery-and-presence).

## Lifecycle and identity

Each runtime has a random `worker_id`, shared by its Job registrations and stable
across reconnects and stop/start cycles of that object. A new runtime object gets
a different ID. Each serving generation has a new `registered_at`; successful
renewals, updates and outage recovery preserve that time and advance acknowledged
`last_seen_at`/`expires_at`. Presence renewal is independent of Attempt heartbeats.

Ready publication precedes consumption. Startup failures roll back publication
and managed resources. Shutdown fences updates and renewal, publishes draining,
stops admission and deletes presence before waiting for active Attempts to finish.
Registry failures remain visible while bounded cleanup continues; unreachable
deletion relies on lease expiry. Presence is an advisory recently acknowledged
lease, without routing or guaranteed future availability.

## Local worker handle

`jobs.worker(job)` returns a runtime-local handle for one registered handler.
It complements producer-side `client(job).workers()` snapshot reads: the handle
observes **this runtime's** registry row and local readiness decisions without
requiring repeated full snapshot enumeration while the runtime is serving.

### Readiness withdrawal (local)

`readiness_withdrawn` and `wait_until_readiness_withdrawn()` track a **local**
decision to stop admitting new work on that handler (for example after calling
the supervisor's readiness withdrawal path during draining). The wait completes
when the runtime records withdrawal; it does not poll the discovery store.
Measured checks cover completion without snapshot reads and reset across a new
serving generation after `stop()`/`start()`.

### Targeted lookup

`read_own_registration()` performs one bounded `read_registration(worker_id,
job)` on the discovery backend when that method exists, decodes the envelope for
the handler's Job, and applies the same active/stale filtering as other registry
reads unless `include_stale=True`. A confirmed missing key returns `None`.
Registry transport failures raise `DiscoveryUnavailableError`, `ConnectionError`,
or `OSError` rather than being treated as confirmed absence.

`reconcile_own_registration()` re-reads the row, updates local observation
state, and returns the typed registration. On registry unavailability it marks
the row as not actively offered locally, then **re-raises** the error so callers
can distinguish outage from a confirmed delete.

Custom backends that implement only the three core methods (`read_snapshot`,
`write_registration`, `delete_registration`) continue to work for publication and
producer snapshot reads. Targeted lookup raises
`WorkerPresenceUnavailableError` when `read_registration` is absent.

### Own-registration observation

When discovery is enabled and the backend supports observation, the runtime
starts a per-handler background monitor after successful startup. Support
requires callable `watch_registration` **and** `read_registration`, unless the
backend sets `own_registration_observation = False` to opt out explicitly.
Backends with only one of those methods, or legacy three-method stores, do not
start the monitor; automatic publication and renewal behave as before.

The monitor loop:

1. Calls `reconcile_own_registration()` once per iteration before subscribing.
2. Subscribes to `watch_registration(self.worker_id, job.identity)` for live
   payload updates (delete/absent payloads surface as `None`).
3. Runs a lease tick task that re-evaluates `expires_at` against the presence
   clock at half the configured renewal interval (minimum 50 ms) using the last
   stored registration bytes, so logical expiry can be observed without a new
   store read when the watch is quiet.

`own_registration_active` reflects **`is_actively_offered`** on the last
observation: `READY` state and `expires_at` strictly after the evaluation
instant. It is not inferred from local acceptance alone when the store cannot be
read. Local draining publication updates the store and watch stream; measured
checks show withdrawal and external delete both driving `own_registration_active`
false without snapshot polling.

`wait_until_own_registration_inactive()` waits on a local event fed by that
observation path. It requires observation support; otherwise it raises
`WorkerPresenceUnavailableError` immediately. When the row is already inactive,
the wait returns at once.

**Broker unavailability versus confirmed absence.** A successful read or watch
that yields no row sets inactive and unblocks waiters. Registry unavailability
during reconcile marks inactive and wakes waiters, but reconcile re-raises the
error; the monitor keeps running and sleeps for one renewal interval before
retrying. Measured checks cover `ConnectionError`/`OSError` on reconcile and
continued monitor life after marking inactive. This does **not** prove delivery
latency bounds across all failure modes.

**Stop and idle behavior.** Runtime `stop()` cancels observation tasks; measured
checks assert watch tasks finish. While serving, the monitor does not call
`read_snapshot` on idle; regression tests count snapshot reads during delete,
withdrawal, and lease-expiry scenarios.

Example (typed handle; requires observation-capable backend such as in-memory
tests or NATS integration fixtures):

```python
async with jobs:
    handle = jobs.worker(MY_JOB)
    assert handle.own_registration_active
    row = await handle.read_own_registration()
    assert row is not None and row.worker_id == jobs.worker_id
    await handle.wait_until_own_registration_inactive(timeout=30.0)
```

Legacy custom store without observation:

```python
# Still publishes via write_registration; no background monitor.
handle = jobs.worker(MY_JOB)
await handle.read_own_registration()  # WorkerPresenceUnavailableError
await handle.wait_until_own_registration_inactive()  # same
```

### NATS watches and transient consumers

Snapshot enumeration and other **short-lived** `watchall`/`watch` uses create
JetStream ephemeral consumers. Cleanup always attempts bounded `watcher.stop()`
first even when consumer metadata lookup, stop, or delete fails; strict paths
surface errors after that stop attempt. After a successful stop, the backend
deletes the consumer reported by the watch subscription when it is **not**
durable (durable consumers are never deleted by this path). Long-lived
`watch_registration` streams used by the own-registration monitor release their
consumer on watch teardown; they are not torn down on idle.

Repeated `read_snapshot()` on a live NATS fixture has a measured upper bound on
consumer count growth; that check does not cover every concurrent cancellation
pattern.

NATS KV `watch_registration` treats an initial empty snapshot sentinel before
any key observation as absent at watch establishment; later `None` entries reflect
live deletes. Broker disconnects during an established watch surface as registry
unavailability on subsequent operations rather than silent confirmed absence.

**Reconnection.** Observation retry timing uses `PresenceConfig.renewal_interval`
after unavailable errors. NATS client reconnect and redelivery semantics depend
on broker connection settings (integration tests often pin
`max_reconnect_attempts=0`). SuperJobs does not add a separate discovery
reconnect policy beyond bounded operation timeouts and the monitor backoff loop.

`SuperJobs(discovery=False)` disables automatic publication/renewal and the local
worker handle. It does not change explicit `jobs.discovery.*` or
`client.workers()` reads, or affect another runtime sharing the transport.
Producer-only runtimes do not publish presence. Third-party transports without
the optional discovery seam can serve with this opt-out; explicit reads on such
transports raise `UnsupportedDiscoveryBackendError`.

## Registry policy and permissions

`PresenceConfig` defaults to a 10-second renewal interval, 30-second logical
lease, 24-hour stale retention, 32 KiB complete envelope limit and a 30-second
operation timeout. Durations are positive and finite; lease timeout must be at
least three renewal intervals. Configure the shared backend/store consistently
for workers and producers. Retention and maximum envelope size are deployment
policy, not values a worker may rewrite on an existing bucket.

NATS snapshot and short-lived watch cleanup unsubscribes via `watcher.stop()`
(with bounded waits as above), then removes matching **ephemeral** JetStream
consumers when consumer metadata is available; durable consumers are left
untouched. Initializer cleanup can add up to the smaller of twice the operation
timeout and the timeout plus one second; stopping the watcher has a separate bound
of at most one second.

NATS uses a dedicated namespace-scoped KV bucket with file storage and history
one. `NatsJobBackend(..., presence_config=config, discovery_provision=False)`
requires an existing compatible bucket. The default permits provisioning an
absent bucket when server authorization allows it. Registry access is lazy;
ordinary backend startup does not provision the discovery bucket. Existing
incompatible history, storage, TTL or envelope limits raise
`DiscoveryConfigurationError` without reconfiguration or deletion.

The additional registry permissions include stream information/configuration
reads, transient consumer creation for snapshot enumeration, KV value reads and
reply inbox subscriptions. Publication/deletion requires KV subject publication;
provisioning additionally requires creating the discovery stream. Exact API
subjects depend on the configured bucket and direct-read support. Apply
deployment-scoped grants rather than broad credentials; denied requests can
surface as timeouts. These requirements are separate from execution/completion
storage permissions. See the official [NATS KV documentation](https://docs.nats.io/learn/key-value)
and [subject authorization documentation](https://docs.nats.io/learn/security/authorization).

Full name/version and worker ID are encoded reversibly into KV-safe keys.
Unversioned Jobs remain distinct from every literal version, including versions
containing dots. The storage TTL removes stale records eventually; active reads
always evaluate `expires_at`, never substitute TTL for the presence lease.
Graceful deletion and history one do not provide a permanent audit trail.

NATS snapshots enumerate then read current entries at one captured evaluation
time. Concurrent deletion/expiry is benign; concurrent updates may appear before
or after an individual read. This is a bounded view, without a transactional
cluster-wide snapshot. InMemory captures its whole snapshot under a store lock.
UTC leases assume adequately synchronized producer/worker clocks; local renewal
scheduling uses the event loop's monotonic clock.

## Verification scope

Current implementation covers snapshot persistence and automatic lifecycle
publication, updates and refresh ([#53](https://github.com/vschroeter/superjobs/issues/53),
[#54](https://github.com/vschroeter/superjobs/issues/54)). Local readiness
withdrawal, targeted `read_own_registration`, own-registration observation via
watch plus lease tick, legacy three-method backend compatibility, partial-backend
opt-out, NATS ephemeral consumer cleanup helpers, and bounded repeated snapshot
consumer counts are covered by in-memory and NATS-marked tests in this
repository; they are not yet recorded as separate verification ledger entries.
Measured checks are recorded in [verification.md](verification.md). Separate
installed applications with hard worker kills and persistent broker/process
recovery remain
[#55](https://github.com/vschroeter/superjobs/issues/55). Contract agreement and
watches remain separate; discovery does not prove execution-contract compatibility.
