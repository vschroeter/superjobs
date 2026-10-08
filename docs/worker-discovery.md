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

NATS snapshot cancellation also cleans up its transient consumer. Initializer
cleanup can add up to the smaller of twice the operation timeout and the timeout
plus one second; stopping the watcher has a separate bound of at most one second.

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
[#54](https://github.com/vschroeter/superjobs/issues/54)). Measured checks are recorded
in [verification.md](verification.md). Separate installed applications with hard
worker kills and persistent broker/process recovery remain
[#55](https://github.com/vschroeter/superjobs/issues/55). Contract agreement and
watches remain separate; discovery does not prove execution-contract compatibility.
