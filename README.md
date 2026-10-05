# SuperJobs

**Pre-alpha.** Requires **Python 3.12+**.

SuperJobs provides typed, durable background-job executions over NATS JetStream.
NATS-backed producers and workers are **separate programs**; each needs a
**running JetStream-enabled NATS server** reachable from both (not bundled with
the library). In-memory transport, deterministic tests, and CLI **`run`** mode do
**not** require a broker. Domain terms live in [CONTEXT.md](CONTEXT.md). API
behavior, typing limits, developer checks, and measured verification are
documented under [docs/](docs/README.md).

Each NATS-backed process owns its own
`SuperJobs` runtime and broker connection. Both import a shared contract module
(for example `my_contracts.py` or an installable contract package) with job
constants and payload types; workers register handlers against those constants
without exposing implementation code to producers.

## 1. Job definition

Define payload types and `Job` constants in a package both sides import:

```python
from dataclasses import dataclass

from superjobs import Job


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
```

Omitted `request`, `result`, or `event` means **no slot** for that payload (not an
unspecified generic). See [docs/api.md](docs/api.md) for the four supported shapes,
strict validation, and static typing limits.

Runnable shared-contract layout: [examples/contract_interface/](examples/contract_interface/).

## 2. Handling

Workers register exactly one handler per job and always receive `JobContext`:

```python
import asyncio

from faststream.nats import NatsBroker

from superjobs import JobContext, SuperJobs
from my_contracts import MANIFEST_JOB, ManifestEvent, ManifestRequest, ManifestResult


async def main() -> None:
    from my_handlers import handlers

    jobs = SuperJobs(broker=NatsBroker("nats://localhost:4222"), handlers=handlers)
    await jobs.serve()
```

`my_handlers.py` (shared by the worker and CLI; not the contract package):

```python
from superjobs import Command, HandlerCatalog, JobContext

from my_contracts import (
    MANIFEST_JOB,
    ManifestEvent,
    ManifestRequest,
    ManifestResult,
)

handlers = HandlerCatalog()


@handlers.handler(
    MANIFEST_JOB,
    cli=Command(
        name="manifest",
        positional_fields=("device_id",),
    ),
)
async def manifest(
    request: ManifestRequest,
    context: JobContext[ManifestEvent],
) -> ManifestResult:
    await context.emit(ManifestEvent(stage="validated"))
    return ManifestResult(revision=f"{request.device_id}-r1")
```

See [docs/api.md](docs/api.md) and [examples/cli_registration/](examples/cli_registration/).
Legacy inline registration on `SuperJobs` remains available:

```python
async def main_legacy() -> None:
    jobs = SuperJobs(broker=NatsBroker("nats://localhost:4222"))
    register_handlers(jobs)
    await jobs.serve()


def register_handlers(jobs: SuperJobs) -> None:
    @jobs.handler(MANIFEST_JOB, concurrency=4)
    async def manifest(
        request: ManifestRequest,
        context: JobContext[ManifestEvent],
    ) -> ManifestResult:
        await context.emit(ManifestEvent(stage="validated"))
        return ManifestResult(revision=f"{request.device_id}-r1")
```

Handlers **must** take `JobContext`. Jobs without a request use `(context,) -> ...`.
Jobs without a final result type must return `None`. Jobs without an event slot
must not call `context.emit` with application events (logs and system observations
remain available).

## 3. Submit + Observation

Producers use `jobs.client(job)` after the runtime is started. Submission supports
explicit request objects, constructor keywords (when the job uses a `RequestJob`),
and `SubmitOptions` for execution identity and deadlines:

```python
import asyncio

from faststream.nats import NatsBroker

from superjobs import SubmitOptions, SuperJobs
from my_contracts import MANIFEST_JOB, ManifestRequest


async def main() -> None:
    jobs = SuperJobs(broker=NatsBroker("nats://localhost:4222"))
    await jobs.start()
    try:
        client = jobs.client(MANIFEST_JOB)
        handle = await client.submit(
            ManifestRequest(device_id="sensor-17"),
            options=SubmitOptions(idempotency_key="request-123"),
        )
        result = await handle.result()
        async for event in handle.events():
            print(event.sequence, event.data)

        again = await client.get(handle.job_id)
        print(await again.status(), await again.outcome())
    finally:
        await jobs.stop()


if __name__ == "__main__":
    asyncio.run(main())
```

`submit()` returns a reusable **client-side handle** to one execution. Its identity
lets another runtime reconstruct a handle and observe that execution later.

| API | Role |
| --- | --- |
| `await handle.result()` / `await handle` | Wait for the typed final result |
| `await handle.outcome()` | Terminal success, failure, or cancellation envelope |
| `await handle.status()` | Latest execution status snapshot |
| `async for event in handle.events(after=...)` | System events, logs, progress, and application events (ordered where retained); cursor replay under retention |
| `await handle.cancel()` | Cooperative cancellation request while execution is active |
| `await client.get(job_id)` / `client.handle(job_id)` | Reconstruct a handle after reconnect or in another process |

Executions are **at least once**; make external side effects idempotent. Outage and
retry semantics are summarized in [ADR 0005](docs/adr/0005-broker-outage-retry-and-optional-stress.md).
Cross-process contract fingerprints are **not** enforced yet ([ADR 0003](docs/adr/0003-cross-process-contract-compatibility.md)).

For deterministic tests without a broker:

```python
from superjobs import InMemoryTransport, SuperJobs

jobs = SuperJobs(transport=InMemoryTransport())
```

## 4. CLI (optional)

Install the optional CLI extra from a **repository checkout** (not PyPI):

```bash
pip install -e ".[cli]"
```

Applications build `JobCLI` from the same `HandlerCatalog` as the worker, expose
their own console entry point (for example `myapp = "myapp.cli:main"`), and set a
default NATS URL for remote `submit`. The library does not ship a global
`superjobs` executable.

| Command | Needs broker | Worker for execution |
| --- | --- | --- |
| `myapp run <command> …` | no | no (handler runs in the CLI process) |
| `myapp submit <command> …` | yes | yes when work must run (acceptance can succeed before a worker is up; queued work can outlive the CLI) |
| `myapp submit … --wait` | yes | yes (waits for final result; default 300 s client bound) |

`myapp` is **your** installed console script name (`[project.scripts]` → the same
`main()` below), not a library-provided executable. To try the snippet without
packaging, save section 1 as `my_contracts.py`, section 2’s `my_handlers.py` beside
it, the CLI block below as `myapp_cli.py`, and invoke `python myapp_cli.py …`.

Request input: flat Jobs with only supported scalar fields may use generated
options; **positionals require explicit** `positional_fields` at registration.
Otherwise use `--json`, `--input path`, or `--input -` (see
[docs/cli.md](docs/cli.md)). stdout
carries one JSON result or execution reference; stderr carries observations and
diagnostics. Exit `0` success, `1` runtime/transport failure, `2` usage,
`130` interrupt. Remote wait timeout or CLI interrupt **does not cancel**
accepted executions.

```python
from superjobs.cli import JobCLI

from my_handlers import handlers


def build_cli() -> JobCLI:
    return JobCLI(handlers=handlers, nats_url="nats://localhost:4222")


def main() -> int:
    return build_cli().main()


if __name__ == "__main__":
    raise SystemExit(main())
```

Remote `submit` uses the built-in NATS producer runtime (no custom factory in the
snippet above). Broker URL resolution: `submit --nats-url`, then
`SUPERJOBS_NATS_URL`, then `JobCLI(nats_url=...)`, then `nats://localhost:4222`.

Example invocations (direct script; replace with `myapp` after install):

```bash
python myapp_cli.py run manifest sensor-17
python myapp_cli.py run manifest --json '{"device_id":"sensor-17"}'
python myapp_cli.py submit manifest sensor-17
python myapp_cli.py submit --nats-url nats://broker:4222 manifest sensor-17 --wait
python myapp_cli.py submit manifest sensor-17 --wait --wait-timeout 30
```

The catalog-backed `manifest` command is the same name for local `run` and remote
`submit` (handler runs in-process for `run`, on a worker for `submit`). A separate
submit-only CLI imports just the contracts and adds `remote_only=True` commands:

```python
from superjobs.cli import JobCLI
from my_contracts import MANIFEST_JOB

cli = JobCLI(nats_url="nats://localhost:4222")
cli.add("manifest", MANIFEST_JOB, remote_only=True)
```

Optional `local_runtime_factory` and
`remote_runtime_factory` async context managers remain for advanced transport or
dependency wiring when the built-in runtimes are not enough. Detail:
[docs/cli.md](docs/cli.md). Examples:
[examples/cli_registration/](examples/cli_registration/README.md),
[examples/contract_interface/](examples/contract_interface/README.md).

Further API detail: [docs/api.md](docs/api.md). Checks and CI:
[docs/development.md](docs/development.md). Measured results:
[docs/verification.md](docs/verification.md).
