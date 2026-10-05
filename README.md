# SuperJobs

**Pre-alpha · Python 3.12+ · Interface under active development.**

SuperJobs runs typed asynchronous jobs over **NATS JetStream**. Producers submit
work; workers execute it and publish results and observations. Both share job
definitions and payload types, so producers can use contracts independently of
handler code.

## Setup

From a repository checkout:

```bash
uv sync
```

Use a **JetStream-enabled NATS server** reachable from both the worker and
producer processes. These examples default to `nats://localhost:4222`.
To use another broker, set `SUPERJOBS_NATS_URL` in each process's environment.

Save the following four files together. In a larger application, package the
contracts separately so producers can install them without worker dependencies.

## Shared contract (`my_contracts.py`)

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

## Handlers (`my_handlers.py`)

Define the handler once in a catalog that the worker consumes:

```python
from superjobs import HandlerCatalog, JobContext

from my_contracts import (
    MANIFEST_JOB,
    ManifestEvent,
    ManifestRequest,
    ManifestResult,
)

handlers = HandlerCatalog()


@handlers.handler(MANIFEST_JOB)
async def manifest(
    request: ManifestRequest,
    context: JobContext[ManifestEvent],
) -> ManifestResult:
    await context.emit(ManifestEvent(stage="validated"))
    return ManifestResult(revision=f"{request.device_id}-r1")
```

`JobContext` lets the handler publish events, logs, and progress for its execution.

## Worker (`worker.py`)

```python
import asyncio
import os

from faststream.nats import NatsBroker

from superjobs import SuperJobs

from my_handlers import handlers

NATS_URL = os.environ.get("SUPERJOBS_NATS_URL", "nats://localhost:4222")


async def main() -> None:
    jobs = SuperJobs(broker=NatsBroker(NATS_URL), handlers=handlers)
    await jobs.serve()


if __name__ == "__main__":
    asyncio.run(main())
```

`serve()` owns startup, waiting, and graceful shutdown for the worker process.

## Producer (`producer.py`)

The producer imports the shared contract:

```python
import asyncio
import os

from faststream.nats import NatsBroker

from superjobs import SuperJobs

from my_contracts import MANIFEST_JOB

NATS_URL = os.environ.get("SUPERJOBS_NATS_URL", "nats://localhost:4222")


async def main() -> None:
    async with SuperJobs(broker=NatsBroker(NATS_URL)) as jobs:
        client = jobs.client(MANIFEST_JOB)
        handle = await client.submit(device_id="sensor-17")
        result = await handle.result()
        print(result.revision)


if __name__ == "__main__":
    asyncio.run(main())
```

Run the worker and producer in **two terminals** against the same broker:

```bash
uv run python worker.py
```

```bash
uv run python producer.py
```

The producer prints `sensor-17-r1`. `submit()` returns a
**handle** to one execution; you can observe status, recorded observations, and
outcomes later or from another process. Details: [docs/api.md](docs/api.md).

Executions are delivered **at least once**, so make external side effects
idempotent. Accepted work can outlive the submitting process. Retry and outage
behavior: [ADR 0005](docs/adr/0005-broker-outage-retry-and-optional-stress.md).

## CLI (optional)

Install the CLI extra, then expose the **same** handler catalog the worker uses:

```bash
uv sync --extra cli
```

On the existing `manifest` handler in `my_handlers.py`, add `cli="manifest"` to
the decorator (same function body as above):

```diff
-@handlers.handler(MANIFEST_JOB)
+@handlers.handler(MANIFEST_JOB, cli="manifest")
```

`myapp_cli.py`:

```python
from superjobs.cli import JobCLI

from my_handlers import handlers


def main() -> int:
    return JobCLI(handlers=handlers).main()


if __name__ == "__main__":
    raise SystemExit(main())
```

Remote `submit` uses the built-in NATS producer runtime. Optional default URL:
`JobCLI(handlers=handlers, nats_url="nats://broker:4222")`. Resolution order for
`submit`: `--nats-url`, then `SUPERJOBS_NATS_URL`, then `JobCLI(nats_url=...)`, then
`nats://localhost:4222`.

With the NATS worker running:

```bash
uv run python myapp_cli.py submit manifest --device-id sensor-17 --wait
uv run python myapp_cli.py submit --nats-url nats://broker:4222 manifest --device-id sensor-17 --wait
```

For local execution without a broker:

```bash
uv run python myapp_cli.py run manifest --device-id sensor-17
```

When packaging the app, map `main()` through a `[project.scripts]` entry.
Input forms, exit codes,
composition, resources, and contracts-only CLIs:
[docs/cli.md](docs/cli.md). Runnable multi-package layout:
[examples/contract_interface/](examples/contract_interface/).

For tests without a broker, use `InMemoryTransport`; see
[docs/development.md](docs/development.md).

Further API detail: [docs/api.md](docs/api.md). Domain terms:
[CONTEXT.md](CONTEXT.md). Checks and CI:
[docs/development.md](docs/development.md). Measured results:
[docs/verification.md](docs/verification.md).
