# SuperJobs

SuperJobs provides typed, durable background-job executions over NATS JetStream.
Handlers and clients use the same `SuperJobs` runtime, while a job definition
keeps its name, version, request, result, and optional event contracts together.

## Define and handle a job

```python
from faststream.nats import NatsBroker
from pydantic import BaseModel

from superjobs import Job, JobContext, SuperJobs


class GenerateRequest(BaseModel):
    count: int


class GenerateResult(BaseModel):
    generated: int


generate = Job(
    "example.generate",
    version="v1",
    request=GenerateRequest,
    result=GenerateResult,
)
broker = NatsBroker("nats://localhost:4222")
jobs = SuperJobs(broker=broker)


@jobs.handler(generate, concurrency=4)
async def generate_handler(
    request: GenerateRequest,
    context: JobContext,
) -> GenerateResult:
    await context.log("generation started")
    return GenerateResult(generated=request.count)
```

## Submit and observe

```python
async with jobs:
    client = jobs.client(generate)
    handle = await client.submit(
        GenerateRequest(count=10),
        idempotency_key="request-123",
    )
    result = await handle

    async for event in handle.events():
        print(event.sequence, event.data)
```

`submit()` returns a reusable handle with `status()`, `result()`, `outcome()`,
`cancel()`, and replayable `events()`. Execution is at least once: handlers
should make external side effects idempotent. Progress is latest-value-wins;
logs and intermediate events remain ordered and are batched internally.

For deterministic tests, inject `InMemoryTransport`:

```python
jobs = SuperJobs(transport=InMemoryTransport())
```

NATS integration tests are marked with `pytest.mark.nats`. By default the harness
starts an isolated JetStream server (pinned `nats-server` v2.15.0). Set `NATS_URL`
to use an external broker, or `NATS_EXECUTABLE` for an offline binary path. See
[docs/design/nats-test-harness.md](docs/design/nats-test-harness.md).

## Developer checks

Routine fast checks (no broker):

```bash
python -m scripts.dev_check fast
```

Full local verification (typing, NATS, installed cross-program and recovery runners):

```bash
python -m scripts.dev_check full
```

Required PR gates and the CI matrix are documented in
[docs/design/dev-checks-verification.md](docs/design/dev-checks-verification.md).
CI-style isolated runs (example Python **3.12**; match the matrix minor on other jobs):

```bash
uv run --isolated --no-project --python 3.12 --with-editable . --with "pytest>=9.1.1" --with "pytest-asyncio>=1.4.0" python -m scripts.dev_check fast
```

```bash
uv run pytest -m nats
```

Verify installed shared contracts between separate producer and worker processes:

```bash
uv run python tools/verify_cross_program.py --python 3.12 --python 3.14 --artifact-dir dist/verification/issue12
```

This gate always owns its broker and fails missing infrastructure. See
[cross-program verification](docs/design/cross-program-verification.md) for scenarios,
isolation checks, failure diagnostics and measured results.

Verify worker crash recovery before and after durable completion:

```bash
uv run python tools/verify_worker_recovery.py --python 3.12 --python 3.14 --artifact-dir dist/verification/issue13
```

See [worker recovery verification](docs/design/worker-recovery-verification.md) for scenarios,
checkpoints, and evidence retention.

Verify execution and result persistence across a NATS broker restart:

```bash
uv run python tools/verify_broker_restart.py --python 3.12 --python 3.14 --artifact-dir dist/verification/issue14
```

See [broker restart verification](docs/design/broker-restart-verification.md) for the scenario,
broker resource evidence, and failure diagnostics.
