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

NATS integration tests are marked with `pytest.mark.nats` and use
`NATS_URL` when set.
