# SuperJobs CLI

Optional Typer-based commands for contract Jobs: **local `run`** (in-process,
no broker) and **remote `submit`** (NATS producer; worker runs elsewhere).
Applications own command names, handlers, runtime factories, and the process
entry point. There is no standalone `superjobs` executable shipped by the
library—your package exposes something like `myapp = "myapp.cli:main"` via
`[project.scripts]`.

Measured verification and slice history: [verification.md](verification.md).
Installed two-process proof (wheels, worker readiness, 23-scenario matrix):
[design/cli-application.md](design/cli-application.md).

## Installation

From a repository checkout (not published to PyPI):

```bash
uv sync --extra cli
```

Core `import superjobs` does not import Typer or `superjobs.cli`. The CLI extra
requires **Typer 0.27.2** or newer.

Runnable sketches:

| Example | Focus |
| --- | --- |
| [examples/cli_registration/](../examples/cli_registration/README.md) | Registration, input forms, lazy factory, local + remote factories |
| [examples/contract_interface/](../examples/contract_interface/README.md) | Installed CLI and worker console commands |

## Application layout

1. Shared **contract module** (`my_contracts.py` or installable package) defines
   `Job` constants and payload types (same as library producers/workers).
2. **Handler catalog** module (optional but recommended) defines
   `HandlerCatalog` bindings, execution policy, lazy `provider` context
   managers, and CLI `Command` metadata. Keep it separate from the contract
   package and from worker-only resources.
3. CLI module (`myapp_cli.py`) builds `JobCLI(handlers=catalog, nats_url=...)`,
   optionally `expose()` aliases, legacy `add()` for `remote_only` commands, and
   exposes `main()` for `[project.scripts]`.
4. **Worker** code stays in a separate package/process:
   `SuperJobs(..., handlers=catalog)` plus `await jobs.serve()` or an
   application-owned `async with` / `wait_until_stopped()` loop. Submit-only
   CLIs must not import worker implementations.

The layout below matches the [repository README](../README.md) contract
definition. Run it without packaging:

```bash
uv run python myapp_cli.py --help
uv run python myapp_cli.py run manifest sensor-17
uv run python myapp_cli.py run manifest --json '{"device_id":"sensor-17"}'
uv run python myapp_cli.py submit manifest-remote --device-id sensor-17
uv run python myapp_cli.py submit manifest-remote --device-id sensor-17 --wait --wait-timeout 30
```

`my_contracts.py` (same types and `MANIFEST_JOB` as the README):

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

`handlers.py` (shared by worker and CLI; not the contract package):

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
    return ManifestResult(revision=request.device_id)
```

`myapp_cli.py`:

```python
from superjobs.cli import JobCLI

from handlers import handlers
from my_contracts import MANIFEST_JOB


def build_cli() -> JobCLI:
    cli = JobCLI(handlers=handlers, nats_url="nats://localhost:4222")
    cli.add("manifest-remote", MANIFEST_JOB, remote_only=True)
    return cli


def main() -> int:
    return build_cli().main()
```

Remote `submit` uses the built-in producer runtime when `remote_runtime_factory`
is omitted. URL resolution order: `submit --nats-url`, then `SUPERJOBS_NATS_URL`,
then `JobCLI(nats_url=...)`, then `nats://localhost:4222`. Help and validation
errors do not open broker connections or enter lazy handler `provider` context
managers. A custom `remote_runtime_factory` cannot be combined with constructor
`nats_url` or `--nats-url`; the factory owns transport configuration.

After packaging, map `main` through `[project.scripts]` and invoke
`uv run myapp run manifest sensor-17`. For remote submit with an explicit broker:

```bash
uv run myapp submit --nats-url nats://broker:4222 manifest-remote --device-id sensor-17 --wait
```

Remote `submit` needs JetStream reachable at the selected URL and a **worker**
that registers the same `MANIFEST_JOB` handler when the execution must run.
Confirmed acceptance does not require a worker to already be running; without
`--wait` the CLI returns after acceptance and work can wait for worker
availability.

Omit `local_runtime_factory` to use the built-in isolated `InMemoryTransport`
factory for `run`. Supply `handler_factory=` (no arguments) to resolve a local
handler only when `run` selects that command. Registration checks the factory's
callability; its returned handler is resolved and validated during local execution.

## Registering commands

`JobCLI.add(command_name, job, ...)`:

| Mode | Registration | `run` | `submit` |
| --- | --- | --- | --- |
| Local | `handler=` or `handler_factory=` | In-process handler | Same request input; no handler invoked |
| Remote-only | `remote_only=True`, no handler | Exit `2` (usage) | NATS submission via built-in runtime or custom factory |

Local `add()` overloads require `RequestJob` / `NoRequestJob` so a concrete
handler signature can be checked. A widened `Job[Req, Res, Event]` annotation is
fine for **`remote_only=True`** (input planning only). With `handler=`, signature
checks run at `add()` time; with `handler_factory=`, checks run when `run`
selects the command. Contract-marked handlers (`@job.handler`) must match their
Job.

Command names use lowercase kebab-case. Duplicate names and the reserved names
`run`, `submit`, and `help` are rejected during registration.

### Typer composition

- `build_typer()` — standalone app with `run` and `submit` groups.
- `mount(host_app)` — attach those groups to your Typer app; fails if `run` or
  `submit` already exists on the host.
- `build_typer()` / `mount()` **snapshot** registrations and the effective
  runtime factories bound into callbacks; later `add()` or factory changes do not
  alter Typer objects already built.

`JobCLI.main(argv=None)` is synchronous for `if __name__ == "__main__"` and
**refuses** to run on a thread that already has a running asyncio event loop
(exit `2`).

### Runtime factories

Async context managers; the CLI enters the selected factory per command and
exits on every outcome. Factories must yield **fresh** runtimes (not a long-lived
worker). They are **not** called for `--help` or unselected commands.

| Factory | Used for | When omitted |
| --- | --- | --- |
| `local_runtime_factory` | `run` | Built-in in-memory `SuperJobs` per command |
| `remote_runtime_factory` | `submit` | Built-in NATS producer runtime when omitted |
| `nats_url` on `JobCLI` | `submit` (built-in mode) | Default before `SUPERJOBS_NATS_URL` / localhost |
| `submit --nats-url` | `submit` (built-in mode) | Overrides env and constructor URL |

Local `run` validates an isolated `InMemoryTransport` runtime without
pre-registered handlers, prior in-memory executions, or active in-memory work
consumers; shared or reused in-memory transports are rejected. NATS-backed
runtimes are rejected for `run`. Remote `submit` requires a broker-backed
`SuperJobs` without pre-registered handlers and without active broker work
subscribers. The CLI starts a yielded runtime when it is not already started and
cleans up its runtime and transport, including interrupted startup. The factory
must release its own application dependencies in `finally` on every path.

## Request input

Every Job with a request accepts **one** whole-request source (mutually
exclusive with field options and with each other):

```text
uv run myapp submit manifest-remote --json '{"device_id":"sensor-17"}'
uv run myapp submit manifest-remote --input request.json
uv run myapp submit manifest-remote --input -
```

`--input -` reads stdin. UTF-8 only. JSON rejects duplicate keys, malformed
syntax, non-finite numbers, and overflow. Failures print to stderr and exit `2`.

JSON is parsed to a logical tree and loaded through the Job’s payload adapter.
Transport codec (for example Msgpack) is independent: CLI input stays JSON.
Canonical **contract property names** apply in all forms; validation aliases are
not alternate CLI names.

**No-request** Jobs expose no payload flags. A **nullable** declared request
requires an explicit whole-request source, including `--json null`.

### Generated field options

Flat object schemas with only supported scalars get Typer options (underscores →
hyphens). Supported: strings, integers, finite floats, booleans, enums with
homogeneous scalar values. Booleans use `--field` / `--no-field`. Omitted fields
stay omitted until adapter validation applies defaults; explicit `false` and
values equal to defaults remain supplied.

Any nested object, list, nullable field, union, or unsupported property makes the
**entire** command JSON-only. Scalar roots, nullable roots, and schema-less custom
adapters also use JSON-only input. `readOnly` schema properties are not inputs.

In field mode, requiredness is enforced by payload validation; help lists required
fields, descriptions, declared defaults, and enum choices. An empty flat request
can use implicit `{}`. Default factories and validators do not run during help.

To customize the `manifest` registration above, replace its `cli.add()` call with:

```python
from superjobs.cli import CLIField, JobCLI

cli.add(
    "manifest",
    MANIFEST_JOB,
    handler=manifest_local,
    positional_fields=("device_id",),
    field_options={"device_id": CLIField(help="Device identifier.")},
)
```

`positional_fields` lists canonical names in order (not inferred from schema).
`field_options` maps canonical names to frozen `CLIField` (`option` renames the
long flag without leading dashes; `help` overrides description). Registration
rejects unknown keys, duplicate positionals, invalid names, collisions with
reserved options (`--json`, `--input`, `--wait`, `--wait-timeout`, `--help`), and
duplicate effective option names after normalization.
For a positional field, `CLIField.option` controls its displayed metavar instead
of creating a flag. Boolean negative-option names are checked for collisions too.

Field mode and whole-request sources cannot be mixed, including `false` and
default-equal values.

## Execution and I/O

| Mode | Broker | Handler | stdout | stderr |
| --- | --- | --- | --- | --- |
| `run` | none (in-memory) | CLI process | one JSON final result (`null` if no result slot) | observations + diagnostics |
| `submit` (no `--wait`) | NATS | worker (when work runs) | JSON execution reference | diagnostics |
| `submit --wait` | NATS | worker | JSON final result (`null` when the Job has no result slot) | observations + diagnostics |

Observations on stderr are compact JSON lines. Factory prints and default FastStream
access logs during remote submit are routed to stderr; use stderr for application
loggers in factories.

### Exit codes

Exported from `superjobs.cli`:

| Code | Meaning |
| --- | --- |
| `0` | Success |
| `1` | Runtime, transport, wait failure, unavailable execution, cleanup failure |
| `2` | Usage / validation (including active event loop) |
| `130` | CLI interruption |

Invalid input exits `2` **before** runtime startup and does not create executions.

### Local `run`

- Legacy `add(..., handler=...)` local commands use one attempt
  (`RetryPolicy(max_attempts=1)`). Catalog-backed `run` inherits each binding's
  configured retry and observation policy; concurrency still applies only within
  the isolated in-memory execution.
- No durable recovery after CLI exit.
- Interrupt requests cooperative cancellation on the execution; side effects are
  not rolled back.
- `LOCAL_RUN_SHUTDOWN_TIMEOUT_SECONDS` (**30 s**, exported) is the cooperative
  cancellation grace after interrupt; cleanup then uses a separate shared budget
  (handler, runtime, transport, factory) with the same nominal duration. Cleanup
  failure prevents success stdout and exits `1`. Interruption exits `130`
  including during startup/teardown.
- After requesting cancellation on owned work, the CLI waits briefly for
  acknowledgement, then diagnoses cancellation-resistant work and continues
  waiting for it rather than leaving owned tasks running. Sync handlers run in a
  thread and cannot be forcibly stopped; blocking event-loop code delays signal
  handling.
- SIGINT is registered on the main thread only during local run.

### Remote `submit`

Without `--wait`, stdout is a JSON object such as
`{"job_id":"execution-id","job_name":"examples.contract.manifest","job_version":"v1"}` after confirmed
acceptance (`job_version` is JSON `null` when the Job has no version). The
execution can outlive the CLI. The CLI does not resubmit after **submission
acceptance unconfirmed** (handle not returned).

With `--wait`, the CLI waits for the terminal outcome and streams observations.
Successful jobs without a result type emit JSON `null` on stdout. Result
serialization failures, observation-stream failures, or observation iterator close
failures exit `1` even when a terminal success was already observed; stderr
still prints a known execution reference when acceptance succeeded. Failures do
not emit a success result on stdout.
`--wait-timeout` requires `--wait`; must be a finite positive number. Default wait
bound: `DEFAULT_CLI_WAIT_TIMEOUT_SECONDS` (**300 s**). This limits **client**
waiting, not worker attempt duration or execution deadlines. After the outcome is
known, observation draining continues for at most the smaller of the remaining wait
budget and `REMOTE_SUBMIT_OBSERVATION_DRAIN_GRACE_SECONDS` (**2 s**).

Startup and submission each use an independent phase bound of
`REMOTE_SUBMIT_SHUTDOWN_TIMEOUT_SECONDS` (**30 s**). Producer teardown after
disconnect uses a separate cleanup budget with the same nominal duration (starts
when disconnect begins, not at CLI start). If submission fails before a handle is
returned, stderr reports **submission acceptance unconfirmed**—distinct from
post-acceptance failures and not an implicit resubmission signal.

On wait timeout, transport failure after acceptance, terminal failure, cleanup
failure, or CLI interrupt, stderr prints a known execution reference once when
available. **Timeout and interrupt do not cancel accepted work.**

### Reference recovery

Retain all reference fields. Resolve the Job from the shared contract package
before `client.get(job_id)`; do not treat a missing version as “latest.”
Recovery depends on broker retention; see [api.md](api.md) and
[cli-application.md](design/cli-application.md) for an installed example.

## Retry, cancellation, and lifetime

The CLI does not add local retry flags or remote cancellation commands. Local
`run` is single-attempt in-memory state. Remote submissions use the contract’s
pool and default retry policy—handlers must tolerate at-least-once attempts.
Wait timeout and CLI disconnect end observation only; they do not stop the worker.

## Limitations (current behavior)

- No cross-process contract fingerprint enforcement on the wire ([ADR 0003](adr/0003-cross-process-contract-compatibility.md)).
- No hard termination of arbitrary Python or cancellation-resistant handlers;
  shutdown paths request asyncio cancellation and cooperative waits rather than
  guaranteed wall-clock bounds on all user code.
- No durable local recovery after CLI exit.
- Runtime validation and lazy `handler_factory` resolution occur when `run`
  executes, not at registration (except static factory callability checks).
