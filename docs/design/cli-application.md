# Installed CLI examples and process verification

Issue [#42](https://github.com/vschroeter/superjobs/issues/42) packages the
contract-interface example as separate installable applications: a CLI producer
(`superjobs-contract-cli-example`) and a worker implementation
(`superjobs-contract-worker-example`). The CLI layout installs library,
contract, and CLI packages only; worker modules are not importable there.
The worker additionally requires `superjobs-contract-worker-resources`, an
example-only dependency used at startup and absent from the CLI installation.
These packages are repository examples; the instructions build local wheels.

Registration, input rules, local/remote semantics, exit codes, and timeouts are
documented in the canonical guide [cli.md](../cli.md). This page covers
**installed** wheel layout, manual invocation, reference recovery, and measured
process verification.

## Installation

Build non-editable wheels from the repository root (temporary directory outside
the checkout is recommended):

```powershell
uv build --project . --out-dir $env:TEMP\superjobs-wheels
uv build --project examples/contract_interface/superjobs_contract_example --out-dir $env:TEMP\superjobs-wheels
uv build --project examples/contract_interface/superjobs_contract_cli_example --out-dir $env:TEMP\superjobs-wheels
uv build --project examples/contract_interface/superjobs_contract_worker_resources --out-dir $env:TEMP\superjobs-wheels
uv build --project examples/contract_interface/superjobs_contract_worker_example --out-dir $env:TEMP\superjobs-wheels
```

CLI-only environment (no worker package):

```powershell
uv venv $env:TEMP\superjobs-cli-only\.venv
uv pip install --python $env:TEMP\superjobs-cli-only\.venv\Scripts\python.exe `
  "$env:TEMP\superjobs-wheels\superjobs-0.1.0-py3-none-any.whl[cli]" `
  "$env:TEMP\superjobs-wheels\superjobs_contract_example-0.0.1-py3-none-any.whl" `
  "$env:TEMP\superjobs-wheels\superjobs_contract_cli_example-0.0.1-py3-none-any.whl"
```

Worker environment (separate venv):

```powershell
uv venv $env:TEMP\superjobs-cli-worker\.venv
uv pip install --python $env:TEMP\superjobs-cli-worker\.venv\Scripts\python.exe `
  "$env:TEMP\superjobs-wheels\superjobs-0.1.0-py3-none-any.whl" `
  "$env:TEMP\superjobs-wheels\superjobs_contract_example-0.0.1-py3-none-any.whl" `
  "$env:TEMP\superjobs-wheels\superjobs_contract_worker_resources-0.0.1-py3-none-any.whl" `
  "$env:TEMP\superjobs-wheels\superjobs_contract_worker_example-0.0.1-py3-none-any.whl"
```

Do not put repository `src/` or example sources on `PYTHONPATH` when verifying
installed behavior.

On Linux, use the same `uv build --project ... --out-dir /tmp/superjobs-wheels`
commands and install into separate venvs using their `bin/python` paths. Pass
all three CLI wheels, or all four worker wheels, to one `uv pip install` command.
The verifier below builds these packages, strips development source overrides,
and creates the isolated environments automatically.

## Invoke the installed application

After the CLI installation above, PowerShell commands are:

```powershell
$cli = "$env:TEMP\superjobs-cli-only\.venv\Scripts\superjobs-contract-cli.exe"
& $cli --help
& $cli run bundle bundle-1
& $cli run observe-local --device-id device-1
& $cli run observe-local --json '{"device_id":"device-1"}'
'{"device_id":"device-1"}' | Set-Content -Encoding utf8 request.json
& $cli run observe-local --input request.json
'{"device_id":"device-1"}' | & $cli run observe-local --input -
```

Local commands require no broker. `bundle` returns `{"accepted":true}`;
`observe-local` returns `{"revision":"device-1"}` and emits structured
observations. For remote mode, start a JetStream-enabled NATS broker and the
worker in another terminal. The example worker uses readiness/stop marker files:

```powershell
$env:SUPERJOBS_NATS_URL = 'nats://localhost:4222'
$env:SUPERJOBS_CLI_RUN_ID = 'manual-example'
$env:SUPERJOBS_CLI_STATE_DIR = "$env:TEMP\superjobs-worker-manual"
New-Item -ItemType Directory -Force $env:SUPERJOBS_CLI_STATE_DIR | Out-Null
& "$env:TEMP\superjobs-cli-worker\.venv\Scripts\superjobs-contract-worker.exe"
```

Use a fresh state directory for each worker invocation. Wait for the worker's
`worker ready` message or its `worker_ready.json` marker before commands that
wait for results. In the CLI terminal:

```powershell
$env:SUPERJOBS_NATS_URL = 'nats://localhost:4222'
& $cli submit bundle bundle-1 --wait --wait-timeout 30
& $cli submit observe-local --device-id device-1 --wait
& $cli submit observe-local --json '{"device_id":"device-1"}' --wait
& $cli submit observe-local --input request.json --wait
'{"device_id":"device-1"}' | & $cli submit observe-local --input - --wait
& $cli submit bundle bundle-1
```

On Linux, the console commands are `.../.venv/bin/superjobs-contract-cli` and
`.../.venv/bin/superjobs-contract-worker`; set the same variables with `export`.
The gate/probe/failure commands in this example exist for verification. Normal
application code owns its own startup, handlers and worker shutdown policy.

To stop this example worker cooperatively, write its stop marker from another
terminal, using the same state directory and run ID as the worker:

```powershell
'{"run_id":"manual-example"}' | Set-Content -Encoding ascii "$env:TEMP\superjobs-worker-manual\worker_stop.json"
```

On Linux, write the same JSON to `worker_stop.json` in the chosen state directory.

## Registration and public API

Applications expose `build_cli() -> JobCLI` and a console entry point
(`superjobs-contract-cli`) that delegates to `main()`. Workers expose
`register_contract_handlers(jobs)` and `superjobs-contract-worker`.

| Command | Job | Local `run` | Remote `submit` |
| --- | --- | --- | --- |
| `bundle` | `examples.contract.manifest.no_events` | handler in CLI process | worker handler |
| `observe-local` | `examples.contract.manifest.with_events` | revision equals device ID | worker revision adds `-r1` |
| `probe-local` | `examples.contract.cli.local_probe` | returns handler PID | no worker handler provided |
| `gate-local` / `gate-remote` | `examples.contract.cli.gate` | file-gated local handler | worker gate |
| `fail-local` / `fail-remote` | `examples.contract.cli.fail` | raises `RuntimeError` | worker raises |

`bundle` demonstrates a **positional** contract field (`bundle_id`). See [cli.md](../cli.md)
for input forms, factories, and lifecycle rules.

Runtime factories in `superjobs_contract_cli_example.main`:

- `local_runtime()` — `SuperJobs(transport=InMemoryTransport())`
- `remote_runtime()` — `SuperJobs(broker=NatsBroker(SUPERJOBS_NATS_URL, ...))`

## Reference recovery (installed example)

Retain all three fields of the execution reference. Resolve its exact contract name and
version from the shared contract package before calling `get`; do not treat an
absent version as “latest.” For an accepted `bundle` reference:

```python
import asyncio
import json
from faststream.nats import NatsBroker
from superjobs import JobSucceeded, SuperJobs
from superjobs_contract_example import MANIFEST_NO_EVENTS_JOB

async def recover(reference_json: str) -> bool:
    reference = json.loads(reference_json)
    job = MANIFEST_NO_EVENTS_JOB
    assert reference["job_name"] == job.name
    assert reference["job_version"] == job.version
    async with asyncio.timeout(30):
        async with SuperJobs(broker=NatsBroker("nats://localhost:4222")) as runtime:
            handle = await runtime.client(job).get(reference["job_id"])
            outcome = await handle.outcome()
            if isinstance(outcome, JobSucceeded):
                return outcome.result.accepted
            raise RuntimeError(f"execution did not succeed: {outcome!r}")
```

Recovery depends on broker store and configured retention. If submission
acceptance was unconfirmed, absence of a printed reference does not prove rejection.
Do not automatically repeat a submission whose acceptance is unknown.

## Verification

| Check | Command |
| --- | --- |
| Deterministic verifier unit tests | `python -m pytest tests/test_verify_cli_process.py -q` |
| Installed CLI + NATS process proof | `python tools/verify_cli_process.py --python 3.12 --python 3.14` |
| Contract typing + runtime (includes CLI example wheel) | `python tools/verify_contract_typing.py --mode both` |
| Required integration stage | `python -m scripts.dev_check integration` (see [development.md](../development.md)) |

Evidence defaults to `dist/verification/cli-process/` (JSON summaries, per-scenario
logs, origin probes). Each invocation uses a fresh `run-<uuid>` evidence directory
to prevent marker reuse. Work directories use OS temp prefixes
`superjobs-cli-process-*`.

Measured **#42** matrix (18 scenarios × four platform/interpreter cells, seven
integration stages, 20 real-NATS pytest cases per cell): [verification.md](../verification.md#42--installed-application-matrix-2026-10-04-current).

### Limitations (measured, not hidden)

- Windows CLI interrupt uses a hidden `CREATE_NEW_CONSOLE` launch and a helper
  that attaches to the target console and delivers `CTRL_C_EVENT`; Unix uses
  `SIGINT`. Both require cooperative handlers for bounded cleanup (exit `130`).
- Python cannot hard-terminate cancellation-resistant in-process work; the CLI
  requests cancellation, diagnoses resistant work, and waits for it ([cli.md](../cli.md)).
- This documentation does **not** claim PyPI publication, contract-manifest
  enforcement on the wire, or durable local recovery after CLI exit.
- Real NATS scenarios require the owned pinned NATS harness; missing
  infrastructure fails verification rather than skipping.

## Related documents

- [CLI user guide](../cli.md)
- [Contract-interface example](../../examples/contract_interface/README.md)
