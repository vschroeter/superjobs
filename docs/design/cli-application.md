# CLI application integration

Issue [#42](https://github.com/vschroeter/superjobs/issues/42) packages the
contract-interface example as separate installable applications: a CLI producer
(`superjobs-contract-cli-example`) and a worker implementation
(`superjobs-contract-worker-example`). The CLI layout installs library,
contract, and CLI packages only; worker modules are not importable there.
The worker additionally requires `superjobs-contract-worker-resources`, an
example-only dependency used at startup and absent from the CLI installation.
These packages are repository examples; the instructions build local wheels.

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

Use a fresh state directory for each worker invocation. In the CLI terminal:

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

`bundle` demonstrates a **positional** contract field (`bundle_id`). Field
options and JSON/file/stdin input follow [CLI input](cli-input.md).

Runtime factories in `superjobs_contract_cli_example.main`:

- `local_runtime()` — `SuperJobs(transport=InMemoryTransport())`
- `remote_runtime()` — `SuperJobs(broker=NatsBroker(SUPERJOBS_NATS_URL, ...))`

Startup dependencies (NATS client, env) live inside these factories; the CLI
enters the selected factory per command.
Help and invalid input do not enter them. `submit` does not resolve local handler
factories. A real application can register `handler_factory=` to import local
implementation dependencies only when `run` selects that command. It can also
use `remote_only=True` when the local implementation is unavailable; `run` then
exits `2`. Factories must yield fresh owned runtimes and release their resources
in `finally`; the CLI registers the selected local handler and starts the runtime.

## Modes

| Mode | Broker | Handler location | stdout | stderr |
| --- | --- | --- | --- | --- |
| `run` | none (in-memory) | CLI process | final result JSON | observations + diagnostics |
| `submit` (no `--wait`) | NATS | worker process | execution reference JSON | diagnostics |
| `submit --wait` | NATS | worker process | final result JSON | observations + diagnostics |

Exit codes match [CLI registration](cli-registration.md): `0` success, `1`
runtime/transport/terminal failure, `2` usage/validation, `130` CLI interrupt.
Invalid input is rejected **before** runtime startup (exit `2`) and must not
create Job executions.

Remote submission preserves execution references on timeout, transport failure
after acceptance, and CLI interrupt; it does **not** cancel accepted work.
Recovery uses `job_id` from the reference with a fresh client (`get` +
`outcome`), as exercised by the installed-process verifier.

Retain all three fields of the reference. Resolve its exact contract name and
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

Recovery depends on the broker store and configured retention. Observation history
and durable final results have distinct retention and worker-loss semantics;
missing observations do not erase a retained successful result. If submission
acceptance was unconfirmed, absence of a printed reference does not prove rejection.
Do not automatically repeat a submission whose acceptance is unknown.

Retry, cancellation, and lifetime semantics follow the core library; the CLI
does not add local retry configuration or remote cancellation.
Local `run` uses one attempt and in-memory state with no durable recovery.
Remote submissions use the contract's existing pool and default retry policy;
handlers must tolerate at-least-once attempts. Wait timeout controls only the
client wait. Local interruption requests cooperative cancellation; remote CLI
interruption disconnects the observer and leaves accepted work uncancelled.

## Conservative supported schemas

Flat object schemas with strings, integers, finite floats, booleans and enums
with homogeneous scalar values generate Typer options. Any nested, union, list,
nullable or unsupported property makes the whole command JSON-only. Custom
adapters without a supported schema also use `--json` or `--input` (including
`-` for stdin).
Canonical JSON field names use contract property names; CLI options replace
underscores with hyphens.
Aliases are not alternate input names. Omitted values stay omitted until strict
adapter validation applies defaults; supplied false and default-equal values
remain supplied. JSON/file/stdin and field/positional inputs cannot be mixed.

Default remote wait bound: `DEFAULT_CLI_WAIT_TIMEOUT_SECONDS` (300s). Local and
remote cooperative shutdown budgets: 30s each (see `superjobs.cli` exports).
Local interruption has a 30s cooperative grace period followed by a separate
30s cleanup budget. These bounds require cancellation-aware application code;
they request cancellation and cannot forcibly stop arbitrary Python threads.

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

### Measured verification

Independent local verification on 2026-10-04 used the CI-style isolated command
from [development.md](../development.md), selecting `integration`, the indicated
Python minor, and `--artifact-dir dist/issue42/final-matrix-<platform>-py<minor>`.
Linux ran under WSL; these results are not a hosted GitHub Actions run.

| Check | Result |
| --- | --- |
| Windows, CPython 3.12 | Seven integration stages passed; 18 CLI scenarios; 192.3 s total, 59.4 s CLI stage |
| Windows, CPython 3.14 | Seven integration stages passed; 18 CLI scenarios; 194.4 s total, 59.9 s CLI stage |
| Linux, CPython 3.12 | Seven integration stages passed; 18 CLI scenarios; 155.8 s total, 48.1 s CLI stage |
| Linux, CPython 3.14 | Seven integration stages passed; 18 CLI scenarios; 156.3 s total, 48.4 s CLI stage |
| Source and wheel typing, all four cells | Positive consumers passed; all negative diagnostics matched by file/line/rule, including 27 CLI diagnostics |
| Installed runtime tests, all four cells | Package origins passed; Windows 247 tests passed, Linux 248 tests passed |
| Real NATS pytest, all four cells | 20 tests passed per cell, no skips |
| Windows fast gate, CPython 3.13 | 719 passed, 2 platform skips, 25 integration tests deselected; 97.6 s gate |
| Verifier unit tests on Windows | 16 passed, including actual Ctrl+C delivery and child-tree cleanup |
| Deliberately missing `NATS_EXECUTABLE` | Verifier exited 1 and retained the setup error; no skip |

The remote observation scenario checks generated options, JSON, file and stdin
inputs individually; the positional `bundle` command is checked separately.
Each matrix artifact directory retains `run-summary.json`, stage stdout/stderr,
origin probes and detailed CLI scenario evidence. The fast-gate evidence is in
`dist/issue42/final-windows-fast-console/`; the missing-infrastructure diagnostic
is in `dist/issue42/broken-nats-control.log`.

Full `dev_check integration` on Linux and Windows CI remains the authoritative
matrix gate alongside the existing **855 s** orchestrator budget / **900 s**
workflow cap.

### Limitations (measured, not hidden)

- Windows CLI interrupt uses a hidden `CREATE_NEW_CONSOLE` launch and a helper
  that attaches to the target console and delivers `CTRL_C_EVENT`; Unix uses
  `SIGINT`. Both require cooperative handlers for bounded cleanup (exit `130`).
  A Windows verification supervisor first restores normal Ctrl+C inheritance
  when launched by `dev_check`'s process group, then invokes the installed
  console executable. It stays alive until that command completes cleanup.
- Python cannot hard-terminate cancellation-resistant in-process work; the CLI
  diagnoses and waits within documented budgets.
- This documentation does **not** claim PyPI publication, contract-manifest
  enforcement on the wire, or durable local recovery after CLI exit.
- Real NATS scenarios require the owned pinned NATS harness; missing
  infrastructure fails verification rather than skipping.

## Related documents

- [CLI registration](cli-registration.md)
- [CLI input](cli-input.md)
- [CLI local execution](cli-local.md)
- [CLI remote submission](cli-remote.md)
- [Contract-interface example](../../examples/contract_interface/README.md)
