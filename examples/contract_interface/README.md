# Contract-interface example

Runnable sketch for shared **contract packages**, producer/worker split, and measured vs proposed typing guarantees. Design discussion: `docs/design/contract-handler-interface.md`.

## Dependencies

| Component | Needs |
| --- | --- |
| In-memory demo | Installed `superjobs` (repo `src/` or wheel), contract on `PYTHONPATH` (see below) |
| `pytest` source test | Same; **no** `pip install` of the contract during the test |
| Pyright suites | Project `.venv` with `superjobs` dependencies; `extraPaths` point at repo `src/` and `superjobs_contract_example/src/` (no editable contract install required) |
| Wheel consumer typing | Editable or wheel install of `superjobs_contract_example` (verified outside `tests/test_contract_interface_example.py`; see [issue 5](https://github.com/vschroeter/superjobs/issues/5)) |
| NATS producer/worker | `NATS_URL`, `faststream[nats]`, running NATS broker |

## Source-layout demo (no contract `pip install`)

From the repository root, with `superjobs` available in the active environment:

```powershell
$env:PYTHONPATH = "examples/contract_interface;examples/contract_interface/superjobs_contract_example/src"
python examples/contract_interface/in_memory_demo.py
```

```bash
export PYTHONPATH="examples/contract_interface:examples/contract_interface/superjobs_contract_example/src"
python examples/contract_interface/in_memory_demo.py
```

Expected: process exits `0` with no output (assertions only). Entire demo is bounded by `asyncio.timeout` (30s).

### Pytest (source example only)

```powershell
python -m pytest tests/test_contract_interface_example.py -q
```

- `test_in_memory_contract_interface_demo` runs the demo via `sys.executable` and appends contract **source** (`superjobs_contract_example/src`) plus `examples/contract_interface` to `PYTHONPATH` for the child process only.
- `test_contract_package_public_imports` uses `pytest` `monkeypatch.syspath_prepend` for contract **source** imports (not from an installed wheel).

## Pyright (source paths, no editable contract install)

Run from each config directory so relative `extraPaths` resolve:

```powershell
cd examples/contract_interface/typing/positive
uv tool run --from pyright==1.1.414 pyright
cd ../negative
uv tool run --from pyright==1.1.414 pyright
cd ../producer_positive
uv tool run --from pyright==1.1.414 pyright
cd ../producer_negative
uv tool run --from pyright==1.1.414 pyright
cd ../measured_gaps
uv tool run --from pyright==1.1.414 pyright
```

Configs include:

- `../../../../src` — `superjobs` library sources
- `../../superjobs_contract_example/src` — contract package sources

### Positive (`typing/positive`)

**Include:** `check_types.py`, `../../producer.py`, `../../worker_handlers.py`.

**Expected:** zero errors. Covers inferred contract Job shapes, explicit-object `submit` / `result` / `await handle`, reconstructed `get` handle, no-event and no-result jobs, `event.data` narrowing after `isinstance`, `outcome()` typing (`JobOutcome[FinalT]`, `JobSucceeded` / failure / cancellation narrowing, including `JobOutcome[None]` for no-result jobs), and all three handler registration forms. Public `RequestJob[Request, Result, Event, ...]` and `NoRequestJob[Result, Event]` retain handler typing when an explicit annotation is needed. `producer_positive` additionally checks typed constructor keywords and `SubmitOptions`.

**Handler typing verified in positive:**

| Area | Verified |
| --- | --- |
| `@jobs.handler(job)`, `jobs.register(job, fn)`, `@job.handler` + `jobs.register(marked)` | Request/context/result shapes for sync and async handlers |
| Decorated callable signature | Original parameter names, positional/keyword calls, defaults and optional extra parameters; original sync/async return type, including a narrower result subclass |
| No-request jobs (`Job[None, ...]`) | Context-only handlers via runtime and metadata decorators |

Two overloads in a callback protocol have separate roles: one checks that the runtime can call the handler with the contract's arguments, while the other captures its full `ParamSpec` and return type. Both decorators return the original function object. Positive fixtures contain no casts, `Any` annotations, or diagnostic suppression.

### Negative (`typing/negative`)

**Expected diagnostics (argument typing still enforced):**

| Location | Expected Pyright code |
| --- | --- |
| `submit("not a manifest request")` | `reportArgumentType` |
| `context.emit("not an event")` | `reportArgumentType` |
| Wrong request/context/result on `@jobs.handler` | `reportArgumentType` |
| Wrong request/context/result on `jobs.register(job, handler)` (one mismatch each) | `reportCallIssue` and `reportArgumentType` |
| Wrong request/context/result on `@job.handler` | `reportArgumentType` |
| Incompatible marked handler | `reportArgumentType` at `@job.handler`; metadata registration relies on that check |
| Missing context on `@jobs.handler` | `reportArgumentType` |
| Wrong no-request shape (`request: None`, correct context/result) on `@jobs.handler`, `@job.handler` | `reportArgumentType` |
| Extra required handler argument on either decorator | `reportArgumentType` |
| Incorrect request/optional parameter on a directly called decorated function | `reportArgumentType` |
| Unknown keyword on a directly called decorated function | `reportCallIssue` |

The inferred `HEARTBEAT_JOB` and an explicitly annotated `NoRequestJob` both reject `jobs.register(job, …)` with a request-bearing callback. `producer_negative` checks missing, mistyped and unknown constructor fields, mixed forms, invalid `SubmitOptions`, and no-request registration.

Run Pyright with failure on diagnostics, e.g. `pyright --outputjson` and assert error count for the deliberate mistakes only.

### Measured gaps (`typing/measured_gaps`)

Probes record **EXPECTED** product targets vs **MEASURED** Pyright 1.1.414 (basic, Python 3.12) output. Outcome typing is verified in the [positive suite](#positive-typingpositive) (`JobHandle.outcome()` → `JobOutcome[FinalT]`); this directory keeps only gaps that remain open.

| Probe | EXPECTED (target) | MEASURED (baseline) |
| --- | --- | --- |
| Inferred `Job(..., request=..., result=...)` without `event=` | No declared events | Inferred `None` event slot |
| `jobs.register(HEARTBEAT_JOB, fn)` with `(request: None, context: …)` callback | Reject wrong arity | `reportCallIssue` and `reportArgumentType` |
| Manually widened `Job[Request, Result, Event]` | Preserve constructor keywords and handler registration | Explicit-object client only; constructor ParamSpec and checked registration unavailable |
| Positional constructor-shaped `submit(value)` on a non-keyword-only request class | Reject | Runtime rejects; Pyright may accept through captured constructor ParamSpec |

## Wheel build and producer-only install (isolated from source layout)

Use this path to verify **installed wheels**, not repo `PYTHONPATH` or editable installs.

From the repository root, build both distributions into `dist/`:

```powershell
uv build --project . --out-dir dist/
uv build --project examples/contract_interface/superjobs_contract_example --out-dir dist/
```

Create an isolated environment and install exactly those two wheels (no editable `superjobs`, no contract source on `PYTHONPATH`):

```powershell
uv venv C:\tmp\contract-wheel-verify\.venv
uv pip install --python C:\tmp\contract-wheel-verify\.venv\Scripts\python.exe `
  C:\path\to\repo\dist\superjobs-0.1.0-py3-none-any.whl `
  C:\path\to\repo\dist\superjobs_contract_example-0.0.1-py3-none-any.whl
```

Producer-only directory: copy **only** `examples/contract_interface/producer.py` (do not copy `worker_handlers.py` or other worker modules):

```powershell
mkdir C:\tmp\contract-wheel-verify\producer-only
copy examples\contract_interface\producer.py C:\tmp\contract-wheel-verify\producer-only\
```

Run Pyright wheel-consumer checks and/or the producer script from that layout using the isolated venv. Imports must be `superjobs_contract_example` and `superjobs` only—never `worker_handlers`.

For day-to-day development, prefer the [source-layout demo](#source-layout-demo-no-contract-pip-install) and [Pyright source paths](#pyright-source-paths-no-editable-contract-install) above instead of mixing wheel and source instructions.

## NATS two-process demo

**Ready file (local demo only):** the caller chooses one **unique** `SUPERJOBS_EXAMPLE_READY_FILE` path per worker run and passes the same path to the producer. The worker refuses to start if that path already exists (avoids reusing a stale marker from a crashed prior run). After `jobs.start()`, the worker writes `ready`; the producer may start later and treats an existing ready file as success. This is not production discovery—leftover files from aborted runs can still mislead if the path is reused without cleaning up.

Terminal 1 (worker; lifetime unbounded until stopped):

```powershell
$env:NATS_URL = "nats://127.0.0.1:4222"
$env:SUPERJOBS_EXAMPLE_READY_FILE = Join-Path $env:TEMP ("superjobs-contract-ready-" + [guid]::NewGuid().ToString())
$env:PYTHONPATH = "examples/contract_interface;examples/contract_interface/superjobs_contract_example/src"
python examples/contract_interface/worker.py
```

Terminal 2 (producer; whole flow bounded by `asyncio.timeout`; shutdown uses an explicit `wait_for`):

```powershell
$env:NATS_URL = "nats://127.0.0.1:4222"
$env:SUPERJOBS_EXAMPLE_READY_FILE = "<same path as worker>"
$env:PYTHONPATH = "examples/contract_interface;examples/contract_interface/superjobs_contract_example/src"
python examples/contract_interface/producer.py
```

Expected producer stdout: `producer completed all contract cases`. On timeout, assertion failure, or missing env, the producer exits non-zero (no pytest skip).

## Layout

| Path | Role |
| --- | --- |
| `superjobs_contract_example/` | Installable contract package (`Job` + payloads) |
| `worker_handlers.py` | Handler registrations (worker only) |
| `in_memory_demo.py` | Deterministic four-case run |
| `worker.py` / `producer.py` | NATS processes |
| `typing/` | Pyright positive, negative, and measured-gap configs |
