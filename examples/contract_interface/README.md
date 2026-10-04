# Contract-interface example

Runnable sketch for shared **contract packages**, producer/worker split, and measured typing guarantees. API and decisions: [docs/api.md](../../docs/api.md), [docs/adr/](../../docs/adr/README.md).

For installed console commands with local `run` and NATS `submit`, see
[CLI application integration](../../docs/design/cli-application.md). The CLI,
worker and worker resource packages are built into separate non-editable wheels;
the CLI environment excludes worker implementation and resource dependencies.
`tools/verify_cli_process.py` exercises installed commands and a separately
installed worker on owned NATS, including recovery after producer exit.

## Dependencies

| Component | Needs |
| --- | --- |
| In-memory demo | Installed `superjobs` (repo `src/` or wheel), contract on `PYTHONPATH` (see below) |
| `pytest` source test | Same; **no** `pip install` of the contract during the test |
| Pyright suites | Project `.venv` with `superjobs` dependencies; `extraPaths` point at repo `src/` and `superjobs_contract_example/src/` (no editable contract install required) |
| Wheel consumer typing | Non-editable wheel install of `superjobs_contract_example` (see `python tools/verify_contract_typing.py`; [issue 5](https://github.com/vschroeter/superjobs/issues/5)) |
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

## Automated source and wheel typing verification

From the repository root:

```powershell
python tools/verify_contract_typing.py
```

- **Source mode** checks the typing suites against repository `src/` and contract sources via generated `extraPaths`, using a fresh isolated dependency environment (or `--source-venv`) — not the repository `.venv`.
- **Wheel mode** builds non-editable `superjobs` and `superjobs_contract_example` wheels into a temporary work directory **outside** the repository, installs them into fresh `uv` virtual environments (default runtimes **3.12** and **3.14**), asserts `site-packages` origins (`py.typed`, no editable installs; local wheel `direct_url.json` allowed), runs copied `tests/test_handler_registration.py` and `tests/test_public_api.py`, and re-runs the typing suites with **no** source `extraPaths` and `autoSearchPaths: false`.
- **Both mode** (default) builds wheels once, uses the same **3.12** wheel-installed environment for source (`extraPaths`) and wheel typing, and requires matching negative diagnostics.
- Pyright is pinned to **1.1.414** (`basic`, static Python **3.12**). Negative controls use inline `# expect: <rule>` markers at each deliberate misuse site; the runner compares normalized `(file, line, rule)` multisets and requires source/wheel agreement.
- Optional evidence: `python tools/verify_contract_typing.py --evidence C:\path\to\evidence` (writes Pyright JSON, origin-probe output, and metadata; temporary work directories are removed unless `--keep-work`).
- Fast pytest selection (no wheel build, no Pyright subprocess): `python -m pytest -m "not nats and not contract_typing" -q`
- Contract typing gate (full runner): `python -m pytest -m contract_typing -q` or `python tools/verify_contract_typing.py --python 3.12 --python 3.14`
- Upgrade the checker only by changing `PYRIGHT_VERSION` in `tools/verify_contract_typing.py` and refreshing markers after review.

`typing/measured_gaps` remains a documented probe suite and is **not** part of this gate.

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
| `@jobs.handler(job)`, `jobs.register(job, fn)`, `@job.handler` + `jobs.register(marked)` | Request/context/result shapes for sync and async handlers, including explicit `RequestJob` / `NoRequestJob` metadata decoration |
| Decorated callable signature | Original parameter names, positional/keyword calls, defaults and optional extra parameters; original sync/async return type, including a narrower result subclass |
| No-request jobs (inferred or `NoRequestJob[...]`) | Context-only handlers via runtime and metadata decorators |

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
| Missing context on `@jobs.handler`, `jobs.register(job, handler)`, or `@job.handler` (request jobs) | `reportArgumentType` |
| Zero-argument no-request handler on `@jobs.handler`, `jobs.register(job, handler)`, or `@job.handler` | `reportArgumentType` |
| Widened base `Job[Request, Result, Event]` or `Job[None, Result, Event]` with otherwise valid handlers | `reportArgumentType` and `reportCallIssue` for runtime forms; `reportAttributeAccessIssue` for metadata decoration |
| Extra required handler argument on `jobs.register(job, handler)` | `reportArgumentType` |
| Wrong no-request shape (`request: None`, correct context/result) on `@jobs.handler`, `@job.handler` | `reportArgumentType` |
| Extra required handler argument on either decorator | `reportArgumentType` |
| Incorrect request/optional parameter on a directly called decorated function | `reportArgumentType` |
| Unknown keyword on a directly called decorated function | `reportCallIssue` |

The inferred `HEARTBEAT_JOB` and an explicitly annotated `NoRequestJob` both reject `jobs.register(job, …)` with a request-bearing callback. `producer_negative` checks missing, mistyped and unknown constructor fields, mixed forms, invalid `SubmitOptions`, and no-request registration.

Prefer `python tools/verify_contract_typing.py` for exact rule/site assertions. Manual runs may use `pyright --outputjson`, but count-only checks are not sufficient.

### Measured gaps (`typing/measured_gaps`)

Probes record **EXPECTED** product targets vs **MEASURED** Pyright 1.1.414 (basic, Python 3.12) output. Outcome typing is verified in the [positive suite](#positive-typingpositive) (`JobHandle.outcome()` → `JobOutcome[FinalT]`); this directory keeps only gaps that remain open.

| Probe | EXPECTED (target) | MEASURED (baseline) |
| --- | --- | --- |
| Inferred `Job(..., request=..., result=...)` without `event=` | No declared events | Inferred `None` event slot |
| `jobs.register(HEARTBEAT_JOB, fn)` with `(request: None, context: …)` callback | Reject wrong arity | `reportCallIssue` and `reportArgumentType` |
| Manually widened `Job[Request, Result, Event]` | Preserve constructor keywords and handler registration | Explicit-object client only; constructor ParamSpec unavailable; checked handler registration rejected (see negative fixtures) |
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
