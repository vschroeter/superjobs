# JobCLI registration shell (issue #38)

Foundation slice for exposing contract Jobs as Typer commands without duplicating
request types or importing worker implementations in remote-only CLIs.

## Installation

```bash
pip install 'superjobs[cli]'
```

Core `import superjobs` does not import Typer or `superjobs.cli`.
The CLI extra requires Typer 0.27.2 or newer; the measured version is 0.27.2.

## API

`JobCLI` registers commands with explicit `add(command_name, job, ...)`:

- **Local command**: pass `handler=` or `handler_factory=`. The handler must match the
  Job request/context/result types (including sync handlers and no-request Jobs).
  Contract-marked handlers (`@job.handler`) may be paired with their Job; association
  is checked at registration time.
- Local registration requires an inferred `RequestJob` / `NoRequestJob` (or an
  annotation preserving those types). A widened general `Job` remains usable
  for remote-only registration, but cannot prove a local handler's signature.
- **Remote-only command**: `remote_only=True` with no handler. `run <name>` is rejected
  with usage exit code `2`; `submit <name>` reports unavailable execution (exit `1`).

Application code owns runtime lifetime via async context-manager factories:

| Factory | Purpose |
| --- | --- |
| `local_runtime_factory` | Yields `SuperJobs` for in-process `run` (future slice) |
| `remote_runtime_factory` | Yields `SuperJobs` for NATS `submit` (future slice) |

Factories must not run for `--help` or when another command is selected. This slice
does not invoke them. Lazy `handler_factory` callables are likewise deferred until
local execution lands.
The application creates and closes resources inside each context manager; the
CLI will enter the selected manager and exit it on every outcome. Factories
must return fresh invocation resources, rather than reusing a running worker
runtime. Handler factories take no arguments and can close over application
configuration. Their returned handler association/signature must be validated
when resolved by the future local executor. This slice checks the factory's
callability and static result type without loading it.

### Typer composition

- `build_typer()` returns a standalone Typer app with `run` and `submit` groups.
- `mount(app)` attaches those groups to an application-owned Typer app.
- `mount()` fails before mutating `app` when either `run` or `submit` is already
  registered as a command or Typer group on the host app.
- `build_typer()` and `mount()` snapshot registrations at call time; later `add()`
  calls do not change Typer objects already built or mounted.

### Request field names and collisions

Registration checks declared logical JSON-schema property names from the Job
request adapter (resolving a top-level `$ref` when present). Names are normalized
for CLI options by replacing underscores with hyphens. Collisions are rejected
when:

- two effective option names normalize to the same name after customization, or
- a normalized name matches a reserved planned option (`--json`, `--input`, …).

Canonical contract field names are used; aliases are not expanded into separate
CLI options. Explicit positionals do not occupy option names. See
[CLI input](cli-input.md) for field customization and JSON input.

### Process entry point

`main(argv=None)` is synchronous and intended for `if __name__ == "__main__"`. It
rejects calls from a thread that already has a running asyncio event loop.

## Output and exit conventions

| Stream | Content |
| --- | --- |
| stdout | One JSON final result or one accepted execution-reference object (future slices) |
| stderr | Diagnostics and observations |

| Code | Meaning |
| --- | --- |
| `0` | Success |
| `1` | Runtime, transport, wait failure, or explicitly unavailable execution |
| `2` | Usage / input errors |
| `130` | Interruption |

Constants are exported from `superjobs.cli` (`EXIT_SUCCESS`, `EXIT_USAGE`, …).

## Registration validation

At `add()` time:

- Duplicate `command_name`
- Invalid command names (reserved names `run` / `submit` / `help`, or non kebab-case)
- Incompatible handler/job marker association
- Handler signature mismatch
- Request field names (normalized to hyphenated options) colliding with reserved
  options: `--json`, `--input`, `--wait`, `--wait-timeout`, `--help`

Issue #39 adds mutually exclusive `--json` / `--input`, generated scalar field
options, optional ``positional_fields`` / ``field_options`` customization, and shared
typed request preparation before the execution-unavailable stub.

Field options are all-or-nothing per command: one unsupported input schema
property (nested, list, nullable or union) forces JSON-only input. Serialization-only
`readOnly` properties are excluded from input generation. Enums retain their actual
scalar values. Registration validates effective option names (including
``field_options`` overrides) and rejects unknown configuration keys for every request
shape.

## Execution status

`run` and `submit` intentionally report that execution is unavailable, write a
diagnostic to stderr, and exit without successful stdout. No handler or runtime
factory is called. Do not rely on this document for NATS or in-process execution
behavior until issues #40–#42 land.

## Example

See [examples/cli_registration](../../examples/cli_registration/README.md).

## Verification on 2026-10-03

Implemented on `feature/cli`; no package publication or hosted CI run
is implied. Cursor Composer 2.5 produced the initial implementation and one
correction pass. Codex reviewed independently and fixed the remaining exit-code,
diagnostic, typing-fixture, example and composition failures after those passes.

| Check | Windows | Linux (WSL) |
| --- | --- | --- |
| `dev_check fast`, Python 3.12 and 3.14 | 523 passed, 1 skipped each | 522 passed, 2 skipped each |
| Installed public runtime checks, Python 3.12 and 3.14 | 67 passed each | 67 passed each |
| Source/wheel Pyright 1.1.414, target Python 3.12 | Positive suites clean; identical negative diagnostics | Same |

The CLI negative suite has 19 expected diagnostics on actual `JobCLI.add()` calls:
wrong request/context/result, invalid lazy factories, widened local registration,
conflicting modes and missing handlers. Existing negative suites retain 46, 17 and
1 diagnostics. The new CLI implementation and runnable example additionally pass
Pyright with zero errors/warnings. All 43 focused CLI tests pass.

Reproduce installed typing/runtime checks with:

```bash
uv run python tools/verify_contract_typing.py --mode both --python 3.12 --python 3.14 --evidence dist/issue38/verified-typing
```

Use the isolated fast command in [development.md](../development.md), selecting
Python 3.12 or 3.14 and the `.[cli]` extra. Local evidence is retained under
`dist/issue38/` (gitignored), including the Windows/Linux typing records and
`*-fast/run-summary.json`. The earlier Typer 0.15.1 probe failed 10 of 28 then-current
tests; the dependency minimum was raised to the verified 0.27.2 version.

Real-NATS and durable execution checks were not added or rerun for this shell-only
slice; execution itself remains unavailable. Python 3.13 full deterministic checks
and the hosted CI matrix were not run in this local verification.
