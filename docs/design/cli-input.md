# CLI request input

Issue [Parse strict CLI JSON inputs and generate options for flat Job requests](https://github.com/vschroeter/superjobs/issues/39)
adds input preparation to both `run` and `submit` on `feature/cli`. Typer is the
main CLI library, available through `superjobs[cli]`. Local `run` execution is
available in-process (issue #40), including the built-in isolated in-memory
runtime when no `local_runtime_factory` is configured; remote `submit` remains
unavailable until issue #41.

## Whole requests

Every Job with a declared request accepts one of:

```text
app submit command --json '{"name":"Ada","count":3}'
app submit command --input request.json
app submit command --input -
```

`--input -` reads stdin. Files and binary stdin must be UTF-8. JSON rejects
duplicate keys at every depth, malformed syntax, non-finite numbers and numeric
overflow. Read/decoding/validation failures print diagnostics to stderr and exit 2.

JSON is parsed into a logical tree and loaded through the declared payload
adapter. The Job's transport codec is independent: a Msgpack-backed Job accepts
JSON CLI input without passing JSON bytes to Msgpack. Custom adapters retain JSON
input even without a schema; their `load()` owns the accepted tree shapes.

Canonical contract names apply to every input form. Validation and serialization
aliases are not alternate CLI names. Built-in adapters preserve strict types,
constraints, defaults and application hooks. JSON `"count":"3"` cannot satisfy
an integer field. Input is loaded once without a dump/load validation round-trip.

No-request Jobs expose no payload flags. A nullable declared request requires an
explicit whole-request source, including `--json null` for the null value.

## Generated fields and customization

A flat object schema with supported scalar fields provides named options.
Underscores become hyphens. Strings, integers, finite floats, booleans and enums
with homogeneous scalar values are supported. Boolean fields provide paired
`--active` / `--no-active` options. Omitted fields stay omitted until adapter
validation applies defaults; explicit false and default-equal values stay supplied.

Any nested object, list, nullable field, union or unknown property schema makes
the entire command JSON-only. Scalar, nullable-root and schema-less requests also
use JSON. Help explains the limitation. Serialization-only `readOnly` properties,
such as Pydantic computed fields, do not become request inputs.

```python
from superjobs.cli import CLIField, JobCLI

cli = JobCLI()
cli.add(
    "greet", GREET_JOB, handler=greet,
    positional_fields=("name",),
    field_options={"name": CLIField(help="Name to greet.")},
)
cli.add(
    "greet-mood", MOOD_JOB, handler=mood_handler,
    field_options={"mood": CLIField(option="tone", help="Greeting tone.")},
)
```

`positional_fields` selects canonical fields in exactly the specified order;
ordering is never inferred from schema layout. `field_options` maps canonical names
to frozen `CLIField` values. `option` is a long option name without leading dashes;
underscores are normalized. For positionals it controls the displayed metavar.
`help` replaces the field description. Both configuration arguments are available
on every typed `JobCLI.add()` overload without relaxing handler typing.

Typer parameters are optional at the parser level so JSON can satisfy required
fields. Shared validation enforces requiredness when using fields. Help includes
descriptions, requiredness in field mode, declared defaults and enum choices.
Default factories and validators do not run at registration or help generation.
An empty flat request can use an implicit `{}`.

Whole-request sources are mutually exclusive with each other and with every
explicit field/positional value, including false and values equal to defaults.
There is no merging. Invalid configuration fails during registration: unknown or
repeated fields, unsupported positionals, invalid names, effective normalized-name
collisions, boolean negative-name collisions and reserved control options. Explicit
renaming can resolve a collision; positionals do not occupy option names.

## Execution seam

Both modes share an internal `PreparedCommandInput` containing the loaded request
and whether a request exists. The command callback currently validates and returns
the execution-unavailable diagnostic (exit 1). It never initializes a handler or
runtime. Local execution is tracked in #40, NATS submission in #41, and end-to-end
installed applications in #42. Help exits 0 and input errors exit 2; stdout remains
empty for unsuccessful commands.

The runnable [registration example](../../examples/cli_registration/README.md)
demonstrates JSON, file/stdin, field options, booleans, enum renaming and positionals.

## Verification

Public CLI tests observe logical trees and typed results through recording payload
adapters, with Msgpack transport codecs, rather than asserting exit codes alone.
They exercise both modes, input equivalence, strict canonical names, hooks/default
counts, source conflicts, schema capability and registration errors. The installed
wheel verifier copies all three CLI test modules outside the checkout.

```text
uv run python -m pytest tests/test_cli_public.py tests/test_cli_input.py tests/test_cli_input_contracts.py -q
uv run --with pyright==1.1.414 pyright src/superjobs/cli examples/cli_registration/main.py --pythonversion 3.12
uv run python tools/verify_contract_typing.py --mode both --python 3.12 --python 3.14 --evidence dist/issue39/verified-final
```

The CLI negative typing consumer preserves the 19 original misuse diagnostics and
adds four diagnostics at genuine invalid field-configuration call sites. No NATS
transport behavior changes in this input-only slice. Full deterministic fast checks
and installed/source typing checks are run on Python 3.12 and 3.14; final evidence
is retained locally under `dist/issue39/`.

Measured on 2026-10-03 with Typer 0.27.2 and Pyright 1.1.414:

| Check | Windows Python 3.12 / 3.14 | Linux (WSL) Python 3.12 / 3.14 |
| --- | --- | --- |
| Deterministic `dev_check fast` | 616 passed, 1 platform skip each | 615 passed, 2 platform skips each |
| Installed public runtime checks | 160 passed each | 160 passed each |
| Source/wheel consumer typing | Positive suites clean; 23 CLI misuse diagnostics | Identical results |

All 136 focused CLI tests pass. Implementation/example Pyright reports zero errors
and warnings; existing negative consumers retain 46, 17 and 1 diagnostics. Final
artifacts use the `final-windows-*` / `final-linux-*` prefixes under `dist/issue39/`.
These are local checks; hosted CI, NATS integration and Python 3.13 full checks
were not run for this input-only slice.

Cursor Composer 2.5 produced the implementation and a targeted correction pass.
After both passes left acceptance failures, Codex corrected effective-name and
positional collisions, enum scalar handling, schema-reference metadata and typing,
then added public adapter-observation regressions and independently ran this matrix.
