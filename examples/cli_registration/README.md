# JobCLI registration example

Demonstrates explicit `JobCLI.add(...)` registration, request input via `--json`,
`--input`, generated field options, optional `positional_fields` / `field_options`,
and `run` / `submit` groups. Local `run` executes in-process through the
example `local_runtime_factory` (issues #39–#40). `submit` still reports remote
execution unavailable until issue #41.

## Run

Install the CLI extra, then invoke help or run a command locally:

```bash
uv run --with-editable ".[cli]" python examples/cli_registration/main.py --help
uv run --with-editable ".[cli]" python examples/cli_registration/main.py run --help
uv run --with-editable ".[cli]" python examples/cli_registration/main.py run greet world
uv run --with-editable ".[cli]" python examples/cli_registration/main.py run greet --json '{"name":"json"}'
uv run --with-editable ".[cli]" python examples/cli_registration/main.py run greet-mood --name ada --tone cheerful --excited
uv run --with-editable ".[cli]" python examples/cli_registration/main.py run greet-mood --name ada --no-excited
uv run --with-editable ".[cli]" python examples/cli_registration/main.py run greet --input request.json
printf '%s' '{"name":"Ada"}' | uv run --with-editable ".[cli]" python examples/cli_registration/main.py run greet --input -
```

Create `request.json` containing `{"name":"Ada"}` before the file example. The
stdin pipeline above uses a POSIX shell. In PowerShell:

```powershell
'{"name":"Ada"}' | uv run --with-editable ".[cli]" python examples/cli_registration/main.py run greet --input -
```

`greet` explicitly uses positional `name`. `greet-mood` keeps generated named
options, renames `mood` to `--tone`, and exposes `--excited` / `--no-excited`.
Canonical JSON still uses `mood`. Required fields can always be supplied through
JSON instead of positional arguments or options. Mixing input forms is an error.

See [CLI input](../../docs/design/cli-input.md) for the customization API,
[CLI local execution](../../docs/design/cli-local.md) for in-process run behavior,
and [CLI registration](../../docs/design/cli-registration.md) for exit conventions.
