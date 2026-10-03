# JobCLI registration example

Demonstrates explicit `JobCLI.add(...)` registration, request input via `--json`,
`--input`, generated field options, optional `positional_fields` / `field_options`,
and `run` / `submit` groups. Execution returns a diagnostic stderr message and a
non-zero exit code until the dependent execution slices land.

## Run

Install the CLI extra, then invoke help or an unavailable command:

```bash
uv run --with-editable ".[cli]" python examples/cli_registration/main.py --help
uv run --with-editable ".[cli]" python examples/cli_registration/main.py run --help
uv run --with-editable ".[cli]" python examples/cli_registration/main.py run greet world
uv run --with-editable ".[cli]" python examples/cli_registration/main.py submit greet --json '{"name":"json"}'
uv run --with-editable ".[cli]" python examples/cli_registration/main.py submit greet-mood --name ada --tone cheerful --excited
uv run --with-editable ".[cli]" python examples/cli_registration/main.py submit greet-mood --name ada --no-excited
uv run --with-editable ".[cli]" python examples/cli_registration/main.py submit greet --input request.json
printf '%s' '{"name":"Ada"}' | uv run --with-editable ".[cli]" python examples/cli_registration/main.py submit greet --input -
```

Create `request.json` containing `{"name":"Ada"}` before the file example. The
stdin pipeline above uses a POSIX shell. In PowerShell:

```powershell
'{"name":"Ada"}' | uv run --with-editable ".[cli]" python examples/cli_registration/main.py submit greet --input -
```

`greet` explicitly uses positional `name`. `greet-mood` keeps generated named
options, renames `mood` to `--tone`, and exposes `--excited` / `--no-excited`.
Canonical JSON still uses `mood`. Required fields can always be supplied through
JSON instead of positional arguments or options. Mixing input forms is an error.

See [CLI input](../../docs/design/cli-input.md) for the customization API and
[CLI registration](../../docs/design/cli-registration.md) for exit conventions.
