# JobCLI registration example

Demonstrates explicit `JobCLI.add(...)` registration, request input via `--json`,
`--input`, generated field options, optional `positional_fields` / `field_options`,
and `run` / `submit` groups. Local `run` executes in-process through the
example `local_runtime_factory` (issues #39–#40). `submit` uses
`remote_runtime_factory` against NATS (`SUPERJOBS_NATS_URL`, default
`nats://localhost:4222`). The example broker uses `allow_reconnect=False`, and the
CLI applies a finite 30-second startup bound. Confirmed
acceptance prints a JSON execution reference (`job_id`, `job_name`,
`job_version`); without `--wait` the CLI does not wait for worker completion.
See [CLI user guide](../../docs/cli.md).

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
uv run --with-editable ".[cli]" python examples/cli_registration/main.py submit greet-remote --json '{"name":"Ada"}'
uv run --with-editable ".[cli]" python examples/cli_registration/main.py submit greet-remote --json '{"name":"Ada"}' --wait --wait-timeout 30
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

See [CLI user guide](../../docs/cli.md) for input customization, local `run`,
remote `submit`, and exit conventions.
