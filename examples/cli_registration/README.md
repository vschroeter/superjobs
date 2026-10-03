# JobCLI registration example

Demonstrates explicit `JobCLI.add(...)` registration, `run` / `submit` groups, and
help output. Execution returns a diagnostic stderr message and a non-zero exit code
until the dependent execution slices land.

## Run

Install the CLI extra, then invoke help or an unavailable command:

```bash
uv run --with-editable ".[cli]" python examples/cli_registration/main.py --help
uv run --with-editable ".[cli]" python examples/cli_registration/main.py run --help
uv run --with-editable ".[cli]" python examples/cli_registration/main.py run greet
```

See [docs/design/cli-registration.md](../../docs/design/cli-registration.md) for API
details and exit conventions.
