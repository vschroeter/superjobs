# JobCLI registration example

Demonstrates a shared `HandlerCatalog`, CLI `Command` metadata, built-in local
`run`, and built-in NATS `submit` via `JobCLI(handlers=..., nats_url=...)`
(issues #39–#46). Legacy `add()` remains for `greet-lazy` (`handler_factory`)
and `greet-remote` (`remote_only=True`).

Configured broker URL for submit (when not using `--nats-url`):
`SUPERJOBS_NATS_URL`, then `SUPERJOBS_CLI_CONFIGURED_NATS_URL` passed to
`JobCLI`, then the library default `nats://localhost:4222`. See
[CLI user guide](../../docs/cli.md).

## Run

```bash
uv run --with-editable ".[cli]" python examples/cli_registration/main.py --help
uv run --with-editable ".[cli]" python examples/cli_registration/main.py run greet world
uv run --with-editable ".[cli]" python examples/cli_registration/main.py run greet --json '{"name":"json"}'
uv run --with-editable ".[cli]" python examples/cli_registration/main.py run greet-mood --name ada --tone cheerful --excited
uv run --with-editable ".[cli]" python examples/cli_registration/main.py submit greet-remote --json '{"name":"Ada"}'
uv run --with-editable ".[cli]" python examples/cli_registration/main.py submit greet-remote --json '{"name":"Ada"}' --wait --wait-timeout 30
```

Remote submit requires a running NATS broker and a worker that registers the same
`GREET_JOB` handler.
