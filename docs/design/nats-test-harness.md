# NATS integration test harness

Issue [#10](https://github.com/vschroeter/superjobs/issues/10) provides native,
isolated JetStream servers. Explicitly selected integration tests fail on missing
infrastructure; they never skip startup errors.

```bash
uv run pytest -m nats
uv run pytest -m "not nats and not contract_typing"
```

The fast selection never provisions a broker. The default integration selection
starts one session-owned server and uses unique Job names per case. Dedicated
recovery cases use `nats_owned_server`, even when `NATS_URL` supplies an external
server for ordinary tests. External stores are never cleaned by the harness.

Automatic provisioning supports Windows/Linux amd64 and pins NATS **2.15.0**.
Official archive SHA-256 values are committed under `tests/support/nats_harness/fixtures/`.
The archive is verified before extracting only the executable. Cached archives
are verified again and cached executable bytes checked against their install
manifest. A valid cache works offline. Corrupt archives fail visibly.
`SUPERJOBS_NATS_CACHE` overrides the default `~/.cache/superjobs/nats` directory.
Other platforms can provide `NATS_EXECUTABLE`; its exact version and exit status
are checked. Downloads and version commands have explicit timeouts; provisioning
is a separate step from native startup.

Native startup uses NATS's random-port setting (`port: -1`) without a free-port
probe, then probes JetStream `account_info`. Startup has one 30-second deadline;
shutdown reserves time for a kill and reap within one 30-second deadline.
Asynchronous NATS scenarios have a 25-second deadline covering application
startup, scenario waits and application shutdown.

`OwnedNatsServer.start()` allocates a fresh owned root and store. `pause(target)`
stops the process while keeping that store; `restart(target)` reuses its store
and client port. `stop(target)` removes the owned root and store. Failure during
startup or restart performs final cleanup. Logs live independently in the system
temporary directory (override `SUPERJOBS_NATS_LOG_DIR`) and remain after cleanup,
including stdout/stderr emitted before the server could start. A fixture uses
the same mutable target across restart so final teardown stops the current process.

Fixtures: `nats_url`, `nats_broker`, `nats_broker_factory`, and
`nats_owned_server` (owner/target pair). They are registered through `tests/conftest.py`.

## Independent verification — 2026-10-01

- Windows Python 3.13.5: full suite **254 passed**; broker-free selection
  **244 passed, 10 deselected**.
- Linux (WSL Ubuntu 24.04) Python 3.12.3, rebuilt library wheel: full suite
  **254 passed**. Repeated integration selection **10 passed**.
- Integration selection includes all seven original library cases and three
  harness cases (store-preserving restart, isolated/offline reuse, native failed startup).
- Wrong-version executable and corrupt cached archive controls each produced
  pytest exit 1 with a setup error, no skip. Unreachable `NATS_URL` produced exit 1
  after 30.03 seconds. A deliberately failing pytest case produced exit 1;
  its process was reaped, owned root removed, and log retained.
- Real fresh-bucket startup exposed [#24](https://github.com/vschroeter/superjobs/issues/24).
  The narrow `NoKeysError` fix and three deterministic regressions are included.

These checks exercise the test foundation and required integration gates (#10–#15,
including idle outage #20). Optional manual repetition (#22) and performance
baselines (#23) are documented separately in
[docs/README.md](../README.md); they do not expand harness closure criteria.
