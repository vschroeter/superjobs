# Deterministic completion and retention test synchronization

Implemented for [Make completion and retention test synchronization deterministic on Windows Python 3.12](https://github.com/vschroeter/superjobs/issues/25).

The original two tests failed independently on Windows Python 3.12: the final acknowledgement still belonged to attempt 1, and result expiry was not observed. Both tests assumed asynchronous completion after a 10 ms sleep. These observations did not establish a production defect.

Cursor Composer 2.5 implemented the test-only correction in an isolated worktree. Codex reviewed the changes and merged them locally. The redelivery test cooperatively waits for the exact attempt-2 acknowledgement with a one-second deadline, preserving the assertion that the handler runs once. The retention test reads the completed typed result with a bounded public call, advances a test-local transport clock beyond the retention interval, and verifies both result expiry and the retained completed status. No production transport or public API changed.

## Verification on 2026-10-01

The deterministic selection is:

```sh
uv run --no-project --python 3.12 --with . --with pytest --with pytest-asyncio python -m pytest -m "not nats and not contract_typing" -q
```

The independently executed regression selection, including real NATS tests, is:

```sh
uv run --no-project --python 3.12 --with . --with pytest --with pytest-asyncio python -m pytest -m "not contract_typing" -q
```

Substitute the selected interpreter minor for `3.12`. Linux checks ran in WSL Ubuntu 24.04 with an isolated parent environment rather than modifying the Windows project environment.

| Selection | Windows | Linux |
| --- | --- | --- |
| Deterministic, Python 3.12 / 3.13 / 3.14 | 334 passed on each minor | 333 passed and one Windows-only skip on each minor |
| Regression including NATS, Python 3.12 | 345 passed | 344 passed and one Windows-only skip |
| Regression including NATS, Python 3.14 | 345 passed | 344 passed and one Windows-only skip |

Five dedicated consumer-typing gate tests are excluded from the regression selection; the typing fixtures and library contracts are unchanged. The CI foundation in issue #15 separately integrates the installed-distribution and static consumer gates. Python 3.14 reports existing harness deprecation warnings for `asyncio.iscoroutinefunction`; these warnings do not change the test outcome.
