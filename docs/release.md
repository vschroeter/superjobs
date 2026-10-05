# PyPI release procedure

SuperJobs **0.1.0** is pre-alpha. Publishing is manual: merge to `main`, wait for
CI, tag, then publish a GitHub Release. Pushes to `main` do **not** upload to PyPI.

## Prerequisites

- `pyproject.toml` `[project].version` matches the intended tag (`v0.1.0` for `0.1.0`).
- The release commit is on `main`.
- GitHub Actions workflow **`checks`** completed successfully on a **push to `main`**
  for that **exact commit** (the most recent matching run by time must be
  `completed`/`success`), including aggregate jobs
  **`checks / fast`** and **`checks / integration`** from that run (see
  [development.md](development.md)).
- Repository secret **`PYPI_API_TOKEN`** is configured (used only in the publish job).

## Local dry run (optional)

From a clean checkout at the release tag:

```bash
uv build --no-sources
uv tool run twine check --strict dist/superjobs-0.1.0.tar.gz dist/superjobs-0.1.0-py3-none-any.whl
```

Inspect the wheel if needed:

```bash
unzip -l dist/superjobs-0.1.0-py3-none-any.whl | head
```

## Publish

1. Merge the release commit to `main` and confirm the **`checks`** workflow on that
   SHA shows both aggregates green.
2. Create and push an annotated tag matching the version, for example `v0.1.0`.
3. On GitHub, create a **Release** from that tag and **publish** it (not draft-only).
4. Workflow **`.github/workflows/release.yml`** runs on `release: published`:
   - checks out the tag, verifies tag ↔ `pyproject.toml` version, `main` ancestry, and CI gates;
   - builds with `uv build --no-sources`, runs `twine check --strict`, uploads wheel and sdist artifacts;
   - publishes exactly those two files with `uv publish` and `UV_PUBLISH_TOKEN`.

Re-run or fix CI on the release commit before publishing if gates are missing, pending,
or failed. The release workflow fails closed rather than bypassing checks.
