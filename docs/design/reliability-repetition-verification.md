# Optional NATS reliability repetition

Implemented for [Add bounded optional repetition of NATS reliability scenarios](https://github.com/vschroeter/superjobs/issues/22).

## Commands

From the repository root, with a **final CPython** interpreter and [uv](https://docs.astral.sh/uv/) available. The runner uses the active interpreter minor for child `verify_* --python` arguments (no override flag).

### Linux

```bash
uv run --isolated --no-project --with-editable . --python 3.14 --with "pytest>=9.1.1" --with "pytest-asyncio>=1.4.0" python tools/verify_reliability_repetition.py --seed 20261003 --artifact-dir dist/verification/reliability-repetition
uv run --isolated --no-project --with-editable . --python 3.14 --with "pytest>=9.1.1" --with "pytest-asyncio>=1.4.0" python -m pytest tests/test_verify_reliability_repetition.py -q
```

### Windows

```powershell
uv run --isolated --no-project --with-editable . --python 3.14 --with "pytest>=9.1.1" --with "pytest-asyncio>=1.4.0" python tools/verify_reliability_repetition.py --seed 20261003 --artifact-dir dist/verification/reliability-repetition
uv run --isolated --no-project --with-editable . --python 3.14 --with "pytest>=9.1.1" --with "pytest-asyncio>=1.4.0" python -m pytest tests/test_verify_reliability_repetition.py -q
```

Replace `3.14` with another supported final CPython minor when exercising locally; the optional weekly workflow pins **3.14** only.

## Scope

The repetition runner shuffles **twelve** isolated child invocations (four families × three repetitions):

| Family | Child command |
| --- | --- |
| Installed-process contracts/retries | Full `tools/verify_cross_program.py` suite |
| Active-handler kill | `tools/verify_worker_recovery.py --only-scenario recovery_before_completion` |
| Completion-before-ack kill | `tools/verify_worker_recovery.py --only-scenario recovery_after_completion` |
| Persistent broker restart | `tools/verify_broker_restart.py` (default scenario) |

`--seed` affects **ordering only**. Child runners keep their existing per-scenario deadlines and cleanup; the repetition layer adds a **900 second** whole-run wall clock (including setup, child work, and bounded cleanup reserve) and stops launching further items when the reserve is reached.

On the first failure the runner exits nonzero, retains per-item stdout/stderr, child artifact trees, and `run-summary.json` under a fresh per-invocation subdirectory of `--artifact-dir` (12-character hex token; family and repetition live in the summary only). Layout:

```
{artifact-dir}/{invocation-token}/
  run-summary.json
  items/
    {NN}/                 # zero-padded shuffle position only (00–11)
      command.json
      stdout.txt
      stderr.txt
      e/                  # child runner --artifact-dir (native py*/scenarios/… trees)
```

Default `{artifact-dir}` is `dist/verification/reliability-repetition` relative to the repository root (including nested `dist/worktrees/*` checkouts). Stale parent summaries cannot masquerade as green because each run writes under its own token directory. The summary records seed, invocation id, rich environment metadata, wall and monotonic elapsed time, and planned-vs-completed counts. Initial and per-item summary snapshots keep `ok: false` while work is in flight or after external kill. It never retries failed items to green. A child exit code of zero is insufficient: the runner validates the same summary evidence the underlying tools already emit (scenario cardinality, monotonic timestamps, exit codes, checkpoints/phases, and no child cleanup errors).

Deterministic orchestration regressions live in `tests/test_verify_reliability_repetition.py` (no full NATS gate by default).

## Measured verification — 2026-10-03

Codex independently reviewed and measured the repetition runner on this working tree. The
production whole-run cap remains **900** seconds (`INTEGRATION_JOB_BUDGET_SECONDS`); the timeout
control below patches that constant only for a one-off negative check.

| Check | Windows 11 | Linux (WSL Ubuntu 24.04) |
| --- | --- | --- |
| Full repetition (`--seed 20261003`) | CPython **3.14.7**: **12/12** items in **154.2** s | CPython **3.14.7**: **12/12** in **104.0** s |
| Artifact (`run-summary.json`) | `dist/verification/reliability-repetition/08cb3eca7708/` | `dist/verification/reliability-repetition-linux/cf7c1b71fbbf/` |
| `dev_check fast` (active interpreter) | **421 passed**, 16 deselected, **17.5** s | (not re-run for this slice) |
| Deterministic regressions | `tests/test_verify_reliability_repetition.py`: **26** passed | (not re-run for this slice) |

Each of the four families ran exactly **three** times after shuffle. Worker-recovery children used
`--only-scenario` for `recovery_before_completion` and `recovery_after_completion` only (not the
full worker-recovery matrix). Cross-program children ran the native **seven** scenarios per
repetition. The runner does **not** retry failed items within one invocation; a single failure
stops the run and retains evidence for completed and failed items.

An early Windows draft used a longer default artifact path and hit **`WinError 206`** (path too
long), leaving a retained **0/12** failure before any child completed. The compact layout
(`items/{NN}/e/` under a short per-invocation token) was corrected before these final green runs.

### Real failure control (Windows)

Missing broker binary: exit **1**, **0/12** completed, first item failed with child `run_error` and
retained `stdout.txt` / `stderr.txt`. Artifact:
`dist/verification/rr-failure/968b99aad0c7/run-summary.json`.

PowerShell (`NATS_EXECUTABLE` is scoped to this invocation and its children):

```powershell
uv run --isolated --no-project --with-editable . --python 3.14 --with "pytest>=9.1.1" --with "pytest-asyncio>=1.4.0" python -c "import os; os.environ['NATS_EXECUTABLE'] = r'C:\no-such-nats-server\nats-server.exe'; from tools.verify_reliability_repetition import main; raise SystemExit(main(['--seed', '20261003', '--artifact-dir', 'dist/verification/rr-failure']))"
```

### Real timeout control (Windows)

Patched whole-run budget **46** seconds (45 s cleanup reserve ⇒ at most **1** s for the first
child): exit **1**, **0/12** completed, first outcome `timed_out: true`, `cleanup_errors: []`, no
owned control processes left. Artifact:
`dist/verification/rr-timeout/417670ed66f2/run-summary.json`.

```powershell
uv run --isolated --no-project --with-editable . --python 3.14 --with "pytest>=9.1.1" --with "pytest-asyncio>=1.4.0" python -c "import scripts.dev_check.constants as c; c.INTEGRATION_JOB_BUDGET_SECONDS = 46; from tools.verify_reliability_repetition import main; raise SystemExit(main(['--seed', '20261003', '--artifact-dir', 'dist/verification/rr-timeout']))"
```

## Optional weekly GitHub Actions

Workflow file: `.github/workflows/reliability-repetition.yml`.

- **Platform:** `ubuntu-latest` only.
- **Python:** latest supported stable **3.14**, isolated `uv run` with the same pinned NATS provisioning as required integration checks.
- **Triggers:** weekly UTC off-peak `schedule` and `workflow_dispatch`.
- **Artifacts:** `dist/verification/reliability-repetition/**` on failure or cancellation.

GitHub runs `schedule` only on the repository **default branch** (`main`). The workflow may be committed on `api_design` or other branches for review, but the weekly schedule does not run until that workflow exists on `main`. This ticket does not merge to `main`.
