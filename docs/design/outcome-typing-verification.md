# Outcome typing: independent verification

Implementation and verification for [Preserve final result types in JobHandle.outcome()](https://github.com/vschroeter/superjobs/issues/6), after the maintainer confirmed and resolved [the contract/handler design](https://github.com/vschroeter/superjobs/issues/4) on 2026-09-29.

## Change and review

Cursor Composer 2.5 changed the return annotation in `src/superjobs/jobs/job_handle.py` from `JobOutcome` to `JobOutcome[FinalT]`. Codex independently reviewed the diff: the existing union, public exports, and method body are unchanged. No transport implementation changed.

The positive public-import consumer now asserts the final result type in the outcome union and after narrowing to `JobSucceeded`, including a reconstructed handle and the no-result `None` contract. It also checks failure and cancellation fields. No casts, `Any` annotations, or diagnostic suppression were added to these checks.

Resolved outcome probes moved out of the measured-gap suite. The missing-event inference and handler callable erasure probes remain. Historical research reports retain their original baseline measurements.

Cursor could edit the bounded files, but its shell actions were rejected. The results below were executed independently by Codex; they are not inferred from Cursor's expected results.

## Measured results

Environment: Windows; Python runtime **3.13.5**; Pyright **1.1.414**, basic mode, Python **3.12 static target**. This iteration does not claim a Python 3.12 runtime execution.

| Check | Result |
| --- | --- |
| Source positive consumers, including producer and worker modules | Zero errors or warnings; three files analyzed |
| Source negative controls | Exactly two `reportArgumentType` errors: incorrect request and event, at lines 16 and 20 |
| Source remaining-gap probes | Zero errors; missing event inferred as `Unknown`, decorated handler as `(...) -> Any` |
| Installed-wheel positive consumers | Zero errors or warnings |
| Installed-wheel negative controls | Exactly the same rules and source locations |
| Installed-wheel remaining-gap probes | Same two information diagnostics; no outcome assertion failures |
| Deterministic runtime suite | **142 passed**, six NATS tests deselected |
| Tracked diff whitespace check | Passed |

The runtime suite includes successful typed and `None` outcomes in the in-memory contract example, plus failed and cancelled outcomes in existing execution tests. NATS was not rerun for this annotation-only library change; no new transport guarantee is claimed.

## Isolation and reproduction

Build the library and contract wheels separately with `uv build --wheel --project ... --out-dir ...`. Install those two wheels into a fresh virtual environment, without editable installs. Copy only the three `check_types.py` consumer fixtures to an isolated directory as `positive.py`, `negative.py`, and `measured_gaps.py`.

The isolated checker configuration used:

```json
{
  "include": ["positive.py"],
  "extraPaths": [],
  "venvPath": "..",
  "venv": "wheel-venv",
  "pythonVersion": "3.12",
  "typeCheckingMode": "basic"
}
```

Pyright verbose output showed the consumer directory, typeshed/stubs, and the isolated virtual environment's `site-packages`; no repository library or contract source directory was searched. Isolated Python imports verified both package origins under that `site-packages`, both `py.typed` markers, and that `worker_handlers` was not importable.

Run `uv tool run --from pyright==1.1.414 pyright --project pyrightconfig.json --outputjson positive.py negative.py measured_gaps.py`. This deliberately exits **1** because of the two negative controls. Codex asserted that every error was `reportArgumentType` in `negative.py` at lines 16 and 20, there were no warnings, and the only two information diagnostics were the remaining gaps. Positive checks were also run separately and exited zero.

Source commands remain documented in `examples/contract_interface/README.md`. The deterministic command was `python -m pytest -q -m "not nats" -p no:cacheprovider`, using the project environment.

Temporary wheels, consumer copies, and the isolated environment are removed after verification. The implementation and fixtures remain local and uncommitted; there is no published package or pull request from this iteration. Broader typing automation and transport verification remain in [issue #5](https://github.com/vschroeter/superjobs/issues/5).
