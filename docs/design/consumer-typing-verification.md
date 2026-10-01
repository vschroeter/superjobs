# Automated consumer typing verification

Implemented for [Automate source and installed-wheel consumer typing verification](https://github.com/vschroeter/superjobs/issues/11).

## Commands

From the repository root, with Python and uv available:

```bash
python tools/verify_contract_typing.py --python 3.12 --python 3.14 --evidence dist/verification/issue11
python -m pytest -m contract_typing -q
python -m pytest -m "not nats and not contract_typing" -q
```

The runner builds non-editable library and shared-contract wheels into a temporary
root outside the checkout. It uses fresh environments, removes source path environment
variables and disables user site packages. Both mode uses the same Python 3.12
environment for dependency resolution in source and wheel typing. Source mode
explicitly selects the repository library/contract sources; wheel mode uses installed
packages with no source extraPaths and autoSearchPaths disabled.

Pyright is pinned to **1.1.414**, basic mode, static Python target **3.12**.
Negative fixtures declare exact expected rules on each misuse line. The runner
compares file/line/rule multisets and rejects missing/unexpected diagnostics,
warnings, malformed checker output, and failed commands. Positive suites require
zero errors and warnings. The measured-gap probes remain outside the normative gate.

Installed runtime checks cover public handler registration and client lifecycle.
An independent stdlib probe records interpreter identity, package import origins,
package-level py.typed markers, install metadata and producer isolation. Producer
imports succeed without any worker implementation module available. Origins and
runtime stdout/stderr are retained separately for each interpreter; source/wheel
checker JSON and normalized diagnostic metadata are retained per suite. Temporary
work is removed by default, including when evidence is requested.

## Independent measurements — 2026-10-01

| Check | Windows | Linux (WSL Ubuntu 24.04) |
| --- | --- | --- |
| Minimum-series installed runtime | Python 3.12.11, 24 passed | Python 3.12.3, 24 passed |
| Latest-stable-series installed runtime | Python 3.14.7, 24 passed | Python 3.14.7, 24 passed |
| Positive, producer-positive, strict-payload-positive (source and wheel) | 0 errors/warnings | 0 errors/warnings |
| Public negative controls (source and wheel) | 46 exact diagnostics | 46 exact diagnostics |
| Producer negative controls (source and wheel) | 17 exact diagnostics | 17 exact diagnostics |
| Strict-payload negative controls (source and wheel) | 1 exact diagnostic | 1 exact diagnostic |
| Source/wheel rules and sites | Match | Match |
| Deterministic runner helpers | 14 passed | Included in regression run |
| Regression selection, excluding contract_typing | 268 passed, 5 deselected | 268 passed, 5 deselected |
| Explicit contract_typing controls | 5 passed | 5 passed |

The five explicit checks exercise removed markers, unexpected imports, a repaired
misuse retaining its expected-error marker, a removed installed py.typed marker,
and the complete source/wheel gate. Missing required errors and invalid package
setups fail instead of becoming accepted diagnostic counts. Fourteen fast helpers
also check command failures, malformed diagnostics and actual origin validation.

Local evidence is retained under `dist/verification/issue11/windows` and
`dist/verification/issue11/linux` (generated artifacts, not tracked source).
The 268-case regression selection includes all ten NATS cases from #10. The runtime
matrix above is specific to the installed consumer checks; this is not the complete
supported-Python CI matrix planned in #15. Verification ran against the working tree before commit.
