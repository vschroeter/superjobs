# Contract interface verification

Measured by Codex on 2026-09-29, library branch `api_design`, commit `d9bdcad`. Cursor Composer 2.5 created the examples and applied targeted corrections; Codex independently reviewed and verified them. No library implementation or root dependency configuration changed. Existing local changes were preserved.

## Results

| Check | Result |
| --- | --- |
| Existing fast suite before examples | 140 passed, 6 NATS tests deselected |
| Final fast suite including source example tests | 142 passed, 6 NATS tests deselected |
| Positive source consumers, producer, worker handlers | Pyright 1.1.414: 0 errors |
| Same positive consumers against installed wheels | Pyright 1.1.414: 0 errors |
| Negative consumers against installed wheels | Exactly 2 `reportArgumentType` errors: incorrect request and event |
| Outcome gap probes against installed wheels | Exactly 2 `reportAssertTypeFailure` errors: generic outcome and narrowed result |
| Inference and handler probes | Omitted event inferred as `Unknown`; decorated handler revealed as `(...) -> Any` |
| In-memory demo using installed wheels | All four contracts, reconstructed handles, ordered intermediate events, and final results passed |
| Real local NATS, separate producer/worker processes | All four cases passed; worker was ready before producer startup |
| Producer import isolation | Only `producer.py` copied; `find_spec('worker_handlers') is None`; both package imports resolved inside isolated `site-packages` |

This NATS run is a concrete cross-process example, not a crash/recovery or transport conformance suite. Existing marked NATS tests were not rerun because library code did not change. No CI workflow was added. Broader guarantees remain in [Test strategy for separate programs and NATS](https://github.com/vschroeter/superjobs/issues/5).

## Environment and reproduction

Runtime Python: 3.13.5 on Windows. Static target: Python 3.12, Pyright `basic`. Python 3.12 runtime execution was not tested.

The isolated environment installed SuperJobs 0.1.0 and shared contract example 0.0.1, with FastStream 0.7.7, nats-py 2.16.0, Pydantic 2.13.5 and msgpack 1.2.3. These are the dependencies resolved for the wheel consumer, not a claim that the existing project environment uses identical versions. Offline installation initially lacked a cached msgpack wheel; normal isolated installation succeeded.

Build/install instructions are in [the example README](../../examples/contract_interface/README.md). Consumer checks used non-editable wheel installs and no source `extraPaths`. ZIP inspection confirmed `superjobs_contract_example/py.typed` in the shared contract wheel.

Source checks from the repository root:

```powershell
.venv/Scripts/python.exe -m pytest -m 'not nats' -q -p no:cacheprovider
uv tool run --offline --from pyright==1.1.414 pyright --project examples/contract_interface/typing/positive
uv tool run --offline --from pyright==1.1.414 pyright --project examples/contract_interface/typing/negative --outputjson
uv tool run --offline --from pyright==1.1.414 pyright --project examples/contract_interface/typing/measured_gaps --outputjson
```

The negative and gap commands intentionally exit nonzero. Check diagnostic rules and locations, not only exit status.

For installed-wheel checks, create separate Pyright configurations selecting the same consumer files, `venvPath` pointing at the isolated environment's parent, `venv` selecting that environment, `pythonVersion: "3.12"`, and `typeCheckingMode: "basic"`. Use relative `include` paths from each configuration and omit source `extraPaths`. Confirm imported package paths resolve into that environment's `site-packages`.

The NATS run used `nats://localhost:4222` and a fresh readiness path. Codex launched its worker in a hidden process, waited for readiness, ran the isolated producer, and terminated only that owned worker after all requests completed. Temporary verification files and environments were cleaned up. Demo execution records may remain subject to server retention.

## Open decisions

- Mandatory context remains a recommendation awaiting maintainer input.
- Explicit `Job[...]` annotations avoid current missing-payload inference gaps; annotation-free inference remains a target.
- Wrong handler registration is still accepted statically; preserving/checking its signature needs a separate library change.
- Nullable payloads, sync-handler guarantees, and incompatible contract versions remain open.
- [Preserve final result types in JobHandle.outcome()](https://github.com/vschroeter/superjobs/issues/6) is planned, not implemented.
