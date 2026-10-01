# Handler registration: implementation and independent verification

Work for [Type handler registration and add explicit and contract-owned forms](https://github.com/vschroeter/superjobs/issues/7), following the confirmed [contract/handler decision](https://github.com/vschroeter/superjobs/issues/4). The earlier design/examples/outcome iteration is committed as `b75616a`; the original registration implementation is committed as `ef6369f`. The final typing-boundary correction below is included in the verification foundation update.

## Implemented interface

```python
@jobs.handler(job)
async def handler(request: Request, context: JobContext[Event]) -> Result: ...

jobs.register(job, plain_handler)

@job.handler
async def marked_handler(request: Request, context: JobContext[Event]) -> Result: ...
jobs.register(marked_handler)
```

These are alternative registrations. Runtime registration uses one authoritative path; it checks association, callability and duplicate identity, constructs the handler to validate its options, then registers with the backend. Job/callback operands of `register` are positional-only; concurrency, retry, observation and heartbeat policies remain keyword-only, including the existing `retry=` spelling.

Every execution supplies context. No-request handlers receive context alone. Contract decoration attaches association metadata and returns the original function without creating a runtime registry or importing application worker code. A marked function can be registered in distinct runtimes. Conflicting metadata, duplicate registration, missing context, extra required arguments and invalid options fail before backend registration.

## Typing strategy and authorship

Cursor Composer 2.5 implemented the registration paths, descriptor, examples and runtime tests. Its shell commands were blocked; Codex ran the independent checks. Two early implementations failed positive static checks, and a third retained fixed-signature limitations. Following the repository's fallback rule after repeated unsuccessful attempts, Codex replaced the callback typing with a measured protocol strategy and completed targeted corrections.

An overloaded callback protocol proves two properties: the callback accepts the runtime's contract arguments and returns the Job's result or an awaitable of it; a separate overload captures its complete `ParamSpec` and concrete return type. The decorator returns `Callable[P, Return]`, preserving names, defaults, additional optional parameters and narrower return types. It returns the same function at runtime, preserving sync/async classification and avoiding invocation wrappers.

Positive public-import consumers prove all three forms, sync/async and no-request/no-result cases, named direct calls, alternative parameter names, prefix defaults, optional keyword parameters and a more specific result subtype. They contain no casts, Any annotations or diagnostic suppression. Negative consumers reject mismatched request/context/result types, missing context, additional required arguments, and invalid direct calls on preserved signatures.

## Measured results on 2026-09-29

Pyright **1.1.414**, basic mode, **Python 3.12 static target**; runtime **Python 3.13.5**.

| Check | Result |
| --- | --- |
| Source positive consumers, including producer/worker examples | Zero errors and warnings |
| New private typing helpers and runtime module checked directly | Zero errors and warnings |
| Source negative consumers | 23 intended diagnostics: 19 `reportArgumentType` and four `reportCallIssue` |
| Isolated installed-wheel positive consumers | Zero errors and warnings |
| Isolated installed-wheel negative consumers | Same 23 rules and locations, asserted exactly |
| Remaining-gap probes | Omitted event remains `Unknown`; explicit no-request registration gap below remains visible |
| Deterministic runtime suite | 159 passed; six existing NATS tests deselected |
| Diff whitespace check | Passed after cleanup |

Fresh library and shared-contract wheels were installed without editable packages. Both import origins and `py.typed` markers were verified under the isolated `site-packages`; worker application code was not importable. Verbose checker search paths contained no repository library/contract source directories. An initial wheel run preceded a positional-only signature alignment; the final wheel is rebuilt and checked after that change.

Runtime tests cover every registration path, sync and async execution, no-request/no-result behavior, callable identity/defaults, metadata-only decoration, separate runtimes, duplicate/conflicting bindings, invalid signatures/options before backend mutation, and existing dynamic registration/lifecycle behavior. The underlying JobHandler and transports remain unchanged. No NATS rerun or Python 3.12 runtime execution is claimed for this registration slice.

## Maintainer-selected typing boundary (2026-10-01)

Checked handler registration requires an inferred `Job(...)` specialization or an explicit `RequestJob[Request, Result, Event, ...]` / `NoRequestJob[Result, Event]` annotation. A deliberately widened base `Job[Request, Result, Event]` remains useful for explicit-object producer clients through `jobs.client(job)`, but must not provide checked registration on `@jobs.handler`, `jobs.register(job, handler)`, or `@job.handler`. Registration overloads use only the request/no-request specializations; there is no base-`Job` registration overload.

The former no-request `jobs.register` gap through a general `Job[None, ...]` overload is closed for inferred and explicitly annotated `NoRequestJob` values. Deliberately widened base `Job` values are rejected by static checks on all three registration forms; negative fixtures cover request and no-request shapes for runtime decoration, explicit `register(job, handler)`, and metadata `@job.handler`. Constructor keyword erasure on widened jobs remains in `typing/measured_gaps`. Omitted-payload inference, keyword submission, SubmitOptions, strict payload policy, fingerprints/manifest and sync producer convenience remain separate scopes selected in #4.

The producer slice's earlier measurements are recorded in [producer constructor verification](producer-interface-verification.md). The measurements above remain historical; the final boundary was checked independently as follows.

## Final independent verification (2026-10-01)

Cursor Composer 2.5 removed the general `Job` descriptor overload and extended public consumer fixtures. Codex rejected its first attempt, which introduced unchecked `Any` decorators, and requested the corrected presence-aware policy and stronger negative controls. The final library change only removes a descriptor overload; runtime registration, callable identity, and transport behavior are unchanged.

Pyright **1.1.414**, basic mode, **Python 3.12 target**:

| Check | Result |
| --- | --- |
| Source typing modules and public positive consumers | Zero errors and warnings |
| Installed-wheel public positive consumers | Zero errors and warnings |
| Public negative consumers, source and wheel | 46 intended diagnostics with identical rules and locations |
| Producer consumers, source and wheel | Zero positive diagnostics; 17 matching intended negative diagnostics |
| Strict-payload consumers, source and wheel | Zero positive diagnostics; one matching intended negative diagnostic |
| Deterministic suite on Python 3.13.5 | 234 passed; seven NATS tests deselected |
| Installed-wheel registration, public API, and outcome tests on Python 3.12.11 | 27 passed |
| Diff whitespace check | Passed |

The negative controls cover missing context, wrong no-request arity, extra required arguments, and all six combinations of widened request/no-request Jobs and registration forms. Widened-Job controls use otherwise valid callbacks and function parameters that prevent narrowing back to a specialization. Every negative function was checked for diagnostics at the intended call or decorator site; missing-import failures were excluded. Positive fixtures retain checked request/event/result and outcome behavior and contain no casts, `Any` annotations, or suppression.

Both rebuilt distributions were installed into an isolated Python 3.12 environment. Wheel consumer configs use empty `extraPaths`; runtime imports and `py.typed` markers were verified under `site-packages`, with no worker handler module available to the producer import check. Source and wheel diagnostics were compared exactly. No new NATS run is claimed: this correction changes only static descriptor overload selection, and the existing deterministic registration/execution tests pass against both source and the installed distribution.

## Reproduction

Source commands and fixtures are in `examples/contract_interface/README.md`. For wheel consumers, build/install the library and contract wheels into a fresh environment, copy only the positive/negative/gap fixtures, and select that environment in a Pyright config with empty `extraPaths` and a Python 3.12 target. The negative suite intentionally exits one; assert the diagnostic rules and locations, rather than treating any failure as success. Temporary wheel/environment files are removed after verification.
