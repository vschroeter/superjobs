# Handler registration: implementation and independent verification

Work for [Type handler registration and add explicit and contract-owned forms](https://github.com/vschroeter/superjobs/issues/7), following the confirmed [contract/handler decision](https://github.com/vschroeter/superjobs/issues/4). The earlier design/examples/outcome iteration is committed as `b75616a`. This registration iteration remains local and uncommitted.

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

## Remaining acceptance criterion

`jobs.register(no_request_job, two_argument_handler)` can still match the general `Job[ReqT, ...]` overload when `ReqT` is `None`. Pyright therefore accepts the deliberately wrong `(request: None, context: JobContext[None]) -> HeartbeatResult` callback on the explicit form. Both decorators reject the same shape statically, and runtime validation rejects it on every form before registration.

This is a measured gap in `typing/measured_gaps`, not a waived requirement or completed static guarantee. Issue #7 remains open until the Job/constructor typing follow-up can represent request presence sufficiently to reject that general-overload fallback. Omitted-payload inference, keyword submission, SubmitOptions, strict payload policy, fingerprints/manifest and sync producer convenience remain separate scopes selected in #4.

Update, 2026-10-01: The producer interface in [issue #8](https://github.com/vschroeter/superjobs/issues/8) introduces inferred request/no-request Job specializations. The explicit no-request registration negative control is now rejected by Pyright. The historical measurements above describe the earlier #7 iteration; current results and the remaining base-Job typing limit are recorded in [producer constructor verification](producer-interface-verification.md).

## Reproduction

Source commands and fixtures are in `examples/contract_interface/README.md`. For wheel consumers, build/install the library and contract wheels into a fresh environment, copy only the positive/negative/gap fixtures, and select that environment in a Pyright config with empty `extraPaths` and a Python 3.12 target. The negative suite intentionally exits one; assert the diagnostic rules and locations, rather than treating any failure as success. Temporary wheel/environment files are removed after verification.
