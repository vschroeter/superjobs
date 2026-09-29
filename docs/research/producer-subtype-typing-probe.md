# Producer constructor typing: subtype factory probe

Static experiment for [Preserve request constructor typing and add optional SubmitOptions](https://github.com/vschroeter/superjobs/issues/8), independently checked on 2026-09-29 with Pyright 1.1.414, basic mode, Python 3.12 target. This is a stub prototype, not a production API or a resolution of #8. Strict construction remains dependent on [Validate payloads strictly before request construction](https://github.com/vschroeter/superjobs/issues/9).

## Representation tested

An overloaded `Job.__new__` returns a subtype of the existing three-slot `Job[Request, Result, Event]`:

- `RequestJob[Request, Result, Event, P]` retains the constructor as `Callable[P, Request]`.
- `NoRequestJob[Result, Event]` represents an absent request.
- `client(RequestJob[..., P])` returns a client with the same ParamSpec. Its overloads are explicit object plus keyword-only `options`, and optional positional-only options plus paired `P.args`/`P.kwargs`.
- A Job passed through a function parameter typed only as `Job[Request, Result, Event]` receives an explicit-object-only client. Constructor keywords then fail visibly rather than accepting arbitrary fields.
- Context-only registration accepts `Job[None, Result, Event]`; request-bearing registration accepts the specialized RequestJob. This avoids the general overload's `Request=None` fallback, but has the compatibility limit below.

Cursor Composer 2.5 authored the prototype and two targeted corrections. Its shell commands were rejected; Codex independently ran all checks. After repeated unsuccessful attempts, Codex corrected the test classification, tested genuinely omitted slots, added a real widened no-request parameter control, and added missing compatibility/positional probes. No production source or existing consumer fixture was changed by this experiment.

## Measured checks

The positive fixture and prototype declarations analyzed together produced **zero errors/warnings** across 11 files. Tested cases include:

- Manifest with events, manifest without events, telemetry without result/events, and heartbeat without request/events. Omitted slots were inferred as None; they were not merely supplied explicitly as None.
- Keyword-only dataclasses and a Pydantic model, typed handles/results, defaulted Pydantic fields, and ordinary dataclass explicit objects.
- Optional options prefix, omitted options, explicit objects with keyword options, and a request business field named `options`.
- Existing full handler signature proofs using the registration prototype, and explicit-object submission after constructor signature erasure through an annotated function boundary.

The negative fixture produced **18 substantive diagnostics**, ten `reportArgumentType` and eight `reportCallIssue`, without declaration cascades. Every intended site was diagnosed:

| Rejection | Rule(s) |
| --- | --- |
| Missing required constructor field | Call issue |
| Incorrect dataclass/Pydantic field type | Argument type |
| Unknown constructor keyword | Call issue |
| Mixed explicit object and constructor fields | Argument type |
| Unrelated explicit request object | Argument type |
| Incorrect options value in both submission forms | Argument type |
| Request keywords on a no-request Job | Call issue |
| Constructor keywords after Job signature erasure | Call issue |
| Bare positional constructor string | Argument type |
| Request-bearing handler on a no-request constant or `Job[None, ...]` function parameter | Call issue and argument type |
| Missing context on a request handler | Call issue and argument type |
| Execution-option spelling used as an undeclared request keyword | Call issue |

The remaining-gap fixture produced **six diagnostics and four type observations**. Three diagnostics reject bare positional strings, as intended. The others expose valid operations that this representation loses:

1. A custom constructor overloaded between positional-only `device_id` and keyword-only `bundle_id` retains only the first signature in this probe. The valid `submit(bundle_id="...")` is rejected. This case was moved from the positive fixture into the gap fixture, not removed or claimed successful; explicit objects remain accepted.
2. A valid request handler cannot be explicitly registered after its Job is widened through a parameter typed as the base `Job[Request, Result, Event]`. Restoring the general registration overload also restores the no-request fallback. This compatibility tradeoff is unresolved.

An additional undesirable call, `ordinary_client.submit(None, "device-id")`, is **accepted statically** after the options prefix because ParamSpec retains positional constructor parameters. A bare string is rejected as an invalid options/object argument; that alone does not prove keyword-only convenience. Production must enforce the selected keyword form and avoid claiming exact static exclusion for arbitrary positional constructors.

## Limits and next implementation boundary

The tested representation proves a useful constructor/inference direction for ordinary shared DTO definitions; it does not settle every constructor overload or base-annotation compatibility case. The production interface must preserve these findings without public Any erasure. RequestJob/NoRequestJob are prototype names, not public exports selected by this report.

This experiment contains stub bodies and uses a scratch package on checker source paths. It proves no runtime construction, payload validation, option defaults, transport semantics, installed-wheel propagation or synchronous lifecycle behavior. Custom callable constructors and all eight possible payload-presence combinations were not tested. The handler implementation in #7 retains its measured no-request registration gap until a production typing change satisfies its acceptance criteria.

The temporary prototype files were removed after recording the results. Recreate the subtype factory/client overloads above with separate positive, negative and remaining-gap fixtures to repeat the experiment. For production acceptance, use the real public SuperJobs imports, the canonical strict construction boundary, and source plus isolated installed-wheel consumers.
