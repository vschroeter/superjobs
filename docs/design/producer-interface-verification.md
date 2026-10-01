# Producer constructor and SubmitOptions verification

Implementation of [issue #8](https://github.com/vschroeter/superjobs/issues/8), based on the interface selected in [issue #4](https://github.com/vschroeter/superjobs/issues/4). The shared contract package remains importable by producers without worker implementations.

## Public interface

```python
client = jobs.client(generate_manifest)
await client.submit(device_id="sensor-17")
await client.submit(SubmitOptions(timeout=5), device_id="sensor-17")
await client.submit(GenerateManifestRequest(device_id="sensor-17"))
await client.submit(GenerateManifestRequest(device_id="sensor-17"), options=SubmitOptions(timeout=5))
```

For constructor keywords, every keyword belongs to the request contract, including `options` and `timeout`. Submission settings use a positional `SubmitOptions` prefix. For an explicit request object, `SubmitOptions` is keyword-only; the existing execution keywords remain available. No-request jobs accept `submit()`, `submit(SubmitOptions(...))`, and the existing `submit(None)` form. Constructor positional arguments are rejected at runtime.

`Job(...)` infers the present request, result, and event types without manual generic annotations. A request-bearing Job retains its constructor parameter specification through `jobs.client(job)`, so Pyright checks required, defaulted, unknown, and mistyped keyword fields on dataclasses and Pydantic models. Explicit request objects and keyword fields use the same strict validation and submission path. Omitted identifiers are freshly generated per submission; timeout and deadline default to `None`, and caller scope defaults to `"default"`.

## Independent checks on 2026-10-01

Pyright 1.1.414 targeted Python 3.12 in basic mode; runtime checks used Python 3.13.5.

| Check | Result |
| --- | --- |
| Changed source package | Zero Pyright errors and warnings |
| Public source positive consumers | Zero Pyright errors and warnings |
| Public source negative producer consumers | 17 intended diagnostics |
| Isolated installed-wheel positive consumers | Zero Pyright errors and warnings in both existing and producer fixtures |
| Isolated installed-wheel negative consumers | 27 existing/explicit-annotation and 17 producer diagnostics after the follow-up |
| Deterministic tests | 233 passed; seven live-NATS tests deselected |
| Live-NATS tests | Seven passed |
| Built-wheel producer and worker in separate processes | All four contract cases completed |

The isolated process check imported both typed packages from `site-packages`, verified their `py.typed` markers, and had no worker handler module available to the producer. It used the keyword, positional-options, and no-request submission forms against a live NATS server. The test environment was created from built wheels with no editable install or source fallback.

Follow-up on 2026-10-01: A second isolated environment used Python 3.12.11 and the rebuilt wheels; all 19 producer-interface runtime tests passed. Public `RequestJob[Request, Result, Event, ...]` and `NoRequestJob[Result, Event]` annotations were added for applications that need an explicit variable or function parameter while retaining checked handler registration. The installed-wheel public positive fixture has zero Pyright errors; the negative fixture adds two intended diagnostics for the wrong explicitly annotated no-request handler. Construct Jobs through `Job(...)`; direct specialized construction rejects the opposite request shape.

Cursor Composer 2.5 made two bounded implementation attempts. Both failed source syntax/static and regression checks. Codex corrected the implementation under the repository's fallback rule and independently ran all checks above. This is verification of correctness for the measured Python/Pyright combination, not a performance claim.

## Measured limits

`ParamSpec` describes the request constructor's positional parameters as well as its keywords. The runtime intentionally rejects constructor positional convenience, but Pyright may still accept a positional call when the request class constructor accepts one. The shared example request dataclasses use keyword-only constructors so that this misuse is also rejected in the public examples. Ordinary positional dataclasses remain supported through explicit request objects.

Constructor keyword typing requires the inferred `RequestJob` specialization with its captured constructor `ParamSpec`. Widening to `Job[Request, Result, Event]` retains explicit-object submission, but loses constructor keyword typing. Handler registration overloads use only the request/no-request specializations; explicitly annotate a request-bearing value as `RequestJob[Request, Result, Event, ...]` or a no-request value as `NoRequestJob[Result, Event]` when its inferred type cannot be retained. The ellipsis form preserves handler typing but does not restore constructor keyword names. A manually widened base `Job` is producer-only for checked handler use: `@jobs.handler`, `jobs.register(job, handler)`, and `@job.handler` reject the widened job statically on all three forms. Runtime registration still validates signatures before backend registration, but Pyright overload consumers never see the broad implementation signatures. The no-request negative controls reject a two-argument `(request: None, context)` callback for both inferred and explicitly annotated no-request specializations.

No sync producer convenience helper was added in this slice. The selected async-first interface remains the implementation target for submission.
