# Historical typing research (consolidated)

**Status:** Historical reference only. Normative behavior and limits are in
[api.md](../api.md); decisions in [adr/](../adr/README.md). Restored primary-source
research on fingerprints and validation lives in
[contract-fingerprint-detection.md](contract-fingerprint-detection.md) and
[strict-payload-validation.md](strict-payload-validation.md).

This file merges three overlapping pre-consolidation typing probes. Baseline
observations below reflect **2026-09-28** library state (`d9bdcad`) and are
**not** current guarantees—many gaps were closed in later slices (outcome typing,
handler registration, producer `ParamSpec`, strict payloads).

## Early static inventory (issue #3) — historical baseline

Pyright **1.1.414**, `basic`, Python **3.12** target, on early `api_design` showed
strong inference for fully specified `Job(...)` constructors and `Unknown` instead of
`None` for omitted event slots. Handler parameter checking did not catch all misuse
without explicit job/handler pairing; `JobHandle.outcome()` lost the result type in
narrowing. That motivated explicit consumer fixtures and `verify_contract_typing.py`.

Measured at the time (historical):

| Consumer expression | Pyright result (then) | Notes |
| --- | --- | --- |
| Full `Job(...)` | `Job[Request, Result, Event]` | Complete contract inferred |
| Missing event slot | `Job[Request, Result, Unknown]` | Not `None` |
| Decorated handler | `(...) -> Any` | Contract not tied to handler |
| Wrong handler annotations | No decorator error | Runtime-only enforcement then |

Primary sources: [Pyright type concepts](https://github.com/microsoft/pyright/blob/main/docs/type-concepts.md),
[Pyright configuration](https://github.com/microsoft/pyright/blob/main/docs/configuration.md),
[PEP 696](https://peps.python.org/pep-0696/).

## Constructor `ParamSpec` and keyword submission (issue #4 research)

Strong static keyword `submit` requires retaining the request constructor as
`Callable[P, Request]`—ordinary `Job[Request, Result, Event]` type parameters do not
carry `P`. [Typing generics / ParamSpec](https://typing.python.org/en/latest/spec/generics.html#paramspec),
[constructor-to-callable](https://typing.python.org/en/latest/spec/constructors.html#converting-a-constructor-to-callable).

**ParamSpec paired limitations:** `P.args` and `P.kwargs` must appear together;
you cannot generically turn positional-or-keyword constructor parameters into
keyword-only submit parameters without separate API choices.
[PEP 612 components](https://peps.python.org/pep-0612/#the-components-of-a-paramspec),
[PEP 612 semantics](https://peps.python.org/pep-0612/#semantics).

**Execution options vs constructor keywords:** Extra keyword-only execution options
cannot sit between `*args: P.args` and `**kwargs: P.kwargs` without collision risk.
Feasible designs include a configured client view (`with_options`) or a positional-only
`SubmitOptions` prefix overload—proved in stub prototypes, shipped as optional
positional-only `SubmitOptions` prefix plus explicit-object overload.

**Python 3.12 defaults/overloads:** Inline PEP-696 defaults on type parameters need
Python 3.13+; compatible solutions for 3.12 use traditional `TypeVar`/`Generic` or
overloads. [Pyright typed libraries / overloads](https://github.com/microsoft/pyright/blob/main/docs/typed-libraries.md#overloads)

**Dataclass / Pydantic:** `dataclass_transform` and Pydantic editor integrations
supply checker constructor views that may be narrower than runtime coercion.
[Pydantic VS Code](https://pydantic.dev/docs/validation/latest/integrations/dev-tools/visual_studio_code/)

## Subtype factory probe (issue #8) — historical stub evidence

A stub `RequestJob[..., P]` / `NoRequestJob` prototype (Pyright 1.1.414, 3.12 target)
showed ordinary DTO keyword submit, explicit objects, omitted slots as `None`, and
rejection of mixed object/keywords, erasure through `Job[Req, Res, Event]` parameters,
and invalid no-request shapes. Remaining gaps documented then included custom constructor
overload loss and widened `Job` parameters blocking specialized handler registration.

Production shipped the inferred `Job`/`RequestJob` model without requiring app
subclasses. Installed-wheel consumers and runtime strict construction are separate
proofs from the stub.

## Sync convenience research (not implemented)

`asyncio.Runner` reuses one loop identity across blocking calls but does **not**
keep I/O alive between those calls; a recurring sync client needs a continuously
running owned background loop (not one-shot `asyncio.run()` per call). Direction in
[ADR 0006](../adr/0006-sync-producer-convenience.md), not implemented.
[Python asyncio runners](https://docs.python.org/3/library/asyncio-runner.html)

## Fingerprint research pointer

Cross-process descriptor and fingerprint **design** (no measured hashing experiments
in this repo) is preserved in
[contract-fingerprint-detection.md](contract-fingerprint-detection.md). Runtime
submission fingerprints scope idempotency only today.

## Active probes

`examples/contract_interface/typing/measured_gaps/` documents remaining static limits
after the producer slice; see [api.md](../api.md) for normative examples.
