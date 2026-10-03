# Strict payload validation before construction

> **Historical status (2026-10-03):** Primary-source research for issue [#9](https://github.com/vschroeter/superjobs/issues/9)
> (2026-09-29). Shipped behavior and supported shapes are documented in
> [payload-validation.md](../payload-validation.md) and [ADR 0002](../adr/0002-strict-payload-validation.md).
> This file retains measured probe tables and upstream citations.

Research for [Validate payloads strictly before request construction](https://github.com/vschroeter/superjobs/issues/9), 2026-09-29. This report separates upstream facts, local measurements and implementation recommendations. It does not claim that the production implementation already satisfies the proposed checks.

The measured environment is Python 3.13.5, Pydantic 2.13.4 and pydantic-core 2.46.4. Probes ran with the repository's installed dependencies through public Pydantic imports; no application class configuration was changed. The current source of pydantic-core is inside the Pydantic monorepo. The older separate `pydantic/pydantic-core` repository has different constructor behavior and must not be used to infer behavior of this installed version.

## Facts that rule out naive implementations

- Strict Python validation and strict JSON validation deliberately differ. JSON strings can represent types that have no native JSON equivalent. Enabling strictness does not mean every representation is identical in the two modes. [Strict mode](https://docs.pydantic.dev/latest/concepts/strict_mode/)
- `TypeAdapter(type, config=...)` rejects config overrides for BaseModel, dataclass and TypedDict. Passing `ConfigDict(strict=True, extra='forbid')` to the adapter constructor is therefore not a general solution. Per-validation strict/extra flags exist, but are not an instance-revalidation switch. [TypeAdapter API](https://docs.pydantic.dev/latest/api/type_adapter/)
- Existing models and dataclasses default to no revalidation. Revalidation is a model/dataclass schema or configuration property, not a `validate_python` keyword. Each nested model owns its configuration; a strict parent config alone does not replace nested config. [Core schema API](https://docs.pydantic.dev/latest/api/pydantic_core_schema/), [pinned model validator source](https://github.com/pydantic/pydantic/blob/v2.13.4/pydantic-core/src/validators/model.rs)
- A custom model `__init__` is invoked by the model validator before ordinary model-field validation. Calling `super().__init__` starts its own validation and loses outer strict/extra/context parameters. Copying a schema and setting `custom_init=False` avoids that path, but also bypasses application constructor behavior. [Models documentation](https://docs.pydantic.dev/latest/concepts/models/), [pinned model construction source](https://github.com/pydantic/pydantic/blob/v2.13.4/pydantic-core/src/validators/model.rs)
- Before validators retain their role in strict validation: their output reaches the declared inner schema. Plain validators, and wrap validators that return without calling their handler, can bypass that inner schema altogether. Keeping arbitrary application validators is consequently different from promising that every final annotation is always enforced independently. [Pinned validator documentation](https://github.com/pydantic/pydantic/blob/v2.13.4/docs/concepts/validators.md)

## Measured failures and useful alternatives

| Probe on the measured environment | Result |
| --- | --- |
| `TypeAdapter(D).validate_python(D(x='bad'), strict=True)` for an ordinary dataclass with `x: int` | Accepted the invalid instance unchanged. |
| `M.model_validate(M.model_construct(x='bad'), strict=True, extra='forbid')` | Accepted the invalid model unchanged. |
| Strict outer model containing the same bypass-constructed nested model | Accepted the invalid nested field. |
| `TypeAdapter(D).validate_python({'x': 1}, strict=True)` | Rejected the dict because strict dataclass validation requires an instance. |
| Model with custom `__init__` forwarding to `super`, validated with `{'x': '1', 'unknown': 2}`, strict and forbid flags | Accepted `x=1` and discarded the unknown field. |
| Copied recursive core schema with `revalidate_instances='always'` and `custom_init=False`, ordinary `SchemaValidator(schema)` | Reused a prebuilt model validator and accepted invalid model instances. |
| Same copied schema, `SchemaValidator(schema, _use_prebuilt=False)` | Rejected invalid top-level and nested models. |
| Copied dataclass nodes using strict field config but `strict=False` on the dataclass envelope; no call-level strict override | Accepted valid raw/nested dataclass dicts and rejected string values for integer fields. |

The prebuilt bypass is particularly easy to miss. In the installed version, the validator obtains a completed class's existing validator instead of rebuilding from the copied node. Removing `ref` or `metadata`, or setting a config key named `use_prebuilt=False`, did not prevent reuse. The shipped constructor explicitly exposes `_use_prebuilt`, and its docstring describes false as the rebuild setting. It remains an underscored upstream seam that deserves containment and compatibility checks. [Pinned constructor stub](https://github.com/pydantic/pydantic/blob/v2.13.4/pydantic-core/python/pydantic_core/_pydantic_core.pyi), [pinned prebuilt lookup](https://github.com/pydantic/pydantic/blob/v2.13.4/pydantic-core/src/common/prebuilt.rs), [pinned constructor implementation](https://github.com/pydantic/pydantic/blob/v2.13.4/pydantic-core/src/validators/mod.rs)

Minimal reproduction for independent review:

```python
from pydantic import BaseModel, TypeAdapter
from pydantic_core import SchemaValidator

class M(BaseModel):
    x: int

schema = dict(TypeAdapter(M).core_schema)
schema.update(revalidate_instances='always', custom_init=False)
bad = M.model_construct(x='bad')

SchemaValidator(schema).validate_python(bad, strict=True)  # accepted
SchemaValidator(schema, _use_prebuilt=False).validate_python(
    bad, strict=True
)  # ValidationError
```

A standalone `model-fields` schema provides a class-independent way to check a raw field dictionary. Its measured strict check rejected the wrong scalar. However, nested unchanged model nodes still have the prebuilt/revalidation problem, and extracting only fields loses surrounding model validators and their invariants. This is a bounded tool, not a complete general replacement.

## Defaults, hooks and repeated conversions

Defaults are not validated by default. Pydantic default factories may consume already validated earlier fields, making field order and one-time evaluation significant. Retain the complete schema/default wrapper rather than reconstructing fields from bare annotations. A separate precheck followed by a regular constructor can invoke factories and validators twice unless the validated values, including materialized defaults, are carried into construction. [Pinned fields documentation](https://github.com/pydantic/pydantic/blob/v2.13.4/docs/concepts/fields.md)

Local measurements:

- A model `x: int = 'bad'` passed ordinary strict validation of `{}`. A copied boundary needs an explicit default-validation decision; strict flags alone do not validate omitted defaults.
- `Annotated[int, BeforeValidator(lambda value: int(value))]` accepted `'1'` under strict validation. This is an explicit contract conversion rather than general coercion.
- An after field validator `value + 1` produced `2` from raw `1`, then `3` on revalidation of that instance. Revalidating at every layer can change a request/result repeatedly.
- Ordinary dataclass `__post_init__` and model `model_post_init` each changed a successfully validated integer into `'bad'`; the initial Pydantic validation still returned success.
- A copied `custom_init=False` model schema produced `x=1` where the normal application constructor changed the same value to `101`. Silently disabling that hook changes semantics even when all fields are valid.

Dataclass core schemas distinguish constructor fields, `InitVar`, `init=False` fields, slots, frozen types and post-init hooks. Ordinary dataclasses adapted by Pydantic can execute their post-init hook after schema validation. Generic field-by-field reconstruction must not pass `init=False` values to the Python constructor or lose init-only inputs. [Pinned dataclass schema generation](https://github.com/pydantic/pydantic/blob/v2.13.4/pydantic/_internal/_generate_schema.py), [pinned dataclass validator](https://github.com/pydantic/pydantic/blob/v2.13.4/pydantic-core/src/validators/dataclass.rs)

Recommendation: avoid recursive validate/construct loops. Distinguish validating raw fields, invoking supported construction semantics and checking the current output object. A final structural check must catch hook corruption without executing the same hook again. Explicitly document supported custom-init behavior and whether converters must be idempotent at repeated process boundaries. Reject an unsupported path clearly rather than declaring its outputs trustworthy. This is an implementation recommendation, not an existing Pydantic guarantee.

## Aliases and wire validation

Validation aliases and serialization aliases are independent. Validation may accept an alias, choices or an alias path, while serialization may emit a different key. Canonical instance field names require a separate revalidation mode; raw keyword input should preserve declared accepted input aliases. [Pinned alias documentation](https://github.com/pydantic/pydantic/blob/v2.13.4/docs/concepts/alias.md)

For a measured model `x: int = Field(validation_alias='input', serialization_alias='output')`, neither `model_dump(mode='json')` yielding `{'x': 1}` nor `by_alias=True` yielding `{'output': 1}` could be fed into the original alias-only validation. A copied revalidation schema with explicit `by_name=True` accepts current canonical instance fields. Preserve raw-input alias matching separately; do not infer accepted keys only from `model_fields` names or constructor signature. Test alias/name duplicates and nested alias paths against unknown-field rejection.

The existing adapter protocol's `WireValue` contains JSON scalar/list/dict shapes. Both JSON and Msgpack codecs transport that logical representation. A strict Python validator receiving the decoded tree does not know those values came from the wire. Under the current schema, a model containing datetime, UUID, enum and bytes dumped strings for all four; strict Python validation rejected all four, while strict JSON validation reconstructed each successfully. Ordinary implicit integer/string coercion remained rejected in JSON strict mode. [Core validation API](https://docs.pydantic.dev/latest/api/pydantic_core/), [conversion table](https://docs.pydantic.dev/latest/concepts/conversion_table/)

Recommendation: keep Python-object validation and canonical wire interpretation as two operations behind one payload boundary. A bounded implementation can encode the canonical JSON-compatible tree and invoke strict JSON validation after either wire codec; measure the allocation overhead before adopting it as the permanent fast path. Do not use a blanket lax wire validator merely to support temporal/UUID types. Tests should cover malformed scalar values as well as valid round trips, including Msgpack, so JSON-specific restoration does not widen all field conversions.

Bytes require a declared representation. The measured default JSON dump accepted UTF-8 bytes but raised `UnicodeDecodeError` for `b'\xff'`. A model-configured base64/hex serialization policy must pair with its decoding policy. Converting arbitrary Msgpack-native binary values to JSON is not automatically part of the existing JSON-compatible protocol. [Configuration API](https://docs.pydantic.dev/latest/api/config/)

## Proposed implementation review checks

1. Wrong scalar/collection and unknown raw fields fail before a coercing constructor and before backend submission; valid defaults are materialized once.
2. Mutated/bypass-constructed top-level and nested objects fail through public codec/adapters; objects remain subject to current field values rather than prior construction history.
3. No application class config, validator or completion flag changes globally. Copied schemas preserve definitions, references, aliases, annotated constraints and declared validators.
4. If `_use_prebuilt=False` is used, contain it in one module and prove the supported dependency versions. A missing capability must fail clearly rather than falling back to trusting instances.
5. Custom initialization, post-init, validators with side effects, invalid defaults and invalid derived fields have explicit tested handling. Guard output checks against repeated hooks and unintended repeated transformations.
6. Requests, final results and application events share the boundary. Invalid worker results retain visible failure and avoid retry or successful coercion.
7. Canonical wire keys round-trip with different validation/serialization aliases, nested nullable fields, datetime, UUID, enum, tuples/sets and declared bytes encoding in both codecs; malicious wire string integers are rejected.
8. Custom adapter policy remains honest: field construction requires an explicit capability; absence cannot silently construct an unchecked payload. Do not claim arbitrary custom adapters satisfy built-in strict guarantees automatically.

These checks are proposed acceptance evidence for Composer and independent review. The research probes establish hazards and a viable bounded low-level route; they do not establish a complete schema transformer for every Pydantic extension.
