# Payload validation (current behavior)

Shipped for issue [#9](https://github.com/vschroeter/superjobs/issues/9). Decision
record: [ADR 0002](adr/0002-strict-payload-validation.md). Primary-source hazards
and probe tables: [research/strict-payload-validation.md](research/strict-payload-validation.md).

## Public behavior

Built-in Pydantic, dataclass, and plain-Python adapters share `superjobs.payload.strict`.
Requests are checked before backend submission, results before successful completion,
and application events before entering the observation batch. Event preparation
preserves the validated object, including declared conversions, for both in-memory
and NATS paths. Nested mutated dataclasses and models created with `model_construct`
are revalidated. Unknown object fields are forbidden, required fields stay required,
defaults are validated, and nullable fields remain supported.

```python
from superjobs import construct_payload_for_type
from superjobs_contract_example import ManifestRequest

request = construct_payload_for_type(ManifestRequest, device_id="sensor-17")
handle = await jobs.client(MANIFEST_JOB).submit(request)
```

Raw fields are validated before Pydantic-managed object creation. This path
deliberately bypasses application `__init__` methods, including coercing custom
dataclass/model constructors. Declared Pydantic before/after/wrap validators and
post-init hooks run during validation; defaults/factories and hooks are not repeated
by the final structural check. Use declared validators for explicit conversions.
Inputs already coerced by application code before SuperJobs sees them cannot be
recovered.

| Operation | Input | Policy |
| --- | --- | --- |
| `construct_payload(adapter, fields)` | Raw field mapping | Strict fields; declared input aliases and defaults; typed result `T` |
| `construct_payload_for_type(type_, **fields)` | Registry-backed raw fields | Same construction path; typed result `T` |
| `validate_payload(adapter, value)` | Python object | Strict current values, model/dataclass instance required, nested revalidation |
| Built-in adapter `dump` / codec `encode` | Python object | Validate once, check output without application hooks, serialize |
| Codec `prepare` / `context.emit` | Python object | Retain converted object; prove adapter serialization and wire encoding before buffering |
| Built-in adapter `load` / codec `decode` | Canonical logical wire tree | Strict JSON interpretation with canonical field names |

Raw keyword names and values have runtime validation, not constructor-signature static
checking (their annotations are `Any`). Constructor forwarding and stronger static
checking live in the producer typing slice ([#8](https://github.com/vschroeter/superjobs/issues/8)).

## Wire policy

Both JSON and Msgpack carry the existing JSON-compatible logical tree. SuperJobs
serializes canonical Python field names with `by_alias=False`, regardless of model
`serialize_by_alias` configuration. Wire decoding accepts those names with
`by_name=True, by_alias=False`; raw construction separately honors validation aliases.

Strict JSON interpretation restores declared datetime, UUID, enum, and bytes
representations while rejecting implicit string-to-integer and boolean-to-integer
conversions. Arbitrary binary data needs a matching declared encoding/decoding policy.
Non-finite float wire values fail before encoding. Serialization aliases are not wire
field names.

The boundary clones schema containers while preserving opaque defaults, classes, and
callables by identity. It builds local core validators with strict scalar validation,
forbidden extra fields, and instance revalidation. Dataclass raw mapping envelopes
use their own non-strict construction node; child fields remain strict. No application
`model_config` or cached validator is changed globally.

## Private capability check and JSON validation cost

Pydantic core's `_use_prebuilt=False` is contained behind a **private capability
check**: without rebuilding, nested cached validators can silently ignore the stricter
schema. An unavailable capability fails clearly rather than falling back to trusting
instances. Declared dependency minimum: Pydantic 2.13.4.

A second, cached structural validator removes application before/after/wrap callbacks
and post-init hooks, checks the validated output against the underlying schema and
constraints, and checks derived dataclass fields. Model after-validation invariants
still run in the main validator. This catches hooks that corrupt field types or
constraints without applying transformations twice in one boundary operation.
Conversions may run again at later producer/worker boundaries and should be idempotent.

The wire path currently renders the logical tree as JSON for strict JSON validation
even after Msgpack decoding. **Additional allocation cost has not been benchmarked**;
there is no latency or throughput claim.

## Custom adapters and unsupported shapes

Custom adapters must reject invalid Python values in `dump` and invalid wire values
in `load`. Adapters implementing only `dump`, `load`, and `schema` remain usable
through codecs. Event preparation uses their `dump` and wire encoder, retaining the
original object unless they implement
`prepare_instance(value) -> (validated_object, wire_value)` explicitly.

The explicit `validate_payload` helper requires `validate_instance`; absence raises
`TypeError`. Raw construction requires `construct_fields`; absence also fails clearly.

Plain validators with no underlying schema cannot receive the built-in structural
guarantee and fail adapter setup with an explicit message. Dataclass `InitVar` fields
and `init=False` fields lacking schema-backed defaults require a custom adapter and
fail setup clearly. Derived fields with schema-backed defaults/factories are checked
and round-trip without rerunning their factories. Custom constructors are bypassed
rather than treated as trusted conversion functions. General custom Pydantic extensions
and every possible dataclass feature are not claimed as verified.

Invalid built-in inputs normally raise Pydantic `ValidationError`; non-Pydantic
failures at codec/helper boundaries are wrapped in `PayloadValidationError`. Invalid
final results retain permanent `invalid_result` behavior. Invalid application events
are rejected before batching using the existing handler exception/retry policy.

Dependency-sensitive regression guards: [#32](https://github.com/vschroeter/superjobs/issues/32).

Runnable typing and runtime proofs:
[examples/contract_interface/README.md](../examples/contract_interface/README.md),
`tests/test_strict_payload_validation.py`, `tests/test_strict_payload_boundaries.py`,
`tests/test_nats_strict_payload.py`.
