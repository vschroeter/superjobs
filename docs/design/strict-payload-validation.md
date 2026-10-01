# Strict payload validation

Implemented locally for [Validate payloads strictly before request construction](https://github.com/vschroeter/superjobs/issues/9). The preceding handler/interface iteration is committed as `ef6369f`. Keyword `JobClient.submit(**fields)` and optional `SubmitOptions` remain [Preserve request constructor typing and add optional SubmitOptions](https://github.com/vschroeter/superjobs/issues/8).

## Public behavior

Built-in Pydantic, dataclass and plain-Python adapters share `superjobs.payload.strict`. Requests are checked before backend submission, results before successful completion, and application events before entering the observation batch. Event preparation preserves the validated object, including declared conversions, for both the in-memory and NATS paths. Nested mutated dataclasses and models created with `model_construct` are revalidated. Unknown object fields are forbidden, required fields stay required, defaults are validated, and nullable fields remain supported.

```python
from superjobs import construct_payload_for_type
from superjobs_contract_example import ManifestRequest

request = construct_payload_for_type(ManifestRequest, device_id="sensor-17")
handle = await jobs.client(generate_manifest).submit(request)
```

Raw fields are validated before Pydantic-managed object creation. This path deliberately bypasses application `__init__` methods, including coercing custom dataclass/model constructors. Declared Pydantic before/after/wrap validators and post-init hooks run during validation; defaults/factories and hooks are not repeated by the final structural check. Use declared validators for explicit conversions. Inputs already coerced by application code before SuperJobs sees them cannot be recovered.

| Operation | Input | Policy |
| --- | --- | --- |
| `construct_payload(adapter, fields)` | Raw field mapping | Strict fields; declared input aliases and defaults; typed result `T` |
| `construct_payload_for_type(type_, **fields)` | Registry-backed raw fields | Same construction path; typed result `T` |
| `validate_payload(adapter, value)` | Python object | Strict current values, model/dataclass instance required, nested revalidation |
| Built-in adapter `dump` / codec `encode` | Python object | Validate once, check output without application hooks, serialize |
| Codec `prepare` / `context.emit` | Python object | Retain converted object; prove adapter serialization and wire encoding before buffering |
| Built-in adapter `load` / codec `decode` | Canonical logical wire tree | Strict JSON interpretation with canonical field names |

Raw keyword names and values currently have runtime validation, not constructor-signature static checking: their annotations are `Any`. The positive static consumers prove returned payload types and adapter/value compatibility. Constructor forwarding and its stronger typing belong to #8.

## Wire policy and implementation boundary

Both JSON and Msgpack carry the existing JSON-compatible logical tree. SuperJobs serializes canonical Python field names with `by_alias=False`, regardless of model `serialize_by_alias` configuration. Wire decoding accepts those names with `by_name=True, by_alias=False`; raw construction separately honors validation aliases. Different input/output aliases therefore round-trip without changing application class configuration.

Strict JSON interpretation restores declared datetime, UUID, enum and bytes representations while rejecting implicit string-to-integer and boolean-to-integer conversions. Arbitrary binary data needs a matching declared encoding/decoding policy, such as Pydantic base64 settings. Non-finite float wire values fail before encoding. Serialization aliases are not wire field names.

The boundary clones schema containers while preserving opaque defaults, classes and callables by identity. It builds local core validators with strict scalar validation, forbidden extra fields and instance revalidation. Dataclass raw mapping envelopes use their own non-strict construction node; child fields remain strict. No application `model_config` or cached validator is changed globally.

Pydantic core's `_use_prebuilt=False` is contained behind a capability check: without rebuilding, nested cached validators can silently ignore the stricter schema. An unavailable capability fails clearly. The declared dependency minimum is Pydantic 2.13.4. Measured combinations are 2.13.4/core 2.46.4 and 2.13.5/core 2.46.5.

A second, cached structural validator removes application before/after/wrap callbacks and post-init hooks, checks the validated output against the underlying schema and constraints, and checks derived dataclass fields. Model after-validation invariants still run in the main validator. This catches hooks that corrupt field types or constraints without applying transformations twice in one boundary operation. Conversions may run again at later producer/worker boundaries and should be idempotent. This does not promise arbitrary Python callback equivalence or absence of user side effects.

The wire path currently renders the logical tree as JSON for strict JSON validation even after Msgpack decoding. Its additional allocation cost has not been benchmarked; there is no latency or throughput claim.

## Custom adapters and support limits

Custom adapters retain responsibility for rejecting invalid Python values in `dump` and invalid wire values in `load`. Existing adapters implementing only `dump`, `load`, and `schema` remain usable through codecs. Event preparation uses their `dump` and wire encoder, retaining the original object unless they implement `prepare_instance(value) -> (validated_object, wire_value)` explicitly. The explicit `validate_payload` helper requires `validate_instance`; its absence raises `TypeError`. Raw construction requires `construct_fields`; absence also fails clearly.

Plain validators with no underlying schema cannot receive the built-in structural guarantee and fail adapter setup with an explicit message. Dataclass `InitVar` fields and `init=False` fields lacking schema-backed defaults also require a custom adapter and fail setup clearly. Derived fields with schema-backed defaults/factories are checked and round-trip without rerunning their factories. Custom constructors are bypassed rather than treated as trusted conversion functions. General custom Pydantic extensions and every possible dataclass feature are not claimed as verified.

Invalid built-in inputs normally raise Pydantic `ValidationError`; non-Pydantic failures at codec/helper boundaries are wrapped in `PayloadValidationError`. Invalid final results retain permanent `invalid_result` behavior, including a retry policy allowing three attempts. Invalid application events are rejected before batching using the existing handler exception/retry policy; they are not mislabeled as invalid final results.

No descriptor, contract fingerprint, manifest, NATS subject protocol, sync lifecycle or `JobClient` field interpretation changed.

## Independent verification on 2026-09-29

Cursor Composer 2.5 supplied adapter integration, helpers and the first regression suite. Codex independently found a construction failure in the first attempt and eight test failures in the corrected attempt. After those two unsuccessful attempts, Codex used the fallback authorized in `AGENTS.md` to fix the core-schema policy, schema copying, dataclass construction, canonical alias serialization and output checking. Independent primary-source research is recorded in [strict payload validation research](../research/strict-payload-validation.md).

| Check | Measured result |
| --- | --- |
| Full deterministic suite, source environment | 215 passed; the combined deterministic/NATS run passed all 222 tests |
| Full live NATS suite | 7 passed; no skips |
| Focused required NATS test with unreachable broker | 1 failure with startup timeout after about 10 seconds; no skip |
| Changed payload/registry/context implementation, Pyright 1.1.414 | 7 files; 0 errors or warnings |
| Existing positive/negative public consumers, source and installed wheels | 0 positive errors; exactly 23 intended negative errors in each layout |
| New strict-payload positive/negative consumers, source and installed wheels | 0 positive errors; exactly 1 `reportArgumentType` error at line 12 in each layout |
| Copied strict regressions through installed wheels | 57 passed, including real NATS |
| Installed library and independent contract wheel in separate processes | Four request/result/event shapes completed through NATS; producer has no worker implementation module |

Runtime verification used Python 3.13.5. Static consumers target Python 3.12; this is not a runtime claim for Python 3.12. The source environment used Pydantic 2.13.4/core 2.46.4; isolated wheels resolved 2.13.5/core 2.46.5. Installed imports and `py.typed` files were checked inside the wheel environment, with no source extra paths for static consumers and no worker implementation available to the producer. These checks do not establish crash recovery, exhaustive transport semantics, or fix the older startup-skipping policy tracked in #5.

Reproducible source checks:

```powershell
uv run python -m pytest -m "not nats" -q -p no:cacheprovider
uv run python -m pytest -m nats -q -p no:cacheprovider
uv tool run --from pyright==1.1.414 pyright --project examples/contract_interface/typing/strict_payload_positive
uv tool run --from pyright==1.1.414 pyright --project examples/contract_interface/typing/strict_payload_negative --outputjson
```

The negative checker invocation intentionally exits 1. New runtime evidence lives in `tests/test_strict_payload_validation.py`, `tests/test_strict_payload_boundaries.py` and `tests/test_nats_strict_payload.py`. Existing packaged example instructions are in [the contract interface README](../../examples/contract_interface/README.md).
