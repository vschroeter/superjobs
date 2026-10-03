# Contract divergence detection across independent processes

> **Historical status (2026-10-03):** Research for decision #4 (2026-09-29).
> Proposal sections describe a **selected direction**, not implemented behavior.
> Normative current state: [ADR 0003](../adr/0003-cross-process-contract-compatibility.md),
> [api.md](../api.md). This file retains primary-source reasoning and links; it
> does **not** claim measured fingerprint hashing experiments were run in this
> repository.

Research for decision #4, 2026-09-29. Everything under "Proposal" is a design proposal, not implemented behavior.

## Current source guarantees

`PayloadAdapter.schema()` is optional, and `PayloadCodec.schema()` forwards it. The built-in Pydantic and dataclass adapters expose Pydantic JSON schemas, currently using the default validation mode. A custom adapter may return no schema. See [adapter protocol](../../src/superjobs/payload/adapter/protocol.py), [Pydantic adapter](../../src/superjobs/payload/adapter/implementations/pydantic.py), and [dataclass adapter](../../src/superjobs/payload/adapter/implementations/dataclass.py).

The existing `submission_fingerprint` hashes Job identity, serialized request bytes, timeout, and deadline. It detects conflicting idempotent submissions; it does not describe request/result/event schemas. `ExecutionRecord` stores this submission fingerprint but no separate contract fingerprint. Worker request decoding uses its local Job definition before invoking the callback. NATS backend registration checks identity, not schema equality. These are source-inspection findings, not new runtime measurements: [client](../../src/superjobs/jobs/job_client.py), [execution record](../../src/superjobs/transport/backend.py), [handler](../../src/superjobs/jobs/job_handler.py), [backend](../../src/superjobs/transport/nats_backend.py).

## Proposal: local descriptors, exchanged fingerprints

Each environment builds the same versioned descriptor format from its local definition:

- Job name and immutable Job version.
- Explicit presence/absence of request, result, and intermediate event payloads.
- Wire format/codec contract identifier and the wire-facing schemas for each payload.
- Explicit validation policy, including strictness and unknown-field behavior.
- Descriptor format and normalization version.

Serialize the normalized descriptor deterministically and hash it, for example with SHA-256. Compute once when the Job is prepared, not for every execution. Send this **contract fingerprint** with each submission and retain it on the execution and completion record. Keep it distinct from the existing submission fingerprint; also bind the submission fingerprint to the contract fingerprint.

The worker compares the submitted fingerprint with its own before request decoding and before running application code. Inequality becomes a visible non-retryable contract-mismatch failure for an already accepted execution. Include Job identity, both fingerprints, and optional package versions in diagnostics. A producer observing an execution with a different local fingerprint must refuse typed result/event decoding. Both sides continue normal payload validation: equality is not a substitute for validation.

This requires no shared Python environment or discovery handshake. The bytes carried over NATS contain the comparison value. NATS supports application headers, although durable envelope/execution metadata should be authoritative for retries and handle reconstruction. [NATS client protocol](https://docs.nats.io/reference/protocols/client#hpub)

## Limits and normalization decisions

An unequal fingerprint establishes different descriptors; it neither chooses which environment is correct nor calculates backward compatibility. Package versions are useful diagnostics but are not Job versions: a package can change unrelated Jobs without changing this Job, or accidentally change this Job without updating its declared version.

Pydantic supports distinct validation and serialization schemas. The descriptor must cover the actual adapter dump/load boundary, potentially both modes, rather than assuming the default validation schema describes emitted bytes. Model titles, descriptions, `$defs` names, references, and generator changes may produce different schema documents for equivalent wire types. Normalization policy needs its own version; a frozen manifest shipped in the contract package is an alternative to regenerating independently under different framework versions. [Pydantic JSON Schema](https://docs.pydantic.dev/latest/concepts/json_schema/)

Canonical JSON solves representation ordering, not logical schema equivalence. RFC 8785 sorts object properties recursively but preserves array order. Therefore blindly sorting every array or removing every `title` key is unsafe: a request field named `title` is data, and ordered serializer/union behavior can matter. [RFC 8785](https://www.rfc-editor.org/rfc/rfc8785#section-3.2.3)

Equal JSON schemas do **not** prove equal Python semantics. Custom validators can enforce conditions or mutate values beyond field annotations, while factories execute arbitrary callables. Their changes can leave JSON schema unchanged. Use declarative constraints where possible and require a new Job version for semantic changes; an explicit semantic revision can supplement custom adapters. Automatic source-code hashing is not a reliable semantic proof. [Pydantic validators](https://docs.pydantic.dev/latest/concepts/validators/), [default factories](https://docs.pydantic.dev/latest/concepts/fields/#default-values)

## Mixed worker pools and optional shared manifest

Per-delivery checks prevent incompatible handlers from executing, but success in a mixed pool can depend on which worker receives the message. Requeueing until a matching worker happens to pull it risks indefinite churn. Hash-specific routing would hide conflicting same-version contracts in separate pools and conflict with the current single-pool domain model.

An optional immutable descriptor entry per Job identity in NATS KV could reject conflicting worker registration and producer submission early. Atomic `create` handles competing initial registrations; later participants compare rather than overwrite. First-writer selection establishes agreement, not correctness. A deployment-controlled manifest provides stronger authority but adds lifecycle and provisioning responsibilities. KV availability failures must remain visible. [NATS KV compare-and-swap](https://docs.nats.io/learn/key-value/history-and-revisions#compare-and-swap-writing-without-locks)

Open decisions: per-message detection versus mandatory shared manifest, normalization rules, schema-less adapter policy, and rollout handling when a fingerprint format changes.
