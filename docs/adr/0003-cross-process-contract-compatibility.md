# ADR 0003: Cross-process contract compatibility (selected, not implemented)

**Status:** Accepted direction, **not implemented**

**Issues:** [#29](https://github.com/vschroeter/superjobs/issues/29) descriptors/fingerprints, [#30](https://github.com/vschroeter/superjobs/issues/30) enforcement manifest, map [#28](https://github.com/vschroeter/superjobs/issues/28)

## Context

Installed-wheel typing proofs show that producers and workers **can** import the
same contract package, but nothing yet proves at runtime that both sides execute
compatible contract versions or payload codecs.

## Decision

Implement, in order:

1. **Versioned contract descriptors** with deterministic fingerprints ([#29](https://github.com/vschroeter/superjobs/issues/29)).
2. **Execution fingerprints and an immutable shared NATS manifest** that reject or
   surface incompatible participants ([#30](https://github.com/vschroeter/superjobs/issues/30)).

Until then, operators must align contract package versions by deployment discipline
only.

### Selected comparison semantics (from research)

- **Worker path:** Compare submitted contract fingerprint to the local descriptor
  **before request decoding** and before application code. Mismatch is a visible,
  **non-retryable `contract_mismatch`** for an already accepted execution.
- **Producer path:** When observing an execution whose stored fingerprint differs from
  the local descriptor, **refuse typed result and event decoding**; rely on diagnostics
  and untyped failure surfaces instead of silent coercion.
- **Durability:** Contract fingerprint and submission fingerprint travel with
  execution metadata so **client `get` / handle reconstruction** and **event cursors**
  remain tied to the accepted contract bytes, not only live connections.
- **Manifest:** Optional per-job immutable KV entries use **atomic compare/create**;
  conflicting registration or submission fails early. **KV availability errors stay
  visible**—no silent fallback. **No hash-specific worker routing** in the single-pool
  model (routing would hide split-brain same-version pools).
- **Normalization:** Equal fingerprints mean equal **normalized descriptor documents**
  and package-version diagnostics—not **Python semantic equivalence** (custom validators
  can share JSON schema). Package version is diagnostic; Job **version** is the
  compatibility axis.

## Rationale

Separate programs are the primary integration style; silent drift between producer
and worker contracts is a production incident class worth blocking at the platform
layer rather than rediscovering via corrupted payloads.

## Consequences

- Current submission uses an internal `submission_fingerprint` for idempotency scope;
  it is **not** a cross-process compatibility gate.
- Design research (source inspection, not runtime hashing experiments):
  [research/contract-fingerprint-detection.md](../research/contract-fingerprint-detection.md).
