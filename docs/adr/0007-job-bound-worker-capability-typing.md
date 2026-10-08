# Job-bound worker capability typing

**Status:** Accepted on 2026-10-06 in [Decide public capability typing and handler association](https://github.com/vschroeter/superjobs/issues/50).

The shared Job definition declares an optional application capability type so producers can infer discovery types automatically, while workers own the concrete values and their runtime updates. This replaces the proposed requirement for clients to select a separate capability descriptor on every query.

The declaration carries discovery typing alongside the execution contract: capability types, schemas, codecs and values stay outside the request/result/event fingerprint and immutable contract manifest. Capability compatibility depends on successful strict client decoding and validation, rather than equal schema/version identities, allowing interpretable capability changes to remain independent of execution-contract versions.

The canonical API and semantics are recorded in the issue's resolution. Generic representation and source/installed-wheel typing remain implementation proof obligations; this accepted decision does not claim a shipped API.
