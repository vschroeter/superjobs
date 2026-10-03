# ADR 0004: Test foundation and bounded recovery scope

**Status:** Accepted (issue [#5](https://github.com/vschroeter/superjobs/issues/5), [#15](https://github.com/vschroeter/superjobs/issues/15))

**Implementation:** Required PR gates shipped; see [development.md](../development.md)

## Context

Pre-alpha reliability claims must be backed by observable proofs across platforms,
especially separate OS processes and real JetStream behavior.

## Decision

Prove implemented guarantees before expanding the contract:

| Priority | Guarantee | Evidence style |
| --- | --- | --- |
| 1 | Integration infrastructure fails visibly when broken | Owned/pinned NATS harness |
| 2 | Public contracts survive wheel distribution | Source + wheel Pyright consumers |
| 3 | Ordinary cross-program job behavior | Installed producer/worker processes |
| 4 | Worker crash before/after durable completion | Installed kill/checkpoint harness |
| 4 | Broker persistent-store restart | Same store, fresh apps |
| 4 | Short idle outage with same processes | Reconnect within broker budget |

Provide `dev_check fast` and `dev_check full` orchestration; CI matrix on Linux and
Windows across stable CPython minors (fast: 3.12–3.14; integration: 3.12 and 3.14).

**Explicit non-goals in the foundation:** exactly-once execution, preserving all
intermediate events across worker loss, power-loss/fsync guarantees, uninterrupted
live iterators across outages, and PR performance benchmarks.

## Rationale

Coverage percentages and mocks do not substitute for transport semantics. Bounding
recovery proofs keeps the library shippable while documenting known gaps.

## Consequences

- Missing proofs remain visible and block merges; infrastructure skips are not
  acceptable for required stages.
- Test-only synchronization fixes ([#25](https://github.com/vschroeter/superjobs/issues/25))
  are summarized in [verification.md](../verification.md).
