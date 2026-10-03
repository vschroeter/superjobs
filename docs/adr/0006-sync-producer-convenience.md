# ADR 0006: Sync producer convenience direction

**Status:** Direction accepted; **names and lifecycle API not selected**

**Issue:** [#36](https://github.com/vschroeter/superjobs/issues/36)

## Context

Many producer programs are synchronous scripts or CLIs. Async `submit`/`result` is
correct but awkward without a long-lived event loop.

## Decision

- **Async remains primary.**
- Add blocking helpers that mirror async submit/result on a client.
- **Recurring** synchronous use must keep transport I/O alive on an **owned background
  event loop** (continuous runtime lifecycle) between blocking calls.
- A **one-shot** blocking entry may own a temporary loop for a single operation.
- Existing synchronous handler callbacks stay supported.
- **Final public names** for sync helpers and the continuous lifecycle API are **not**
  decided in this record.

## Rationale

Convenience should not reintroduce per-call broker connect/disconnect overhead or
hide lifecycle requirements that NATS consumers already need on the worker side.

## Consequences

- No sync API is implemented yet; documentation and examples remain async-first.
- Packaging cleanup may remove scaffold APIs separately ([#35](https://github.com/vschroeter/superjobs/issues/35)).
