# ADR 0001: Shared contract packages and handler interface

**Status:** Accepted (issue [#4](https://github.com/vschroeter/superjobs/issues/4), 2026-09-29)

**Implementation:** Shipped in bounded tickets [#6](https://github.com/vschroeter/superjobs/issues/6)–[#9](https://github.com/vschroeter/superjobs/issues/9), [#11](https://github.com/vschroeter/superjobs/issues/11)–[#12](https://github.com/vschroeter/superjobs/issues/12)

## Context

Producers and workers are separate programs exchanging typed jobs over NATS.
Coupling producers to worker modules breaks versioning and deployment independence.

## Decision

- Publish **contract packages** containing `Job` constants and payload types importable
  by producers without worker code.
- Workers register handlers against the same `Job` constants.
- Every handler receives **`JobContext`**; with a request, `(request, context)`.
- Exactly **one** active handler per job in a worker registry.
- Async `submit` / observe APIs are primary; producers use `jobs.client(job)` and
  `SubmitOptions` for execution identity and deadlines.
- Support explicit request objects and, for `RequestJob`, constructor keyword
  submission through the same validation path.

## Rationale

Shared contracts mirror telemetry/manifest integration patterns in the domain model
([GLOSSARY.md](../../GLOSSARY.md)) while keeping transport details inside SuperJobs.
Mandatory context carries logging, progress, events, and cancellation consistently.

## Consequences

- Positive and negative **installed-wheel** typing consumers are required evidence
  ([#11](https://github.com/vschroeter/superjobs/issues/11)).
- Remaining typing ergonomics are tracked separately ([#31](https://github.com/vschroeter/superjobs/issues/31)).
- Runnable reference: [examples/contract_interface/](../../examples/contract_interface/).
