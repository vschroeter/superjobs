# ADR 0002: Strict payload validation at boundaries

**Status:** Accepted (issue [#9](https://github.com/vschroeter/superjobs/issues/9))

**Implementation:** Shipped

**Follow-up:** [#32](https://github.com/vschroeter/superjobs/issues/32) guards validation against Pydantic dependency drift

## Context

Typed jobs are useless if invalid payloads cross process boundaries silently or
if handler outputs coerce into the wrong shapes.

## Decision

- Validate request, result, and event payloads at the SuperJobs boundary **strictly**
  by default.
- Reject unknown fields on keyword submission; validate before model construction
  when coercion would hide errors.
- Revalidate explicit objects supplied to `submit`.
- Invalid producer input fails **before** submission; invalid handler outputs become
  **failed executions**, not silent coercion.
- Contract-defined explicit conversions are allowed; arbitrary coercion is not.

## Rationale

Fail-fast producer behavior prevents poison messages. Surfacing handler output
errors as execution failures keeps observation and outcomes trustworthy without
requiring producers to trust worker-side discipline alone.

## Consequences

- Dependency upgrades (especially Pydantic) can change validation behavior; [#32](https://github.com/vschroeter/superjobs/issues/32)
  will add explicit guards/tests when prioritized.
- Shipped behavior, adapter policy, and capability checks:
  [payload-validation.md](../payload-validation.md).
- Primary-source Pydantic probes:
  [research/strict-payload-validation.md](../research/strict-payload-validation.md).
