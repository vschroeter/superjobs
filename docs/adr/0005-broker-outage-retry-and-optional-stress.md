# ADR 0005: Broker outage, retry windows, optional stress and performance

**Status:** Accepted scope decisions [#16](https://github.com/vschroeter/superjobs/issues/16)–[#19](https://github.com/vschroeter/superjobs/issues/19)

**Implementation:** #20 idle outage in required full checks; #21–#23 optional/manual

## Broker outages ([#16](https://github.com/vschroeter/superjobs/issues/16) / [#20](https://github.com/vschroeter/superjobs/issues/20))

- Reconnect behavior stays in **NatsBroker / FastStream / nats-py** configuration;
  SuperJobs adds no public connection-state API.
- Calls may raise or time out during outages; **no automatic operation resubmission**.
- Callers retry reads on the same handle; uncertain submits reuse `job_id`,
  `idempotency_key`, caller scope, payload, and execution-affecting options.
- Observation iterators may fail; reopen from the last delivered cursor under retention
  rules.
- Smallest proof: idle installed producer/worker survive a short outage and remain
  usable.

Deferred until triggered by a **reproduced defect**, a **production incident**, or a
**semantic contract change** (not merely hypothetical windows): active handlers crossing
outages, prolonged outages, and failover matrices.

## Additional crash windows ([#17](https://github.com/vschroeter/superjobs/issues/17) / [#21](https://github.com/vschroeter/superjobs/issues/21))

- Keep mandatory worker-kill and broker-restart proofs from ADR 0004.
- Add **one optional** proof for retry published before original ack ([#21](https://github.com/vschroeter/superjobs/issues/21)), manual/exploratory.
- Defer exhaustive boundary matrices until the same **incident / reproduced defect /
  semantic change** bar as above—not only because a window is theoretically possible.

## Optional repetition ([#18](https://github.com/vschroeter/superjobs/issues/18) / [#22](https://github.com/vschroeter/superjobs/issues/22))

- Weekly Linux workflow on default branch, three repetitions per existing family,
  fifteen-minute cap, fail-fast (no retry-to-green).
- Does not expand required PR matrix.

## Manual performance baselines ([#19](https://github.com/vschroeter/superjobs/issues/19) / [#23](https://github.com/vschroeter/superjobs/issues/23))

- Telemetry (~1 KiB) and manifest (~64 KiB) fixtures, bounded concurrency 1 and 8,
  producer-side timing, dated artifacts — not SLOs ([#33](https://github.com/vschroeter/superjobs/issues/33)).

## Rationale

Useful reconnect behavior without building an outage state machine on every API.
Optional stress/performance slices inform optimization later without blocking contract
safety work.

## Consequences

Documented in [design/reliability-repetition-verification.md](../design/reliability-repetition-verification.md),
[design/performance-baseline.md](../design/performance-baseline.md), and
[verification.md](../verification.md).
