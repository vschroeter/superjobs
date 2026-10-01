# Test strategy for separate programs and NATS

Confirmed by the maintainer on **2026-10-01** in [Decide the test strategy for separate programs and NATS](https://github.com/vschroeter/superjobs/issues/5). This records selected requirements and implementation order, not completed automated proofs.

## Scope and current evidence

Prove currently implemented public guarantees before expanding the reliability contract. New guarantees and exposed implementation defects receive separate decision/fix tickets. Coverage percentages do not substitute for observable behavior.

The local audit found seven real-NATS tests, six of which can skip arbitrary startup failures; inconsistent resource isolation; a runtime-stop restart test within one interpreter; and no committed CI workflow or installed-wheel/two-process verification runner. Public typing fixtures and a separate producer/worker example exist. The latest recorded independent checks include 234 deterministic tests, 27 Python 3.12 installed-wheel tests, and matching source/wheel typing diagnostics; those manual runs do not establish automated crash or broker-restart coverage. See [handler registration verification](handler-registration-verification.md) and [producer interface verification](producer-interface-verification.md).

## Prioritized guarantee matrix

| Order | Guarantee | Fast deterministic evidence | Required real/installed evidence |
| --- | --- | --- | --- |
| 1 | Integration infrastructure is usable and failures stay visible | Harness failure/cleanup controls | Owned JetStream broker readiness; broken/missing environment fails; all current NATS cases run |
| 2 | Public contracts retain typing when distributed | Public positive and marked negative source consumers | Built, non-editable library and contract wheels; isolated imports and py.typed; matching source/wheel diagnostics |
| 3 | Ordinary request, result, outcome and observation behavior | Broad runtime/contract tests | Existing same-process NATS transport tests plus installed independent producer/worker processes |
| 3 | Four existing request/result/event shapes, ordered application events on normal completion, success/failure/cancellation and retries cross program boundaries | Fast API/serialization tests | Real separate OS processes using only installed distributions; producer cannot import worker implementations |
| 4 | A worker killed during an active handler can be replaced | Attempt/retry state tests | Confirm handler entry, hard-kill, start replacement and recover the same execution identity/outcome; another attempt is allowed |
| 4 | Durable completion prevents completed-handler re-execution | Completion-before-ack and completed-redelivery tests | Kill after the real completion write succeeds but before returning to terminal publication/ack; reconstruct public handle, recover result/outcome and terminal closure without invoking the handler again |
| 4 | Broker state survives a persistent-store restart | State reconciliation tests | Completed and published-pending executions survive an actual broker restart with the same store; fresh installed applications recover results and process retained work without fixture reinsertion/resubmission |

All implementation rows ultimately become required PR evidence. A missing or failing proof remains visible and gets a linked fix ticket; it must not be converted into a weaker assertion, mock-only transport proof or infrastructure skip. At-least-once execution allows duplicate attempts/external effects before durable completion. No exactly-once execution, worker-loss preservation of intermediate events, power-loss/fsync or uninterrupted application reconnection promise is added here.

## Local checks and CI

Provide two documented, exact cross-platform invocations in the orchestration slice:

- **Fast check:** routine deterministic checks without a broker requirement.
- **Full check:** typing, built/installed distributions, real NATS/process scenarios and selected recovery scenarios, including owned-broker provisioning.

The names `check-fast` and `check-all` used during the discussion were illustrative, not existing executable commands. The implementation ticket must publish runnable commands; explicitly selecting integration always makes its environment required.

Every PR runs required gates on **Linux and Windows**. Fast tests cover every supported stable CPython minor from **3.12** onward. Heavier integration covers minimum and latest stable Python on both platforms. Exclude prereleases from required gates. Maintain an explicit reviewed matrix, verifying stable releases when implementing/updating it rather than assuming future versions are supported by prior runs.

Pin **Pyright 1.1.414** initially, basic mode and the Python 3.12 static target. Positive fixtures have zero errors/warnings. Mark negative misuse sites with expected diagnostic rules; require every expected error, reject unexpected diagnostics/import failures, and compare source/wheel rules/sites. Do not match diagnostic prose, brittle absolute paths, or accept any nonzero checker exit as evidence. No consumer casts, Any widening or suppression may conceal incompatibilities. Checker upgrades are explicit reviewed changes.

Installed-wheel consumers use fresh non-editable environments with no library/contract repository source paths. Verify site-packages origins and py.typed; producer-only checks have no worker implementation available.

## Broker ownership and isolation

Use one explicitly supported, reviewed and pinned native NATS version on both platforms. Download official platform binaries on first full use, verify pinned checksums before execution, and cache outside tracked source. Reuse valid cached binaries. Support an explicit executable path for offline use with a version check. Provisioning, checksum, version and startup failures fail the full check.

The harness owns JetStream configuration, process, dynamically allocated port and a fresh store per run. Handle port allocation races without assuming a free-port probe reserves a port. Check actual server readiness. Ordinary tests use unique Job/resource names. Crash/restart scenarios own dedicated brokers. Only a broker-restart scenario deliberately reuses its own persistent store; verify the actual stream/KV storage configuration.

An externally supplied server is an explicit option for compatible ordinary tests. Restart tests require harness-owned server/storage. Docker is not a mandatory prerequisite.

## Synchronization, faults and deadlines

Use observable readiness and explicit checkpoints, not sleeps as evidence of success. Prevent stale readiness markers.

A worker-only adapter uses existing injected backend seams. For the completion checkpoint, await the real durable completion write, signal success, and block before returning; then the parent kills the OS process. For the active-handler checkpoint, signal handler entry before killing it. Keep broker operations real and recovery assertions on public handles. Persist invocation evidence independently of the killed worker; process-local counters are insufficient. Production fault-injection flags are not selected.

Initial safety caps:

- Integration CI job: **15 minutes**.
- Recovery scenario: **90 seconds**.
- Process startup or shutdown: **30 seconds**.

These are bounds, not measured performance expectations. Every child exit, wait and cleanup is bounded. Terminate owned processes and remove owned resources/stores on success and failure; retain diagnostic evidence before cleanup.

A failed required attempt keeps its CI run red. Automatic successful retries do not erase the failure. Preserve broker logs, child stdout/stderr, exit codes, checkpoints/timings, environment/import origins and checker diagnostics. Investigate flaky tests and product races. A diagnosed infrastructure outage can justify a fresh reviewed run.

## Implementation tickets and native dependencies

1. [Build an isolated NATS test harness with fail-required integration checks](https://github.com/vschroeter/superjobs/issues/10) — see [nats-test-harness.md](nats-test-harness.md).
2. [Automate source and installed-wheel consumer typing verification](https://github.com/vschroeter/superjobs/issues/11) can proceed independently of the broker foundation.
3. [Verify installed producer and worker contracts in separate NATS processes](https://github.com/vschroeter/superjobs/issues/12) depends on both foundation and wheel verification.
4. [Prove worker crash recovery before and after durable completion](https://github.com/vschroeter/superjobs/issues/13) and [Prove execution and result persistence across a NATS restart](https://github.com/vschroeter/superjobs/issues/14) depend on the process harness and can proceed independently of one another.
5. [Add fast and full developer checks with required Linux and Windows CI gates](https://github.com/vschroeter/superjobs/issues/15) integrates all proofs. Add gates incrementally as prerequisites land; closure requires the complete selected matrix.

These are native sub-issues of the resolved strategy decision, which remains under [Wayfinder: Typed contracts and a reliable test foundation](https://github.com/vschroeter/superjobs/issues/1). Cursor Composer 2.5 implements bounded slices; Codex independently reviews/verifies. No implementation is started by recording this strategy. Repository branch-protection changes need separate authorization; documenting required gates does not claim protection is already configured.

## Resolved follow-up decisions

- [Decide recovery guarantees for applications that stay running during a NATS outage](https://github.com/vschroeter/superjobs/issues/16): one short idle-outage reconnect proof using the same application processes and existing broker configuration; in-flight operations may fail. Implementation: [Prove idle producer and worker recovery after a short NATS outage](https://github.com/vschroeter/superjobs/issues/20).
- [Prioritize additional submission, retry and observation crash windows](https://github.com/vschroeter/superjobs/issues/17): one optional retry-publication-before-old-ack proof; defer exhaustive boundary matrices. Implementation: [Prove retry recovery after publication before original acknowledgement](https://github.com/vschroeter/superjobs/issues/21).
- [Decide scheduled repetition and stress coverage for NATS reliability](https://github.com/vschroeter/superjobs/issues/18): optional weekly Linux repetition of existing scenarios, three repetitions each, fifteen-minute cap, no retry-to-green. Implementation: [Add bounded optional repetition of NATS reliability scenarios](https://github.com/vschroeter/superjobs/issues/22). No workflow or automation has been created yet.
- [Define workload-based performance measurements after the reliable test foundation](https://github.com/vschroeter/superjobs/issues/19): two manual telemetry/manifest workloads with simple throughput and producer-side timing; no invented SLOs, detailed instrumentation or PR benchmark gate. Implementation: [Add manual telemetry and manifest performance baselines](https://github.com/vschroeter/superjobs/issues/23).

These directions were selected autonomously on 2026-10-01 under explicit maintainer authorization to choose reasonable recommendations and avoid overengineering. The [detailed decision record](reliability-performance-followups.md) distinguishes requirements from measured evidence. All four follow-up tickets depend on the initial CI foundation and do not expand its closure criteria; no additional recovery/performance guarantee is claimed as implemented.


## Implemented foundation slices — 2026-10-01

The initial audit above is historical. [The isolated NATS harness](nats-test-harness.md)
now provisions reviewed NATS 2.15.0, fails required integration infrastructure,
isolates resources and verifies cleanup/restart behavior (#10 and its narrow
empty-completion-bucket fix #24). [The automated consumer gate](consumer-typing-verification.md)
now checks exact source/wheel diagnostics and isolated installed runtime behavior
on Python 3.12 and 3.14 (#11). Both slices were independently verified on Windows
and Linux. Cross-process/recovery/required-CI work in #12–#15 remains open.
