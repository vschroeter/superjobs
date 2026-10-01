# Practical reliability and performance follow-ups

Resolved on **2026-10-01** after parallel autonomous grilling sessions for decisions 16–19. The maintainer explicitly authorized recommended choices without another confirmation and requested a good, performant library with a pleasant API rather than exhaustive edge-case coverage. Codex reviewed three agents' proposals and trimmed the benchmark scope.

These are selected directions, not implemented features or measured recovery/performance results. The [test foundation](test-strategy.md) remains the first implementation priority. All four new tickets depend on its CI integration slice; none blocks that slice or changes its original closure criteria.

## Short broker outages — decision 16

[Decide recovery guarantees for applications that stay running during a NATS outage](https://github.com/vschroeter/superjobs/issues/16).

| Internal grilling question | Selected answer |
| --- | --- |
| What ordinary behavior is useful now? | An already-started producer and idle worker remain usable after a short broker outage within the configured broker reconnect budget, with the same endpoint/store and the same application processes. |
| Who owns reconnect configuration? | Existing NatsBroker/FastStream/nats-py configuration. Add no SuperJobs reconnect controls or public connection-state API. |
| Must calls transparently survive the outage? | No. Calls may raise transport errors or time out. SuperJobs does not automatically resubmit application operations. A transport failure does not prove a submission was never accepted. |
| How does the caller recover? | Retry reads on the same handle. For uncertain submissions, retain/reuse both job_id and idempotency_key, caller scope, request and execution-affecting options. Do not retry by generating a new identity. |
| What about observations and exhausted reconnect attempts? | An iterator may fail; reopen from its last delivered cursor under existing retention rules. After the broker reconnect budget is exhausted, rebuild the connection/runtime. |
| What is the smallest proof? | One installed two-process idle-outage case: old handle works, a new Job executes, and a failed read can be retried after reconnect. |

Rationale: ordinary reconnect is useful without introducing an outage state machine across every operation. Official [NATS reconnection documentation](https://docs.nats.io/learn/resilient-clients/reconnection) describes connection/subscription restoration and a configured reconnect budget; that is dependency evidence, not proof that a SuperJobs consumer remains healthy. Local inspection found that `JobHandler._consume` can log a subscriber error and exit, and submission writes the execution record before its idempotency mapping. Those facts justify the focused proof and stable-identifier guidance. The currently unused `NatsQueueConfig.reconnect_delay` must not be documented as controlling reconnect.

Implementation: [Prove idle producer and worker recovery after a short NATS outage](https://github.com/vschroeter/superjobs/issues/20). Once implemented, the single case joins normal full verification on Linux/Windows minimum/latest Python; it is not a prerequisite for the initial foundation.

Deferred: active handlers crossing outages, outage-specific deadline/cancellation timing, uninterrupted live iterators, prolonged outages, buffer saturation, credentials and cluster failover. Existing at-least-once, deadline and cancellation semantics are not weakened by deferring additional outage proofs.

## Additional crash windows — decision 17

[Prioritize additional submission, retry and observation crash windows](https://github.com/vschroeter/superjobs/issues/17).

| Internal grilling question | Selected answer |
| --- | --- |
| Add more mandatory PR fault cases now? | No. Keep the two worker kills and persistent broker restart already selected in the foundation. |
| Which additional boundary merits one proof? | Retry successfully published, old work delivery not yet acknowledged. It is a distinct queue handoff where old and new messages may coexist. |
| What is the oracle? | Same execution identity; correct authoritative result/outcome; terminal state and bounded terminal observation closure. Preserve retained-history/cursor behavior. |
| What duplicate behavior is valid? | Duplicate attempts/effects before durable completion are allowed. Do not assert exactly two invocations or contiguous attempt numbers. |
| How much test machinery? | Reuse the installed-process kill/checkpoint harness and real broker writes. One worker-only adapter; no production fault framework. |
| What justifies another case later? | A reproduced defect, incident, or relevant change to submission/retry/observation reconciliation. |

The extra case uses zero-backoff retry to keep delayed-retry semantics out of the first proof. Kill after the real publication succeeds and before the original acknowledgement, then replace the worker and reconstruct the public handle. Invocation evidence must survive the killed process.

Implementation: [Prove retry recovery after publication before original acknowledgement](https://github.com/vschroeter/superjobs/issues/21), manual/exploratory initially, outside the initial required PR gate. Failures still return nonzero and get targeted fixes.

Deferred inspected windows: producer execution/idempotency mapping before publication, publication before the published flag, observation sequence allocation before append, and terminal append before its publication flag. Separate writes indicate unproven risk, not measured defects. This prioritization does not permit silently skipping retained observations or replacing authoritative results.

## Optional repetition — decision 18

[Decide scheduled repetition and stress coverage for NATS reliability](https://github.com/vschroeter/superjobs/issues/18).

| Internal grilling question | Selected answer |
| --- | --- |
| When and where? | After the foundation, one optional weekly GitHub Actions run on the default branch, Linux/latest supported stable Python, pinned NATS. The same command runs manually on Linux or Windows. |
| Which cases and how many? | Four existing families: ordinary installed-process contracts/retries, active-handler kill, completion-before-ack kill, persistent broker restart; three repetitions each. |
| Why a seed? | Record scenario ordering for reproduction only. No random sleeps or new fault boundaries. |
| What budget? | Fifteen minutes overall, plus existing scenario/lifecycle caps and fresh isolation. Incomplete or timed-out runs fail. |
| What happens on failure? | Stop at the first failure; retain logs, scenario/repetition/seed and environment. Never retry to green. |
| Does this block PRs? | No. It supplements the required matrix and does not add a stress-platform matrix. |

Implementation: [Add bounded optional repetition of NATS reliability scenarios](https://github.com/vschroeter/superjobs/issues/22). The weekly direction is selected; its exact low-contention UTC trigger is a routine implementation choice. No workflow, Codex automation or notification service is created by this decision.

Deferred: load/saturation generators, random failpoints, long soaks, repetition across every platform/version and broader chaos testing. Add them only for a concrete reliability question.

## Manual performance baseline — decision 19

[Define workload-based performance measurements after the reliable test foundation](https://github.com/vschroeter/superjobs/issues/19).

| Internal grilling question | Selected answer |
| --- | --- |
| What workloads? | Typed telemetry around 1 KiB serialized with no final payload/events; a typed manifest around 64 KiB with a compact result and three small ordered events. Report actual bytes; these are fixtures, not API limits. |
| What topology and concurrency? | One installed producer, one installed worker, one local disk-backed broker. Bounded producer in-flight/worker concurrency at 1 and 8. |
| What samples? | Brief warmup, then three 20-second samples for each combination, with bounded drain and cleanup. Report actual elapsed time and incomplete work. |
| What metrics first? | Completed executions/second, failures, submit latency and submit-start-to-terminal latency, timed in the producer with a monotonic clock. Counts and median/p95 when samples support them. |
| What proves a useful result? | Correct terminal outcomes and event count/order, readable public API examples, revision/environment/workload metadata, and simple machine-readable samples plus a report. |
| What counts as a regression? | Compare revisions on the same machine/configuration and show measured variation. No universal SLO or noisy PR threshold before baselines exist. |

Implementation: [Add manual telemetry and manifest performance baselines](https://github.com/vschroeter/superjobs/issues/23). Minimal predictable handlers keep application work visible; end-to-end timings include submission, serialization, broker/storage and dispatch costs and must not be called pure queue wait. Preserve a dated baseline artifact and report concrete API friction; public interface changes get separate bounded work.

Codex deliberately removed proposed detailed phase instrumentation from this first slice. Defer pure queue/handler/event-delivery latency, cross-process clock subtraction, per-job production instrumentation, CPU/RSS exporters, dashboards, baseline services, p99 claims, multi-host/tuning matrices, hard-real-time/rich-media targets and required PR benchmarks. Measure those only when a concrete question needs them.

## Evidence, dependencies and completion

The sessions inspected repository code, existing test coverage and dependency documentation. They ran no fault scenarios, stress sessions or benchmarks. No production code, workflow, broker or application behavior changed.

All four implementation tickets are native children of their resolved decisions and depend on [Add fast and full developer checks with required Linux and Windows CI gates](https://github.com/vschroeter/superjobs/issues/15). The decisions remain under [Wayfinder: Typed contracts and a reliable test foundation](https://github.com/vschroeter/superjobs/issues/1).

Closing decisions 16–19 records settled scope; implementation tickets 20–23 remain open. Cursor Composer 2.5 implements bounded slices; Codex independently verifies results. Extend the plan when measured behavior warrants it, rather than completing an exhaustive theoretical fault matrix.
