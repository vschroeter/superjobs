# SuperJobs documentation

Pre-alpha typed jobs over NATS JetStream. Start with the repository
[README](../README.md), then:

| Guide | Contents |
| --- | --- |
| [api.md](api.md) | Public behavior, semantics, typing limits |
| [payload-validation.md](payload-validation.md) | Strict boundary behavior and adapter policy |
| [development.md](development.md) | Canonical `dev_check`, CI matrix, runner index |
| [verification.md](verification.md) | Dated measured results and limitations |
| [adr/](adr/README.md) | Accepted decisions with issue links |

Domain vocabulary: [CONTEXT.md](../CONTEXT.md) (not duplicated here). Reference
revision: **`api_design`** at **`04f24f2`**.

Follow-up work is tracked on GitHub (for example
[Wayfinder #28](https://github.com/vschroeter/superjobs/issues/28),
[#31](https://github.com/vschroeter/superjobs/issues/31) typing,
[#32](https://github.com/vschroeter/superjobs/issues/32) validation,
[#33](https://github.com/vschroeter/superjobs/issues/33) performance,
[#34](https://github.com/vschroeter/superjobs/issues/34) CI activation,
[#35](https://github.com/vschroeter/superjobs/issues/35) packaging,
[#36](https://github.com/vschroeter/superjobs/issues/36) sync API).

## Specialized procedural records

| Document | Role |
| --- | --- |
| [design/nats-test-harness.md](design/nats-test-harness.md) | Owned JetStream broker for tests |
| [design/cross-program-verification.md](design/cross-program-verification.md) | Installed producer/worker scenarios |
| [design/worker-recovery-verification.md](design/worker-recovery-verification.md) | Crash before/after completion; optional #21 |
| [design/broker-restart-verification.md](design/broker-restart-verification.md) | Persistent-store restart |
| [design/idle-outage-verification.md](design/idle-outage-verification.md) | Short outage reconnect |
| [design/reliability-repetition-verification.md](design/reliability-repetition-verification.md) | Optional #22 repetition |
| [design/performance-baseline.md](design/performance-baseline.md) | Optional #23 harness |
| [Manifest replay investigation (#27)](https://github.com/vschroeter/superjobs/issues/27) | Investigation outcome and retained evidence |
| [performance-baselines/](performance-baselines/README.md) | Dated JSON/MD measurement artifacts |
| [research/contract-fingerprint-detection.md](research/contract-fingerprint-detection.md) | Cross-process fingerprint design research |
| [research/strict-payload-validation.md](research/strict-payload-validation.md) | Pydantic strict-boundary research |
| [research/job-queue-api-comparison.md](research/job-queue-api-comparison.md) | Comparative queue API research (#2) |
| [research/historical-typing-and-validation-notes.md](research/historical-typing-and-validation-notes.md) | Consolidated historical typing probes |

Runnable contracts: [examples/contract_interface/README.md](../examples/contract_interface/README.md).

## Original file disposition (consolidation 2026-10-03)

Every prior `docs/**/*.md` path is either retained above or merged below. Rationale
is explicit so GitHub issues need not duplicate file history.

| Original path | Disposition | Rationale / destination |
| --- | --- | --- |
| `README.md` (this file) | **Rewritten** | Canonical index and disposition |
| `api.md` | **Created** | Public API and typing-limit examples |
| `payload-validation.md` | **Created** | Current strict validation from deleted `design/strict-payload-validation.md` |
| `development.md` | **Created, canonical** | Absorbed `design/dev-checks-verification.md` orchestration detail |
| `verification.md` | **Created, canonical** | Absorbed `issues22-23` and `deterministic-test-synchronization` evidence |
| `adr/*.md` | **Created** | ADR 0001–0006 with expanded 0002/0003/0005 rationale |
| `design/nats-test-harness.md` | **Retained** | Owned-broker protocol still needed |
| `design/cross-program-verification.md` | **Retained** | Installed-process checkpoints |
| `design/worker-recovery-verification.md` | **Retained** | Kill/recovery protocol |
| `design/broker-restart-verification.md` | **Retained** | Persistent-store restart protocol |
| `design/idle-outage-verification.md` | **Retained** | Idle outage protocol |
| `design/reliability-repetition-verification.md` | **Retained** | Optional #22 runner detail |
| `design/performance-baseline.md` | **Retained** | Optional #23 harness detail |
| `design/manifest-replay-investigation.md` | **Retained untouched** | Local #27 notes |
| `design/dev-checks-verification.md` | **Removed** | Unique CI/JUnit/wheel-isolation rules → `development.md` |
| `design/issues22-23-verification.md` | **Removed** | Dated counts/controls → `verification.md` |
| `design/deterministic-test-synchronization.md` | **Removed** | Issue #25 rationale → `verification.md` |
| `design/contract-handler-interface.md` | **Removed** | Handler/context rules → ADR 0001, `api.md` |
| `design/strict-payload-validation.md` | **Removed** | Shipped behavior → `payload-validation.md`, ADR 0002 |
| `design/contract-interface-verification.md` | **Removed** | Gate commands → `development.md`, results → `verification.md` |
| `design/consumer-typing-verification.md` | **Removed** | Typing gate → `development.md`, `api.md` |
| `design/handler-registration-verification.md` | **Removed** | Registration rules → `api.md`, `verification.md` |
| `design/producer-interface-verification.md` | **Removed** | Producer/submit → `api.md` |
| `design/outcome-typing-verification.md` | **Removed** | Outcome typing → `api.md` |
| `design/test-strategy.md` | **Removed** | Matrix intent → ADR 0004, `development.md` |
| `design/reliability-performance-followups.md` | **Removed** | Optional scope → ADR 0005, `verification.md` |
| `research/typing-baseline.md` | **Removed** | Historical baseline → `historical-typing-and-validation-notes.md` |
| `research/contract-convenience-typing.md` | **Removed** | ParamSpec/options research → historical note |
| `research/producer-subtype-typing-probe.md` | **Removed** | RequestJob stub evidence → historical note |
| `research/contract-fingerprint-detection.md` | **Restored** | Primary-source fingerprint/manifest reasoning; ADR 0003 |
| `research/strict-payload-validation.md` | **Restored** | Primary-source Pydantic probes; `payload-validation.md` |
| `research/job-queue-api-comparison.md` | **Restored** | Comparative API research (#2) |
| `research/historical-typing-and-validation-notes.md` | **Created** | Index for merged typing probes |
| `performance-baselines/high-resolution/2026-10-03-baseline.md` | **Retained** | Passing #23 baseline report |
| `performance-baselines/high-resolution/2026-10-03-baseline.json` | **Retained** | Machine-readable companion |
| `performance-baselines/2026-10-03-failed-baseline.md` | **Retained** | Failed measurement evidence; no verified replay fix |
| `performance-baselines/2026-10-03-failed-baseline.json` | **Retained** | Machine-readable companion |
| `performance-baselines/2026-10-03-original-order-diagnostic.json` | **Retained** | Manifest event-read diagnostic (#27 context) |
| `performance-baselines/README.md` | **Retained** | Artifact index |
