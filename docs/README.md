# SuperJobs documentation

Pre-alpha typed jobs over NATS JetStream. Start with the repository
[README](../README.md), then:

| Guide | Contents |
| --- | --- |
| [api.md](api.md) | Public behavior, semantics, typing limits |
| [cli.md](cli.md) | Optional Typer CLI: registration, input, local `run`, remote `submit` |
| [payload-validation.md](payload-validation.md) | Strict boundary behavior and adapter policy |
| [development.md](development.md) | Canonical `dev_check`, CI matrix, runner index |
| [design/cli-application.md](design/cli-application.md) | Installed CLI/worker wheels, process proof, reference recovery example |
| [verification.md](verification.md) | Dated measured results and limitations |
| [adr/](adr/README.md) | Accepted decisions with issue links |

Domain vocabulary: [CONTEXT.md](../CONTEXT.md) (not duplicated here). Current
product docs track branch **`feature/cli`**; hosted CI at **`api_design` /
`04f24f2`** predates the CLI integration stage (see [verification.md](verification.md)).

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
| [design/manifest-replay-investigation.md](design/manifest-replay-investigation.md) | Optional replay diagnostics, commands and evidence limits |
| [performance-baselines/](performance-baselines/README.md) | Dated JSON/MD measurement artifacts |
| [research/contract-fingerprint-detection.md](research/contract-fingerprint-detection.md) | Cross-process fingerprint design research |
| [research/strict-payload-validation.md](research/strict-payload-validation.md) | Pydantic strict-boundary research |
| [research/job-queue-api-comparison.md](research/job-queue-api-comparison.md) | Comparative queue API research (#2) |
| [research/historical-typing-and-validation-notes.md](research/historical-typing-and-validation-notes.md) | Consolidated historical typing probes |

Runnable contracts: [examples/contract_interface/README.md](../examples/contract_interface/README.md).

## Original file disposition

Grouped history of prior `docs/**/*.md` paths (details in git). **Current** user
guides are listed in the table above. Cross-cutting dated gate summaries live in
[verification.md](verification.md); specialized historical evidence, baselines, and
procedural `design/*-verification.md` files remain linked from the tables above.

| Group | Disposition |
| --- | --- |
| **2026-10-03 core consolidation** | Created `api.md`, `payload-validation.md`, `development.md`, `verification.md`, `adr/*`; removed superseded API/typing/CI report pages (`design/test-strategy.md`, `design/strict-payload-validation.md`, and related one-off verification write-ups); retained specialized NATS/recovery/outage/repetition/performance `design/*-verification.md`; merged typing probes into `research/historical-typing-and-validation-notes.md` |
| **2026-10-04 CLI consolidation** | Created [cli.md](cli.md); removed `design/cli-registration.md`, `design/cli-input.md`, `design/cli-local.md`, `design/cli-remote.md` (behavior → `cli.md`; slice evidence → `verification.md`); trimmed [design/cli-application.md](design/cli-application.md) to installed proof |
| **Retained specialized `design/*`** | NATS harness, cross-program, worker recovery, broker restart, idle outage, reliability repetition, performance baseline, manifest replay investigation |
| **Retained `research/*` and `performance-baselines/*`** | Primary-source research and measurement artifacts (see specialized table above) |
