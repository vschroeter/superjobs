# Architecture decision records

Short, **accepted** decisions for SuperJobs on **`api_design`**. Implementation
status and measured proofs are separated in [api.md](../api.md) and
[verification.md](../verification.md).

| ADR | Title |
| --- | --- |
| [0001](0001-shared-contract-packages.md) | Shared contract packages and handler interface |
| [0002](0002-strict-payload-validation.md) | Strict payload validation at boundaries |
| [0003](0003-cross-process-contract-compatibility.md) | Cross-process compatibility (selected, not implemented) |
| [0004](0004-test-foundation-and-bounded-recovery.md) | Reliable test foundation and bounded recovery scope |
| [0005](0005-broker-outage-retry-and-optional-stress.md) | Broker outage, retry windows, optional stress/performance |
| [0006](0006-sync-producer-convenience.md) | Sync producer convenience direction |

Wayfinder map: [issue #28](https://github.com/vschroeter/superjobs/issues/28).
