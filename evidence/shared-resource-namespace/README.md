# Independent shared resource-namespace verification

Date: 2026-10-29. Branch: `codex/shared-resource-namespace`. Base: `dd8116c`.
All live transport tests used an owned ephemeral loopback NATS server/store. No
production broker, old job, application database, model, or synthesis run was used.

## Implementation contract

`NatsQueueConfig.resource_namespace` is optional and appended to preserve existing
positional calls. `None` preserves the original admission bucket derived from
`subject_prefix`; explicit values use strict ASCII dot-separated token validation.
The one central `NatsResourceAdmissionStore` receives the resolved namespace, so
capacity, owner/claim/fence, unavailable, revocation, recovery, snapshot and cache
invalidation all use the same bucket. Work streams, completion/idempotency and
observation/discovery remain queue-specific. No admission record format changed.

## Measured verification

- Final focused/configuration/process-proof and existing admission/fencing suite:
  **63 passed in 11.35 s**, `pytest-final-results.txt`.
- Public consumer typing, exact project Pyright 1.1.414: source **OK**,
  `typing-independent/`; installed wheel **OK**, `typing-wheel-independent/`.
  `typing-summary.json` retains per-suite counts/hashes; full raw negative
  diagnostics remain local. Every positive suite passed and every negative suite
  matched its expected ruled
  errors (46/17/1/27/9/9/30/12 in the verifier's suite order).
- Installed wheel Python 3.13.5: package origin verified; **460 passed, 1 skipped, 8 deselected**, exit 0.
  Source and wheel were run independently; the verifier's combined source/wheel
  diagnostic matching mode was not run.
- Python 3.12/3.14 and the full repository integration matrix were not run.
- Read-only post-test OS check found no owned namespace-test worker interpreters.
  The registered-handle exit audit also passed.

Tests run using `uv run --no-sync --offline python -m pytest` with the existing
SuperJobs venv, this worktree on `PYTHONPATH`, bytecode disabled and `NATS_URL` unset.
After moving default evidence output to pytest temporary storage, the affected
30-test proof suite also passed in 9.30 s (`pytest-proof-results.txt`).
Set `SJ_SHARED_NAMESPACE_EVIDENCE_DIR` explicitly to retain process evidence;
ordinary runs write it under pytest's temporary directory.

The checker was initially absent from the offline tool cache. The normal project
verifier then obtained the exact pinned checker with allowed network access; it
changed no shared project environment. The subsequent wheel gate ran offline.
The repository does not declare Ruff and it was unavailable; no lint dependency
was installed. `git diff --check` is the whitespace gate.

## Cross-process evidence and fail-closed lifecycle

`process-exits/cross_process_exclusion/coordination.json` records two different
work prefixes, their actual common admission bucket, the exact CPU resource key,
distinct actual interpreter PIDs, observed owner and both queue-scoped results.
The host ID is synthetic, but key geometry matches Samplegen's physical CPU key.
The handlers only wait for test control messages; they perform no synthesis.

Scenarios prove B reaches `WAITING_FOR_RESOURCE` and cannot enter while A owns the
CPU; both observers see A and its claim. After release, B enters and both results
stay with their respective queue. Final owner/claim state is empty. Other cases
exercise an owned broker restart, cache invalidation, failed cleanup retaining
`UNAVAILABLE`, and cross-prefix recovery only after the exact A interpreter exits.

Crash recovery waits until B actually holds a newer fence generation/token. A
late release must return **STALE** and leave B's owner/fence/claim unchanged.
`NOT_OWNER` applies after ownership/claim is already absent; accepting it while B
actively owns the resource would weaken the proof and is not allowed here.

Windows interpreter identity is reported by a stdlib bootstrap before importing
worker dependencies. Its owned spawn tree is checked, and the actual interpreter
process handle is opened before any graceful stop or kill. Recovery evidence is
returned only after that handle proves interpreter exit and the launcher is
reaped. Per-case exit JSON preserves both PIDs and exact worker incarnation.
No global process scan grants ownership or recovery. Failure to prove exit keeps
cleanup unconfirmed. Separate unit regressions reject a foreign marker and prove
that launcher exit alone cannot authorize resource recovery.

Earlier Composer helper attempts were rejected during independent review: they
could confuse a Windows launcher exit with interpreter exit, or block reading a
live stderr pipe. After repeated targeted Composer corrections, the maintainer
allowed a direct helper-only repair. The main library implementation remained
Composer's; the repaired lifecycle and safety regressions were independently run.

## Application integration remains a separate step

This commit enables generic transport separation. No Samplegen integration or
Piper diagnostic run has been performed. The existing programmatic runtime and
broker-check seams can supply a private queue/workspace, but must coherently use
the production admission namespace and physical host ID. Never infer permission
to recover a foreign owner from missing metadata in private diagnostic storage.
Never delete/reset the shared admission bucket as diagnostic queue cleanup.
