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

## CI collection correction

The first CI fast matrix failed during collection: this change's top-level
`evidence/` directory became a Python namespace package and shadowed an existing
resource-verifier helper import. Relocating the feature's evidence under
`docs/evidence/` removes that collision without changing existing runtime imports.
The initially narrow checks did not collect that existing helper test module.

After relocation, the exact isolated CI fast command passed: **1006 passed,
2 skipped, 84 deselected**, stage 154.3 s, exit 0, no cleanup errors; complete
stage result is in `fast-after-relocation/run-summary.json`. The focused namespace,
process-proof and previously failing helper-import suite passed **39 tests in
9.40 s** (`pytest-after-relocation.txt`). Combined source/wheel typing, diagnostic
matching and installed-wheel origin/runtime verification all passed again; current
resource API and runtime records are in `typing-after-relocation/`.

## Startup and exit-proof correction

Independent Standards and Spec reviews of `0c5267b` both identified the same P2:
registration failure could kill only the Windows launcher before retaining the
actual interpreter. An owned idle-child reproducer observed launcher exit while
the interpreter was momentarily still alive and the registry/audit were empty.
No reproducer remains running. The Ubuntu CI artifact independently exposed a
POSIX race: after SIGKILL, exit proof ignored its deadline and immediately polled
instead of waiting for the actual child to exit.

The dummy-worker spawner now starts the actual CPython binary (avoiding the
Windows venv redirector) and preserves the parent's dependency paths in its
stdlib bootstrap. Native Popen ownership is retained before marker or extra
handle acquisition can fail. Registry identity is the retained process object;
startup marker PIDs cannot select another process. Startup failures reap the
same owned interpreter. POSIX proof honors its deadline with `Popen.wait()`.
A failed Windows terminate call is accepted only when the same held handle proves
concurrent exit. Unproved exit remains registered and never yields recovery proof.

Current targeted proof: **48 passed in 9.47 s**, including actual native-child
marker/foreign-marker/handle/timeout failures, a deliberately exited launcher with
a still-living captured interpreter, and the missing-handle case staying visibly
unconfirmed until the test's independent pre-held backup handle proves cleanup.
A follow-up Standards review identified a setup-only version of the same leak in
that adversarial test's nested child/marker handoff. The fixture now directly
owns both native processes from creation and holds their Popen handles before
all additional handle/marker setup. It models the captured split-process state
without an unowned descendant or fallible identity handoff.

Final full isolated fast gate: **1015 passed, 2 skipped, 84 deselected**, stage
154.5 s, exit 0, no cleanup errors; `fast-after-fixture-correction/run-summary.json`.
The preceding lifecycle pass also had **1015 passed**, stage 155.2 s. Logs: `pytest-lifecycle-correction.txt` and
`fast-after-lifecycle-correction/run-summary.json`. The new head's hosted Linux
and Windows integration matrices still need their own successful result.

## Initial measured verification

- Final focused/configuration/process-proof and existing admission/fencing suite:
  **63 passed in 11.35 s**, `pytest-final-results.txt`.
- Public consumer typing, exact project Pyright 1.1.414: source **OK**,
  `typing-independent/`; installed wheel **OK**, `typing-wheel-independent/`.
  `typing-summary.json` retains per-suite counts/hashes; full raw negative
  diagnostics remain local. Every positive suite passed and every negative suite
  matched its expected ruled
  errors (46/17/1/27/9/9/30/12 in the verifier's suite order).
- Installed wheel Python 3.13.5: package origin verified; **460 passed, 1 skipped, 8 deselected**, exit 0.
  Source and wheel initially ran independently; after relocation the combined
  source/wheel diagnostic matching mode also passed.
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
worker dependencies. The actual native interpreter is directly spawned; its
Popen handle is retained immediately, then an additional actual-interpreter handle
is opened before any graceful stop or kill. The startup marker must match that
already owned interpreter. Recovery evidence is
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
