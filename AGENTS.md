# SuperJobs development

SuperJobs is a pre-alpha Python alternative to Taskiq for typed asynchronous jobs
over NATS, especially telemetry and manifest data exchanged between programs.
Optimize the developer interface and reliability first; keep overhead and latency
low without treating rich media streaming or hard real-time as target use cases.

Write all file outputs and GitHub issue titles, bodies, and comments in English,
regardless of the language used in user instructions or chat replies. Chat replies
may follow the user's language, including German.

The current design is open to change. Read `CONTEXT.md` for domain terminology.
Start with a shared contract package containing Job definitions and payload types,
importable by producers without importing worker implementations.

Use GitHub Issues in `vschroeter/superjobs` for planned features and open decisions.
Keep design questions separate from implementation acceptance criteria. Use a
Wayfinder map for efforts spanning several decisions; link tickets by title and
use native sub-issues and dependencies where available.

Codex plans work, directs agents, and independently reviews their results. Delegate
most implementation work to the Cursor agent CLI with model `composer-2.5`.
The maintainer explicitly authorized transferring this repository's source and
project context to Cursor/Composer 2.5 for analysis and implementation on
2026-09-28.
Give each task a bounded scope, acceptance criteria, and appropriate verification.
Request targeted corrections when a result is poor. Implement directly only after
repeated unsuccessful Cursor attempts; report CLI or authentication blockers
instead of silently substituting another implementation model.

Prove typing with a static checker and positive and negative consumer examples.
Exercise contracts through public imports, including the installed distribution.
Use deterministic in-memory tests for fast feedback, real NATS integration tests
for transport semantics, and separate producer/worker processes for cross-program
behavior. Required integration checks must fail when their environment is broken.

Keep each iteration small and reviewable. Preserve existing work and distinguish
measured behavior, design proposals, and unresolved questions in reports.
