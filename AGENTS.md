# SuperJobs development

SuperJobs is a pre-alpha Python alternative to Taskiq for typed asynchronous jobs
over NATS, especially telemetry and manifest data exchanged between programs.
Optimize the developer interface and reliability first; keep overhead and latency
low without treating rich media streaming or hard real-time as target use cases.

Write all file outputs and GitHub issue titles, bodies, and comments in English,
regardless of the language used in user instructions or chat replies. Chat replies
may follow the user's language, including German.

The current design is open to change. Read `GLOSSARY.md` for domain terminology.
Keep `GLOSSARY.md` limited to context-specific terms and definitions; record accepted
architectural choices in `docs/adr/` only when they are hard to reverse, surprising
without context, and the result of a real trade-off. Keep implementation status and
measured evidence in the relevant guides and verification records.
Start with a shared contract package containing Job definitions and payload types,
importable by producers without importing worker implementations.

Use GitHub Issues in `vschroeter/superjobs` for planned features and open decisions.
Keep design questions separate from implementation acceptance criteria. Use a
Wayfinder map for efforts spanning several decisions; link tickets by title and
use native sub-issues and dependencies where available.

Implement directly. Use external agents and transfer project context to them only
when the maintainer explicitly requests it in the current chat.

Prove typing with a static checker and positive and negative consumer examples.
Exercise contracts through public imports, including the installed distribution.
Use deterministic in-memory tests for fast feedback, real NATS integration tests
for transport semantics, and separate producer/worker processes for cross-program
behavior. Required integration checks must fail when their environment is broken.

Keep each iteration small and reviewable. Preserve existing work and distinguish
measured behavior, design proposals, and unresolved questions in reports.

Use **uv** exclusively for Python package and environment management and for
running scripts, modules, and tools in this repository. Sync dependencies with
`uv sync` or `uv sync --dev` when dev dependencies are needed; use targeted
`uv pip install` only for exceptional layouts (for example isolated verifier
virtual environments). Run commands with `uv run` (for example
`uv run python -m pytest …` or `uv run python tools/verify_contract_typing.py`);
`uv run python …` is acceptable.

When an agent creates a git commit, it may push that commit to the remote
without asking for separate push confirmation.

## Temporary local-only validation

Owner instruction, 2026-11-13: validate changes with local tests only while
GitHub Actions is paused for account limits. Preserve workflow definitions;
enable, dispatch or rerun hosted CI only after an explicit owner request.
Report local validation and any untested environments separately.
