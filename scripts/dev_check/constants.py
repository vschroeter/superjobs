"""Shared constants for developer check orchestration (#15)."""

from __future__ import annotations

# Reviewed stable CPython minors for required fast gates (3.15 excluded).
FAST_PYTHON_MINORS: tuple[str, ...] = ("3.12", "3.13", "3.14")

# Heavier integration gates use minimum and latest stable minors only.
INTEGRATION_PYTHON_MINORS: tuple[str, ...] = ("3.12", "3.14")

# Pinned toolchain versions (also recorded in tests/support/nats_harness and verify_contract_typing).
NATS_SERVER_VERSION = "2.15.0"
PYRIGHT_VERSION = "1.1.414"

# Safety caps from docs/design/test-strategy.md (bounds, not performance targets).
INTEGRATION_JOB_BUDGET_SECONDS = 15 * 60
# Reserve wall time at end of a job so CI can still upload failure artifacts before the 15m limit.
JOB_CLEANUP_RESERVE_SECONDS = 45.0
INTEGRATION_STAGE_BUDGET_SECONDS = INTEGRATION_JOB_BUDGET_SECONDS - JOB_CLEANUP_RESERVE_SECONDS

SUBPROCESS_PROBE_TIMEOUT_SECONDS = 10.0
COMMUNICATE_AFTER_KILL_TIMEOUT_SECONDS = 5.0

# Stable aggregate names for branch-protection documentation (jobs use these prefixes).
GATE_FAMILY_FAST = "checks / fast"
GATE_FAMILY_INTEGRATION = "checks / integration"

FAST_PYTEST_MARKER = "not nats and not contract_typing"
INTEGRATION_PYTEST_MARKER = "nats"

# Default evidence root under the repository (generated, not tracked).
DEFAULT_ARTIFACT_ROOT = "dist/verification/dev-check"

# Documented example minor for copy-paste CI-style commands (matrix cells pass their own minor).
UV_ISOLATED_EXAMPLE_PYTHON = "3.12"

# CI / docs: isolated uv environment with worktree source and test deps (see docs/development.md).
UV_ISOLATED_RUN_CMD = (
    "uv run --isolated --no-project --with-editable . "
    f'--python {UV_ISOLATED_EXAMPLE_PYTHON} '
    '--with "pytest>=9.1.1" --with "pytest-asyncio>=1.4.0"'
)
