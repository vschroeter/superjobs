"""CLI exit codes and reserved option names (issue #38 foundation slice)."""

from __future__ import annotations

EXIT_SUCCESS = 0
EXIT_RUNTIME_FAILURE = 1
EXIT_USAGE = 2
EXIT_INTERRUPTED = 130

# Planned whole-request and control options (issue #39+). Normalized without leading dashes.
RESERVED_CLI_OPTION_NAMES: frozenset[str] = frozenset(
    {
        "json",
        "input",
        "wait",
        "wait-timeout",
        "help",
    }
)

UNAVAILABLE_EXECUTION_MESSAGE = (
    "CLI execution is not available in this SuperJobs release slice; "
    "registration and help only (see docs/design/cli-registration.md)."
)

REMOTE_ONLY_RUN_MESSAGE = (
    "Cannot run command locally: it is registered for remote submission only."
)

ACTIVE_EVENT_LOOP_MESSAGE = (
    "JobCLI.main() is for process entry points only. "
    "An asyncio event loop is already running in this thread; "
    "invoke the Typer app from a thread without a running loop instead."
)
