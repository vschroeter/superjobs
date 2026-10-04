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

MISSING_REMOTE_FACTORY_MESSAGE = (
    "remote submission requires JobCLI.remote_runtime_factory "
    "(application-owned NATS SuperJobs lifecycle)"
)

SUBMISSION_ACCEPTANCE_UNCONFIRMED_MESSAGE = (
    "submission acceptance unconfirmed (broker did not return an execution handle)"
)

# Default client wait bound for ``submit --wait``; does not cap worker attempt runtime.
DEFAULT_CLI_WAIT_TIMEOUT_SECONDS = 300.0

# Bounded cleanup after remote submit disconnects (factory, runtime, transport).
REMOTE_SUBMIT_SHUTDOWN_TIMEOUT_SECONDS = 30.0

# After the authoritative outcome is known, drain observations briefly even when the
# public iterator has not yet delivered a terminal event (durable completion).
REMOTE_SUBMIT_OBSERVATION_DRAIN_GRACE_SECONDS = 2.0

# Cancellation grace period and subsequent shared cleanup budget for local run.
# Deadlines request asyncio cancellation; they cannot kill arbitrary Python code.
LOCAL_RUN_SHUTDOWN_TIMEOUT_SECONDS = 30.0

REMOTE_ONLY_RUN_MESSAGE = (
    "Cannot run command locally: it is registered for remote submission only."
)

ACTIVE_EVENT_LOOP_MESSAGE = (
    "JobCLI.main() is for process entry points only. "
    "An asyncio event loop is already running in this thread; "
    "invoke the Typer app from a thread without a running loop instead."
)
