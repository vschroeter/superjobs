"""Optional typed CLI registration (install ``superjobs[cli]``)."""

from superjobs.cli.app import JobCLI
from superjobs.cli.runtime_factory import LocalRuntimeFactory, RemoteRuntimeFactory
from superjobs.cli.field_config import CLIField
from superjobs.cli.constants import (
    DEFAULT_CLI_WAIT_TIMEOUT_SECONDS,
    EXIT_INTERRUPTED,
    EXIT_RUNTIME_FAILURE,
    EXIT_SUCCESS,
    EXIT_USAGE,
    LOCAL_RUN_SHUTDOWN_TIMEOUT_SECONDS,
    MISSING_REMOTE_FACTORY_MESSAGE,
    REMOTE_SUBMIT_SHUTDOWN_TIMEOUT_SECONDS,
    RESERVED_CLI_OPTION_NAMES,
)
from superjobs.cli.registration import CommandRegistration
from superjobs.cli.validation import CLIRegistrationError

__all__ = [
    "CLIField",
    "CLIRegistrationError",
    "CommandRegistration",
    "DEFAULT_CLI_WAIT_TIMEOUT_SECONDS",
    "EXIT_INTERRUPTED",
    "EXIT_RUNTIME_FAILURE",
    "EXIT_SUCCESS",
    "EXIT_USAGE",
    "LOCAL_RUN_SHUTDOWN_TIMEOUT_SECONDS",
    "MISSING_REMOTE_FACTORY_MESSAGE",
    "REMOTE_SUBMIT_SHUTDOWN_TIMEOUT_SECONDS",
    "JobCLI",
    "LocalRuntimeFactory",
    "RemoteRuntimeFactory",
    "RESERVED_CLI_OPTION_NAMES",
]
