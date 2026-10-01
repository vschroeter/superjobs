"""Runtime checks that idle-outage children see only wheel installs and role layout."""

from __future__ import annotations

import importlib.util
from pathlib import Path


class RuntimeIsolationError(RuntimeError):
    """Raised when a child process can import forbidden modules."""


def assert_producer_layout(role_dir: Path) -> None:
    if importlib.util.find_spec("worker_handlers") is not None:
        raise RuntimeIsolationError("worker_handlers must not be importable in producer child")


def assert_worker_layout(role_dir: Path) -> None:
    if importlib.util.find_spec("worker_handlers") is None:
        raise RuntimeIsolationError("worker_handlers must be importable in worker child")
