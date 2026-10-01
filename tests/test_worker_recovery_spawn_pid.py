"""Unit checks for spawn vs ready pid relationship helpers."""

from __future__ import annotations

import subprocess
import sys
import time

from tools.worker_recovery_support.spawn_pid import marker_pid_in_spawn_tree


def test_marker_pid_matches_child_process() -> None:
    process = subprocess.Popen(
        [
            sys.executable,
            "-c",
            "import os, time; time.sleep(2)",
        ],
        creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0,
    )
    try:
        assert process.pid is not None
        marker_pid = process.pid
        assert marker_pid_in_spawn_tree(process.pid, marker_pid)
    finally:
        process.kill()
        process.wait(timeout=5)


def test_marker_pid_rejects_unrelated_pid() -> None:
    process = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(2)"],
        creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0,
    )
    try:
        assert process.pid is not None
        assert not marker_pid_in_spawn_tree(process.pid, process.pid + 999_999)
    finally:
        process.kill()
        process.wait(timeout=5)
