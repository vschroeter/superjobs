"""Isolated subprocess environments for cross-program verification."""

from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import Mapping
from pathlib import Path

_BOOTSTRAP = Path(__file__).resolve().parent / "isolated_bootstrap.py"


def isolated_child_env(extra: Mapping[str, str] | None = None) -> dict[str, str]:
    env = os.environ.copy()
    for key in ("PYTHONPATH", "PYTHONHOME"):
        env.pop(key, None)
    env["PYTHONNOUSERSITE"] = "1"
    if extra:
        env.update(extra)
    return env


def python_command(
    python_executable: str,
    role_dir: Path,
    script: str,
    *script_args: str,
) -> list[str]:
    return [
        python_executable,
        "-I",
        str(_BOOTSTRAP),
        str(role_dir.resolve()),
        script,
        *script_args,
    ]


def subprocess_creationflags() -> int:
    if sys.platform == "win32":
        return subprocess.CREATE_NO_WINDOW
    return 0
