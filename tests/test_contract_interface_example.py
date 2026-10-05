"""Exercise the contract-interface example package and in-memory demo.

This module covers **source-layout** behavior only: contract code is found via
``PYTHONPATH`` (see ``CONTRACT_SRC``), not via ``uv pip install`` during pytest.
Wheel / editable-install consumer typing for ``superjobs_contract_example`` is
verified separately by Codex (not in this pytest module).
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
EXAMPLE_ROOT = REPO_ROOT / "examples" / "contract_interface"
CONTRACT_SRC = EXAMPLE_ROOT / "superjobs_contract_example" / "src"
DEMO_TIMEOUT = 60.0


def _source_example_pythonpath() -> str:
    """Preserve the ambient PYTHONPATH and append example + contract sources."""
    existing = os.environ.get("PYTHONPATH", "")
    segments = [segment for segment in (existing, str(EXAMPLE_ROOT), str(CONTRACT_SRC)) if segment]
    return os.pathsep.join(segments)


def _run_source_example_script(*script_args: str) -> subprocess.CompletedProcess[str]:
    env = {**os.environ, "PYTHONPATH": _source_example_pythonpath()}
    return subprocess.run(
        [sys.executable, *script_args],
        cwd=REPO_ROOT,
        env=env,
        timeout=DEMO_TIMEOUT,
        capture_output=True,
        text=True,
        check=False,
    )


def test_contract_package_public_imports(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.syspath_prepend(str(CONTRACT_SRC))
    from superjobs_contract_example import HEARTBEAT_JOB, MANIFEST_WITH_EVENTS_JOB, ManifestRequest

    assert MANIFEST_WITH_EVENTS_JOB.name == "examples.contract.manifest.with_events"
    assert HEARTBEAT_JOB.request_type is None
    assert ManifestRequest(device_id="x").device_id == "x"


def test_in_memory_contract_interface_demo() -> None:
    completed = _run_source_example_script(str(EXAMPLE_ROOT / "in_memory_demo.py"))
    if completed.returncode != 0:
        pytest.fail(
            "in_memory_demo failed\n"
            f"stdout:\n{completed.stdout}\n"
            f"stderr:\n{completed.stderr}",
        )
