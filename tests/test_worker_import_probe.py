from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
WORKER_IMPORT_PROBE = REPO_ROOT / "examples" / "contract_interface" / "worker_import_probe.py"
CONTRACT_EXAMPLE_SRC = (
    REPO_ROOT / "examples" / "contract_interface" / "superjobs_contract_example" / "src"
)


@pytest.mark.asyncio
async def test_worker_import_probe_from_installed_script(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    probe = Path(__file__).with_name("worker_import_probe.py")
    if not probe.is_file():
        probe = WORKER_IMPORT_PROBE
        monkeypatch.syspath_prepend(str(CONTRACT_EXAMPLE_SRC))
    assert probe.is_file(), "Required worker import probe is missing"
    spec = importlib.util.spec_from_file_location("worker_import_probe", probe)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    await module.run_worker_import_probe()
