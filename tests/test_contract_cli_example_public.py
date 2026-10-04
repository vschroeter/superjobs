"""Public imports for the installed contract-interface CLI example (issue #42)."""

from __future__ import annotations

from pathlib import Path

import pytest


def test_build_cli_is_public_api(monkeypatch: pytest.MonkeyPatch) -> None:
    # Source tests explicitly select sources. Copied wheel tests have no example
    # tree and must import the installed application; missing installs fail.
    examples = Path(__file__).resolve().parents[1] / "examples" / "contract_interface"
    if examples.is_dir():
        for package in ("superjobs_contract_example", "superjobs_contract_cli_example"):
            monkeypatch.syspath_prepend(str(examples / package / "src"))
    from superjobs_contract_cli_example import build_cli
    from superjobs.cli import JobCLI

    cli = build_cli()
    assert isinstance(cli, JobCLI)
