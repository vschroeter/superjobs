"""Shared handler catalog for the contract-interface source layout (issue #46)."""

from __future__ import annotations

import sys
from pathlib import Path

_HANDLERS_SRC = Path(__file__).resolve().parent / "superjobs_contract_handlers" / "src"
if _HANDLERS_SRC.is_dir() and str(_HANDLERS_SRC) not in sys.path:
    sys.path.insert(0, str(_HANDLERS_SRC))

from superjobs_contract_handlers import CONTRACT_CATALOG

__all__ = ["CONTRACT_CATALOG"]
