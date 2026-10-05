"""Compatibility shim; handlers live in ``superjobs_contract_handlers``."""

from __future__ import annotations

from superjobs import SuperJobs
from superjobs_contract_handlers import CONTRACT_CATALOG


def register_contract_handlers(jobs: SuperJobs) -> None:
    """Deprecated: construct ``SuperJobs(..., handlers=CONTRACT_CATALOG)`` instead."""
    raise RuntimeError(
        "register_contract_handlers is deprecated; pass handlers=CONTRACT_CATALOG to SuperJobs",
    )


__all__ = ["CONTRACT_CATALOG", "register_contract_handlers"]
