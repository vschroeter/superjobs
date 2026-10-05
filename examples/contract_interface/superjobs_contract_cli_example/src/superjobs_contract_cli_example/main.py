"""Contract-interface CLI with catalog-backed ``run`` and built-in NATS ``submit``."""

from __future__ import annotations

import os

from superjobs.cli import JobCLI
from superjobs_contract_example import CLI_FAIL_JOB, CLI_GATE_JOB
from superjobs_contract_handlers import CONTRACT_CATALOG


def _configured_nats_url() -> str | None:
    raw = os.environ.get("SUPERJOBS_CLI_CONFIGURED_NATS_URL")
    return raw if raw else None


def build_cli() -> JobCLI:
    cli = JobCLI(handlers=CONTRACT_CATALOG, nats_url=_configured_nats_url())
    cli.add("gate-remote", CLI_GATE_JOB, remote_only=True)
    cli.add("fail-remote", CLI_FAIL_JOB, remote_only=True)
    return cli


def main(argv: list[str] | None = None) -> int:
    import sys

    if os.environ.get("SUPERJOBS_CLI_EMIT_PID") == "1":
        print(f"cli-pid:{os.getpid()}", file=sys.stderr, flush=True)
    return build_cli().main(argv)


if __name__ == "__main__":
    raise SystemExit(main())
