"""Origin checks for installed CLI and worker verification layouts."""

from __future__ import annotations

import importlib.metadata
import importlib.util
import json
import sys
from pathlib import Path
from typing import Any

from wheel_origin_probe import (  # noqa: E402
    OriginProbeError,
    site_package_roots,
    validate_install_metadata,
    validate_package_origin,
)


def _assert_sys_path_clean(repo_root: Path) -> None:
    repo = repo_root.resolve()
    for entry in sys.path:
        if not entry:
            continue
        try:
            path = Path(entry).resolve()
        except OSError:
            continue
        if path == repo or path.is_relative_to(repo):
            raise OriginProbeError(f"repository path on sys.path: {path}")


def _assert_not_importable(name: str) -> None:
    if importlib.util.find_spec(name) is not None:
        raise OriginProbeError(f"{name} must not be importable in this layout")


def _assert_distribution_absent(name: str) -> None:
    try:
        importlib.metadata.distribution(name)
    except importlib.metadata.PackageNotFoundError:
        return
    raise OriginProbeError(f"distribution {name!r} must not be installed in this layout")


def collect_cli_evidence(repo_root: Path) -> dict[str, Any]:
    _assert_sys_path_clean(repo_root)
    site_roots = site_package_roots()
    validate_install_metadata(site_roots, repo_root)
    packages = [
        validate_package_origin("superjobs", site_roots, repo_root=repo_root),
        validate_package_origin("superjobs_contract_example", site_roots, repo_root=repo_root),
        validate_package_origin("superjobs_contract_handlers", site_roots, repo_root=repo_root),
        validate_package_origin("superjobs_contract_cli_example", site_roots, repo_root=repo_root),
    ]
    _assert_not_importable("superjobs_contract_worker_example")
    _assert_distribution_absent("superjobs-contract-worker-example")
    _assert_not_importable("superjobs_contract_worker_resources")
    _assert_distribution_absent("superjobs-contract-worker-resources")
    try:
        worker_handlers = importlib.util.find_spec("superjobs_contract_worker_example.handlers")
    except ModuleNotFoundError:
        worker_handlers = None
    if worker_handlers is not None:
        raise OriginProbeError("worker handlers package must not be importable in CLI layout")

    eps = importlib.metadata.entry_points()
    select = getattr(eps, "select", None)
    if select is not None:
        matches = list(select(group="console_scripts", name="superjobs-contract-cli"))
    else:
        matches = [ep for ep in eps.get("console_scripts", []) if ep.name == "superjobs-contract-cli"]
    if len(matches) != 1:
        raise OriginProbeError(
            f"expected one superjobs-contract-cli console script, found {len(matches)}",
        )

    from superjobs_contract_cli_example import build_cli  # noqa: F401

    import superjobs  # noqa: F401

    try:
        import superjobs.cli  # noqa: F401
    except ImportError as exc:
        raise OriginProbeError(f"superjobs.cli must be importable with [cli] extra: {exc}") from exc

    return {
        "sys_version": sys.version,
        "sys_executable": sys.executable,
        "site_packages": [str(p) for p in site_roots],
        "packages": packages,
        "console_script": matches[0].value,
    }


def collect_worker_evidence(repo_root: Path) -> dict[str, Any]:
    _assert_sys_path_clean(repo_root)
    site_roots = site_package_roots()
    validate_install_metadata(site_roots, repo_root)
    packages = [
        validate_package_origin("superjobs", site_roots, repo_root=repo_root),
        validate_package_origin("superjobs_contract_example", site_roots, repo_root=repo_root),
        validate_package_origin("superjobs_contract_handlers", site_roots, repo_root=repo_root),
        validate_package_origin("superjobs_contract_worker_example", site_roots, repo_root=repo_root),
        validate_package_origin("superjobs_contract_worker_resources", site_roots, repo_root=repo_root),
    ]
    _assert_not_importable("superjobs_contract_cli_example")
    _assert_not_importable("typer")
    missing_extra = collect_core_without_typer(repo_root)

    from superjobs_contract_handlers import CONTRACT_CATALOG  # noqa: F401
    from superjobs_contract_worker_resources import WORKER_RESOURCE_TOKEN  # noqa: F401

    import superjobs  # noqa: F401

    if not WORKER_RESOURCE_TOKEN:
        raise OriginProbeError("worker resource token missing")

    return {
        "sys_version": sys.version,
        "sys_executable": sys.executable,
        "site_packages": [str(p) for p in site_roots],
        "packages": packages,
        "worker_resource_token": WORKER_RESOURCE_TOKEN,
        "core_without_typer": missing_extra,
    }


def collect_core_without_typer(repo_root: Path) -> dict[str, Any]:
    """Worker/core layout: superjobs without CLI extra must not require typer."""
    _assert_sys_path_clean(repo_root)
    import superjobs  # noqa: F401
    from superjobs_contract_example import CLI_GATE_JOB  # noqa: F401

    _assert_not_importable("typer")
    try:
        import superjobs.cli  # noqa: F401
    except ImportError as exc:
        if "optional 'cli' extra" not in str(exc):
            raise OriginProbeError(f"unexpected missing-extra diagnostic: {exc}") from exc
        return {"superjobs_cli": "missing_extra", "diagnostic": str(exc)}
    raise OriginProbeError("superjobs.cli importable without [cli] extra")


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("repo_root", type=Path)
    parser.add_argument("--role", choices=("cli", "worker", "core"), required=True)
    args = parser.parse_args(argv)
    try:
        if args.role == "cli":
            evidence = collect_cli_evidence(args.repo_root)
        elif args.role == "worker":
            evidence = collect_worker_evidence(args.repo_root)
        else:
            evidence = collect_core_without_typer(args.repo_root)
    except OriginProbeError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(json.dumps(evidence, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
