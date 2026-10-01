"""Stdlib-only checks that wheel installs resolve under isolated site-packages."""

from __future__ import annotations

import importlib.util
import json
import site
import sys
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse


class OriginProbeError(Exception):
    """Raised when an install origin or typing marker check fails."""


def site_package_roots() -> list[Path]:
    roots: list[Path] = []
    for entry in site.getsitepackages():
        roots.append(Path(entry).resolve())
    return roots


def py_typed_path_for_spec(spec: importlib.util.ModuleSpec) -> Path:
    if spec.submodule_search_locations:
        return Path(spec.submodule_search_locations[0]).resolve() / "py.typed"
    if not spec.origin or spec.origin == "namespace":
        raise OriginProbeError(f"cannot locate package directory for {spec.name!r}")
    return Path(spec.origin).resolve().parent / "py.typed"


def validate_package_origin(
    name: str,
    site_roots: list[Path],
    *,
    repo_root: Path | None,
) -> dict[str, str]:
    spec = importlib.util.find_spec(name)
    if spec is None or not spec.origin:
        raise OriginProbeError(f"missing import {name!r}")
    origin = Path(spec.origin).resolve()
    if not any(origin.is_relative_to(root) for root in site_roots):
        raise OriginProbeError(f"{name} not in site-packages: {origin}")
    if repo_root is not None and origin.is_relative_to(repo_root.resolve()):
        raise OriginProbeError(f"{name} resolves into repository source: {origin}")
    typed = py_typed_path_for_spec(spec)
    if not typed.is_file():
        raise OriginProbeError(f"missing py.typed for {name} at {typed}")
    return {"name": name, "origin": str(origin), "py_typed": str(typed)}


def _file_url_path(url: str) -> Path | None:
    parsed = urlparse(url)
    if parsed.scheme != "file":
        return None
    return Path(unquote(parsed.path)).resolve()


def validate_install_metadata(site_roots: list[Path], repo_root: Path) -> None:
    repo = repo_root.resolve()
    for root in site_roots:
        for direct in root.glob("*.dist-info/direct_url.json"):
            payload = json.loads(direct.read_text(encoding="utf-8"))
            dir_info = payload.get("dir_info") or {}
            if dir_info.get("editable"):
                raise OriginProbeError(f"editable install metadata present: {direct}")
            url = str(payload.get("url", ""))
            file_path = _file_url_path(url)
            if file_path is not None:
                if file_path.suffix != ".whl":
                    raise OriginProbeError(f"direct_url must reference a wheel file: {direct} -> {url}")
                if file_path.is_relative_to(repo):
                    if not file_path.match("*.whl"):
                        raise OriginProbeError(f"install points at repository tree: {direct} -> {url}")
            elif repo.as_posix() in url or str(repo) in url:
                raise OriginProbeError(f"install points at repository source: {direct} -> {url}")
        for pth in root.glob("*.pth"):
            text = pth.read_text(encoding="utf-8")
            if str(repo) in text:
                raise OriginProbeError(f"repository source on PYTHONPATH via .pth: {pth}")


def load_producer_module(producer_only: Path) -> None:
    if importlib.util.find_spec("worker_handlers") is not None:
        raise OriginProbeError("worker_handlers must not be importable in producer-only layout")
    sys.path.insert(0, str(producer_only))
    spec = importlib.util.spec_from_file_location("producer", producer_only / "producer.py")
    if spec is None or spec.loader is None:
        raise OriginProbeError("producer.py could not be loaded")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)


def load_worker_module(worker_only: Path) -> None:
    sys.path.insert(0, str(worker_only))
    spec = importlib.util.find_spec("worker_handlers")
    if spec is None:
        raise OriginProbeError("worker_handlers must be importable in worker layout")
    spec = importlib.util.spec_from_file_location("worker", worker_only / "worker_app.py")
    if spec is None or spec.loader is None:
        raise OriginProbeError("worker_app.py could not be loaded")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)


def collect_evidence(producer_only: Path, repo_root: Path) -> dict[str, Any]:
    site_roots = site_package_roots()
    validate_install_metadata(site_roots, repo_root)
    packages = [
        validate_package_origin("superjobs", site_roots, repo_root=repo_root),
        validate_package_origin("superjobs_contract_example", site_roots, repo_root=repo_root),
    ]
    load_producer_module(producer_only)
    return {
        "sys_version": sys.version,
        "sys_executable": sys.executable,
        "site_packages": [str(p) for p in site_roots],
        "packages": packages,
        "producer_only": str(producer_only.resolve()),
    }


def collect_worker_evidence(worker_only: Path, repo_root: Path) -> dict[str, Any]:
    site_roots = site_package_roots()
    validate_install_metadata(site_roots, repo_root)
    packages = [
        validate_package_origin("superjobs", site_roots, repo_root=repo_root),
        validate_package_origin("superjobs_contract_example", site_roots, repo_root=repo_root),
    ]
    load_worker_module(worker_only)
    return {
        "sys_version": sys.version,
        "sys_executable": sys.executable,
        "site_packages": [str(p) for p in site_roots],
        "packages": packages,
        "worker_only": str(worker_only.resolve()),
    }


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("role_dir", type=Path)
    parser.add_argument("repo_root", type=Path)
    parser.add_argument(
        "--role",
        choices=("producer", "worker"),
        default="producer",
        help="Which role layout to validate (default: producer).",
    )
    args = parser.parse_args(argv)
    try:
        if args.role == "worker":
            evidence = collect_worker_evidence(args.role_dir, args.repo_root)
        else:
            evidence = collect_evidence(args.role_dir, args.repo_root)
    except OriginProbeError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(json.dumps(evidence, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
