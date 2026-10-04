#!/usr/bin/env python3
"""Verify contract-interface typing consumers in source and installed-wheel layouts."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Literal

PYRIGHT_VERSION = "1.1.414"
STATIC_PYTHON_VERSION = "3.12"
TYPE_CHECKING_MODE = "basic"
DEFAULT_RUNTIMES = ("3.12", "3.14")
SUBPROCESS_TIMEOUT = 600
PROJECT_DEPS = (
    "faststream[nats]>=0.7.4",
    "msgpack>=1.2.1",
    "pydantic>=2.13.4",
    "typer>=0.27.2",
)

REPO_ROOT = Path(__file__).resolve().parents[1]
EXAMPLE_ROOT = REPO_ROOT / "examples" / "contract_interface"
TYPING_ROOT = EXAMPLE_ROOT / "typing"
CONTRACT_SRC = EXAMPLE_ROOT / "superjobs_contract_example"
CLI_EXAMPLE_SRC = EXAMPLE_ROOT / "superjobs_contract_cli_example"
LIBRARY_SRC = REPO_ROOT / "src"
ORIGIN_PROBE = Path(__file__).resolve().parent / "wheel_origin_probe.py"

EXPECT_MARKER_RE = re.compile(r"#\s*expect:\s*([^#\n]+)", re.IGNORECASE)
LEGACY_MARKER_RE = re.compile(r"#\s*(report[A-Za-z]+)\s*$")
PYRIGHT_RULE_RE = re.compile(r"^report[A-Za-z]+$")

DiagnosticKey = tuple[str, int, str]


@dataclass(frozen=True)
class TypingSuite:
    name: str
    rel_dir: str
    include: tuple[str, ...]
    negative: bool


TYPING_SUITES: tuple[TypingSuite, ...] = (
    TypingSuite("positive", "positive", ("check_types.py", "producer.py", "worker_handlers.py"), False),
    TypingSuite("producer_positive", "producer_positive", ("check_types.py",), False),
    TypingSuite("negative", "negative", ("check_types.py",), True),
    TypingSuite("producer_negative", "producer_negative", ("check_types.py",), True),
    TypingSuite("strict_payload_positive", "strict_payload_positive", ("check_types.py",), False),
    TypingSuite("strict_payload_negative", "strict_payload_negative", ("check_types.py",), True),
    TypingSuite("cli_positive", "cli_positive", (
        "check_types.py", "cli_registration_example.py", "cli_application.py",
        "cli_application_main.py", "cli_application_sync.py", "cli_worker_handlers.py",
        "cli_observe_execution.py",
    ), False),
    TypingSuite("cli_negative", "cli_negative", ("check_types.py",), True),
)

RUNTIME_TEST_FILES = (
    REPO_ROOT / "tests" / "test_handler_registration.py",
    REPO_ROOT / "tests" / "test_public_api.py",
    REPO_ROOT / "tests" / "test_cli_public.py",
    REPO_ROOT / "tests" / "test_cli_input.py",
    REPO_ROOT / "tests" / "test_cli_input_contracts.py",
    REPO_ROOT / "tests" / "test_cli_local_run.py",
    REPO_ROOT / "tests" / "test_cli_remote_submit.py",
    REPO_ROOT / "tests" / "test_cli_remote_contracts.py",
    REPO_ROOT / "tests" / "test_contract_cli_example_public.py",
)


class VerificationError(Exception):
    """Raised when contract typing verification fails."""


def run_cmd(
    args: list[str],
    *,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
    timeout: int = SUBPROCESS_TIMEOUT,
    allowed_codes: tuple[int, ...] = (0,),
) -> subprocess.CompletedProcess[str]:
    merged = os.environ.copy()
    if env:
        merged.update(env)
    for key in ("PYTHONPATH", "PYTHONHOME"):
        merged.pop(key, None)
    try:
        completed = subprocess.run(
            args,
            cwd=cwd,
            env=merged,
            text=True,
            capture_output=True,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise VerificationError(f"Command timed out after {timeout}s: {args!r}") from exc
    if completed.returncode not in allowed_codes:
        label = "Pyright command failed" if allowed_codes == (0, 1) else "Command failed"
        raise VerificationError(
            f"{label} ({completed.returncode}): {args!r}\n"
            f"stdout:\n{completed.stdout}\nstderr:\n{completed.stderr}"
        )
    return completed


def assert_final_runtime_python(python: Path) -> str:
    completed = run_cmd(
        [
            str(python),
            "-c",
            "import sys; assert sys.version_info.releaselevel == 'final', sys.version_info; print(sys.version)",
        ],
        env={"PYTHONNOUSERSITE": "1"},
    )
    return completed.stdout.strip()


def parse_expect_markers(text: str, rel_file: str) -> list[DiagnosticKey]:
    expected: list[DiagnosticKey] = []
    for line_no, line in enumerate(text.splitlines(), start=1):
        if "expect:" in line.lower():
            match = EXPECT_MARKER_RE.search(line)
            if not match:
                raise VerificationError(
                    f"{rel_file}:{line_no} has malformed expect marker (need '# expect: <rule>')."
                )
            rules = [part.strip() for part in match.group(1).split(",") if part.strip()]
            if not rules:
                raise VerificationError(f"{rel_file}:{line_no} has empty expect marker.")
            for rule in rules:
                if not PYRIGHT_RULE_RE.match(rule):
                    raise VerificationError(
                        f"{rel_file}:{line_no} has invalid expect rule {rule!r} "
                        "(expected reportRuleName)."
                    )
                expected.append((rel_file, line_no, rule))
            continue
        legacy = LEGACY_MARKER_RE.search(line)
        if legacy:
            raise VerificationError(
                f"{rel_file}:{line_no} uses legacy marker {legacy.group(1)!r}; "
                "use '# expect: <rule>' instead."
            )
    return expected


def load_suite_expectations(suite_dir: Path, includes: Iterable[str]) -> list[DiagnosticKey]:
    expected: list[DiagnosticKey] = []
    for name in includes:
        path = suite_dir / name
        if not path.is_file():
            continue
        expected.extend(parse_expect_markers(path.read_text(encoding="utf-8"), name))
    return expected


def normalize_pyright_file(path: str, suite_dir: Path) -> str:
    resolved = Path(path).resolve()
    suite_resolved = suite_dir.resolve()
    try:
        return resolved.relative_to(suite_resolved).as_posix()
    except ValueError:
        raise VerificationError(
            f"Diagnostic path {path!r} is outside suite directory {suite_dir}"
        ) from None


def _parse_diagnostic_line(item: dict[str, Any]) -> int:
    range_obj = item.get("range")
    if not isinstance(range_obj, dict):
        raise VerificationError(f"Diagnostic missing range: {item!r}")
    start = range_obj.get("start")
    if not isinstance(start, dict) or "line" not in start:
        raise VerificationError(f"Diagnostic missing range.start.line: {item!r}")
    return int(start["line"]) + 1


def pyright_diagnostics(payload: dict[str, Any], suite_dir: Path) -> tuple[list[DiagnosticKey], list[DiagnosticKey]]:
    errors: list[DiagnosticKey] = []
    warnings: list[DiagnosticKey] = []
    for item in payload.get("generalDiagnostics", []):
        if not isinstance(item, dict):
            raise VerificationError(f"Malformed diagnostic entry: {item!r}")
        severity = item.get("severity")
        if severity not in ("error", "warning"):
            raise VerificationError(f"Diagnostic has invalid severity: {item!r}")
        rule = item.get("rule")
        if not rule or not isinstance(rule, str):
            raise VerificationError(f"Diagnostic missing rule: {item!r}")
        if not PYRIGHT_RULE_RE.match(rule):
            raise VerificationError(f"Diagnostic has invalid rule {rule!r}: {item!r}")
        if "file" not in item:
            raise VerificationError(f"Diagnostic missing file: {item!r}")
        rel = normalize_pyright_file(item["file"], suite_dir)
        line = _parse_diagnostic_line(item)
        key = (rel, line, rule)
        if severity == "error":
            errors.append(key)
        else:
            warnings.append(key)
    return errors, warnings


def validate_pyright_payload(payload: dict[str, Any], suite_dir: Path) -> tuple[list[DiagnosticKey], list[DiagnosticKey]]:
    if not isinstance(payload, dict):
        raise VerificationError(f"Pyright JSON must be an object, got {type(payload).__name__}.")
    general = payload.get("generalDiagnostics")
    if general is None:
        raise VerificationError("Pyright JSON missing generalDiagnostics list.")
    if not isinstance(general, list):
        raise VerificationError(
            f"Pyright generalDiagnostics must be a list, got {type(general).__name__}."
        )
    version = payload.get("version")
    if version != PYRIGHT_VERSION:
        raise VerificationError(
            f"Unexpected Pyright version {version!r} (wanted exact {PYRIGHT_VERSION})."
        )
    summary = payload.get("summary")
    if not isinstance(summary, dict):
        raise VerificationError("Pyright JSON missing summary object.")
    errors, warnings = pyright_diagnostics(payload, suite_dir)
    error_count = summary.get("errorCount")
    warning_count = summary.get("warningCount")
    if error_count is None or warning_count is None:
        raise VerificationError(f"Pyright summary missing counts: {summary!r}")
    if int(error_count) != len(errors):
        raise VerificationError(
            f"Pyright summary errorCount={error_count} does not match "
            f"parsed errors ({len(errors)})."
        )
    if int(warning_count) != len(warnings):
        raise VerificationError(
            f"Pyright summary warningCount={warning_count} does not match "
            f"parsed warnings ({len(warnings)})."
        )
    return errors, warnings


def compare_negative(
    expected: list[DiagnosticKey],
    actual_errors: list[DiagnosticKey],
    actual_warnings: list[DiagnosticKey],
    *,
    suite: str,
    mode: str,
) -> None:
    if not expected:
        raise VerificationError(f"{suite} ({mode}): negative suite has no expect markers.")
    if actual_warnings:
        raise VerificationError(
            f"{suite} ({mode}): unexpected warnings: {sorted(actual_warnings)}"
        )
    exp_counter = Counter(expected)
    act_counter = Counter(actual_errors)
    if exp_counter != act_counter:
        missing = exp_counter - act_counter
        unexpected = act_counter - exp_counter
        raise VerificationError(
            f"{suite} ({mode}): diagnostic mismatch.\n"
            f"  missing: {sorted(missing.elements())}\n"
            f"  unexpected: {sorted(unexpected.elements())}"
        )


def compare_positive(
    actual_errors: list[DiagnosticKey],
    actual_warnings: list[DiagnosticKey],
    *,
    suite: str,
    mode: str,
) -> None:
    if actual_errors or actual_warnings:
        raise VerificationError(
            f"{suite} ({mode}): expected zero diagnostics, got "
            f"errors={sorted(actual_errors)} warnings={sorted(actual_warnings)}"
        )


def run_pyright(project_dir: Path) -> dict[str, Any]:
    args = [
        "uv",
        "tool",
        "run",
        "--from",
        f"pyright=={PYRIGHT_VERSION}",
        "pyright",
        "--outputjson",
        f"--project={project_dir}",
    ]
    completed = run_cmd(args, cwd=project_dir, allowed_codes=(0, 1))
    try:
        payload = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise VerificationError(
            f"Invalid Pyright JSON from {project_dir}.\nstdout:\n{completed.stdout}\n"
            f"stderr:\n{completed.stderr}"
        ) from exc
    errors, warnings = validate_pyright_payload(payload, project_dir)
    if completed.returncode == 1 and not errors:
        raise VerificationError(
            f"Pyright exited 1 without ruled errors in {project_dir}.\n"
            f"stdout:\n{completed.stdout}\nstderr:\n{completed.stderr}"
        )
    if completed.returncode == 0 and errors:
        raise VerificationError(
            f"Pyright exited 0 but reported errors in {project_dir}: {sorted(errors)}"
        )
    return payload


def write_pyrightconfig(
    target: Path,
    *,
    include: tuple[str, ...],
    mode: Literal["source", "wheel"],
    venv_path: Path,
    venv_name: str,
) -> None:
    config: dict[str, Any] = {
        "include": list(include),
        "pythonVersion": STATIC_PYTHON_VERSION,
        "typeCheckingMode": TYPE_CHECKING_MODE,
        "venvPath": str(venv_path),
        "venv": venv_name,
        "autoSearchPaths": False,
    }
    if mode == "source":
        config["extraPaths"] = [
            str(LIBRARY_SRC),
            str(CONTRACT_SRC / "src"),
            str(CLI_EXAMPLE_SRC / "src"),
        ]
    target.write_text(json.dumps(config, indent=2) + "\n", encoding="utf-8")


def copy_suite_tree(suite: TypingSuite, dest: Path) -> None:
    src = TYPING_ROOT / suite.rel_dir
    shutil.copytree(src, dest, dirs_exist_ok=True)
    if "producer.py" in suite.include:
        shutil.copy2(EXAMPLE_ROOT / "producer.py", dest / "producer.py")
    if "worker_handlers.py" in suite.include:
        shutil.copy2(EXAMPLE_ROOT / "worker_handlers.py", dest / "worker_handlers.py")
    if suite.name == "cli_positive":
        shutil.copy2(REPO_ROOT / "examples/cli_registration/main.py", dest / "cli_registration_example.py")
        for source, name in (
            (CLI_EXAMPLE_SRC / "src/superjobs_contract_cli_example/main.py", "cli_application_main.py"),
            (CLI_EXAMPLE_SRC / "src/superjobs_contract_cli_example/sync.py", "cli_application_sync.py"),
            (EXAMPLE_ROOT / "superjobs_contract_worker_example/src/superjobs_contract_worker_example/handlers.py", "cli_worker_handlers.py"),
            (REPO_ROOT / "tools/cli_process_support/observe_execution.py", "cli_observe_execution.py"),
        ):
            shutil.copy2(source, dest / name)


def strip_contract_sources(pyproject: Path) -> None:
    text = pyproject.read_text(encoding="utf-8")
    if "[tool.uv.sources]" not in text:
        return
    lines = text.splitlines()
    out: list[str] = []
    skip = False
    for line in lines:
        if line.strip() == "[tool.uv.sources]":
            skip = True
            continue
        if skip:
            if line.startswith("[") and not line.startswith("[tool.uv.sources"):
                skip = False
                out.append(line)
            continue
        out.append(line)
    pyproject.write_text("\n".join(out).rstrip() + "\n", encoding="utf-8")


def build_wheels(work_dir: Path) -> tuple[Path, Path]:
    dist = work_dir / "dist"
    dist.mkdir(parents=True, exist_ok=True)
    run_cmd(["uv", "build", "--project", str(REPO_ROOT), "--out-dir", str(dist)])
    contract_build = work_dir / "contract_pkg"
    shutil.copytree(
        CONTRACT_SRC,
        contract_build,
        dirs_exist_ok=True,
        ignore=shutil.ignore_patterns(".venv", "__pycache__"),
    )
    strip_contract_sources(contract_build / "pyproject.toml")
    run_cmd(["uv", "build", "--project", str(contract_build), "--out-dir", str(dist)])
    wheels = sorted(dist.glob("*.whl"))
    library = next((w for w in wheels if w.name.startswith("superjobs-")), None)
    contract = next((w for w in wheels if "contract" in w.name), None)
    if library is None or contract is None:
        raise VerificationError(f"Could not find built wheels in {dist}: {[w.name for w in wheels]}")
    return library, contract


def build_cli_example_wheel(work_dir: Path) -> Path:
    dist = work_dir / "dist"
    dist.mkdir(parents=True, exist_ok=True)
    cli_build = work_dir / "cli_pkg"
    shutil.copytree(
        CLI_EXAMPLE_SRC,
        cli_build,
        dirs_exist_ok=True,
        ignore=shutil.ignore_patterns(".venv", "__pycache__"),
    )
    strip_contract_sources(cli_build / "pyproject.toml")
    run_cmd(["uv", "build", "--project", str(cli_build), "--out-dir", str(dist)])
    wheels = sorted(dist.glob("*.whl"))
    cli_wheel = next((w for w in wheels if w.name.startswith("superjobs_contract_cli")), None)
    if cli_wheel is None:
        raise VerificationError(f"Could not find CLI example wheel in {dist}: {[w.name for w in wheels]}")
    return cli_wheel


def venv_python(venv_dir: Path) -> Path:
    if sys.platform == "win32":
        return venv_dir / "Scripts" / "python.exe"
    return venv_dir / "bin" / "python"


def create_dependency_venv(work_dir: Path, python: str, *, name: str) -> Path:
    venv_dir = work_dir / name
    if venv_dir.exists():
        shutil.rmtree(venv_dir)
    run_cmd(["uv", "venv", "--python", python, str(venv_dir)])
    py = venv_python(venv_dir)
    assert_final_runtime_python(py)
    run_cmd(
        ["uv", "pip", "install", "--python", str(py), *PROJECT_DEPS, "pytest>=9.1.1", "pytest-asyncio>=1.4.0"],
        env={"PYTHONNOUSERSITE": "1"},
    )
    return venv_dir


def create_wheel_venv(
    work_dir: Path,
    python: str,
    library: Path,
    contract: Path,
    *,
    cli_example: Path | None = None,
    name: str | None = None,
) -> Path:
    venv_dir = work_dir / (name or f"venv-py{python.replace('.', '')}")
    if venv_dir.exists():
        shutil.rmtree(venv_dir)
    run_cmd(["uv", "venv", "--python", python, str(venv_dir)])
    py = venv_python(venv_dir)
    assert_final_runtime_python(py)
    install = [
        "uv",
        "pip",
        "install",
        "--python",
        str(py),
        f"{library}[cli]",
        str(contract),
    ]
    if cli_example is not None:
        install.append(str(cli_example))
    install.extend(["pytest>=9.1.1", "pytest-asyncio>=1.4.0"])
    run_cmd(install, env={"PYTHONNOUSERSITE": "1"})
    return venv_dir


def runtime_evidence_tag(python_version: str) -> str:
    return f"py{python_version.replace('.', '')}"


def assert_wheel_origins(
    python: Path,
    producer_only_dir: Path,
    evidence_dir: Path | None,
    *,
    python_version: str,
) -> dict[str, Any]:
    shutil.copy2(ORIGIN_PROBE, producer_only_dir / "wheel_origin_probe.py")
    try:
        completed = run_cmd(
            [
                str(python),
                str(producer_only_dir / "wheel_origin_probe.py"),
                str(producer_only_dir),
                str(REPO_ROOT),
            ],
            cwd=producer_only_dir,
            env={"PYTHONNOUSERSITE": "1"},
        )
    except VerificationError as exc:
        raise VerificationError(f"Wheel origin probe failed.\n{exc}") from exc
    try:
        evidence = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise VerificationError(
            f"Wheel origin probe returned invalid JSON.\nstdout:\n{completed.stdout}\n"
            f"stderr:\n{completed.stderr}"
        ) from exc
    if evidence_dir is not None:
        evidence_dir.mkdir(parents=True, exist_ok=True)
        tag = runtime_evidence_tag(python_version)
        origin_record = {
            "python_version": python_version,
            "interpreter": str(python),
            "probe": evidence,
        }
        (evidence_dir / f"{tag}-origin.json").write_text(
            json.dumps(origin_record, indent=2),
            encoding="utf-8",
        )
    return evidence


_PYTEST_SUMMARY_RE = re.compile(
    r"(?P<passed>\d+) passed(?:,\s*(?P<failed>\d+) failed)?(?:,\s*(?P<skipped>\d+) skipped)?"
)


def _parse_pytest_summary(stdout: str) -> dict[str, int | None]:
    for line in reversed(stdout.splitlines()):
        match = _PYTEST_SUMMARY_RE.search(line.strip())
        if match:
            return {
                "passed": int(match.group("passed")),
                "failed": int(match.group("failed") or 0),
                "skipped": int(match.group("skipped") or 0),
            }
    return {"passed": None, "failed": None, "skipped": None}


def run_runtime_tests(
    work_dir: Path,
    venv_dir: Path,
    python_version: str,
    evidence_dir: Path | None = None,
) -> dict[str, Any]:
    work_dir.mkdir(parents=True, exist_ok=True)
    py = venv_python(venv_dir)
    runtime_version = assert_final_runtime_python(py)
    runtime_dir = work_dir / "runtime_tests"
    if runtime_dir.exists():
        shutil.rmtree(runtime_dir)
    runtime_dir.mkdir(parents=True)
    (runtime_dir / "pytest.ini").write_text(
        "[pytest]\nmarkers =\n    nats: tests requiring the owned NATS harness\n",
        encoding="utf-8",
    )
    for src in RUNTIME_TEST_FILES:
        shutil.copy2(src, runtime_dir / src.name)
    shutil.copy2(REPO_ROOT / "examples/cli_registration/main.py", runtime_dir / "cli_registration_example.py")
    merged = os.environ.copy()
    merged.pop("PYTHONPATH", None)
    merged.pop("PYTHONHOME", None)
    merged["PYTHONNOUSERSITE"] = "1"
    try:
        completed = subprocess.run(
            [str(py), "-m", "pytest", "-q", "-m", "not nats", str(runtime_dir)],
            cwd=work_dir,
            env=merged,
            text=True,
            capture_output=True,
            timeout=SUBPROCESS_TIMEOUT,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise VerificationError(
            f"Runtime tests timed out for Python {python_version} ({runtime_version})."
        ) from exc
    summary = _parse_pytest_summary(completed.stdout)
    record: dict[str, Any] = {
        "python_version": python_version,
        "runtime_version": runtime_version,
        "interpreter": str(py),
        "exit_code": completed.returncode,
        "pytest": summary,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
    }
    if evidence_dir is not None:
        evidence_dir.mkdir(parents=True, exist_ok=True)
        tag = runtime_evidence_tag(python_version)
        (evidence_dir / f"{tag}-runtime.json").write_text(
            json.dumps(record, indent=2),
            encoding="utf-8",
        )
    if completed.returncode != 0:
        raise VerificationError(
            f"Runtime tests failed for Python {python_version} ({runtime_version}).\n"
            f"exit_code={completed.returncode}\nstdout:\n{completed.stdout}\n"
            f"stderr:\n{completed.stderr}"
        )
    return record


def typing_pass(
    *,
    mode: Literal["source", "wheel"],
    work_dir: Path,
    venv_dir: Path,
    evidence_dir: Path | None,
) -> dict[str, list[DiagnosticKey]]:
    results: dict[str, list[DiagnosticKey]] = {}
    typing_root = work_dir / f"typing-{mode}"
    if typing_root.exists():
        shutil.rmtree(typing_root)
    typing_root.mkdir(parents=True)
    venv_parent = venv_dir.parent
    venv_name = venv_dir.name
    py = venv_python(venv_dir)
    runtime_version = assert_final_runtime_python(py)
    for suite in TYPING_SUITES:
        suite_dir = typing_root / suite.name
        copy_suite_tree(suite, suite_dir)
        write_pyrightconfig(
            suite_dir / "pyrightconfig.json",
            include=suite.include,
            mode=mode,
            venv_path=venv_parent,
            venv_name=venv_name,
        )
        payload = run_pyright(suite_dir)
        if evidence_dir is not None:
            evidence_dir.mkdir(parents=True, exist_ok=True)
            (evidence_dir / f"{suite.name}-{mode}-pyright.json").write_text(
                json.dumps(payload, indent=2),
                encoding="utf-8",
            )
        errors, warnings = validate_pyright_payload(payload, suite_dir)
        if suite.negative:
            expected = load_suite_expectations(suite_dir, suite.include)
            compare_negative(expected, errors, warnings, suite=suite.name, mode=mode)
        else:
            compare_positive(errors, warnings, suite=suite.name, mode=mode)
        results[suite.name] = sorted(errors)
        if evidence_dir is not None:
            meta = {
                "mode": mode,
                "suite": suite.name,
                "pyright": PYRIGHT_VERSION,
                "python_static": STATIC_PYTHON_VERSION,
                "venv": str(venv_dir),
                "runtime_python": runtime_version,
                "runtime_executable": str(py),
                "errors": results[suite.name],
            }
            (evidence_dir / f"{suite.name}-{mode}-meta.json").write_text(
                json.dumps(meta, indent=2),
                encoding="utf-8",
            )
    return results


def assert_matching_modes(source: dict[str, list[DiagnosticKey]], wheel: dict[str, list[DiagnosticKey]]) -> None:
    for suite in TYPING_SUITES:
        if not suite.negative:
            continue
        if source.get(suite.name) != wheel.get(suite.name):
            raise VerificationError(
                f"{suite.name}: source/wheel negative diagnostics differ.\n"
                f"  source: {source.get(suite.name)}\n"
                f"  wheel:  {wheel.get(suite.name)}"
            )


def resolve_source_venv(explicit: Path | None, work_dir: Path, python: str) -> Path:
    if explicit is not None:
        py = venv_python(explicit)
        if not py.is_file():
            raise VerificationError(f"--source-venv interpreter missing: {py}")
        assert_final_runtime_python(py)
        return explicit
    return create_dependency_venv(work_dir, python, name="venv-source-deps")


def assert_work_parent_outside_repo(parent: Path) -> None:
    resolved_parent = parent.resolve()
    repo = REPO_ROOT.resolve()
    try:
        resolved_parent.relative_to(repo)
    except ValueError:
        return
    raise VerificationError(
        f"Work directory parent {resolved_parent} must be outside the repository ({repo})."
    )


def collect_python_versions(args: argparse.Namespace) -> tuple[str, ...]:
    versions: list[str] = list(args.python_versions or [])
    if not versions:
        return DEFAULT_RUNTIMES
    return tuple(dict.fromkeys(versions))


def format_suite_summary(label: str, results: dict[str, list[DiagnosticKey]]) -> str:
    parts: list[str] = []
    for suite in TYPING_SUITES:
        errors = results.get(suite.name, [])
        if suite.negative:
            parts.append(f"{suite.name}={len(errors)} ruled errors")
        else:
            parts.append(f"{suite.name}={'OK' if not errors else f'{len(errors)} errors'}")
    return f"{label}: " + ", ".join(parts)


def format_matching_status(
    source: dict[str, list[DiagnosticKey]] | None,
    wheel: dict[str, list[DiagnosticKey]] | None,
) -> str:
    if source is None or wheel is None:
        return "source/wheel negative matching: n/a"
    mismatches: list[str] = []
    for suite in TYPING_SUITES:
        if not suite.negative:
            continue
        if source.get(suite.name) != wheel.get(suite.name):
            mismatches.append(suite.name)
    if mismatches:
        return f"source/wheel negative matching: MISMATCH ({', '.join(mismatches)})"
    return "source/wheel negative matching: OK"


def print_run_summary(
    *,
    python_versions: tuple[str, ...],
    source_results: dict[str, list[DiagnosticKey]] | None,
    wheel_results: dict[str, list[DiagnosticKey]] | None,
    runtime_records: dict[str, dict[str, Any]],
) -> None:
    if source_results is not None:
        print(format_suite_summary("Source typing", source_results))
    if wheel_results is not None:
        print(format_suite_summary("Wheel typing", wheel_results))
    print(format_matching_status(source_results, wheel_results))
    for py_version in python_versions:
        tag = runtime_evidence_tag(py_version)
        record = runtime_records.get(py_version)
        if record is None:
            print(f"Runtime {tag}: (not run)")
            continue
        pytest_info = record.get("pytest") or {}
        passed = pytest_info.get("passed")
        count_text = f"{passed} passed" if passed is not None else "counts unknown"
        print(
            f"Runtime {tag}: origin OK, pytest {count_text}, "
            f"exit {record.get('exit_code')} ({record.get('runtime_version')})"
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode",
        choices=("source", "wheel", "both"),
        default="both",
        help="source uses repo src extraPaths; wheel uses installed distributions only.",
    )
    parser.add_argument(
        "--python",
        action="append",
        dest="python_versions",
        help=f"Python runtime for wheel installs (default: {', '.join(DEFAULT_RUNTIMES)}).",
    )
    parser.add_argument(
        "--source-venv",
        type=Path,
        help="Existing virtual environment for source-mode third-party dependencies.",
    )
    parser.add_argument(
        "--evidence",
        type=Path,
        help="Directory to store Pyright JSON, origin probe output, and metadata.",
    )
    parser.add_argument(
        "--keep-work",
        action="store_true",
        help="Retain the temporary work directory after a successful run.",
    )
    parser.add_argument(
        "--work-dir",
        type=Path,
        help="Parent directory for temporary verification files (must be outside the repo).",
    )
    args = parser.parse_args(argv)
    python_versions = collect_python_versions(args)
    parent = args.work_dir or Path(tempfile.gettempdir())
    assert_work_parent_outside_repo(parent)
    work_dir = Path(tempfile.mkdtemp(prefix="superjobs-contract-typing-", dir=parent))
    evidence_dir = args.evidence
    if evidence_dir is not None:
        evidence_dir.mkdir(parents=True, exist_ok=True)

    try:
        source_results: dict[str, list[DiagnosticKey]] | None = None
        wheel_results: dict[str, list[DiagnosticKey]] | None = None
        runtime_records: dict[str, dict[str, Any]] = {}
        shared_typing_venv: Path | None = None
        library: Path | None = None
        contract: Path | None = None

        if args.mode in ("wheel", "both"):
            library, contract = build_wheels(work_dir)
            cli_example = build_cli_example_wheel(work_dir)
            shared_typing_venv = create_wheel_venv(
                work_dir,
                STATIC_PYTHON_VERSION,
                library,
                contract,
                cli_example=cli_example,
                name="venv-typing-shared",
            )

        if args.mode in ("source", "both"):
            if args.mode == "both":
                assert shared_typing_venv is not None
                source_venv = shared_typing_venv
            else:
                source_venv = resolve_source_venv(args.source_venv, work_dir, STATIC_PYTHON_VERSION)
            source_results = typing_pass(
                mode="source",
                work_dir=work_dir,
                venv_dir=source_venv,
                evidence_dir=evidence_dir,
            )

        if args.mode in ("wheel", "both"):
            assert library is not None and contract is not None
            assert shared_typing_venv is not None
            for py_version in python_versions:
                if py_version == STATIC_PYTHON_VERSION:
                    venv_dir = shared_typing_venv
                else:
                    venv_dir = create_wheel_venv(
                        work_dir,
                        py_version,
                        library,
                        contract,
                        cli_example=cli_example,
                    )
                py = venv_python(venv_dir)
                producer_only = work_dir / f"producer-only-py{py_version.replace('.', '')}"
                producer_only.mkdir(parents=True, exist_ok=True)
                shutil.copy2(EXAMPLE_ROOT / "producer.py", producer_only / "producer.py")
                assert_wheel_origins(
                    py, producer_only, evidence_dir, python_version=py_version
                )
                runtime_records[py_version] = run_runtime_tests(
                    work_dir / f"runtime-{py_version.replace('.', '')}",
                    venv_dir,
                    py_version,
                    evidence_dir,
                )
            wheel_results = typing_pass(
                mode="wheel",
                work_dir=work_dir,
                venv_dir=shared_typing_venv,
                evidence_dir=evidence_dir,
            )

        if source_results is not None and wheel_results is not None:
            assert_matching_modes(source_results, wheel_results)
    except VerificationError as exc:
        print(f"verify_contract_typing: {exc}", file=sys.stderr)
        if evidence_dir is not None:
            print(f"Evidence directory: {evidence_dir}", file=sys.stderr)
        if args.keep_work:
            print(f"Work directory (inspection): {work_dir}", file=sys.stderr)
        return 1
    finally:
        if not args.keep_work:
            shutil.rmtree(work_dir, ignore_errors=True)

    print("verify_contract_typing: OK")
    print_run_summary(
        python_versions=python_versions,
        source_results=source_results,
        wheel_results=wheel_results,
        runtime_records=runtime_records,
    )
    if evidence_dir is not None:
        print(f"Evidence written to {evidence_dir}")
    if args.keep_work:
        print(f"Work directory preserved at {work_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
