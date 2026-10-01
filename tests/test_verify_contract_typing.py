"""Deterministic checks for tools/verify_contract_typing.py (no full wheel gate by default)."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
RUNNER = REPO_ROOT / "tools" / "verify_contract_typing.py"

from tools import verify_contract_typing as vct  # noqa: E402
from tools.verify_contract_typing import (  # noqa: E402
    VerificationError,
    compare_negative,
    parse_expect_markers,
    run_cmd,
    validate_pyright_payload,
)
from tools.wheel_origin_probe import (  # noqa: E402
    OriginProbeError,
    py_typed_path_for_spec,
    validate_package_origin,
)


def test_parse_expect_marker_multirule() -> None:
    text = "x = 1  # expect: reportCallIssue, reportArgumentType\n"
    assert parse_expect_markers(text, "sample.py") == [
        ("sample.py", 1, "reportCallIssue"),
        ("sample.py", 1, "reportArgumentType"),
    ]


def test_compare_negative_rejects_unexpected() -> None:
    expected = [("a.py", 1, "reportArgumentType")]
    actual = [("a.py", 1, "reportCallIssue")]
    with pytest.raises(VerificationError, match="diagnostic mismatch"):
        compare_negative(expected, actual, [], suite="demo", mode="source")


def test_compare_negative_rejects_warnings() -> None:
    expected = [("a.py", 1, "reportArgumentType")]
    with pytest.raises(VerificationError, match="unexpected warnings"):
        compare_negative(
            expected,
            [],
            [("a.py", 1, "reportOptionalMemberAccess")],
            suite="demo",
            mode="wheel",
        )


def test_compare_negative_requires_expect_markers() -> None:
    with pytest.raises(VerificationError, match="no expect markers"):
        compare_negative([], [], [], suite="demo", mode="source")


def test_pyright_diagnostics_rejects_outside_suite_path() -> None:
    payload = {
        "version": "1.1.414",
        "summary": {"errorCount": 1, "warningCount": 0},
        "generalDiagnostics": [
            {
                "file": r"C:\tmp\other\check_types.py",
                "severity": "error",
                "rule": "reportMissingImports",
                "range": {"start": {"line": 0}},
            }
        ],
    }
    suite_dir = Path(r"C:\tmp\suite")
    with pytest.raises(VerificationError, match="outside suite"):
        validate_pyright_payload(payload, suite_dir)


def test_pyright_diagnostics_rejects_unruled() -> None:
    payload = {
        "version": "1.1.414",
        "summary": {"errorCount": 1, "warningCount": 0},
        "generalDiagnostics": [
            {
                "file": "check_types.py",
                "severity": "error",
                "range": {"start": {"line": 0}},
            }
        ],
    }
    suite_dir = Path.cwd()
    with pytest.raises(VerificationError, match="missing rule"):
        validate_pyright_payload(payload, suite_dir)


def test_run_cmd_rejects_exit_one() -> None:
    with pytest.raises(VerificationError, match="Command failed \\(1\\)"):
        run_cmd([sys.executable, "-c", "raise SystemExit(1)"])


def test_marker_helper_detects_missing_expect_control() -> None:
    sample = "x = 1  # reportArgumentType\n"
    with pytest.raises(VerificationError, match="legacy marker"):
        parse_expect_markers(sample, "legacy.py")


def test_marker_helper_rejects_empty_expect_list() -> None:
    with pytest.raises(VerificationError, match="empty expect marker"):
        parse_expect_markers("x = 1  # expect:  \n", "empty.py")


def test_marker_helper_rejects_invalid_rule_name() -> None:
    with pytest.raises(VerificationError, match="invalid expect rule"):
        parse_expect_markers("x = 1  # expect: notARealRule\n", "bad.py")


    with pytest.raises(VerificationError, match="missing generalDiagnostics"):
        validate_pyright_payload(
            {"version": "1.1.414", "summary": {"errorCount": 0, "warningCount": 0}},
            Path.cwd(),
        )


def test_validate_pyright_payload_requires_exact_version() -> None:
    payload = {
        "version": "1.1.414-extra",
        "summary": {"errorCount": 0, "warningCount": 0},
        "generalDiagnostics": [],
    }
    with pytest.raises(VerificationError, match="Unexpected Pyright version"):
        validate_pyright_payload(payload, Path.cwd())


def test_origin_validator_requires_py_typed_in_package_dir(tmp_path: Path) -> None:
    site_root = tmp_path / "site-packages"
    pkg = site_root / "demo_pkg"
    pkg.mkdir(parents=True)
    (pkg / "__init__.py").write_text("", encoding="utf-8")
    typed_parent = site_root / "py.typed"
    typed_parent.write_text("", encoding="utf-8")
    sys.path.insert(0, str(site_root))
    try:
        with pytest.raises(OriginProbeError, match="missing py.typed"):
            validate_package_origin("demo_pkg", [site_root.resolve()], repo_root=None)
    finally:
        sys.path.remove(str(site_root))


def test_origin_validator_accepts_package_level_marker(tmp_path: Path) -> None:
    site_root = tmp_path / "site-packages"
    pkg = site_root / "demo_pkg"
    pkg.mkdir(parents=True)
    (pkg / "__init__.py").write_text("", encoding="utf-8")
    (pkg / "py.typed").write_text("", encoding="utf-8")
    sys.path.insert(0, str(site_root))
    try:
        info = validate_package_origin("demo_pkg", [site_root.resolve()], repo_root=None)
        assert info["py_typed"].endswith("py.typed")
    finally:
        sys.path.remove(str(site_root))


def test_py_typed_path_for_spec_namespace(tmp_path: Path) -> None:
    import importlib.util

    pkg = tmp_path / "pkg"
    pkg.mkdir()
    (pkg / "__init__.py").write_text("", encoding="utf-8")
    spec = importlib.util.spec_from_file_location("pkg", pkg / "__init__.py", submodule_search_locations=[str(pkg)])
    assert spec is not None
    assert py_typed_path_for_spec(spec) == (pkg / "py.typed").resolve()


def _patch_negative_copy(monkeypatch: pytest.MonkeyPatch, transform: str) -> None:
    original = vct.copy_suite_tree

    def wrapped(suite: vct.TypingSuite, dest: Path) -> None:
        original(suite, dest)
        if suite.name != "negative":
            return
        path = dest / "check_types.py"
        text = path.read_text(encoding="utf-8")
        if transform == "drop_marker":
            text = text.replace("  # expect: reportArgumentType\n", "\n", 1)
        elif transform == "extra_error":
            text = "import not_a_real_module\n" + text
        elif transform == "valid_submit_keeps_marker":
            text = text.replace(
                'await client.submit("not a manifest request")  # expect: reportArgumentType',
                'await client.submit(ManifestRequest(device_id="probe"))  # expect: reportArgumentType',
                1,
            )
        else:
            raise ValueError(transform)
        path.write_text(text, encoding="utf-8")

    monkeypatch.setattr(vct, "copy_suite_tree", wrapped)


@pytest.mark.contract_typing
def test_runner_fails_when_negative_marker_removed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_negative_copy(monkeypatch, "drop_marker")
    venv = vct.create_dependency_venv(tmp_path, vct.STATIC_PYTHON_VERSION, name="venv-test")
    with pytest.raises(VerificationError, match="diagnostic mismatch"):
        vct.typing_pass(
            mode="source",
            work_dir=tmp_path,
            venv_dir=venv,
            evidence_dir=None,
        )


@pytest.mark.contract_typing
def test_runner_fails_on_unexpected_import_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_negative_copy(monkeypatch, "extra_error")
    venv = vct.create_dependency_venv(tmp_path, vct.STATIC_PYTHON_VERSION, name="venv-test")
    with pytest.raises(VerificationError, match="diagnostic mismatch"):
        vct.typing_pass(
            mode="source",
            work_dir=tmp_path,
            venv_dir=venv,
            evidence_dir=None,
        )


@pytest.mark.contract_typing
def test_runner_fails_when_valid_code_keeps_expect_marker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_negative_copy(monkeypatch, "valid_submit_keeps_marker")
    venv = vct.create_dependency_venv(tmp_path, vct.STATIC_PYTHON_VERSION, name="venv-test")
    with pytest.raises(VerificationError, match="diagnostic mismatch"):
        vct.typing_pass(
            mode="source",
            work_dir=tmp_path / "work",
            venv_dir=venv,
            evidence_dir=None,
        )


@pytest.mark.contract_typing
def test_wheel_origin_probe_rejects_missing_py_typed(tmp_path: Path) -> None:
    work = tmp_path / "work"
    work.mkdir()
    library, contract = vct.build_wheels(work)
    venv = vct.create_wheel_venv(work, "3.12", library, contract, name="venv-origin")
    py = vct.venv_python(venv)
    producer_only = tmp_path / "producer-only"
    producer_only.mkdir()
    shutil.copy2(vct.EXAMPLE_ROOT / "producer.py", producer_only / "producer.py")

    completed = subprocess.run(
        [
            str(py),
            "-c",
            "import superjobs; from pathlib import Path; print(Path(superjobs.__file__).resolve().parent / 'py.typed')",
        ],
        text=True,
        capture_output=True,
        check=True,
        env={**os.environ, "PYTHONNOUSERSITE": "1"},
    )
    typed = Path(completed.stdout.strip())
    assert typed.is_file()
    typed.unlink()

    with pytest.raises(VerificationError, match="missing py.typed"):
        vct.assert_wheel_origins(py, producer_only, None, python_version="3.12")


@pytest.mark.contract_typing
def test_contract_typing_runner_both_mode() -> None:
    completed = subprocess.run(
        [
            sys.executable,
            str(RUNNER),
            "--mode",
            "both",
            "--python",
            "3.12",
            "--python",
            "3.14",
        ],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        timeout=1800,
    )
    assert completed.returncode == 0, completed.stderr + completed.stdout
