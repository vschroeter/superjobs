"""Deterministic checks for tools/verify_reliability_repetition.py."""

from __future__ import annotations

import json
import signal
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]

from scripts.dev_check.constants import INTEGRATION_JOB_BUDGET_SECONDS  # noqa: E402
from scripts.dev_check.process_tree import StageProcessResult  # noqa: E402
from tools import verify_worker_recovery as vwr  # noqa: E402
from tools.broker_restart_support import protocol as br_protocol  # noqa: E402
from tools.reliability_repetition_support import evidence as rre  # noqa: E402
from tools.reliability_repetition_support import plan as rrp  # noqa: E402
from tools.verify_contract_typing import runtime_evidence_tag  # noqa: E402
from tools.verify_cross_program import SCENARIOS  # noqa: E402
from tools.verify_worker_recovery import SCENARIO_AFTER, SCENARIO_BEFORE  # noqa: E402
from tools.worker_recovery_support import protocol as wr_protocol  # noqa: E402
from tools import verify_reliability_repetition as vrr  # noqa: E402
from tools.verify_broker_restart import SCENARIO_NAME as BROKER_SCENARIO  # noqa: E402


def test_planned_items_cover_four_families_three_times() -> None:
    items = rrp.planned_items()
    assert len(items) == 12
    for family in (
        rrp.FAMILY_CROSS_PROGRAM,
        rrp.FAMILY_RECOVERY_BEFORE_COMPLETION,
        rrp.FAMILY_RECOVERY_AFTER_COMPLETION,
        rrp.FAMILY_BROKER_RESTART,
    ):
        reps = [item.repetition for item in items if item.family == family]
        assert reps == [1, 2, 3]


def test_shuffle_items_is_seed_deterministic_and_reorders() -> None:
    base = rrp.planned_items()
    first = rrp.shuffle_items(42, base)
    second = rrp.shuffle_items(42, base)
    assert first == second
    assert [item.family for item in first] != [item.family for item in base]


def test_build_child_argv_worker_recovery_uses_only_scenario() -> None:
    before = rrp.build_child_argv(
        repo_root=REPO_ROOT,
        python_version="3.14",
        family=rrp.FAMILY_RECOVERY_BEFORE_COMPLETION,
        artifact_dir=Path("/tmp/art"),
    )
    assert before[-2:] == ["--only-scenario", SCENARIO_BEFORE]
    after = rrp.build_child_argv(
        repo_root=REPO_ROOT,
        python_version="3.14",
        family=rrp.FAMILY_RECOVERY_AFTER_COMPLETION,
        artifact_dir=Path("/tmp/art"),
    )
    assert after[-2:] == ["--only-scenario", SCENARIO_AFTER]


def test_resolve_recovery_scenarios_default_pair_and_selectors() -> None:
    assert vwr.resolve_recovery_scenarios(None, ()) == vwr.RECOVERY_SCENARIOS
    assert vwr.resolve_recovery_scenarios((SCENARIO_BEFORE,), ()) == (SCENARIO_BEFORE,)
    assert vwr.resolve_recovery_scenarios(
        (SCENARIO_AFTER,),
        (vwr.SCENARIO_AFTER_RETRY,),
    ) == (SCENARIO_AFTER, vwr.SCENARIO_AFTER_RETRY)


def test_collect_only_scenarios_defaults_and_validation() -> None:
    assert vwr.collect_only_scenarios(SimpleNamespace(only_scenarios=None)) is None
    assert vwr.collect_only_scenarios(
        SimpleNamespace(only_scenarios=[SCENARIO_BEFORE]),
    ) == (SCENARIO_BEFORE,)
    with pytest.raises(vwr.WorkerRecoveryError, match="unsupported"):
        vwr.collect_only_scenarios(
            SimpleNamespace(only_scenarios=["recovery_after_retry_publication"]),
        )


def _pass_runtime_metadata(python_version: str = "3.14") -> dict:
    return {
        "python_version": python_version,
        "origins": {
            "producer": {"name": "superjobs", "origin": "/site/superjobs"},
            "worker": {"name": "superjobs", "origin": "/site/superjobs"},
        },
    }


def _child_record(name: str, exit_code: int, *, kill: bool = False) -> dict:
    payload = {
        "name": name,
        "exit_code": exit_code,
        "ended_at_monotonic": 10.0,
        "started_at_monotonic": 1.0,
    }
    if kill:
        payload["termination"] = {"launcher_exit": exit_code}
    return payload


def _cross_program_scenario(spec_name: str) -> dict:
    spec = next(item for item in SCENARIOS if item.name == spec_name)
    worker = None
    if spec.start_worker:
        worker = _child_record("worker", spec.expected_worker_code or 0)
    producer = None
    if spec.start_producer:
        producer = _child_record("producer", spec.expected_producer_code or 0)
    return {
        "name": spec.name,
        "error": None,
        "ended_at_monotonic": 5.0,
        "checkpoints": {"checkpoint_submitted.json": {}},
        "producer_exit": spec.expected_producer_code,
        "worker_exit": spec.expected_worker_code if spec.expected_worker_code is not None else (0 if spec.start_worker else None),
        "producer": producer,
        "worker": worker,
    }


def _write_cross_program_summary(path: Path, *, complete: bool, duplicate: bool = False) -> None:
    tag_dir = path / runtime_evidence_tag("3.14")
    tag_dir.mkdir(parents=True)
    scenarios = [_cross_program_scenario(spec.name) for spec in SCENARIOS]
    if not complete:
        scenarios = scenarios[:-1]
    if duplicate:
        scenarios.append(scenarios[0].copy())
    (tag_dir / "summary.json").write_text(
        json.dumps({**_pass_runtime_metadata(), "error": None, "scenarios": scenarios}),
        encoding="utf-8",
    )


def _recovery_record(scenario_name: str) -> dict:
    kill_exit = 1 if sys.platform == "win32" else -signal.SIGKILL
    checkpoints = {
        f"checkpoint_{wr_protocol.SUBMITTED}.json": {"checkpoint": wr_protocol.SUBMITTED},
        f"checkpoint_{wr_protocol.REPLACEMENT_ACK}.json": {
            "checkpoint": wr_protocol.REPLACEMENT_ACK,
            "terminal_state": "COMPLETED",
            "terminal_event_published": True,
        },
    }
    if scenario_name == SCENARIO_AFTER:
        checkpoints[f"checkpoint_{wr_protocol.COMPLETION_SAVED}.json"] = {
            "checkpoint": wr_protocol.COMPLETION_SAVED,
        }
    return {
        "name": scenario_name,
        "error": None,
        "ended_at_monotonic": 20.0,
        "checkpoints": checkpoints,
        "workers": [
            _child_record("worker-1", kill_exit, kill=True),
            _child_record("worker-2", 0),
        ],
        "producers": [
            _child_record("producer-submit", 0),
            _child_record("producer-recover", 0),
        ],
        "generation_snapshots": {"1": {"worker_generation": "1"}},
        "invocations": [{"worker_generation": "1"}, {"worker_generation": "2"}],
    }


def _write_worker_recovery_summary(path: Path, scenario_name: str) -> None:
    tag_dir = path / runtime_evidence_tag("3.14")
    tag_dir.mkdir(parents=True)
    record = _recovery_record(scenario_name)
    payload = {**_pass_runtime_metadata(), "error": None, "scenarios": [record]}
    (tag_dir / "summary.json").write_text(json.dumps(payload), encoding="utf-8")
    (tag_dir / f"summary-{scenario_name}.json").write_text(json.dumps(payload), encoding="utf-8")


def _write_broker_restart_summary(path: Path) -> None:
    tag_dir = path / runtime_evidence_tag("3.14")
    tag_dir.mkdir(parents=True)
    checkpoints = {
        f"checkpoint_{br_protocol.COMPLETED_DONE}.json": {"checkpoint": br_protocol.COMPLETED_DONE},
        f"checkpoint_{br_protocol.PENDING_SUBMITTED}.json": {
            "checkpoint": br_protocol.PENDING_SUBMITTED,
        },
        f"checkpoint_{br_protocol.PENDING_DONE}.json": {"checkpoint": br_protocol.PENDING_DONE},
    }
    scenario = {
        "name": BROKER_SCENARIO,
        "error": None,
        "ended_at_monotonic": 30.0,
        "checkpoints": checkpoints,
        "workers": [
            _child_record("worker-gen1", 0),
            _child_record("worker-gen2", 0),
        ],
        "producers": [
            _child_record("producer-run-completed", 0),
            _child_record("producer-submit-pending", 0),
            _child_record("producer-recover-completed", 0),
            _child_record("producer-await-pending", 0),
        ],
    }
    (tag_dir / "summary.json").write_text(
        json.dumps({**_pass_runtime_metadata(), "error": None, "scenario": scenario}),
        encoding="utf-8",
    )


def _ok_result(stdout: str) -> StageProcessResult:
    return StageProcessResult(
        returncode=0,
        timed_out=False,
        stdout=stdout,
        stderr="",
        duration_seconds=0.1,
        pid=1,
    )


def test_assess_child_rejects_zero_exit_without_ok_marker(tmp_path: Path) -> None:
    artifact = tmp_path / "child"
    _write_cross_program_summary(artifact, complete=True)
    result = _ok_result("silent success")
    with pytest.raises(rre.ReliabilityEvidenceError, match="success marker"):
        rre.assess_child_outcome(
            family=rrp.FAMILY_CROSS_PROGRAM,
            artifact_dir=artifact,
            python_version="3.14",
            result=result,
            combined_output=result.stdout,
        )


def test_assess_child_rejects_incomplete_cross_program_summary(tmp_path: Path) -> None:
    artifact = tmp_path / "child"
    _write_cross_program_summary(artifact, complete=False)
    ok_line = rre.expected_ok_line(rrp.FAMILY_CROSS_PROGRAM)
    result = _ok_result(ok_line)
    with pytest.raises(rre.ReliabilityEvidenceError, match="expected 7 scenarios"):
        rre.assess_child_outcome(
            family=rrp.FAMILY_CROSS_PROGRAM,
            artifact_dir=artifact,
            python_version="3.14",
            result=result,
            combined_output=result.stdout,
        )


def test_assess_child_rejects_duplicate_cross_program_records(tmp_path: Path) -> None:
    artifact = tmp_path / "child"
    _write_cross_program_summary(artifact, complete=True, duplicate=True)
    result = _ok_result(rre.expected_ok_line(rrp.FAMILY_CROSS_PROGRAM))
    with pytest.raises(rre.ReliabilityEvidenceError, match="expected 7 scenarios"):
        rre.assess_child_outcome(
            family=rrp.FAMILY_CROSS_PROGRAM,
            artifact_dir=artifact,
            python_version="3.14",
            result=result,
            combined_output=result.stdout,
        )


def test_assess_child_rejects_malformed_summary_json(tmp_path: Path) -> None:
    artifact = tmp_path / "child"
    tag_dir = artifact / runtime_evidence_tag("3.14")
    tag_dir.mkdir(parents=True)
    (tag_dir / "summary.json").write_text("{not json", encoding="utf-8")
    result = _ok_result(rre.expected_ok_line(rrp.FAMILY_CROSS_PROGRAM))
    with pytest.raises(rre.ReliabilityEvidenceError, match="cannot read summary"):
        rre.assess_child_outcome(
            family=rrp.FAMILY_CROSS_PROGRAM,
            artifact_dir=artifact,
            python_version="3.14",
            result=result,
            combined_output=result.stdout,
        )


def test_assess_child_rejects_cleanup_errors(tmp_path: Path) -> None:
    artifact = tmp_path / "child"
    _write_cross_program_summary(artifact, complete=True)
    result = StageProcessResult(
        returncode=0,
        timed_out=False,
        stdout=rre.expected_ok_line(rrp.FAMILY_CROSS_PROGRAM),
        stderr="",
        duration_seconds=0.1,
        pid=1,
        cleanup_errors=("leftover process",),
    )
    with pytest.raises(rre.ReliabilityEvidenceError, match="cleanup errors"):
        rre.assess_child_outcome(
            family=rrp.FAMILY_CROSS_PROGRAM,
            artifact_dir=artifact,
            python_version="3.14",
            result=result,
            combined_output=result.stdout,
        )


def test_assess_child_rejects_nonzero_exit_and_timeout(tmp_path: Path) -> None:
    artifact = tmp_path / "child"
    _write_cross_program_summary(artifact, complete=True)
    nonzero = StageProcessResult(
        returncode=2,
        timed_out=False,
        stdout=rre.expected_ok_line(rrp.FAMILY_CROSS_PROGRAM),
        stderr="",
        duration_seconds=0.1,
        pid=1,
    )
    with pytest.raises(rre.ReliabilityEvidenceError, match="exit code 2"):
        rre.assess_child_outcome(
            family=rrp.FAMILY_CROSS_PROGRAM,
            artifact_dir=artifact,
            python_version="3.14",
            result=nonzero,
            combined_output=nonzero.stdout,
        )
    timed_out = StageProcessResult(
        returncode=0,
        timed_out=True,
        stdout=rre.expected_ok_line(rrp.FAMILY_CROSS_PROGRAM),
        stderr="",
        duration_seconds=0.1,
        pid=1,
    )
    with pytest.raises(rre.ReliabilityEvidenceError, match="timed out"):
        rre.assess_child_outcome(
            family=rrp.FAMILY_CROSS_PROGRAM,
            artifact_dir=artifact,
            python_version="3.14",
            result=timed_out,
            combined_output=timed_out.stdout,
        )


def test_assess_child_rejects_unfinished_worker_recovery_checkpoint(tmp_path: Path) -> None:
    artifact = tmp_path / "child"
    _write_worker_recovery_summary(artifact, SCENARIO_BEFORE)
    tag = runtime_evidence_tag("3.14")
    payload = json.loads((artifact / tag / "summary.json").read_text())
    payload["scenarios"][0]["checkpoints"] = {}
    (artifact / tag / "summary.json").write_text(json.dumps(payload))
    result = _ok_result(rre.expected_ok_line(rrp.FAMILY_RECOVERY_BEFORE_COMPLETION))
    with pytest.raises(rre.ReliabilityEvidenceError, match="submitted"):
        rre.assess_child_outcome(
            family=rrp.FAMILY_RECOVERY_BEFORE_COMPLETION,
            artifact_dir=artifact,
            python_version="3.14",
            result=result,
            combined_output=result.stdout,
        )


def _write_child_evidence_for_argv(argv: list[str], artifact_dir: Path) -> str:
    script = Path(argv[1]).name
    if script == "verify_cross_program.py":
        _write_cross_program_summary(artifact_dir, complete=True)
        return rre.expected_ok_line(rrp.FAMILY_CROSS_PROGRAM)
    if script == "verify_worker_recovery.py":
        scenario = argv[argv.index("--only-scenario") + 1]
        _write_worker_recovery_summary(artifact_dir, scenario)
        return rre.expected_ok_line(rrp.FAMILY_RECOVERY_BEFORE_COMPLETION)
    if script == "verify_broker_restart.py":
        _write_broker_restart_summary(artifact_dir)
        return rre.expected_ok_line(rrp.FAMILY_BROKER_RESTART)
    raise AssertionError(f"unexpected child argv script {script}")


def test_run_repetition_fail_fast_records_partial_progress(tmp_path: Path) -> None:
    calls = {"count": 0}

    def fail_on_second(
        argv: list[str],
        *,
        cwd: Path,
        env: dict[str, str] | None,
        timeout_seconds: float,
    ) -> StageProcessResult:
        calls["count"] += 1
        artifact_flag = argv.index("--artifact-dir")
        artifact_dir = Path(argv[artifact_flag + 1])
        if calls["count"] == 1:
            ok_line = _write_child_evidence_for_argv(argv, artifact_dir)
            return StageProcessResult(
                returncode=0,
                timed_out=False,
                stdout=ok_line + "\n",
                stderr="",
                duration_seconds=0.1,
                pid=100,
            )
        return StageProcessResult(
            returncode=0,
            timed_out=False,
            stdout="",
            stderr="",
            duration_seconds=0.1,
            pid=200,
        )

    run_dir = tmp_path / "run"
    with patch("tools.verify_reliability_repetition.run_bounded", side_effect=fail_on_second):
        state = vrr.run_repetition(
            seed=7,
            artifact_dir=run_dir,
            python_version="3.14",
            invocation_id="test-invocation",
        )

    assert state.run_error is not None
    assert state.completed_count == 1
    assert len(state.outcomes) == 2
    summary = json.loads((run_dir / "run-summary.json").read_text(encoding="utf-8"))
    assert summary["completed_items"] == 1
    assert summary["planned_items"] == 12
    assert summary["ok"] is False
    assert summary["invocation_id"] == "test-invocation"
    assert summary["elapsed_wall_seconds"] is not None
    assert summary["environment"]["python_executable"]


def test_run_repetition_rejects_whole_run_timeout_before_start(tmp_path: Path) -> None:
    mono_values = iter([100.0, 955.0])

    def fake_monotonic() -> float:
        return next(mono_values, 955.0)

    with (
        patch("tools.verify_reliability_repetition.run_bounded") as run_bounded,
        patch("tools.verify_reliability_repetition.time.monotonic", side_effect=fake_monotonic),
    ):
        state = vrr.run_repetition(
            seed=1,
            artifact_dir=tmp_path / "late",
            python_version="3.14",
            invocation_id="timeout-before-start",
        )
        run_bounded.assert_not_called()

    assert state.run_error == "whole-run budget exhausted before next item"
    assert state.completed_count == 0
    summary = json.loads((tmp_path / "late" / "run-summary.json").read_text(encoding="utf-8"))
    assert summary["completed_items"] == 0
    assert summary["ok"] is False


def test_initial_summary_snapshot_marks_ok_false(tmp_path: Path) -> None:
    with patch("tools.verify_reliability_repetition.run_bounded") as run_bounded:
        run_bounded.side_effect = RuntimeError("launch failed")
        state = vrr.run_repetition(
            seed=3,
            artifact_dir=tmp_path / "abort",
            python_version="3.14",
            invocation_id="initial-snapshot",
        )
    assert state.run_error is not None
    summary = json.loads((tmp_path / "abort" / "run-summary.json").read_text(encoding="utf-8"))
    assert summary["ok"] is False
    assert summary["planned_items"] == 12
    assert summary["seed"] == 3


def test_stale_parent_summary_does_not_make_new_invocation_ok(tmp_path: Path) -> None:
    root = tmp_path / "artifact-root"
    root.mkdir()
    (root / "run-summary.json").write_text(
        json.dumps({"ok": True, "completed_items": 12, "planned_items": 12}),
        encoding="utf-8",
    )
    invocation_dir = root / "fresh-invocation"

    with patch("tools.verify_reliability_repetition.run_bounded") as run_bounded:
        run_bounded.side_effect = RuntimeError("boom")
        vrr.run_repetition(
            seed=9,
            artifact_dir=invocation_dir,
            python_version="3.14",
            invocation_id="fresh-invocation",
        )

    parent = json.loads((root / "run-summary.json").read_text(encoding="utf-8"))
    assert parent["ok"] is True
    child = json.loads((invocation_dir / "run-summary.json").read_text(encoding="utf-8"))
    assert child["ok"] is False
    assert child["invocation_id"] == "fresh-invocation"


def test_run_state_ok_requires_finalized_timestamps_and_budget() -> None:
    item = rrp.planned_items()[0]
    state = vrr.RunState(
        seed=1,
        python_version="3.14",
        environment={},
        artifact_dir=Path("/tmp"),
        invocation_id="id",
        planned=(item,),
        started_at=0.0,
        started_mono=0.0,
        outcomes=[
            vrr.ItemOutcome(
                item=item,
                status="passed",
                returncode=0,
                duration_seconds=1.0,
            ),
        ],
    )
    assert state.is_run_ok() is False
    state.finished_at = 10.0
    state.finished_mono = 5.0
    assert state.is_run_ok() is True
    state.finished_mono = state.started_mono + INTEGRATION_JOB_BUDGET_SECONDS + 1
    assert state.is_run_ok() is False


def test_assess_child_rejects_null_recovery_checkpoint(tmp_path: Path) -> None:
    artifact = tmp_path / "child"
    _write_worker_recovery_summary(artifact, SCENARIO_BEFORE)
    tag = runtime_evidence_tag("3.14")
    payload = json.loads((artifact / tag / "summary.json").read_text())
    payload["scenarios"][0]["checkpoints"][
        f"checkpoint_{wr_protocol.REPLACEMENT_ACK}.json"
    ] = {}
    (artifact / tag / "summary.json").write_text(json.dumps(payload))
    result = _ok_result(rre.expected_ok_line(rrp.FAMILY_RECOVERY_BEFORE_COMPLETION))
    with pytest.raises(rre.ReliabilityEvidenceError, match="replacement_ack"):
        rre.assess_child_outcome(
            family=rrp.FAMILY_RECOVERY_BEFORE_COMPLETION,
            artifact_dir=artifact,
            python_version="3.14",
            result=result,
            combined_output=result.stdout,
        )


def test_assess_child_rejects_incomplete_main_recovery_record(tmp_path: Path) -> None:
    artifact = tmp_path / "child"
    _write_worker_recovery_summary(artifact, SCENARIO_BEFORE)
    tag = runtime_evidence_tag("3.14")
    payload = json.loads((artifact / tag / "summary.json").read_text())
    payload["scenarios"][0]["workers"] = []
    (artifact / tag / "summary.json").write_text(json.dumps(payload))
    result = _ok_result(rre.expected_ok_line(rrp.FAMILY_RECOVERY_BEFORE_COMPLETION))
    with pytest.raises(rre.ReliabilityEvidenceError, match="two workers"):
        rre.assess_child_outcome(
            family=rrp.FAMILY_RECOVERY_BEFORE_COMPLETION,
            artifact_dir=artifact,
            python_version="3.14",
            result=result,
            combined_output=result.stdout,
        )


def test_assess_child_rejects_runtime_python_version_mismatch(tmp_path: Path) -> None:
    artifact = tmp_path / "child"
    _write_cross_program_summary(artifact, complete=True)
    tag = runtime_evidence_tag("3.14")
    payload = json.loads((artifact / tag / "summary.json").read_text())
    payload["python_version"] = "3.12"
    (artifact / tag / "summary.json").write_text(json.dumps(payload))
    result = _ok_result(rre.expected_ok_line(rrp.FAMILY_CROSS_PROGRAM))
    with pytest.raises(rre.ReliabilityEvidenceError, match="python_version"):
        rre.assess_child_outcome(
            family=rrp.FAMILY_CROSS_PROGRAM,
            artifact_dir=artifact,
            python_version="3.14",
            result=result,
            combined_output=result.stdout,
        )


def test_run_repetition_final_budget_failure_persisted(tmp_path: Path) -> None:
    def finalize_over_budget(state: vrr.RunState) -> None:
        state.started_mono = 0.0
        state.finished_at = 100.0
        state.finished_mono = float(INTEGRATION_JOB_BUDGET_SECONDS + 1)

    def pass_once(
        argv: list[str],
        *,
        cwd: Path,
        env: dict[str, str] | None,
        timeout_seconds: float,
    ) -> StageProcessResult:
        artifact_flag = argv.index("--artifact-dir")
        artifact_dir = Path(argv[artifact_flag + 1])
        ok_line = _write_child_evidence_for_argv(argv, artifact_dir)
        return StageProcessResult(
            returncode=0,
            timed_out=False,
            stdout=ok_line + "\n",
            stderr="",
            duration_seconds=0.1,
            pid=1,
        )

    run_dir = tmp_path / "budget"
    with (
        patch("tools.verify_reliability_repetition.run_bounded", side_effect=pass_once),
        patch("tools.verify_reliability_repetition._finalize_run_state", side_effect=finalize_over_budget),
        patch("tools.verify_reliability_repetition.shuffle_items", return_value=rrp.planned_items()[:1]),
    ):
        state = vrr.run_repetition(
            seed=1,
            artifact_dir=run_dir,
            python_version="3.14",
            invocation_id="budget",
        )

    assert state.run_error is not None
    assert "monotonic budget exceeded" in state.run_error
    assert state.completed_count == 1
    summary = json.loads((run_dir / "run-summary.json").read_text(encoding="utf-8"))
    assert summary["ok"] is False


def test_artifact_path_length_under_windows_limit_for_deep_worktree() -> None:
    deepest = vrr.deepest_default_artifact_file(
        repo_root=REPO_ROOT,
        python_version="3.14",
    )
    assert len(str(deepest)) < vrr.WINDOWS_MAX_PATH


def test_new_invocation_token_is_twelve_hex_chars() -> None:
    token = vrr.new_invocation_token()
    assert len(token) == vrr.INVOCATION_TOKEN_HEX_LEN
    assert token == token.lower()
    int(token, 16)


def test_run_repetition_stops_after_summary_persist_failure(tmp_path: Path) -> None:
    calls = {"persist": 0, "run": 0}
    real_persist = vrr.persist_run_summary

    def flaky_persist(state: vrr.RunState) -> Path:
        calls["persist"] += 1
        if calls["persist"] == 2:
            raise OSError("disk full")
        return real_persist(state)

    def pass_once(
        argv: list[str],
        *,
        cwd: Path,
        env: dict[str, str] | None,
        timeout_seconds: float,
    ) -> StageProcessResult:
        calls["run"] += 1
        artifact_flag = argv.index("--artifact-dir")
        artifact_dir = Path(argv[artifact_flag + 1])
        ok_line = _write_child_evidence_for_argv(argv, artifact_dir)
        return StageProcessResult(
            returncode=0,
            timed_out=False,
            stdout=ok_line + "\n",
            stderr="",
            duration_seconds=0.1,
            pid=1,
        )

    run_dir = tmp_path / "persist-stop"
    with (
        patch("tools.verify_reliability_repetition.run_bounded", side_effect=pass_once),
        patch("tools.verify_reliability_repetition.persist_run_summary", side_effect=flaky_persist),
        patch("tools.verify_reliability_repetition.shuffle_items", return_value=rrp.planned_items()[:2]),
    ):
        state = vrr.run_repetition(
            seed=2,
            artifact_dir=run_dir,
            python_version="3.14",
            invocation_id="persist-stop",
        )

    assert calls["run"] == 1
    assert state.completed_count == 1
    assert state.run_error is not None
    assert "failed to persist run summary" in state.run_error
    summary = json.loads((run_dir / "run-summary.json").read_text(encoding="utf-8"))
    assert summary["completed_items"] == 1
    assert summary["ok"] is False


def test_run_repetition_initial_persist_failure_skips_children(tmp_path: Path) -> None:
    with (
        patch("tools.verify_reliability_repetition.persist_run_summary", side_effect=OSError("disk full")),
        patch("tools.verify_reliability_repetition.run_bounded") as run_bounded,
    ):
        state = vrr.run_repetition(
            seed=4,
            artifact_dir=tmp_path / "initial-persist",
            python_version="3.14",
            invocation_id="initial-persist",
        )
        run_bounded.assert_not_called()

    assert state.run_error is not None
    assert "failed to persist run summary" in state.run_error
    assert state.completed_count == 0


def test_main_exception_persists_full_summary(tmp_path: Path) -> None:
    artifact_root = tmp_path / "root"
    with (
        patch.object(vrr, "run_repetition", side_effect=OSError("disk full")),
        patch.object(vrr, "require_gate_python", return_value="3.14"),
    ):
        code = vrr.main(
            ["--seed", "11", "--artifact-dir", str(artifact_root)],
        )
    assert code == 1
    summaries = list(artifact_root.glob("*/run-summary.json"))
    assert len(summaries) == 1
    summary = json.loads(summaries[0].read_text(encoding="utf-8"))
    assert summary["ok"] is False
    assert summary["seed"] == 11
    assert summary["planned_items"] == 12
    assert "environment" in summary
