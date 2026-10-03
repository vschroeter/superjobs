"""Validate child verification runners for reliability repetition."""

from __future__ import annotations

import json
import signal
import sys
from pathlib import Path
from typing import Any

from scripts.dev_check.process_tree import StageProcessResult
from tools.broker_restart_support import protocol as br_protocol
from tools.reliability_repetition_support.plan import (
    FAMILY_BROKER_RESTART,
    FAMILY_CROSS_PROGRAM,
    FAMILY_RECOVERY_AFTER_COMPLETION,
    FAMILY_RECOVERY_BEFORE_COMPLETION,
    ReliabilityFamily,
)
from tools.verify_broker_restart import SCENARIO_NAME as BROKER_RESTART_SCENARIO
from tools.verify_contract_typing import runtime_evidence_tag
from tools.verify_cross_program import SCENARIOS as CROSS_PROGRAM_SCENARIOS
from tools.verify_worker_recovery import (
    PRODUCER_EXIT_OK,
    SCENARIO_AFTER,
    SCENARIO_BEFORE,
    WORKER_EXIT_OK,
)
from tools.worker_recovery_support import protocol as wr_protocol


class ReliabilityEvidenceError(Exception):
    """Raised when a child run lacks required success evidence."""


def expected_ok_line(family: ReliabilityFamily) -> str:
    if family == FAMILY_CROSS_PROGRAM:
        return "verify_cross_program: OK"
    if family in (FAMILY_RECOVERY_BEFORE_COMPLETION, FAMILY_RECOVERY_AFTER_COMPLETION):
        return "verify_worker_recovery: OK"
    if family == FAMILY_BROKER_RESTART:
        return "verify_broker_restart: OK"
    raise ValueError(f"unknown family: {family}")


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ReliabilityEvidenceError(f"cannot read summary {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise ReliabilityEvidenceError(f"summary {path} is not a JSON object")
    return payload


def _require_monotonic(value: Any, *, label: str) -> None:
    if not isinstance(value, (int, float)):
        raise ReliabilityEvidenceError(f"{label} must be numeric, saw {type(value)}")


def _checkpoint_key(name: str) -> str:
    return f"checkpoint_{name}.json"


BROKER_RESTART_WORKER_NAMES = ("worker-gen1", "worker-gen2")
BROKER_RESTART_PRODUCER_NAMES = (
    "producer-run-completed",
    "producer-submit-pending",
    "producer-recover-completed",
    "producer-await-pending",
)


def _validate_pass_runtime_summary(payload: dict[str, Any], *, python_version: str) -> None:
    if payload.get("python_version") != python_version:
        raise ReliabilityEvidenceError(
            f"summary python_version {payload.get('python_version')!r} "
            f"!= expected {python_version!r}",
        )
    origins = payload.get("origins")
    if not isinstance(origins, dict):
        raise ReliabilityEvidenceError("summary missing origins object")
    producer = origins.get("producer")
    worker = origins.get("worker")
    if not isinstance(producer, dict) or not producer:
        raise ReliabilityEvidenceError("summary origins missing nonempty producer evidence")
    if not isinstance(worker, dict) or not worker:
        raise ReliabilityEvidenceError("summary origins missing nonempty worker evidence")


def _require_checkpoint_payload(
    checkpoints: dict[str, Any],
    *,
    name: str,
    label: str,
) -> dict[str, Any]:
    key = _checkpoint_key(name)
    if key not in checkpoints:
        raise ReliabilityEvidenceError(f"{label} missing checkpoint evidence for {name}")
    payload = checkpoints[key]
    if not isinstance(payload, dict) or not payload:
        raise ReliabilityEvidenceError(
            f"{label} checkpoint {name} is not a nonempty object",
        )
    if payload.get("error") is not None:
        raise ReliabilityEvidenceError(
            f"{label} checkpoint {name} failed to load: {payload.get('error')}",
        )
    return payload


def _assert_no_run_error(artifact_dir: Path) -> None:
    run_error = artifact_dir / "run_error.json"
    if run_error.is_file():
        payload = _load_json(run_error)
        raise ReliabilityEvidenceError(
            f"child left run_error.json: {payload.get('error')}",
        )


def _validate_cross_program_scenario(item: dict[str, Any], spec_name: str) -> None:
    if item.get("name") != spec_name:
        raise ReliabilityEvidenceError(
            f"cross-program scenario name {item.get('name')} != {spec_name}",
        )
    if item.get("error"):
        raise ReliabilityEvidenceError(
            f"cross-program scenario {spec_name} error: {item['error']}",
        )
    _require_monotonic(item.get("ended_at_monotonic"), label=f"{spec_name} ended_at_monotonic")
    checkpoints = item.get("checkpoints")
    if not isinstance(checkpoints, dict):
        raise ReliabilityEvidenceError(
            f"cross-program scenario {spec_name} missing checkpoints object",
        )
    spec = next((entry for entry in CROSS_PROGRAM_SCENARIOS if entry.name == spec_name), None)
    if spec is None:
        raise ReliabilityEvidenceError(f"unknown cross-program scenario {spec_name}")
    producer_exit = item.get("producer_exit")
    if producer_exit != spec.expected_producer_code:
        raise ReliabilityEvidenceError(
            f"cross-program {spec_name} producer_exit {producer_exit} "
            f"!= expected {spec.expected_producer_code}",
        )
    worker_exit = item.get("worker_exit")
    if spec.expected_worker_code is not None:
        if worker_exit != spec.expected_worker_code:
            raise ReliabilityEvidenceError(
                f"cross-program {spec_name} worker_exit {worker_exit} "
                f"!= expected {spec.expected_worker_code}",
            )
    elif spec.start_worker and worker_exit != 0:
        raise ReliabilityEvidenceError(
            f"cross-program {spec_name} worker_exit {worker_exit} != expected 0",
        )
    producer = item.get("producer")
    if spec.start_producer:
        if not isinstance(producer, dict):
            raise ReliabilityEvidenceError(
                f"cross-program {spec_name} missing producer child record",
            )
        _require_monotonic(
            producer.get("ended_at_monotonic"),
            label=f"{spec_name} producer ended_at_monotonic",
        )
        if producer.get("exit_code") != spec.expected_producer_code:
            raise ReliabilityEvidenceError(
                f"cross-program {spec_name} producer exit_code mismatch",
            )
    worker = item.get("worker")
    if spec.start_worker:
        if not isinstance(worker, dict):
            raise ReliabilityEvidenceError(
                f"cross-program {spec_name} missing worker child record",
            )
        _require_monotonic(
            worker.get("ended_at_monotonic"),
            label=f"{spec_name} worker ended_at_monotonic",
        )


def _validate_cross_program_summary(payload: dict[str, Any], *, python_version: str) -> None:
    _validate_pass_runtime_summary(payload, python_version=python_version)
    if payload.get("error"):
        raise ReliabilityEvidenceError(f"cross-program summary error: {payload['error']}")
    scenarios = payload.get("scenarios")
    if not isinstance(scenarios, list):
        raise ReliabilityEvidenceError("cross-program summary missing scenarios list")
    expected_names = [spec.name for spec in CROSS_PROGRAM_SCENARIOS]
    if len(scenarios) != len(expected_names):
        raise ReliabilityEvidenceError(
            f"cross-program expected {len(expected_names)} scenarios, saw {len(scenarios)}",
        )
    by_name = {
        item.get("name"): item
        for item in scenarios
        if isinstance(item, dict) and item.get("name") is not None
    }
    if set(by_name) != set(expected_names):
        raise ReliabilityEvidenceError(
            f"cross-program incomplete scenarios: expected {expected_names}, "
            f"saw {sorted(by_name)}",
        )
    for spec_name in expected_names:
        _validate_cross_program_scenario(by_name[spec_name], spec_name)


def _expected_worker_kill_exit() -> int:
    return 1 if sys.platform == "win32" else -signal.SIGKILL


def _validate_recovery_child(
    child: dict[str, Any],
    *,
    label: str,
    expected_exit: int | None,
    require_kill_termination: bool,
) -> None:
    if child.get("name") != label:
        raise ReliabilityEvidenceError(
            f"worker recovery child name {child.get('name')} != {label}",
        )
    _require_monotonic(child.get("ended_at_monotonic"), label=f"{label} ended_at_monotonic")
    exit_code = child.get("exit_code")
    if expected_exit is not None and exit_code != expected_exit:
        raise ReliabilityEvidenceError(
            f"worker recovery {label} exit_code {exit_code} != expected {expected_exit}",
        )
    if require_kill_termination:
        termination = child.get("termination")
        if not isinstance(termination, dict):
            raise ReliabilityEvidenceError(
                f"worker recovery {label} missing kill termination evidence",
            )
        if termination.get("launcher_exit") != expected_exit:
            raise ReliabilityEvidenceError(
                f"worker recovery {label} launcher_exit mismatch",
            )


def _validate_worker_recovery_record(
    record: dict[str, Any],
    *,
    scenario_name: str,
) -> None:
    if record.get("name") != scenario_name:
        raise ReliabilityEvidenceError(
            f"worker recovery record name {record.get('name')} != {scenario_name}",
        )
    if record.get("error"):
        raise ReliabilityEvidenceError(f"worker recovery scenario error: {record['error']}")
    _require_monotonic(
        record.get("ended_at_monotonic"),
        label=f"{scenario_name} ended_at_monotonic",
    )
    checkpoints = record.get("checkpoints")
    if not isinstance(checkpoints, dict):
        raise ReliabilityEvidenceError("worker recovery missing checkpoints object")
    _require_checkpoint_payload(
        checkpoints,
        name=wr_protocol.SUBMITTED,
        label="worker recovery",
    )
    replacement_ack = _require_checkpoint_payload(
        checkpoints,
        name=wr_protocol.REPLACEMENT_ACK,
        label="worker recovery",
    )
    if replacement_ack.get("terminal_state") != "COMPLETED":
        raise ReliabilityEvidenceError(
            "worker recovery replacement_ack terminal_state "
            f"{replacement_ack.get('terminal_state')!r} != COMPLETED",
        )
    if replacement_ack.get("terminal_event_published") is not True:
        raise ReliabilityEvidenceError(
            "worker recovery replacement_ack requires terminal_event_published true",
        )
    if scenario_name == SCENARIO_AFTER:
        _require_checkpoint_payload(
            checkpoints,
            name=wr_protocol.COMPLETION_SAVED,
            label="worker recovery",
        )
    workers = record.get("workers")
    if not isinstance(workers, list) or len(workers) != 2:
        raise ReliabilityEvidenceError(
            f"worker recovery expected two workers, saw {len(workers) if isinstance(workers, list) else type(workers)}",
        )
    kill_exit = _expected_worker_kill_exit()
    _validate_recovery_child(
        workers[0],
        label="worker-1",
        expected_exit=kill_exit,
        require_kill_termination=True,
    )
    _validate_recovery_child(
        workers[1],
        label="worker-2",
        expected_exit=WORKER_EXIT_OK,
        require_kill_termination=False,
    )
    producers = record.get("producers")
    if not isinstance(producers, list) or len(producers) != 2:
        raise ReliabilityEvidenceError(
            f"worker recovery expected two producers, saw {len(producers) if isinstance(producers, list) else type(producers)}",
        )
    _validate_recovery_child(
        producers[0],
        label="producer-submit",
        expected_exit=PRODUCER_EXIT_OK,
        require_kill_termination=False,
    )
    _validate_recovery_child(
        producers[1],
        label="producer-recover",
        expected_exit=PRODUCER_EXIT_OK,
        require_kill_termination=False,
    )
    generation_snapshots = record.get("generation_snapshots")
    if not isinstance(generation_snapshots, dict) or "1" not in generation_snapshots:
        raise ReliabilityEvidenceError("worker recovery missing generation-1 snapshot")
    invocations = record.get("invocations")
    if not isinstance(invocations, list) or not invocations:
        raise ReliabilityEvidenceError("worker recovery missing handler invocations")


def _validate_worker_recovery_summary(
    payload: dict[str, Any],
    *,
    scenario_name: str,
    artifact_dir: Path,
    python_version: str,
) -> None:
    _validate_pass_runtime_summary(payload, python_version=python_version)
    if payload.get("error"):
        raise ReliabilityEvidenceError(f"worker recovery summary error: {payload['error']}")
    scenarios = payload.get("scenarios")
    if not isinstance(scenarios, list) or len(scenarios) != 1:
        raise ReliabilityEvidenceError(
            "worker recovery expected exactly one scenario, "
            f"saw {len(scenarios) if isinstance(scenarios, list) else type(scenarios)}",
        )
    item = scenarios[0]
    if not isinstance(item, dict):
        raise ReliabilityEvidenceError("worker recovery scenario entry is not an object")
    _validate_worker_recovery_record(item, scenario_name=scenario_name)
    per_scenario = (
        artifact_dir
        / runtime_evidence_tag(python_version)
        / f"summary-{scenario_name}.json"
    )
    if not per_scenario.is_file():
        raise ReliabilityEvidenceError(
            f"worker recovery missing summary-{scenario_name}.json at {per_scenario}",
        )
    per_payload = _load_json(per_scenario)
    _validate_pass_runtime_summary(per_payload, python_version=python_version)
    if per_payload.get("error"):
        raise ReliabilityEvidenceError(
            f"worker recovery per-scenario summary error: {per_payload['error']}",
        )
    per_scenarios = per_payload.get("scenarios")
    if not isinstance(per_scenarios, list):
        raise ReliabilityEvidenceError("worker recovery per-scenario summary missing scenarios")
    matching = [entry for entry in per_scenarios if isinstance(entry, dict) and entry.get("name") == scenario_name]
    if len(matching) != 1:
        raise ReliabilityEvidenceError(
            f"worker recovery per-scenario summary missing record for {scenario_name}",
        )
    _validate_worker_recovery_record(matching[0], scenario_name=scenario_name)


def _validate_broker_restart_record(record: dict[str, Any]) -> None:
    if record.get("name") != BROKER_RESTART_SCENARIO:
        raise ReliabilityEvidenceError(
            f"broker restart scenario name {record.get('name')} != {BROKER_RESTART_SCENARIO}",
        )
    if record.get("error"):
        raise ReliabilityEvidenceError(f"broker restart scenario error: {record['error']}")
    _require_monotonic(
        record.get("ended_at_monotonic"),
        label="broker restart ended_at_monotonic",
    )
    checkpoints = record.get("checkpoints")
    if not isinstance(checkpoints, dict):
        raise ReliabilityEvidenceError("broker restart missing checkpoints object")
    for checkpoint in (
        br_protocol.COMPLETED_DONE,
        br_protocol.PENDING_SUBMITTED,
        br_protocol.PENDING_DONE,
    ):
        _require_checkpoint_payload(
            checkpoints,
            name=checkpoint,
            label="broker restart",
        )
    workers = record.get("workers")
    if not isinstance(workers, list) or len(workers) != len(BROKER_RESTART_WORKER_NAMES):
        raise ReliabilityEvidenceError(
            f"broker restart expected {len(BROKER_RESTART_WORKER_NAMES)} workers, "
            f"saw {len(workers) if isinstance(workers, list) else type(workers)}",
        )
    worker_names = [worker.get("name") for worker in workers if isinstance(worker, dict)]
    if worker_names != list(BROKER_RESTART_WORKER_NAMES):
        raise ReliabilityEvidenceError(
            f"broker restart worker names {worker_names!r} "
            f"!= expected {list(BROKER_RESTART_WORKER_NAMES)!r}",
        )
    for worker in workers:
        if not isinstance(worker, dict):
            raise ReliabilityEvidenceError("broker restart worker entry is not an object")
        _require_monotonic(
            worker.get("ended_at_monotonic"),
            label=f"broker restart worker {worker.get('name')} ended_at_monotonic",
        )
        if worker.get("exit_code") != WORKER_EXIT_OK:
            raise ReliabilityEvidenceError(
                f"broker restart worker {worker.get('name')} exit {worker.get('exit_code')}",
            )
    producers = record.get("producers")
    if not isinstance(producers, list) or len(producers) != len(BROKER_RESTART_PRODUCER_NAMES):
        raise ReliabilityEvidenceError(
            f"broker restart expected {len(BROKER_RESTART_PRODUCER_NAMES)} producers, "
            f"saw {len(producers) if isinstance(producers, list) else type(producers)}",
        )
    producer_names = [
        producer.get("name") for producer in producers if isinstance(producer, dict)
    ]
    if producer_names != list(BROKER_RESTART_PRODUCER_NAMES):
        raise ReliabilityEvidenceError(
            f"broker restart producer names {producer_names!r} "
            f"!= expected {list(BROKER_RESTART_PRODUCER_NAMES)!r}",
        )
    for producer in producers:
        if not isinstance(producer, dict):
            raise ReliabilityEvidenceError("broker restart producer entry is not an object")
        _require_monotonic(
            producer.get("ended_at_monotonic"),
            label=f"broker restart producer {producer.get('name')} ended_at_monotonic",
        )
        if producer.get("exit_code") != PRODUCER_EXIT_OK:
            raise ReliabilityEvidenceError(
                f"broker restart producer {producer.get('name')} exit {producer.get('exit_code')}",
            )


def _validate_broker_restart_summary(payload: dict[str, Any], *, python_version: str) -> None:
    _validate_pass_runtime_summary(payload, python_version=python_version)
    if payload.get("error"):
        raise ReliabilityEvidenceError(f"broker restart summary error: {payload['error']}")
    scenario = payload.get("scenario")
    if not isinstance(scenario, dict):
        raise ReliabilityEvidenceError("broker restart summary missing scenario object")
    _validate_broker_restart_record(scenario)


def validate_artifact_evidence(
    *,
    family: ReliabilityFamily,
    artifact_dir: Path,
    python_version: str,
) -> None:
    _assert_no_run_error(artifact_dir)
    tag = runtime_evidence_tag(python_version)
    summary_path = artifact_dir / tag / "summary.json"
    if not summary_path.is_file():
        raise ReliabilityEvidenceError(f"missing summary.json at {summary_path}")
    payload = _load_json(summary_path)
    if family == FAMILY_CROSS_PROGRAM:
        _validate_cross_program_summary(payload, python_version=python_version)
    elif family == FAMILY_RECOVERY_BEFORE_COMPLETION:
        _validate_worker_recovery_summary(
            payload,
            scenario_name=SCENARIO_BEFORE,
            artifact_dir=artifact_dir,
            python_version=python_version,
        )
    elif family == FAMILY_RECOVERY_AFTER_COMPLETION:
        _validate_worker_recovery_summary(
            payload,
            scenario_name=SCENARIO_AFTER,
            artifact_dir=artifact_dir,
            python_version=python_version,
        )
    elif family == FAMILY_BROKER_RESTART:
        _validate_broker_restart_summary(payload, python_version=python_version)
    else:
        raise ReliabilityEvidenceError(f"unknown family: {family}")


def assess_child_outcome(
    *,
    family: ReliabilityFamily,
    artifact_dir: Path,
    python_version: str,
    result: StageProcessResult,
    combined_output: str,
) -> None:
    if result.cleanup_errors:
        joined = "; ".join(result.cleanup_errors)
        raise ReliabilityEvidenceError(f"child cleanup errors: {joined}")
    ok_line = expected_ok_line(family)
    if result.interrupted:
        raise ReliabilityEvidenceError("child interrupted")
    if result.timed_out:
        raise ReliabilityEvidenceError("child timed out before completion")
    if result.returncode != 0:
        raise ReliabilityEvidenceError(f"child exit code {result.returncode}")
    if ok_line not in combined_output:
        raise ReliabilityEvidenceError(f"child stdout/stderr missing success marker {ok_line!r}")
    validate_artifact_evidence(
        family=family,
        artifact_dir=artifact_dir,
        python_version=python_version,
    )
