#!/usr/bin/env python3
"""Verify exclusive resource admission across installed producer/worker/controller processes."""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import tempfile
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
SUPPORT_ROOT = Path(__file__).resolve().parent / "resource_admission_support"
ORIGIN_PROBE = Path(__file__).resolve().parent / "wheel_origin_probe.py"

from tests.support.nats_harness.server import (  # noqa: E402
    OwnedNatsServer,
)

from tools.cross_program_support.child_env import (  # noqa: E402
    isolated_child_env,
    python_command,
)
from tools.cross_program_support.protocol import write_worker_stop  # noqa: E402
from tools.resource_admission_support import protocol as ra_protocol  # noqa: E402
from tools.resource_admission_support.evidence import (  # noqa: E402
    count_handler_callbacks,
    read_events,
    wait_for_work_pulls,
)
from tools.resource_admission_support.observation_collector import (  # noqa: E402
    collect_scenario_observations,
)
from tools.resource_admission_support.orchestration import (  # noqa: E402
    ResourceAdmissionError,
    _TrackedChild,
    assert_holder_evidence_from_state,
    execution_id_from_submitted,
    finalize_tracked,
    load_owner_snapshot,
    publish_release,
    run_controller_phase,
    spawn_worker,
)
from tools.verify_contract_typing import (  # noqa: E402
    DEFAULT_RUNTIMES,
    REPO_ROOT as TYPING_REPO_ROOT,
    VerificationError,
    assert_final_runtime_python,
    assert_work_parent_outside_repo,
    build_wheels,
    create_wheel_venv,
    run_cmd,
    runtime_evidence_tag,
    venv_python,
)
from tools.verify_cross_program import (  # noqa: E402
    ChildRecord,
    CrossProgramError,
    _collect_checkpoint_snapshot,
    _reap_child,
    _write_evidence,
)
from tools.verify_worker_recovery import (  # noqa: E402
    _assert_ready_pid_matches_spawn,
    _broker_store_dir,
    _broker_store_fingerprint,
    _hard_kill_worker,
)

assert TYPING_REPO_ROOT == REPO_ROOT

CHILD_TIMEOUT_SECONDS = 30
SCENARIO_TIMEOUT_SECONDS = 120
KILL_REAP_SECONDS = 5
WORKER_EXIT_OK = 0

SCENARIO_CROSS_JOB = "cross_job_contention"
SCENARIO_REDELIVERY = "duplicate_redelivery"
SCENARIO_CRASH = "crash_active_owner"
SCENARIO_BROKER_RESTART = "broker_restart_held"
ALL_SCENARIOS = (
    SCENARIO_CROSS_JOB,
    SCENARIO_REDELIVERY,
    SCENARIO_CRASH,
    SCENARIO_BROKER_RESTART,
)

ROLE_SHARED_FILES = (
    "protocol.py",
    "scenario_jobs.py",
    "runtime_isolation.py",
    "constants.py",
    "queue_config.py",
    "stdmsg.py",
    "nats_control.py",
    "catalog_factory.py",
    "admission_helpers.py",
    "api_compat.py",
    "evidence.py",
)
WORKER_SUPPORT_FILES = ROLE_SHARED_FILES + (
    "worker_handlers.py",
    "worker_app.py",
    "delivery_evidence_backend.py",
)
CONTROLLER_SUPPORT_FILES = ROLE_SHARED_FILES + ("controller_app.py",)


@dataclass
class ScenarioRecord:
    name: str
    run_id: str
    state_dir: Path
    started_at: float
    ended_at: float | None = None
    workers: list[ChildRecord] = field(default_factory=list)
    controllers: list[ChildRecord] = field(default_factory=list)
    checkpoints: dict[str, Any] | None = None
    holder_max: int | None = None
    broker_store_fingerprint_before: str | None = None
    broker_store_fingerprint_after: str | None = None
    api_compat: dict[str, Any] | None = None
    observations: dict[str, Any] | None = None
    error: str | None = None


@dataclass
class PythonPassState:
    python_version: str
    tag: str
    controller_py: Path
    worker_py: Path
    controller_dir: Path
    worker_dir: Path
    scenario_records: list[ScenarioRecord] = field(default_factory=list)
    origins: dict[str, Any] | None = None
    runtime_version: str | None = None
    broker_log: str = ""
    api_compat: dict[str, Any] | None = None
    error: str | None = None


def materialize_role_dir(work_dir: Path, role: str, files: tuple[str, ...]) -> Path:
    dest = work_dir / role
    dest.mkdir(parents=True, exist_ok=True)
    for name in files:
        shutil.copy2(SUPPORT_ROOT / name, dest / name)
    return dest


def probe_api_compat(python: Path, controller_dir: Path) -> dict[str, Any]:
    command = python_command(str(python), controller_dir, "api_compat.py")
    completed = run_cmd(
        command,
        cwd=controller_dir,
        env=isolated_child_env(),
        allowed_codes=(0, 2),
    )
    return json.loads(completed.stdout)


def probe_role_origins(
    python: Path,
    role_dir: Path,
    *,
    role: str,
    evidence_dir: Path | None,
    python_version: str,
) -> dict[str, Any]:
    probe_dir = role_dir / f"origin_probe_{role}"
    probe_dir.mkdir(parents=True, exist_ok=True)
    files = CONTROLLER_SUPPORT_FILES if role == "controller" else WORKER_SUPPORT_FILES
    for name in files:
        shutil.copy2(role_dir / name, probe_dir / name)
    if role == "controller":
        shutil.copy2(probe_dir / "controller_app.py", probe_dir / "producer.py")
    shutil.copy2(ORIGIN_PROBE, probe_dir / "wheel_origin_probe.py")
    command = python_command(
        str(python),
        probe_dir,
        "wheel_origin_probe.py",
        str(probe_dir),
        str(REPO_ROOT),
        "--role",
        "producer" if role == "controller" else "worker",
    )
    completed = run_cmd(command, cwd=probe_dir, env=isolated_child_env())
    evidence = json.loads(completed.stdout)
    if evidence_dir is not None:
        evidence_dir.mkdir(parents=True, exist_ok=True)
        tag = runtime_evidence_tag(python_version)
        (evidence_dir / f"{tag}-{role}-origin.json").write_text(
            json.dumps(evidence, indent=2),
            encoding="utf-8",
        )
    return evidence


def _scenario_record_json(item: ScenarioRecord) -> dict[str, Any]:
    return {
        "name": item.name,
        "run_id": item.run_id,
        "started_at_monotonic": item.started_at,
        "ended_at_monotonic": item.ended_at,
        "duration_seconds": (item.ended_at or item.started_at) - item.started_at,
        "error": item.error,
        "checkpoints": item.checkpoints,
        "holder_max": item.holder_max,
        "workers": [child.pid for child in item.workers],
        "controllers": [child.name for child in item.controllers],
        "broker_store_fingerprint_before": item.broker_store_fingerprint_before,
        "broker_store_fingerprint_after": item.broker_store_fingerprint_after,
        "api_compat": item.api_compat,
        "observations": item.observations,
    }


def _run_scenario(
    name,
    *,
    nats_url,
    controller_python,
    worker_python,
    controller_dir,
    worker_dir,
    scenario_deadline,
    log_dir,
    owner=None,
    broker_target=None,
):
    from tools.resource_admission_support.evidence import (
        append_event,
    )

    run_id = uuid.uuid4().hex
    state_dir = controller_dir / "state" / name / run_id
    state_dir.mkdir(parents=True, exist_ok=True)
    record = ScenarioRecord(
        name=name, run_id=run_id, state_dir=state_dir, started_at=time.monotonic()
    )
    tracked = []
    environment = {
        "NATS_URL": nats_url,
        "SUPERJOBS_CROSS_RUN_ID": run_id,
        "SUPERJOBS_CROSS_SCENARIO": name,
        "SUPERJOBS_CROSS_STATE_DIR": str(state_dir),
        "SUPERJOBS_RA_ACK_WAIT": "1.5" if name == SCENARIO_REDELIVERY else "3.0",
    }
    deadline = scenario_deadline - KILL_REAP_SECONDS

    def worker(label, generation, jobs):
        process, child, out, err = spawn_worker(
            worker_python=worker_python,
            worker_dir=worker_dir,
            env=isolated_child_env(
                {
                    **environment,
                    "SUPERJOBS_WORKER_GENERATION": generation,
                    "SUPERJOBS_RA_REGISTER_JOBS": jobs,
                }
            ),
            log_dir=log_dir / name,
            label=label,
        )
        record.workers.append(child)
        tracked.append(_TrackedChild(process, child, (out, err)))
        ready = ra_protocol.wait_for_ready(
            state_dir,
            expected_run_id=run_id,
            deadline=min(deadline, time.monotonic() + CHILD_TIMEOUT_SECONDS),
            child_process=process,
            expected_worker_generation=generation,
        )
        _assert_ready_pid_matches_spawn(process, ready, label=label)
        return process, child

    def phase(phase_name, token=None, execution=None, extra=None):
        settings = {"SUPERJOBS_RA_PHASE": phase_name}
        if token is not None:
            settings["SUPERJOBS_RA_JOB"] = token
        if execution is not None:
            settings["SUPERJOBS_RA_EXECUTION_ID"] = execution
        settings.update(extra or {})
        child = run_controller_phase(
            controller_python=controller_python,
            controller_dir=controller_dir,
            base_env=environment,
            phase_env=settings,
            log_dir=log_dir / name,
            label=f"controller-{len(record.controllers)}-{phase_name}",
            scenario_deadline=scenario_deadline,
            kill_reap=KILL_REAP_SECONDS,
        )
        record.controllers.append(child)
        return child

    def submit(token):
        phase("submit", token)
        return execution_id_from_submitted(state_dir, run_id, token)

    def checkpoint(which, execution, process):
        return ra_protocol.wait_for_checkpoint(
            state_dir,
            name=which,
            expected_run_id=run_id,
            expected_execution_id=execution,
            deadline=deadline,
            child_process=process,
        )

    try:
        if name == SCENARIO_REDELIVERY:
            worker_one, _ = worker("worker-one", "1", "redelivery")
            worker_two, _ = worker("worker-two", "2", "redelivery")
            execution = submit("redelivery")
            checkpoint(ra_protocol.MODEL_LOAD_ENTERED, execution, worker_one)
            wait_for_work_pulls(
                state_dir, execution_id=execution, minimum=2, deadline=deadline
            )
            assert (
                count_handler_callbacks(read_events(state_dir), execution_id=execution)
                == 1
            )
            publish_release(nats_url, run_id, execution_id=execution)
            phase("await_completion", "redelivery", execution)
            checkpoint(ra_protocol.MODEL_LOAD_RELEASED, execution, worker_one)
            assert (
                count_handler_callbacks(read_events(state_dir), execution_id=execution)
                == 1
            )
            assert all(
                event.get("attempt") == 1
                for event in read_events(state_dir)
                if event.get("kind") == "work_pull"
            )
            minimum = 1
        else:
            worker_one, first_record = worker("worker-owner", "1", "a")
            first_execution = submit("a")
            checkpoint(ra_protocol.MODEL_LOAD_ENTERED, first_execution, worker_one)
            phase("snapshot_owner")
            first_snapshot = load_owner_snapshot(state_dir)
            if name == SCENARIO_CRASH:
                _hard_kill_worker(worker_one, first_record, deadline=scenario_deadline)
                assert worker_one.poll() is not None
                append_event(
                    state_dir,
                    kind="holder_exit",
                    run_id=run_id,
                    execution_id=first_execution,
                    extra={"reason": "supervisor confirmed process-tree exit"},
                )
            worker_two, _ = worker("worker-competitor", "2", "b")
            second_execution = submit("b")
            phase("observe_waiting", "b", second_execution)
            assert (
                count_handler_callbacks(
                    read_events(state_dir), execution_id=second_execution
                )
                == 0
            )
            if name == SCENARIO_CRASH:
                phase(
                    "resolve_termination",
                    extra={
                        "SUPERJOBS_RA_TERMINATION_EVIDENCE": f"owned PID {first_record.pid} and descendants exited; exit={worker_one.returncode}",
                    },
                )
            elif name == SCENARIO_BROKER_RESTART:
                assert owner is not None and broker_target is not None
                store_dir = _broker_store_dir(broker_target)
                record.broker_store_fingerprint_before = _broker_store_fingerprint(
                    store_dir
                )
                restarted = owner.restart(
                    broker_target, deadline=min(deadline, time.monotonic() + 30)
                )
                assert restarted.url == nats_url
                record.broker_store_fingerprint_after = _broker_store_fingerprint(
                    store_dir
                )
                assert worker_one.poll() is None and worker_two.poll() is None
                phase("snapshot_owner")
                restored = load_owner_snapshot(state_dir)
                assert restored["execution_id"] == first_snapshot["execution_id"]
                assert restored["fencing"] == first_snapshot["fencing"]
                phase("observe_waiting", "b", second_execution)
                assert (
                    count_handler_callbacks(
                        read_events(state_dir), execution_id=second_execution
                    )
                    == 0
                )
            if name != SCENARIO_CRASH:
                publish_release(nats_url, run_id, execution_id=first_execution)
                checkpoint(ra_protocol.MODEL_LOAD_RELEASED, first_execution, worker_one)
                phase("await_completion", "a", first_execution)
            checkpoint(ra_protocol.MODEL_LOAD_ENTERED, second_execution, worker_two)
            if name == SCENARIO_CRASH:
                phase("stale_release")
                stale = ra_protocol.read_checkpoint(
                    state_dir, ra_protocol.STALE_RELEASE_REJECTED
                )
                assert stale.release_outcome in {"stale", "not_owner"}
            publish_release(nats_url, run_id, execution_id=second_execution)
            phase("await_completion", "b", second_execution)
            checkpoint(ra_protocol.MODEL_LOAD_RELEASED, second_execution, worker_two)
            minimum = 2
        write_worker_stop(state_dir, run_id)
        for tracked_child in tracked:
            if tracked_child.process.poll() is None:
                _reap_child(
                    tracked_child.process,
                    label=tracked_child.record.name,
                    deadline=deadline,
                    expected_code=WORKER_EXIT_OK,
                )
                tracked_child.record.exit_code = WORKER_EXIT_OK
        record.holder_max = assert_holder_evidence_from_state(
            state_dir, maximum=1, minimum_enters=minimum
        )
    except BaseException as failure:
        record.error = f"{type(failure).__name__}: {failure}"
        raise
    finally:
        try:
            finalize_tracked(
                tracked,
                deadline=max(scenario_deadline, time.monotonic() + KILL_REAP_SECONDS),
            )
        finally:
            record.ended_at = time.monotonic()
            record.checkpoints = _collect_checkpoint_snapshot(state_dir)
            record.observations = collect_scenario_observations(
                state_dir,
                worker_logs=[
                    item.stdout_path for item in record.workers if item.stdout_path
                ],
            )
            _write_evidence(
                log_dir / name / "scenario.json", _scenario_record_json(record)
            )
    return record


def _run_cross_job_scenario(**kwargs):
    return _run_scenario(SCENARIO_CROSS_JOB, **kwargs)


def _run_redelivery_scenario(**kwargs):
    return _run_scenario(SCENARIO_REDELIVERY, **kwargs)


def _run_crash_scenario(**kwargs):
    return _run_scenario(SCENARIO_CRASH, **kwargs)


def _run_broker_restart_scenario(**kwargs):
    return _run_scenario(SCENARIO_BROKER_RESTART, **kwargs)


def _pass_payload(state: PythonPassState) -> dict[str, Any]:
    return {
        "python_version": state.python_version,
        "runtime_version": state.runtime_version,
        "controller_interpreter": str(state.controller_py),
        "worker_interpreter": str(state.worker_py),
        "origins": state.origins,
        "api_compat": state.api_compat,
        "error": state.error,
        "broker_log": state.broker_log,
        "scenarios": [_scenario_record_json(item) for item in state.scenario_records],
    }


def verify_python_version(
    *,
    python_version: str,
    work_dir: Path,
    library: Path,
    contract: Path,
    artifact_dir: Path,
) -> PythonPassState:
    tag = runtime_evidence_tag(python_version)
    state = PythonPassState(
        python_version=python_version,
        tag=tag,
        controller_py=Path(),
        worker_py=Path(),
        controller_dir=Path(),
        worker_dir=Path(),
    )
    evidence_dir = artifact_dir / tag
    evidence_dir.mkdir(parents=True, exist_ok=True)
    owner: OwnedNatsServer | None = None
    scenario_deadline = time.monotonic() + SCENARIO_TIMEOUT_SECONDS
    try:
        controller_venv = create_wheel_venv(
            work_dir,
            python_version,
            library,
            contract,
            name=f"venv-controller-{tag}",
        )
        worker_venv = create_wheel_venv(
            work_dir,
            python_version,
            library,
            contract,
            name=f"venv-worker-{tag}",
        )
        state.controller_py = venv_python(controller_venv)
        state.worker_py = venv_python(worker_venv)
        state.runtime_version = assert_final_runtime_python(state.controller_py)
        assert_final_runtime_python(state.worker_py)

        state.controller_dir = materialize_role_dir(
            work_dir / tag,
            "controller",
            CONTROLLER_SUPPORT_FILES,
        )
        state.worker_dir = materialize_role_dir(
            work_dir / tag,
            "worker",
            WORKER_SUPPORT_FILES,
        )

        compat = probe_api_compat(state.controller_py, state.controller_dir)
        state.api_compat = compat
        if not compat.get("ok"):
            raise ResourceAdmissionError(
                "installed library missing resource admission integration: "
                + "; ".join(compat.get("errors") or []),
            )

        state.origins = {
            "controller": probe_role_origins(
                state.controller_py,
                state.controller_dir,
                role="controller",
                evidence_dir=evidence_dir,
                python_version=python_version,
            ),
            "worker": probe_role_origins(
                state.worker_py,
                state.worker_dir,
                role="worker",
                evidence_dir=evidence_dir,
                python_version=python_version,
            ),
        }

        owner = OwnedNatsServer()
        target = owner.start()
        nats_url = target.url
        artifact_scenarios = evidence_dir / "scenarios"

        scenario_runners = [
            _run_cross_job_scenario,
            _run_redelivery_scenario,
            _run_crash_scenario,
            lambda **kwargs: _run_broker_restart_scenario(
                broker_target=target,
                owner=owner,
                **kwargs,
            ),
        ]
        for runner in scenario_runners:
            scenario_deadline = time.monotonic() + 90.0
            record = runner(
                nats_url=nats_url,
                controller_python=state.controller_py,
                worker_python=state.worker_py,
                controller_dir=state.controller_dir,
                worker_dir=state.worker_dir,
                scenario_deadline=scenario_deadline,
                log_dir=artifact_scenarios,
            )
            state.scenario_records.append(record)
            if record.error:
                raise ResourceAdmissionError(record.error)
            _write_evidence(
                artifact_scenarios / record.name / record.run_id / "scenario.json",
                _scenario_record_json(record),
            )
    except BaseException as exc:
        state.error = str(exc)
        raise
    finally:
        broker_log = ""
        if owner is not None and owner.target is not None:
            try:
                owner.stop(owner.target)
            except BaseException as exc:
                if state.error is None:
                    state.error = f"Broker cleanup failed: {exc}"
            log_path = owner.target.log_path
            if log_path is not None and log_path.is_file():
                broker_log = log_path.read_text(encoding="utf-8")
        state.broker_log = broker_log
        _write_evidence(evidence_dir / "summary.json", _pass_payload(state))
    return state


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--python",
        action="append",
        dest="python_versions",
        help=f"Python runtime matrix (default: {', '.join(DEFAULT_RUNTIMES)}).",
    )
    parser.add_argument(
        "--artifact-dir",
        type=Path,
        help="Directory for per-run JSON evidence (origins, scenario logs, broker log).",
    )
    parser.add_argument(
        "--keep-work",
        action="store_true",
        help="Retain temporary work directories after a successful run.",
    )
    parser.add_argument(
        "--work-dir",
        type=Path,
        help="Parent directory for temporary verification files (must be outside the repo).",
    )
    parser.add_argument(
        "--probe-api-only",
        action="store_true",
        help="Build wheels and probe API compatibility without running NATS scenarios.",
    )
    args = parser.parse_args(argv)
    python_versions = tuple(dict.fromkeys(args.python_versions or DEFAULT_RUNTIMES))
    parent = args.work_dir or Path(tempfile.gettempdir())
    assert_work_parent_outside_repo(parent)
    work_dir = Path(
        tempfile.mkdtemp(prefix="superjobs-resource-admission-", dir=parent)
    )
    artifact_dir = args.artifact_dir or Path(
        tempfile.mkdtemp(prefix="superjobs-resource-admission-artifacts-", dir=parent),
    )
    artifact_dir.mkdir(parents=True, exist_ok=True)
    handoff_path = (
        REPO_ROOT
        / "dist"
        / "implementation"
        / "resource-admission"
        / "process-proof-handoff.md"
    )
    handoff_path.parent.mkdir(parents=True, exist_ok=True)

    exit_code = 0
    run_error: BaseException | None = None
    compat_report: dict[str, Any] | None = None
    try:
        library, contract = build_wheels(work_dir)
        for py_version in python_versions:
            if args.probe_api_only:
                tag = runtime_evidence_tag(py_version)
                controller_venv = create_wheel_venv(
                    work_dir,
                    py_version,
                    library,
                    contract,
                    name=f"venv-controller-probe-{tag}",
                )
                controller_dir = materialize_role_dir(
                    work_dir / f"probe-{tag}",
                    "controller",
                    CONTROLLER_SUPPORT_FILES,
                )
                compat_report = probe_api_compat(
                    venv_python(controller_venv), controller_dir
                )
                _write_evidence(artifact_dir / "api_compat.json", compat_report)
                if not compat_report.get("ok"):
                    raise ResourceAdmissionError(
                        "API probe failed: "
                        + "; ".join(compat_report.get("errors") or []),
                    )
                continue
            verify_python_version(
                python_version=py_version,
                work_dir=work_dir,
                library=library,
                contract=contract,
                artifact_dir=artifact_dir,
            )
    except (
        ResourceAdmissionError,
        VerificationError,
        CrossProgramError,
        KeyboardInterrupt,
    ) as exc:
        exit_code = 1
        run_error = exc
        print(f"verify_resource_admission: {exc}", file=sys.stderr)
    except BaseException as exc:
        exit_code = 1
        run_error = exc
        print(f"verify_resource_admission: {exc}", file=sys.stderr)
    finally:
        handoff_lines = [
            "# Resource admission process proof handoff",
            "",
            f"- Artifact directory: `{artifact_dir}`",
            f"- Exit code: {exit_code}",
        ]
        if compat_report is not None:
            handoff_lines.append(f"- API compat ok: `{compat_report.get('ok')}`")
            for item in compat_report.get("errors") or []:
                handoff_lines.append(f"  - missing: {item}")
        if run_error is not None:
            handoff_lines.append(f"- Last error: `{run_error}`")
        if compat_report is not None and not compat_report.get("ok"):
            handoff_lines.append("")
            handoff_lines.append("## Library integration still required (issue #61)")
            handoff_lines.append(
                "The runner is present; end-to-end process proof is blocked until the "
                "installed wheel exposes:",
            )
            handoff_lines.append(
                "- `HandlerCatalog.handler(job, resources={key: 1})`",
            )
            handoff_lines.append(
                "- `SuperJobs(..., resource_capacities={key: 1})` and `jobs.resources` "
                "(`snapshot()`, `resolve_terminated_incarnation(TerminationConfirmation(...))`)",
            )
            handoff_lines.append(
                "- `JobState.WAITING_FOR_RESOURCE` with value `waiting_for_resource`",
            )
            handoff_lines.append(
                "- Handler acquire/release inside the user callback with cleanup in `finally` "
                "before return",
            )
        elif run_error is None and args.probe_api_only:
            handoff_lines.append(
                "- API compatibility probe succeeded; NATS process scenarios were not run.",
            )
        elif run_error is None:
            handoff_lines.append(
                "- Scenarios executed against owned NATS; see artifact JSON for checkpoints and logs.",
            )
        handoff_lines.append("")
        handoff_lines.append("## Scenarios (when API is available)")
        for scenario in ALL_SCENARIOS:
            handoff_lines.append(f"- `{scenario}`")
        handoff_lines.append("")
        handoff_lines.append("## Command")
        handoff_lines.append("```")
        handoff_lines.append(
            "uv run python tools/verify_resource_admission.py --python 3.12 --artifact-dir dist/verification/resource-admission",
        )
        handoff_lines.append("```")
        handoff_path.write_text("\n".join(handoff_lines) + "\n", encoding="utf-8")
        print(f"Artifact directory: {artifact_dir}", file=sys.stderr)
        print(f"Handoff notes: {handoff_path}", file=sys.stderr)
        if run_error is not None:
            _write_evidence(artifact_dir / "run_error.json", {"error": str(run_error)})
        if not args.keep_work and work_dir.exists():
            shutil.rmtree(work_dir, ignore_errors=True)

    if exit_code == 0 and not args.probe_api_only:
        print("verify_resource_admission: OK")
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
