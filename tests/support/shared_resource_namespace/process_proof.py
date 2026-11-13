"""Keep exact owned interpreter handles until test termination is proven."""

from __future__ import annotations

import json
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


class ProcessProofError(RuntimeError):
    """No trusted termination evidence is available for an owned test worker."""


@dataclass(frozen=True)
class ReadyMarker:
    worker_label: str
    pid: int
    worker_id: str
    incarnation_id: str


@dataclass(frozen=True)
class TerminationEvidence:
    launcher_pid: int
    interpreter_pid: int
    worker_id: str
    incarnation_id: str
    interpreter_terminated: bool
    launcher_exit: int
    mode: str

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class _OwnedSpawn:
    process: subprocess.Popen[str]
    interpreter_pid: int
    handle: Any = None
    exit_proven: bool = False
    evidence: TerminationEvidence | None = None


# Only spawn_worker registers these exact children. No OS-wide name/PID scans.
_OWNED: dict[int, _OwnedSpawn] = {}


def ready_marker_from_payload(payload: dict[str, Any]) -> ReadyMarker:
    return ReadyMarker(
        worker_label=payload["worker_label"], pid=payload["pid"],
        worker_id=payload["worker_id"], incarnation_id=payload["incarnation_id"],
    )


def load_ready_marker(state_dir: Path, worker_label: str) -> ReadyMarker:
    path = state_dir / f"worker_ready_{worker_label}.json"
    if not path.is_file():
        raise ProcessProofError(f"missing ready marker for worker {worker_label!r}")
    return ready_marker_from_payload(json.loads(path.read_text(encoding="utf-8")))


def native_interpreter_executable() -> str:
    """Use the actual CPython binary, never the Windows venv redirector."""
    executable = getattr(sys, "_base_executable", None) if sys.platform == "win32" else sys.executable
    if not executable or not Path(executable).is_file():
        raise ProcessProofError("native interpreter executable is unavailable")
    return str(Path(executable).resolve())


def _open_native_handle(pid: int):
    if sys.platform == "win32":
        from tools.worker_recovery_support.windows_process import OwnedWindowsProcess

        return OwnedWindowsProcess(pid)
    return None


def register_owned_worker(process: subprocess.Popen[str], state_dir: Path, label: str) -> None:
    """Retain native Popen ownership before reading any fallible startup marker.

    spawn_worker uses the actual interpreter binary. Popen therefore already owns
    its stable process handle, even if opening an additional Windows handle fails.
    Registry identity is the retained Popen object, never a reusable numeric PID.
    """
    owned = _OWNED.setdefault(id(process), _OwnedSpawn(process, process.pid))
    if owned.process is not process:
        raise ProcessProofError("owned process identity changed")
    started = state_dir / f"worker_started_{label}.json"
    deadline = time.monotonic() + 10
    try:
        if owned.handle is None:
            owned.handle = _open_native_handle(owned.interpreter_pid)
        while time.monotonic() < deadline:
            if started.is_file():
                try:
                    pid = json.loads(started.read_text(encoding="utf-8"))["pid"]
                except (json.JSONDecodeError, KeyError):
                    time.sleep(.01)
                    continue
                if type(pid) is not int or pid <= 0:
                    raise ProcessProofError("invalid interpreter PID")
                if pid != owned.interpreter_pid:
                    raise ProcessProofError("startup marker disagrees with the owned native interpreter")
                return
            if process.poll() is not None:
                raise ProcessProofError("worker exited before its startup identity was captured")
            time.sleep(.01)
        raise ProcessProofError("worker did not report its interpreter identity")
    except BaseException as startup_error:
        try:
            finalize_owned_worker(
                process, state_dir=state_dir, worker_label=label,
                deadline=time.monotonic(), signal_stop=False, read_ready_marker=False,
            )
        except BaseException as cleanup_error:
            raise BaseExceptionGroup(
                "Worker startup failed and actual interpreter cleanup is unconfirmed",
                [startup_error, cleanup_error],
            ) from None
        raise


def _interpreter_exited(owned: _OwnedSpawn, deadline: float) -> bool:
    if owned.exit_proven:
        return True
    if owned.handle is not None:
        return owned.handle.exited(deadline=deadline)
    if owned.interpreter_pid != owned.process.pid:
        raise ProcessProofError("actual interpreter handle is unavailable")
    try:
        owned.process.wait(timeout=max(0, deadline - time.monotonic()))
        return True
    except subprocess.TimeoutExpired:
        return False


def _terminate_interpreter(owned: _OwnedSpawn) -> None:
    if not _interpreter_exited(owned, time.monotonic()):
        try:
            if owned.handle is not None:
                owned.handle.terminate()
            else:
                owned.process.kill()
        except OSError:
            # TerminateProcess can return access denied after a concurrent exit.
            # Accept only an actual exit observed through the same held handle.
            if not _interpreter_exited(owned, time.monotonic() + 5):
                raise


def finalize_owned_worker(
    process: subprocess.Popen[str], *, state_dir: Path, worker_label: str,
    deadline: float, marker: ReadyMarker | None = None, hard: bool = False,
    signal_stop: bool = True, evidence_dir: Path | None = None,
    read_ready_marker: bool = True,
) -> TerminationEvidence | None:
    """Confirm both actual interpreter and launcher exit before returning evidence.

    The Windows handle was opened at spawn registration, before graceful stop or
    forced termination. PID reuse and disappearance of a launcher are not proofs.
    These dummy test interpreters do not launch resource-consuming descendants.
    """
    owned = _OWNED.get(id(process))
    if owned is None or owned.process is not process:
        raise ProcessProofError("process was not registered as this test's owned child")
    if marker is None and read_ready_marker:
        try:
            marker = load_ready_marker(state_dir, worker_label)
        except ProcessProofError:
            if hard:
                raise
    if marker is not None and (
        marker.pid != owned.interpreter_pid or marker.worker_label != worker_label
    ):
        raise ProcessProofError("ready marker disagrees with the captured interpreter")
    if owned.exit_proven:
        return owned.evidence
    if signal_stop:
        (state_dir / f"stop_{worker_label}").write_text("1", encoding="utf-8")
    forced = hard
    if hard:
        _terminate_interpreter(owned)
    else:
        graceful_end = min(deadline, time.monotonic() + 2)
        while time.monotonic() < graceful_end:
            if _interpreter_exited(owned, time.monotonic()):
                break
            time.sleep(.02)
        if not _interpreter_exited(owned, time.monotonic()):
            forced = True
            _terminate_interpreter(owned)
    # Reserve a bounded reap budget even if the scenario's observation expired.
    reap_deadline = time.monotonic() + 5
    if not _interpreter_exited(owned, reap_deadline):
        raise ProcessProofError("actual interpreter exit is unconfirmed")
    try:
        process.wait(timeout=max(.1, reap_deadline - time.monotonic()))
    except subprocess.TimeoutExpired:
        # The interpreter is proven dead; only its owned launcher remains.
        process.kill()
        process.wait(timeout=2)
    if process.poll() is None:
        raise ProcessProofError("owned launcher exit is unconfirmed")
    owned.exit_proven = True
    if owned.handle is not None:
        owned.handle.close()
        owned.handle = None
    if marker is not None:
        owned.evidence = TerminationEvidence(
            launcher_pid=process.pid, interpreter_pid=owned.interpreter_pid,
            worker_id=marker.worker_id, incarnation_id=marker.incarnation_id,
            interpreter_terminated=True, launcher_exit=process.returncode,
            mode="hard_kill" if forced else "graceful",
        )
    # Both processes have exited, so no live child can retain this stderr pipe.
    stderr = process.stderr.read(65536) if process.stderr is not None else ""
    for stream in (process.stdout, process.stderr):
        if stream is not None:
            stream.close()
    if evidence_dir is not None:
        evidence_dir.mkdir(parents=True, exist_ok=True)
        payload = {"worker_label": worker_label, "interpreter_pid": owned.interpreter_pid,
                   "launcher_exit": process.returncode, "interpreter_terminated": True,
                   "stderr_tail": stderr[-4000:]}
        if owned.evidence is not None:
            payload["termination"] = owned.evidence.to_json()
        (evidence_dir / f"worker_{worker_label}_exit.json").write_text(
            json.dumps(payload, indent=2), encoding="utf-8",
        )
    return owned.evidence


def audit_no_worker_entry_processes() -> list[int]:
    """Inspect only registered owned handles; never scan unrelated processes."""
    return [owned.interpreter_pid for owned in _OWNED.values() if not owned.exit_proven]
