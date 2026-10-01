"""Bounded subprocess execution with descendant cleanup on Linux and Windows."""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from scripts.dev_check.constants import (
    COMMUNICATE_AFTER_KILL_TIMEOUT_SECONDS,
    SUBPROCESS_PROBE_TIMEOUT_SECONDS,
)


@dataclass(frozen=True)
class StageProcessResult:
    returncode: int | None
    timed_out: bool
    stdout: str
    stderr: str
    duration_seconds: float
    pid: int | None
    cleanup_errors: tuple[str, ...] = ()
    interrupted: bool = False


def _decode_stream(value: str | bytes | None) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def _collect_unix_descendants(root_pid: int) -> list[int]:
    descendants: list[int] = []
    pending = [root_pid]
    while pending:
        parent = pending.pop()
        try:
            for entry in os.listdir("/proc"):
                if not entry.isdigit():
                    continue
                child = int(entry)
                try:
                    with open(f"/proc/{child}/stat", encoding="utf-8") as handle:
                        stat = handle.read()
                except OSError:
                    continue
                ppid = int(stat.rsplit(")", 1)[1].split()[1])
                if ppid == parent:
                    descendants.append(child)
                    pending.append(child)
        except OSError:
            break
    return descendants


def kill_process_tree(pid: int) -> list[str]:
    """Terminate a process and its descendants. Returns non-fatal cleanup warnings."""
    errors: list[str] = []
    if pid <= 0:
        return errors

    if sys.platform == "win32":
        try:
            completed = subprocess.run(
                ["taskkill", "/T", "/F", "/PID", str(pid)],
                capture_output=True,
                check=False,
                timeout=SUBPROCESS_PROBE_TIMEOUT_SECONDS,
            )
        except subprocess.TimeoutExpired as exc:
            errors.append(
                "taskkill pid={pid} timed out: {out} {err}".format(
                    pid=pid,
                    out=_decode_stream(exc.stdout).strip(),
                    err=_decode_stream(exc.stderr).strip(),
                ).strip()
            )
            return errors
        stdout = _decode_stream(completed.stdout)
        stderr = _decode_stream(completed.stderr)
        if completed.returncode not in (0, 128, 255):
            errors.append(
                f"taskkill pid={pid} exit={completed.returncode}: "
                f"{stdout.strip()} {stderr.strip()}".strip()
            )
        return errors

    descendants = _collect_unix_descendants(pid)
    targets = list(reversed(descendants)) + [pid]
    for target in targets:
        try:
            os.kill(target, signal.SIGTERM)
        except ProcessLookupError:
            continue
        except OSError as exc:
            errors.append(f"SIGTERM pid={target}: {exc}")
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        if not any(_pid_alive(t) for t in targets):
            break
        time.sleep(0.05)
    for target in targets:
        if not _pid_alive(target):
            continue
        try:
            os.kill(target, signal.SIGKILL)
        except ProcessLookupError:
            continue
        except OSError as exc:
            errors.append(f"SIGKILL pid={target}: {exc}")
    return errors


def _pid_alive(pid: int) -> bool:
    if sys.platform == "win32":
        try:
            completed = subprocess.run(
                ["tasklist", "/FI", f"PID eq {pid}"],
                capture_output=True,
                check=False,
                timeout=SUBPROCESS_PROBE_TIMEOUT_SECONDS,
            )
        except subprocess.TimeoutExpired as exc:
            return False
        stdout = _decode_stream(completed.stdout)
        return str(pid) in stdout
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except OSError:
        return True
    return True


def popen_kwargs() -> dict:
    kwargs: dict = {
        "stdout": subprocess.PIPE,
        "stderr": subprocess.PIPE,
        "text": True,
        "env": None,
    }
    if sys.platform == "win32":
        kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
    else:
        kwargs["start_new_session"] = True
    return kwargs


def _communicate_after_kill(process: subprocess.Popen[str]) -> tuple[str, str, list[str]]:
    errors: list[str] = []
    try:
        stdout, stderr = process.communicate(timeout=COMMUNICATE_AFTER_KILL_TIMEOUT_SECONDS)
        return stdout or "", stderr or "", errors
    except subprocess.TimeoutExpired as exc:
        errors.append(f"communicate after kill timed out for pid={process.pid}")
        return (
            _decode_stream(exc.stdout),
            _decode_stream(exc.stderr),
            errors,
        )


def run_bounded(
    args: Sequence[str],
    *,
    cwd: Path,
    env: dict[str, str] | None,
    timeout_seconds: float,
) -> StageProcessResult:
    """Run a command with a monotonic bound and kill its descendant tree on timeout."""
    merged = os.environ.copy()
    if env:
        merged.update(env)
    for key in ("PYTHONPATH", "PYTHONHOME"):
        merged.pop(key, None)

    start = time.monotonic()
    popen = {**popen_kwargs(), "cwd": cwd, "env": merged}
    try:
        process = subprocess.Popen(list(args), **popen)
    except OSError as exc:
        return StageProcessResult(
            returncode=127,
            timed_out=False,
            stdout="",
            stderr=str(exc),
            duration_seconds=time.monotonic() - start,
            pid=None,
            cleanup_errors=(),
        )

    pid = process.pid
    cleanup_errors: list[str] = []
    try:
        stdout, stderr = process.communicate(timeout=timeout_seconds)
        return StageProcessResult(
            returncode=process.returncode,
            timed_out=False,
            stdout=stdout or "",
            stderr=stderr or "",
            duration_seconds=time.monotonic() - start,
            pid=pid,
            cleanup_errors=tuple(cleanup_errors),
        )
    except subprocess.TimeoutExpired as exc:
        partial_out = _decode_stream(exc.stdout)
        partial_err = _decode_stream(exc.stderr)
        cleanup_errors.extend(kill_process_tree(pid))
        stdout, stderr, comm_errors = _communicate_after_kill(process)
        cleanup_errors.extend(comm_errors)
        if not stdout:
            stdout = partial_out
        if not stderr:
            stderr = partial_err
        return StageProcessResult(
            returncode=None,
            timed_out=True,
            stdout=stdout,
            stderr=stderr,
            duration_seconds=time.monotonic() - start,
            pid=pid,
            cleanup_errors=tuple(cleanup_errors),
        )
    except KeyboardInterrupt:
        cleanup_errors.extend(kill_process_tree(pid))
        try:
            process.kill()
        except OSError as exc:
            cleanup_errors.append(f"kill after interrupt pid={pid}: {exc}")
        stdout = ""
        stderr = ""
        try:
            stdout, stderr, comm_errors = _communicate_after_kill(process)
            cleanup_errors.extend(comm_errors)
        except KeyboardInterrupt:
            cleanup_errors.append("communicate after interrupt raised KeyboardInterrupt again")
        if not stderr.strip():
            stderr = "KeyboardInterrupt during stage subprocess"
        return StageProcessResult(
            returncode=None,
            timed_out=False,
            stdout=stdout,
            stderr=stderr,
            duration_seconds=time.monotonic() - start,
            pid=pid,
            cleanup_errors=tuple(cleanup_errors),
            interrupted=True,
        )
