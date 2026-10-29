"""Real owned-process regressions for failures before worker readiness."""

import itertools
import json
import subprocess
import sys
import time

import pytest

from tests.support.shared_resource_namespace import process_proof as proof


def native_sleep():
    return subprocess.Popen(
        [proof.native_interpreter_executable(), "-c", "import time; time.sleep(60)"],
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True,
    )


def reap_native(process):
    if process.poll() is None:
        process.kill()
    process.wait(timeout=5)
    if process.stderr is not None:
        process.stderr.close()
    record = proof._OWNED.get(id(process))
    if record is not None:
        # This fallback only owns a directly spawned native interpreter.
        assert record.interpreter_pid == process.pid
        record.exit_proven = True
        if record.handle is not None:
            record.handle.close()
            record.handle = None


@pytest.mark.parametrize("failure", ["marker", "foreign_marker", "handle", "timeout"])
def test_startup_failure_reaps_actual_native_interpreter(monkeypatch, tmp_path, failure):
    process = native_sleep()
    try:
        if failure == "marker":
            (tmp_path / "worker_started_a.json").write_text('{"pid":0}')
        elif failure == "foreign_marker":
            (tmp_path / "worker_started_a.json").write_text(json.dumps({"pid": process.pid + 1}))
        elif failure == "handle":
            def fail_open(pid):
                assert proof._OWNED[id(process)].process is process
                raise PermissionError("injected handle-open failure")
            monkeypatch.setattr(proof, "_open_native_handle", fail_open)
        else:
            clock = itertools.chain([0.0, 11.0], itertools.repeat(11.0))
            monkeypatch.setattr(proof.time, "monotonic", lambda: next(clock))
        with pytest.raises((proof.ProcessProofError, PermissionError)):
            proof.register_owned_worker(process, tmp_path, "a")
        record = proof._OWNED[id(process)]
        assert process.poll() is not None
        assert record.exit_proven and record.evidence is None
        assert process.pid not in proof.audit_no_worker_entry_processes()
    finally:
        reap_native(process)


@pytest.mark.skipif(sys.platform != "win32", reason="Windows launcher/interpreter split")
@pytest.mark.parametrize("failure", ["marker", "handle"])
def test_early_launcher_exit_keeps_actual_interpreter_owned(monkeypatch, tmp_path, failure):
    from tools.worker_recovery_support.spawn_pid import marker_pid_in_spawn_tree
    from tools.worker_recovery_support.windows_process import OwnedWindowsProcess

    identity = tmp_path / "child.json"
    exit_launcher = tmp_path / "exit-launcher"
    executable = proof.native_interpreter_executable()
    source = (
        "import subprocess,time,json;from pathlib import Path;"
        f"child=subprocess.Popen([{executable!r},'-c','import time;time.sleep(60)']);"
        f"Path({str(identity)!r}).write_text(json.dumps({{'pid':child.pid}}));"
        f"gate=Path({str(exit_launcher)!r});"
        "\nwhile not gate.exists(): time.sleep(.01)"
    )
    launcher = subprocess.Popen(
        [executable, "-c", source], stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE, text=True,
    )
    backup = None
    record = None
    try:
        end = time.monotonic() + 5
        while not identity.exists() and time.monotonic() < end:
            time.sleep(.01)
        interpreter_pid = json.loads(identity.read_text())["pid"]
        assert marker_pid_in_spawn_tree(launcher.pid, interpreter_pid)
        backup = OwnedWindowsProcess(interpreter_pid)
        record = proof._OwnedSpawn(launcher, interpreter_pid, OwnedWindowsProcess(interpreter_pid))
        proof._OWNED[id(launcher)] = record
        exit_launcher.write_text("1")
        launcher.wait(timeout=5)
        assert not backup.exited(deadline=time.monotonic())
        if failure == "marker":
            (tmp_path / "worker_started_a.json").write_text('{"pid":0}')
            with pytest.raises(proof.ProcessProofError, match="invalid interpreter PID"):
                proof.register_owned_worker(launcher, tmp_path, "a")
            assert record.exit_proven and record.evidence is None
            assert backup.exited(deadline=time.monotonic() + 5)
            assert interpreter_pid not in proof.audit_no_worker_entry_processes()
        else:
            record.handle.close()
            record.handle = None
            def fail_open(pid):
                raise PermissionError("injected unavailable interpreter handle")
            monkeypatch.setattr(proof, "_open_native_handle", fail_open)
            with pytest.raises(BaseExceptionGroup, match="cleanup is unconfirmed"):
                proof.register_owned_worker(launcher, tmp_path, "a")
            assert not record.exit_proven and record.evidence is None
            assert interpreter_pid in proof.audit_no_worker_entry_processes()
            assert not backup.exited(deadline=time.monotonic())
    finally:
        if backup is not None:
            if not backup.exited(deadline=time.monotonic()):
                try:
                    backup.terminate()
                except OSError:
                    if not backup.exited(deadline=time.monotonic() + 5):
                        raise
            assert backup.exited(deadline=time.monotonic() + 5)
            if record is not None:
                record.exit_proven = True
                if record.handle is not None:
                    record.handle.close()
                    record.handle = None
            backup.close()
        if launcher.poll() is None:
            launcher.kill()
        launcher.wait(timeout=5)
        if launcher.stderr is not None:
            launcher.stderr.close()
