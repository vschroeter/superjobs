"""A launcher exit must never fabricate actual-interpreter termination evidence."""

import io
import time
from types import SimpleNamespace

import pytest

from tests.support.shared_resource_namespace import process_proof as proof


class Handle:
    def __init__(self, *, exits_on_terminate):
        self.alive = True
        self.exits_on_terminate = exits_on_terminate
        self.closed = False
        self.terminations = 0

    def exited(self, *, deadline):
        return not self.alive

    def terminate(self):
        self.terminations += 1
        if self.exits_on_terminate:
            self.alive = False

    def close(self):
        self.closed = True


def owned(monkeypatch, handle):
    process = SimpleNamespace(pid=100001, returncode=0, stdout=None, stderr=io.StringIO())
    process.poll = lambda: process.returncode
    process.wait = lambda **kwargs: process.returncode
    process.kill = lambda: None
    record = proof._OwnedSpawn(process, 100002, handle)
    monkeypatch.setitem(proof._OWNED, process.pid, record)
    marker = proof.ReadyMarker("a", 100002, "worker-a", "incarnation-a")
    return process, record, marker


def test_unconfirmed_interpreter_exit_cannot_return_recovery_proof(monkeypatch, tmp_path):
    handle = Handle(exits_on_terminate=False)
    process, record, marker = owned(monkeypatch, handle)
    with pytest.raises(proof.ProcessProofError, match="interpreter exit is unconfirmed"):
        proof.finalize_owned_worker(
            process, state_dir=tmp_path, worker_label="a", deadline=time.monotonic() + 1,
            marker=marker, hard=True, signal_stop=False, evidence_dir=tmp_path,
        )
    assert not record.exit_proven
    assert record.evidence is None
    assert not handle.closed
    assert not list(tmp_path.glob("*_exit.json"))


def test_actual_handle_exit_is_required_even_after_launcher_exit(monkeypatch, tmp_path):
    handle = Handle(exits_on_terminate=True)
    process, record, marker = owned(monkeypatch, handle)
    result = proof.finalize_owned_worker(
        process, state_dir=tmp_path, worker_label="a", deadline=time.monotonic() + 1,
        marker=marker, hard=True, signal_stop=False,
    )
    assert handle.terminations == 1
    assert handle.closed and record.exit_proven
    assert result.interpreter_pid != result.launcher_pid
    assert result.interpreter_terminated


def test_foreign_marker_is_rejected_before_any_process_action(monkeypatch, tmp_path):
    handle = Handle(exits_on_terminate=True)
    process, record, marker = owned(monkeypatch, handle)
    foreign = proof.ReadyMarker("a", marker.pid + 1, marker.worker_id, marker.incarnation_id)
    with pytest.raises(proof.ProcessProofError, match="disagrees"):
        proof.finalize_owned_worker(
            process, state_dir=tmp_path, worker_label="a", deadline=time.monotonic() + 1,
            marker=foreign, hard=True, signal_stop=False,
        )
    assert not handle.terminations and not record.exit_proven
