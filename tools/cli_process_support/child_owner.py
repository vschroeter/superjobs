"""Context-managed subprocess ownership with descendant cleanup."""

from __future__ import annotations

import subprocess
import time
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from typing import Any, TextIO

from scripts.dev_check.process_tree import kill_process_tree

from tools.cli_process_support.protocol import ProtocolError

KILL_REAP_SECONDS = 5.0
POLL_INTERVAL_SECONDS = 0.05

_current_owner: ContextVar[ChildOwner | None] = ContextVar("cli_process_child_owner", default=None)


def current_child_owner() -> ChildOwner | None:
    return _current_owner.get()


@dataclass
class OwnedChild:
    label: str
    process: subprocess.Popen[str]
    stdout_io: TextIO | None = None
    stderr_io: TextIO | None = None
    expected_exit: int | None = None
    forced_kill: bool = False
    record: Any | None = None

    def finalize_record(self) -> None:
        if self.record is None:
            return
        code = self.process.poll()
        if code is not None:
            self.record.exit_code = code
        if self.record.ended_at is None:
            self.record.ended_at = time.monotonic()
        if self.record.pid is None and self.process.pid is not None:
            self.record.pid = self.process.pid

    def close_streams(self) -> None:
        for stream in (self.stdout_io, self.stderr_io):
            if stream is None:
                continue
            try:
                stream.close()
            except OSError:
                pass


@dataclass
class ChildOwner:
    """Track spawned children and kill entire trees on every exit path."""

    children: list[OwnedChild] = field(default_factory=list)
    cleanup_errors: list[str] = field(default_factory=list)
    forced_kills: list[str] = field(default_factory=list)
    allow_forced_cleanup: bool = False
    _token: Token[ChildOwner | None] | None = field(default=None, repr=False)

    def __enter__(self) -> ChildOwner:
        self._token = _current_owner.set(self)
        return self

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> bool:
        try:
            self.cleanup_all()
        finally:
            if self._token is not None:
                _current_owner.reset(self._token)
                self._token = None
        problems = list(self.cleanup_errors)
        if self.forced_kills and exc_type is None and not self.allow_forced_cleanup:
            problems.append("unexpected forced cleanup: " + "; ".join(self.forced_kills))
        if problems:
            diagnostic = "child cleanup failed: " + "; ".join(problems)
            if isinstance(exc_val, BaseException):
                exc_val.add_note(diagnostic)
            else:
                raise ProtocolError(diagnostic)
        return False

    def register(self, child: OwnedChild) -> OwnedChild:
        self.children.append(child)
        return child

    def owned_for_process(self, process: subprocess.Popen[str]) -> OwnedChild | None:
        for child in self.children:
            if child.process is process:
                return child
        return None

    def force_kill(self, child: OwnedChild, *, reason: str) -> None:
        if child.process.poll() is not None:
            child.finalize_record()
            return
        child.forced_kill = True
        self.forced_kills.append(f"{child.label}: {reason}")
        self.cleanup_errors.extend(kill_process_tree(child.process.pid or 0))
        deadline = time.monotonic() + KILL_REAP_SECONDS
        while time.monotonic() < deadline:
            if child.process.poll() is not None:
                break
            time.sleep(POLL_INTERVAL_SECONDS)
        if child.process.poll() is None:
            self.cleanup_errors.append(f"{child.label}: did not exit after forced kill")
        child.finalize_record()
        child.close_streams()

    def cleanup_all(self) -> None:
        deadline = time.monotonic() + KILL_REAP_SECONDS
        for child in reversed(self.children):
            if child.process.poll() is None:
                self.force_kill(child, reason="owner cleanup")
                continue
            while time.monotonic() < deadline and child.process.poll() is None:
                time.sleep(POLL_INTERVAL_SECONDS)
            child.finalize_record()
            child.close_streams()

    def assert_no_forced_kills(self) -> None:
        if self.forced_kills:
            raise ProtocolError(
                "forced child termination is a scenario failure: "
                + "; ".join(self.forced_kills),
            )


def reap_with_owner(
    child: OwnedChild,
    *,
    owner: ChildOwner,
    deadline: float,
    poll_interval: float = POLL_INTERVAL_SECONDS,
    allow_forced_kill: bool = False,
) -> int:
    """Wait for exit; kill process tree on timeout unless allow_forced_kill."""
    wait_deadline = deadline - KILL_REAP_SECONDS
    while time.monotonic() < wait_deadline:
        code = child.process.poll()
        if code is not None:
            if child.expected_exit is not None and code != child.expected_exit:
                raise ProtocolError(
                    f"{child.label} exit {code} != expected {child.expected_exit}",
                )
            child.finalize_record()
            child.close_streams()
            return code
        time.sleep(poll_interval)
    if child.process.poll() is None:
        owner.force_kill(child, reason="reap timeout")
        if not allow_forced_kill:
            owner.assert_no_forced_kills()
    code = child.process.poll()
    if code is None:
        raise ProtocolError(f"{child.label} did not exit after forced kill")
    if child.expected_exit is not None and code != child.expected_exit:
        raise ProtocolError(
            f"{child.label} exit {code} != expected {child.expected_exit}",
        )
    child.finalize_record()
    return code
