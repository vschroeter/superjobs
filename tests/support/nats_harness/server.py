"""Owned native JetStream servers; stop always removes the owned store."""
from __future__ import annotations

import asyncio
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

import nats

from .pinned import STARTUP_TIMEOUT_SECONDS, SHUTDOWN_TIMEOUT_SECONDS
from .provision import resolve_executable


def _phase_deadline(supplied: float | None, phase_seconds: float) -> float:
    cap = time.monotonic() + phase_seconds
    if supplied is None:
        return cap
    return min(supplied, cap)


@dataclass
class NatsServerTarget:
    url: str = ""
    owned: bool = False
    work_dir: Path | None = None
    log_path: Path | None = None
    process: subprocess.Popen | None = None


async def wait_for_jetstream_ready(url: str, *, deadline: float) -> None:
    last_error = None

    async def record_error(error):
        nonlocal last_error
        last_error = error

    try:
        async with asyncio.timeout(max(0, deadline - time.monotonic())):
            while True:
                connection = None
                try:
                    connection = await nats.connect(
                        url, connect_timeout=0.5, max_reconnect_attempts=0,
                        error_cb=record_error,
                    )
                    await connection.jetstream(timeout=0.5).account_info()
                    return
                except Exception as error:
                    last_error = error
                finally:
                    if connection is not None:
                        await connection.close()
                await asyncio.sleep(0.05)
    except TimeoutError as error:
        raise TimeoutError(f"JetStream unavailable at {url}: {last_error}") from error


class OwnedNatsServer:
    def __init__(self, *, extra_config: str = "") -> None:
        self.target: NatsServerTarget | None = None
        self._executable: Path | None = None
        self._extra_config = extra_config

    def start(self, *, deadline: float | None = None) -> NatsServerTarget:
        if self.target is not None:
            raise RuntimeError("Use restart() for an existing owned server")
        # Provisioning is separate from the 30-second native startup budget.
        self._executable = resolve_executable()
        root = Path(tempfile.mkdtemp(prefix="superjobs-nats-"))
        log_root = Path(os.environ.get("SUPERJOBS_NATS_LOG_DIR", tempfile.gettempdir()))
        self.target = NatsServerTarget(owned=True, work_dir=root)
        try:
            log_root.mkdir(parents=True, exist_ok=True)
            self.target.log_path = log_root / f"{root.name}.log"
            self.target.log_path.touch()
            return self._launch(port=-1, deadline=deadline)
        except BaseException as exc:
            try:
                self.stop(self.target, deadline=deadline)
            except BaseException as cleanup_exc:
                raise RuntimeError(
                    f"{type(exc).__name__}: {exc}; cleanup: {cleanup_exc}",
                ) from exc
            raise

    def _reap_launch_process(self, target: NatsServerTarget, *, deadline: float) -> None:
        process = target.process
        if process is None:
            return
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=min(5, max(0, deadline - time.monotonic())))
            except subprocess.TimeoutExpired:
                process.kill()
        try:
            process.wait(timeout=max(0, deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            # Retain ownership so the caller's reserved cleanup can reap it.
            return
        target.process = None

    def _launch(self, *, port: int, deadline: float | None = None) -> NatsServerTarget:
        target = self.target
        assert target is not None and target.work_dir and target.log_path
        deadline = _phase_deadline(deadline, STARTUP_TIMEOUT_SECONDS)
        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"NATS launch deadline expired before startup; log: {target.log_path}",
            )
        config = target.work_dir / "server.conf"
        base = (
            f'host: 127.0.0.1\nport: {port}\n'
            f'jetstream {{ store_dir: "{(target.work_dir / "store").as_posix()}" }}\n'
        )
        extra = self._extra_config
        if extra and not extra.endswith("\n"):
            extra = f"{extra}\n"
        config.write_text(base + extra, encoding="utf-8")
        offset = target.log_path.stat().st_size
        with target.log_path.open("ab") as log:
            target.process = subprocess.Popen(
                [str(self._executable), "-c", str(config)],
                stdout=log, stderr=subprocess.STDOUT,
                creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0,
            )
        while time.monotonic() < deadline:
            if target.process.poll() is not None:
                raise RuntimeError(f"NATS startup exited {target.process.returncode}; log: {target.log_path}")
            with target.log_path.open("rb") as log:
                log.seek(offset)
                match = re.search(rb"Listening for client connections on 127\.0\.0\.1:(\d+)", log.read())
            if match:
                target.url = f"nats://127.0.0.1:{int(match.group(1))}"
                try:
                    asyncio.run(wait_for_jetstream_ready(target.url, deadline=deadline))
                except BaseException:
                    self._reap_launch_process(target, deadline=deadline)
                    raise
                return target
            time.sleep(0.025)
        self._reap_launch_process(target, deadline=deadline)
        raise TimeoutError(f"NATS startup exceeded 30 seconds; log: {target.log_path}")

    def pause(self, target: NatsServerTarget, *, deadline: float | None = None) -> None:
        """Terminate the process while retaining its owned store for recovery tests."""
        self._check_owner(target)
        process = target.process
        if process is None:
            return
        deadline = _phase_deadline(deadline, SHUTDOWN_TIMEOUT_SECONDS)
        if process.poll() is None:
            process.terminate()
        try:
            process.wait(timeout=min(5, max(0, deadline - time.monotonic())))
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=max(0, deadline - time.monotonic()))
        target.process = None

    def restart(
        self,
        target: NatsServerTarget,
        *,
        deadline: float | None = None,
    ) -> NatsServerTarget:
        """Reuse this owner's store and client port; failure performs final cleanup."""
        self._check_owner(target)
        port = int(target.url.rsplit(":", 1)[1])
        overall = deadline
        shutdown_deadline = _phase_deadline(overall, SHUTDOWN_TIMEOUT_SECONDS)
        try:
            self.pause(target, deadline=shutdown_deadline)
            launch_deadline = _phase_deadline(overall, STARTUP_TIMEOUT_SECONDS)
            return self._launch(port=port, deadline=launch_deadline)
        except BaseException as exc:
            cleanup_deadline = _phase_deadline(overall, SHUTDOWN_TIMEOUT_SECONDS)
            try:
                self.stop(target, deadline=cleanup_deadline)
            except BaseException as cleanup_exc:
                raise RuntimeError(
                    f"{type(exc).__name__}: {exc}; cleanup: {cleanup_exc}",
                ) from exc
            raise

    def _check_owner(self, target: NatsServerTarget) -> None:
        if target is not self.target or not target.owned:
            raise ValueError("Target does not belong to this server owner")

    def stop(self, target: NatsServerTarget, *, deadline: float | None = None) -> None:
        if not target.owned:
            return
        self._check_owner(target)
        self.pause(target, deadline=deadline)
        root = target.work_dir
        if root is not None and root.exists():
            # Only the root allocated by this owner is ever removed.
            shutil.rmtree(root)


def ensure_external_target(url: str) -> NatsServerTarget:
    asyncio.run(wait_for_jetstream_ready(url, deadline=time.monotonic() + STARTUP_TIMEOUT_SECONDS))
    return NatsServerTarget(url=url)
