from __future__ import annotations

import asyncio
import subprocess
import time
from pathlib import Path

import pytest

from tests.support.nats_harness import provision, server


def test_failed_launch_reap_retains_process_for_reserved_cleanup():
    class PendingProcess:
        def poll(self):
            return None

        def terminate(self):
            pass

        def kill(self):
            pass

        def wait(self, timeout):
            raise subprocess.TimeoutExpired("owned-server", timeout)

    process = PendingProcess()
    target = server.NatsServerTarget(owned=True, process=process)
    owner = server.OwnedNatsServer()
    owner._reap_launch_process(target, deadline=time.monotonic())
    assert target.process is process


def test_failed_restart_preserves_primary_and_cleanup_errors(monkeypatch):
    owner = server.OwnedNatsServer()
    target = server.NatsServerTarget(owned=True, url="nats://127.0.0.1:4222")
    owner.target = target
    primary = subprocess.TimeoutExpired("owned-server", 1)

    def fail_pause(*args, **kwargs):
        raise primary

    def fail_stop(*args, **kwargs):
        raise OSError("cleanup control")

    monkeypatch.setattr(owner, "pause", fail_pause)
    monkeypatch.setattr(owner, "stop", fail_stop)
    with pytest.raises(RuntimeError, match="TimeoutExpired:.*cleanup: cleanup control") as error:
        owner.restart(target)
    assert error.value.__cause__ is primary


def test_checksum_mismatch_never_extracts_or_executes(tmp_path, monkeypatch):
    monkeypatch.delenv('NATS_EXECUTABLE', raising=False)
    monkeypatch.setenv('SUPERJOBS_NATS_CACHE', str(tmp_path))
    asset = provision.platform_asset()
    root = tmp_path / provision.GITHUB_RELEASE_TAG
    root.mkdir()
    (root / asset.archive_name).write_bytes(b'corrupt archive')
    monkeypatch.setattr(provision, '_extract_executable', lambda *a: pytest.fail('extracted corrupt download'))
    monkeypatch.setattr(provision, 'verify_executable_version', lambda *a: pytest.fail('executed corrupt download'))
    with pytest.raises(provision.NatsProvisionError, match='digest mismatch'):
        provision.resolve_executable()


@pytest.mark.parametrize(('output', 'code'), [
    ('nats-server: v2.15.00', 0), ('nats-server: v2.15.0', 1), ('nats-server: v2.14.0', 0),
])
def test_wrong_version_or_failed_version_command(output, code, monkeypatch):
    monkeypatch.setattr(provision.subprocess, 'run', lambda *a, **k: subprocess.CompletedProcess(a, code, output, ''))
    with pytest.raises(provision.NatsProvisionError, match='version check failed'):
        provision.verify_executable_version(Path('nats-server'))


@pytest.mark.parametrize('failure', ['spawn', 'readiness'])
def test_failed_start_cleans_owned_root_and_retains_log(tmp_path, monkeypatch, failure):
    root = tmp_path / 'owned'
    root.mkdir()
    monkeypatch.setattr(server, 'resolve_executable', lambda: Path('server'))
    monkeypatch.setattr(server.tempfile, 'mkdtemp', lambda **k: str(root))
    monkeypatch.setenv('SUPERJOBS_NATS_LOG_DIR', str(tmp_path / 'logs'))
    owner = server.OwnedNatsServer()
    def launch(**kwargs):
        owner.target.log_path.write_text('startup diagnosis')
        raise OSError(failure)
    monkeypatch.setattr(owner, '_launch', launch)
    with pytest.raises(OSError, match=failure):
        owner.start()
    assert not root.exists()
    assert owner.target.log_path.read_text() == 'startup diagnosis'


def test_launch_caps_long_supplied_deadline(tmp_path, monkeypatch):
    root = tmp_path / 'owned'
    root.mkdir()
    monkeypatch.setattr(server, 'resolve_executable', lambda: Path('server'))
    monkeypatch.setenv('SUPERJOBS_NATS_LOG_DIR', str(tmp_path / 'logs'))
    owner = server.OwnedNatsServer()
    owner._executable = Path('server')
    log_path = tmp_path / 'owned.log'
    log_path.write_text('', encoding='utf-8')
    owner.target = server.NatsServerTarget(
        owned=True,
        work_dir=root,
        log_path=log_path,
    )

    class HungProcess:
        def poll(self):
            return None

        def terminate(self):
            return None

        def kill(self):
            return None

        def wait(self, timeout=None):
            return 0

    monkeypatch.setattr(server.subprocess, 'Popen', lambda *a, **k: HungProcess())
    mono = time.monotonic()
    def fake_monotonic():
        nonlocal mono
        mono += 0.05
        return mono
    monkeypatch.setattr(server.time, 'monotonic', fake_monotonic)
    monkeypatch.setattr(server.time, 'sleep', lambda *a: None)
    iterations = {'count': 0}
    original_bounded = server._phase_deadline

    def bounded(deadline, phase_seconds):
        iterations['count'] += 1
        return original_bounded(deadline, 0.2 if phase_seconds == server.STARTUP_TIMEOUT_SECONDS else phase_seconds)

    monkeypatch.setattr(server, '_phase_deadline', bounded)
    long_deadline = fake_monotonic() + 600
    with pytest.raises(TimeoutError, match='30 seconds'):
        owner._launch(port=4222, deadline=long_deadline)
    assert owner.target.process is None


@pytest.mark.asyncio
async def test_core_connection_without_jetstream_is_not_ready(monkeypatch):
    class Connection:
        def jetstream(self, **kwargs):
            return self
        async def account_info(self):
            raise RuntimeError('JetStream unavailable')
        async def close(self):
            pass
    async def connect(*a, **k):
        return Connection()
    monkeypatch.setattr(server.nats, 'connect', connect)
    started = time.monotonic()
    with pytest.raises(TimeoutError):
        await server.wait_for_jetstream_ready('nats://fake', deadline=started + .05)
    assert time.monotonic() - started < .5


@pytest.mark.nats
@pytest.mark.asyncio
async def test_owned_restart_preserves_store_and_port(nats_owned_server):
    import nats
    owner, target = nats_owned_server
    original_url = target.url
    connection = await nats.connect(original_url)
    try:
        kv = await connection.jetstream().create_key_value(bucket='harness_recovery')
        await kv.put('value', b'persisted')
    finally:
        await connection.close()
    await asyncio.to_thread(owner.restart, target)
    assert target.url == original_url
    connection = await nats.connect(target.url)
    try:
        kv = await connection.jetstream().key_value('harness_recovery')
        assert (await kv.get('value')).value == b'persisted'
    finally:
        await connection.close()


@pytest.mark.nats
def test_owned_servers_isolate_ports_cleanup_and_reuse_offline_cache(monkeypatch):
    first, second = server.OwnedNatsServer(), server.OwnedNatsServer()
    a = first.start()
    try:
        monkeypatch.setattr(provision, '_download_archive', lambda *a: pytest.fail('valid cache attempted download'))
        b = second.start()
        try:
            assert a.url != b.url
            assert a.work_dir != b.work_dir
            with pytest.raises(RuntimeError, match='intentional scenario failure'):
                raise RuntimeError('intentional scenario failure')
        finally:
            process = b.process
            second.stop(b)
        assert process.poll() is not None
        assert not b.work_dir.exists()
        assert b.log_path.is_file()
    finally:
        process = a.process
        first.stop(a)
    assert process.poll() is not None
    assert not a.work_dir.exists()
    assert a.log_path.is_file()

@pytest.mark.nats
def test_native_start_failure_cleans_store_and_preserves_stderr(monkeypatch):
    executable = provision.resolve_executable()
    monkeypatch.setattr(server, 'resolve_executable', lambda: executable)
    original = server.subprocess.Popen
    processes = []
    def fail_start(args, **kwargs):
        process = original([*args, '--invalid-harness-control'], **kwargs)
        processes.append(process)
        return process
    monkeypatch.setattr(server.subprocess, 'Popen', fail_start)
    owner = server.OwnedNatsServer()
    with pytest.raises(RuntimeError, match='startup exited'):
        owner.start()
    assert all(process.poll() is not None for process in processes)
    assert not owner.target.work_dir.exists()
    assert 'invalid-harness-control' in owner.target.log_path.read_text()
