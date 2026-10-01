from __future__ import annotations

import asyncio
import subprocess
import time
from pathlib import Path

import pytest

from tests.support.nats_harness import provision, server


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
