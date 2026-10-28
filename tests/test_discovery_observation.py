from __future__ import annotations

from superjobs.discovery.observation import supports_own_registration_observation


class _WatchAndReadBackend:
    async def watch_registration(self, worker_id: str, job):
        if False:
            yield b""

    async def read_registration(self, worker_id: str, job) -> bytes | None:
        return None


class _WatchOnlyBackend:
    async def watch_registration(self, worker_id: str, job):
        if False:
            yield b""


def test_supports_own_registration_observation_requires_read_and_watch() -> None:
    assert supports_own_registration_observation(_WatchAndReadBackend())
    assert not supports_own_registration_observation(_WatchOnlyBackend())
    assert not supports_own_registration_observation(object())

    class OptOut(_WatchAndReadBackend):
        own_registration_observation = False

    assert not supports_own_registration_observation(OptOut())
