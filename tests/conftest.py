from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

import pytest

from superjobs.superjobs import SuperJobs

_TESTS_ROOT = Path(__file__).resolve().parent
if (_TESTS_ROOT / "support" / "nats_harness").is_dir():
    pytest_plugins = ["tests.support.nats_harness.pytest_fixtures"]


@asynccontextmanager
async def _unavailable_builtin_remote(**_kwargs: object) -> AsyncIterator[SuperJobs]:
    raise ConnectionError(
        "built-in remote runtime is stubbed in deterministic CLI tests",
    )
    yield SuperJobs()  # pragma: no cover


def _apply_builtin_remote_stub(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "superjobs.cli.builtin_remote_runtime.builtin_remote_runtime",
        _unavailable_builtin_remote,
    )
    monkeypatch.setattr(
        "superjobs.cli.app.builtin_remote_runtime",
        _unavailable_builtin_remote,
    )


@pytest.fixture
def stub_builtin_remote_runtime(monkeypatch: pytest.MonkeyPatch) -> None:
    _apply_builtin_remote_stub(monkeypatch)


@pytest.fixture
def fake_transport():
    from tests.support.fake_transport import FakeTransport

    return FakeTransport()


@pytest.fixture
def superjobs(fake_transport) -> SuperJobs:
    return SuperJobs(transport=fake_transport)
