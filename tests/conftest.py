import pytest

from superjobs.superjobs import SuperJobs
from tests.support.fake_transport import FakeTransport

pytest_plugins = ["tests.support.nats_harness.pytest_fixtures"]


@pytest.fixture
def fake_transport() -> FakeTransport:
    return FakeTransport()


@pytest.fixture
def superjobs(fake_transport: FakeTransport) -> SuperJobs:
    return SuperJobs(transport=fake_transport)
