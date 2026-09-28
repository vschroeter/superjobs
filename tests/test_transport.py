from faststream.nats import NatsBroker

from superjobs.transport.implementations.nats import NatsQueueConfig
from superjobs.transport.transport import Source, Transport


def test_durable_consumer_name_is_stable_per_source() -> None:
    source = Source(name="superjobs.generate.request", stream="superjobs")
    transport = Transport(NatsBroker(), NatsQueueConfig())
    restarted_transport = Transport(
        NatsBroker(),
        NatsQueueConfig(),
    )

    assert transport.consumer_name(source) == restarted_transport.consumer_name(source)
    assert transport.consumer_name(source) != transport.consumer_name(
        Source(name="superjobs.other.request", stream="superjobs"),
    )


def test_non_durable_configuration_uses_an_ephemeral_consumer() -> None:
    source = Source(name="superjobs.generate.request", stream="superjobs")
    transport = Transport(NatsBroker(), NatsQueueConfig(durable=False))

    assert transport.consumer_name(source) is None
