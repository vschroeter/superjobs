import asyncio
from dataclasses import dataclass
from hashlib import sha256

from faststream.nats import JStream, NatsBroker, PullSub
from faststream.nats.subscriber.usecases import LogicSubscriber
from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy
from nats.js.errors import APIError, NotFoundError

from superjobs.transport.implementations.nats import NatsQueueConfig


@dataclass(frozen=True)
class Destination:
    name: str
    stream: str


@dataclass(frozen=True)
class Source:
    name: str
    stream: str


class ConsumerConfigurationError(RuntimeError):
    """Raised when a durable consumer does not match its requested source."""


class Transport:
    def __init__(self, broker: NatsBroker, queue_config: NatsQueueConfig | None = None):
        self.broker = broker
        self.queue_config = queue_config or NatsQueueConfig()
        self._stream_lock = asyncio.Lock()

    async def _ensure_stream(self, stream: JStream) -> None:
        if not self.broker.running:
            raise RuntimeError("Transport must be started before creating NATS resources")

        async with self._stream_lock:
            jetstream = self.broker.connection.jetstream()

            try:
                stream_info = await jetstream.stream_info(stream.name)
            except NotFoundError:
                try:
                    await jetstream.add_stream(
                        config=stream.config,
                        subjects=list(stream.subjects),
                    )
                    return
                except APIError:
                    # Another process may have declared the stream between
                    # stream_info and add_stream. Re-read it and reconcile
                    # subjects below instead of failing spuriously.
                    stream_info = await jetstream.stream_info(stream.name)

            current_subjects = list(stream_info.config.subjects or ())
            new_subjects = [subject for subject in stream.subjects if not any(self._subject_matches(subject, current_subject) for current_subject in current_subjects)]

            if new_subjects:
                stream_info.config.subjects = [*current_subjects, *new_subjects]
                await jetstream.update_stream(config=stream_info.config)

    def consumer_name(self, source: Source) -> str | None:
        if not self.queue_config.durable:
            return None

        consumer_key = "\0".join(
            (source.stream, self.queue_config.consumer_group, source.name),
        )
        digest = sha256(consumer_key.encode("utf-8")).hexdigest()[:32]
        return f"sj-{digest}"

    def _consumer_config(
        self,
        source: Source,
        consumer_name: str | None,
    ) -> ConsumerConfig:
        return ConsumerConfig(
            durable_name=consumer_name,
            filter_subject=source.name,
            deliver_policy=DeliverPolicy.ALL,
            ack_policy=AckPolicy.EXPLICIT,
            ack_wait=self.queue_config.ack_wait,
            max_deliver=self.queue_config.max_deliver,
        )

    async def _ensure_consumer(
        self,
        source: Source,
        consumer_name: str | None,
    ) -> None:
        if consumer_name is None:
            return

        jetstream = self.broker.connection.jetstream()
        try:
            consumer_info = await jetstream.consumer_info(
                source.stream,
                consumer_name,
            )
        except NotFoundError:
            return

        actual_filter = consumer_info.config.filter_subject
        actual_filters = list(consumer_info.config.filter_subjects or ())
        actual_ack_policy = getattr(
            consumer_info.config.ack_policy,
            "value",
            consumer_info.config.ack_policy,
        )
        actual_deliver_policy = getattr(
            consumer_info.config.deliver_policy,
            "value",
            consumer_info.config.deliver_policy,
        )

        if (
            actual_filter != source.name
            or actual_filters
            or actual_ack_policy != AckPolicy.EXPLICIT.value
            or actual_deliver_policy != DeliverPolicy.ALL.value
        ):
            raise ConsumerConfigurationError(
                f"Durable consumer {consumer_name!r} on stream "
                f"{source.stream!r} is configured for filter "
                f"{actual_filter or actual_filters!r}, ack policy "
                f"{actual_ack_policy!r}, and delivery policy "
                f"{actual_deliver_policy!r}; expected exact filter "
                f"{source.name!r}, explicit acknowledgements, and "
                "deliver-all. Migrate or remove the durable consumer.",
            )

    @staticmethod
    def _subject_matches(subject: str, pattern: str) -> bool:
        subject_tokens = subject.split(".")
        pattern_tokens = pattern.split(".")

        for index, pattern_token in enumerate(pattern_tokens):
            if pattern_token == ">":
                return index < len(subject_tokens)

            if index >= len(subject_tokens):
                return False

            if pattern_token != "*" and pattern_token != subject_tokens[index]:
                return False

        return len(subject_tokens) == len(pattern_tokens)

    async def create_publisher(self, destination: Destination, *, start: bool = True):
        stream = JStream(name=destination.stream, subjects=[destination.name])

        if start:
            await self._ensure_stream(stream)

        pub = self.broker.publisher(
            subject=destination.name,
            stream=stream,
        )

        if start:
            await pub.start()

        return pub

    async def create_subscriber(self, source: Source, *, start: bool = True):
        stream = JStream(name=source.stream, subjects=[source.name])
        consumer_name = self.consumer_name(source)

        if start:
            await self._ensure_stream(stream)
            await self._ensure_consumer(source, consumer_name)

        sub: LogicSubscriber = self.broker.subscriber(
            subject=source.name,
            stream=stream,
            pull_sub=PullSub(batch_size=1) if self.queue_config.pull else False,
            config=self._consumer_config(source, consumer_name),
            durable=consumer_name,
        )

        if start:
            await sub.start()

        return sub

    async def start(self):
        if not self.broker.running:
            await self.broker.start()

    async def stop(self):
        if self.broker.running:
            await self.broker.stop()


# class Transport(Protocol):
#     async def publish(
#         self,
#         destination: Destination,
#         message: OutgoingMessage,
#     ) -> PublishReceipt: ...

#     def subscribe(
#         self,
#         source: Source,
#         handler: MessageHandler,
#         *,
#         options: ConsumerOptions,
#     ) -> Subscription: ...

#     async def start(self) -> None: ...

#     async def stop(self) -> None: ...
