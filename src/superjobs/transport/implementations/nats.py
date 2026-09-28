from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import timedelta

from faststream.nats import NatsMessage

from superjobs.transport.delivery import Delivery


class NatsDelivery(Delivery):
    def __init__(
        self,
        msg: NatsMessage,
        *,
        attempt: int = 1,
        retry_publisher: Callable[
            [int, timedelta | None],
            Awaitable[None],
        ]
        | None = None,
    ):
        self._msg = msg
        self._attempt = attempt
        self._retry_publisher = retry_publisher

    @property
    def attempt(self) -> int:
        return self._attempt

    async def ack(self) -> None:
        await self._msg.ack_sync()

    async def retry(
        self,
        *,
        delay: timedelta | None = None,
        attempt: int | None = None,
    ) -> None:
        if self._retry_publisher is not None and attempt is not None:
            await self._retry_publisher(attempt, delay)
            await self._msg.ack_sync()
            return
        if delay is None:
            await self._msg.nack()
        else:
            await self._msg.nack(delay=delay.total_seconds())

    async def reject(
        self,
        *,
        reason: str | None = None,
    ) -> None:
        await self._msg.reject()

    async def extend_lease(self) -> None:
        await self._msg.in_progress()


@dataclass(frozen=True)
class NatsQueueConfig:
    stream: str = "superjobs"
    observation_stream: str = "superjobs-observations"
    subject_prefix: str = "superjobs"
    completion_bucket: str = "superjobs-completions"
    idempotency_bucket: str = "superjobs-idempotency"
    pull: bool = True
    durable: bool = True
    ack_wait: float | None = None
    max_deliver: int | None = None
    retry_delay: float | None = 1.0
    reconnect_delay: float = 1.0
    max_result_bytes: int | None = None
    observation_max_age: float | None = None
    observation_max_bytes: int | None = None
    observation_max_msgs: int | None = None
    result_max_age: float | None = None

    def __post_init__(self) -> None:
        if not self.stream or not self.observation_stream:
            raise ValueError("stream names must not be empty")
        if not self.subject_prefix:
            raise ValueError("subject_prefix must not be empty")
        if self.ack_wait is not None and self.ack_wait <= 0:
            raise ValueError("ack_wait must be positive")
        if self.max_deliver is not None and self.max_deliver < 1:
            raise ValueError("max_deliver must be positive")
        if self.retry_delay is not None and self.retry_delay < 0:
            raise ValueError("retry_delay must be non-negative")
        if self.reconnect_delay <= 0:
            raise ValueError("reconnect_delay must be positive")
        if self.max_result_bytes is not None and self.max_result_bytes < 1:
            raise ValueError("max_result_bytes must be positive")
        if self.observation_max_age is not None and self.observation_max_age <= 0:
            raise ValueError("observation_max_age must be positive")
        if self.observation_max_bytes is not None and self.observation_max_bytes < 1:
            raise ValueError("observation_max_bytes must be positive")
        if self.observation_max_msgs is not None and self.observation_max_msgs < 1:
            raise ValueError("observation_max_msgs must be positive")
        if self.result_max_age is not None and self.result_max_age <= 0:
            raise ValueError("result_max_age must be positive")
