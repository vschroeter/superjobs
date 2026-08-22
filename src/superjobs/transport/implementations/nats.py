from dataclasses import dataclass
from datetime import timedelta

from faststream.nats import NatsMessage

from superjobs.transport.delivery import Delivery


class NatsDelivery(Delivery):
    def __init__(self, msg: NatsMessage):
        self._msg = msg

    async def ack(self) -> None:
        await self._msg.ack_sync()

    async def retry(
        self,
        *,
        delay: timedelta | None = None,
    ) -> None:
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
    pull: bool = True
    durable: bool = True
    consumer_group: str = "default"
    ack_wait: float | None = None
    max_deliver: int | None = None
    retry_delay: float | None = 1.0
    reconnect_delay: float = 1.0
