from collections.abc import Mapping
from dataclasses import dataclass, field

from superjobs.transport.delivery import Delivery


@dataclass(frozen=True)
class OutgoingMessage:
    payload: bytes
    media_type: str

    headers: Mapping[str, str] = field(default_factory=dict)

    correlation_id: str | None = None


class IncomingMessage:
    payload: bytes
    media_type: str
    headers: Mapping[str, str] = field(default_factory=dict)
    correlation_id: str | None = None

    delivery: Delivery
