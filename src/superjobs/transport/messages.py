from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum

from superjobs.transport.delivery import Delivery


class MessageKind(StrEnum):
    REQUEST = "request"
    OBSERVATION = "observation"
    CANCELLATION = "cancellation"


@dataclass(frozen=True)
class MessageEnvelope:
    job_id: str
    job_name: str
    version: str | None
    kind: MessageKind
    payload: bytes
    media_type: str | None
    attempt: int = 1
    idempotency_key: str | None = None
    fingerprint: str | None = None


@dataclass(frozen=True)
class OutgoingMessage:
    payload: bytes
    media_type: str | None
    headers: Mapping[str, str] = field(default_factory=dict)
    correlation_id: str | None = None
    envelope: MessageEnvelope | None = None


@dataclass
class IncomingMessage:
    payload: bytes
    media_type: str | None
    headers: Mapping[str, str] = field(default_factory=dict)
    correlation_id: str | None = None
    delivery: Delivery | None = None
    envelope: MessageEnvelope | None = None
