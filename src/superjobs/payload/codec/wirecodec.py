from typing import Protocol

from superjobs.payload.adapter.protocol import WireValue


class WireCodec(Protocol):
    media_type: str

    def encode(self, value: WireValue) -> bytes: ...

    def decode(self, data: bytes) -> WireValue: ...
