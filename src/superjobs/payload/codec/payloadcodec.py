from typing import Any

from superjobs.payload.adapter.protocol import PayloadAdapter
from superjobs.payload.codec.wirecodec import WireCodec


class PayloadCodec[T]:
    def __init__(
        self,
        adapter: PayloadAdapter[T],
        wire: WireCodec,
    ):
        self.adapter = adapter
        self.wire = wire

    @property
    def media_type(self) -> str:
        return self.wire.media_type

    def encode(self, value: T) -> bytes:
        wire_value = self.adapter.dump(value)
        return self.wire.encode(wire_value)

    def decode(self, data: bytes) -> T:
        wire_value = self.wire.decode(data)
        return self.adapter.load(wire_value)

    def schema(self) -> Any | None:
        return self.adapter.schema()
