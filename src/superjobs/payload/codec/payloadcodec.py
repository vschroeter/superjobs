from typing import Any

from superjobs.payload.adapter.protocol import PayloadAdapter
from superjobs.payload.codec.wirecodec import WireCodec
from superjobs.payload.strict import dump_python_value, load_wire_value, prepare_python_value


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
        wire_value = dump_python_value(self.adapter, value)
        return self.wire.encode(wire_value)

    def decode(self, data: bytes) -> T:
        wire_value = self.wire.decode(data)
        return load_wire_value(self.adapter, wire_value)

    def prepare(self, value: T) -> T:
        """Validate for buffering, preserving declared conversions on built-in payloads."""
        prepared, wire_value = prepare_python_value(self.adapter, value)
        self.wire.encode(wire_value)
        return prepared

    def schema(self) -> Any | None:
        return self.adapter.schema()
