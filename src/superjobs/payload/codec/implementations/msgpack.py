import msgpack

from superjobs.payload.adapter.protocol import WireValue


class MsgpackCodec:
    media_type = "application/msgpack"

    def encode(self, value: WireValue) -> bytes:
        return msgpack.packb(value)

    def decode(self, data: bytes) -> WireValue:
        return msgpack.unpackb(data)
