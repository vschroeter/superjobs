import msgpack
from typing import Any, cast

from superjobs.payload.adapter.protocol import WireValue


class MsgpackCodec:
    media_type = "application/msgpack"

    def encode(self, value: WireValue) -> bytes:
        return cast(bytes, msgpack.packb(value, use_bin_type=True))

    def decode(self, data: bytes) -> WireValue:
        return cast(Any, msgpack.unpackb(data, raw=False))
