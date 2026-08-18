import json

from superjobs.payload.adapter.protocol import WireValue


class JsonCodec:
    media_type = "application/json"

    def encode(self, value: WireValue) -> bytes:
        return json.dumps(
            value,
            separators=(",", ":"),
        ).encode("utf-8")

    def decode(self, data: bytes) -> WireValue:
        return json.loads(data)
