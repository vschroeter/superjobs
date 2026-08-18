from __future__ import annotations

from typing import TYPE_CHECKING

from superjobs.payload.adapter.implementations.dataclass import DataclassAdapterFactory
from superjobs.payload.adapter.implementations.pydantic import PydanticAdapterFactory
from superjobs.payload.adapter.implementations.python import PlainPythonAdapterFactory
from superjobs.payload.codec.implementations.msgpack import MsgpackCodec
from superjobs.payload.codec.payloadcodec import PayloadCodec
from superjobs.payload.codec.wirecodec import WireCodec

if TYPE_CHECKING:
    from superjobs.payload.adapter.factory import AdapterFactory


class SuperjobsRegistry:
    def __init__(self):
        self.adapters: list[AdapterFactory] = []
        self._type_cache: dict[type, AdapterFactory] = {}

        self.default_wire_codec: WireCodec = MsgpackCodec()

    def register_adapter(self, adapter: AdapterFactory):
        self.adapters.append(adapter)

    def get_adapter(self, type_: type) -> AdapterFactory:
        if type_ in self._type_cache:
            return self._type_cache[type_]
        for adapter in self.adapters:
            if adapter.supports(type_):
                self._type_cache[type_] = adapter
                return adapter
        raise ValueError(f"No adapter found for type {type_}")

    def get_payload_codec(self, type_: type, wire_codec: WireCodec | None = None) -> PayloadCodec:
        adapter_factory = self.get_adapter(type_)
        return PayloadCodec(
            adapter_factory.create(type_),
            wire_codec or self.default_wire_codec,
        )


registry = SuperjobsRegistry()

###############################################################################################
# Default adapters
###############################################################################################

registry.register_adapter(PydanticAdapterFactory())
registry.register_adapter(DataclassAdapterFactory())
registry.register_adapter(PlainPythonAdapterFactory())
