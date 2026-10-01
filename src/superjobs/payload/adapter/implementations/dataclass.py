from __future__ import annotations

from collections.abc import Mapping
from dataclasses import is_dataclass
from typing import Any

from superjobs.payload.adapter.factory import AdapterFactory
from superjobs.payload.adapter.protocol import PayloadAdapter, WireValue
from superjobs.payload.strict import strict_boundary_for


class DataclassAdapterFactory(AdapterFactory[Any]):
    def supports(self, type_: Any) -> bool:
        return isinstance(type_, type) and is_dataclass(type_)

    def create(self, type_: type[Any]) -> DataclassPayloadAdapter[Any]:
        return DataclassPayloadAdapter(type_)


class DataclassPayloadAdapter[T](PayloadAdapter[T]):
    def __init__(self, dataclass: type[T]):
        self.dataclass = dataclass
        self._boundary = strict_boundary_for(dataclass)

    def validate_instance(self, value: T) -> T:
        return self._boundary.validate_instance(value)

    def construct_fields(self, fields: Mapping[str, Any]) -> T:
        return self._boundary.construct_fields(fields)

    def prepare_instance(self, value: T) -> tuple[T, WireValue]:
        return self._boundary.prepare_instance(value)

    def dump(self, value: T) -> WireValue:
        validated = self._boundary.validate_instance(value)
        return self._boundary.dump_wire(validated)

    def load(self, value: WireValue) -> T:
        return self._boundary.validate_wire(value)

    def schema(self) -> Any:
        return self._boundary.json_schema()
