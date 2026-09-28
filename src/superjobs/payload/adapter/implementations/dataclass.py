from __future__ import annotations

from dataclasses import is_dataclass
from typing import Any

import pydantic

from superjobs.payload.adapter.factory import AdapterFactory
from superjobs.payload.adapter.protocol import PayloadAdapter, WireValue


class DataclassAdapterFactory(AdapterFactory[Any]):
    def supports(self, type_: Any) -> bool:
        return isinstance(type_, type) and is_dataclass(type_)

    def create(self, type_: type[Any]) -> DataclassPayloadAdapter[Any]:
        return DataclassPayloadAdapter(type_)


class DataclassPayloadAdapter[T](PayloadAdapter[T]):
    def __init__(self, dataclass: type[T]):
        self.dataclass = dataclass
        self._adapter = pydantic.TypeAdapter(dataclass)

    def dump(self, value: T) -> WireValue:
        validated = self._adapter.validate_python(value)
        return self._adapter.dump_python(validated, mode="json")

    def load(self, value: WireValue) -> T:
        return self._adapter.validate_python(value)

    def schema(self) -> Any:
        return self._adapter.json_schema()
