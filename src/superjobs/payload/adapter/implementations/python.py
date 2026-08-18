from __future__ import annotations

from typing import Any

import pydantic

from superjobs.payload.adapter.factory import AdapterFactory
from superjobs.payload.adapter.protocol import PayloadAdapter, WireValue


class PlainPythonAdapterFactory(AdapterFactory[Any]):
    def supports(self, type_: Any) -> bool:
        allowed_types = (list, dict, str, int, float, bool, type(None))

        return type_ in allowed_types

    def create(self, type_: type[Any]) -> PlainPythonPayloadAdapter[Any]:
        return PlainPythonPayloadAdapter(type_)


class PlainPythonPayloadAdapter[T](PayloadAdapter[T]):
    def __init__(self, type_: type[T]):
        self.type_ = type_
        self._adapter = pydantic.TypeAdapter(type_)

    def dump(self, value: T) -> WireValue:
        return self._adapter.dump_python(value, mode="json")

    def load(self, value: WireValue) -> T:
        return self._adapter.validate_python(value)

    def schema(self) -> Any:
        return self._adapter.json_schema()
