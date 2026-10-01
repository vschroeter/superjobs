from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from superjobs.payload.adapter.factory import AdapterFactory
from superjobs.payload.adapter.protocol import PayloadAdapter, WireValue
from superjobs.payload.strict import strict_boundary_for


class PlainPythonAdapterFactory(AdapterFactory[Any]):
    def supports(self, type_: Any) -> bool:
        allowed_types = (list, dict, str, int, float, bool, type(None))

        return type_ in allowed_types

    def create(self, type_: type[Any]) -> PlainPythonPayloadAdapter[Any]:
        return PlainPythonPayloadAdapter(type_)


class PlainPythonPayloadAdapter[T](PayloadAdapter[T]):
    def __init__(self, type_: type[T]):
        self.type_ = type_
        self._boundary = strict_boundary_for(type_)

    def validate_instance(self, value: T) -> T:
        return self._boundary.validate_instance(value)

    def prepare_instance(self, value: T) -> tuple[T, WireValue]:
        return self._boundary.prepare_instance(value)

    def construct_fields(self, fields: Mapping[str, Any]) -> T:
        if self.type_ is dict:
            return self._boundary.construct_fields(fields)
        raise TypeError(
            f"Plain Python payload type {self.type_!r} does not support keyword field construction",
        )

    def dump(self, value: T) -> WireValue:
        validated = self._boundary.validate_instance(value)
        return self._boundary.dump_wire(validated)

    def load(self, value: WireValue) -> T:
        return self._boundary.validate_wire(value)

    def schema(self) -> Any:
        return self._boundary.json_schema()
