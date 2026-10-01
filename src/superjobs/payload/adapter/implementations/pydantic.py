from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from pydantic import BaseModel

from superjobs.payload.adapter.factory import AdapterFactory
from superjobs.payload.adapter.protocol import PayloadAdapter, WireValue
from superjobs.payload.strict import strict_boundary_for


class PydanticAdapterFactory(AdapterFactory[BaseModel]):
    def supports(self, type_: Any) -> bool:
        return isinstance(type_, type) and issubclass(type_, BaseModel)

    def create(self, type_: type[BaseModel]) -> PydanticPayloadAdapter[BaseModel]:
        return PydanticPayloadAdapter(type_)


class PydanticPayloadAdapter[T: BaseModel](PayloadAdapter[T]):
    def __init__(self, model: type[T]):
        self.model = model
        self._boundary = strict_boundary_for(model)

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
