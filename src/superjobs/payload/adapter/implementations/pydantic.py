from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from superjobs.payload.adapter.factory import AdapterFactory
from superjobs.payload.adapter.protocol import PayloadAdapter, WireValue


class PydanticAdapterFactory(AdapterFactory[BaseModel]):
    def supports(self, type_: Any) -> bool:
        return isinstance(type_, type) and issubclass(type_, BaseModel)

    def create(self, type_: type[BaseModel]) -> PydanticPayloadAdapter[BaseModel]:
        return PydanticPayloadAdapter(type_)


class PydanticPayloadAdapter[T: BaseModel](PayloadAdapter[T]):
    def __init__(self, model: type[T]):
        self.model = model

    def dump(self, value: T) -> WireValue:
        return value.model_dump(mode="json")

    def load(self, value: WireValue) -> T:
        return self.model.model_validate(value)

    def schema(self) -> Any:
        return self.model.model_json_schema()
