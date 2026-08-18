from typing import Any, Protocol

from superjobs.payload.adapter.protocol import PayloadAdapter


class AdapterFactory[T](Protocol):
    def supports(self, type_: Any) -> bool: ...

    def create(self, type_: type[T]) -> PayloadAdapter[T]: ...
