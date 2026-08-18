from __future__ import annotations

from typing import Any, Protocol

JsonScalar = str | int | float | bool | None
WireValue = JsonScalar | list["WireValue"] | dict[str, "WireValue"]


class PayloadAdapter[T](Protocol):
    def dump(self, value: T) -> WireValue:
        """Python object -> wire-compatible value."""
        ...

    def load(self, value: WireValue) -> T:
        """Wire-compatible value -> Python object."""
        ...

    def schema(self) -> Any | None:
        """Optional logical schema of T."""
        ...


class AdapterFactory[T](Protocol):
    def supports(self, type_: Any) -> bool: ...

    def create(self, type_: type[T]) -> PayloadAdapter[T]: ...
