from datetime import timedelta
from typing import Protocol


class Delivery(Protocol):
    @property
    def attempt(self) -> int: ...

    async def ack(self) -> None: ...

    async def retry(
        self,
        *,
        delay: timedelta | None = None,
    ) -> None: ...

    async def reject(
        self,
        *,
        reason: str | None = None,
    ) -> None: ...

    async def extend_lease(self) -> None: ...
