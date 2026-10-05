"""Private lifetime completion and cancellation-safe cleanup primitives."""

import asyncio
from dataclasses import dataclass, field


@dataclass
class ShutdownCompletion:
    done: asyncio.Event = field(default_factory=asyncio.Event)
    error: BaseException | None = None

    async def wait(self) -> None:
        await self.done.wait()
        if self.error is not None:
            raise self.error


async def finish_cleanup(task: asyncio.Task[None]) -> None:
    """Finish owned cleanup before propagating caller cancellation.

    Shielding alone would let a cancelled owner exit while cleanup still runs.
    Repeated cancellation requests are deferred until the owned task settles.
    """
    cancellation: asyncio.CancelledError | None = None
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError as error:
            if task.cancelled():
                raise
            cancellation = error
    task.result()
    if cancellation is not None:
        raise cancellation
