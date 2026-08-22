import asyncio
from collections.abc import Awaitable, Callable

from faststream.nats import NatsBroker

from superjobs.jobs.job import Job
from superjobs.jobs.job_client import JobClient
from superjobs.jobs.job_context import JobContext
from superjobs.jobs.job_handler import JobHandler
from superjobs.transport.implementations.nats import NatsQueueConfig
from superjobs.transport.transport import Transport


class SuperJobs:
    def __init__(self, broker: NatsBroker, queue_config: NatsQueueConfig | None = None):

        self.transport = Transport(broker, queue_config)
        self._handlers: list[JobHandler] = []
        self._started = False
        self._lifecycle_lock = asyncio.Lock()

    async def start(self):
        async with self._lifecycle_lock:
            if self._started:
                return

            await self.transport.start()
            self._started = True

            try:
                await asyncio.gather(
                    *(handler.start() for handler in self._handlers),
                )
            except BaseException:
                self._started = False
                await asyncio.gather(
                    *(handler.stop() for handler in self._handlers),
                    return_exceptions=True,
                )
                await self.transport.stop()
                raise

    async def stop(self):
        async with self._lifecycle_lock:
            self._started = False
            await asyncio.gather(
                *(handler.stop() for handler in self._handlers),
                return_exceptions=True,
            )

            await self.transport.stop()

    def _register_handler(self, handler: JobHandler) -> None:
        self._handlers.append(handler)
        if self._started:
            handler.schedule_start()

    def handle[ReqT, FinalT, InterT](self, job: Job[ReqT, FinalT, InterT]):
        def decorator(
            fct: Callable[
                [ReqT, JobContext[ReqT, FinalT, InterT]],
                FinalT | Awaitable[FinalT],
            ],
        ) -> Callable[
            [ReqT, JobContext[ReqT, FinalT, InterT]],
            FinalT | Awaitable[FinalT],
        ]:
            return job.handler(self, fct)

        return decorator

    def client(self, job: Job) -> JobClient:
        return JobClient(job, self.transport)
