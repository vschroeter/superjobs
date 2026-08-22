from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

from faststream.nats.subscriber.usecases import LogicSubscriber

from superjobs.jobs.job_context import JobContext, JobStatus
from superjobs.jobs.job_destination import JobChannelType, JobDestination
from superjobs.transport.transport import Source, Transport

if TYPE_CHECKING:
    from superjobs.jobs.job import Job
    from superjobs.superjobs import SuperJobs


logger = logging.getLogger(__name__)


class JobHandler[ReqT: Any | None, FinalT: Any | None, InterT: Any | None]:
    def __init__(
        self,
        job: Job[ReqT, FinalT, InterT],
        callback: Callable[
            [ReqT, JobContext[ReqT, FinalT, InterT]],
            FinalT | Awaitable[FinalT],
        ],
        transport: Transport,
    ):
        self.job = job
        self.callback = callback
        self.transport = transport

        self.request_destination = self.get_destination_for_channel(JobChannelType.REQUEST)
        self.request_subscriber: LogicSubscriber[Any] | None = None
        self._start_task: asyncio.Task[None] | None = None
        self._consumer_task: asyncio.Task[None] | None = None

        self._stop_event = asyncio.Event()
        self._ready_event = asyncio.Event()
        self._lifecycle_lock = asyncio.Lock()

    async def _process_message(self, msg: Any) -> None:
        try:
            request = self.job.request_codec.decode(msg.body)
        except Exception:
            logger.exception("Rejecting undecodable request for job %s", self.job)
            await msg.reject()
            return

        context = JobContext(job=self.job, message=msg)
        context.status = JobStatus.RUNNING

        try:
            result = self.callback(request, context)
            if inspect.isawaitable(result):
                await result
        except asyncio.CancelledError:
            raise
        except Exception:
            context.status = JobStatus.FAILED
            logger.exception("Retrying failed request for job %s", self.job)

            retry_delay = self.transport.queue_config.retry_delay
            if retry_delay is None:
                await msg.nack()
            else:
                await msg.nack(delay=retry_delay)
            return

        context.status = JobStatus.COMPLETED
        await msg.ack_sync()

    async def _restart_subscriber(self) -> None:
        old_subscriber = self.request_subscriber
        self.request_subscriber = None

        if old_subscriber is not None:
            try:
                await old_subscriber.stop()
            except Exception:
                logger.exception("Failed to stop an unhealthy job subscriber")

        while not self._stop_event.is_set():
            try:
                await asyncio.sleep(self.transport.queue_config.reconnect_delay)
                if self._stop_event.is_set():
                    return

                self.request_subscriber = await self.transport.create_subscriber(
                    Source(
                        name=self.request_destination.address,
                        stream=self.transport.queue_config.stream,
                    ),
                    start=True,
                )
                return
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Failed to recreate subscriber for job %s", self.job)

    async def _request_consumer_task(self) -> None:
        while not self._stop_event.is_set():
            subscriber = self.request_subscriber
            if subscriber is None:
                raise RuntimeError("Job subscriber is not initialized")

            try:
                async for msg in subscriber:
                    if self._stop_event.is_set():
                        return
                    await self._process_message(msg)
            except asyncio.CancelledError:
                raise
            except Exception:
                if self._stop_event.is_set():
                    return

                logger.exception("Job subscriber failed for %s", self.job)
                await self._restart_subscriber()

    async def start(self) -> None:
        async with self._lifecycle_lock:
            if self._consumer_task is not None and not self._consumer_task.done():
                return

            self._stop_event.clear()
            self._ready_event.clear()

            self.request_subscriber = await self.transport.create_subscriber(
                Source(
                    name=self.request_destination.address,
                    stream=self.transport.queue_config.stream,
                ),
                start=True,
            )

            self._consumer_task = asyncio.create_task(
                self._request_consumer_task(),
                name=f"superjobs-consumer-{self.job.canonical_name}",
            )
            self._ready_event.set()

    async def stop(self):
        self._stop_event.set()

        tasks = [
            task
            for task in (self._start_task, self._consumer_task)
            if task is not None and task is not asyncio.current_task()
        ]
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

        if self.request_subscriber is not None:
            try:
                await self.request_subscriber.stop()
            except Exception:
                logger.exception("Failed to stop subscriber for job %s", self.job)

        self._start_task = None
        self._consumer_task = None
        self.request_subscriber = None
        self._ready_event.clear()

    def schedule_start(self) -> None:
        if self._start_task is None or self._start_task.done():
            self._start_task = asyncio.create_task(
                self.start(),
                name=f"superjobs-start-{self.job.canonical_name}",
            )

    async def wait_ready(self) -> None:
        if self._start_task is not None:
            await self._start_task

        if self._consumer_task is None:
            raise RuntimeError(f"Job handler for {self.job} has not been started")

        await self._ready_event.wait()

    def get_destination_for_channel(self, channel: JobChannelType) -> JobDestination:
        return JobDestination(identity=self.job.identity, channel=channel)
