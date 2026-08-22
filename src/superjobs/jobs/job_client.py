from __future__ import annotations

from typing import TYPE_CHECKING, Any

from superjobs.jobs.job_destination import JobChannelType, JobDestination
from superjobs.transport.messages import OutgoingMessage
from superjobs.transport.transport import Destination, Transport

if TYPE_CHECKING:
    from superjobs.jobs.job import Job
    from superjobs.superjobs import SuperJobs


class JobClient[ReqT: Any | None, FinalT: Any | None, InterT: Any | None]:
    def __init__(
        self,
        job: Job[ReqT, FinalT, InterT],
        transport: Transport,
    ):
        self.job = job
        self.transport = transport

        self.request_destination = self.get_destination_for_channel(JobChannelType.REQUEST)
        self.request_publisher = None

    async def submit(self, request: ReqT) -> None:
        if self.request_publisher is None:
            raise RuntimeError("Job client must be started before submitting requests")

        if self.job._handler is not None:
            await self.job._handler.wait_ready()

        encoded_message = self.job.encode_request(request)

        out_message = OutgoingMessage(
            payload=encoded_message,
            media_type=self.job.request_codec.media_type,
        )

        await self.request_publisher.publish(
            out_message.payload,
            headers={
                **out_message.headers,
                "content-type": out_message.media_type,
            },
            correlation_id=out_message.correlation_id,
        )

    async def start(self):
        self.request_publisher = await self.transport.create_publisher(
            Destination(name=self.request_destination.address, stream=self.transport.queue_config.stream),
            start=True,
        )

        if self.job._handler is not None:
            await self.job._handler.wait_ready()

    def get_destination_for_channel(self, channel: JobChannelType) -> JobDestination:
        return JobDestination(identity=self.job.identity, channel=channel)
