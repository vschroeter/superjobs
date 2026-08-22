from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from superjobs.jobs.job_identity import JobIdentity


class JobChannelType(StrEnum):
    REQUEST = "request"
    EVENT = "event"
    CANCEL = "cancel"


@dataclass(frozen=True)
class JobDestination:
    identity: JobIdentity
    channel: JobChannelType

    job_id: str | None = None

    @property
    def address(self) -> str:
        s = f"{self.identity.canonical_name}.{self.channel.value}"
        if self.job_id:
            s += f".{self.job_id}"
        return s

    def with_id(self, job_id: str) -> JobDestination:
        return JobDestination(identity=self.identity, channel=self.channel, job_id=job_id)
