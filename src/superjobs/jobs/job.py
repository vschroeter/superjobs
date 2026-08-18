from pydantic import BaseModel

from superjobs.jobs.job_identity import JobIdentity


class Job[ReqT: BaseModel | None, FinalT: BaseModel | None, InterT: BaseModel | None]:
    def __init__(
        self,
        name: str,
        version: int = 0,
        *,
        request: ReqT | None = None,
        namespace: str | None = None,
        job_identity: JobIdentity | None = None,
    ):
        self.identity = job_identity or JobIdentity(name=name, namespace=namespace, version=version)

    @property
    def name(self) -> str:
        return self.identity.name

    @property
    def namespace(self) -> str | None:
        return self.identity.namespace

    @property
    def version(self) -> int:
        return self.identity.version

    @property
    def canonical_name(self) -> str:
        return self.identity.canonical_name

    def __call__(self, request: ReqT) -> FinalT:
        pass

    def __str__(self) -> str:
        return self.canonical_name

    def __repr__(self) -> str:
        return f"Job({self.canonical_name})"
