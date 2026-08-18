
from pydantic import BaseModel
from superjobs.jobs.job import Job


class GenerateRequest(BaseModel):
    text: str

class GenerateResult(BaseModel):
    test_result: str

class GenerateEvent(BaseModel):
    event: str

def test_job_generation():
    GenerateTTS = Job[
        GenerateRequest,
        GenerateResult,
        GenerateEvent,
    ](
        "superjobs.generate",
        version=1,
    )

    assert GenerateTTS is not None
    

