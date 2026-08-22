class JobEvent:
    pass


class JobStarted(JobEvent):
    pass


class JobCompleted(JobEvent):
    pass


class JobFailed(JobEvent):
    pass


class JobCancelled(JobEvent):
    pass


class JobProgress(JobEvent):
    completed: int
    total: int


class JobLog(JobEvent):
    message: str
    level: str
