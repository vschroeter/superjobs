from __future__ import annotations

from superjobs import NoRequestJob

RESOURCE_KEY = (
    "samplegen/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/cpu/"
    "c4fc787d1d3b4cac81eaa0b90f0845011a0d0fced368b92dd2d9eec55ff50dfc"
)
SHARED_RESOURCE_NAMESPACE = "physical_host"
HOLD_JOB = NoRequestJob(
    "tests.shared_resource_namespace.hold",
    version="v1",
    result=str,
)
