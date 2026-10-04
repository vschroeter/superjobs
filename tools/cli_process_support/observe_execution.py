"""Fresh-client observation helper for CLI process verification."""

from __future__ import annotations

import asyncio
import json
import os
import sys
from contextlib import redirect_stdout
from typing import Any

from faststream.nats import NatsBroker

from superjobs import JobCompleted, JobLog, JobProgress, JobStarted, JobState, JobSucceeded, SuperJobs
from superjobs_contract_example import CLI_GATE_JOB, CliGateResult


async def _observe() -> dict[str, Any]:
    execution_id = os.environ.get("SUPERJOBS_CLI_EXECUTION_ID")
    if not execution_id:
        print("SUPERJOBS_CLI_EXECUTION_ID must be set", file=sys.stderr)
        raise SystemExit(11)
    nats_url = os.environ.get("SUPERJOBS_NATS_URL", "nats://127.0.0.1:4222")
    gate = os.environ.get("SUPERJOBS_CLI_GATE", "observe")
    jobs = SuperJobs(broker=NatsBroker(nats_url, connect_timeout=5, allow_reconnect=False))
    async with asyncio.timeout(30), jobs:
        client = jobs.client(CLI_GATE_JOB)
        handle = await client.get(execution_id)
        if handle.id != execution_id:
            print("execution identity mismatch", file=sys.stderr)
            raise SystemExit(1)
        envelopes: list[dict[str, Any]] = []
        async for event in handle.events():
            data = event.data
            observation: dict[str, object]
            if isinstance(data, JobStarted):
                observation = {"kind": "started"}
            elif isinstance(data, JobCompleted):
                observation = {"kind": "completed"}
            elif isinstance(data, JobLog):
                observation = {"kind": "log", "message": data.message, "extra": data.extra}
            elif isinstance(data, JobProgress):
                observation = {"kind": "progress", "completed": data.completed, "total": data.total}
            else:
                raise AssertionError(f"unexpected gate observation: {data!r}")
            assert event.job_id == execution_id
            assert not envelopes or event.sequence > int(envelopes[-1]["sequence"])
            envelopes.append({"job_id": event.job_id, "sequence": event.sequence, "observation": observation})
        kinds = [item["observation"]["kind"] for item in envelopes]
        assert kinds[0] == "started" and kinds[-1] == "completed", kinds
        assert "log" in kinds and "progress" in kinds, kinds
        status = await handle.status()
        if status.state is not JobState.COMPLETED:
            print(f"execution not completed: {status.state}", file=sys.stderr)
            raise SystemExit(1)
        if status.cancellation_requested:
            print("cancellation was requested", file=sys.stderr)
            raise SystemExit(1)
        outcome = await asyncio.wait_for(handle.outcome(), timeout=30.0)
        if not isinstance(outcome, JobSucceeded):
            print(f"execution did not succeed: {outcome}", file=sys.stderr)
            raise SystemExit(1)
        result = outcome.result
        if not isinstance(result, CliGateResult) or result.gate != gate:
            print(f"unexpected result {result!r}", file=sys.stderr)
            raise SystemExit(1)
    return {
                "execution_id": execution_id,
                "gate": gate,
                "observations": envelopes,
                "cancellation_requested": False,
            }


def main() -> None:
    with redirect_stdout(sys.stderr):
        evidence = asyncio.run(_observe())
    print(json.dumps(evidence))


if __name__ == "__main__":
    main()
