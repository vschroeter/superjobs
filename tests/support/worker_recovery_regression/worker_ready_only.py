"""Regression worker that publishes ready without connecting to NATS."""

from __future__ import annotations

import os
import time
from pathlib import Path

from protocol import write_ready

state_dir = Path(os.environ["SUPERJOBS_CROSS_STATE_DIR"])
run_id = os.environ["SUPERJOBS_CROSS_RUN_ID"]
generation = os.environ.get("SUPERJOBS_WORKER_GENERATION", "1")
write_ready(state_dir, run_id, pid=os.getpid(), worker_generation=generation)
print("ready-only worker published", flush=True)
while True:
    time.sleep(0.05)
