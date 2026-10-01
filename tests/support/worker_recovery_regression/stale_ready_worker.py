"""Regression worker that publishes a stale ready marker (wrong generation/pid)."""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

from protocol import write_ready

state_dir = Path(os.environ["SUPERJOBS_CROSS_STATE_DIR"])
run_id = os.environ["SUPERJOBS_CROSS_RUN_ID"]
write_ready(state_dir, run_id, pid=os.getpid(), worker_generation="0")
print("published stale ready", flush=True)
time.sleep(30)
raise SystemExit(0)
