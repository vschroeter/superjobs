"""Minimal worker: publish ready, wait for stop marker, exit 0."""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

from protocol import assert_ready_absent, read_worker_stop, stop_path, write_ready


def main() -> None:
    run_id = os.environ["SUPERJOBS_CROSS_RUN_ID"]
    state_dir = Path(os.environ["SUPERJOBS_CROSS_STATE_DIR"])
    assert_ready_absent(state_dir)
    write_ready(state_dir, run_id)
    while not stop_path(state_dir).is_file():
        time.sleep(0.02)
    read_worker_stop(state_dir)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"worker failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
