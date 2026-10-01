"""Minimal worker: write corrupt ready JSON then sleep."""

from __future__ import annotations

import os
import time
from pathlib import Path


def main() -> None:
    state_dir = Path(os.environ["SUPERJOBS_CROSS_STATE_DIR"])
    (state_dir / "worker_ready.json").write_text("{not-json", encoding="utf-8")
    while True:
        time.sleep(3600)


if __name__ == "__main__":
    main()
