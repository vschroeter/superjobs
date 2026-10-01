"""Minimal producer: block until killed (scenario timeout tests)."""

from __future__ import annotations

import time


def main() -> None:
    while True:
        time.sleep(3600)


if __name__ == "__main__":
    main()
