"""Minimal worker: stderr then exit 42 before ready."""

from __future__ import annotations

import sys


def main() -> None:
    print("worker crash before ready", file=sys.stderr, flush=True)
    raise SystemExit(42)


if __name__ == "__main__":
    main()
