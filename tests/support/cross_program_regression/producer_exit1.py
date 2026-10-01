"""Minimal producer: unexpected failure exit."""

from __future__ import annotations

import sys


def main() -> None:
    raise SystemExit(1)


if __name__ == "__main__":
    main()
