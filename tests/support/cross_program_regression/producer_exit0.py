"""Minimal producer: immediate success exit."""

from __future__ import annotations

import sys


def main() -> None:
    raise SystemExit(0)


if __name__ == "__main__":
    main()
