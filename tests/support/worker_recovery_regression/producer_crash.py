"""Regression producer that exits before doing work."""

from __future__ import annotations

import sys

print("producer crash before submit", file=sys.stderr, flush=True)
raise SystemExit(7)
