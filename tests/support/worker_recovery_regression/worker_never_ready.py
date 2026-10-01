"""Regression worker that exits before publishing ready."""

from __future__ import annotations

import sys

print("crash before ready", file=sys.stderr, flush=True)
raise SystemExit(42)
