"""Regression worker that exits before publishing ready."""

from __future__ import annotations

import sys

print("exiting before ready", flush=True)
raise SystemExit(42)
