"""Regression producer that never exits."""

from __future__ import annotations

import time

print("producer hanging", flush=True)
while True:
    time.sleep(0.05)
