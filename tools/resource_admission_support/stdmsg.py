"""JSON line protocol on stdout for parent/controller coordination."""

from __future__ import annotations

import json
import sys
from typing import Any


PREFIX = "RA_MSG "


def emit(message: dict[str, Any]) -> None:
    line = PREFIX + json.dumps(message, sort_keys=True)
    print(line, flush=True)


def parse_line(line: str) -> dict[str, Any] | None:
    stripped = line.strip()
    if not stripped.startswith(PREFIX):
        return None
    payload = json.loads(stripped[len(PREFIX) :])
    if not isinstance(payload, dict):
        raise ValueError("RA_MSG payload must be a JSON object")
    return payload


def emit_error(summary: str, **extra: Any) -> None:
    emit({"type": "error", "summary": summary, **extra})


def fatal(summary: str, *, code: int = 3) -> None:
    emit_error(summary)
    print(summary, file=sys.stderr)
    raise SystemExit(code)
