"""Strict JSON parsing for CLI whole-request input (issue #39)."""

from __future__ import annotations

import json
import math
import sys
from typing import Any

from superjobs.payload.adapter.protocol import WireValue


class CLIInputError(ValueError):
    """Invalid CLI JSON or UTF-8 input."""


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    seen: set[str] = set()
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in seen:
            raise CLIInputError(f"duplicate JSON object key: {key!r}")
        seen.add(key)
        result[key] = value
    return result


def _parse_float(text: str) -> float:
    value = float(text)
    if not math.isfinite(value):
        raise CLIInputError("non-finite JSON number")
    return value


def parse_strict_json(text: str) -> WireValue:
    """Parse JSON text with duplicate-key and non-finite number rejection."""
    try:
        return json.loads(
            text,
            object_pairs_hook=_reject_duplicate_keys,
            parse_float=_parse_float,
            parse_constant=lambda _name: (_ for _ in ()).throw(
                CLIInputError("non-finite JSON constant"),
            ),
        )
    except json.JSONDecodeError as exc:
        raise CLIInputError(f"malformed JSON: {exc.msg}") from exc
    except CLIInputError:
        raise
    except ValueError as exc:
        raise CLIInputError(str(exc)) from exc
    except RecursionError as exc:
        raise CLIInputError("JSON nesting is too deep") from exc


def read_utf8_text(path: str) -> str:
    """Read UTF-8 from a file path or stdin when ``path`` is ``-``."""
    if path == "-":
        try:
            stream = getattr(sys.stdin, "buffer", sys.stdin)
            data = stream.read()
        except (OSError, ValueError) as exc:
            raise CLIInputError(f"cannot read stdin input: {exc}") from exc
    else:
        try:
            with open(path, "rb") as handle:
                data = handle.read()
        except (OSError, ValueError) as exc:
            raise CLIInputError(f"cannot read input file: {exc}") from exc
    if isinstance(data, str):
        return data
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise CLIInputError(f"input is not valid UTF-8: {exc}") from exc
