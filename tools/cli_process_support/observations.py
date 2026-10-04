"""Parse and validate CLI observation JSON envelopes."""

from __future__ import annotations

import json
from typing import Any

from tools.cli_process_support.protocol import ProtocolError


def parse_observation_lines(stderr: str) -> list[dict[str, Any]]:
    envelopes: list[dict[str, Any]] = []
    for line in stderr.splitlines():
        text = line.strip()
        if not text or text.startswith("cli-pid:"):
            continue
        if not text.startswith("{"):
            continue
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ProtocolError("malformed JSON observation on stderr") from exc
        if isinstance(payload, dict) and "observation" in payload:
            envelopes.append(payload)
    return envelopes


def _kind(envelope: dict[str, Any]) -> str:
    obs = envelope.get("observation")
    if not isinstance(obs, dict):
        raise ProtocolError("observation envelope missing observation object")
    kind = obs.get("kind")
    if not isinstance(kind, str):
        raise ProtocolError("observation missing kind")
    return kind


def assert_manifest_with_events_stream(
    envelopes: list[dict[str, Any]],
    *,
    job_id: str,
    device_id: str,
) -> None:
    if not envelopes:
        raise ProtocolError("expected observation envelopes on stderr")
    kinds = [_kind(item) for item in envelopes]
    expected = ["started", "log", "application", "progress", "completed"]
    if kinds != expected:
        raise ProtocolError(f"unexpected observation order for {device_id}: {kinds!r}")
    for envelope in envelopes:
        if envelope.get("job_id") != job_id:
            raise ProtocolError("observation job_id mismatch")
    app = next(
        item["observation"]["payload"]
        for item in envelopes
        if _kind(item) == "application"
    )
    if not isinstance(app, dict) or app.get("stage") != "mid":
        raise ProtocolError("unexpected application event payload")
    if envelopes[1]["observation"].get("message") != "phase":
        raise ProtocolError("unexpected log message")
    progress = envelopes[3]["observation"]
    if progress.get("completed") != 1 or progress.get("total") != 1:
        raise ProtocolError("unexpected progress values")
    assert_monotonic_sequences(envelopes)


def assert_monotonic_sequences(envelopes: list[dict[str, Any]]) -> None:
    sequences = [item.get("sequence") for item in envelopes]
    if any(not isinstance(s, int) for s in sequences):
        raise ProtocolError("non-integer observation sequence")
    for prev, current in zip(sequences, sequences[1:]):
        if current <= prev:
            raise ProtocolError("observation sequences must increase")
