"""JetStream evidence for failed manifest observation replay (orchestrator-side)."""

from __future__ import annotations

import asyncio
from typing import Any

import msgpack
import nats
from nats.js.errors import NotFoundError

from superjobs.transport.implementations.nats import NatsQueueConfig

from tools.manifest_replay_repro_support.subjects import (
    execution_kv_key,
    observation_subject,
    sequence_kv_bucket,
)

_DEFAULT_CONFIG = NatsQueueConfig()


def _unpack_observation_batch(data: bytes | None) -> dict[str, Any]:
    if not data:
        return {"present": False, "event_count": 0, "sequences": []}
    try:
        body = msgpack.unpackb(data, raw=False)
    except Exception as exc:
        return {"present": True, "decode_error": repr(exc)}
    if not isinstance(body, dict):
        return {"present": True, "decode_error": f"unexpected body type {type(body)!r}"}
    events = body.get("events") or []
    sequences: list[int] = []
    terminal_kinds: list[str] = []
    for item in events:
        if not isinstance(item, dict):
            continue
        seq = item.get("sequence")
        if isinstance(seq, int):
            sequences.append(seq)
        data_block = item.get("data")
        if isinstance(data_block, dict):
            kind = data_block.get("kind")
            if kind is not None:
                terminal_kinds.append(str(kind))
    return {
        "present": True,
        "event_count": len(events),
        "sequences": sequences,
        "event_kind_hints": terminal_kinds,
    }


def _sequence_value(entry: Any) -> dict[str, Any]:
    raw = getattr(entry, "value", None)
    if raw is None:
        return {"present": False}
    try:
        unpacked = msgpack.unpackb(raw, raw=False)
    except Exception as exc:
        return {"present": True, "decode_error": repr(exc)}
    return {"present": True, "value": unpacked}


def _consumer_summary(info: Any) -> dict[str, Any]:
    config = getattr(info, "config", None)
    filter_subject = getattr(config, "filter_subject", None) if config is not None else None
    filter_subjects = list(getattr(config, "filter_subjects", None) or ())
    delivered = getattr(info, "delivered", None)
    ack_floor = getattr(info, "ack_floor", None)
    return {
        "name": getattr(info, "name", None),
        "filter_subject": filter_subject,
        "filter_subjects": filter_subjects,
        "num_pending": getattr(info, "num_pending", None),
        "num_ack_pending": getattr(info, "num_ack_pending", None),
        "delivered_consumer_seq": getattr(delivered, "consumer_seq", None),
        "delivered_stream_seq": getattr(delivered, "stream_seq", None),
        "ack_floor_consumer_seq": getattr(ack_floor, "consumer_seq", None),
        "ack_floor_stream_seq": getattr(ack_floor, "stream_seq", None),
    }


async def _probe_execution(
    jetstream: Any,
    *,
    job_id: str,
    queue_config: NatsQueueConfig,
) -> dict[str, Any]:
    subject = observation_subject(job_id, queue_config=queue_config)
    kv_key = execution_kv_key(job_id)
    snapshot: dict[str, Any] = {
        "execution_id": job_id,
        "observation_subject": subject,
        "sequence_kv_key": kv_key,
    }
    try:
        sequence_kv = await jetstream.key_value(sequence_kv_bucket(queue_config))
        try:
            entry = await sequence_kv.get(kv_key)
            snapshot["sequence_kv"] = _sequence_value(entry)
        except NotFoundError:
            snapshot["sequence_kv"] = {"present": False}
    except Exception as exc:
        snapshot["sequence_kv_error"] = repr(exc)

    try:
        message = await jetstream.get_msg(
            queue_config.observation_stream,
            subject=subject,
            direct=True,
        )
        snapshot["direct_get_msg"] = {
            "stream_seq": getattr(message, "seq", None),
            "time": str(getattr(message, "time", None)),
            "batch": _unpack_observation_batch(message.data),
        }
    except NotFoundError:
        snapshot["direct_get_msg"] = {"present": False}
    except Exception as exc:
        snapshot["direct_get_msg"] = {"error": repr(exc)}

    consumers: list[dict[str, Any]] = []
    try:
        consumer_infos = await jetstream.consumers_info(queue_config.observation_stream)
        for info in consumer_infos:
            summary = _consumer_summary(info)
            summary_filter = summary.get("filter_subject")
            extra_filters = summary.get("filter_subjects") or []
            if summary_filter == subject or subject in extra_filters:
                consumers.append(summary)
        snapshot["observation_consumers_for_subject"] = consumers
        if not consumers:
            snapshot["observation_consumers_note"] = (
                "no durable consumers on observation stream match this execution subject "
                "(ephemeral consumers may already be closed)"
            )
    except Exception as exc:
        snapshot["consumers_error"] = repr(exc)
    return snapshot


async def _collect_async(
    nats_url: str,
    execution_ids: list[str],
    *,
    queue_config: NatsQueueConfig | None = None,
) -> dict[str, Any]:
    config = queue_config or _DEFAULT_CONFIG
    connection = await nats.connect(nats_url, connect_timeout=5, max_reconnect_attempts=0)
    try:
        jetstream = connection.jetstream(timeout=5)
        executions: dict[str, Any] = {}
        for job_id in execution_ids:
            executions[job_id] = await _probe_execution(
                jetstream,
                job_id=job_id,
                queue_config=config,
            )
        return {
            "observation_stream": config.observation_stream,
            "sequence_kv_bucket": sequence_kv_bucket(config),
            "executions": executions,
        }
    finally:
        await connection.close()


def collect_broker_snapshots(
    nats_url: str,
    execution_ids: list[str],
) -> dict[str, Any]:
    if not execution_ids:
        return {"executions": {}}
    return asyncio.run(_collect_async(nats_url, execution_ids))


def attach_broker_snapshots(
    samples: list[dict[str, Any]],
    broker_payload: dict[str, Any],
) -> list[dict[str, Any]]:
    by_id = dict(broker_payload.get("executions") or {})
    updated: list[dict[str, Any]] = []
    for sample in samples:
        sample_copy = dict(sample)
        rows = []
        for row in sample.get("failure_diagnostics") or []:
            row_copy = dict(row)
            execution_id = row_copy.get("execution_id")
            if isinstance(execution_id, str) and execution_id in by_id:
                row_copy["broker_snapshot"] = by_id[execution_id]
            rows.append(row_copy)
        sample_copy["failure_diagnostics"] = rows
        updated.append(sample_copy)
    return updated
