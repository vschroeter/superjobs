"""JetStream resource evidence for broker restart verification (orchestrator-side)."""

from __future__ import annotations

import asyncio
import time
from typing import Any

import msgpack
import nats
from nats.js.errors import NotFoundError

from superjobs.transport.implementations.nats import NatsQueueConfig

PERSISTED_FILE_RESOURCE_KEYS = (
    "work_stream",
    "observation_stream",
    "completion_bucket",
    "idempotency_bucket",
)


class BrokerResourceError(RuntimeError):
    """Broker resources missing or not using persistent FILE storage."""


def _storage_name(value: Any) -> str:
    if value is None:
        return "missing"
    name = getattr(value, "name", None)
    if isinstance(name, str):
        return name.lower()
    text = str(value).lower()
    if "file" in text:
        return "file"
    return text


async def _collect_work_messages(jetstream: Any, stream_name: str) -> list[dict[str, Any]]:
    try:
        work_info = await jetstream.stream_info(stream_name)
    except NotFoundError:
        return []
    state = work_info.state
    first_seq = int(getattr(state, "first_seq", 1) or 1)
    last_seq = int(getattr(state, "last_seq", 0) or 0)
    if last_seq < first_seq:
        return []
    messages: list[dict[str, Any]] = []
    for seq in range(first_seq, last_seq + 1):
        try:
            message = await jetstream.get_msg(stream_name, seq=seq)
        except NotFoundError:
            continue
        job_id: str | None = None
        job_version: str | None = None
        try:
            body = msgpack.unpackb(message.data, raw=False)
            if isinstance(body, dict):
                raw_job_id = body.get("job_id")
                if isinstance(raw_job_id, str):
                    job_id = raw_job_id
                # The work envelope carries execution identity; Job identity
                # (including version) is encoded in the routing subject.
                if ":" in message.subject:
                    job_version = message.subject.rsplit(":", 1)[1]
        except Exception:
            job_id = None
            job_version = None
        messages.append(
            {
                "seq": seq,
                "subject": message.subject,
                "job_id": job_id,
                "version": job_version,
            },
        )
    return messages


async def _collect_async(url: str, queue_config: NatsQueueConfig) -> dict[str, Any]:
    connection = await nats.connect(url, connect_timeout=2, max_reconnect_attempts=0)
    try:
        jetstream = connection.jetstream(timeout=2)
        try:
            work_info = await jetstream.stream_info(queue_config.stream)
        except NotFoundError as exc:
            raise BrokerResourceError(
                f"work stream {queue_config.stream!r} missing; persisted resources not recovered",
            ) from exc
        try:
            observation_info = await jetstream.stream_info(queue_config.observation_stream)
        except NotFoundError as exc:
            raise BrokerResourceError(
                f"observation stream {queue_config.observation_stream!r} missing; "
                "persisted resources not recovered",
            ) from exc
        try:
            completion_kv = await jetstream.key_value(queue_config.completion_bucket)
            idempotency_kv = await jetstream.key_value(queue_config.idempotency_bucket)
        except NotFoundError as exc:
            raise BrokerResourceError(
                "completion or idempotency KV bucket missing; persisted resources not recovered",
            ) from exc
        completion_status = await completion_kv.status()
        idempotency_status = await idempotency_kv.status()
        work_messages = await _collect_work_messages(jetstream, queue_config.stream)

        def _kv_section(status: Any) -> dict[str, Any]:
            stream_info = status.stream_info
            return {
                "name": status.bucket,
                "values": status.values,
                "bytes": stream_info.state.bytes,
                "storage": _storage_name(stream_info.config.storage),
            }

        return {
            "work_stream": {
                "name": work_info.config.name,
                "storage": _storage_name(work_info.config.storage),
                "messages": work_info.state.messages,
                "bytes": work_info.state.bytes,
                "subjects": list(work_info.config.subjects or ()),
            },
            "observation_stream": {
                "name": observation_info.config.name,
                "storage": _storage_name(observation_info.config.storage),
                "messages": observation_info.state.messages,
                "bytes": observation_info.state.bytes,
            },
            "completion_bucket": _kv_section(completion_status),
            "idempotency_bucket": _kv_section(idempotency_status),
            "work_messages": work_messages,
        }
    finally:
        await connection.close()


def collect_broker_resource_evidence(
    url: str,
    queue_config: NatsQueueConfig,
    *,
    deadline: float | None = None,
) -> dict[str, Any]:
    async def _run() -> dict[str, Any]:
        if deadline is not None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise BrokerResourceError("resource collection exceeded scenario budget")
            return await asyncio.wait_for(
                _collect_async(url, queue_config),
                timeout=remaining,
            )
        return await _collect_async(url, queue_config)

    return asyncio.run(_run())


def assert_persisted_file_storage(evidence: dict[str, Any]) -> None:
    for key in PERSISTED_FILE_RESOURCE_KEYS:
        section = evidence.get(key)
        if not isinstance(section, dict):
            raise BrokerResourceError(f"missing {key} evidence")
        storage = section.get("storage")
        if storage != "file":
            raise BrokerResourceError(
                f"{key} storage {storage!r} != file (memory-only broker config?)",
            )


def assert_resources_nonempty(evidence: dict[str, Any]) -> None:
    work = evidence.get("work_stream")
    if not isinstance(work, dict):
        raise BrokerResourceError("missing work_stream evidence")
    messages = work.get("messages")
    if not isinstance(messages, int) or messages < 1:
        raise BrokerResourceError(
            f"work stream has no persisted messages ({messages!r}); empty store substituted?",
        )
    completion = evidence.get("completion_bucket")
    if not isinstance(completion, dict):
        raise BrokerResourceError("missing completion_bucket evidence")
    values = completion.get("values")
    if not isinstance(values, int) or values < 1:
        raise BrokerResourceError(
            f"completion bucket empty ({values!r}); durable completion not persisted?",
        )
    idempotency = evidence.get("idempotency_bucket")
    if not isinstance(idempotency, dict):
        raise BrokerResourceError("missing idempotency_bucket evidence")
    idem_values = idempotency.get("values")
    if not isinstance(idem_values, int) or idem_values < 1:
        raise BrokerResourceError(
            f"idempotency bucket empty ({idem_values!r}); durable idempotency not persisted?",
        )


def assert_pending_work_retained(
    evidence: dict[str, Any],
    *,
    pending_execution_id: str,
    pending_work_subject: str | None = None,
) -> None:
    raw_messages = evidence.get("work_messages")
    if not isinstance(raw_messages, list):
        raise BrokerResourceError("missing work_messages evidence")
    matching = [
        item
        for item in raw_messages
        if isinstance(item, dict) and item.get("job_id") == pending_execution_id
    ]
    if not matching:
        raise BrokerResourceError(
            f"pending execution_id {pending_execution_id!r} not found in retained work stream "
            f"messages ({raw_messages!r})",
        )
    if pending_work_subject is not None:
        subjects = [str(item.get("subject") or "") for item in matching]
        if pending_work_subject not in subjects:
            raise BrokerResourceError(
                f"pending work subject {pending_work_subject!r} not among retained messages "
                f"({subjects!r})",
            )


def assert_resources_survive_restart(
    before: dict[str, Any],
    after: dict[str, Any],
) -> None:
    assert_persisted_file_storage(after)
    before_work = before.get("work_stream", {})
    after_work = after.get("work_stream", {})
    if before_work.get("name") != after_work.get("name"):
        raise BrokerResourceError("work stream name changed across broker restart")
    if (after_work.get("messages") or 0) < (before_work.get("messages") or 0):
        raise BrokerResourceError("work stream message count shrank after restart")
