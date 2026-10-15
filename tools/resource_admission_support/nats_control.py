"""NATS control channel for explicit release signals (no sleep-as-proof)."""

from __future__ import annotations

import asyncio
import json
from typing import Any

import nats


async def publish_control(
    nats_url: str,
    subject: str,
    message: dict[str, Any],
    *,
    timeout: float = 15.0,
) -> None:
    async with asyncio.timeout(timeout):
        connection = await nats.connect(nats_url, connect_timeout=timeout)
        try:
            deadline = asyncio.get_running_loop().time() + timeout
            while True:
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    raise TimeoutError("Active invocation did not acknowledge the control request")
                try:
                    reply = await connection.request(subject, json.dumps(message).encode("utf-8"), timeout=min(1.0, remaining))
                except (nats.errors.TimeoutError, nats.errors.NoRespondersError):
                    await asyncio.sleep(0.05)
                    continue
                acknowledged = json.loads(reply.data.decode("utf-8"))
                if acknowledged.get("released") == message.get("execution_id"):
                    break
        finally:
            await connection.drain()


async def wait_for_control(
    nats_url: str,
    subject: str,
    *,
    expected_type: str,
    deadline: float,
    poll_interval: float = 0.05,
) -> dict[str, Any]:
    loop = asyncio.get_running_loop()
    end = loop.time() + max(0.0, deadline - loop.time())
    subscription = await nats.connect(nats_url, connect_timeout=5.0)
    try:
        sub = await subscription.subscribe(subject)
        while loop.time() < end:
            remaining = end - loop.time()
            if remaining <= 0:
                break
            try:
                msg = await asyncio.wait_for(
                    sub.next_msg(), timeout=min(poll_interval, remaining)
                )
            except TimeoutError:
                continue
            payload = json.loads(msg.data.decode("utf-8"))
            if isinstance(payload, dict) and payload.get("type") == expected_type:
                return payload
        raise TimeoutError(
            f"control message {expected_type!r} not received on {subject}"
        )
    finally:
        await subscription.drain()
