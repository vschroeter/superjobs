from __future__ import annotations

import asyncio
import json
from pathlib import Path

import nats
import pytest

from tests.support.nats_harness import server

_REPO_ROOT = Path(__file__).resolve().parents[1]
_EVIDENCE_DIR = _REPO_ROOT / "dist" / "issues-53-54" / "harness"

_PROBE_SERVER_NAME = "superjobs-harness-extra-config-probe"

_ISSUE53_AUTH_EXTRA = """
authorization {
  users = [
    { user: "restricted", password: "restricted-pass" }
    { user: "admin", password: "admin-pass" }
  ]
}
no_auth_user: admin
"""


def _server_name(connection: nats.NATS) -> str:
    info = connection._server_info
    name = info.get("server_name")
    if not isinstance(name, str) or not name:
        raise AssertionError(f"NATS INFO missing server_name: {info!r}")
    return name


async def _probe_server_name(url: str) -> str:
    connection = await nats.connect(
        url, connect_timeout=2, max_reconnect_attempts=0,
    )
    try:
        return _server_name(connection)
    finally:
        await connection.close()


async def _probe_credentialless_jetstream(url: str) -> dict[str, object]:
    connection = await nats.connect(
        url, connect_timeout=2, max_reconnect_attempts=0,
    )
    try:
        account = await connection.jetstream(timeout=2).account_info()
        return {
            "server_name": _server_name(connection),
            "auth_required": connection._server_info.get("auth_required"),
            "memory": account.memory,
            "storage": account.storage,
            "streams": account.streams,
            "consumers": account.consumers,
        }
    finally:
        await connection.close()


def _write_evidence(name: str, payload: dict[str, object]) -> None:
    _EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
    path = _EVIDENCE_DIR / name
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


@pytest.mark.nats
def test_owned_server_extra_config_server_name_observed_across_restart() -> None:
    extra = f'server_name: "{_PROBE_SERVER_NAME}"\n'
    owner = server.OwnedNatsServer(extra_config=extra)
    target = owner.start()
    try:
        original_url = target.url
        before = asyncio.run(_probe_server_name(original_url))
        assert before == _PROBE_SERVER_NAME

        owner.restart(target)
        assert target.url == original_url

        after = asyncio.run(_probe_server_name(target.url))
        assert after == _PROBE_SERVER_NAME

        _write_evidence(
            "extra-config-server-name-restart.json",
            {
                "probe": "server_name_via_nats_info",
                "url_before_restart": original_url,
                "url_after_restart": target.url,
                "server_name_before": before,
                "server_name_after": after,
            },
        )
    finally:
        owner.stop(target)


@pytest.mark.nats
def test_owned_server_issue53_no_auth_user_admin_allows_credentialless_jetstream() -> None:
    """Harness readiness and probes must work without credentials when no_auth_user is admin."""
    owner = server.OwnedNatsServer(extra_config=_ISSUE53_AUTH_EXTRA)
    target = owner.start()
    try:
        assert target.url.startswith("nats://127.0.0.1:")
        evidence = asyncio.run(_probe_credentialless_jetstream(target.url))
        assert evidence["server_name"]
        _write_evidence("issue53-no-auth-user-admin.json", evidence)
    finally:
        owner.stop(target)
