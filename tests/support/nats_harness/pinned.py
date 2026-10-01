"""Reviewed NATS server pin for integration tests."""

from __future__ import annotations

NATS_SERVER_VERSION = "2.15.0"

GITHUB_RELEASE_TAG = f"v{NATS_SERVER_VERSION}"

RELEASE_BASE_URL = (
    "https://github.com/nats-io/nats-server/releases/download/" f"{GITHUB_RELEASE_TAG}"
)

# Total wall-clock budget for owned-server startup (log line + JetStream readiness).
STARTUP_TIMEOUT_SECONDS = 30

# Total wall-clock budget for owned-server shutdown (terminate + kill if needed).
SHUTDOWN_TIMEOUT_SECONDS = 30

ENV_EXECUTABLE = "NATS_EXECUTABLE"
ENV_URL = "NATS_URL"
ENV_CACHE_DIR = "SUPERJOBS_NATS_CACHE"
DEFAULT_CACHE_DIR_NAME = "superjobs/nats"

BUNDLED_CHECKSUMS_FILENAME = f"checksums.{GITHUB_RELEASE_TAG}.txt"
