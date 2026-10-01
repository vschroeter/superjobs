from __future__ import annotations

import hashlib
import json
import os
import platform
import re
import shutil
import stat
import subprocess
import sys
import tarfile
import tempfile
import urllib.error
import urllib.request
import zipfile
from dataclasses import dataclass
from pathlib import Path

from tests.support.nats_harness.pinned import (
    BUNDLED_CHECKSUMS_FILENAME,
    DEFAULT_CACHE_DIR_NAME,
    ENV_CACHE_DIR,
    ENV_EXECUTABLE,
    GITHUB_RELEASE_TAG,
    NATS_SERVER_VERSION,
    RELEASE_BASE_URL,
)

_CHECKSUMS_LINE = re.compile(
    r"^(?P<hash>[0-9a-fA-F]{64})\s+(?P<name>\S+)\s*$"
)

_MANIFEST_NAME = "provisioned.json"


class NatsProvisionError(RuntimeError):
    """Raised when the NATS test binary cannot be provisioned safely."""


@dataclass(frozen=True)
class PlatformAsset:
    archive_name: str
    executable_relative: str


def default_cache_dir() -> Path:
    override = os.environ.get(ENV_CACHE_DIR)
    if override:
        return Path(override).expanduser()
    return Path.home() / ".cache" / DEFAULT_CACHE_DIR_NAME


def platform_asset() -> PlatformAsset:
    system = sys.platform
    machine = platform.machine().lower()
    versioned = f"nats-server-v{NATS_SERVER_VERSION}"
    if system == "win32":
        if machine not in {"amd64", "x86_64"}:
            raise NatsProvisionError(
                f"Unsupported Windows machine for pinned NATS tests: {machine}"
            )
        archive = f"{versioned}-windows-amd64.zip"
        return PlatformAsset(
            archive_name=archive,
            executable_relative=f"{versioned}-windows-amd64/nats-server.exe",
        )
    if system.startswith("linux"):
        if machine in {"x86_64", "amd64"}:
            suffix = "linux-amd64"
        else:
            raise NatsProvisionError(
                f"Unsupported Linux machine for pinned NATS tests: {machine}"
            )
        archive = f"{versioned}-{suffix}.tar.gz"
        return PlatformAsset(
            archive_name=archive,
            executable_relative=f"{versioned}-{suffix}/nats-server",
        )
    raise NatsProvisionError(f"Unsupported platform for pinned NATS tests: {system}")


def parse_checksums(text: str) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for line in text.splitlines():
        match = _CHECKSUMS_LINE.match(line.strip())
        if match is None:
            continue
        mapping[match.group("name")] = match.group("hash").lower()
    return mapping


def bundled_checksums_path() -> Path:
    return Path(__file__).with_name("fixtures") / BUNDLED_CHECKSUMS_FILENAME


def load_checksums_text() -> str:
    bundled = bundled_checksums_path()
    if not bundled.is_file():
        raise NatsProvisionError(
            "Missing reviewed NATS archive digests at "
            f"{bundled}. Copy upstream checksums.txt for {GITHUB_RELEASE_TAG} "
            "into that path (see fixtures/README.md)."
        )
    return bundled.read_text(encoding="utf-8")


def expected_archive_digest(archive_name: str) -> str:
    mapping = parse_checksums(load_checksums_text())
    try:
        return mapping[archive_name]
    except KeyError as error:
        raise NatsProvisionError(
            f"Reviewed NATS digests do not list archive {archive_name!r}"
        ) from error


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _download_archive(archive_name: str, destination: Path) -> None:
    url = f"{RELEASE_BASE_URL}/{archive_name}"
    try:
        with urllib.request.urlopen(url, timeout=20) as response:
            destination.write_bytes(response.read())
    except (urllib.error.URLError, TimeoutError) as error:
        raise NatsProvisionError(
            f"Unable to download NATS archive {archive_name!r} from {url}"
        ) from error


def _ensure_verified_archive(
    archive_path: Path,
    archive_name: str,
    expected_digest: str,
) -> None:
    if archive_path.is_file():
        actual = sha256_file(archive_path)
        if actual == expected_digest:
            return
        raise NatsProvisionError(
            "Cached NATS archive digest mismatch for "
            f"{archive_name}: expected {expected_digest}, got {actual}"
        )
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=archive_path.parent) as temp_dir_name:
        temp_archive = Path(temp_dir_name) / archive_name
        _download_archive(archive_name, temp_archive)
        actual = sha256_file(temp_archive)
        if actual != expected_digest:
            raise NatsProvisionError(
                "Downloaded NATS archive digest mismatch for "
                f"{archive_name}: expected {expected_digest}, got {actual}"
            )
        shutil.move(str(temp_archive), str(archive_path))


def _extract_executable(
    archive_path: Path,
    destination: Path,
    asset: PlatformAsset,
) -> Path:
    member_name = asset.executable_relative.replace("\\", "/")
    destination.mkdir(parents=True, exist_ok=True)
    if archive_path.name.endswith(".zip"):
        with zipfile.ZipFile(archive_path) as archive:
            try:
                payload = archive.read(member_name)
            except KeyError as error:
                raise NatsProvisionError(
                    f"NATS archive {archive_path.name!r} does not contain {member_name!r}"
                ) from error
        executable = destination / member_name
        executable.parent.mkdir(parents=True, exist_ok=True)
        executable.write_bytes(payload)
    else:
        with tarfile.open(archive_path, "r:gz") as archive:
            try:
                member = archive.getmember(member_name)
            except KeyError as error:
                raise NatsProvisionError(
                    f"NATS archive {archive_path.name!r} does not contain {member_name!r}"
                ) from error
            if not member.isfile():
                raise NatsProvisionError("NATS executable must be a regular archive member")
            source = archive.extractfile(member)
            assert source is not None
            executable = destination / member_name
            executable.parent.mkdir(parents=True, exist_ok=True)
            with source, executable.open("wb") as output:
                shutil.copyfileobj(source, output)
        executable = destination / member_name
    if not executable.is_file():
        raise NatsProvisionError(
            f"Extracted NATS archive did not contain expected executable {executable}"
        )
    if sys.platform != "win32":
        executable.chmod(executable.stat().st_mode | stat.S_IXUSR)
    return executable


def _read_manifest(install_root: Path) -> dict[str, str] | None:
    manifest_path = install_root / _MANIFEST_NAME
    if not manifest_path.is_file():
        return None
    try:
        data = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    if not isinstance(data, dict):
        return None
    archive = data.get("archive_name")
    archive_sha256 = data.get("archive_sha256")
    executable_sha256 = data.get("executable_sha256")
    if not isinstance(archive, str) or not isinstance(archive_sha256, str):
        return None
    if not isinstance(executable_sha256, str):
        return None
    return {
        "archive_name": archive,
        "archive_sha256": archive_sha256.lower(),
        "executable_sha256": executable_sha256.lower(),
    }


def _write_manifest(
    install_root: Path,
    *,
    archive_name: str,
    archive_sha256: str,
    executable_sha256: str,
) -> None:
    manifest_path = install_root / _MANIFEST_NAME
    manifest_path.write_text(
        json.dumps(
            {
                "archive_name": archive_name,
                "archive_sha256": archive_sha256,
                "executable_sha256": executable_sha256,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def verify_executable_version(executable: Path) -> None:
    try:
        completed = subprocess.run(
            [str(executable), "--version"],
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
            creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise NatsProvisionError(
            f"Unable to execute NATS server binary {executable}"
        ) from error
    output = f"{completed.stdout}\n{completed.stderr}"
    if completed.returncode != 0 or re.fullmatch(
        rf"nats-server: v{re.escape(NATS_SERVER_VERSION)}", output.strip()
    ) is None:
        raise NatsProvisionError(
            "NATS executable version check failed: "
            f"expected {NATS_SERVER_VERSION} in output, got {output!r}"
        )


def resolve_executable() -> Path:
    override = os.environ.get(ENV_EXECUTABLE)
    if override:
        executable = Path(override).expanduser()
        if not executable.is_file():
            raise NatsProvisionError(
                f"{ENV_EXECUTABLE} points to missing file: {executable}"
            )
        verify_executable_version(executable)
        return executable

    asset = platform_asset()
    cache_dir = default_cache_dir()
    install_root = cache_dir / GITHUB_RELEASE_TAG
    executable = install_root / asset.executable_relative
    expected_digest = expected_archive_digest(asset.archive_name)
    archive_path = install_root / asset.archive_name

    _ensure_verified_archive(archive_path, asset.archive_name, expected_digest)

    manifest = _read_manifest(install_root)
    needs_extract = not executable.is_file()
    if not needs_extract and manifest is not None:
        if (
            manifest["archive_name"] != asset.archive_name
            or manifest["archive_sha256"] != expected_digest
        ):
            needs_extract = True
        else:
            actual_executable_digest = sha256_file(executable)
            if actual_executable_digest != manifest["executable_sha256"]:
                needs_extract = True
    elif not needs_extract:
        needs_extract = True

    if needs_extract:
        install_root.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=install_root) as temporary:
            extracted = _extract_executable(archive_path, Path(temporary), asset)
            verify_executable_version(extracted)
            executable.parent.mkdir(parents=True, exist_ok=True)
            os.replace(extracted, executable)
        _write_manifest(
            install_root,
            archive_name=asset.archive_name,
            archive_sha256=expected_digest,
            executable_sha256=sha256_file(executable),
        )

    if not executable.is_file():
        raise NatsProvisionError(
            f"NATS executable missing after provisioning: {executable}"
        )
    verify_executable_version(executable)
    return executable
