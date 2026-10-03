"""Environment metadata capture for performance reports (harness-side)."""

from __future__ import annotations

import json
import os
import platform
import subprocess
import sys
from pathlib import Path
from typing import Any

from tests.support.nats_harness.pinned import NATS_SERVER_VERSION


def git_revision(repo_root: Path) -> str:
    try:
        completed = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_root,
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError:
        return "unknown"
    if completed.returncode != 0:
        return "unknown"
    return completed.stdout.strip()


def git_clean(repo_root: Path) -> bool | None:
    try:
        completed = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=repo_root,
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError:
        return None
    if completed.returncode != 0:
        return None
    return completed.stdout.strip() == ""


def _powershell_json(script: str) -> dict[str, Any] | None:
    if platform.system() != "Windows":
        return None
    try:
        completed = subprocess.run(
            [
                "powershell",
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                script,
            ],
            capture_output=True,
            text=True,
            check=False,
            creationflags=subprocess.CREATE_NO_WINDOW,
        )
    except OSError:
        return None
    if completed.returncode != 0 or not completed.stdout.strip():
        return None
    try:
        return json.loads(completed.stdout)
    except json.JSONDecodeError:
        return None


def capture_hardware() -> dict[str, Any]:
    override = os.environ.get("SUPERJOBS_PERF_HARDWARE_JSON")
    if override:
        try:
            return json.loads(override)
        except json.JSONDecodeError:
            pass

    payload = _powershell_json(
        "& {"
        "$cpu = Get-CimInstance Win32_Processor | Select-Object -First 1 Name, NumberOfCores, NumberOfLogicalProcessors;"
        "$mem = (Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory;"
        "$os = Get-CimInstance Win32_OperatingSystem;"
        "$cPart = Get-Partition -DriveLetter C -ErrorAction SilentlyContinue;"
        "$disk = $null;"
        "if ($cPart) { $disk = Get-Disk -Number $cPart.DiskNumber -ErrorAction SilentlyContinue };"
        "@{"
        "source='powershell_readonly';"
        "cpu_model=$cpu.Name;"
        "cpu_cores=$cpu.NumberOfCores;"
        "cpu_logical=$cpu.NumberOfLogicalProcessors;"
        "ram_bytes=$mem;"
        "os_caption=$os.Caption;"
        "c_drive_disk_number=$cPart.DiskNumber;"
        "c_drive_model=$disk.Model;"
        "c_drive_media_type=$disk.MediaType;"
        "} | ConvertTo-Json -Compress"
        "}",
    )
    if payload is None:
        return {
            "source": "unknown",
            "cpu_model": "unknown",
            "cpu_cores": None,
            "cpu_logical": None,
            "ram_bytes": None,
            "os_caption": None,
            "c_drive_storage_medium": "unknown",
            "c_drive_storage_evidence": "hardware capture unavailable",
        }

    media = str(payload.get("c_drive_media_type") or "").upper()
    model = str(payload.get("c_drive_model") or "")
    storage = "unknown"
    evidence = "C: partition disk mapping unavailable"
    if payload.get("c_drive_disk_number") is not None:
        evidence = (
            f"C: maps to Disk{payload.get('c_drive_disk_number')} "
            f"model={model!r} media_type={payload.get('c_drive_media_type')!r}"
        )
        if "SSD" in media or "SSD" in model.upper():
            storage = "ssd"

    return {
        "source": payload.get("source", "powershell_readonly"),
        "cpu_model": payload.get("cpu_model") or "unknown",
        "cpu_cores": payload.get("cpu_cores"),
        "cpu_logical": payload.get("cpu_logical"),
        "ram_bytes": payload.get("ram_bytes"),
        "os_caption": payload.get("os_caption"),
        "c_drive_media_type": payload.get("c_drive_media_type"),
        "c_drive_model": payload.get("c_drive_model"),
        "c_drive_disk_number": payload.get("c_drive_disk_number"),
        "c_drive_storage_medium": storage,
        "c_drive_storage_evidence": evidence,
    }


def broker_storage_for_path(
    broker_store_path: Path | None,
    hardware: dict[str, Any],
) -> tuple[str, str]:
    if broker_store_path is None:
        return "unknown", "broker JetStream store path unavailable"
    resolved = broker_store_path.resolve()
    drive = resolved.drive.upper()
    if not drive:
        return "unknown", f"broker store path has no drive letter: {resolved}"
    if drive != "C:":
        return (
            "unknown",
            f"broker store on {drive}; medium not mapped (C: inference only)",
        )
    medium = str(hardware.get("c_drive_storage_medium") or "unknown")
    evidence = str(
        hardware.get("c_drive_storage_evidence") or "C: disk mapping unavailable",
    )
    return medium, f"broker store on {drive}; {evidence}"


_DEPENDENCY_PROBE_PACKAGES = (
    "superjobs",
    "faststream",
    "nats-py",
    "msgpack",
    "pydantic",
)

_MEASUREMENT_CLOCK_PROBE_SCRIPT = """
import json
import time

def record(function: str) -> dict:
    info = time.get_clock_info(function)
    return {
        "function": function,
        "implementation": info.implementation,
        "resolution": info.resolution,
        "monotonic": info.monotonic,
        "adjustable": info.adjustable,
    }

print(
    json.dumps(
        {
            "measurement_clock": record("perf_counter"),
            "legacy_monotonic_clock": record("monotonic"),
        },
    ),
)
"""

_DEPENDENCY_PROBE_SCRIPT = """
import importlib.metadata as metadata
import json
import sys

packages = {packages!r}
payload = {{"python": sys.version}}
for name in packages:
    try:
        payload[name] = metadata.version(name)
    except metadata.PackageNotFoundError:
        payload[name] = "missing"
print(json.dumps(payload))
""".format(
    packages=list(_DEPENDENCY_PROBE_PACKAGES),
)


def probe_measurement_clocks(python: Path) -> dict[str, Any]:
    command = [
        str(python),
        "-c",
        _MEASUREMENT_CLOCK_PROBE_SCRIPT,
    ]
    try:
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,
            creationflags=subprocess.CREATE_NO_WINDOW if platform.system() == "Windows" else 0,
        )
    except OSError as exc:
        return {
            "measurement_clock": {"function": "perf_counter", "probe_error": str(exc)},
        }
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or "").strip()
        return {
            "measurement_clock": {
                "function": "perf_counter",
                "probe_error": f"exit {completed.returncode}: {detail}",
            },
        }
    try:
        return json.loads(completed.stdout)
    except json.JSONDecodeError:
        return {
            "measurement_clock": {
                "function": "perf_counter",
                "probe_error": "invalid json from probe",
            },
        }


def probe_role_dependency_versions(python: Path) -> dict[str, str]:
    command = [
        str(python),
        "-c",
        _DEPENDENCY_PROBE_SCRIPT,
    ]
    try:
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,
            creationflags=subprocess.CREATE_NO_WINDOW if platform.system() == "Windows" else 0,
        )
    except OSError as exc:
        return {"python": f"probe failed: {exc}"}
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or "").strip()
        return {"python": f"probe exit {completed.returncode}: {detail}"}
    try:
        payload = json.loads(completed.stdout)
    except json.JSONDecodeError:
        return {"python": "probe returned invalid json"}
    versions: dict[str, str] = {}
    for key, value in payload.items():
        if key == "python":
            versions["python"] = str(value).split(" [")[0]
        else:
            versions[str(key)] = str(value)
    return versions


def child_dependency_versions(
    origins: dict[str, Any] | None,
    *,
    producer_python: Path | None = None,
    worker_python: Path | None = None,
) -> dict[str, Any]:
    if producer_python is not None or worker_python is not None:
        payload: dict[str, Any] = {}
        if producer_python is not None:
            payload["producer"] = probe_role_dependency_versions(producer_python)
        if worker_python is not None:
            payload["worker"] = probe_role_dependency_versions(worker_python)
        return payload
    if not origins:
        return {"producer": {"python": "unknown"}, "worker": {"python": "unknown"}}
    return {
        "producer": probe_role_dependency_versions_from_origins(origins.get("producer")),
        "worker": probe_role_dependency_versions_from_origins(origins.get("worker")),
    }


def probe_role_dependency_versions_from_origins(role: dict[str, Any] | None) -> dict[str, str]:
    versions: dict[str, str] = {"python": "unknown"}
    if not role:
        return versions
    versions["python"] = str(role.get("sys_version", "unknown")).split(" [")[0]
    for package in role.get("packages") or []:
        name = package.get("name")
        if name in _DEPENDENCY_PROBE_PACKAGES:
            versions[str(name)] = str(package.get("version") or "unknown")
    return versions


def environment_block(
    *,
    repo_root: Path,
    origins: dict[str, Any] | None,
    broker_store_path: Path | None,
    fixture_bytes: dict[str, Any],
    report_date: str | None = None,
    producer_python: Path | None = None,
    worker_python: Path | None = None,
) -> dict[str, Any]:
    hardware = capture_hardware()
    store_path = broker_store_path
    if store_path is None:
        store_path_str = "unknown"
    else:
        store_path_str = str(store_path.resolve())

    storage_medium, storage_evidence = broker_storage_for_path(store_path, hardware)
    revision = git_revision(repo_root)
    clean = git_clean(repo_root)
    clock_probe: dict[str, Any] = {}
    if producer_python is not None:
        clock_probe = probe_measurement_clocks(producer_python)
    return {
        "revision": revision,
        "revision_clean": clean,
        "report_date": report_date,
        "platform": {
            "system": platform.system(),
            "release": platform.release(),
            "machine": platform.machine(),
        },
        "hardware": hardware,
        "storage_medium": storage_medium,
        "storage_evidence": storage_evidence,
        "broker_jetstream_store_path": store_path_str,
        "harness_python": sys.version,
        "child_runtime": child_dependency_versions(
            origins,
            producer_python=producer_python,
            worker_python=worker_python,
        ),
        "measurement_clock": clock_probe.get("measurement_clock"),
        "legacy_monotonic_clock": clock_probe.get("legacy_monotonic_clock"),
        "dependencies_note": (
            "child_runtime versions come from isolated importlib.metadata probes "
            "in each role venv"
        ),
        "nats_server_version": NATS_SERVER_VERSION,
        "fixture_request_bytes": fixture_bytes,
    }
