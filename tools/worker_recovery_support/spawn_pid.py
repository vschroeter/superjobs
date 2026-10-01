"""Relate Popen.pid to worker-reported pid (Windows venv launcher vs interpreter)."""

from __future__ import annotations

import sys
from typing import Callable


def _parent_pid_unix(pid: int) -> int | None:
    status = f"/proc/{pid}/status"
    try:
        with open(status, encoding="utf-8") as handle:
            for line in handle:
                if line.startswith("PPid:"):
                    return int(line.split()[1])
    except OSError:
        return None
    return None


def _parent_map_windows() -> dict[int, int]:
    import ctypes
    from ctypes import wintypes

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)

    TH32CS_SNAPPROCESS = 0x00000002
    INVALID_HANDLE_VALUE = ctypes.c_void_p(-1).value

    class PROCESSENTRY32W(ctypes.Structure):
        _fields_ = [
            ("dwSize", wintypes.DWORD),
            ("cntUsage", wintypes.DWORD),
            ("th32ProcessID", wintypes.DWORD),
            ("th32DefaultHeapID", ctypes.POINTER(ctypes.c_ulong)),
            ("th32ModuleID", wintypes.DWORD),
            ("cntThreads", wintypes.DWORD),
            ("th32ParentProcessID", wintypes.DWORD),
            ("pcPriClassBase", ctypes.c_long),
            ("dwFlags", wintypes.DWORD),
            ("szExeFile", wintypes.WCHAR * 260),
        ]

    kernel32.CreateToolhelp32Snapshot.argtypes = [wintypes.DWORD, wintypes.DWORD]
    kernel32.CreateToolhelp32Snapshot.restype = wintypes.HANDLE
    kernel32.Process32FirstW.argtypes = [wintypes.HANDLE, ctypes.POINTER(PROCESSENTRY32W)]
    kernel32.Process32FirstW.restype = wintypes.BOOL
    kernel32.Process32NextW.argtypes = [wintypes.HANDLE, ctypes.POINTER(PROCESSENTRY32W)]
    kernel32.Process32NextW.restype = wintypes.BOOL
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.CloseHandle.restype = wintypes.BOOL

    snapshot = kernel32.CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0)
    if snapshot == INVALID_HANDLE_VALUE:
        return {}
    mapping: dict[int, int] = {}
    entry = PROCESSENTRY32W()
    entry.dwSize = ctypes.sizeof(PROCESSENTRY32W)
    try:
        if not kernel32.Process32FirstW(snapshot, ctypes.byref(entry)):
            return mapping
        while True:
            mapping[int(entry.th32ProcessID)] = int(entry.th32ParentProcessID)
            if not kernel32.Process32NextW(snapshot, ctypes.byref(entry)):
                break
    finally:
        kernel32.CloseHandle(snapshot)
    return mapping


def _parent_pid_for(pid: int, parent_map: dict[int, int] | None = None) -> int | None:
    if sys.platform == "win32":
        mapping = parent_map if parent_map is not None else _parent_map_windows()
        return mapping.get(pid)
    return _parent_pid_unix(pid)


def marker_pid_in_spawn_tree(
    spawn_pid: int,
    marker_pid: int,
    *,
    parent_pid: Callable[[int], int | None] | None = None,
) -> bool:
    """True when marker_pid equals spawn_pid or belongs to the same process tree."""
    if marker_pid == spawn_pid:
        return True
    if sys.platform == "win32":
        parent_map = _parent_map_windows()
        resolve = (lambda p: parent_map.get(p)) if parent_pid is None else parent_pid
    else:
        parent_map = None
        resolve = parent_pid if parent_pid is not None else _parent_pid_unix

    current = marker_pid
    seen: set[int] = set()
    while current and current not in seen:
        if current == spawn_pid:
            return True
        seen.add(current)
        parent = resolve(current)
        if parent is None or parent <= 0:
            break
        current = parent

    if parent_map is not None:
        children: dict[int, list[int]] = {}
        for child, parent in parent_map.items():
            children.setdefault(parent, []).append(child)
        stack = [spawn_pid]
        visited = {spawn_pid}
        while stack:
            current = stack.pop()
            if current == marker_pid:
                return True
            for child in children.get(current, ()):
                if child not in visited:
                    visited.add(child)
                    stack.append(child)
    return False
