"""Non-destructive handles for an already verified, owned Windows interpreter."""

from __future__ import annotations

import ctypes
import time
from ctypes import wintypes


class OwnedWindowsProcess:
    def __init__(self, pid: int) -> None:
        self._api = ctypes.WinDLL("kernel32", use_last_error=True)
        self._api.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
        self._api.OpenProcess.restype = wintypes.HANDLE
        self._api.WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]
        self._api.WaitForSingleObject.restype = wintypes.DWORD
        self._api.TerminateProcess.argtypes = [wintypes.HANDLE, wintypes.UINT]
        self._api.TerminateProcess.restype = wintypes.BOOL
        self._api.CloseHandle.argtypes = [wintypes.HANDLE]
        self._api.CloseHandle.restype = wintypes.BOOL
        self._handle = self._api.OpenProcess(0x100000 | 0x1000 | 0x0001, False, pid)
        if not self._handle:
            raise ctypes.WinError(ctypes.get_last_error())

    def exited(self, *, deadline: float) -> bool:
        milliseconds = max(0, int((deadline - time.monotonic()) * 1000))
        result = self._api.WaitForSingleObject(self._handle, milliseconds)
        if result == 0:
            return True
        if result == 258:
            return False
        raise ctypes.WinError(ctypes.get_last_error())

    def terminate(self) -> None:
        if not self._api.TerminateProcess(self._handle, 1):
            raise ctypes.WinError(ctypes.get_last_error())

    def close(self) -> None:
        if self._handle:
            self._api.CloseHandle(self._handle)
            self._handle = None
