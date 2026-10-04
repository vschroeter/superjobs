"""Deliver CTRL+C to a console-attached process on Windows (issue #42 verifier)."""

from __future__ import annotations

import argparse
import ctypes
import sys
import time

CTRL_C_EVENT = 0


def send_ctrl_c_to_console_group(target_pid: int, *, settle_seconds: float) -> None:
    """Attach to target console, ignore signals locally, broadcast CTRL+C."""
    k32 = ctypes.windll.kernel32
    if not k32.FreeConsole():
        raise OSError("FreeConsole failed")
    if not k32.AttachConsole(target_pid):
        raise OSError(f"AttachConsole({target_pid}) failed")
    handler_routine = ctypes.WINFUNCTYPE(ctypes.c_bool, ctypes.c_uint)(lambda _sig: True)
    if not k32.SetConsoleCtrlHandler(handler_routine, True):
        k32.FreeConsole()
        raise OSError("SetConsoleCtrlHandler failed")
    if not k32.GenerateConsoleCtrlEvent(CTRL_C_EVENT, 0):
        k32.SetConsoleCtrlHandler(handler_routine, False)
        k32.FreeConsole()
        raise OSError("GenerateConsoleCtrlEvent(CTRL_C_EVENT) failed")
    time.sleep(settle_seconds)
    k32.SetConsoleCtrlHandler(handler_routine, False)
    k32.FreeConsole()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("target_pid", type=int)
    parser.add_argument("--settle", type=float, default=2.0)
    args = parser.parse_args(argv)
    if sys.platform != "win32":
        print("win_ctrl_c only supports Windows", file=sys.stderr)
        return 2
    try:
        send_ctrl_c_to_console_group(args.target_pid, settle_seconds=args.settle)
    except OSError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
