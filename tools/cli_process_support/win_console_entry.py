"""Start an installed console command with normal Windows CTRL+C inheritance."""

from __future__ import annotations

import ctypes
import signal
import subprocess
import sys


def main() -> int:
    # dev_check's new process group ignores CTRL+C. A new console alone does
    # not reset that inherited flag. Reset it before creating the real entry
    # point, then keep this supervisor alive while the CLI handles the event.
    if not ctypes.windll.kernel32.SetConsoleCtrlHandler(None, False):
        raise OSError("could not restore CTRL+C inheritance")
    signal.signal(signal.SIGINT, lambda *_args: None)
    with subprocess.Popen(sys.argv[1:]) as process:
        return process.wait()


if __name__ == "__main__":
    raise SystemExit(main())
