"""Run a role script with -I isolation and only the copied role directory on sys.path."""

from __future__ import annotations

import runpy
import sys
from pathlib import Path


def main() -> None:
    if len(sys.argv) < 3:
        print(
            "usage: isolated_bootstrap.py <role_dir> <script_name> [script_args...]",
            file=sys.stderr,
        )
        raise SystemExit(2)
    role_dir = Path(sys.argv[1]).resolve()
    script_name = sys.argv[2]
    script = (role_dir / script_name).resolve()
    if not script.is_file():
        print(f"script not found: {script}", file=sys.stderr)
        raise SystemExit(2)
    sys.path.insert(0, str(role_dir))
    sys.argv = [str(script)] + sys.argv[3:]
    runpy.run_path(str(script), run_name="__main__")


if __name__ == "__main__":
    main()
