#!/usr/bin/env python3
"""Compare two performance baseline JSON reports on the same machine/config."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.performance_support.reporting import compare_reports  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("left", type=Path, help="Earlier or reference baseline JSON")
    parser.add_argument("right", type=Path, help="Later baseline JSON to compare against")
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON")
    args = parser.parse_args(argv)

    left = json.loads(args.left.read_text(encoding="utf-8"))
    right = json.loads(args.right.read_text(encoding="utf-8"))
    result = compare_reports(left, right)
    if args.json:
        print(json.dumps(result, indent=2))
    else:
        if not result["compatible_environment"]:
            print("Environment mismatch; refusing ratio comparison.", file=sys.stderr)
            print(json.dumps(result, indent=2))
            return 2
        for item in result["ratios"]:
            ratio = item.get("throughput_ratio_left_over_right")
            label = f"{item['workload']}@{item['concurrency']}"
            print(
                f"{label}: left_avg={item['left_avg_throughput']:.2f}/s "
                f"right_avg={item['right_avg_throughput']:.2f}/s ratio={ratio}",
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
