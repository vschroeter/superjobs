"""Scenario family planning for reliability repetition."""

from __future__ import annotations

import random
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from tools.verify_contract_typing import runtime_evidence_tag
from tools.verify_worker_recovery import SCENARIO_AFTER, SCENARIO_BEFORE

FAMILY_CROSS_PROGRAM: Literal["cross_program"] = "cross_program"
FAMILY_RECOVERY_BEFORE_COMPLETION: Literal["recovery_before_completion"] = SCENARIO_BEFORE
FAMILY_RECOVERY_AFTER_COMPLETION: Literal["recovery_after_completion"] = SCENARIO_AFTER
FAMILY_BROKER_RESTART: Literal["broker_restart"] = "broker_restart"

ReliabilityFamily = Literal[
    "cross_program",
    "recovery_before_completion",
    "recovery_after_completion",
    "broker_restart",
]

REPETITIONS_PER_FAMILY = 3

_ALL_FAMILIES: tuple[ReliabilityFamily, ...] = (
    FAMILY_CROSS_PROGRAM,
    FAMILY_RECOVERY_BEFORE_COMPLETION,
    FAMILY_RECOVERY_AFTER_COMPLETION,
    FAMILY_BROKER_RESTART,
)


@dataclass(frozen=True)
class ReliabilityRunItem:
    family: ReliabilityFamily
    repetition: int
    sequence_index: int

    @property
    def label(self) -> str:
        return f"{self.family}#{self.repetition}"


def planned_items() -> tuple[ReliabilityRunItem, ...]:
    items: list[ReliabilityRunItem] = []
    index = 0
    for family in _ALL_FAMILIES:
        for repetition in range(1, REPETITIONS_PER_FAMILY + 1):
            items.append(
                ReliabilityRunItem(
                    family=family,
                    repetition=repetition,
                    sequence_index=index,
                )
            )
            index += 1
    return tuple(items)


def shuffle_items(seed: int, items: tuple[ReliabilityRunItem, ...]) -> tuple[ReliabilityRunItem, ...]:
    ordered = list(items)
    random.Random(seed).shuffle(ordered)
    return tuple(
        ReliabilityRunItem(
            family=item.family,
            repetition=item.repetition,
            sequence_index=position,
        )
        for position, item in enumerate(ordered)
    )


def build_child_argv(
    *,
    repo_root: Path,
    python_version: str,
    family: ReliabilityFamily,
    artifact_dir: Path,
) -> list[str]:
    script_map = {
        FAMILY_CROSS_PROGRAM: "tools/verify_cross_program.py",
        FAMILY_RECOVERY_BEFORE_COMPLETION: "tools/verify_worker_recovery.py",
        FAMILY_RECOVERY_AFTER_COMPLETION: "tools/verify_worker_recovery.py",
        FAMILY_BROKER_RESTART: "tools/verify_broker_restart.py",
    }
    script = repo_root / script_map[family]
    argv = [
        sys.executable,
        str(script),
        "--python",
        python_version,
        "--artifact-dir",
        str(artifact_dir),
    ]
    if family == FAMILY_RECOVERY_BEFORE_COMPLETION:
        argv.extend(["--only-scenario", SCENARIO_BEFORE])
    elif family == FAMILY_RECOVERY_AFTER_COMPLETION:
        argv.extend(["--only-scenario", SCENARIO_AFTER])
    return argv
