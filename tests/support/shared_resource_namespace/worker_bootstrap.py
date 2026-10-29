"""Publish the actual interpreter identity before importing worker dependencies."""

import json
import os
import runpy
import sys
from pathlib import Path

state = Path(os.environ["SJ_STATE_DIR"])
state.mkdir(parents=True, exist_ok=True)
identity = state / ("worker_started_" + os.environ["SJ_WORKER_LABEL"] + ".json")
identity.write_text(json.dumps({"pid": os.getpid()}), encoding="utf-8")
# The direct base interpreter avoids the Windows venv redirector. Preserve the
# parent's dependency paths without spawning a launcher or importing dependencies
# before publishing identity.
if "SJ_PARENT_SYS_PATH" in os.environ:
    sys.path[:] = [str(Path(__file__).resolve().parent), *json.loads(os.environ["SJ_PARENT_SYS_PATH"])]
runpy.run_path(sys.argv[1], run_name="__main__")
