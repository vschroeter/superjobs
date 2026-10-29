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
runpy.run_path(sys.argv[1], run_name="__main__")
