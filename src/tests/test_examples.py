"""Every example must run: they are documentation that cannot rot."""

import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
EXAMPLES = sorted((ROOT / "examples").glob("*.py"))


@pytest.mark.parametrize("script", EXAMPLES, ids=lambda p: p.name)
def test_example_runs(script):
    # importing hpredis in this process sets PYTHONPATH at the C level, which
    # a child Python must not inherit (see conftest.py)
    env = {k: v for k, v in os.environ.items()
           if k not in ("PYTHONPATH", "PYTHONEXECUTABLE", "PYTHONHOME")}
    r = subprocess.run([sys.executable, str(script)], capture_output=True,
                       text=True, env=env, cwd=ROOT)
    assert r.returncode == 0, r.stderr[-2000:]
