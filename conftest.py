"""Shared pytest fixtures for all phases.

NOTE: importing a PythonMojo-bound extension module `setenv`s
`PYTHONPATH=':'` and `PYTHONEXECUTABLE=/usr/bin/python3` at the C level
(invisible to os.environ, inherited by children). Any subprocess spawned
after import — including the `mojo` build — must run with these scrubbed,
or the child Python loses its venv site-packages.
"""

import os
import subprocess
import sys
from pathlib import Path


def mojo_env() -> dict:
    env = os.environ.copy()
    for var in ("PYTHONPATH", "PYTHONEXECUTABLE", "PYTHONHOME"):
        env.pop(var, None)
    return env


def build_module(phase_dir: Path, src_name: str, so_name: str) -> Path:
    """Compile a Mojo phase source into a shared-lib extension module."""
    output_dir = phase_dir / "output"
    src = output_dir / src_name
    if not src.exists():
        raise FileNotFoundError(f"missing {src} (implementation)")
    so = output_dir / so_name
    subprocess.run(
        [
            str(Path(sys.executable).with_name("mojo")),
            "build",
            str(src),
            "--emit",
            "shared-lib",
            "-o",
            str(so),
        ],
        check=True,
        capture_output=True,
        text=True,
        env=mojo_env(),
    )
    return so


def ensure_release_bundle() -> None:
    """Build src/hpredis (release bundle) if missing: src/tests/ imports it."""
    root = Path(__file__).parent
    so = root / "src" / "hpredis" / "hpredis_core.so"
    if so.exists():
        return
    subprocess.run(
        [str(root / "dev/phase/05/output/build_release.sh")],
        check=True,
        capture_output=True,
        env=mojo_env(),
    )


def pytest_configure(config):
    """src/tests/ (adapted hiredis suite) imports the release bundle.
    Runs before collection, so the import resolves."""
    ensure_release_bundle()
