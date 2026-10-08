"""Shared pytest fixtures.

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
    """Compile a Mojo phase source into a shared-lib extension module.

    Used by the optional dev/ phase tests; the release suite only needs
    ensure_release_bundle() below.
    """
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
    """Build src/hpredis (release bundle) if missing or older than its source.

    A stale .so silently misses new core methods (e.g. dispose()), so compare
    mtimes instead of only checking existence.
    """
    root = Path(__file__).parent
    so = root / "src" / "hpredis" / "hpredis_core.so"
    source = root / "src" / "hpredis_core.mojo"
    if so.exists() and so.stat().st_mtime >= source.stat().st_mtime:
        return
    subprocess.run(
        ["bash", str(root / "build.sh")],
        check=True,
        capture_output=True,
        env=mojo_env(),
    )


def pytest_configure(config):
    """src/tests/ imports the release bundle. Runs before collection, so the
    import resolves on a fresh clone without any prebuilt artifacts."""
    ensure_release_bundle()
