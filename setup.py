"""Bundle the prebuilt Mojo extension and mark the wheel platform-specific.

A prebuilt wheel ships hpredis_core.so, so installing it needs no toolchain.
Installing from a source checkout builds the extension with Mojo (via
build.sh) when the .so is absent.
"""

import shutil
import subprocess
from pathlib import Path

from setuptools import Extension, setup
from setuptools.command.build_ext import build_ext

ROOT = Path(__file__).parent
SO = ROOT / "src" / "hpredis" / "hpredis_core.so"


class MojoBuildExt(build_ext):
    """Build hpredis_core.so with Mojo, then stage it for the wheel.

    build_py copies package data before build_ext runs, so the compiled
    artifact is copied into build_lib here or it would miss the wheel.
    """

    def run(self):
        if not SO.exists():
            subprocess.run(["bash", str(ROOT / "build.sh")], check=True)
        target = Path(self.build_lib) / "hpredis" / "hpredis_core.so"
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(SO, target)

    def build_extension(self, ext):
        pass  # built from source by Mojo above (or shipped prebuilt)


setup(
    ext_modules=[Extension("hpredis.hpredis_core", sources=[])],
    cmdclass={"build_ext": MojoBuildExt},
)
