"""Bundle the prebuilt Mojo extension and mark the wheel platform-specific."""

from setuptools import Extension, setup
from setuptools.command.build_ext import build_ext


class SkipBuildExt(build_ext):
    def run(self):
        pass  # nothing to build or copy: the .so is prebuilt by Mojo

    def build_extension(self, ext):
        pass


setup(
    ext_modules=[Extension("hpredis.hpredis_core", sources=[])],
    cmdclass={"build_ext": SkipBuildExt},
)
