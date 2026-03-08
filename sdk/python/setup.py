from __future__ import annotations

import os
import shutil
import stat
import subprocess
import sys
from pathlib import Path

from setuptools import setup
from setuptools.command.build_py import build_py as _build_py

try:
    from wheel.bdist_wheel import bdist_wheel as _bdist_wheel
except ImportError:  # pragma: no cover - wheel is expected in build environments
    _bdist_wheel = None


class build_py(_build_py):
    def run(self) -> None:
        self._ensure_featurectl_bundled()
        super().run()

    def _ensure_featurectl_bundled(self) -> None:
        package_root = Path(__file__).resolve().parent
        bin_dir = package_root / "src" / "mxdb" / "bin"
        bin_dir.mkdir(parents=True, exist_ok=True)

        exe_name = "featurectl.exe" if sys.platform.startswith("win") else "featurectl"
        dest_binary = bin_dir / exe_name

        explicit_binary = os.environ.get("MXDB_FEATURECTL_BIN")
        if explicit_binary:
            source_binary = Path(explicit_binary)
            if not source_binary.exists():
                raise FileNotFoundError(
                    f"MXDB_FEATURECTL_BIN does not exist: {source_binary}"
                )
            shutil.copy2(source_binary, dest_binary)
            self._ensure_executable(dest_binary)
            return

        if dest_binary.exists():
            self._ensure_executable(dest_binary)
            return

        repo_root = package_root.parents[1]
        build_dir = Path(
            os.environ.get("MXDB_FEATURECTL_BUILD_DIR", str(repo_root / "build-wheel"))
        )

        self._run(["cmake", "-S", str(repo_root), "-B", str(build_dir)])

        build_cmd = ["cmake", "--build", str(build_dir), "--target", "featurectl"]
        if sys.platform.startswith("win"):
            build_cmd.extend(["--config", "Release"])
        self._run(build_cmd)

        built_binary = self._find_built_binary(build_dir, exe_name)
        shutil.copy2(built_binary, dest_binary)
        self._ensure_executable(dest_binary)

    @staticmethod
    def _run(cmd: list[str]) -> None:
        subprocess.run(cmd, check=True)

    @staticmethod
    def _find_built_binary(build_dir: Path, exe_name: str) -> Path:
        candidates = [
            build_dir / exe_name,
            build_dir / "Release" / exe_name,
            build_dir / "RelWithDebInfo" / exe_name,
            build_dir / "MinSizeRel" / exe_name,
        ]
        for candidate in candidates:
            if candidate.exists() and candidate.is_file():
                return candidate
        raise FileNotFoundError(f"featurectl binary not found in {build_dir}")

    @staticmethod
    def _ensure_executable(path: Path) -> None:
        if sys.platform.startswith("win"):
            return
        mode = path.stat().st_mode
        path.chmod(mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


if _bdist_wheel is not None:

    class bdist_wheel(_bdist_wheel):
        def finalize_options(self) -> None:
            super().finalize_options()
            self.root_is_pure = False

    cmdclass = {"build_py": build_py, "bdist_wheel": bdist_wheel}
else:
    cmdclass = {"build_py": build_py}


setup(cmdclass=cmdclass)
