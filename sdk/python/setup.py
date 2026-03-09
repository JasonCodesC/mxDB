from __future__ import annotations

import os
import shutil
import stat
import subprocess
import sys
import gzip
from pathlib import Path

from setuptools import setup
from setuptools.command.build_py import build_py as _build_py
from setuptools import Distribution

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
        dest_gzip = bin_dir / f"{exe_name}.gz"

        explicit_binary = os.environ.get("MXDB_FEATURECTL_BIN")
        if explicit_binary:
            source_binary = Path(explicit_binary)
            if not source_binary.exists():
                raise FileNotFoundError(
                    f"MXDB_FEATURECTL_BIN does not exist: {source_binary}"
                )
            shutil.copy2(source_binary, dest_binary)
            self._compress_binary(dest_binary, dest_gzip)
            return

        if dest_gzip.exists():
            return
        if dest_binary.exists():
            self._compress_binary(dest_binary, dest_gzip)
            return

        repo_root = self._discover_repo_root(package_root)
        build_dir = Path(
            os.environ.get("MXDB_FEATURECTL_BUILD_DIR", str(repo_root / "build-wheel"))
        )
        shutil.rmtree(build_dir, ignore_errors=True)

        configure_cmd = ["cmake", "-S", str(repo_root), "-B", str(build_dir)]
        configure_cmd.extend(self._configure_args_for_env())
        self._run(configure_cmd)

        build_cmd = ["cmake", "--build", str(build_dir), "--target", "featurectl"]
        if sys.platform.startswith("win"):
            build_cmd.extend(["--config", "Release"])
        self._run(build_cmd)

        built_binary = self._find_built_binary(build_dir, exe_name)
        shutil.copy2(built_binary, dest_binary)
        self._compress_binary(dest_binary, dest_gzip)

    @staticmethod
    def _run(cmd: list[str]) -> None:
        subprocess.run(cmd, check=True)

    @staticmethod
    def _configure_args_for_env() -> list[str]:
        args: list[str] = []

        toolchain = os.environ.get("MXDB_CMAKE_TOOLCHAIN_FILE")
        if toolchain:
            args.append(f"-DCMAKE_TOOLCHAIN_FILE={toolchain}")

        triplet = os.environ.get("MXDB_VCPKG_TARGET_TRIPLET")
        if triplet:
            args.append(f"-DVCPKG_TARGET_TRIPLET={triplet}")

        return args

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

    def _compress_binary(self, src_binary: Path, dest_gzip: Path) -> None:
        self._ensure_executable(src_binary)
        with src_binary.open("rb") as source, gzip.open(dest_gzip, "wb") as target:
            shutil.copyfileobj(source, target)
        src_binary.unlink(missing_ok=True)

    @staticmethod
    def _discover_repo_root(package_root: Path) -> Path:
        env_repo_root = os.environ.get("MXDB_REPO_ROOT")
        if env_repo_root:
            repo_root = Path(env_repo_root).resolve()
            if (repo_root / "CMakeLists.txt").exists() and (repo_root / "engine").exists():
                return repo_root

        for candidate in [package_root, *package_root.parents]:
            if (candidate / "CMakeLists.txt").exists() and (candidate / "engine").exists():
                return candidate

        raise FileNotFoundError(
            "Repository root with CMakeLists.txt + engine/ was not found while "
            "building mxdb wheel. Set MXDB_FEATURECTL_BIN or MXDB_REPO_ROOT."
        )


if _bdist_wheel is not None:

    class bdist_wheel(_bdist_wheel):
        def finalize_options(self) -> None:
            super().finalize_options()
            self.root_is_pure = False

    cmdclass = {"build_py": build_py, "bdist_wheel": bdist_wheel}
else:
    cmdclass = {"build_py": build_py}


class BinaryDistribution(Distribution):
    def has_ext_modules(self) -> bool:
        return True

    def is_pure(self) -> bool:
        return False


setup(cmdclass=cmdclass, distclass=BinaryDistribution)
