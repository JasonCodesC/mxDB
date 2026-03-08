#!/usr/bin/env python3
from __future__ import annotations

import os
import shutil
import stat
import subprocess
import sys
from pathlib import Path


def run(cmd: list[str], cwd: Path | None = None) -> None:
    subprocess.run(cmd, check=True, cwd=str(cwd) if cwd else None)


def binary_name() -> str:
    return "featurectl.exe" if sys.platform.startswith("win") else "featurectl"


def candidate_binary_paths(build_dir: Path) -> list[Path]:
    name = binary_name()
    candidates = [
        build_dir / name,
        build_dir / "Release" / name,
        build_dir / "RelWithDebInfo" / name,
        build_dir / "MinSizeRel" / name,
    ]
    return candidates


def find_built_binary(build_dir: Path) -> Path:
    for candidate in candidate_binary_paths(build_dir):
        if candidate.exists() and candidate.is_file():
            return candidate
    raise FileNotFoundError(f"featurectl binary not found under {build_dir}")


def main() -> int:
    script_path = Path(__file__).resolve()
    package_root = script_path.parents[1]  # sdk/python
    repo_root = script_path.parents[3]  # repository root

    build_dir = Path(
        os.environ.get("MXDB_FEATURECTL_BUILD_DIR", str(repo_root / "build-wheel"))
    )

    run(["cmake", "-S", str(repo_root), "-B", str(build_dir)])

    build_cmd = ["cmake", "--build", str(build_dir), "--target", "featurectl"]
    if sys.platform.startswith("win"):
        build_cmd.extend(["--config", "Release"])
    run(build_cmd)

    built_binary = find_built_binary(build_dir)

    dest_dir = package_root / "src" / "mxdb" / "bin"
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_binary = dest_dir / binary_name()

    shutil.copy2(built_binary, dest_binary)
    if not sys.platform.startswith("win"):
        mode = dest_binary.stat().st_mode
        dest_binary.chmod(mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

    print(f"Bundled featurectl binary: {dest_binary}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
