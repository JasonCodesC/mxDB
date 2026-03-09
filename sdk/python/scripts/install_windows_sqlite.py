#!/usr/bin/env python3
from __future__ import annotations

import os
import subprocess
from pathlib import Path


def find_vcpkg() -> Path:
    candidates: list[Path] = []

    env_root = os.environ.get("VCPKG_INSTALLATION_ROOT")
    if env_root:
        candidates.append(Path(env_root))

    candidates.extend([Path("C:/vcpkg"), Path("D:/vcpkg")])

    for root in candidates:
        exe = root / "vcpkg.exe"
        if exe.exists() and exe.is_file():
            return exe

    raise FileNotFoundError(
        "vcpkg.exe not found. Set VCPKG_INSTALLATION_ROOT or install vcpkg on runner."
    )


def main() -> int:
    vcpkg = find_vcpkg()
    cmd = [str(vcpkg), "install", "sqlite3:x64-windows"]
    print("Running:", " ".join(cmd))
    subprocess.run(cmd, check=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
