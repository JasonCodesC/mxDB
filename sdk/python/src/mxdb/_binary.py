from __future__ import annotations

import os
import shutil
import stat
import sys
from importlib import resources
from pathlib import Path
from typing import Optional


def _platform_binary_names() -> list[str]:
    if sys.platform.startswith("win"):
        return ["featurectl.exe"]
    return ["featurectl"]


def _packaged_binary_candidates() -> list[Path]:
    try:
        base = resources.files("mxdb").joinpath("bin")
    except (ModuleNotFoundError, FileNotFoundError):
        return []

    paths: list[Path] = []
    for name in _platform_binary_names():
        candidate = Path(base.joinpath(name))
        paths.append(candidate)
    return paths


def _ensure_executable(path: Path) -> None:
    if sys.platform.startswith("win"):
        return
    mode = path.stat().st_mode
    path.chmod(mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


def resolve_featurectl_binary(explicit: Optional[str] = None) -> str:
    if explicit:
        return explicit

    env_override = os.environ.get("MXDB_FEATURECTL_BIN")
    if env_override:
        return env_override

    for candidate in _packaged_binary_candidates():
        if candidate.exists():
            _ensure_executable(candidate)
            return str(candidate)

    path_binary = shutil.which("featurectl")
    if path_binary:
        return path_binary

    raise RuntimeError(
        "featurectl binary was not found. Install a wheel with bundled binaries, "
        "or set MXDB_FEATURECTL_BIN to a valid featurectl path."
    )
