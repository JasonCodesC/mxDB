from __future__ import annotations

import os
import shutil
import stat
import sys
import gzip
import tempfile
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


def _packaged_compressed_candidates() -> list[Path]:
    try:
        base = resources.files("mxdb").joinpath("bin")
    except (ModuleNotFoundError, FileNotFoundError):
        return []

    paths: list[Path] = []
    for name in _platform_binary_names():
        candidate = Path(base.joinpath(f"{name}.gz"))
        paths.append(candidate)
    return paths


def _ensure_executable(path: Path) -> None:
    if sys.platform.startswith("win"):
        return
    mode = path.stat().st_mode
    path.chmod(mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


def _runtime_cache_dir() -> Path:
    candidates: list[Path] = []

    explicit = os.environ.get("MXDB_CACHE_DIR")
    if explicit:
        candidates.append(Path(explicit))

    if sys.platform.startswith("win"):
        candidates.append(
            Path(os.environ.get("LOCALAPPDATA", str(Path.home() / "AppData" / "Local")))
        )
    elif sys.platform == "darwin":
        candidates.append(Path.home() / "Library" / "Caches")
    else:
        candidates.append(
            Path(os.environ.get("XDG_CACHE_HOME", str(Path.home() / ".cache")))
        )

    candidates.append(Path(tempfile.gettempdir()))

    for base in candidates:
        path = base / "mxdb" / "bin"
        try:
            path.mkdir(parents=True, exist_ok=True)
            return path
        except OSError:
            continue

    raise RuntimeError("failed to create runtime cache directory for mxdb binary")


def _extract_compressed_binary(compressed_path: Path) -> str:
    name = compressed_path.name[:-3]  # strip .gz
    target = _runtime_cache_dir() / name
    with gzip.open(compressed_path, "rb") as source, target.open("wb") as out:
        shutil.copyfileobj(source, out)
    _ensure_executable(target)
    return str(target)


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

    for candidate in _packaged_compressed_candidates():
        if candidate.exists():
            return _extract_compressed_binary(candidate)

    path_binary = shutil.which("featurectl")
    if path_binary:
        return path_binary

    raise RuntimeError(
        "featurectl binary was not found. Install a wheel with bundled binaries, "
        "or set MXDB_FEATURECTL_BIN to a valid featurectl path."
    )
