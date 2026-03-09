#!/usr/bin/env python3
from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

NO_ELF_MARKERS = (
    "no ELF executable or shared library file",
    "This does not look like a platform wheel",
)
PLATFORM_TAG_MAP = {
    "linux_x86_64": "manylinux_2_17_x86_64.manylinux2014_x86_64",
    "linux_aarch64": "manylinux_2_17_aarch64.manylinux2014_aarch64",
}


def run_capture(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        cmd,
        check=False,
        capture_output=True,
        text=True,
    )


def print_result(result: subprocess.CompletedProcess[str]) -> None:
    if result.stdout:
        print(result.stdout, end="")
    if result.stderr:
        print(result.stderr, end="", file=sys.stderr)


def auditwheel_repair(wheel: Path, dest_dir: Path) -> subprocess.CompletedProcess[str]:
    return run_capture(
        [
            "auditwheel",
            "repair",
            "-w",
            str(dest_dir),
            str(wheel),
        ]
    )


def fallback_platform_tag(wheel: Path) -> str | None:
    for original_tag, repaired_tag in PLATFORM_TAG_MAP.items():
        if original_tag in wheel.name:
            return repaired_tag
    return None


def fallback_retag(wheel: Path, dest_dir: Path) -> int:
    platform_tag = fallback_platform_tag(wheel)
    if not platform_tag:
        print(
            f"unable to derive manylinux fallback tag from wheel name: {wheel.name}",
            file=sys.stderr,
        )
        return 1

    dest_dir.mkdir(parents=True, exist_ok=True)
    staged_wheel = dest_dir / wheel.name
    shutil.copy2(wheel, staged_wheel)

    result = run_capture(
        [
            sys.executable,
            "-m",
            "wheel",
            "tags",
            "--remove",
            "--platform-tag",
            platform_tag,
            str(staged_wheel),
        ]
    )
    print_result(result)
    if result.returncode != 0:
        return result.returncode

    produced = [
        line.strip()
        for line in result.stdout.splitlines()
        if line.strip().endswith(".whl")
    ]
    if not produced:
        print(
            "wheel retag completed but did not report output wheel path",
            file=sys.stderr,
        )
        return 1

    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Repair Linux wheels for cibuildwheel. "
            "Uses auditwheel when possible and falls back to manylinux retagging "
            "for wheels that only bundle a standalone executable."
        )
    )
    parser.add_argument("wheel", type=Path)
    parser.add_argument("dest_dir", type=Path)
    args = parser.parse_args()

    result = auditwheel_repair(args.wheel, args.dest_dir)
    if result.returncode == 0:
        print_result(result)
        return 0

    output = f"{result.stdout}\n{result.stderr}"
    if any(marker in output for marker in NO_ELF_MARKERS):
        print(
            "auditwheel reported no ELF payload for repair; "
            "falling back to manylinux wheel retagging",
            file=sys.stderr,
        )
        return fallback_retag(args.wheel, args.dest_dir)

    print_result(result)
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
