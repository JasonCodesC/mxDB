#!/usr/bin/env python3
from __future__ import annotations

import sys
import zipfile
from pathlib import Path


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: check_wheel_contents.py <wheel_path>")
        return 2

    wheel_path = Path(sys.argv[1])
    with zipfile.ZipFile(wheel_path, "r") as zf:
        names = set(zf.namelist())

    def has_suffix(suffix: str) -> bool:
        return any(name.endswith(suffix) for name in names)

    required_suffixes = [
        "mxdb/client.py",
        "mxdb/_binary.py",
    ]
    missing = [suffix for suffix in required_suffixes if not has_suffix(suffix)]
    if missing:
        print("missing entries:")
        for suffix in missing:
            print(suffix)
        return 1

    # Linux/macOS wheels should contain a native featurectl payload.
    if not (has_suffix("mxdb/bin/featurectl") or has_suffix("mxdb/bin/featurectl.gz")):
        print("missing entries:")
        print("mxdb/bin/featurectl (or legacy mxdb/bin/featurectl.gz)")
        return 1

    print("wheel contains bundled featurectl and SDK files")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
