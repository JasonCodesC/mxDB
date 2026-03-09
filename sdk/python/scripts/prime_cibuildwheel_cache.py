#!/usr/bin/env python3
from __future__ import annotations

import pathlib
import re
import time
import urllib.request

import cibuildwheel.util as util


def main() -> int:
    cache_dir = pathlib.Path(util.CIBW_CACHE_PATH)
    cache_dir.mkdir(parents=True, exist_ok=True)

    source = pathlib.Path(util.__file__).read_text(encoding="utf-8")
    match = re.search(r"virtualenv-(\d+\.\d+\.\d+)\.pyz", source)
    version = match.group(1) if match else "20.26.6"

    target = cache_dir / f"virtualenv-{version}.pyz"
    if target.exists():
        print(f"virtualenv cache already present: {target}")
        return 0

    url = "https://bootstrap.pypa.io/virtualenv.pyz"
    last_error: Exception | None = None

    for attempt in range(1, 6):
        try:
            print(f"download attempt {attempt}: {url}")
            with urllib.request.urlopen(url, timeout=120) as response:
                target.write_bytes(response.read())
            print(f"cached virtualenv at {target}")
            return 0
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            print(f"attempt {attempt} failed: {exc}")
            time.sleep(10 * attempt)

    raise SystemExit(f"failed to prime virtualenv cache: {last_error}")


if __name__ == "__main__":
    raise SystemExit(main())
