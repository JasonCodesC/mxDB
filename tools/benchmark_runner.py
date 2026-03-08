#!/usr/bin/env python3
from __future__ import annotations

import argparse
import random
import time

from mxdb import MXDBClient


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a local mxDB micro-benchmark")
    parser.add_argument("--config", required=True)
    parser.add_argument("--featurectl-bin", default="featurectl")
    parser.add_argument("--entities", type=int, default=1000)
    parser.add_argument("--writes", type=int, default=10000)
    parser.add_argument("--reads", type=int, default=10000)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    client = MXDBClient(args.config, args.featurectl_bin)

    client.register_feature("prod", "instrument", "f_price", "price", "double")

    entity_ids = [f"E{i:06d}" for i in range(args.entities)]
    base_time = int(time.time() * 1_000_000)

    t0 = time.perf_counter()
    for i in range(args.writes):
        entity_id = entity_ids[i % args.entities]
        client.ingest_double(
            tenant="prod",
            entity_type="instrument",
            entity_id=entity_id,
            feature_id="f_price",
            event_time_us=base_time + i,
            system_time_us=base_time + i,
            value=float(i),
            write_id=f"bench-write-{i}",
        )
    write_s = time.perf_counter() - t0

    t1 = time.perf_counter()
    for _ in range(args.reads):
        entity_id = random.choice(entity_ids)
        client.latest_double("prod", "instrument", entity_id, "f_price")
    read_s = time.perf_counter() - t1

    print(f"writes={args.writes} duration_s={write_s:.3f} qps={args.writes / write_s:.1f}")
    print(f"reads={args.reads} duration_s={read_s:.3f} qps={args.reads / read_s:.1f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
