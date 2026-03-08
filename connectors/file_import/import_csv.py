#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv

from mxdb import MXDBClient


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import CSV rows into mxDB")
    parser.add_argument("--config", required=True)
    parser.add_argument("--featurectl-bin", default="featurectl")
    parser.add_argument("--tenant", required=True)
    parser.add_argument("--entity-type", required=True)
    parser.add_argument("--feature-id", required=True)
    parser.add_argument("--csv", required=True)
    parser.add_argument("--entity-column", default="entity_id")
    parser.add_argument("--event-time-column", default="event_time_us")
    parser.add_argument("--value-column", default="value")
    parser.add_argument("--write-id-column", default="write_id")
    parser.add_argument("--system-time-column", default="system_time_us")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    client = MXDBClient(args.config, args.featurectl_bin)

    imported = 0
    with open(args.csv, "r", encoding="utf-8", newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            system_time = row.get(args.system_time_column)
            client.ingest_double(
                tenant=args.tenant,
                entity_type=args.entity_type,
                entity_id=row[args.entity_column],
                feature_id=args.feature_id,
                event_time_us=int(row[args.event_time_column]),
                value=float(row[args.value_column]),
                write_id=row[args.write_id_column],
                system_time_us=int(system_time) if system_time else None,
            )
            imported += 1

    print(f"imported_rows={imported}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
