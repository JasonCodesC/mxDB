<p align="center">
  <img src="docs/assets/mxdb-logo.svg" alt="mxDB logo" width="760" />
</p>

<p align="center">
  <strong>A C++ bitemporal feature engine for ML systems that need correctness under late data and corrections.</strong>
</p>

<p align="center">
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-0f172a" alt="License" /></a>
  <a href="docs/implementation-backlog.md"><img src="https://img.shields.io/badge/status-alpha-0ea5e9" alt="Status" /></a>
  <a href=".github/workflows/python-wheels.yml"><img src="https://img.shields.io/badge/python%20wheels-cibuildwheel-22c55e" alt="Wheels" /></a>
</p>

## What Is mxDB?

**mxDB** is a specialized feature database for machine learning workloads that require:

- low-latency latest feature serving
- historical **as-of** retrieval with both `event_time` and `system_time`
- durable ingest with WAL + deterministic replay
- operational controls (checkpoint, compaction, backup/restore)

This project follows the architecture and milestone rules in [AGENT.md](AGENT.md), with detailed design in [docs/](docs/README.md).

## Why It Exists

Most feature systems optimize either online serving or offline history.
mxDB focuses on both, with **bitemporal correctness** as a first-class requirement.

If data can arrive late, be revised, or be corrected, you need to answer:

- What was true in the world at time `T`? (`event_time`)
- What did the system know at time `T`? (`system_time`)

mxDB stores both timelines and resolves queries with explicit cutoffs.

## Current Capabilities

Implemented today:

- C++20 engine with CMake build
- SQLite metadata plane (feature registration + lookup)
- WAL with checksums, sync and group-commit durability modes
- partitioned in-memory apply path with idempotency by `write_id`
- immutable segment flush + manifest tracking
- as-of lookups over mutable + immutable state
- baseline PIT dataset builder (correctness-first)
- checkpoints, conservative compaction, backup/restore
- admin/ops controls via `featurectl`
- Python SDK (`mxdb`) that calls `featurectl`
- wheel packaging path that bundles `featurectl`

See [docs/known-limitations.md](docs/known-limitations.md) for what is not done yet.

## Architecture At A Glance

```text
Producers -> Validation -> WAL -> Partition Apply -> Memtable + Latest Cache
                                      |                |
                                      |                +-> Immutable Segments -> As-Of/PIT
                                      +-> Recovery Replay
```

Main implementation modules:

- engine core: `engine/`
- process binaries: `server/process/`, `tools/featurectl/`
- contracts: `proto/`
- Python SDK: `sdk/python/`

## Install

### Option A: Python Users (recommended)

When published, install from PyPI:

```bash
pip install mxdb
```

The wheel bundles `featurectl` for target platforms, and `MXDBClient` auto-resolves it.

### Option B: Build From Source

```bash
cmake -S . -B build
cmake --build build -j8
ctest --test-dir build --output-on-failure
```

## Quickstart (CLI)

1. Create config:

```bash
cp deploy/config/featured.conf.example featured.conf
```

2. Register a feature:

```bash
build/featurectl featured.conf register-feature prod instrument f_price price double
```

3. Ingest a value:

```bash
build/featurectl featured.conf ingest prod instrument AAPL f_price 100 100 101.5 w1
```

4. Read latest and as-of values:

```bash
build/featurectl featured.conf latest prod instrument AAPL f_price
build/featurectl featured.conf asof prod instrument AAPL f_price 100 100
```

5. Check health:

```bash
build/featurectl featured.conf health
```

## Quickstart (Python)

```python
from mxdb import MXDBClient

client = MXDBClient("featured.conf")

client.register_feature("prod", "instrument", "f_price", "price", "double")

client.ingest_double(
    tenant="prod",
    entity_type="instrument",
    entity_id="AAPL",
    feature_id="f_price",
    event_time_us=100,
    system_time_us=100,
    value=101.5,
    write_id="w1",
)

latest = client.latest_double("prod", "instrument", "AAPL", "f_price")
asof = client.asof_double("prod", "instrument", "AAPL", "f_price", 100, 100)

print(latest)
print(asof)
```

### Binary Resolution Rules in Python SDK

`MXDBClient` resolves `featurectl` in this order:

1. explicit `featurectl_bin=` argument
2. `MXDB_FEATURECTL_BIN` environment variable
3. bundled wheel binary
4. `featurectl` on `PATH`

## Operational Guides

- Runbook: [docs/operational-runbook.md](docs/operational-runbook.md)
- Packaging + wheel bundling: [docs/python-packaging.md](docs/python-packaging.md)

Common commands:

```bash
build/featurectl featured.conf checkpoint
build/featurectl featured.conf compact
build/featurectl featured.conf backup /tmp/mxdb-backup
build/featurectl featured.conf readonly on
```

## Configuration

Example config: [deploy/config/featured.conf.example](deploy/config/featured.conf.example)

Key knobs:

- `partition_count`: internal partition fanout
- `default_durability_sync`: sync vs group-commit default
- `memtable_flush_event_threshold`: flush trigger
- `wal_segment_target_bytes`: WAL rotation size

## Testing

Run all tests:

```bash
ctest --test-dir build --output-on-failure
```

Coverage includes:

- metadata validation
- WAL append/replay/truncation handling
- temporal selection semantics
- end-to-end write/read/recovery
- checkpoint recovery
- compaction equivalence
- admin ops + Python SDK flows

## Benchmarks

Baseline workload definition:

- [bench/workloads/latest-read-workload.md](bench/workloads/latest-read-workload.md)

Runner:

```bash
PYTHONPATH=sdk/python/src python3 tools/benchmark_runner.py \
  --config featured.conf \
  --featurectl-bin build/featurectl
```

## Documentation Index

- architecture index: [docs/README.md](docs/README.md)
- execution charter: [AGENT.md](AGENT.md)
- system spec: [docs/bitemporal-feature-engine-spec.md](docs/bitemporal-feature-engine-spec.md)
- API contracts: [docs/bitemporal-feature-engine-api-contracts.md](docs/bitemporal-feature-engine-api-contracts.md)
- implementation backlog: [docs/implementation-backlog.md](docs/implementation-backlog.md)
- definition of done: [docs/definition-of-done.md](docs/definition-of-done.md)

## Roadmap

Next major gaps to close:

1. gRPC server surfaces for ingest/serving/historical/admin APIs
2. Arrow-native historical materialization
3. stronger retention/tombstone policy enforcement
4. higher-performance PIT execution and richer benchmark suites

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

MIT - see [LICENSE](LICENSE).
