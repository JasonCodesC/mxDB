# mxDB: Bitemporal Feature Engine

mxDB is a C++20 single-node bitemporal feature engine for:

- low-latency latest feature serving
- bitemporal as-of retrieval (`event_time` + `system_time`)
- deterministic recovery from WAL
- point-in-time dataset construction workflows

## Current Status

Implemented in this repository:

- CMake-based C++ build
- SQLite metadata plane with migrations
- WAL write path with checksums, sync and group-commit durability
- partitioned in-memory state and idempotent write handling (`write_id`)
- recovery manager with checkpoint-bounded WAL replay
- immutable segment flush and manifest tracking
- latest and as-of query APIs in the core engine
- PIT dataset builder (correctness-first implementation)
- checkpointing, conservative compaction, admin service, backup/restore
- `featurectl` CLI for ops + data-plane workflows
- thin Python SDK over `featurectl`
- test suite (unit + integration + recovery + SDK)

Known limitations are documented in [docs/known-limitations.md](docs/known-limitations.md).

## Build and Test

```bash
cmake -S . -B build
cmake --build build -j8
ctest --test-dir build --output-on-failure
```

## Quickstart

1. Copy config:

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

4. Query latest / as-of:

```bash
build/featurectl featured.conf latest prod instrument AAPL f_price
build/featurectl featured.conf asof prod instrument AAPL f_price 100 100
```

## Python SDK

Install from PyPI (wheel with bundled `featurectl`):

```bash
pip install mxdb
```

Use local source checkout mode:

```bash
PYTHONPATH=sdk/python/src python3 -c "from mxdb import MXDBClient; print(MXDBClient)"
```

`MXDBClient` auto-resolves `featurectl` from bundled wheel binaries first.

Example benchmark runner:

```bash
PYTHONPATH=sdk/python/src python3 tools/benchmark_runner.py \
  --config featured.conf \
  --featurectl-bin build/featurectl
```

## Documentation Map

- Architecture index: [docs/README.md](docs/README.md)
- Execution charter: [AGENT.md](AGENT.md)
- Implementation plan: [docs/bitemporal-feature-engine-implementation-plan.md](docs/bitemporal-feature-engine-implementation-plan.md)
- Backlog: [docs/implementation-backlog.md](docs/implementation-backlog.md)
- Definition of done: [docs/definition-of-done.md](docs/definition-of-done.md)
- Operational runbook: [docs/operational-runbook.md](docs/operational-runbook.md)
