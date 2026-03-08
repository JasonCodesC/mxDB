# Implementation Backlog

This backlog turns the milestone plan into concrete execution work. It is ordered by dependency, not by convenience.

Completed items are checked off. Remaining items are active scope.

## Current State

Completed architecture work:

- [x] Full system spec
- [x] ADR set
- [x] API contract document
- [x] Protobuf contracts
- [x] HTML rendering of the spec
- [x] Architecture index
- [x] Agent execution charter
- [x] Definition of done
- [x] Development guide

Implementation status:

- [x] Build system and source tree
- [x] Metadata plane
- [x] WAL and commit pipeline
- [x] Recovery path
- [x] Online serving path
- [x] Historical as-of path
- [x] PIT join engine (correctness-first baseline)
- [x] Compaction and checkpointing (baseline)
- [x] Admin and ops plane (baseline)
- [x] Python SDK (embedded CLI-backed baseline)

## Milestone 1: Build Skeleton and Metadata Plane

- [x] Create the source-tree layout described in the spec
- [x] Add root `CMakeLists.txt`
- [x] Add toolchain and dependency management strategy
- [x] Create core library targets for common types, status, logging, and config
- [x] Create SQLite-backed metadata persistence layer
- [x] Create metadata migrations system
- [x] Implement feature definition CRUD
- [ ] Implement feature group CRUD
- [x] Implement metadata validation library
- [x] Add tests for metadata persistence and validation
- [x] Add initial server binary skeleton with config loading

## Milestone 2: WAL and Commit Pipeline

- [x] Define concrete WAL binary format and invariants
- [x] Implement `LSN` allocator
- [x] Implement WAL segment writer
- [x] Implement checksums and corruption detection
- [x] Implement sync and group-commit modes
- [ ] Implement ingestion RPC server skeleton
- [x] Implement per-entity batch validation and atomic write behavior
- [x] Implement idempotency handling keyed by `write_id`
- [x] Add WAL-focused crash and replay tests
- [ ] Add basic write-path metrics

## Milestone 3: Partitioned In-Memory State and Recovery

- [x] Implement local partition router
- [x] Implement mutable memtable
- [x] Implement immutable memtable freeze and flush handoff
- [ ] Implement partition apply queues
- [x] Implement recovery manager
- [x] Replay WAL into partitioned in-memory state
- [x] Rebuild latest visible state after restart
- [ ] Add fault-injection harness for crash testing
- [x] Add recovery correctness and determinism tests

## Milestone 4: Latest Cache and Serving Path

- [x] Implement latest-cache shard structure
- [ ] Implement immutable feature-vector snapshots
- [ ] Implement lock-free publication strategy
- [x] Support serving by explicit feature list
- [ ] Support serving by feature group
- [ ] Implement freshness checks
- [x] Implement consistency modes for latest reads
- [x] Implement `GetLatestFeatures`
- [x] Add latency benchmarks for online reads
- [ ] Add concurrency tests for publication and reads

## Milestone 5: Immutable Segments, Manifest, and As-Of Reads

- [x] Define segment file format
- [x] Implement segment writer
- [x] Implement segment reader
- [ ] Implement block index and metadata summaries
- [ ] Implement bloom or xor existence filters
- [x] Implement manifest log and rewrite handling
- [x] Implement memtable flush to immutable segment
- [x] Implement `AsOfLookup`
- [x] Implement segment pruning by key and time bounds
- [ ] Add differential tests against a reference as-of implementation

## Milestone 6: Historical Jobs and PIT Joins

- [x] Implement historical query coordinator
- [x] Implement timeline scan operator
- [x] Implement PIT join operator
- [ ] Implement Arrow materialization
- [ ] Implement asynchronous dataset job state machine
- [x] Implement `BuildTrainingDataset`
- [ ] Implement dataset job status and cancellation
- [ ] Add leakage-prevention tests
- [ ] Add PIT throughput benchmarks on realistic workloads

## Milestone 7: Checkpoints, Compaction, and Retention

- [x] Implement flush-based checkpoint workflow
- [x] Implement checkpoint metadata and restart integration
- [x] Implement compaction planner
- [x] Implement compaction executor
- [ ] Implement retention policy evaluation
- [ ] Implement tombstone lifecycle handling with retention windows
- [x] Add pre/post-compaction equivalence tests
- [ ] Add restart-time benchmarks with checkpoints

## Milestone 8: Admin, Metrics, and Backup/Restore

- [x] Implement `AdminService`
- [x] Implement health-state reporting
- [x] Implement compaction status reporting
- [x] Implement checkpoint trigger and status reporting
- [x] Implement backup workflow
- [x] Implement restore workflow
- [ ] Add structured logs for core lifecycle events
- [ ] Add metrics coverage for writes, reads, recovery, compaction, and jobs
- [ ] Add backup/restore integration tests

## Milestone 9: SDKs and Connectors

- [x] Implement Python SDK package structure
- [x] Add metadata client
- [x] Add ingest client
- [x] Add online serving client
- [x] Add historical as-of client
- [ ] Add Arrow/Pandas/Polars conversion helpers
- [ ] Add Kafka ingestion connector
- [x] Add one ingestion connector path (`connectors/file_import/import_csv.py`)
- [x] Add usage examples

## Milestone 10: Hardening and Release

- [x] Publish benchmark harness and reproducible workloads (baseline)
- [x] Add operational runbook
- [x] Add packaging and deployment assets (baseline)
- [x] Write top-level user-facing README examples
- [x] Add known-limitations document
- [ ] Verify milestone acceptance criteria against the definition of done
- [ ] Prepare first alpha release

## Ongoing Work Rules

- [ ] Keep docs synchronized with implementation
- [ ] Add tests alongside each subsystem
- [ ] Record new architecture decisions as ADRs when needed
- [ ] Keep API contracts and generated code aligned
- [ ] Do not skip earlier dependency work to chase later features

## Immediate Next Tasks

1. implement gRPC service layer over the existing engine APIs
2. add Arrow-native output path for historical workflows
3. add retention policy engine and tombstone lifecycle enforcement
4. add concurrency-focused serving and write-apply tests
