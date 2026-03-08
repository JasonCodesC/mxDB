# Bitemporal Feature Engine: Implementation Plan

Status: Draft execution plan

This plan turns the architecture spec into a build order. It is organized by dependency and risk retirement, not by superficial component ownership.

The plan assumes a serious open-source build with one primary engineer and occasional contributors. If the team is larger, the same milestone order still holds; only the schedule compresses.

## 1. Delivery Strategy

The project should be built in this order:

1. lock semantics
2. build the durable write path
3. make recovery trustworthy
4. add fast latest-value serving
5. add immutable history and as-of reads
6. add PIT joins and dataset build workflows
7. harden operations, benchmarks, and developer ergonomics

The critical path is not UI, connectors, or clustering. The critical path is:

- temporal correctness
- WAL durability
- restart recovery
- historical retrieval correctness
- serving latency predictability

## 2. Planning Assumptions

Assumptions for this plan:

- v1 target is single-node
- C++ core with gRPC and protobuf
- Arrow for batch interchange
- Python SDK is thin and comes after the core server path is usable
- distributed clustering is not required for the first public release

## 3. Release Definitions

### Milestone Release: `MVP-alpha`

Must include:

- single-node service
- metadata registry
- WAL durability
- crash recovery
- latest reads
- as-of reads

This is the first release that proves the engine is real.

### Milestone Release: `beta`

Adds:

- PIT joins
- dataset build API
- compaction
- checkpoints
- benchmark suite
- Python SDK

### Milestone Release: `v1.0`

Adds:

- operational hardening
- backup/restore
- stronger observability
- packaging and deployment artifacts
- docs good enough for outside users

## 4. Workstreams

The work naturally breaks into these workstreams:

- architecture and contracts
- metadata plane
- storage engine
- query engine
- serving layer
- SDK and connectors
- testing and verification
- operations and release engineering

These should not be developed independently. Each milestone should cut vertically through at least storage, API, and tests.

## 5. Milestone Plan

## 5.1 Milestone 0: Spec Freeze and Project Skeleton

Duration:

- 1 week

Goals:

- freeze the v1 product boundary
- freeze core ADRs
- define protobuf contracts
- create repository layout and build system skeleton

Deliverables:

- architecture spec
- ADR set
- API contract doc
- initial source-tree layout
- CI skeleton

Acceptance criteria:

- no unresolved ambiguity around bitemporal semantics
- write/read semantics are documented clearly enough to test
- all future code directories map back to the spec

## 5.2 Milestone 1: Metadata Plane and Validation Layer

Duration:

- 2 weeks

Goals:

- implement metadata persistence in SQLite
- support feature registration and lookup
- implement schema/type validation at ingestion boundary

Deliverables:

- `MetadataService`
- feature definitions and feature groups
- schema validation library
- metadata migration system

Acceptance criteria:

- features can be created, listed, and fetched
- invalid writes are rejected before the WAL
- entity type and feature type mismatches are caught deterministically

Risks retired:

- registry ambiguity
- type drift between client and engine

## 5.3 Milestone 2: WAL and Commit Pipeline

Duration:

- 3 weeks

Goals:

- implement node-wide WAL
- implement `LSN` assignment
- implement synchronous and group-commit durability modes
- return commit tokens to clients

Deliverables:

- WAL segment writer
- checksummed WAL format
- group-commit scheduler
- ingestion API with per-entity batch semantics

Acceptance criteria:

- acknowledged writes survive process crash in sync mode
- duplicate writes with same `write_id` do not create duplicate visible state
- load tests confirm WAL ordering and batching behavior

Risks retired:

- durability uncertainty
- write atomicity ambiguity

## 5.4 Milestone 3: Memtables, Apply Queues, and Recovery

Duration:

- 3 weeks

Goals:

- implement partition-local memtables
- implement partition apply queues
- support recovery by replaying WAL into memtables

Deliverables:

- local partition router
- mutable and immutable memtable lifecycle
- recovery manager
- crash-injection tests

Acceptance criteria:

- restart from arbitrary crash points yields correct visible state
- single-entity batch visibility remains atomic after recovery
- recovery time is bounded and measurable

Risks retired:

- incorrect replay
- concurrency issues in write apply

## 5.5 Milestone 4: Latest Cache and Online Read Path

Duration:

- 2 weeks

Goals:

- implement lock-free latest-cache publication
- implement feature-group-aware serving
- expose `GetLatestFeatures`

Deliverables:

- latest-cache shard implementation
- freshness checking
- serving consistency modes
- online-serving benchmarks

Acceptance criteria:

- latest reads do not scan history in the common case
- p99 latency hits the project target on local benchmarks
- readers never observe partially updated vectors

Risks retired:

- weak serving latency story
- cache publication race conditions

## 5.6 Milestone 5: Immutable Segments and As-Of Lookup

Duration:

- 4 weeks

Goals:

- flush memtables into immutable segments
- implement manifest updates
- implement segment pruning and as-of retrieval

Deliverables:

- segment writer and reader
- block index and segment metadata
- manifest log and snapshot handling
- `AsOfLookup` RPC

Acceptance criteria:

- flushed data remains queryable after restart
- as-of results match the in-memory reference implementation
- pruning measurably reduces scan work on benchmark datasets

Risks retired:

- historical durability gaps
- query correctness across multiple sources of truth

## 5.7 Milestone 6: Point-in-Time Join Operator

Duration:

- 4 weeks

Goals:

- implement the PIT join operator
- implement Arrow batch materialization
- support asynchronous dataset build jobs

Deliverables:

- historical query coordinator
- dataset job state machine
- Arrow export path
- correctness suite for leakage prevention

Acceptance criteria:

- training datasets are reproducible with pinned snapshots
- PIT joins never include revisions beyond the requested system cutoff
- large jobs stream or write output without unbounded memory growth

Risks retired:

- core product differentiation still missing
- historical jobs unusable at realistic scale

## 5.8 Milestone 7: Compaction, Checkpoints, and Retention

Duration:

- 4 weeks

Goals:

- reduce read amplification
- implement flush-based checkpoints
- implement conservative retention enforcement

Deliverables:

- compaction planner and executor
- checkpoint lifecycle
- retention policy engine
- pre/post-compaction equivalence tests

Acceptance criteria:

- compaction does not change query answers incorrectly
- checkpoints reduce restart time relative to replay-only recovery
- retention policies are testable and auditable

Risks retired:

- runaway storage growth
- poor restart behavior
- silent history corruption

## 5.9 Milestone 8: Admin Plane, Metrics, and Backup/Restore

Duration:

- 3 weeks

Goals:

- expose health and maintenance APIs
- add metrics and tracing hooks
- support consistent backups and restore

Deliverables:

- `AdminService`
- metrics instrumentation
- backup manifest capture
- restore workflow

Acceptance criteria:

- operators can inspect health state, compaction backlog, and checkpoint status
- backups restore into a readable system
- failure modes are visible without reading raw logs

Risks retired:

- weak operational story
- no disaster recovery path

## 5.10 Milestone 9: Python SDK and Connectors

Duration:

- 3 weeks

Goals:

- make the project easy to adopt
- expose historical and online APIs in Python
- add at least one streaming connector and one analytics integration

Deliverables:

- Python SDK
- Kafka ingestion connector
- DuckDB or Polars integration
- SDK examples and notebooks

Acceptance criteria:

- Python users can register features, ingest data, fetch latest vectors, and run historical jobs
- connector behavior matches core engine semantics
- Arrow interchange stays zero-copy where practical

Risks retired:

- great engine with weak adoption path
- connector semantic drift

## 5.11 Milestone 10: Hardening and Public Release

Duration:

- 3 weeks

Goals:

- fix correctness bugs found under stress
- publish benchmarks and docs
- cut a credible open-source release

Deliverables:

- release notes
- benchmark report
- operational runbook
- packaging artifacts
- sample datasets and demos

Acceptance criteria:

- recovery, compaction, and PIT correctness suites pass reliably
- docs are enough for a new user to run the system end to end
- benchmark claims are reproducible from the repository

## 6. Critical Path Dependencies

The critical dependency graph is:

```text
spec + ADRs
  -> metadata validation
  -> WAL commit pipeline
  -> memtable apply + recovery
  -> latest cache
  -> immutable segments + manifest
  -> as-of lookup
  -> PIT joins
  -> compaction + checkpoints
  -> admin/backup
  -> SDK/connectors
  -> public release
```

Important dependency rules:

- do not implement PIT joins before as-of lookups are provably correct
- do not implement shared-memory serving before normal serving is benchmarked and stable
- do not start distributed clustering before v1 recovery and compaction are trustworthy

## 7. Parallelization Opportunities

If more than one engineer is available, parallelize like this:

- engineer 1: storage engine and recovery path
- engineer 2: metadata plane, API server, admin plane, SDK
- engineer 3: query engine, PIT joins, and benchmarks

But these efforts should converge at milestone boundaries, not drift into isolated subprojects.

## 8. Test Plan by Milestone

## 8.1 Always-On Test Suites

Run on every change:

- unit tests for temporal ordering and value encoding
- metadata validation tests
- write-path idempotency tests
- latest-cache publication concurrency tests

## 8.2 Milestone-Gated Suites

Must pass before advancing:

- WAL crash-injection suite
- replay equivalence suite
- as-of correctness differential tests
- compaction equivalence suite
- PIT leakage-prevention suite
- backup/restore round-trip suite

## 8.3 Benchmark Gates

Before `beta`:

- online-serving latency benchmark
- as-of lookup latency benchmark
- PIT join throughput benchmark

Before `v1.0`:

- mixed workload benchmark with serving plus background jobs
- restart-time benchmark from realistic WAL size

## 9. Risk Register and Mitigations

## 9.1 Semantic Risk

Risk:

- getting bitemporal selection wrong in edge cases

Mitigation:

- reference implementation
- property tests
- differential tests

## 9.2 Systems Risk

Risk:

- WAL or compaction design creates bad tail-latency spikes

Mitigation:

- strict background budgets
- milestone-specific latency benchmarks
- profiling before adding new features

## 9.3 Scope Risk

Risk:

- project expands into warehouse or workflow-platform territory

Mitigation:

- ADR 0001
- explicit non-goals
- roadmap reviews at each milestone

## 9.4 Adoption Risk

Risk:

- strong engine with weak onboarding

Mitigation:

- Python SDK before public release
- clear examples
- benchmark-driven positioning against existing alternatives

## 10. Open-Source Release Strategy

The project should be released in stages:

### Stage 1

- architecture docs
- ADRs
- public roadmap

### Stage 2

- runnable alpha focused on durability and latest reads

### Stage 3

- beta with PIT joins and dataset builds

### Stage 4

- v1.0 with operational hardening and benchmarks

Each stage should include:

- a short benchmark story
- a concrete demo
- a known limitations section

## 11. Success Criteria

The project is successful at `v1.0` if it can show all of the following:

- correct and reproducible PIT retrieval under delayed corrections
- fast latest-value serving without history scans on the hot path
- recovery that preserves acknowledged writes
- enough observability and operational control for a serious user
- an adoption path through Python and Arrow-based tooling

That is the minimum bar for this to be viewed as a real systems-and-ML open-source project rather than a concept repository.
