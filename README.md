# Bitemporal Feature Engine

A specialized feature database for machine learning systems that need:

- point-in-time correct historical retrieval
- event-time and system-time tracking
- low-latency latest-value serving
- durable database behavior with WAL, recovery, checkpoints, and compaction

The project is aimed at ML workloads where late data, corrections, and training leakage actually matter. Quant finance is an obvious use case, but the design also applies to fraud, recommender systems, IoT, healthcare, and other real-time or delayed-data ML domains.

## Status

Current repository state:

- architecture spec complete
- ADR set complete
- API contract complete
- protobuf contracts complete
- implementation plan complete
- execution docs for an implementation agent complete
- core codebase not started yet

This repository is currently in the transition from architecture to implementation.

## Read First

- Project execution rules: [AGENT.md](/Users/jasonmajoros/Documents/New project/AGENT.md)
- Architecture index: [README.md](/Users/jasonmajoros/Documents/New project/docs/README.md)
- Full system spec: [bitemporal-feature-engine-spec.md](/Users/jasonmajoros/Documents/New project/docs/bitemporal-feature-engine-spec.md)
- API contracts: [bitemporal-feature-engine-api-contracts.md](/Users/jasonmajoros/Documents/New project/docs/bitemporal-feature-engine-api-contracts.md)
- Implementation plan: [bitemporal-feature-engine-implementation-plan.md](/Users/jasonmajoros/Documents/New project/docs/bitemporal-feature-engine-implementation-plan.md)
- Backlog: [implementation-backlog.md](/Users/jasonmajoros/Documents/New project/docs/implementation-backlog.md)
- Definition of done: [definition-of-done.md](/Users/jasonmajoros/Documents/New project/docs/definition-of-done.md)
- Development guide: [development-guide.md](/Users/jasonmajoros/Documents/New project/docs/development-guide.md)

## Core Idea

The engine stores feature values along two timelines:

- `event_time`: when the fact was true
- `system_time`: when the system learned or accepted that fact

That lets the engine answer both:

- "What was the feature value as of time T?"
- "What did the system know as of time T?"

This is the foundation for:

- leakage-safe training data
- auditability
- correction handling
- reproducible model inputs

## Planned V1

The first real release is a single-node service with:

- metadata registry
- WAL-backed ingestion
- memtables and immutable segments
- crash recovery
- latest-value serving
- as-of lookups
- point-in-time joins
- checkpoints and compaction
- admin and observability surfaces
- Python SDK

Distributed clustering is intentionally later.

## Repository Layout

The intended repository structure is documented in the main spec, but the high-level layout is:

- `docs/`: architecture, decisions, contracts, backlog, execution guidance
- `proto/`: machine-readable API contracts
- `tools/`: support scripts such as spec rendering

As implementation starts, the repository should grow into the full structure described in the spec.

## Current Next Step

The highest-value implementation starting point is:

1. create the build skeleton and source tree
2. implement the metadata plane and validation library
3. implement the WAL format and crash-recovery harness

Those steps are detailed in [implementation-backlog.md](/Users/jasonmajoros/Documents/New project/docs/implementation-backlog.md).
