# Architecture Decision Records

This directory locks the highest-impact architectural decisions from the system spec.

## ADR Index

- [0001 Product Boundary and Query Model](0001-product-boundary-and-query-model.md)
- [0002 Bitemporal Data Model and Version Selection](0002-bitemporal-data-model-and-version-selection.md)
- [0003 V1 Node Architecture with Shared WAL and Local Partitions](0003-v1-node-architecture-shared-wal-local-partitions.md)
- [0004 Hybrid Storage Layout and Lock-Free Latest Cache](0004-hybrid-storage-and-lock-free-latest-cache.md)
- [0005 Metadata Plane, SDK Boundary, and Arrow Interchange](0005-metadata-plane-sdk-boundary-arrow-interchange.md)
- [0006 Checkpointing, Recovery, Compaction, and Retention](0006-checkpointing-recovery-compaction-retention.md)
- [0007 Distributed Evolution Path and Replication Model](0007-distributed-evolution-path-and-replication-model.md)

## Status Conventions

- `Accepted`: implement unless superseded
- `Superseded`: replaced by later ADR
- `Proposed`: not yet locked
