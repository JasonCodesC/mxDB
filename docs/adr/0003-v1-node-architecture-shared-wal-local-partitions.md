# ADR 0003: V1 Node Architecture with Shared WAL and Local Partitions

Status: Accepted

## Context

The v1 engine must be durable and fast enough to matter, but still implementable by a small team. Two common failure modes are:

- building a single giant lock-protected node that collapses under concurrency
- prematurely designing a distributed engine before the local semantics are correct

The project needs a node architecture that scales across cores while keeping durability and recovery understandable.

## Decision

v1 is a single-node service with internal local partitions.

Partitioning function:

```text
partition_id = hash(tenant_id, entity_type, entity_id) % P
```

Each partition owns:

- a mutable memtable
- immutable memtables pending flush
- a latest-cache shard
- a set of immutable on-disk segments
- flush and compaction work queues

The node shares:

- one WAL
- one manifest
- one metadata service
- one RPC ingress layer

Write flow:

1. ingress validates request
2. write coordinator assigns `system_time`, `sequence_no`, and `LSN`
3. request appends to a shared WAL
4. after the durability gate, the record is routed to the owning partition
5. the partition worker applies it to the memtable and publishes latest-cache state

Atomicity rule:

- single-entity batches are atomic
- multi-entity batches are not transactional in v1

## Consequences

Positive:

- one global durability pipeline keeps recovery simple
- partition-local apply queues reduce lock contention
- the node architecture maps cleanly to a later sharded cluster design

Negative:

- the WAL writer can become a central throughput bottleneck
- cross-partition batch transactions are intentionally unsupported
- some throughput optimizations are deferred to later versions

## Alternatives Considered

### Single Global Memtable and Cache

Rejected.

Reason:

- too much lock contention
- poor cache locality
- weak path to scale across cores

### Per-Partition WAL in v1

Rejected.

Reason:

- harder recovery model
- harder checkpointing
- more moving parts before the product is proven

### Distributed Cluster in v1

Rejected.

Reason:

- consensus and rebalancing would dominate project scope
- the main unknown is still local correctness and performance

## Implementation Notes

- apply queues should preserve commit order within a partition
- historical queries should execute partition-locally and merge centrally
- admission control must protect serving traffic from background work
