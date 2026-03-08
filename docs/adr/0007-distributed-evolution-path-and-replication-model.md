# ADR 0007: Distributed Evolution Path and Replication Model

Status: Accepted

## Context

Scale-out will eventually matter, but building the distributed system first would delay proof of correctness and adoption. The architecture still needs a clear evolution path so that v1 decisions do not trap the project later.

## Decision

The product roadmap is:

- v1: single-node durable service
- v1.x: stronger operations, performance, shared-memory serving, and connector maturity
- v2: distributed cluster with shard leadership and WAL-based replication

Shard key:

```text
(tenant_id, entity_type, hash(entity_id))
```

Replication model:

- one leader per shard
- one or more followers
- WAL-based replication
- Raft or an equivalent existing replicated-log protocol

Query routing:

- latest lookups route directly to the owning shard
- large historical jobs are coordinator-driven fan-out jobs or run near data

Consistency model:

- leader-committed writes define visibility
- stronger read-your-writes modes are opt-in
- active-active multi-region is explicitly out of scope

## Consequences

Positive:

- the local partition model in v1 maps cleanly to the future shard model
- the project avoids inventing a new consensus protocol
- distributed complexity is deferred until the single-node engine is trustworthy

Negative:

- some large users will outgrow v1 before v2 exists
- distributed joins and rebalancing remain future work
- follower-read semantics will require careful definition later

## Alternatives Considered

### Build Multi-Node Consensus into v1

Rejected.

Reason:

- too much complexity before local semantics are proven

### Shard by Feature Instead of Entity

Rejected.

Reason:

- online latest reads would fan out unnecessarily
- per-entity timelines would be fragmented

### Active-Active Multi-Region First

Rejected.

Reason:

- incompatible with the goal of a credible first release
- conflict resolution under bitemporal semantics is too large a scope jump

## Implementation Notes

- all v1 APIs should already carry tenant and entity-type fields
- the manifest and WAL format should be designed so shard-local versions are a later extension, not a rewrite
- distributed historical execution should prefer data-local scans over central row-by-row RPCs
