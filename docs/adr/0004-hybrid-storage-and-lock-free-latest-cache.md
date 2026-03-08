# ADR 0004: Hybrid Storage Layout and Lock-Free Latest Cache

Status: Accepted

## Context

The engine has two fundamentally different access patterns:

- historical timeline scans and PIT joins
- latest-vector serving for online inference

One structure will not serve both workloads well. A pure row-store underperforms on batch scans. A pure column-store is awkward for point-in-time temporal retrieval. Recomputing latest state on demand also adds tail-latency risk to serving.

## Decision

The storage engine uses two complementary structures:

1. immutable hybrid segments for history
2. a dedicated in-memory latest cache for serving

Hybrid segments store:

- row-oriented temporal keys for revision lookup
- value blocks and optional columnar projections for scan-heavy reads
- block indexes, zone maps, and existence filters

The latest cache stores immutable per-entity snapshots:

```text
latest_cache[tenant_id, entity_type, entity_id, feature_group] -> FeatureVectorSnapshot*
```

Publication rule:

1. build a new immutable snapshot
2. atomically swap the pointer
3. reclaim old snapshots with epoch-based reclamation or hazard pointers

Serving groups are explicit metadata objects. The engine does not publish one giant universal feature vector by default.

## Consequences

Positive:

- historical access and online serving both get optimized layouts
- latest reads avoid timeline scans
- readers never observe partially updated vectors
- feature groups reduce rebuild cost and memory waste

Negative:

- the engine maintains more than one physical representation
- write amplification increases for serving-enabled features
- memory budgeting becomes more important

## Alternatives Considered

### Pure Row-Oriented Historical Store

Rejected.

Reason:

- weaker scan throughput
- poorer Arrow export characteristics

### Pure Columnar Store for Everything

Rejected.

Reason:

- inefficient for latest-value temporal lookups
- awkward mutation and revision traversal behavior

### No Dedicated Latest Cache

Rejected.

Reason:

- read latency becomes more variable
- serving path touches too much historical structure

## Implementation Notes

- feature groups should be first-class in metadata
- online-serving cache entries should carry publication version and freshness metadata
- cache rebuild on restart may be lazy, but visibility rules must stay precise
