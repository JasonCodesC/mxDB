# ADR 0002: Bitemporal Data Model and Version Selection

Status: Accepted

## Context

Feature systems that store only the latest value cannot correctly answer:

- what was true at time `T`
- what the system knew at time `T`
- how later corrections changed training data or model inputs

Many production ML domains receive delayed events, corrections, backfills, and deletions. Without a clear temporal model, the engine will either leak future information into training or lose auditability.

## Decision

The engine stores immutable `feature_event` records with the following logical identity:

```text
(tenant_id, entity_type, entity_id, feature_id, event_time, system_time, sequence_no)
```

Each record also carries:

- `op`: `UPSERT` or `DELETE`
- `value`
- `write_id`
- `source_id`
- `quality_flags`

Selection semantics for `get_as_of` and PIT joins are:

1. filter to matching tenant, entity, and feature
2. require `event_time <= event_cutoff`
3. require `system_time <= system_cutoff`
4. choose maximal `(event_time, system_time, sequence_no)`
5. if the chosen record is a tombstone, surface null or missing

`system_time` is server-assigned by default.

Trusted connectors may provide external `system_time` values only when:

- the source is explicitly configured as trusted
- the request opts into trusted time mode
- the feature policy allows external system time

Idempotency is keyed by `write_id`. The engine guarantees:

- at-least-once ingestion transport
- exactly-once visible state when `write_id` is stable and correctly supplied

## Consequences

Positive:

- late corrections become first-class
- training reproducibility and auditing become possible
- the semantics of historical retrieval are precise and testable

Negative:

- storage footprint grows due to revision retention
- compaction and retention become more subtle
- write APIs need stronger validation

## Alternatives Considered

### Event Time Only

Rejected.

Reason:

- cannot answer what the system knew at a prior time
- fails for corrections and delayed ingestion

### Overwrite-in-Place Updates

Rejected.

Reason:

- destroys audit history
- makes recovery and deterministic replay weaker

### Client-Controlled System Time for All Writes

Rejected.

Reason:

- too easy to corrupt temporal correctness
- upstream clocks and contracts cannot be trusted by default

## Implementation Notes

- tombstones are logically different from null values
- equality edge cases must be resolved with `sequence_no`
- correctness tests should compare engine results against a simple reference implementation
