# ADR 0006: Checkpointing, Recovery, Compaction, and Retention

Status: Accepted

## Context

The engine is only credible if it behaves like a database under failure:

- acknowledged writes must survive according to durability mode
- recovery must be deterministic
- compaction must not change query answers incorrectly
- retention must not silently destroy audit history

These concerns interact directly with bitemporal semantics.

## Decision

Checkpointing:

- v1 uses flush-based checkpoints
- a checkpoint records a manifest snapshot and a durable LSN boundary
- mutable state is reduced by freezing and flushing memtables

Recovery:

- startup loads the latest valid manifest snapshot
- opens immutable segments
- replays WAL records after the checkpoint LSN
- rebuilds memtables and latest-cache state

Compaction:

- writes new immutable segments
- never rewrites files in place
- only swaps visibility through manifest updates

Retention:

- defaults are conservative
- old revisions may only be dropped when policy proves they can no longer affect allowed queries
- tombstones remain until retention and visibility windows expire

## Consequences

Positive:

- the recovery model is simple enough to reason about
- compaction cannot partially corrupt the visible dataset if manifest swaps are atomic
- retention bugs are less likely because the default posture is conservative

Negative:

- checkpoints may create more IO than snapshot-based schemes
- storage growth is higher under conservative retention
- compaction policies require careful policy design and tests

## Alternatives Considered

### Snapshot Mutable Structures Without Flush in v1

Rejected.

Reason:

- more complex crash semantics
- harder to implement safely as a first version

### In-Place Segment Rewriting

Rejected.

Reason:

- much riskier failure modes
- harder atomicity story

### Aggressive Revision Pruning by Default

Rejected.

Reason:

- too easy to break audit and training reproducibility
- unsafe for regulated or delayed-data domains

## Implementation Notes

- compaction equivalence tests should compare answers before and after compaction
- recovery tests should inject crashes after each step of the write path
- backup and restore are separate from crash recovery and need their own tests
