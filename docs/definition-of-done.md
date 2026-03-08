# Definition of Done

This document defines when work in this project is actually complete.

It exists to prevent false completion signals, especially for core systems work where it is easy to stop at a demo, stub, or happy-path implementation.

## 1. Global Rule

A task is not done when code exists. A task is done when the intended behavior exists, is tested, is documented, and is operable.

## 2. Done for Any Code Change

Any meaningful code change is only done when all applicable items are true:

- the code builds
- relevant tests exist and pass
- error handling is not left as TODOs
- docs are updated if behavior or interfaces changed
- logging, metrics, or admin visibility exist where needed
- no fake or placeholder implementation is being represented as final behavior

## 3. Done for Core Persistence Work

Changes in WAL, recovery, memtables, segments, compaction, or checkpoints are only done when:

- happy-path behavior works
- crash behavior is tested
- replay behavior is deterministic
- corrupt or partial state is handled safely
- invariants are documented
- pre/post-compaction query results are verified where applicable

## 4. Done for Query Semantics

Changes affecting `get_latest`, `get_as_of`, PIT joins, or window reads are only done when:

- results match the documented semantics
- null and tombstone behavior is tested
- equal-timestamp and ordering edge cases are tested
- time-cutoff rules are explicit in code and tests
- behavior is validated against a simple reference implementation where practical

## 5. Done for Online Serving

Serving functionality is only done when:

- correctness is proven
- visibility semantics are clear
- latency is measured
- cache invalidation or publication semantics are tested
- failure and stale-read behavior are explicit

## 6. Done for Historical Jobs

Historical dataset build functionality is only done when:

- jobs operate on pinned snapshots
- output is reproducible
- memory usage is bounded
- output schemas are documented
- cancellation and failure behavior are defined

## 7. Done for Metadata and APIs

Metadata or API work is only done when:

- protobuf or schema contracts are updated
- validation behavior is implemented
- incompatible changes are avoided or documented
- examples or usage documentation exist for public surfaces

## 8. Done for Milestones

A milestone is done only when:

- all milestone acceptance criteria in the implementation plan are met
- the backlog items for that milestone are closed or explicitly deferred
- key tests for the milestone are automated
- the docs reflect the new repository reality

## 9. Done for Releases

`MVP-alpha` is only done when:

- the single-node engine is real, durable, and recoverable
- latest reads and as-of reads work correctly
- core crash and correctness tests pass

`beta` is only done when:

- PIT joins and dataset builds work end to end
- checkpoints and compaction work
- benchmark suites exist
- Python adoption path exists

`v1.0` is only done when:

- operational visibility is strong
- backup and restore work
- public docs are coherent
- benchmark claims are reproducible
- the engine is credible to an external user

## 10. Not-Done Examples

The following states are explicitly not done:

- code compiles but core edge cases are untested
- persistence works only if the process exits cleanly
- an API exists but the semantics are inconsistent with the spec
- historical joins work only by slow row-by-row fallback while being described as production-ready
- docs describe features that the code does not actually support

## 11. Required Evidence

Before claiming completion, the agent should be able to point to:

- code paths
- tests
- metrics or logs
- docs
- benchmark or verification results where relevant

If that evidence does not exist, the work is not complete.
