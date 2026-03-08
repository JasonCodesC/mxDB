# Development Guide

This document gives implementation-facing guidance for building the project described in the architecture docs.

It is not the product spec. It explains how to translate the spec into a codebase without losing rigor.

## 1. Working Defaults

Unless an ADR changes them, use these defaults:

- language: C++20 for the core engine
- build system: CMake
- RPC: gRPC + protobuf
- metadata storage: SQLite
- batch interchange: Arrow
- Python packaging: modern `pyproject.toml` based packaging for the SDK

These defaults are execution guidance, not immutable architecture law. If they need to change, document the change explicitly.

## 2. Source Tree Policy

Implementation should grow into the source-tree shape documented in the main spec.

When adding code:

- put shared primitives in `engine/common/` or a similarly scoped module
- keep storage concerns in storage-focused modules
- keep API-facing code in `server/` and `proto/`
- keep SDK logic thin
- do not bury core engine logic inside connector or SDK code

## 3. Dependency Rules

Be conservative with dependencies.

Allowed dependency pattern:

- add a dependency when it materially reduces implementation risk or complexity
- prefer mature, boring libraries for infrastructure concerns

Avoid:

- adding large frameworks that redefine the architecture
- hiding core semantics behind opaque third-party abstractions
- introducing separate dependency stacks for each subsystem without a strong reason

## 4. Coding Priorities

Optimize for:

1. correctness
2. clarity of invariants
3. recoverability
4. observability
5. performance

Performance matters, but only after the semantics are solid.

## 5. Testing Strategy

Every subsystem should have the right kind of tests:

- unit tests for deterministic helpers and ordering logic
- integration tests for subsystem boundaries
- property or differential tests for temporal selection logic
- fault-injection tests for recovery paths
- benchmarks for hot paths

Do not rely on only one test style for persistence or query semantics.

## 6. Implementation Style Rules

Use these rules when building the codebase:

- prefer explicit types and explicit invariants
- keep serialization, persistence, and business logic separated
- do not mix test-only shortcuts into production paths
- document non-obvious invariants close to the code
- avoid premature abstraction that hides important storage behavior

## 7. Documentation Rules

When implementation reveals missing detail:

- update the appropriate doc
- add or update an ADR if the change is architectural
- update backlog and definition-of-done references if task shape changed

Documentation is part of the product, not cleanup work for later.

## 8. Benchmark Rules

Benchmarks should be:

- reproducible
- versioned in the repo
- based on explicit workloads
- tied to claims made in docs or release notes

Do not make performance claims that are not backed by benchmark artifacts in the repository.

## 9. Failure-Mode Discipline

For any change in persistence, serving, or historical query code, ask:

- what happens on crash?
- what happens on partial write?
- what happens on replay?
- what happens on corruption?
- what happens under overload?

If those answers are unclear, the implementation is not ready.

## 10. API Discipline

The `proto/` files are the machine-readable public contract.

Rules:

- do not make ad hoc API changes in server code without updating the proto files
- do not create hidden behavior that is absent from the contract docs
- keep request semantics explicit, especially around time cutoffs and consistency

## 11. Milestone Discipline

The project should advance through vertical slices:

- metadata
- write durability
- recovery
- serving
- historical reads
- PIT joins
- compaction and ops

Avoid spending large effort on:

- connectors
- dashboards
- packaging
- distributed clustering

before the core engine milestones are real.

## 12. Day-One Implementation Guidance

The first code should establish:

- build skeleton
- module boundaries
- metadata persistence
- config and logging primitives
- WAL test harness

That work creates a stable base for the storage engine rather than forcing later rewrites.
