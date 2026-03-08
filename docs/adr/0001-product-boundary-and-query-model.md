# ADR 0001: Product Boundary and Query Model

Status: Accepted

## Context

The project risks becoming one of three things if the product boundary is not set early:

- a thin control plane on top of other databases
- a generic SQL engine with temporal features bolted on
- a cache-like online feature service with weak historical semantics

All three already exist in some form. The actual differentiator here is narrow and specific:

- strong bitemporal correctness
- high-performance latest-value serving
- first-class point-in-time joins

If the system tries to become a warehouse, stream processor, and feature-engineering platform at the same time, it will fail before the storage engine is credible.

## Decision

The product is a specialized feature database for ML workloads.

The core supported query model is:

- latest lookup by entity or batch of entities
- as-of lookup under explicit event-time and system-time cutoffs
- point-in-time joins driven by labeled datasets
- bounded window scans and engine-native rolling aggregates
- audit and revision history

The product explicitly is not:

- a full ANSI SQL warehouse
- a general OLAP engine
- a Python-executed transformation engine
- a feature notebook or orchestration platform

An optional SQL surface may exist, but only as an adapter over engine-native operators. SQL is not the source of truth for semantics.

## Consequences

Positive:

- architecture stays focused on the hard problem: temporal correctness plus serving speed
- the engine can optimize heavily for PIT joins and latest lookups
- documentation and product positioning stay clear
- the system can integrate with DuckDB, Polars, Spark, or warehouses instead of competing with them head-on

Negative:

- users cannot expect arbitrary joins, arbitrary ad hoc SQL, or broad OLAP support
- some analytics use cases will require exporting data into external tools
- early adopters may ask for generic warehouse features that the project should decline

## Alternatives Considered

### Build a Full SQL Database

Rejected.

Reason:

- much larger scope
- weaker product differentiation
- would force time away from the unique operator set

### Build Only an Online Cache and Delegate Offline History Elsewhere

Rejected.

Reason:

- misses the core value of training-time correctness
- turns the product into a thinner variant of existing online stores

### Build Only a Control Plane and Reuse Existing Storage Engines

Rejected.

Reason:

- the storage/query semantics are the differentiator
- existing substrates do not naturally model bitemporal corrections and low-latency serving together

## Implementation Notes

- the API surface should expose explicit engine-native RPCs
- the historical job layer should use pinned snapshots and Arrow export
- external SQL integrations should be built as table functions or adapters, not as the primary contract
