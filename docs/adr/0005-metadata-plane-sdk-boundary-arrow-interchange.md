# ADR 0005: Metadata Plane, SDK Boundary, and Arrow Interchange

Status: Accepted

## Context

The product needs a registry, auth model, feature definitions, and connector configuration. It also needs a Python-friendly adoption path. These concerns are necessary, but they are not the core of the storage engine.

If the project stores metadata in its own temporal engine from day one, it effectively tries to build two databases at once.

## Decision

The system separates the control plane from the data plane.

For v1:

- control-plane metadata is stored in embedded SQLite
- metadata is accessed through a typed C++ service layer
- feature traffic never goes through SQLite

The Python SDK is thin:

- it calls gRPC APIs
- it exposes Arrow-native batch results
- it provides Pandas and Polars conversion helpers
- it does not implement its own temporal logic

Arrow is the canonical batch interchange format between:

- historical query engine
- Python SDK
- external analytical tools

Connectors are thin adapters over core APIs. They must not redefine:

- system-time assignment rules
- PIT join semantics
- deduplication logic
- registry interpretation

## Consequences

Positive:

- metadata stays transactional and easy to manage
- the core engine remains focused on feature data
- Python adoption stays strong without moving the hot path out of C++
- Arrow reduces copying and integration friction

Negative:

- there are now two storage technologies in the system
- version compatibility between metadata and data plane must be documented
- some users will ask for more logic in the SDK than the design should allow

## Alternatives Considered

### Store Metadata in the Core Feature Engine

Rejected.

Reason:

- unnecessary complexity for low-volume control-plane state
- blurs the product boundary

### Make the Python SDK Smart and Stateful

Rejected.

Reason:

- duplicates temporal semantics outside the engine
- increases drift between clients

### Use JSON or CSV as the Primary Batch Interchange

Rejected.

Reason:

- poor type fidelity
- much worse analytics interoperability
- unnecessary copy costs

## Implementation Notes

- metadata version compatibility should be part of startup checks
- Arrow schemas should include quality flags and nullability consistently
- future SQL adapters should still lower into the same core operators
