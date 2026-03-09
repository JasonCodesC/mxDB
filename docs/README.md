# Architecture Index

This directory maps the specification, ADRs, API contracts, and execution plan for mxDB.

## Core Documents

- Full system spec: [bitemporal-feature-engine-spec.md](bitemporal-feature-engine-spec.md)
- HTML rendering of the spec: [bitemporal-feature-engine-spec.html](bitemporal-feature-engine-spec.html)
- API contract: [bitemporal-feature-engine-api-contracts.md](bitemporal-feature-engine-api-contracts.md)
- Implementation plan: [bitemporal-feature-engine-implementation-plan.md](bitemporal-feature-engine-implementation-plan.md)
- Implementation backlog: [implementation-backlog.md](implementation-backlog.md)
- Definition of done: [definition-of-done.md](definition-of-done.md)
- Development guide: [development-guide.md](development-guide.md)
- ADR index: [adr/README.md](adr/README.md)
- Operational runbook: [operational-runbook.md](operational-runbook.md)
- Python packaging: [python-packaging.md](python-packaging.md)
- Python SDK usage: [../sdk/python/README.md](../sdk/python/README.md)
- Known limitations: [known-limitations.md](known-limitations.md)

## Public API (Current v1)

The current public runtime API is:

- `featurectl` CLI (`tools/featurectl/main.cc`)
- Python SDK (`sdk/python/`) that shells out to `featurectl`

## Planned RPC Schemas (Not v1 Public API)

`proto/` documents planned gRPC surfaces for later milestones:

- [common.proto](../proto/common.proto)
- [metadata.proto](../proto/metadata.proto)
- [ingestion.proto](../proto/ingestion.proto)
- [serving.proto](../proto/serving.proto)
- [historical.proto](../proto/historical.proto)
- [admin.proto](../proto/admin.proto)

## Reading Order

1. [bitemporal-feature-engine-spec.md](bitemporal-feature-engine-spec.md)
2. [adr/README.md](adr/README.md)
3. [bitemporal-feature-engine-api-contracts.md](bitemporal-feature-engine-api-contracts.md)
4. [bitemporal-feature-engine-implementation-plan.md](bitemporal-feature-engine-implementation-plan.md)
5. [implementation-backlog.md](implementation-backlog.md)
6. [definition-of-done.md](definition-of-done.md)
7. [development-guide.md](development-guide.md)
