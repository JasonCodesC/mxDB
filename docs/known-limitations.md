# Known Limitations

- Public transport APIs are currently CLI-based (`featurectl`), not gRPC server endpoints yet.
- Arrow-native historical export and protobuf/gRPC service bindings are not implemented yet.
- PIT join execution currently uses repeated as-of lookups and is correctness-first, not optimized.
- Retention policy evaluation is conservative and does not yet support policy-specific pruning.
- Compaction currently rewrites immutable segments and preserves all revisions; advanced pruning is deferred.
- Backup/restore currently performs filesystem copy and assumes local single-node storage.
- Python SDK is an embedded thin wrapper over `featurectl`, not a persistent network client yet.
