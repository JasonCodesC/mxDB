# Bitemporal Feature Engine: API and Protobuf Contract

Status: Draft v1 contract

This document turns the architecture spec into transport-level contracts. It defines the RPC surface, message shapes, semantics, error model, and versioning rules for the first public API.

The intent is to be exact enough that these definitions can be split into `proto/` files without changing meaning.

## 1. Contract Design Goals

The API contract is designed to preserve the product boundary established in the main spec:

- explicit temporal semantics
- explicit visibility and durability semantics
- narrow, engine-native RPCs
- thin client bindings
- compatibility with gRPC and Arrow-oriented batch workflows

The API contract must make the following impossible or at least explicit:

- silent lookahead leakage
- ambiguous system-time behavior
- fake transactional guarantees
- connector-specific semantic drift

## 2. Versioning and Namespacing

Package:

```proto
syntax = "proto3";
package bitemporal.feature.v1;
```

Compatibility rules:

- additive fields are allowed in `v1`
- field numbers are never reused
- message meaning may not change incompatibly within `v1`
- behavioral breaking changes require `v2`
- enum additions are allowed if old clients can safely ignore unknown values

Transport:

- primary transport is gRPC over HTTP/2
- protobuf is the control and request/response format
- Arrow is used for bulk batch payloads and historical outputs

## 3. Shared Semantics

## 3.1 Identity Model

Every request is scoped by:

- `tenant_id`
- `entity_type`
- one or more `entity_id`
- one or more features

The API never assumes a global default tenant or entity type.

## 3.2 Time Model

The API distinguishes:

- `event_time`: when the fact was true in the domain
- `system_time`: when the engine accepted or trusted the fact

Default rule:

- clients supply `event_time`
- server assigns `system_time`

Override rule:

- trusted connectors may provide `system_time` only when explicitly authorized

## 3.3 Atomicity Model

- all events for one entity inside one write batch are atomic together
- multi-entity requests are not cross-entity transactions
- historical jobs run on pinned snapshots and are not blocked by new writes

## 3.4 Durability Model

Supported durability modes:

- `DURABILITY_SYNC`
- `DURABILITY_GROUP_COMMIT`
- `DURABILITY_ASYNC`

The response includes the committed `lsn` and effective `system_time` boundary for each successful entity batch.

## 3.5 Read Consistency Model

Supported consistency modes:

- `CONSISTENCY_LATEST_VISIBLE`
- `CONSISTENCY_AT_LEAST_LSN`
- `CONSISTENCY_BEST_EFFORT_FRESH`

Historical APIs use explicit snapshot and cutoff fields instead of generic read consistency names.

## 4. Common Types

Recommended `common.proto` content:

```proto
syntax = "proto3";
package bitemporal.feature.v1;

import "google/protobuf/duration.proto";
import "google/protobuf/empty.proto";
import "google/protobuf/timestamp.proto";

enum ValueType {
  VALUE_TYPE_UNSPECIFIED = 0;
  VALUE_TYPE_BOOL = 1;
  VALUE_TYPE_INT64 = 2;
  VALUE_TYPE_DOUBLE = 3;
  VALUE_TYPE_STRING = 4;
  VALUE_TYPE_FLOAT_VECTOR = 5;
  VALUE_TYPE_DOUBLE_VECTOR = 6;
}

enum OperationType {
  OPERATION_TYPE_UNSPECIFIED = 0;
  OPERATION_TYPE_UPSERT = 1;
  OPERATION_TYPE_DELETE = 2;
}

enum DurabilityMode {
  DURABILITY_MODE_UNSPECIFIED = 0;
  DURABILITY_SYNC = 1;
  DURABILITY_GROUP_COMMIT = 2;
  DURABILITY_ASYNC = 3;
}

enum ConsistencyMode {
  CONSISTENCY_MODE_UNSPECIFIED = 0;
  CONSISTENCY_LATEST_VISIBLE = 1;
  CONSISTENCY_AT_LEAST_LSN = 2;
  CONSISTENCY_BEST_EFFORT_FRESH = 3;
}

enum JobState {
  JOB_STATE_UNSPECIFIED = 0;
  JOB_STATE_PENDING = 1;
  JOB_STATE_RUNNING = 2;
  JOB_STATE_SUCCEEDED = 3;
  JOB_STATE_FAILED = 4;
  JOB_STATE_CANCELLED = 5;
}

enum HealthState {
  HEALTH_STATE_UNSPECIFIED = 0;
  HEALTH_HEALTHY = 1;
  HEALTH_DEGRADED_WRITES = 2;
  HEALTH_DEGRADED_READS = 3;
  HEALTH_COMPACTION_BACKLOG = 4;
  HEALTH_RECOVERING = 5;
  HEALTH_READ_ONLY = 6;
}

enum OutputFormat {
  OUTPUT_FORMAT_UNSPECIFIED = 0;
  OUTPUT_FORMAT_ARROW_FILE = 1;
  OUTPUT_FORMAT_ARROW_STREAM = 2;
  OUTPUT_FORMAT_PARQUET = 3;
}

enum ErrorCode {
  ERROR_CODE_UNSPECIFIED = 0;
  ERROR_INVALID_ARGUMENT = 1;
  ERROR_NOT_FOUND = 2;
  ERROR_ALREADY_EXISTS = 3;
  ERROR_PERMISSION_DENIED = 4;
  ERROR_UNAUTHENTICATED = 5;
  ERROR_OVERLOADED = 6;
  ERROR_RATE_LIMITED = 7;
  ERROR_TEMPORAL_CONFLICT = 8;
  ERROR_SCHEMA_MISMATCH = 9;
  ERROR_STALE_READ = 10;
  ERROR_SNAPSHOT_EXPIRED = 11;
  ERROR_INTERNAL = 12;
}

message RequestContext {
  string request_id = 1;
  string caller = 2;
  string trace_id = 3;
  string auth_principal = 4;
}

message TenantRef {
  string tenant_id = 1;
}

message EntityKey {
  string tenant_id = 1;
  string entity_type = 2;
  string entity_id = 3;
}

message FeatureRef {
  oneof ref {
    string feature_id = 1;
    string feature_name = 2;
  }
}

message CommitToken {
  uint64 lsn = 1;
  google.protobuf.Timestamp commit_system_time = 2;
}

message SnapshotToken {
  uint64 manifest_version = 1;
  uint64 max_visible_lsn = 2;
  google.protobuf.Timestamp system_cutoff = 3;
}

message QualityFlags {
  bool is_corrected = 1;
  bool is_inferred = 2;
  bool is_stale = 3;
  bool source_validation_failed = 4;
  repeated string tags = 5;
}

message NullValue {}

message FloatVector {
  uint32 dimension = 1;
  repeated float values = 2;
}

message DoubleVector {
  uint32 dimension = 1;
  repeated double values = 2;
}

message FeatureValue {
  ValueType value_type = 1;
  oneof value {
    bool bool_value = 2;
    int64 int64_value = 3;
    double double_value = 4;
    string string_value = 5;
    FloatVector float_vector_value = 6;
    DoubleVector double_vector_value = 7;
    NullValue null_value = 8;
  }
}

message FeatureValueResult {
  FeatureRef feature = 1;
  FeatureValue value = 2;
  google.protobuf.Timestamp event_time = 3;
  google.protobuf.Timestamp system_time = 4;
  QualityFlags quality_flags = 5;
  bool found = 6;
}

message ErrorDetail {
  ErrorCode code = 1;
  string message = 2;
  string retry_hint = 3;
}

message Pagination {
  uint32 page_size = 1;
  string page_token = 2;
}
```

## 5. Metadata Contract

The metadata service defines features, groups, and policies. It is not used for high-rate data retrieval.

Recommended `metadata.proto` content:

```proto
syntax = "proto3";
package bitemporal.feature.v1;

import "common.proto";
import "google/protobuf/timestamp.proto";

message FeatureDefinition {
  string tenant_id = 1;
  string feature_id = 2;
  string feature_name = 3;
  string entity_type = 4;
  ValueType value_type = 5;
  bool serving_enabled = 6;
  bool historical_enabled = 7;
  bool allow_external_system_time = 8;
  bool nullable = 9;
  string description = 10;
  string owner = 11;
  repeated string tags = 12;
  string retention_policy_id = 13;
  string freshness_sla = 14;
  google.protobuf.Timestamp created_at = 15;
  google.protobuf.Timestamp updated_at = 16;
}

message FeatureGroup {
  string tenant_id = 1;
  string group_id = 2;
  string group_name = 3;
  string entity_type = 4;
  repeated FeatureRef features = 5;
  string description = 6;
  google.protobuf.Timestamp created_at = 7;
  google.protobuf.Timestamp updated_at = 8;
}

message CreateFeatureRequest {
  RequestContext context = 1;
  FeatureDefinition feature = 2;
}

message CreateFeatureResponse {
  FeatureDefinition feature = 1;
}

message UpdateFeatureRequest {
  RequestContext context = 1;
  FeatureDefinition feature = 2;
}

message UpdateFeatureResponse {
  FeatureDefinition feature = 1;
}

message GetFeatureRequest {
  RequestContext context = 1;
  string tenant_id = 2;
  FeatureRef feature = 3;
}

message ListFeaturesRequest {
  RequestContext context = 1;
  string tenant_id = 2;
  string entity_type = 3;
  Pagination pagination = 4;
}

message ListFeaturesResponse {
  repeated FeatureDefinition features = 1;
  string next_page_token = 2;
}

message CreateFeatureGroupRequest {
  RequestContext context = 1;
  FeatureGroup group = 2;
}

message CreateFeatureGroupResponse {
  FeatureGroup group = 1;
}

service MetadataService {
  rpc CreateFeature(CreateFeatureRequest) returns (CreateFeatureResponse);
  rpc UpdateFeature(UpdateFeatureRequest) returns (UpdateFeatureResponse);
  rpc GetFeature(GetFeatureRequest) returns (GetFeatureResponse);
  rpc ListFeatures(ListFeaturesRequest) returns (ListFeaturesResponse);
  rpc CreateFeatureGroup(CreateFeatureGroupRequest) returns (CreateFeatureGroupResponse);
}
```

Metadata semantics:

- feature names are unique within `(tenant_id, entity_type)`
- immutable fields should include `feature_id`, `feature_name`, `entity_type`, and incompatible type changes
- backward-compatible metadata additions are preferred over in-place semantic changes

## 6. Ingestion Contract

The ingestion service owns write admission, validation, temporal stamping, and commit visibility.

Recommended `ingestion.proto` content:

```proto
syntax = "proto3";
package bitemporal.feature.v1;

import "common.proto";
import "google/protobuf/timestamp.proto";

message FeatureEventInput {
  FeatureRef feature = 1;
  google.protobuf.Timestamp event_time = 2;
  google.protobuf.Timestamp system_time = 3;
  FeatureValue value = 4;
  OperationType operation = 5;
  string write_id = 6;
  string source_id = 7;
  QualityFlags quality_flags = 8;
}

message EntityFeatureBatch {
  EntityKey entity = 1;
  repeated FeatureEventInput events = 2;
}

message WriteFeatureEventsRequest {
  RequestContext context = 1;
  repeated EntityFeatureBatch batches = 2;
  DurabilityMode durability_mode = 3;
  bool allow_trusted_system_time = 4;
}

message EntityCommitResult {
  EntityKey entity = 1;
  CommitToken commit = 2;
  uint32 accepted_events = 3;
  repeated ErrorDetail event_errors = 4;
}

message WriteFeatureEventsResponse {
  repeated EntityCommitResult results = 1;
}

message StreamFeatureEventsRequest {
  RequestContext context = 1;
  EntityFeatureBatch batch = 2;
  DurabilityMode durability_mode = 3;
  bool allow_trusted_system_time = 4;
}

message StreamFeatureEventsResponse {
  EntityCommitResult result = 1;
}

service IngestionService {
  rpc WriteFeatureEvents(WriteFeatureEventsRequest) returns (WriteFeatureEventsResponse);
  rpc StreamFeatureEvents(stream StreamFeatureEventsRequest) returns (stream StreamFeatureEventsResponse);
}
```

Ingestion semantics:

- events inside one `EntityFeatureBatch` are atomic together
- ordering inside a batch is preserved for `sequence_no` assignment when timestamps tie
- if `allow_trusted_system_time` is false, any client-supplied `system_time` must be rejected
- if one event in a batch fails validation, the whole entity batch fails
- response `CommitToken.lsn` is the visibility fence for `CONSISTENCY_AT_LEAST_LSN`

Validation rules:

- feature must exist and match `entity_type`
- `event_time` must be present
- value type must match feature definition unless operation is delete
- `write_id` is required for production-grade sources and strongly recommended always

## 7. Online Serving Contract

The online path is optimized for current feature retrieval, with optional freshness and visibility constraints.

Recommended `serving.proto` content:

```proto
syntax = "proto3";
package bitemporal.feature.v1;

import "common.proto";
import "google/protobuf/duration.proto";

message EntityFeatureVector {
  EntityKey entity = 1;
  string feature_group_id = 2;
  repeated FeatureValueResult features = 3;
  CommitToken visible_commit = 4;
}

message GetLatestFeaturesRequest {
  RequestContext context = 1;
  string tenant_id = 2;
  string entity_type = 3;
  repeated string entity_ids = 4;
  repeated FeatureRef features = 5;
  repeated string feature_group_ids = 6;
  google.protobuf.Duration freshness_requirement = 7;
  ConsistencyMode consistency_mode = 8;
  uint64 min_visible_lsn = 9;
}

message GetLatestFeaturesResponse {
  repeated EntityFeatureVector vectors = 1;
}

message SharedMemoryDescriptor {
  string region_name = 1;
  uint64 offset = 2;
  uint64 length = 3;
  uint64 published_version = 4;
  string encoding = 5;
}

message GetSharedMemoryVectorRequest {
  RequestContext context = 1;
  EntityKey entity = 2;
  string feature_group_id = 3;
  uint64 min_visible_lsn = 4;
}

message GetSharedMemoryVectorResponse {
  SharedMemoryDescriptor descriptor = 1;
  CommitToken visible_commit = 2;
}

service OnlineServingService {
  rpc GetLatestFeatures(GetLatestFeaturesRequest) returns (GetLatestFeaturesResponse);
  rpc GetSharedMemoryVector(GetSharedMemoryVectorRequest) returns (GetSharedMemoryVectorResponse);
}
```

Serving semantics:

- caller may specify explicit features, feature groups, or both
- `feature_group_ids` should be preferred for latency-critical serving
- `CONSISTENCY_AT_LEAST_LSN` requires `min_visible_lsn`
- if freshness cannot be satisfied, the service returns `ERROR_STALE_READ` or marks results stale depending on request policy

## 8. Historical Query Contract

The historical service handles as-of lookups, window scans, and long-running dataset builds.

Recommended `historical.proto` content:

```proto
syntax = "proto3";
package bitemporal.feature.v1;

import "common.proto";
import "google/protobuf/timestamp.proto";

message AsOfLookupRow {
  EntityKey entity = 1;
  repeated FeatureRef features = 2;
  google.protobuf.Timestamp event_cutoff = 3;
  google.protobuf.Timestamp system_cutoff = 4;
}

message AsOfLookupRequest {
  RequestContext context = 1;
  repeated AsOfLookupRow rows = 2;
  SnapshotToken snapshot = 3;
}

message AsOfLookupResult {
  EntityKey entity = 1;
  google.protobuf.Timestamp event_cutoff = 2;
  google.protobuf.Timestamp system_cutoff = 3;
  repeated FeatureValueResult features = 4;
}

message AsOfLookupResponse {
  repeated AsOfLookupResult results = 1;
}

message WindowScanRequest {
  RequestContext context = 1;
  EntityKey entity = 2;
  FeatureRef feature = 3;
  google.protobuf.Timestamp event_start = 4;
  google.protobuf.Timestamp event_end = 5;
  google.protobuf.Timestamp system_cutoff = 6;
  SnapshotToken snapshot = 7;
}

message WindowScanResponse {
  repeated FeatureValueResult values = 1;
}

message DrivingTableRef {
  string uri = 1;
  string format = 2;
  string row_id_column = 3;
  string entity_id_column = 4;
  string event_time_column = 5;
  string system_time_column = 6;
}

message BuildTrainingDatasetRequest {
  RequestContext context = 1;
  string tenant_id = 2;
  string entity_type = 3;
  DrivingTableRef driving_table = 4;
  repeated FeatureRef features = 5;
  google.protobuf.Timestamp default_system_cutoff = 6;
  OutputFormat output_format = 7;
  string output_uri = 8;
  SnapshotToken snapshot = 9;
}

message BuildTrainingDatasetResponse {
  string job_id = 1;
  JobState state = 2;
}

message GetDatasetJobRequest {
  RequestContext context = 1;
  string job_id = 2;
}

message DatasetJobStatus {
  string job_id = 1;
  JobState state = 2;
  SnapshotToken snapshot = 3;
  uint64 rows_processed = 4;
  uint64 rows_output = 5;
  string output_uri = 6;
  ErrorDetail error = 7;
}

message CancelDatasetJobRequest {
  RequestContext context = 1;
  string job_id = 2;
}

service HistoricalQueryService {
  rpc AsOfLookup(AsOfLookupRequest) returns (AsOfLookupResponse);
  rpc WindowScan(WindowScanRequest) returns (WindowScanResponse);
  rpc BuildTrainingDataset(BuildTrainingDatasetRequest) returns (BuildTrainingDatasetResponse);
  rpc GetDatasetJob(GetDatasetJobRequest) returns (DatasetJobStatus);
  rpc CancelDatasetJob(CancelDatasetJobRequest) returns (google.protobuf.Empty);
}
```

Historical semantics:

- `snapshot` is optional; if omitted, the server chooses a valid snapshot and returns it in job status or headers
- dataset jobs are asynchronous by default
- `system_cutoff` is explicit per row when supplied; otherwise `default_system_cutoff` applies
- if neither is supplied, the server must reject the request rather than guessing

## 9. Admin Contract

The admin surface manages health, maintenance operations, and operational state.

Recommended `admin.proto` content:

```proto
syntax = "proto3";
package bitemporal.feature.v1;

import "common.proto";
import "google/protobuf/empty.proto";
import "google/protobuf/timestamp.proto";

message HealthStatus {
  HealthState state = 1;
  uint64 current_lsn = 2;
  uint64 manifest_version = 3;
  bool read_only = 4;
  repeated string warnings = 5;
}

message TriggerCheckpointRequest {
  RequestContext context = 1;
  bool wait = 2;
}

message TriggerCheckpointResponse {
  uint64 checkpoint_lsn = 1;
  google.protobuf.Timestamp completed_at = 2;
}

message CompactionStatus {
  uint32 queued_tasks = 1;
  uint32 running_tasks = 2;
  uint64 bytes_pending = 3;
  google.protobuf.Timestamp last_success_at = 4;
}

message BackupRequest {
  RequestContext context = 1;
  string destination_uri = 2;
}

message BackupResponse {
  string backup_id = 1;
  JobState state = 2;
}

message RestoreRequest {
  RequestContext context = 1;
  string source_uri = 2;
  bool start_read_only = 3;
}

message SetReadOnlyModeRequest {
  RequestContext context = 1;
  bool read_only = 2;
}

service AdminService {
  rpc GetHealth(google.protobuf.Empty) returns (HealthStatus);
  rpc TriggerCheckpoint(TriggerCheckpointRequest) returns (TriggerCheckpointResponse);
  rpc GetCompactionStatus(google.protobuf.Empty) returns (CompactionStatus);
  rpc StartBackup(BackupRequest) returns (BackupResponse);
  rpc RestoreBackup(RestoreRequest) returns (BackupResponse);
  rpc SetReadOnlyMode(SetReadOnlyModeRequest) returns (google.protobuf.Empty);
}
```

## 10. Error and Retry Model

API errors should use:

- gRPC status codes for transport-level categories
- `ErrorDetail` in response payloads or rich status details for engine-specific meaning

Mapping guidance:

- invalid schema or type mismatch -> `INVALID_ARGUMENT`
- auth failure -> `UNAUTHENTICATED` or `PERMISSION_DENIED`
- overload or backpressure -> `UNAVAILABLE` with `ERROR_OVERLOADED`
- stale read under strict freshness -> `FAILED_PRECONDITION` with `ERROR_STALE_READ`
- missing feature or entity -> `NOT_FOUND`

Retry guidance:

- safe to retry idempotent writes with the same `write_id`
- safe to retry reads unless caller requires a pinned snapshot that has expired
- dataset jobs should return stable `job_id` on duplicate submission only if a request-id dedupe layer is added later

## 11. Authentication and Request Metadata

Required metadata on every RPC:

- `authorization`
- `x-request-id`
- `x-trace-id`

Optional:

- `x-caller`
- `x-tenant-id` as a routing hint, but never as the only tenant field

Authorization rules should be evaluated against:

- tenant
- operation type
- feature definition or feature group

## 12. Contract Gaps Intentionally Deferred

These details are intentionally left for later releases:

- streaming Arrow result transport over gRPC
- SQL-over-gRPC surface
- push-based cache invalidation subscriptions
- distributed read routing metadata
- remote execution protocol for cross-shard historical jobs

Those features should extend the contract, not rewrite the semantics above.
