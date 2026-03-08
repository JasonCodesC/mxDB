#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

namespace mxdb {

using TimestampMicros = int64_t;
using Lsn = uint64_t;

enum class ValueType {
  kUnspecified = 0,
  kBool = 1,
  kInt64 = 2,
  kDouble = 3,
  kString = 4,
  kFloatVector = 5,
  kDoubleVector = 6,
};

enum class OperationType {
  kUnspecified = 0,
  kUpsert = 1,
  kDelete = 2,
};

enum class DurabilityMode {
  kUnspecified = 0,
  kSync = 1,
  kGroupCommit = 2,
  kAsync = 3,
};

struct EntityKey {
  std::string tenant_id;
  std::string entity_type;
  std::string entity_id;

  bool operator==(const EntityKey& other) const {
    return tenant_id == other.tenant_id && entity_type == other.entity_type &&
           entity_id == other.entity_id;
  }

  bool operator<(const EntityKey& other) const {
    return std::tie(tenant_id, entity_type, entity_id) <
           std::tie(other.tenant_id, other.entity_type, other.entity_id);
  }
};

struct EntityKeyHash {
  std::size_t operator()(const EntityKey& key) const {
    std::size_t h1 = std::hash<std::string>{}(key.tenant_id);
    std::size_t h2 = std::hash<std::string>{}(key.entity_type);
    std::size_t h3 = std::hash<std::string>{}(key.entity_id);
    return h1 ^ (h2 << 1U) ^ (h3 << 2U);
  }
};

struct EntityFeatureKey {
  EntityKey entity;
  std::string feature_id;

  bool operator==(const EntityFeatureKey& other) const {
    return entity == other.entity && feature_id == other.feature_id;
  }
};

struct EntityFeatureKeyHash {
  std::size_t operator()(const EntityFeatureKey& key) const {
    std::size_t h1 = EntityKeyHash{}(key.entity);
    std::size_t h2 = std::hash<std::string>{}(key.feature_id);
    return h1 ^ (h2 << 1U);
  }
};

struct FeatureValue {
  using Variant = std::variant<std::monostate, bool, int64_t, double, std::string,
                               std::vector<float>, std::vector<double>>;

  ValueType type = ValueType::kUnspecified;
  Variant value{};

  static FeatureValue Null(ValueType value_type) {
    FeatureValue v;
    v.type = value_type;
    v.value = std::monostate{};
    return v;
  }

  bool IsNull() const { return std::holds_alternative<std::monostate>(value); }
};

struct FeatureDefinition {
  std::string tenant_id;
  std::string feature_id;
  std::string feature_name;
  std::string entity_type;
  ValueType value_type = ValueType::kUnspecified;
  bool serving_enabled = true;
  bool historical_enabled = true;
  bool allow_external_system_time = false;
  bool nullable = true;
  std::string description;
  std::string owner;
  std::vector<std::string> tags;
  std::string retention_policy_id;
  std::string freshness_sla;
  TimestampMicros created_at_us = 0;
  TimestampMicros updated_at_us = 0;
};

struct FeatureGroup {
  std::string tenant_id;
  std::string group_id;
  std::string group_name;
  std::string entity_type;
  std::vector<std::string> feature_ids;
  std::string description;
  TimestampMicros created_at_us = 0;
  TimestampMicros updated_at_us = 0;
};

struct FeatureEventInput {
  std::string feature_id;
  TimestampMicros event_time_us = 0;
  std::optional<TimestampMicros> system_time_us;
  FeatureValue value;
  OperationType operation = OperationType::kUpsert;
  std::string write_id;
  std::string source_id;
};

struct FeatureEvent {
  EntityKey entity;
  std::string feature_id;
  TimestampMicros event_time_us = 0;
  TimestampMicros system_time_us = 0;
  uint64_t sequence_no = 0;
  OperationType operation = OperationType::kUpsert;
  FeatureValue value;
  std::string write_id;
  std::string source_id;
  Lsn lsn = 0;
};

struct EntityFeatureBatch {
  EntityKey entity;
  std::vector<FeatureEventInput> events;
};

struct CommitToken {
  Lsn lsn = 0;
  TimestampMicros commit_system_time_us = 0;
};

struct EntityCommitResult {
  EntityKey entity;
  CommitToken commit;
  uint32_t accepted_events = 0;
  std::string error;
};

struct AsOfLookupInput {
  EntityKey entity;
  std::vector<std::string> feature_ids;
  TimestampMicros event_cutoff_us = 0;
  TimestampMicros system_cutoff_us = 0;
};

struct FeatureValueResult {
  std::string feature_id;
  FeatureValue value;
  TimestampMicros event_time_us = 0;
  TimestampMicros system_time_us = 0;
  bool found = false;
};

struct AsOfLookupResult {
  EntityKey entity;
  TimestampMicros event_cutoff_us = 0;
  TimestampMicros system_cutoff_us = 0;
  std::vector<FeatureValueResult> features;
};

struct DrivingRow {
  std::string row_id;
  EntityKey entity;
  TimestampMicros label_time_us = 0;
  std::optional<TimestampMicros> system_cutoff_us;
};

using PitJoinRow = std::unordered_map<std::string, FeatureValueResult>;

}  // namespace mxdb
