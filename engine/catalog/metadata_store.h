#pragma once

#include <sqlite3.h>

#include <optional>
#include <cstddef>
#include <string>
#include <vector>

#include "engine/common/status/status.h"
#include "engine/types/types.h"

namespace mxdb {

class MetadataStore {
 public:
  MetadataStore() = default;
  ~MetadataStore();

  MetadataStore(const MetadataStore&) = delete;
  MetadataStore& operator=(const MetadataStore&) = delete;

  Status Open(const std::string& sqlite_path);
  Status Close();
  void InjectCloseFailureForTest(size_t count = 1);
  Status RunMigrations();

  Status CreateFeature(const FeatureDefinition& feature);
  Status UpdateFeature(const FeatureDefinition& feature);
  StatusOr<FeatureDefinition> GetFeatureById(const std::string& tenant_id,
                                             const std::string& feature_id) const;
  StatusOr<FeatureDefinition> GetFeatureByName(
      const std::string& tenant_id, const std::string& entity_type,
      const std::string& feature_name) const;
  StatusOr<std::vector<FeatureDefinition>> ListFeatures(
      const std::string& tenant_id, std::optional<std::string> entity_type) const;

  Status CreateFeatureGroup(const FeatureGroup& group);
  StatusOr<FeatureGroup> GetFeatureGroup(const std::string& tenant_id,
                                         const std::string& group_id) const;

 private:
  sqlite3* db_ = nullptr;
  size_t fail_close_for_test_count_ = 0;

  static Status SqliteError(sqlite3* db, const std::string& prefix);
  static std::string EncodeTags(const std::vector<std::string>& tags);
  static std::vector<std::string> DecodeTags(const std::string& tags);
  static Status ValidateFeatureDefinitionForPersistence(
      const FeatureDefinition& feature);
};

std::string ToString(ValueType value_type);
StatusOr<ValueType> ParseValueType(const std::string& value_type);

}  // namespace mxdb
