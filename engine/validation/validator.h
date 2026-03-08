#pragma once

#include <string>
#include <unordered_map>

#include "engine/catalog/metadata_store.h"
#include "engine/common/status/status.h"
#include "engine/types/types.h"

namespace mxdb {

class Validator {
 public:
  explicit Validator(const MetadataStore& metadata_store)
      : metadata_store_(metadata_store) {}

  Status ValidateFeatureDefinition(const FeatureDefinition& feature) const;

  StatusOr<std::unordered_map<std::string, FeatureDefinition>> ValidateWriteBatch(
      const EntityFeatureBatch& batch, bool allow_trusted_system_time) const;

 private:
  const MetadataStore& metadata_store_;
};

bool ValueMatchesType(const FeatureValue& value, ValueType expected_type,
                     bool nullable);

}  // namespace mxdb
