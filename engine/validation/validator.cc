#include "engine/validation/validator.h"

#include <variant>

namespace mxdb {

namespace {

bool IsValueTypeConfigured(ValueType value_type) {
  return value_type != ValueType::kUnspecified;
}

}  // namespace

Status Validator::ValidateFeatureDefinition(const FeatureDefinition& feature) const {
  if (feature.tenant_id.empty()) {
    return Status::InvalidArgument("tenant_id is required");
  }
  if (feature.feature_id.empty()) {
    return Status::InvalidArgument("feature_id is required");
  }
  if (feature.feature_name.empty()) {
    return Status::InvalidArgument("feature_name is required");
  }
  if (feature.entity_type.empty()) {
    return Status::InvalidArgument("entity_type is required");
  }
  if (!IsValueTypeConfigured(feature.value_type)) {
    return Status::InvalidArgument("value_type is required");
  }
  return Status::Ok();
}

StatusOr<std::unordered_map<std::string, FeatureDefinition>>
Validator::ValidateWriteBatch(const EntityFeatureBatch& batch,
                              bool allow_trusted_system_time) const {
  if (batch.entity.tenant_id.empty() || batch.entity.entity_type.empty() ||
      batch.entity.entity_id.empty()) {
    return Status::InvalidArgument("entity tenant/entity_type/entity_id are required");
  }
  if (batch.events.empty()) {
    return Status::InvalidArgument("entity batch must include at least one event");
  }

  std::unordered_map<std::string, FeatureDefinition> definitions;

  for (const auto& event : batch.events) {
    if (event.feature_id.empty()) {
      return Status::InvalidArgument("feature_id is required for event");
    }
    if (event.event_time_us <= 0) {
      return Status::InvalidArgument("event_time_us must be > 0");
    }
    if (event.write_id.empty()) {
      return Status::InvalidArgument("write_id is required");
    }

    if (event.system_time_us.has_value() && !allow_trusted_system_time) {
      return Status::InvalidArgument(
          "client system_time_us rejected when allow_trusted_system_time=false");
    }

    auto it = definitions.find(event.feature_id);
    if (it == definitions.end()) {
      auto definition =
          metadata_store_.GetFeatureById(batch.entity.tenant_id, event.feature_id);
      if (!definition.ok()) {
        return definition.status();
      }

      if (definition.value().entity_type != batch.entity.entity_type) {
        return Status::InvalidArgument(
            "feature entity_type does not match entity batch entity_type");
      }

      definitions.emplace(event.feature_id, definition.value());
      it = definitions.find(event.feature_id);
    }

    const FeatureDefinition& def = it->second;
    if (event.system_time_us.has_value() && def.allow_external_system_time == false) {
      return Status::PermissionDenied(
          "feature does not allow external system_time");
    }

    if (event.operation != OperationType::kDelete &&
        !ValueMatchesType(event.value, def.value_type, def.nullable)) {
      return Status::InvalidArgument(
          "feature value does not match metadata value_type/nullability");
    }
  }

  return definitions;
}

bool ValueMatchesType(const FeatureValue& value, ValueType expected_type,
                     bool nullable) {
  if (value.IsNull()) {
    return nullable;
  }

  switch (expected_type) {
    case ValueType::kBool:
      return std::holds_alternative<bool>(value.value);
    case ValueType::kInt64:
      return std::holds_alternative<int64_t>(value.value);
    case ValueType::kDouble:
      return std::holds_alternative<double>(value.value);
    case ValueType::kString:
      return std::holds_alternative<std::string>(value.value);
    case ValueType::kFloatVector:
      return std::holds_alternative<std::vector<float>>(value.value);
    case ValueType::kDoubleVector:
      return std::holds_alternative<std::vector<double>>(value.value);
    case ValueType::kUnspecified:
      return false;
  }
  return false;
}

}  // namespace mxdb
