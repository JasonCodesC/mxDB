#include <cassert>
#include <chrono>
#include <filesystem>
#include <string>

#include "engine/catalog/metadata_store.h"
#include "engine/validation/validator.h"

namespace {

mxdb::TimestampMicros NowMicros() {
  const auto now = std::chrono::system_clock::now();
  return std::chrono::duration_cast<std::chrono::microseconds>(
             now.time_since_epoch())
      .count();
}

std::filesystem::path UniqueTmpDir(const std::string& name) {
  const auto base = std::filesystem::temp_directory_path();
  const auto stamp = std::to_string(NowMicros());
  std::filesystem::path path = base / ("mxdb-" + name + "-" + stamp);
  std::filesystem::create_directories(path);
  return path;
}

}  // namespace

int main() {
  const auto tmp = UniqueTmpDir("metadata");
  const auto db_path = (tmp / "metadata.db").string();

  mxdb::MetadataStore store;
  mxdb::Status status = store.Open(db_path);
  assert(status.ok());

  mxdb::FeatureDefinition f;
  f.tenant_id = "prod";
  f.feature_id = "f_price";
  f.feature_name = "price";
  f.entity_type = "instrument";
  f.value_type = mxdb::ValueType::kDouble;
  f.serving_enabled = true;
  f.historical_enabled = true;
  f.allow_external_system_time = false;
  f.nullable = false;
  f.owner = "team";
  f.description = "instrument price";
  f.tags = {"market", "price"};
  f.retention_policy_id = "default";
  f.freshness_sla = "1000ms";
  f.created_at_us = NowMicros();
  f.updated_at_us = f.created_at_us;

  mxdb::Validator validator(store);
  status = validator.ValidateFeatureDefinition(f);
  assert(status.ok());

  // Direct MetadataStore callers must be validated too.
  {
    mxdb::FeatureDefinition invalid = f;
    invalid.feature_id = "f_bad_unspecified";
    invalid.feature_name = "bad_unspecified";
    invalid.value_type = mxdb::ValueType::kUnspecified;
    status = store.CreateFeature(invalid);
    assert(!status.ok());
    assert(status.code() == mxdb::StatusCode::kInvalidArgument);
  }
  {
    mxdb::FeatureDefinition invalid = f;
    invalid.tenant_id = "";
    invalid.feature_id = "f_bad_tenant";
    invalid.feature_name = "bad_tenant";
    status = store.CreateFeature(invalid);
    assert(!status.ok());
    assert(status.code() == mxdb::StatusCode::kInvalidArgument);
  }
  {
    mxdb::FeatureDefinition invalid = f;
    invalid.feature_id = "";
    invalid.feature_name = "bad_feature_id";
    status = store.CreateFeature(invalid);
    assert(!status.ok());
    assert(status.code() == mxdb::StatusCode::kInvalidArgument);
  }
  {
    mxdb::FeatureDefinition invalid = f;
    invalid.feature_id = "f_bad_name";
    invalid.feature_name = "";
    status = store.CreateFeature(invalid);
    assert(!status.ok());
    assert(status.code() == mxdb::StatusCode::kInvalidArgument);
  }
  {
    mxdb::FeatureDefinition invalid = f;
    invalid.feature_id = "f_bad_entity";
    invalid.feature_name = "bad_entity";
    invalid.entity_type = "";
    status = store.CreateFeature(invalid);
    assert(!status.ok());
    assert(status.code() == mxdb::StatusCode::kInvalidArgument);
  }

  status = store.CreateFeature(f);
  assert(status.ok());

  auto loaded = store.GetFeatureById("prod", "f_price");
  assert(loaded.ok());
  assert(loaded.value().feature_name == "price");
  assert(loaded.value().entity_type == "instrument");

  auto by_name = store.GetFeatureByName("prod", "instrument", "price");
  assert(by_name.ok());
  assert(by_name.value().feature_id == "f_price");

  auto listed = store.ListFeatures("prod", std::nullopt);
  assert(listed.ok());
  assert(listed.value().size() == 1);

  f.description = "instrument last trade";
  f.updated_at_us = NowMicros();
  status = store.UpdateFeature(f);
  assert(status.ok());

  auto updated = store.GetFeatureById("prod", "f_price");
  assert(updated.ok());
  assert(updated.value().description == "instrument last trade");

  mxdb::FeatureDefinition invalid_update = f;
  invalid_update.feature_name = "";
  invalid_update.updated_at_us = NowMicros();
  status = store.UpdateFeature(invalid_update);
  assert(!status.ok());
  assert(status.code() == mxdb::StatusCode::kInvalidArgument);
  auto unchanged_after_invalid_update = store.GetFeatureById("prod", "f_price");
  assert(unchanged_after_invalid_update.ok());
  assert(unchanged_after_invalid_update.value().description ==
         "instrument last trade");

  mxdb::FeatureDefinition other_entity_feature = f;
  other_entity_feature.feature_id = "f_portfolio_price";
  other_entity_feature.feature_name = "portfolio_price";
  other_entity_feature.entity_type = "portfolio";
  other_entity_feature.created_at_us = NowMicros();
  other_entity_feature.updated_at_us = other_entity_feature.created_at_us;
  status = store.CreateFeature(other_entity_feature);
  assert(status.ok());

  mxdb::FeatureGroup group;
  group.tenant_id = "prod";
  group.group_id = "g_serving";
  group.group_name = "serving";
  group.entity_type = "instrument";
  group.feature_ids = {"f_price"};
  group.description = "serving features";
  group.created_at_us = NowMicros();
  group.updated_at_us = group.created_at_us;

  status = store.CreateFeatureGroup(group);
  assert(status.ok());

  auto loaded_group = store.GetFeatureGroup("prod", "g_serving");
  assert(loaded_group.ok());
  assert(loaded_group.value().feature_ids.size() == 1);
  assert(loaded_group.value().feature_ids[0] == "f_price");

  mxdb::FeatureGroup invalid_group = group;
  invalid_group.group_id = "g_invalid";
  invalid_group.group_name = "invalid";
  invalid_group.feature_ids = {"missing_feature"};
  invalid_group.created_at_us = NowMicros();
  invalid_group.updated_at_us = invalid_group.created_at_us;
  status = store.CreateFeatureGroup(invalid_group);
  assert(!status.ok());

  mxdb::FeatureGroup mismatched_group = group;
  mismatched_group.group_id = "g_mismatch";
  mismatched_group.group_name = "mismatch";
  mismatched_group.feature_ids = {"f_price", "f_portfolio_price"};
  mismatched_group.created_at_us = NowMicros();
  mismatched_group.updated_at_us = mismatched_group.created_at_us;
  status = store.CreateFeatureGroup(mismatched_group);
  assert(!status.ok());

  status = store.Close();
  assert(status.ok());

  status = store.Open(db_path);
  assert(status.ok());

  auto reopened_group = store.GetFeatureGroup("prod", "g_serving");
  assert(reopened_group.ok());
  assert(reopened_group.value().feature_ids.size() == 1);
  assert(reopened_group.value().feature_ids[0] == "f_price");

  auto missing_group = store.GetFeatureGroup("prod", "g_invalid");
  assert(!missing_group.ok());
  assert(missing_group.status().code() == mxdb::StatusCode::kNotFound);

  mxdb::EntityFeatureBatch batch;
  batch.entity = {.tenant_id = "prod", .entity_type = "instrument", .entity_id = "AAPL"};
  batch.events.push_back({
      .feature_id = "f_price",
      .event_time_us = NowMicros(),
      .system_time_us = std::nullopt,
      .value = {.type = mxdb::ValueType::kDouble, .value = 123.45},
      .operation = mxdb::OperationType::kUpsert,
      .write_id = "w1",
      .source_id = "src",
  });

  auto validation = validator.ValidateWriteBatch(batch, false);
  assert(validation.ok());

  batch.events[0].value = {.type = mxdb::ValueType::kString,
                           .value = std::string("bad")};
  validation = validator.ValidateWriteBatch(batch, false);
  assert(!validation.ok());

  status = store.Close();
  assert(status.ok());

  std::filesystem::remove_all(tmp);
  return 0;
}
