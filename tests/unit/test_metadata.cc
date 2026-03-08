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
