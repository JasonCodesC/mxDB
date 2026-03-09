#include <cassert>
#include <chrono>
#include <filesystem>
#include <string>

#include "engine/catalog/metadata_store.h"
#include "engine/storage/feature_engine.h"

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

mxdb::FeatureDefinition MakeFeature() {
  mxdb::FeatureDefinition feature;
  feature.tenant_id = "prod";
  feature.feature_id = "f_price";
  feature.feature_name = "price";
  feature.entity_type = "instrument";
  feature.value_type = mxdb::ValueType::kDouble;
  feature.allow_external_system_time = true;
  feature.nullable = false;
  feature.description = "price";
  feature.owner = "team";
  feature.created_at_us = NowMicros();
  feature.updated_at_us = feature.created_at_us;
  return feature;
}

mxdb::FeatureEventInput MakeInput(const std::string& write_id, double value,
                                  mxdb::TimestampMicros event_time) {
  mxdb::FeatureEventInput event;
  event.feature_id = "f_price";
  event.event_time_us = event_time;
  event.system_time_us = event_time;
  event.value = {.type = mxdb::ValueType::kDouble, .value = value};
  event.operation = mxdb::OperationType::kUpsert;
  event.write_id = write_id;
  event.source_id = "batch-atomicity-test";
  return event;
}

double ReadLatestPrice(mxdb::FeatureEngine* engine) {
  auto latest = engine->GetLatest(
      {.tenant_id = "prod", .entity_type = "instrument", .entity_id = "AAPL"},
      {"f_price"});
  assert(latest.ok());
  assert(latest.value().features.size() == 1);
  assert(latest.value().features[0].found);
  return std::get<double>(latest.value().features[0].value.value);
}

}  // namespace

int main() {
  const auto tmp = UniqueTmpDir("batch-atomicity");

  mxdb::EngineConfig config;
  config.data_dir = tmp.string();
  config.metadata_path = (tmp / "catalog" / "metadata.db").string();
  config.wal_dir = (tmp / "wal").string();
  config.segment_dir = (tmp / "segments").string();
  config.manifest_path = (tmp / "manifest" / "manifest.log").string();
  config.checkpoint_path = (tmp / "checkpoints" / "checkpoint.meta").string();

  mxdb::MetadataStore metadata;
  mxdb::Status status = metadata.Open(config.metadata_path);
  assert(status.ok());
  status = metadata.CreateFeature(MakeFeature());
  assert(status.ok());

  mxdb::FeatureEngine engine(config, &metadata);
  status = engine.Start();
  assert(status.ok());

  mxdb::EntityFeatureBatch first;
  first.entity = {
      .tenant_id = "prod", .entity_type = "instrument", .entity_id = "AAPL"};
  first.events.push_back(MakeInput("w1", 10.0, 100));
  auto first_result =
      engine.WriteEntityBatch(first, mxdb::DurabilityMode::kSync, true);
  assert(first_result.ok());
  assert(first_result.value().accepted_events == 1);
  assert(ReadLatestPrice(&engine) == 10.0);

  // Mixed duplicate/new batch must not partially commit.
  mxdb::EntityFeatureBatch mixed;
  mixed.entity = first.entity;
  mixed.events.push_back(MakeInput("w1", 20.0, 200));  // duplicate
  mixed.events.push_back(MakeInput("w2", 30.0, 300));  // new
  auto mixed_result =
      engine.WriteEntityBatch(mixed, mxdb::DurabilityMode::kSync, true);
  assert(mixed_result.ok());
  assert(mixed_result.value().accepted_events == 0);
  assert(ReadLatestPrice(&engine) == 10.0);

  // Internal duplicate IDs in one batch must be rejected.
  mxdb::EntityFeatureBatch internal_dup;
  internal_dup.entity = first.entity;
  internal_dup.events.push_back(MakeInput("dup-in-batch", 40.0, 400));
  internal_dup.events.push_back(MakeInput("dup-in-batch", 50.0, 500));
  auto internal_dup_result =
      engine.WriteEntityBatch(internal_dup, mxdb::DurabilityMode::kSync, true);
  assert(!internal_dup_result.ok());
  assert(internal_dup_result.status().code() == mxdb::StatusCode::kInvalidArgument);
  assert(ReadLatestPrice(&engine) == 10.0);

  status = engine.Stop();
  assert(status.ok());
  status = metadata.Close();
  assert(status.ok());
  std::filesystem::remove_all(tmp);
  return 0;
}
