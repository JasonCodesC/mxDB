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

mxdb::EntityFeatureBatch MakeEvent(const std::string& write_id, double value,
                                   mxdb::TimestampMicros event_time,
                                   mxdb::TimestampMicros system_time) {
  mxdb::EntityFeatureBatch batch;
  batch.entity = {.tenant_id = "prod", .entity_type = "instrument", .entity_id = "AAPL"};
  mxdb::FeatureEventInput event;
  event.feature_id = "f_price";
  event.event_time_us = event_time;
  event.system_time_us = system_time;
  event.value = {.type = mxdb::ValueType::kDouble, .value = value};
  event.operation = mxdb::OperationType::kUpsert;
  event.write_id = write_id;
  event.source_id = "lifecycle-test";
  batch.events.push_back(std::move(event));
  return batch;
}

void ExpectReadFailedPrecondition(mxdb::FeatureEngine* engine) {
  auto latest = engine->GetLatest(
      {.tenant_id = "prod", .entity_type = "instrument", .entity_id = "AAPL"},
      {"f_price"});
  assert(!latest.ok());
  assert(latest.status().code() == mxdb::StatusCode::kFailedPrecondition);

  auto latest_events = engine->GetLatestEvents(
      {.tenant_id = "prod", .entity_type = "instrument", .entity_id = "AAPL"},
      "f_price", 5);
  assert(!latest_events.ok());
  assert(latest_events.status().code() == mxdb::StatusCode::kFailedPrecondition);

  auto as_of = engine->AsOfLookup(
      {.entity = {.tenant_id = "prod", .entity_type = "instrument", .entity_id = "AAPL"},
       .feature_ids = {"f_price"},
       .event_cutoff_us = 999,
       .system_cutoff_us = 999});
  assert(!as_of.ok());
  assert(as_of.status().code() == mxdb::StatusCode::kFailedPrecondition);
}

}  // namespace

int main() {
  const auto tmp = UniqueTmpDir("engine-lifecycle");

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

  ExpectReadFailedPrecondition(&engine);

  status = engine.Start();
  assert(status.ok());

  auto write = engine.WriteEntityBatch(MakeEvent("w1", 10.0, 100, 100),
                                       mxdb::DurabilityMode::kSync, true);
  assert(write.ok());
  assert(write.value().accepted_events == 1);

  status = engine.Stop();
  assert(status.ok());

  ExpectReadFailedPrecondition(&engine);

  status = metadata.Close();
  assert(status.ok());
  std::filesystem::remove_all(tmp);
  return 0;
}
