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

mxdb::EntityFeatureBatch OneEvent(const std::string& write_id,
                                  mxdb::OperationType operation,
                                  double value,
                                  mxdb::TimestampMicros event_time,
                                  mxdb::TimestampMicros system_time) {
  mxdb::EntityFeatureBatch batch;
  batch.entity = {.tenant_id = "prod", .entity_type = "instrument", .entity_id = "AAPL"};
  mxdb::FeatureEventInput event;
  event.feature_id = "f_price";
  event.event_time_us = event_time;
  event.system_time_us = system_time;
  event.value = {.type = mxdb::ValueType::kDouble, .value = value};
  event.operation = operation;
  event.write_id = write_id;
  event.source_id = "test";
  batch.events.push_back(std::move(event));
  return batch;
}

}  // namespace

int main() {
  const auto tmp = UniqueTmpDir("temporal");

  mxdb::EngineConfig config;
  config.data_dir = tmp.string();
  config.metadata_path = (tmp / "catalog" / "metadata.db").string();
  config.wal_dir = (tmp / "wal").string();
  config.partition_count = 2;

  mxdb::MetadataStore metadata;
  mxdb::Status status = metadata.Open(config.metadata_path);
  assert(status.ok());

  mxdb::FeatureDefinition f;
  f.tenant_id = "prod";
  f.feature_id = "f_price";
  f.feature_name = "price";
  f.entity_type = "instrument";
  f.value_type = mxdb::ValueType::kDouble;
  f.serving_enabled = true;
  f.historical_enabled = true;
  f.allow_external_system_time = true;
  f.nullable = false;
  f.description = "price";
  f.owner = "team";
  f.created_at_us = NowMicros();
  f.updated_at_us = f.created_at_us;

  status = metadata.CreateFeature(f);
  assert(status.ok());

  mxdb::FeatureEngine engine(config, &metadata);
  status = engine.Start();
  assert(status.ok());

  auto r1 = engine.WriteEntityBatch(
      OneEvent("w1", mxdb::OperationType::kUpsert, 1.0, 100, 110),
      mxdb::DurabilityMode::kSync, true);
  assert(r1.ok());
  assert(r1.value().accepted_events == 1);

  auto r2 = engine.WriteEntityBatch(
      OneEvent("w2", mxdb::OperationType::kUpsert, 2.0, 100, 120),
      mxdb::DurabilityMode::kSync, true);
  assert(r2.ok());

  auto r3 = engine.WriteEntityBatch(
      OneEvent("w3", mxdb::OperationType::kUpsert, 3.0, 130, 130),
      mxdb::DurabilityMode::kSync, true);
  assert(r3.ok());

  mxdb::AsOfLookupInput q1;
  q1.entity = {.tenant_id = "prod", .entity_type = "instrument", .entity_id = "AAPL"};
  q1.feature_ids = {"f_price"};
  q1.event_cutoff_us = 105;
  q1.system_cutoff_us = 115;

  auto a1 = engine.AsOfLookup(q1);
  assert(a1.ok());
  assert(a1.value().features.size() == 1);
  assert(a1.value().features[0].found);
  assert(std::get<double>(a1.value().features[0].value.value) == 1.0);

  q1.system_cutoff_us = 125;
  auto a2 = engine.AsOfLookup(q1);
  assert(a2.ok());
  assert(a2.value().features[0].found);
  assert(std::get<double>(a2.value().features[0].value.value) == 2.0);

  q1.event_cutoff_us = 140;
  q1.system_cutoff_us = 200;
  auto a3 = engine.AsOfLookup(q1);
  assert(a3.ok());
  assert(a3.value().features[0].found);
  assert(std::get<double>(a3.value().features[0].value.value) == 3.0);

  auto r4 = engine.WriteEntityBatch(
      OneEvent("w4", mxdb::OperationType::kDelete, 0.0, 130, 140),
      mxdb::DurabilityMode::kSync, true);
  assert(r4.ok());

  q1.event_cutoff_us = 140;
  q1.system_cutoff_us = 150;
  auto deleted = engine.AsOfLookup(q1);
  assert(deleted.ok());
  assert(!deleted.value().features[0].found);

  auto latest = engine.GetLatest(
      {.tenant_id = "prod", .entity_type = "instrument", .entity_id = "AAPL"},
      {"f_price"});
  assert(latest.ok());
  assert(latest.value().features.size() == 1);
  assert(!latest.value().features[0].found);

  status = engine.Stop();
  assert(status.ok());
  status = metadata.Close();
  assert(status.ok());

  std::filesystem::remove_all(tmp);
  return 0;
}
