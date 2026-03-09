#include <cassert>
#include <chrono>
#include <filesystem>
#include <string>

#include "engine/admin/admin_service.h"
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
  batch.entity = {
      .tenant_id = "prod", .entity_type = "instrument", .entity_id = "AAPL"};

  mxdb::FeatureEventInput event;
  event.feature_id = "f_price";
  event.event_time_us = event_time;
  event.system_time_us = system_time;
  event.value = {.type = mxdb::ValueType::kDouble, .value = value};
  event.operation = mxdb::OperationType::kUpsert;
  event.write_id = write_id;
  event.source_id = "restore-test";
  batch.events.push_back(std::move(event));
  return batch;
}

double ReadLatestPrice(mxdb::FeatureEngine* engine) {
  auto latest = engine->GetLatest({.tenant_id = "prod",
                                   .entity_type = "instrument",
                                   .entity_id = "AAPL"},
                                  {"f_price"});
  assert(latest.ok());
  assert(latest.value().features.size() == 1);
  assert(latest.value().features[0].found);
  return std::get<double>(latest.value().features[0].value.value);
}

}  // namespace

int main() {
  const auto tmp = UniqueTmpDir("restore-correctness");
  const auto backup_dir = tmp / "backup";

  mxdb::EngineConfig config;
  config.data_dir = (tmp / "data").string();
  config.metadata_path = (tmp / "data" / "catalog" / "metadata.db").string();
  config.wal_dir = (tmp / "data" / "wal").string();
  config.segment_dir = (tmp / "data" / "segments").string();
  config.manifest_path = (tmp / "data" / "manifest" / "manifest.log").string();
  config.checkpoint_path = (tmp / "data" / "checkpoints" / "checkpoint.meta").string();

  mxdb::MetadataStore metadata;
  mxdb::Status status = metadata.Open(config.metadata_path);
  assert(status.ok());

  status = metadata.CreateFeature(MakeFeature());
  assert(status.ok());

  mxdb::FeatureEngine engine(config, &metadata);
  status = engine.Start();
  assert(status.ok());

  mxdb::AdminService admin(&engine, &config, &metadata);

  auto w1 = engine.WriteEntityBatch(MakeEvent("w1", 10.0, 100, 100),
                                    mxdb::DurabilityMode::kSync, true);
  assert(w1.ok());
  assert(ReadLatestPrice(&engine) == 10.0);

  status = admin.StartBackup(backup_dir.string());
  assert(status.ok());

  auto w2 = engine.WriteEntityBatch(MakeEvent("w2", 20.0, 200, 200),
                                    mxdb::DurabilityMode::kSync, true);
  assert(w2.ok());
  assert(ReadLatestPrice(&engine) == 20.0);

  status = admin.RestoreBackup(backup_dir.string(), /*start_read_only=*/false);
  assert(status.ok());

  // Restore must refresh in-memory state from restored on-disk snapshot.
  assert(ReadLatestPrice(&engine) == 10.0);

  auto w3 = engine.WriteEntityBatch(MakeEvent("w3", 30.0, 300, 300),
                                    mxdb::DurabilityMode::kSync, true);
  assert(w3.ok());
  assert(ReadLatestPrice(&engine) == 30.0);

  status = engine.Stop();
  assert(status.ok());

  status = metadata.Close();
  assert(status.ok());

  std::filesystem::remove_all(tmp);
  return 0;
}
