#include <atomic>
#include <cassert>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>

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
  event.source_id = "admin-test";
  batch.events.push_back(std::move(event));
  return batch;
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

void ExpectEngineStillLiveAfterBackupPathFailure(mxdb::FeatureEngine* engine,
                                                 int write_suffix,
                                                 double value) {
  auto write = engine->WriteEntityBatch(
      MakeEvent("live-" + std::to_string(write_suffix), value, 5000 + write_suffix,
                5000 + write_suffix),
      mxdb::DurabilityMode::kSync, true);
  assert(write.ok());
  assert(ReadLatestPrice(engine) == value);
}

}  // namespace

int main() {
  const auto tmp = UniqueTmpDir("admin");
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
  status = metadata.CreateFeature(feature);
  assert(status.ok());

  mxdb::FeatureEngine engine(config, &metadata);
  status = engine.Start();
  assert(status.ok());

  mxdb::AdminService admin(&engine, &config);

  auto health = admin.GetHealth();
  assert(health.state == "HEALTHY");

  auto write = engine.WriteEntityBatch(MakeEvent("w1", 1.0, 100, 100),
                                       mxdb::DurabilityMode::kSync, true);
  assert(write.ok());

  // Reject dangerous backup destinations that overlap the live data tree.
  status = admin.StartBackup(tmp.string());  // parent(config.data_dir)
  assert(!status.ok());
  assert(std::filesystem::exists(config.data_dir));
  ExpectEngineStillLiveAfterBackupPathFailure(&engine, 1, 1.1);

  status = admin.StartBackup(config.data_dir);  // equal to live data dir
  assert(!status.ok());
  assert(std::filesystem::exists(config.data_dir));
  ExpectEngineStillLiveAfterBackupPathFailure(&engine, 2, 1.2);

  status = admin.StartBackup((std::filesystem::path(config.data_dir) / "backup-child")
                                 .string());  // nested under live data dir
  assert(!status.ok());
  assert(std::filesystem::exists(config.data_dir));
  ExpectEngineStillLiveAfterBackupPathFailure(&engine, 3, 1.3);

  // Slow backup copy enough to overlap concurrent write attempts.
  {
    std::ofstream filler(std::filesystem::path(config.data_dir) / "backup-load.bin",
                         std::ios::binary | std::ios::trunc);
    assert(filler.is_open());
    std::string chunk(1024 * 1024, 'x');
    for (int i = 0; i < 8; ++i) {
      filler.write(chunk.data(), static_cast<std::streamsize>(chunk.size()));
    }
    assert(filler.good());
  }

  std::atomic<bool> stop_writer{false};
  std::atomic<int> failed_writes{0};
  std::atomic<int> attempted_writes{0};
  std::thread writer([&]() {
    int counter = 0;
    while (!stop_writer.load()) {
      auto result = engine.WriteEntityBatch(
          MakeEvent("race-" + std::to_string(counter), 50.0 + counter, 1000 + counter,
                    1000 + counter),
          mxdb::DurabilityMode::kSync, true);
      attempted_writes.fetch_add(1);
      if (!result.ok()) {
        failed_writes.fetch_add(1);
      }
      ++counter;
    }
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  status = admin.StartBackup(backup_dir.string());
  stop_writer.store(true);
  writer.join();

  assert(status.ok());
  assert(std::filesystem::exists(backup_dir / "data"));
  assert(attempted_writes.load() > 0);
  assert(failed_writes.load() > 0);

  status = admin.SetReadOnlyMode(true);
  assert(status.ok());
  auto blocked = engine.WriteEntityBatch(MakeEvent("w2", 2.0, 101, 101),
                                         mxdb::DurabilityMode::kSync, true);
  assert(!blocked.ok());
  auto compact_blocked = engine.CompactImmutableSegments();
  assert(!compact_blocked.ok());

  status = admin.SetReadOnlyMode(false);
  assert(status.ok());
  auto unblocked = engine.WriteEntityBatch(MakeEvent("w2", 2.0, 101, 101),
                                           mxdb::DurabilityMode::kSync, true);
  assert(unblocked.ok());

  status = engine.Stop();
  assert(status.ok());
  status = metadata.Close();
  assert(status.ok());

  std::filesystem::remove_all(tmp);
  return 0;
}
