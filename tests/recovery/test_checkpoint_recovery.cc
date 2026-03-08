#include <cassert>
#include <chrono>
#include <filesystem>
#include <string>

#include "engine/catalog/metadata_store.h"
#include "engine/recovery/recovery_manager.h"
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
  event.source_id = "checkpoint-test";
  batch.events.push_back(std::move(event));
  return batch;
}

void SeedMetadata(mxdb::MetadataStore* metadata) {
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

  mxdb::Status status = metadata->CreateFeature(feature);
  assert(status.ok());
}

}  // namespace

int main() {
  const auto tmp = UniqueTmpDir("checkpoint-recovery");

  mxdb::EngineConfig config;
  config.data_dir = tmp.string();
  config.metadata_path = (tmp / "catalog" / "metadata.db").string();
  config.wal_dir = (tmp / "wal").string();
  config.segment_dir = (tmp / "segments").string();
  config.manifest_path = (tmp / "manifest" / "manifest.log").string();
  config.checkpoint_path = (tmp / "checkpoints" / "checkpoint.meta").string();
  config.memtable_flush_event_threshold = 1;

  mxdb::MetadataStore metadata;
  mxdb::Status status = metadata.Open(config.metadata_path);
  assert(status.ok());
  SeedMetadata(&metadata);

  {
    mxdb::FeatureEngine engine(config, &metadata);
    status = engine.Start();
    assert(status.ok());

    auto r1 = engine.WriteEntityBatch(MakeEvent("w1", 10.0, 100, 110),
                                      mxdb::DurabilityMode::kSync, true);
    assert(r1.ok());

    auto r2 = engine.WriteEntityBatch(MakeEvent("w2", 11.0, 101, 111),
                                      mxdb::DurabilityMode::kSync, true);
    assert(r2.ok());

    status = engine.TriggerCheckpoint();
    assert(status.ok());

    status = engine.Stop();
    assert(status.ok());
  }

  assert(std::filesystem::exists(config.manifest_path));
  assert(std::filesystem::exists(config.checkpoint_path));

  {
    mxdb::FeatureEngine engine(config, &metadata);
    status = engine.Start();
    assert(status.ok());

    mxdb::RecoveryManager recovery(&engine);
    bool had_truncated_tail = false;
    status = recovery.RecoverFromWalDirectory(config.wal_dir, &had_truncated_tail);
    assert(status.ok());
    assert(!had_truncated_tail);

    auto as_of = engine.AsOfLookup(
        {.entity = {.tenant_id = "prod", .entity_type = "instrument", .entity_id = "AAPL"},
         .feature_ids = {"f_price"},
         .event_cutoff_us = 200,
         .system_cutoff_us = 200});
    assert(as_of.ok());
    assert(as_of.value().features[0].found);
    assert(std::get<double>(as_of.value().features[0].value.value) == 11.0);

    // Duplicate write_id from pre-checkpoint history should be deduped after recovery.
    auto dedupe = engine.WriteEntityBatch(MakeEvent("w2", 99.0, 300, 300),
                                          mxdb::DurabilityMode::kSync, true);
    assert(dedupe.ok());
    assert(dedupe.value().accepted_events == 0);

    status = engine.Stop();
    assert(status.ok());
  }

  status = metadata.Close();
  assert(status.ok());

  std::filesystem::remove_all(tmp);
  return 0;
}
