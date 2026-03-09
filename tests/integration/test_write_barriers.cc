#include <cassert>
#include <chrono>
#include <filesystem>
#include <optional>
#include <string>
#include <thread>

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
                                   mxdb::TimestampMicros event_time) {
  mxdb::EntityFeatureBatch batch;
  batch.entity = {
      .tenant_id = "prod", .entity_type = "instrument", .entity_id = "AAPL"};
  mxdb::FeatureEventInput event;
  event.feature_id = "f_price";
  event.event_time_us = event_time;
  event.system_time_us = event_time;
  event.value = {.type = mxdb::ValueType::kDouble, .value = value};
  event.operation = mxdb::OperationType::kUpsert;
  event.write_id = write_id;
  event.source_id = "write-barrier-test";
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

void Recover(mxdb::FeatureEngine* engine, const std::string& wal_dir) {
  mxdb::RecoveryManager recovery(engine);
  bool truncated_tail = false;
  mxdb::Status status = recovery.RecoverFromWalDirectory(wal_dir, &truncated_tail);
  assert(status.ok());
}

}  // namespace

int main() {
  const auto tmp = UniqueTmpDir("write-barriers");

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

  auto seed = engine.WriteEntityBatch(MakeEvent("seed", 10.0, 100),
                                      mxdb::DurabilityMode::kSync, true);
  assert(seed.ok());
  assert(ReadLatestPrice(&engine) == 10.0);

  // SetReadOnly(true) must block post-admission writes from becoming visible.
  engine.ResetPausedWritesForTest();
  engine.InjectPauseAfterAdmissionForTest(1);
  std::optional<mxdb::StatusOr<mxdb::EntityCommitResult>> paused_write_result;
  std::thread paused_writer([&]() {
    paused_write_result = engine.WriteEntityBatch(
        MakeEvent("paused-readonly", 11.0, 110), mxdb::DurabilityMode::kSync, true);
  });
  assert(engine.WaitForPausedWritesForTest(1, 2000));

  mxdb::Status set_read_only_status;
  std::thread toggle_read_only([&]() { set_read_only_status = engine.SetReadOnly(true); });
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  engine.ReleasePausedWritesForTest();
  toggle_read_only.join();
  paused_writer.join();

  assert(set_read_only_status.ok());
  assert(paused_write_result.has_value());
  assert(!paused_write_result->ok());
  assert(paused_write_result->status().code() ==
         mxdb::StatusCode::kFailedPrecondition);
  assert(ReadLatestPrice(&engine) == 10.0);
  status = engine.SetReadOnly(false);
  assert(status.ok());

  // Checkpoint must not skip a WAL-only write.
  engine.InjectSkipApplyAfterWalAppendForTest(1);
  auto wal_only = engine.WriteEntityBatch(MakeEvent("wal-only", 20.0, 200),
                                          mxdb::DurabilityMode::kSync, true);
  assert(!wal_only.ok());
  assert(ReadLatestPrice(&engine) == 10.0);
  status = engine.TriggerCheckpoint();
  assert(status.ok());

  status = engine.Stop();
  assert(status.ok());
  status = engine.Start();
  assert(status.ok());
  Recover(&engine, config.wal_dir);
  assert(ReadLatestPrice(&engine) == 20.0);

  // Stop() must form a hard barrier for admitted-but-not-appended writes.
  engine.ResetPausedWritesForTest();
  engine.InjectPauseAfterAdmissionForTest(1);
  std::optional<mxdb::StatusOr<mxdb::EntityCommitResult>> paused_stop_write;
  std::thread writer_before_stop([&]() {
    paused_stop_write = engine.WriteEntityBatch(
        MakeEvent("paused-stop", 30.0, 300), mxdb::DurabilityMode::kSync, true);
  });
  assert(engine.WaitForPausedWritesForTest(1, 2000));

  mxdb::Status stop_status;
  std::thread stop_thread([&]() { stop_status = engine.Stop(); });
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  engine.ReleasePausedWritesForTest();

  stop_thread.join();
  writer_before_stop.join();
  assert(stop_status.ok());
  assert(paused_stop_write.has_value());
  assert(!paused_stop_write->ok());
  assert(paused_stop_write->status().code() ==
         mxdb::StatusCode::kFailedPrecondition);

  status = engine.Start();
  assert(status.ok());
  Recover(&engine, config.wal_dir);
  assert(ReadLatestPrice(&engine) == 20.0);

  status = engine.Stop();
  assert(status.ok());
  status = metadata.Close();
  assert(status.ok());
  std::filesystem::remove_all(tmp);
  return 0;
}
