#include <atomic>
#include <cassert>
#include <chrono>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>

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
  event.source_id = "concurrency-test";
  batch.events.push_back(std::move(event));
  return batch;
}

}  // namespace

int main() {
  const auto tmp = UniqueTmpDir("write-id-concurrency");

  mxdb::EngineConfig config;
  config.data_dir = tmp.string();
  config.metadata_path = (tmp / "catalog" / "metadata.db").string();
  config.wal_dir = (tmp / "wal").string();

  mxdb::MetadataStore metadata;
  mxdb::Status status = metadata.Open(config.metadata_path);
  assert(status.ok());

  status = metadata.CreateFeature(MakeFeature());
  assert(status.ok());

  mxdb::FeatureEngine engine(config, &metadata);
  status = engine.Start();
  assert(status.ok());

  constexpr int kThreadCount = 16;
  std::atomic<int> accepted_total{0};
  std::atomic<int> ready_threads{0};
  std::atomic<bool> start{false};

  const std::string write_id = "dup-write-id";
  const mxdb::TimestampMicros base_time = NowMicros();

  std::vector<std::thread> workers;
  workers.reserve(kThreadCount);
  for (int i = 0; i < kThreadCount; ++i) {
    workers.emplace_back([&, i]() {
      ready_threads.fetch_add(1);
      while (!start.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }

      auto result = engine.WriteEntityBatch(
          MakeEvent(write_id, 100.0 + i, base_time + i, base_time + i),
          mxdb::DurabilityMode::kSync, true);
      assert(result.ok());
      accepted_total.fetch_add(static_cast<int>(result.value().accepted_events));
    });
  }

  while (ready_threads.load() < kThreadCount) {
    std::this_thread::yield();
  }
  start.store(true, std::memory_order_release);

  for (auto& worker : workers) {
    worker.join();
  }

  assert(accepted_total.load() == 1);

  auto dedupe = engine.WriteEntityBatch(
      MakeEvent(write_id, 999.0, base_time + 999, base_time + 999),
      mxdb::DurabilityMode::kSync, true);
  assert(dedupe.ok());
  assert(dedupe.value().accepted_events == 0);

  auto as_of = engine.AsOfLookup({.entity = {.tenant_id = "prod",
                                              .entity_type = "instrument",
                                              .entity_id = "AAPL"},
                                  .feature_ids = {"f_price"},
                                  .event_cutoff_us = base_time + 2000,
                                  .system_cutoff_us = base_time + 2000});
  assert(as_of.ok());
  assert(as_of.value().features.at(0).found);

  status = engine.Stop();
  assert(status.ok());

  status = metadata.Close();
  assert(status.ok());

  std::filesystem::remove_all(tmp);
  return 0;
}
