#include <cassert>
#include <chrono>
#include <filesystem>
#include <string>

#include "engine/wal/wal_reader.h"
#include "engine/wal/wal_writer.h"

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

std::filesystem::path FindWalFile(const std::filesystem::path& wal_dir) {
  for (const auto& entry : std::filesystem::directory_iterator(wal_dir)) {
    if (entry.is_regular_file() &&
        entry.path().filename().string().rfind("wal-", 0) == 0) {
      return entry.path();
    }
  }
  return {};
}

}  // namespace

int main() {
  const auto tmp = UniqueTmpDir("wal");
  const auto wal_dir = tmp / "wal";
  std::filesystem::create_directories(wal_dir);

  mxdb::WalWriter writer;
  mxdb::Status status = writer.Open(wal_dir.string(), 1024 * 1024, 5, 4);
  assert(status.ok());

  mxdb::FeatureEvent event;
  event.entity = {.tenant_id = "prod", .entity_type = "instrument", .entity_id = "AAPL"};
  event.feature_id = "f_price";
  event.event_time_us = 100;
  event.system_time_us = 150;
  event.sequence_no = 1;
  event.operation = mxdb::OperationType::kUpsert;
  event.value = {.type = mxdb::ValueType::kDouble, .value = 10.5};
  event.write_id = "w1";
  event.source_id = "unit";
  event.lsn = 1;

  mxdb::WalBatchPayload payload;
  payload.commit_system_time_us = NowMicros();
  payload.events = {event};

  status = writer.Append(1, payload, mxdb::DurabilityMode::kSync);
  assert(status.ok());

  status = writer.Close();
  assert(status.ok());

  auto replay = mxdb::WalReader::ReadAll(wal_dir.string());
  assert(replay.ok());
  assert(!replay.value().had_truncated_tail);
  assert(replay.value().records.size() == 1);
  assert(replay.value().records[0].header.lsn == 1);
  assert(replay.value().records[0].payload.events.size() == 1);
  assert(replay.value().records[0].payload.events[0].feature_id == "f_price");

  const auto wal_file = FindWalFile(wal_dir);
  assert(!wal_file.empty());
  const auto current_size = std::filesystem::file_size(wal_file);
  assert(current_size > 10);

  std::filesystem::resize_file(wal_file, current_size - 5);
  auto replay_after_truncation = mxdb::WalReader::ReadAll(wal_dir.string());
  assert(replay_after_truncation.ok());
  assert(replay_after_truncation.value().had_truncated_tail);
  assert(replay_after_truncation.value().records.empty());

  std::filesystem::remove_all(tmp);
  return 0;
}
