#include <cassert>
#include <chrono>
#include <cstddef>
#include <filesystem>
#include <fstream>
#include <string>

#include "engine/common/crc32/crc32.h"
#include "engine/wal/wal_reader.h"
#include "engine/wal/wal_record.h"
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

mxdb::Status AppendRecord(mxdb::WalWriter* writer, uint64_t lsn,
                          const std::string& write_id, double value) {
  mxdb::FeatureEvent event;
  event.entity = {
      .tenant_id = "prod", .entity_type = "instrument", .entity_id = "AAPL"};
  event.feature_id = "f_price";
  event.event_time_us = static_cast<mxdb::TimestampMicros>(100 + lsn);
  event.system_time_us = static_cast<mxdb::TimestampMicros>(150 + lsn);
  event.sequence_no = lsn;
  event.operation = mxdb::OperationType::kUpsert;
  event.value = {.type = mxdb::ValueType::kDouble, .value = value};
  event.write_id = write_id;
  event.source_id = "unit";
  event.lsn = lsn;

  mxdb::WalBatchPayload payload;
  payload.commit_system_time_us = NowMicros();
  payload.events = {event};
  return writer->Append(lsn, payload, mxdb::DurabilityMode::kSync);
}

bool ReadHeader(std::fstream* file, mxdb::WalRecordHeader* header) {
  file->read(reinterpret_cast<char*>(header), sizeof(*header));
  return static_cast<size_t>(file->gcount()) == sizeof(*header);
}

void CorruptSecondRecordCrc(const std::filesystem::path& wal_file) {
  std::fstream file(wal_file, std::ios::in | std::ios::out | std::ios::binary);
  assert(file.is_open());

  mxdb::WalRecordHeader first{};
  assert(ReadHeader(&file, &first));
  file.seekg(first.payload_length, std::ios::cur);

  const std::streamoff second_header_pos = file.tellg();
  mxdb::WalRecordHeader second{};
  assert(ReadHeader(&file, &second));
  const uint32_t bad_crc = second.crc32 ^ 0xFFFFFFFFU;

  file.seekp(second_header_pos +
             static_cast<std::streamoff>(offsetof(mxdb::WalRecordHeader, crc32)));
  file.write(reinterpret_cast<const char*>(&bad_crc), sizeof(bad_crc));
  file.flush();
}

void CorruptSecondRecordToParseFailure(const std::filesystem::path& wal_file) {
  std::fstream file(wal_file, std::ios::in | std::ios::out | std::ios::binary);
  assert(file.is_open());

  mxdb::WalRecordHeader first{};
  assert(ReadHeader(&file, &first));
  file.seekg(first.payload_length, std::ios::cur);

  const std::streamoff second_header_pos = file.tellg();
  mxdb::WalRecordHeader second{};
  assert(ReadHeader(&file, &second));

  uint8_t first_payload_byte = 0;
  file.read(reinterpret_cast<char*>(&first_payload_byte), 1);
  assert(file.gcount() == 1);

  const uint32_t tiny_payload_length = 1;
  const uint32_t tiny_crc = mxdb::Crc32(&first_payload_byte, 1);
  file.seekp(second_header_pos +
             static_cast<std::streamoff>(
                 offsetof(mxdb::WalRecordHeader, payload_length)));
  file.write(reinterpret_cast<const char*>(&tiny_payload_length),
             sizeof(tiny_payload_length));
  file.write(reinterpret_cast<const char*>(&tiny_crc), sizeof(tiny_crc));
  file.flush();
}

}  // namespace

int main() {
  // Tail truncation remains a recoverable condition.
  {
    const auto tmp = UniqueTmpDir("wal-truncate");
    const auto wal_dir = tmp / "wal";
    std::filesystem::create_directories(wal_dir);

    mxdb::WalWriter writer;
    mxdb::Status status =
        writer.Open(wal_dir.string(), 1024 * 1024, 5, 4);
    assert(status.ok());
    status = AppendRecord(&writer, 1, "w1", 10.5);
    assert(status.ok());
    status = writer.Close();
    assert(status.ok());

    const auto wal_file = FindWalFile(wal_dir);
    assert(!wal_file.empty());
    const auto current_size = std::filesystem::file_size(wal_file);
    assert(current_size > 10);
    std::filesystem::resize_file(wal_file, current_size - 5);

    auto replay = mxdb::WalReader::ReadAll(wal_dir.string());
    assert(replay.ok());
    assert(replay.value().had_truncated_tail);
    assert(replay.value().records.empty());
    std::filesystem::remove_all(tmp);
  }

  // Mid-log CRC corruption is a hard recovery error.
  {
    const auto tmp = UniqueTmpDir("wal-mid-crc");
    const auto wal_dir = tmp / "wal";
    std::filesystem::create_directories(wal_dir);

    mxdb::WalWriter writer;
    mxdb::Status status =
        writer.Open(wal_dir.string(), 1024 * 1024, 5, 4);
    assert(status.ok());
    assert(AppendRecord(&writer, 1, "w1", 10.0).ok());
    assert(AppendRecord(&writer, 2, "w2", 20.0).ok());
    assert(AppendRecord(&writer, 3, "w3", 30.0).ok());
    status = writer.Close();
    assert(status.ok());

    const auto wal_file = FindWalFile(wal_dir);
    assert(!wal_file.empty());
    CorruptSecondRecordCrc(wal_file);

    auto replay = mxdb::WalReader::ReadAll(wal_dir.string());
    assert(!replay.ok());
    assert(replay.status().message().find("CRC mismatch") != std::string::npos);
    std::filesystem::remove_all(tmp);
  }

  // Mid-log payload parse corruption is a hard recovery error.
  {
    const auto tmp = UniqueTmpDir("wal-mid-parse");
    const auto wal_dir = tmp / "wal";
    std::filesystem::create_directories(wal_dir);

    mxdb::WalWriter writer;
    mxdb::Status status =
        writer.Open(wal_dir.string(), 1024 * 1024, 5, 4);
    assert(status.ok());
    assert(AppendRecord(&writer, 1, "w1", 10.0).ok());
    assert(AppendRecord(&writer, 2, "w2", 20.0).ok());
    assert(AppendRecord(&writer, 3, "w3", 30.0).ok());
    status = writer.Close();
    assert(status.ok());

    const auto wal_file = FindWalFile(wal_dir);
    assert(!wal_file.empty());
    CorruptSecondRecordToParseFailure(wal_file);

    auto replay = mxdb::WalReader::ReadAll(wal_dir.string());
    assert(!replay.ok());
    const std::string message = replay.status().message();
    assert(message.find("parse") != std::string::npos ||
           message.find("Parse") != std::string::npos);
    std::filesystem::remove_all(tmp);
  }

  return 0;
}
