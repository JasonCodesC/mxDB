#include "engine/wal/wal_reader.h"

#include <filesystem>
#include <fstream>
#include <vector>

#include "engine/common/crc32/crc32.h"

namespace mxdb {

namespace {

uint32_t ParseWalIndex(const std::filesystem::path& path) {
  const std::string name = path.filename().string();
  if (name.rfind("wal-", 0) != 0 || name.size() < 12 ||
      name.substr(name.size() - 4) != ".log") {
    return 0;
  }
  return static_cast<uint32_t>(std::stoul(name.substr(4, name.size() - 8)));
}

bool ReadExact(std::ifstream* in, void* dst, size_t bytes) {
  in->read(static_cast<char*>(dst), bytes);
  return static_cast<size_t>(in->gcount()) == bytes;
}

StatusOr<std::vector<std::filesystem::path>> SortedWalSegments(
    const std::string& wal_dir) {
  std::vector<std::pair<uint32_t, std::filesystem::path>> indexed;
  std::error_code ec;
  if (!std::filesystem::exists(wal_dir, ec)) {
    return std::vector<std::filesystem::path>{};
  }
  for (const auto& entry : std::filesystem::directory_iterator(wal_dir)) {
    if (!entry.is_regular_file()) {
      continue;
    }
    const uint32_t index = ParseWalIndex(entry.path());
    if (index == 0) {
      continue;
    }
    indexed.emplace_back(index, entry.path());
  }

  std::sort(indexed.begin(), indexed.end(),
            [](const auto& lhs, const auto& rhs) { return lhs.first < rhs.first; });

  std::vector<std::filesystem::path> segments;
  segments.reserve(indexed.size());
  for (const auto& it : indexed) {
    segments.push_back(it.second);
  }
  return segments;
}

}  // namespace

StatusOr<WalReplayResult> WalReader::ReadAll(const std::string& wal_dir) {
  auto segment_paths = SortedWalSegments(wal_dir);
  if (!segment_paths.ok()) {
    return segment_paths.status();
  }

  WalReplayResult result;

  for (const auto& path : segment_paths.value()) {
    std::ifstream in(path, std::ios::binary);
    if (!in.is_open()) {
      return Status::Internal("failed to open WAL segment for read: " +
                              path.string());
    }

    while (true) {
      WalRecordHeader header;
      if (!ReadExact(&in, &header, sizeof(header))) {
        if (in.gcount() == 0) {
          break;
        }
        result.had_truncated_tail = true;
        break;
      }

      if (header.magic != kWalMagic || header.version != kWalVersion ||
          header.record_type != kWalRecordTypeEntityBatch) {
        return Status::Internal("invalid WAL header in segment: " + path.string());
      }

      std::vector<uint8_t> payload_bytes(header.payload_length);
      if (header.payload_length > 0 &&
          !ReadExact(&in, payload_bytes.data(), header.payload_length)) {
        result.had_truncated_tail = true;
        break;
      }

      const uint32_t crc = Crc32(payload_bytes.data(), payload_bytes.size());
      if (crc != header.crc32) {
        result.had_truncated_tail = true;
        break;
      }

      auto payload = ParseWalBatch(header.lsn, payload_bytes);
      if (!payload.ok()) {
        result.had_truncated_tail = true;
        break;
      }

      result.records.push_back(WalReadRecord{header, payload.value()});
    }
  }

  return result;
}

}  // namespace mxdb
