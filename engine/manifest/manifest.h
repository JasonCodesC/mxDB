#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "engine/common/status/status.h"
#include "engine/types/types.h"

namespace mxdb {

struct ManifestEntry {
  uint64_t manifest_version = 0;
  size_t partition_id = 0;
  std::string segment_path;
  TimestampMicros min_event_time_us = 0;
  TimestampMicros max_event_time_us = 0;
  TimestampMicros min_system_time_us = 0;
  TimestampMicros max_system_time_us = 0;
  uint64_t max_lsn = 0;
  uint64_t row_count = 0;
};

class ManifestLog {
 public:
  ManifestLog() = default;

  Status Open(const std::string& manifest_path);

  StatusOr<std::vector<ManifestEntry>> LoadEntries() const;

  StatusOr<ManifestEntry> AppendSegment(const ManifestEntry& entry);

  Status RewriteEntries(const std::vector<ManifestEntry>& entries);

  uint64_t LatestVersion() const { return version_; }

 private:
  std::string manifest_path_;
  uint64_t version_ = 0;
};

}  // namespace mxdb
