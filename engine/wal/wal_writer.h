#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <string>

#include "engine/common/status/status.h"
#include "engine/types/types.h"
#include "engine/wal/wal_record.h"

namespace mxdb {

class WalWriter {
 public:
  WalWriter() = default;
  ~WalWriter();

  WalWriter(const WalWriter&) = delete;
  WalWriter& operator=(const WalWriter&) = delete;

  Status Open(const std::string& wal_dir, size_t segment_target_bytes,
              uint64_t group_commit_window_ms,
              size_t group_commit_max_records);
  Status Close();

  Status Append(Lsn lsn, const WalBatchPayload& payload, DurabilityMode mode);

 private:
  Status EnsureSegmentOpen();
  Status RotateSegment();
  Status FsyncCurrentSegment();
  Status WriteAll(const void* data, size_t bytes);
  std::string SegmentPath(uint32_t index) const;

  std::mutex mu_;
  std::string wal_dir_;
  int fd_ = -1;
  uint32_t segment_index_ = 0;
  size_t segment_target_bytes_ = 0;
  size_t current_segment_bytes_ = 0;

  uint64_t group_commit_window_ms_ = 0;
  size_t group_commit_max_records_ = 0;
  size_t pending_since_fsync_ = 0;
  std::chrono::steady_clock::time_point last_fsync_at_{};
};

}  // namespace mxdb
