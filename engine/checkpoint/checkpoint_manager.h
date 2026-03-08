#pragma once

#include <cstdint>
#include <string>

#include "engine/common/status/status.h"
#include "engine/types/types.h"

namespace mxdb {

struct CheckpointState {
  bool exists = false;
  uint64_t checkpoint_lsn = 0;
  uint64_t manifest_version = 0;
  TimestampMicros created_at_us = 0;
};

class CheckpointManager {
 public:
  CheckpointManager() = default;

  Status Open(const std::string& checkpoint_path);
  Status SaveCheckpoint(uint64_t checkpoint_lsn, uint64_t manifest_version,
                        TimestampMicros created_at_us);
  StatusOr<CheckpointState> LoadCheckpoint() const;

 private:
  std::string checkpoint_path_;
};

}  // namespace mxdb
