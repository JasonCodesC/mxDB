#pragma once

#include <string>
#include <vector>

#include "engine/common/status/status.h"
#include "engine/wal/wal_record.h"

namespace mxdb {

struct WalReadRecord {
  WalRecordHeader header;
  WalBatchPayload payload;
};

struct WalReplayResult {
  std::vector<WalReadRecord> records;
  bool had_truncated_tail = false;
};

class WalReader {
 public:
  static StatusOr<WalReplayResult> ReadAll(const std::string& wal_dir);
};

}  // namespace mxdb
