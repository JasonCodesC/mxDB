#pragma once

#include <string>

#include "engine/common/status/status.h"
#include "engine/storage/feature_engine.h"

namespace mxdb {

class RecoveryManager {
 public:
  explicit RecoveryManager(FeatureEngine* engine) : engine_(engine) {}

  Status RecoverFromWalDirectory(const std::string& wal_dir,
                                 bool* had_truncated_tail);

 private:
  FeatureEngine* engine_ = nullptr;
};

}  // namespace mxdb
