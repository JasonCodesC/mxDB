#include "engine/recovery/recovery_manager.h"

#include "engine/wal/wal_reader.h"

namespace mxdb {

Status RecoveryManager::RecoverFromWalDirectory(const std::string& wal_dir,
                                                bool* had_truncated_tail) {
  auto replay = WalReader::ReadAll(wal_dir);
  if (!replay.ok()) {
    return replay.status();
  }

  if (had_truncated_tail != nullptr) {
    *had_truncated_tail = replay.value().had_truncated_tail;
  }

  return engine_->RecoverFromWal(replay.value());
}

}  // namespace mxdb
