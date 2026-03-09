#pragma once

#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

#include "engine/catalog/metadata_store.h"
#include "engine/common/config/config.h"
#include "engine/common/status/status.h"
#include "engine/storage/feature_engine.h"

namespace mxdb {

struct HealthStatus {
  std::string state;
  uint64_t current_lsn = 0;
  uint64_t manifest_version = 0;
  bool read_only = false;
  std::vector<std::string> warnings;
};

struct CompactionStatus {
  uint32_t queued_tasks = 0;
  uint32_t running_tasks = 0;
  uint64_t bytes_pending = 0;
};

class AdminService {
 public:
  AdminService(FeatureEngine* engine, const EngineConfig* config,
               MetadataStore* metadata_store = nullptr)
      : engine_(engine), config_(config), metadata_store_(metadata_store) {}

  HealthStatus GetHealth() const;
  CompactionStatus GetCompactionStatus() const;

  Status TriggerCheckpoint(bool wait);
  Status TriggerCompaction();
  Status SetReadOnlyMode(bool read_only);
  Status StartBackup(const std::string& destination_dir);
  Status RestoreBackup(const std::string& source_dir, bool start_read_only);

 private:
  static Status CopyPath(const std::string& from, const std::string& to,
                         bool recursive);
  static Status RemoveIfExists(const std::filesystem::path& path);
  static Status SwapInDirectory(const std::filesystem::path& prepared_source,
                                const std::filesystem::path& target);
  static std::string TempPathSuffix(const std::string& prefix);

  FeatureEngine* engine_ = nullptr;
  const EngineConfig* config_ = nullptr;
  MetadataStore* metadata_store_ = nullptr;
};

}  // namespace mxdb
