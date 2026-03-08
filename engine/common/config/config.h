#pragma once

#include <string>

#include "engine/types/types.h"

namespace mxdb {

struct EngineConfig {
  std::string data_dir = "./data";
  std::string metadata_path = "./data/catalog/metadata.db";
  std::string wal_dir = "./data/wal";
  std::string segment_dir = "./data/segments";
  std::string manifest_path = "./data/manifest/manifest.log";
  std::string checkpoint_path = "./data/checkpoints/checkpoint.meta";
  size_t partition_count = 8;
  size_t memtable_flush_event_threshold = 2048;
  size_t wal_segment_target_bytes = 16 * 1024 * 1024;
  size_t wal_group_commit_max_records = 1024;
  uint64_t wal_group_commit_window_ms = 5;
  DurabilityMode default_durability_mode = DurabilityMode::kGroupCommit;
};

class ConfigLoader {
 public:
  // Minimal key=value parser for local runs.
  static EngineConfig LoadFromFile(const std::string& path);
};

}  // namespace mxdb
