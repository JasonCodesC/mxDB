#include "engine/common/config/config.h"

#include <fstream>
#include <sstream>
#include <string>

namespace mxdb {

namespace {

bool ParseBool(const std::string& value) {
  return value == "1" || value == "true" || value == "TRUE";
}

}  // namespace

EngineConfig ConfigLoader::LoadFromFile(const std::string& path) {
  EngineConfig config;
  std::ifstream in(path);
  if (!in.is_open()) {
    return config;
  }

  std::string line;
  while (std::getline(in, line)) {
    if (line.empty() || line[0] == '#') {
      continue;
    }

    const auto sep = line.find('=');
    if (sep == std::string::npos) {
      continue;
    }

    const std::string key = line.substr(0, sep);
    const std::string value = line.substr(sep + 1);

    if (key == "data_dir") {
      config.data_dir = value;
    } else if (key == "metadata_path") {
      config.metadata_path = value;
    } else if (key == "wal_dir") {
      config.wal_dir = value;
    } else if (key == "segment_dir") {
      config.segment_dir = value;
    } else if (key == "manifest_path") {
      config.manifest_path = value;
    } else if (key == "checkpoint_path") {
      config.checkpoint_path = value;
    } else if (key == "partition_count") {
      config.partition_count = static_cast<size_t>(std::stoull(value));
    } else if (key == "memtable_flush_event_threshold") {
      config.memtable_flush_event_threshold = static_cast<size_t>(std::stoull(value));
    } else if (key == "wal_segment_target_bytes") {
      config.wal_segment_target_bytes = static_cast<size_t>(std::stoull(value));
    } else if (key == "wal_group_commit_max_records") {
      config.wal_group_commit_max_records =
          static_cast<size_t>(std::stoull(value));
    } else if (key == "wal_group_commit_window_ms") {
      config.wal_group_commit_window_ms = std::stoull(value);
    } else if (key == "default_durability_sync") {
      config.default_durability_mode =
          ParseBool(value) ? DurabilityMode::kSync : DurabilityMode::kGroupCommit;
    }
  }

  return config;
}

}  // namespace mxdb
