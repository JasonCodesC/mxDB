#include "engine/common/config/config.h"

#include <cerrno>
#include <cctype>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <limits>
#include <string>

namespace mxdb {

namespace {

std::string Trim(const std::string& input) {
  size_t begin = 0;
  while (begin < input.size() &&
         std::isspace(static_cast<unsigned char>(input[begin]))) {
    ++begin;
  }

  size_t end = input.size();
  while (end > begin &&
         std::isspace(static_cast<unsigned char>(input[end - 1]))) {
    --end;
  }

  return input.substr(begin, end - begin);
}

bool ParseBool(const std::string& raw_value) {
  const std::string value = Trim(raw_value);
  return value == "1" || value == "true" || value == "TRUE";
}

StatusOr<uint64_t> ParseUint64Strict(const std::string& raw_value,
                                     const std::string& key) {
  const std::string value = Trim(raw_value);
  if (value.empty()) {
    return Status::InvalidArgument("config key '" + key + "' has empty value");
  }
  if (value[0] == '-') {
    return Status::InvalidArgument("config key '" + key +
                                   "' must be a non-negative integer");
  }

  char* end = nullptr;
  errno = 0;
  const unsigned long long parsed =
      std::strtoull(value.c_str(), &end, 10);
  if (errno == ERANGE) {
    return Status::InvalidArgument("config key '" + key + "' is out of range");
  }
  if (end == value.c_str() || (end != nullptr && *end != '\0')) {
    return Status::InvalidArgument("config key '" + key +
                                   "' is not a valid integer: " + value);
  }
  return static_cast<uint64_t>(parsed);
}

StatusOr<std::filesystem::path> ResolveConfigPath(
    const std::filesystem::path& raw_path) {
  std::error_code ec;
  std::filesystem::path absolute = std::filesystem::absolute(raw_path, ec);
  if (ec) {
    return Status::Internal("failed to resolve config path: " + ec.message());
  }
  return absolute.lexically_normal();
}

std::string ResolvePathValue(const std::filesystem::path& base_dir,
                             const std::string& raw_value) {
  const std::string value = Trim(raw_value);
  const std::filesystem::path input_path(value);
  if (input_path.is_absolute()) {
    return input_path.lexically_normal().string();
  }
  return (base_dir / input_path).lexically_normal().string();
}

StatusOr<size_t> ParseSize(const std::string& raw_value, const std::string& key) {
  auto parsed = ParseUint64Strict(raw_value, key);
  if (!parsed.ok()) {
    return parsed.status();
  }
  if (parsed.value() >
      static_cast<uint64_t>(std::numeric_limits<size_t>::max())) {
    return Status::InvalidArgument("config key '" + key + "' is out of range");
  }
  return static_cast<size_t>(parsed.value());
}

}  // namespace

StatusOr<EngineConfig> ConfigLoader::LoadFromFile(const std::string& path) {
  auto config_path_or = ResolveConfigPath(std::filesystem::path(path));
  if (!config_path_or.ok()) {
    return config_path_or.status();
  }
  const std::filesystem::path config_path = config_path_or.value();
  const std::filesystem::path config_dir = config_path.parent_path().empty()
                                               ? std::filesystem::current_path()
                                               : config_path.parent_path();

  std::ifstream in(config_path);
  if (!in.is_open()) {
    return Status::NotFound("failed to open config file: " + config_path.string());
  }

  EngineConfig config;
  config.data_dir = ResolvePathValue(config_dir, config.data_dir);
  config.metadata_path = ResolvePathValue(config_dir, config.metadata_path);
  config.wal_dir = ResolvePathValue(config_dir, config.wal_dir);
  config.segment_dir = ResolvePathValue(config_dir, config.segment_dir);
  config.manifest_path = ResolvePathValue(config_dir, config.manifest_path);
  config.checkpoint_path = ResolvePathValue(config_dir, config.checkpoint_path);

  std::string line;
  size_t line_no = 0;
  while (std::getline(in, line)) {
    ++line_no;
    const std::string trimmed = Trim(line);
    if (trimmed.empty() || trimmed[0] == '#') {
      continue;
    }

    const size_t sep = trimmed.find('=');
    if (sep == std::string::npos) {
      continue;
    }

    const std::string key = Trim(trimmed.substr(0, sep));
    const std::string value = Trim(trimmed.substr(sep + 1));

    if (key.empty()) {
      return Status::InvalidArgument("invalid config key at line " +
                                     std::to_string(line_no));
    }

    if (key == "data_dir") {
      config.data_dir = ResolvePathValue(config_dir, value);
    } else if (key == "metadata_path") {
      config.metadata_path = ResolvePathValue(config_dir, value);
    } else if (key == "wal_dir") {
      config.wal_dir = ResolvePathValue(config_dir, value);
    } else if (key == "segment_dir") {
      config.segment_dir = ResolvePathValue(config_dir, value);
    } else if (key == "manifest_path") {
      config.manifest_path = ResolvePathValue(config_dir, value);
    } else if (key == "checkpoint_path") {
      config.checkpoint_path = ResolvePathValue(config_dir, value);
    } else if (key == "partition_count") {
      auto parsed = ParseSize(value, key);
      if (!parsed.ok()) {
        return parsed.status();
      }
      config.partition_count = parsed.value();
    } else if (key == "memtable_flush_event_threshold") {
      auto parsed = ParseSize(value, key);
      if (!parsed.ok()) {
        return parsed.status();
      }
      config.memtable_flush_event_threshold = parsed.value();
    } else if (key == "wal_segment_target_bytes") {
      auto parsed = ParseSize(value, key);
      if (!parsed.ok()) {
        return parsed.status();
      }
      config.wal_segment_target_bytes = parsed.value();
    } else if (key == "wal_group_commit_max_records") {
      auto parsed = ParseSize(value, key);
      if (!parsed.ok()) {
        return parsed.status();
      }
      config.wal_group_commit_max_records = parsed.value();
    } else if (key == "wal_group_commit_window_ms") {
      auto parsed = ParseUint64Strict(value, key);
      if (!parsed.ok()) {
        return parsed.status();
      }
      config.wal_group_commit_window_ms = parsed.value();
    } else if (key == "default_durability_sync") {
      config.default_durability_mode =
          ParseBool(value) ? DurabilityMode::kSync : DurabilityMode::kGroupCommit;
    }
  }

  return config;
}

}  // namespace mxdb
