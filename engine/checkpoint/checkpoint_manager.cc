#include "engine/checkpoint/checkpoint_manager.h"

#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>

namespace mxdb {

Status CheckpointManager::Open(const std::string& checkpoint_path) {
  checkpoint_path_ = checkpoint_path;
  const auto parent = std::filesystem::path(checkpoint_path_).parent_path();
  std::error_code ec;
  if (!parent.empty()) {
    std::filesystem::create_directories(parent, ec);
    if (ec) {
      return Status::Internal("failed to create checkpoint directory: " +
                              ec.message());
    }
  }
  return Status::Ok();
}

Status CheckpointManager::SaveCheckpoint(uint64_t checkpoint_lsn,
                                         uint64_t manifest_version,
                                         TimestampMicros created_at_us) {
  std::ofstream out(checkpoint_path_, std::ios::out | std::ios::trunc);
  if (!out.is_open()) {
    return Status::Internal("failed to open checkpoint metadata file");
  }

  out << "checkpoint_lsn=" << checkpoint_lsn << '\n';
  out << "manifest_version=" << manifest_version << '\n';
  out << "created_at_us=" << created_at_us << '\n';

  if (!out.good()) {
    return Status::Internal("failed to write checkpoint metadata");
  }

  return Status::Ok();
}

StatusOr<CheckpointState> CheckpointManager::LoadCheckpoint() const {
  CheckpointState state;

  if (!std::filesystem::exists(checkpoint_path_)) {
    return state;
  }

  std::ifstream in(checkpoint_path_);
  if (!in.is_open()) {
    return Status::Internal("failed to open checkpoint metadata");
  }

  std::string line;
  while (std::getline(in, line)) {
    const auto sep = line.find('=');
    if (sep == std::string::npos) {
      continue;
    }

    const std::string key = line.substr(0, sep);
    const std::string value = line.substr(sep + 1);

    if (key == "checkpoint_lsn") {
      state.checkpoint_lsn = std::stoull(value);
    } else if (key == "manifest_version") {
      state.manifest_version = std::stoull(value);
    } else if (key == "created_at_us") {
      state.created_at_us = std::stoll(value);
    }
  }

  state.exists = true;
  return state;
}

}  // namespace mxdb
