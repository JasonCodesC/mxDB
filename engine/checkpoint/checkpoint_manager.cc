#include "engine/checkpoint/checkpoint_manager.h"

#include <chrono>
#include <exception>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>

#include "engine/common/io/file_ops.h"

namespace mxdb {

namespace {

Status WriteFileAtomically(const std::string& path, const std::string& contents) {
  const std::filesystem::path target(path);
  const std::filesystem::path parent = target.parent_path();
  std::error_code ec;
  if (!parent.empty()) {
    std::filesystem::create_directories(parent, ec);
    if (ec) {
      return Status::Internal("failed to create checkpoint directory: " +
                              ec.message());
    }
  }

  const auto nonce = std::chrono::steady_clock::now().time_since_epoch().count();
  const std::filesystem::path tmp_path =
      target.string() + ".tmp." + std::to_string(nonce);
  const int fd = io::OpenWriteTruncate(tmp_path.string());
  if (fd < 0) {
    return Status::Internal("failed to open temporary checkpoint file: " +
                            io::ErrnoMessage());
  }

  Status status = io::WriteAll(fd, contents.data(), contents.size(),
                               "checkpoint write failed");
  if (status.ok()) {
    status = io::SyncFile(fd, "failed to fsync checkpoint file");
  }
  Status close_status =
      io::CloseFile(fd, "failed to close temporary checkpoint file");
  if (status.ok()) {
    status = close_status;
  }

  if (!status.ok()) {
    std::error_code ignore_ec;
    std::filesystem::remove(tmp_path, ignore_ec);
    return status;
  }

  status = io::AtomicReplace(tmp_path, target, "checkpoint metadata replace");
  if (!status.ok()) {
    std::filesystem::remove(tmp_path, ec);
    return status;
  }

  return io::SyncDirectory(parent, "checkpoint directory sync");
}

}  // namespace

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
  std::ostringstream out;
  out << "checkpoint_lsn=" << checkpoint_lsn << '\n';
  out << "manifest_version=" << manifest_version << '\n';
  out << "created_at_us=" << created_at_us << '\n';
  return WriteFileAtomically(checkpoint_path_, out.str());
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

  bool saw_lsn = false;
  bool saw_manifest_version = false;
  bool saw_created_at = false;

  std::string line;
  while (std::getline(in, line)) {
    const auto sep = line.find('=');
    if (sep == std::string::npos) {
      continue;
    }

    const std::string key = line.substr(0, sep);
    const std::string value = line.substr(sep + 1);

    try {
      if (key == "checkpoint_lsn") {
        state.checkpoint_lsn = std::stoull(value);
        saw_lsn = true;
      } else if (key == "manifest_version") {
        state.manifest_version = std::stoull(value);
        saw_manifest_version = true;
      } else if (key == "created_at_us") {
        state.created_at_us = std::stoll(value);
        saw_created_at = true;
      }
    } catch (const std::exception&) {
      return Status::Internal("malformed checkpoint metadata");
    }
  }

  if (!saw_lsn || !saw_manifest_version || !saw_created_at) {
    // Treat incomplete checkpoint state as absent so recovery replays WAL safely.
    return state;
  }

  state.exists = true;
  return state;
}

}  // namespace mxdb
