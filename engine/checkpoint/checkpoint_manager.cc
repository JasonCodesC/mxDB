#include "engine/checkpoint/checkpoint_manager.h"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstring>
#include <exception>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>

namespace mxdb {

namespace {

Status WriteAll(int fd, const void* data, size_t bytes) {
  const auto* cursor = static_cast<const uint8_t*>(data);
  size_t remaining = bytes;
  while (remaining > 0) {
    const ssize_t written = ::write(fd, cursor, remaining);
    if (written <= 0) {
      return Status::Internal("checkpoint write failed: " +
                              std::string(std::strerror(errno)));
    }
    remaining -= static_cast<size_t>(written);
    cursor += written;
  }
  return Status::Ok();
}

Status FsyncDirectory(const std::filesystem::path& dir) {
  const std::string dir_str = dir.empty() ? "." : dir.string();
  const int dir_fd = ::open(dir_str.c_str(), O_RDONLY);
  if (dir_fd < 0) {
    return Status::Internal("failed to open checkpoint directory for fsync: " +
                            std::string(std::strerror(errno)));
  }

  const int rc = ::fsync(dir_fd);
  const int saved_errno = errno;
  ::close(dir_fd);
  if (rc != 0) {
    return Status::Internal("failed to fsync checkpoint directory: " +
                            std::string(std::strerror(saved_errno)));
  }
  return Status::Ok();
}

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
  const int fd = ::open(tmp_path.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644);
  if (fd < 0) {
    return Status::Internal("failed to open temporary checkpoint file: " +
                            std::string(std::strerror(errno)));
  }

  Status status = WriteAll(fd, contents.data(), contents.size());
  if (status.ok() && ::fsync(fd) != 0) {
    status = Status::Internal("failed to fsync checkpoint file: " +
                              std::string(std::strerror(errno)));
  }

  const int close_rc = ::close(fd);
  if (status.ok() && close_rc != 0) {
    status = Status::Internal("failed to close temporary checkpoint file: " +
                              std::string(std::strerror(errno)));
  }

  if (!status.ok()) {
    std::error_code ignore_ec;
    std::filesystem::remove(tmp_path, ignore_ec);
    return status;
  }

  std::filesystem::rename(tmp_path, target, ec);
  if (ec) {
    std::filesystem::remove(tmp_path, ec);
    return Status::Internal("failed to atomically replace checkpoint file: " +
                            ec.message());
  }

  return FsyncDirectory(parent);
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
