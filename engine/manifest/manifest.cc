#include "engine/manifest/manifest.h"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

namespace mxdb {

namespace {

constexpr char kEntryTypeAdd[] = "ADD";

Status WriteAll(int fd, const void* data, size_t bytes) {
  const auto* cursor = static_cast<const uint8_t*>(data);
  size_t remaining = bytes;
  while (remaining > 0) {
    const ssize_t written = ::write(fd, cursor, remaining);
    if (written <= 0) {
      return Status::Internal("manifest write failed: " +
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
    return Status::Internal("failed to open manifest directory for fsync: " +
                            std::string(std::strerror(errno)));
  }

  const int rc = ::fsync(dir_fd);
  const int saved_errno = errno;
  ::close(dir_fd);
  if (rc != 0) {
    return Status::Internal("failed to fsync manifest directory: " +
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
      return Status::Internal("failed to create manifest directory: " +
                              ec.message());
    }
  }

  const auto nonce = std::chrono::steady_clock::now().time_since_epoch().count();
  const std::filesystem::path tmp_path =
      target.string() + ".tmp." + std::to_string(nonce);
  const int fd = ::open(tmp_path.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644);
  if (fd < 0) {
    return Status::Internal("failed to open temporary manifest file: " +
                            std::string(std::strerror(errno)));
  }

  Status status = WriteAll(fd, contents.data(), contents.size());
  if (status.ok() && ::fsync(fd) != 0) {
    status = Status::Internal("failed to fsync manifest file: " +
                              std::string(std::strerror(errno)));
  }

  const int close_rc = ::close(fd);
  if (status.ok() && close_rc != 0) {
    status = Status::Internal("failed to close temporary manifest file: " +
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
    return Status::Internal("failed to atomically replace manifest file: " +
                            ec.message());
  }

  return FsyncDirectory(parent);
}

StatusOr<ManifestEntry> ParseEntryLine(const std::string& line) {
  std::vector<std::string> tokens;
  std::string token;
  std::istringstream in(line);
  while (std::getline(in, token, '\t')) {
    tokens.push_back(token);
  }

  if (tokens.size() != 10) {
    return Status::InvalidArgument("invalid manifest entry token count");
  }
  if (tokens[0] != kEntryTypeAdd) {
    return Status::InvalidArgument("unsupported manifest entry type");
  }

  ManifestEntry entry;
  entry.manifest_version = std::stoull(tokens[1]);
  entry.partition_id = static_cast<size_t>(std::stoull(tokens[2]));
  entry.segment_path = tokens[3];
  entry.min_event_time_us = std::stoll(tokens[4]);
  entry.max_event_time_us = std::stoll(tokens[5]);
  entry.min_system_time_us = std::stoll(tokens[6]);
  entry.max_system_time_us = std::stoll(tokens[7]);
  entry.max_lsn = std::stoull(tokens[8]);
  entry.row_count = std::stoull(tokens[9]);
  return entry;
}

std::string EncodeEntryLine(const ManifestEntry& entry) {
  std::ostringstream out;
  out << kEntryTypeAdd << '\t' << entry.manifest_version << '\t'
      << entry.partition_id << '\t' << entry.segment_path << '\t'
      << entry.min_event_time_us << '\t' << entry.max_event_time_us << '\t'
      << entry.min_system_time_us << '\t' << entry.max_system_time_us << '\t'
      << entry.max_lsn << '\t' << entry.row_count << '\n';
  return out.str();
}

}  // namespace

Status ManifestLog::Open(const std::string& manifest_path) {
  manifest_path_ = manifest_path;
  std::error_code ec;
  const auto parent = std::filesystem::path(manifest_path_).parent_path();
  if (!parent.empty()) {
    std::filesystem::create_directories(parent, ec);
    if (ec) {
      return Status::Internal("failed to create manifest directory: " +
                              ec.message());
    }
  }

  if (!std::filesystem::exists(manifest_path_)) {
    Status status = WriteFileAtomically(manifest_path_, "");
    if (!status.ok()) {
      return status;
    }
  }

  auto entries = LoadEntries();
  if (!entries.ok()) {
    return entries.status();
  }
  if (!entries.value().empty()) {
    version_ = entries.value().back().manifest_version;
  }

  return Status::Ok();
}

StatusOr<std::vector<ManifestEntry>> ManifestLog::LoadEntries() const {
  std::ifstream in(manifest_path_);
  if (!in.is_open()) {
    return Status::Internal("failed to open manifest file");
  }

  std::vector<ManifestEntry> entries;
  std::string line;
  while (std::getline(in, line)) {
    if (line.empty()) {
      continue;
    }
    auto entry = ParseEntryLine(line);
    if (!entry.ok()) {
      return entry.status();
    }
    entries.push_back(entry.value());
  }

  return entries;
}

StatusOr<ManifestEntry> ManifestLog::AppendSegment(const ManifestEntry& entry) {
  auto entries = LoadEntries();
  if (!entries.ok()) {
    return entries.status();
  }

  std::vector<ManifestEntry> next_entries = entries.value();
  next_entries.push_back(entry);
  Status status = RewriteEntries(next_entries);
  if (!status.ok()) {
    return status;
  }

  ManifestEntry persisted = entry;
  persisted.manifest_version = version_;
  return persisted;
}

Status ManifestLog::RewriteEntries(const std::vector<ManifestEntry>& entries) {
  std::ostringstream out;
  uint64_t version = 0;
  for (const auto& entry : entries) {
    ManifestEntry rewritten = entry;
    rewritten.manifest_version = ++version;
    out << EncodeEntryLine(rewritten);
  }

  if (!out.good()) {
    return Status::Internal("failed while buffering manifest rewrite");
  }

  Status status = WriteFileAtomically(manifest_path_, out.str());
  if (!status.ok()) {
    return status;
  }

  version_ = version;
  return Status::Ok();
}

}  // namespace mxdb
