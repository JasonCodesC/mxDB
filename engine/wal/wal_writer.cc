#include "engine/wal/wal_writer.h"

#include <cstring>
#include <filesystem>
#include <sstream>

#include "engine/common/io/file_ops.h"
#include "engine/common/crc32/crc32.h"

namespace mxdb {

namespace {

uint32_t ParseWalIndex(const std::filesystem::path& path) {
  const std::string name = path.filename().string();
  if (name.rfind("wal-", 0) != 0 || name.size() < 12 ||
      name.substr(name.size() - 4) != ".log") {
    return 0;
  }
  return static_cast<uint32_t>(std::stoul(name.substr(4, name.size() - 8)));
}

}  // namespace

WalWriter::~WalWriter() { (void)Close(); }

Status WalWriter::Open(const std::string& wal_dir, size_t segment_target_bytes,
                       uint64_t group_commit_window_ms,
                       size_t group_commit_max_records) {
  std::lock_guard<std::mutex> lock(mu_);
  wal_dir_ = wal_dir;
  segment_target_bytes_ = segment_target_bytes;
  group_commit_window_ms_ = group_commit_window_ms;
  group_commit_max_records_ = group_commit_max_records;
  pending_since_fsync_ = 0;
  last_fsync_at_ = std::chrono::steady_clock::now();

  std::error_code ec;
  std::filesystem::create_directories(wal_dir_, ec);
  if (ec) {
    return Status::Internal("failed to create wal directory: " + ec.message());
  }

  uint32_t max_index = 0;
  for (const auto& entry : std::filesystem::directory_iterator(wal_dir_)) {
    if (!entry.is_regular_file()) {
      continue;
    }
    max_index = std::max(max_index, ParseWalIndex(entry.path()));
  }
  segment_index_ = max_index == 0 ? 1 : max_index + 1;

  return EnsureSegmentOpen();
}

Status WalWriter::Close() {
  std::lock_guard<std::mutex> lock(mu_);
  if (fd_ < 0) {
    return Status::Ok();
  }

  Status status = FsyncCurrentSegment();
  if (!status.ok()) {
    return status;
  }

  status = io::CloseFile(fd_, "close WAL fd failed");
  if (!status.ok()) {
    return status;
  }
  fd_ = -1;
  return Status::Ok();
}

Status WalWriter::Append(Lsn lsn, const WalBatchPayload& payload,
                         DurabilityMode mode) {
  std::lock_guard<std::mutex> lock(mu_);
  Status status = EnsureSegmentOpen();
  if (!status.ok()) {
    return status;
  }

  std::vector<uint8_t> payload_bytes = SerializeWalBatch(payload);

  WalRecordHeader header;
  header.payload_length = static_cast<uint32_t>(payload_bytes.size());
  header.lsn = lsn;
  header.crc32 = Crc32(payload_bytes.data(), payload_bytes.size());

  status = WriteAll(&header, sizeof(header));
  if (!status.ok()) {
    return status;
  }

  status = WriteAll(payload_bytes.data(), payload_bytes.size());
  if (!status.ok()) {
    return status;
  }

  current_segment_bytes_ += sizeof(header) + payload_bytes.size();
  pending_since_fsync_ += 1;

  const auto now = std::chrono::steady_clock::now();
  const auto elapsed =
      std::chrono::duration_cast<std::chrono::milliseconds>(now - last_fsync_at_)
          .count();

  if (mode == DurabilityMode::kSync) {
    status = FsyncCurrentSegment();
    if (!status.ok()) {
      return status;
    }
  } else if (mode == DurabilityMode::kGroupCommit) {
    if (pending_since_fsync_ >= group_commit_max_records_ ||
        static_cast<uint64_t>(elapsed) >= group_commit_window_ms_) {
      status = FsyncCurrentSegment();
      if (!status.ok()) {
        return status;
      }
    }
  }

  if (current_segment_bytes_ >= segment_target_bytes_) {
    status = RotateSegment();
    if (!status.ok()) {
      return status;
    }
  }

  return Status::Ok();
}

Status WalWriter::EnsureSegmentOpen() {
  if (fd_ >= 0) {
    return Status::Ok();
  }

  const std::string path = SegmentPath(segment_index_);
  fd_ = io::OpenWriteAppend(path);
  if (fd_ < 0) {
    return Status::Internal("failed to open WAL segment: " + path);
  }

  std::error_code ec;
  const auto segment_size = std::filesystem::file_size(path, ec);
  if (ec) {
    current_segment_bytes_ = 0;
  } else {
    current_segment_bytes_ = static_cast<size_t>(segment_size);
  }

  return Status::Ok();
}

Status WalWriter::RotateSegment() {
  Status status = FsyncCurrentSegment();
  if (!status.ok()) {
    return status;
  }

  if (fd_ >= 0) {
    status = io::CloseFile(fd_, "failed to close WAL segment during rotation");
    if (!status.ok()) {
      return status;
    }
  }

  fd_ = -1;
  current_segment_bytes_ = 0;
  ++segment_index_;

  return EnsureSegmentOpen();
}

Status WalWriter::FsyncCurrentSegment() {
  if (fd_ < 0) {
    return Status::Ok();
  }
  Status status = io::SyncFile(fd_, "fsync failed");
  if (!status.ok()) {
    return status;
  }
  pending_since_fsync_ = 0;
  last_fsync_at_ = std::chrono::steady_clock::now();
  return Status::Ok();
}

Status WalWriter::WriteAll(const void* data, size_t bytes) {
  return io::WriteAll(fd_, data, bytes, "write to WAL failed");
}

std::string WalWriter::SegmentPath(uint32_t index) const {
  std::ostringstream out;
  out << wal_dir_ << "/wal-";
  out.width(6);
  out.fill('0');
  out << index << ".log";
  return out.str();
}

}  // namespace mxdb
