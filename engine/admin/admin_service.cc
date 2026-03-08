#include "engine/admin/admin_service.h"

#include <filesystem>

namespace mxdb {

HealthStatus AdminService::GetHealth() const {
  HealthStatus health;
  health.current_lsn = engine_->CurrentLsn();
  health.manifest_version = engine_->ManifestVersion();
  health.read_only = engine_->IsReadOnly();

  if (health.read_only) {
    health.state = "READ_ONLY";
  } else if (engine_->SegmentCount() > 1000) {
    health.state = "COMPACTION_BACKLOG";
    health.warnings.push_back("high immutable segment count");
  } else {
    health.state = "HEALTHY";
  }

  return health;
}

CompactionStatus AdminService::GetCompactionStatus() const {
  CompactionStatus status;
  const size_t segments = engine_->SegmentCount();
  status.queued_tasks = segments > 0 ? static_cast<uint32_t>(segments - 1) : 0;
  status.running_tasks = 0;
  status.bytes_pending = 0;
  return status;
}

Status AdminService::TriggerCheckpoint(bool /*wait*/) {
  return engine_->TriggerCheckpoint();
}

Status AdminService::TriggerCompaction() { return engine_->CompactImmutableSegments(); }

Status AdminService::SetReadOnlyMode(bool read_only) {
  return engine_->SetReadOnly(read_only);
}

Status AdminService::StartBackup(const std::string& destination_dir) {
  Status status = engine_->TriggerCheckpoint();
  if (!status.ok()) {
    return status;
  }

  std::error_code ec;
  std::filesystem::create_directories(destination_dir, ec);
  if (ec) {
    return Status::Internal("failed to create backup destination: " + ec.message());
  }

  const std::string data_backup_dir =
      (std::filesystem::path(destination_dir) / "data").string();

  return CopyPath(config_->data_dir, data_backup_dir, /*recursive=*/true);
}

Status AdminService::RestoreBackup(const std::string& source_dir,
                                   bool start_read_only) {
  const std::string data_source =
      (std::filesystem::path(source_dir) / "data").string();

  Status status = CopyPath(data_source, config_->data_dir, /*recursive=*/true);
  if (!status.ok()) {
    return status;
  }

  return engine_->SetReadOnly(start_read_only);
}

Status AdminService::CopyPath(const std::string& from, const std::string& to,
                              bool recursive) {
  std::error_code ec;
  if (!std::filesystem::exists(from, ec)) {
    return Status::NotFound("path not found for backup/restore: " + from);
  }

  if (recursive) {
    std::filesystem::create_directories(to, ec);
    if (ec) {
      return Status::Internal("failed to create destination path: " + ec.message());
    }

    std::filesystem::copy(from, to,
                          std::filesystem::copy_options::recursive |
                              std::filesystem::copy_options::overwrite_existing,
                          ec);
    if (ec) {
      return Status::Internal("recursive copy failed: " + ec.message());
    }
    return Status::Ok();
  }

  std::filesystem::copy_file(from, to,
                             std::filesystem::copy_options::overwrite_existing,
                             ec);
  if (ec) {
    return Status::Internal("file copy failed: " + ec.message());
  }
  return Status::Ok();
}

}  // namespace mxdb
