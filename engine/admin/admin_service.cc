#include "engine/admin/admin_service.h"

#include <chrono>
#include <filesystem>

#include "engine/recovery/recovery_manager.h"

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
  const bool was_read_only = engine_->IsReadOnly();
  if (!was_read_only) {
    Status status = engine_->SetReadOnly(true);
    if (!status.ok()) {
      return status;
    }
  }

  Status status = engine_->TriggerCheckpoint();
  if (!status.ok()) {
    if (!was_read_only) {
      (void)engine_->SetReadOnly(false);
    }
    return status;
  }

  std::error_code ec;
  std::filesystem::create_directories(destination_dir, ec);
  if (ec) {
    if (!was_read_only) {
      (void)engine_->SetReadOnly(false);
    }
    return Status::Internal("failed to create backup destination: " + ec.message());
  }

  const std::string data_backup_dir =
      (std::filesystem::path(destination_dir) / "data").string();
  const std::string data_backup_staging =
      (std::filesystem::path(destination_dir) /
       ("data" + TempPathSuffix("backup-staging")))
          .string();

  status = RemoveIfExists(data_backup_staging);
  if (!status.ok()) {
    if (!was_read_only) {
      (void)engine_->SetReadOnly(false);
    }
    return status;
  }

  status = CopyPath(config_->data_dir, data_backup_staging, /*recursive=*/true);
  if (status.ok()) {
    status = RemoveIfExists(data_backup_dir);
  }
  if (status.ok()) {
    std::filesystem::rename(data_backup_staging, data_backup_dir, ec);
    if (ec) {
      status = Status::Internal("failed to publish backup snapshot: " + ec.message());
    }
  }
  if (!status.ok()) {
    (void)RemoveIfExists(data_backup_staging);
  }

  if (!was_read_only) {
    Status restore_mode = engine_->SetReadOnly(false);
    if (status.ok() && !restore_mode.ok()) {
      status = restore_mode;
    }
  }

  return status;
}

Status AdminService::RestoreBackup(const std::string& source_dir,
                                   bool start_read_only) {
  if (metadata_store_ == nullptr) {
    return Status::FailedPrecondition(
        "restore requires an attached metadata store handle");
  }

  const std::string data_source =
      (std::filesystem::path(source_dir) / "data").string();
  std::error_code ec;
  if (!std::filesystem::exists(data_source, ec)) {
    return Status::NotFound("path not found for backup/restore: " + data_source);
  }

  const bool was_read_only = engine_->IsReadOnly();
  Status status = engine_->SetReadOnly(true);
  if (!status.ok()) {
    return status;
  }

  status = engine_->Stop();
  if (!status.ok()) {
    if (!was_read_only) {
      (void)engine_->SetReadOnly(false);
    }
    return status;
  }

  status = metadata_store_->Close();
  if (!status.ok()) {
    (void)engine_->Start();
    (void)engine_->SetReadOnly(true);
    return status;
  }

  const std::filesystem::path target_data = config_->data_dir;
  const std::filesystem::path stage_data =
      target_data.parent_path() /
      (target_data.filename().string() + TempPathSuffix("restore-staging"));

  status = RemoveIfExists(stage_data);
  if (status.ok()) {
    status = CopyPath(data_source, stage_data.string(), /*recursive=*/true);
  }
  if (status.ok()) {
    status = SwapInDirectory(stage_data, target_data);
  } else {
    (void)RemoveIfExists(stage_data);
  }

  Status reopen_status = metadata_store_->Open(config_->metadata_path);
  if (status.ok() && !reopen_status.ok()) {
    status = reopen_status;
  }

  Status start_status = engine_->Start();
  if (status.ok() && !start_status.ok()) {
    status = start_status;
  }

  if (status.ok()) {
    RecoveryManager recovery(engine_);
    bool had_truncated_tail = false;
    Status recovery_status =
        recovery.RecoverFromWalDirectory(config_->wal_dir, &had_truncated_tail);
    if (!recovery_status.ok()) {
      status = recovery_status;
    }
  }

  Status mode_status = engine_->SetReadOnly(start_read_only);
  if (status.ok() && !mode_status.ok()) {
    status = mode_status;
  }

  return status;
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

Status AdminService::RemoveIfExists(const std::filesystem::path& path) {
  std::error_code ec;
  if (!std::filesystem::exists(path, ec)) {
    return Status::Ok();
  }

  std::filesystem::remove_all(path, ec);
  if (ec) {
    return Status::Internal("failed to remove path: " + ec.message());
  }
  return Status::Ok();
}

Status AdminService::SwapInDirectory(const std::filesystem::path& prepared_source,
                                     const std::filesystem::path& target) {
  std::error_code ec;
  if (!std::filesystem::exists(prepared_source, ec)) {
    return Status::NotFound("prepared restore directory not found");
  }

  const std::filesystem::path parent = target.parent_path();
  if (!parent.empty()) {
    std::filesystem::create_directories(parent, ec);
    if (ec) {
      return Status::Internal("failed to create restore parent directory: " +
                              ec.message());
    }
  }

  const std::filesystem::path old_target =
      parent / (target.filename().string() + TempPathSuffix("restore-old"));
  bool moved_old_target = false;
  if (std::filesystem::exists(target, ec)) {
    std::filesystem::rename(target, old_target, ec);
    if (ec) {
      return Status::Internal("failed to move current data directory aside: " +
                              ec.message());
    }
    moved_old_target = true;
  }

  std::filesystem::rename(prepared_source, target, ec);
  if (ec) {
    if (moved_old_target) {
      std::error_code rollback_ec;
      std::filesystem::rename(old_target, target, rollback_ec);
    }
    return Status::Internal("failed to publish restored data directory: " +
                            ec.message());
  }

  if (moved_old_target) {
    Status cleanup_status = RemoveIfExists(old_target);
    if (!cleanup_status.ok()) {
      return cleanup_status;
    }
  }

  return Status::Ok();
}

std::string AdminService::TempPathSuffix(const std::string& prefix) {
  const auto now = std::chrono::steady_clock::now().time_since_epoch().count();
  return "." + prefix + "-" + std::to_string(now);
}

}  // namespace mxdb
