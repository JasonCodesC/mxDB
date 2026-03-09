#include "engine/common/process_lock/data_dir_lock.h"

#include <cerrno>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <string>

#ifdef _WIN32
#include <windows.h>
#else
#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

namespace mxdb {

namespace {

StatusOr<std::filesystem::path> NormalizePath(
    const std::filesystem::path& raw_path) {
  std::error_code ec;
  std::filesystem::path absolute = std::filesystem::absolute(raw_path, ec);
  if (ec) {
    return Status::Internal("failed to resolve absolute path: " + ec.message());
  }

  std::filesystem::path normalized = std::filesystem::weakly_canonical(absolute, ec);
  if (ec) {
    normalized = absolute.lexically_normal();
  }
  return normalized;
}

void WriteOwnerMetadata(
#ifdef _WIN32
    void* handle,
#else
    int fd,
#endif
    const std::string& owner) {
  const auto now =
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count();

#ifdef _WIN32
  DWORD pid = GetCurrentProcessId();
  const std::string payload =
      owner + " pid=" + std::to_string(pid) + " at_us=" + std::to_string(now) +
      "\n";

  HANDLE file = static_cast<HANDLE>(handle);
  SetFilePointer(file, 0, nullptr, FILE_BEGIN);
  SetEndOfFile(file);
  DWORD written = 0;
  (void)WriteFile(file, payload.data(), static_cast<DWORD>(payload.size()),
                  &written, nullptr);
#else
  const std::string payload =
      owner + " pid=" + std::to_string(getpid()) + " at_us=" + std::to_string(now) +
      "\n";
  (void)ftruncate(fd, 0);
  (void)lseek(fd, 0, SEEK_SET);
  (void)write(fd, payload.data(), payload.size());
#endif
}

}  // namespace

DataDirProcessLock::~DataDirProcessLock() { Release(); }

DataDirProcessLock::DataDirProcessLock(DataDirProcessLock&& other) noexcept
    : lock_path_(std::move(other.lock_path_)), held_(other.held_) {
#ifdef _WIN32
  handle_ = other.handle_;
  other.handle_ = nullptr;
#else
  fd_ = other.fd_;
  other.fd_ = -1;
#endif
  other.held_ = false;
}

DataDirProcessLock& DataDirProcessLock::operator=(
    DataDirProcessLock&& other) noexcept {
  if (this != &other) {
    Release();

    lock_path_ = std::move(other.lock_path_);
    held_ = other.held_;

#ifdef _WIN32
    handle_ = other.handle_;
    other.handle_ = nullptr;
#else
    fd_ = other.fd_;
    other.fd_ = -1;
#endif
    other.held_ = false;
  }
  return *this;
}

StatusOr<std::filesystem::path> DataDirProcessLock::ResolveLockPath(
    const std::string& data_dir) {
  auto normalized = NormalizePath(std::filesystem::path(data_dir));
  if (!normalized.ok()) {
    return normalized.status();
  }

  std::filesystem::path root = normalized.value();
  std::filesystem::path parent = root.parent_path();
  if (parent.empty()) {
    parent = std::filesystem::current_path();
  }

  std::string leaf = root.filename().string();
  if (leaf.empty()) {
    leaf = "mxdb-data";
  }
  return (parent / (leaf + ".mxdb.process.lock")).lexically_normal();
}

StatusOr<DataDirProcessLock> DataDirProcessLock::Acquire(
    const std::string& data_dir, const std::string& owner) {
  std::error_code ec;
  std::filesystem::create_directories(std::filesystem::path(data_dir), ec);
  if (ec) {
    return Status::Internal("failed to create data_dir for process lock: " +
                            ec.message());
  }

  auto lock_path_or = ResolveLockPath(data_dir);
  if (!lock_path_or.ok()) {
    return lock_path_or.status();
  }

  DataDirProcessLock lock;
  lock.lock_path_ = lock_path_or.value();

#ifdef _WIN32
  const std::string lock_file = lock.lock_path_.string();
  HANDLE handle = CreateFileA(
      lock_file.c_str(), GENERIC_READ | GENERIC_WRITE,
      FILE_SHARE_READ | FILE_SHARE_WRITE, nullptr, OPEN_ALWAYS,
      FILE_ATTRIBUTE_NORMAL, nullptr);
  if (handle == INVALID_HANDLE_VALUE) {
    return Status::Internal("failed to open process lock file");
  }

  OVERLAPPED ov{};
  if (!LockFileEx(handle,
                  LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY, 0,
                  MAXDWORD, MAXDWORD, &ov)) {
    const DWORD err = GetLastError();
    CloseHandle(handle);
    if (err == ERROR_LOCK_VIOLATION || err == ERROR_SHARING_VIOLATION) {
      return Status::FailedPrecondition(
          "another mxdb process is already using this data_dir");
    }
    return Status::Internal("failed to acquire process lock");
  }

  lock.handle_ = handle;
  lock.held_ = true;
  WriteOwnerMetadata(lock.handle_, owner);
#else
  const int fd = open(lock.lock_path_.c_str(), O_RDWR | O_CREAT, 0644);
  if (fd < 0) {
    return Status::Internal("failed to open process lock file: " +
                            std::string(std::strerror(errno)));
  }

  if (flock(fd, LOCK_EX | LOCK_NB) != 0) {
    const int lock_errno = errno;
    close(fd);
    if (lock_errno == EWOULDBLOCK) {
      return Status::FailedPrecondition(
          "another mxdb process is already using this data_dir");
    }
    return Status::Internal("failed to acquire process lock: " +
                            std::string(std::strerror(lock_errno)));
  }

  lock.fd_ = fd;
  lock.held_ = true;
  WriteOwnerMetadata(lock.fd_, owner);
#endif

  return lock;
}

void DataDirProcessLock::Release() {
  if (!held_) {
    return;
  }

#ifdef _WIN32
  HANDLE handle = static_cast<HANDLE>(handle_);
  if (handle != nullptr && handle != INVALID_HANDLE_VALUE) {
    OVERLAPPED ov{};
    (void)UnlockFileEx(handle, 0, MAXDWORD, MAXDWORD, &ov);
    (void)CloseHandle(handle);
  }
  handle_ = nullptr;
#else
  if (fd_ >= 0) {
    (void)flock(fd_, LOCK_UN);
    (void)close(fd_);
  }
  fd_ = -1;
#endif

  held_ = false;
}

}  // namespace mxdb
