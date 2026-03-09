#pragma once

#include <filesystem>
#include <string>

#include "engine/common/status/status.h"

namespace mxdb {

class DataDirProcessLock {
 public:
  DataDirProcessLock() = default;
  ~DataDirProcessLock();

  DataDirProcessLock(const DataDirProcessLock&) = delete;
  DataDirProcessLock& operator=(const DataDirProcessLock&) = delete;
  DataDirProcessLock(DataDirProcessLock&& other) noexcept;
  DataDirProcessLock& operator=(DataDirProcessLock&& other) noexcept;

  static StatusOr<DataDirProcessLock> Acquire(const std::string& data_dir,
                                              const std::string& owner);
  static StatusOr<std::filesystem::path> ResolveLockPath(
      const std::string& data_dir);

  void Release();
  const std::filesystem::path& lock_path() const { return lock_path_; }
  bool held() const { return held_; }

 private:
#ifdef _WIN32
  void* handle_ = nullptr;
#else
  int fd_ = -1;
#endif
  std::filesystem::path lock_path_;
  bool held_ = false;
};

}  // namespace mxdb
