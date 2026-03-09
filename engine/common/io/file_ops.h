#pragma once

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <limits>
#include <string>
#include <system_error>

#include "engine/common/status/status.h"

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <Windows.h>
#include <fcntl.h>
#include <io.h>
#include <sys/stat.h>
#else
#include <fcntl.h>
#include <unistd.h>
#endif

namespace mxdb::io {

inline std::string ErrnoMessage() { return std::string(std::strerror(errno)); }

#ifdef _WIN32
inline std::string WindowsErrorMessage(unsigned long code) {
  wchar_t buffer[512] = {};
  const unsigned long size = ::FormatMessageW(
      FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
      nullptr, code, MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
      buffer, 512, nullptr);
  if (size == 0) {
    return "error code " + std::to_string(code);
  }

  std::wstring wide(buffer, size);
  while (!wide.empty() &&
         (wide.back() == L'\r' || wide.back() == L'\n' || wide.back() == L' ' ||
          wide.back() == L'\t')) {
    wide.pop_back();
  }

  const int utf8_size = ::WideCharToMultiByte(CP_UTF8, 0, wide.c_str(),
                                              static_cast<int>(wide.size()),
                                              nullptr, 0, nullptr, nullptr);
  if (utf8_size <= 0) {
    return "error code " + std::to_string(code);
  }

  std::string out(static_cast<size_t>(utf8_size), '\0');
  (void)::WideCharToMultiByte(CP_UTF8, 0, wide.c_str(),
                              static_cast<int>(wide.size()), out.data(),
                              utf8_size, nullptr, nullptr);
  return out;
}
#endif

inline int OpenReadOnly(const std::string& path) {
#ifdef _WIN32
  return ::_open(path.c_str(), _O_RDONLY | _O_BINARY);
#else
  return ::open(path.c_str(), O_RDONLY);
#endif
}

inline int OpenWriteTruncate(const std::string& path) {
#ifdef _WIN32
  return ::_open(path.c_str(),
                 _O_CREAT | _O_TRUNC | _O_WRONLY | _O_BINARY,
                 _S_IREAD | _S_IWRITE);
#else
  return ::open(path.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644);
#endif
}

inline int OpenWriteAppend(const std::string& path) {
#ifdef _WIN32
  return ::_open(path.c_str(), _O_CREAT | _O_WRONLY | _O_APPEND | _O_BINARY,
                 _S_IREAD | _S_IWRITE);
#else
  return ::open(path.c_str(), O_CREAT | O_WRONLY | O_APPEND, 0644);
#endif
}

inline Status WriteAll(int fd, const void* data, size_t bytes,
                       const std::string& context) {
  const uint8_t* cursor = static_cast<const uint8_t*>(data);
  size_t remaining = bytes;
  while (remaining > 0) {
#ifdef _WIN32
    const unsigned int chunk =
        remaining > static_cast<size_t>(std::numeric_limits<unsigned int>::max())
            ? std::numeric_limits<unsigned int>::max()
            : static_cast<unsigned int>(remaining);
    const int written = ::_write(fd, cursor, chunk);
#else
    const ssize_t written = ::write(fd, cursor, remaining);
#endif
    if (written <= 0) {
      return Status::Internal(context + ": " + ErrnoMessage());
    }
    remaining -= static_cast<size_t>(written);
    cursor += written;
  }
  return Status::Ok();
}

inline Status SyncFile(int fd, const std::string& context) {
#ifdef _WIN32
  if (::_commit(fd) != 0) {
#else
  if (::fsync(fd) != 0) {
#endif
    return Status::Internal(context + ": " + ErrnoMessage());
  }
  return Status::Ok();
}

inline Status CloseFile(int fd, const std::string& context) {
#ifdef _WIN32
  if (::_close(fd) != 0) {
#else
  if (::close(fd) != 0) {
#endif
    return Status::Internal(context + ": " + ErrnoMessage());
  }
  return Status::Ok();
}

inline Status SyncDirectory(const std::filesystem::path& dir,
                            const std::string& context) {
  const std::filesystem::path resolved = dir.empty() ? std::filesystem::path(".") : dir;
#ifdef _WIN32
  const std::wstring dir_path = resolved.wstring();
  HANDLE handle =
      ::CreateFileW(dir_path.c_str(), GENERIC_READ,
                    FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                    nullptr, OPEN_EXISTING, FILE_FLAG_BACKUP_SEMANTICS, nullptr);
  if (handle == INVALID_HANDLE_VALUE) {
    return Status::Internal(context + ": failed to open directory: " +
                            WindowsErrorMessage(::GetLastError()));
  }

  if (!::FlushFileBuffers(handle)) {
    const unsigned long flush_error = ::GetLastError();
    (void)::CloseHandle(handle);
    // Some file systems do not support directory flush handles.
    if (flush_error == ERROR_INVALID_HANDLE) {
      return Status::Ok();
    }
    return Status::Internal(context + ": failed to flush directory: " +
                            WindowsErrorMessage(flush_error));
  }

  if (!::CloseHandle(handle)) {
    return Status::Internal(context + ": failed to close directory handle: " +
                            WindowsErrorMessage(::GetLastError()));
  }
  return Status::Ok();
#else
  const int dir_fd = OpenReadOnly(resolved.string());
  if (dir_fd < 0) {
    return Status::Internal(context + ": failed to open directory: " +
                            ErrnoMessage());
  }

  Status status = SyncFile(dir_fd, context + ": failed to fsync directory");
  Status close_status = CloseFile(dir_fd, context + ": failed to close directory");
  if (!status.ok()) {
    return status;
  }
  return close_status;
#endif
}

inline Status AtomicReplace(const std::filesystem::path& source,
                            const std::filesystem::path& target,
                            const std::string& context) {
#ifdef _WIN32
  const std::wstring source_w = source.wstring();
  const std::wstring target_w = target.wstring();
  if (!::MoveFileExW(source_w.c_str(), target_w.c_str(),
                     MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH)) {
    return Status::Internal(context + ": failed to atomically replace: " +
                            WindowsErrorMessage(::GetLastError()));
  }
  return Status::Ok();
#else
  std::error_code ec;
  std::filesystem::rename(source, target, ec);
  if (ec) {
    return Status::Internal(context + ": failed to atomically replace: " +
                            ec.message());
  }
  return Status::Ok();
#endif
}

}  // namespace mxdb::io
