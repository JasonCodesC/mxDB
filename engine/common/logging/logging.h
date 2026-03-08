#pragma once

#include <mutex>
#include <string>

namespace mxdb {

enum class LogLevel {
  kDebug = 0,
  kInfo = 1,
  kWarn = 2,
  kError = 3,
};

class Logger {
 public:
  static Logger& Instance();

  void SetMinLevel(LogLevel level);
  void Log(LogLevel level, const std::string& message);

 private:
  Logger() = default;

  std::mutex mu_;
  LogLevel min_level_ = LogLevel::kInfo;
};

}  // namespace mxdb
