#include "engine/common/logging/logging.h"

#include <chrono>
#include <iostream>

namespace mxdb {

namespace {

const char* ToString(LogLevel level) {
  switch (level) {
    case LogLevel::kDebug:
      return "DEBUG";
    case LogLevel::kInfo:
      return "INFO";
    case LogLevel::kWarn:
      return "WARN";
    case LogLevel::kError:
      return "ERROR";
  }
  return "UNKNOWN";
}

}  // namespace

Logger& Logger::Instance() {
  static Logger logger;
  return logger;
}

void Logger::SetMinLevel(LogLevel level) {
  std::lock_guard<std::mutex> lock(mu_);
  min_level_ = level;
}

void Logger::Log(LogLevel level, const std::string& message) {
  std::lock_guard<std::mutex> lock(mu_);
  if (level < min_level_) {
    return;
  }

  const auto now = std::chrono::system_clock::now();
  const auto epoch_us = std::chrono::duration_cast<std::chrono::microseconds>(
      now.time_since_epoch());

  std::cerr << epoch_us.count() << " [" << ToString(level) << "] " << message
            << '\n';
}

}  // namespace mxdb
