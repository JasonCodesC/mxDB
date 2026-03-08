#pragma once

#include <string>

namespace mxdb {

enum class StatusCode {
  kOk = 0,
  kInvalidArgument,
  kNotFound,
  kAlreadyExists,
  kPermissionDenied,
  kUnavailable,
  kFailedPrecondition,
  kInternal,
};

class Status {
 public:
  Status() = default;
  Status(StatusCode code, std::string message)
      : code_(code), message_(std::move(message)) {}

  static Status Ok() { return Status(); }
  static Status InvalidArgument(std::string message) {
    return Status(StatusCode::kInvalidArgument, std::move(message));
  }
  static Status NotFound(std::string message) {
    return Status(StatusCode::kNotFound, std::move(message));
  }
  static Status AlreadyExists(std::string message) {
    return Status(StatusCode::kAlreadyExists, std::move(message));
  }
  static Status PermissionDenied(std::string message) {
    return Status(StatusCode::kPermissionDenied, std::move(message));
  }
  static Status Unavailable(std::string message) {
    return Status(StatusCode::kUnavailable, std::move(message));
  }
  static Status FailedPrecondition(std::string message) {
    return Status(StatusCode::kFailedPrecondition, std::move(message));
  }
  static Status Internal(std::string message) {
    return Status(StatusCode::kInternal, std::move(message));
  }

  bool ok() const { return code_ == StatusCode::kOk; }
  StatusCode code() const { return code_; }
  const std::string& message() const { return message_; }

 private:
  StatusCode code_ = StatusCode::kOk;
  std::string message_;
};

template <typename T>
class StatusOr {
 public:
  StatusOr(const Status& status) : status_(status) {}
  StatusOr(Status&& status) : status_(std::move(status)) {}
  StatusOr(const T& value) : value_(value) {}
  StatusOr(T&& value) : value_(std::move(value)) {}

  bool ok() const { return status_.ok(); }
  const Status& status() const { return status_; }
  const T& value() const { return value_; }
  T& value() { return value_; }

 private:
  Status status_ = Status::Ok();
  T value_{};
};

}  // namespace mxdb
