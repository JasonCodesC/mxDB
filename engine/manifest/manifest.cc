#include "engine/manifest/manifest.h"

#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

namespace mxdb {

namespace {

constexpr char kEntryTypeAdd[] = "ADD";

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
    std::ofstream out(manifest_path_, std::ios::out | std::ios::trunc);
    if (!out.is_open()) {
      return Status::Internal("failed to create manifest file");
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
  ManifestEntry persisted = entry;
  persisted.manifest_version = version_ + 1;

  std::ofstream out(manifest_path_, std::ios::out | std::ios::app);
  if (!out.is_open()) {
    return Status::Internal("failed to append manifest entry");
  }

  out << EncodeEntryLine(persisted);
  if (!out.good()) {
    return Status::Internal("failed to write manifest entry");
  }

  version_ = persisted.manifest_version;
  return persisted;
}

Status ManifestLog::RewriteEntries(const std::vector<ManifestEntry>& entries) {
  std::ofstream out(manifest_path_, std::ios::out | std::ios::trunc);
  if (!out.is_open()) {
    return Status::Internal("failed to rewrite manifest");
  }

  uint64_t version = 0;
  for (const auto& entry : entries) {
    ManifestEntry rewritten = entry;
    rewritten.manifest_version = ++version;
    out << EncodeEntryLine(rewritten);
    if (!out.good()) {
      return Status::Internal("failed while rewriting manifest");
    }
  }

  version_ = version;
  return Status::Ok();
}

}  // namespace mxdb
