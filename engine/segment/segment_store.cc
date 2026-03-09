#include "engine/segment/segment_store.h"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <sstream>

#include "engine/common/io/file_ops.h"
#include "engine/common/crc32/crc32.h"
#include "engine/wal/wal_record.h"

namespace mxdb {

namespace {

constexpr uint32_t kSegmentMagic = 0x53454731U;  // SEG1
constexpr uint16_t kSegmentVersion = 1;

#pragma pack(push, 1)
struct SegmentFileHeader {
  uint32_t magic = kSegmentMagic;
  uint16_t version = kSegmentVersion;
  uint16_t reserved = 0;
  uint32_t payload_length = 0;
  uint32_t crc32 = 0;
};
#pragma pack(pop)

bool IsNewer(const FeatureEvent& lhs, const FeatureEvent& rhs) {
  if (lhs.event_time_us != rhs.event_time_us) {
    return lhs.event_time_us > rhs.event_time_us;
  }
  if (lhs.system_time_us != rhs.system_time_us) {
    return lhs.system_time_us > rhs.system_time_us;
  }
  return lhs.sequence_no > rhs.sequence_no;
}

}  // namespace

Status SegmentStore::Open(const std::string& segment_dir) {
  segment_dir_ = segment_dir;
  std::error_code ec;
  std::filesystem::create_directories(segment_dir_, ec);
  if (ec) {
    return Status::Internal("failed to create segment directory: " + ec.message());
  }
  return Status::Ok();
}

StatusOr<SegmentData> SegmentStore::WriteSegment(
    size_t partition_id, uint64_t segment_id,
    const std::vector<FeatureEvent>& events) {
  if (events.empty()) {
    return Status::InvalidArgument("cannot flush empty segment");
  }

  WalBatchPayload payload;
  payload.events = events;
  payload.commit_system_time_us = 0;
  for (const auto& event : events) {
    payload.commit_system_time_us =
        std::max(payload.commit_system_time_us, event.system_time_us);
  }

  std::vector<uint8_t> payload_bytes = SerializeWalBatch(payload);

  SegmentFileHeader header;
  header.payload_length = static_cast<uint32_t>(payload_bytes.size());
  header.crc32 = Crc32(payload_bytes.data(), payload_bytes.size());

  const std::string path = SegmentPath(partition_id, segment_id);
  const auto parent = std::filesystem::path(path).parent_path();
  std::error_code ec;
  std::filesystem::create_directories(parent, ec);
  if (ec) {
    return Status::Internal("failed to create segment partition directory: " +
                            ec.message());
  }

  const int fd = io::OpenWriteTruncate(path);
  if (fd < 0) {
    return Status::Internal("failed to open segment file for write");
  }

  Status status = io::WriteAll(fd, &header, sizeof(header), "segment write failed");
  if (status.ok()) {
    status = io::WriteAll(fd, payload_bytes.data(), payload_bytes.size(),
                          "segment write failed");
  }
  if (status.ok()) {
    status = io::SyncFile(fd, "failed to fsync segment file");
  }

  Status close_status = io::CloseFile(fd, "failed to close segment file");
  if (status.ok()) {
    status = close_status;
  }
  if (!status.ok()) {
    return status;
  }

  ManifestEntry manifest;
  manifest.partition_id = partition_id;
  manifest.segment_path = path;
  manifest.min_event_time_us = events.front().event_time_us;
  manifest.max_event_time_us = events.front().event_time_us;
  manifest.min_system_time_us = events.front().system_time_us;
  manifest.max_system_time_us = events.front().system_time_us;
  manifest.max_lsn = events.front().lsn;
  manifest.row_count = events.size();

  for (const auto& event : events) {
    manifest.min_event_time_us = std::min(manifest.min_event_time_us, event.event_time_us);
    manifest.max_event_time_us = std::max(manifest.max_event_time_us, event.event_time_us);
    manifest.min_system_time_us =
        std::min(manifest.min_system_time_us, event.system_time_us);
    manifest.max_system_time_us =
        std::max(manifest.max_system_time_us, event.system_time_us);
    manifest.max_lsn = std::max(manifest.max_lsn, event.lsn);
  }

  SegmentData data;
  data.manifest = manifest;
  data.timeline_by_entity_feature = BuildIndex(events);
  return data;
}

StatusOr<SegmentData> SegmentStore::LoadSegment(
    const ManifestEntry& manifest_entry) const {
  std::ifstream in(manifest_entry.segment_path, std::ios::binary);
  if (!in.is_open()) {
    return Status::Internal("failed to open segment for read: " +
                            manifest_entry.segment_path);
  }

  SegmentFileHeader header;
  in.read(reinterpret_cast<char*>(&header), sizeof(header));
  if (static_cast<size_t>(in.gcount()) != sizeof(header)) {
    return Status::Internal("truncated segment header");
  }

  if (header.magic != kSegmentMagic || header.version != kSegmentVersion) {
    return Status::Internal("invalid segment header");
  }

  std::vector<uint8_t> payload_bytes(header.payload_length);
  in.read(reinterpret_cast<char*>(payload_bytes.data()), header.payload_length);
  if (static_cast<size_t>(in.gcount()) != header.payload_length) {
    return Status::Internal("truncated segment payload");
  }

  const uint32_t crc = Crc32(payload_bytes.data(), payload_bytes.size());
  if (crc != header.crc32) {
    return Status::Internal("segment CRC mismatch");
  }

  auto parsed = ParseWalBatch(/*lsn=*/0, payload_bytes);
  if (!parsed.ok()) {
    return parsed.status();
  }

  SegmentData data;
  data.manifest = manifest_entry;
  data.timeline_by_entity_feature = BuildIndex(parsed.value().events);
  return data;
}

std::string SegmentStore::SegmentPath(size_t partition_id, uint64_t segment_id) const {
  std::ostringstream out;
  out << segment_dir_ << "/partition-";
  out.width(3);
  out.fill('0');
  out << partition_id << "/segment-";
  out.width(10);
  out.fill('0');
  out << segment_id << ".sgm";
  return out.str();
}

std::unordered_map<EntityFeatureKey, std::vector<FeatureEvent>, EntityFeatureKeyHash>
SegmentStore::BuildIndex(const std::vector<FeatureEvent>& events) {
  std::unordered_map<EntityFeatureKey, std::vector<FeatureEvent>, EntityFeatureKeyHash>
      index;
  for (const auto& event : events) {
    index[EntityFeatureKey{event.entity, event.feature_id}].push_back(event);
  }

  for (auto& [_, timeline] : index) {
    std::sort(timeline.begin(), timeline.end(),
              [](const FeatureEvent& lhs, const FeatureEvent& rhs) {
                return IsNewer(lhs, rhs);
              });
  }

  return index;
}

}  // namespace mxdb
