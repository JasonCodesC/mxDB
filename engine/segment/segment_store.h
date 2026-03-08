#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include "engine/common/status/status.h"
#include "engine/manifest/manifest.h"
#include "engine/types/types.h"

namespace mxdb {

struct SegmentData {
  ManifestEntry manifest;
  std::unordered_map<EntityFeatureKey, std::vector<FeatureEvent>, EntityFeatureKeyHash>
      timeline_by_entity_feature;
};

class SegmentStore {
 public:
  Status Open(const std::string& segment_dir);

  StatusOr<SegmentData> WriteSegment(size_t partition_id, uint64_t segment_id,
                                     const std::vector<FeatureEvent>& events);

  StatusOr<SegmentData> LoadSegment(const ManifestEntry& manifest_entry) const;

 private:
  std::string SegmentPath(size_t partition_id, uint64_t segment_id) const;
  static std::unordered_map<EntityFeatureKey, std::vector<FeatureEvent>,
                            EntityFeatureKeyHash>
  BuildIndex(const std::vector<FeatureEvent>& events);

  std::string segment_dir_;
};

}  // namespace mxdb
