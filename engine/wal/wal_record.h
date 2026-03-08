#pragma once

#include <cstdint>
#include <vector>

#include "engine/common/status/status.h"
#include "engine/types/types.h"

namespace mxdb {

constexpr uint32_t kWalMagic = 0x4D584442U;  // MXDB
constexpr uint16_t kWalVersion = 1;
constexpr uint16_t kWalRecordTypeEntityBatch = 1;

#pragma pack(push, 1)
struct WalRecordHeader {
  uint32_t magic = kWalMagic;
  uint16_t version = kWalVersion;
  uint16_t record_type = kWalRecordTypeEntityBatch;
  uint32_t payload_length = 0;
  uint64_t lsn = 0;
  uint32_t crc32 = 0;
};
#pragma pack(pop)

struct WalBatchPayload {
  TimestampMicros commit_system_time_us = 0;
  std::vector<FeatureEvent> events;
};

std::vector<uint8_t> SerializeWalBatch(const WalBatchPayload& payload);
StatusOr<WalBatchPayload> ParseWalBatch(uint64_t lsn,
                                        const std::vector<uint8_t>& bytes);

}  // namespace mxdb
