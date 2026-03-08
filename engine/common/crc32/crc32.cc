#include "engine/common/crc32/crc32.h"

namespace mxdb {

namespace {

constexpr uint32_t kPolynomial = 0xEDB88320U;

uint32_t BuildEntry(uint32_t index) {
  uint32_t value = index;
  for (int i = 0; i < 8; ++i) {
    if ((value & 1U) != 0U) {
      value = (value >> 1U) ^ kPolynomial;
    } else {
      value >>= 1U;
    }
  }
  return value;
}

const uint32_t* Table() {
  static uint32_t table[256] = {};
  static bool initialized = false;
  if (!initialized) {
    for (uint32_t i = 0; i < 256; ++i) {
      table[i] = BuildEntry(i);
    }
    initialized = true;
  }
  return table;
}

}  // namespace

uint32_t Crc32(const void* data, size_t length) {
  const auto* bytes = static_cast<const uint8_t*>(data);
  const uint32_t* table = Table();
  uint32_t crc = 0xFFFFFFFFU;
  for (size_t i = 0; i < length; ++i) {
    const uint32_t idx = (crc ^ bytes[i]) & 0xFFU;
    crc = (crc >> 8U) ^ table[idx];
  }
  return crc ^ 0xFFFFFFFFU;
}

}  // namespace mxdb
