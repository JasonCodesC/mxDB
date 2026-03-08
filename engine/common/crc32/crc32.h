#pragma once

#include <cstddef>
#include <cstdint>

namespace mxdb {

uint32_t Crc32(const void* data, size_t length);

}  // namespace mxdb
