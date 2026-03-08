#include "engine/wal/wal_record.h"

#include <cstring>
#include <limits>
#include <string>

namespace mxdb {

namespace {

void WriteUint8(std::vector<uint8_t>* out, uint8_t value) { out->push_back(value); }

void WriteUint32(std::vector<uint8_t>* out, uint32_t value) {
  for (int i = 0; i < 4; ++i) {
    out->push_back(static_cast<uint8_t>((value >> (8 * i)) & 0xFFU));
  }
}

void WriteUint64(std::vector<uint8_t>* out, uint64_t value) {
  for (int i = 0; i < 8; ++i) {
    out->push_back(static_cast<uint8_t>((value >> (8 * i)) & 0xFFU));
  }
}

void WriteInt64(std::vector<uint8_t>* out, int64_t value) {
  WriteUint64(out, static_cast<uint64_t>(value));
}

void WriteDouble(std::vector<uint8_t>* out, double value) {
  uint64_t bits = 0;
  static_assert(sizeof(bits) == sizeof(value));
  std::memcpy(&bits, &value, sizeof(bits));
  WriteUint64(out, bits);
}

void WriteFloat(std::vector<uint8_t>* out, float value) {
  uint32_t bits = 0;
  static_assert(sizeof(bits) == sizeof(value));
  std::memcpy(&bits, &value, sizeof(bits));
  WriteUint32(out, bits);
}

Status WriteString(std::vector<uint8_t>* out, const std::string& value) {
  if (value.size() > std::numeric_limits<uint32_t>::max()) {
    return Status::InvalidArgument("string too long for WAL encoding");
  }
  WriteUint32(out, static_cast<uint32_t>(value.size()));
  out->insert(out->end(), value.begin(), value.end());
  return Status::Ok();
}

Status WriteFeatureValue(std::vector<uint8_t>* out, const FeatureValue& value) {
  WriteUint8(out, static_cast<uint8_t>(value.type));
  const uint8_t is_null = value.IsNull() ? 1U : 0U;
  WriteUint8(out, is_null);
  if (is_null == 1U) {
    return Status::Ok();
  }

  switch (value.type) {
    case ValueType::kBool:
      WriteUint8(out, std::get<bool>(value.value) ? 1U : 0U);
      return Status::Ok();
    case ValueType::kInt64:
      WriteInt64(out, std::get<int64_t>(value.value));
      return Status::Ok();
    case ValueType::kDouble:
      WriteDouble(out, std::get<double>(value.value));
      return Status::Ok();
    case ValueType::kString:
      return WriteString(out, std::get<std::string>(value.value));
    case ValueType::kFloatVector: {
      const auto& values = std::get<std::vector<float>>(value.value);
      if (values.size() > std::numeric_limits<uint32_t>::max()) {
        return Status::InvalidArgument("float vector too long for WAL encoding");
      }
      WriteUint32(out, static_cast<uint32_t>(values.size()));
      for (float f : values) {
        WriteFloat(out, f);
      }
      return Status::Ok();
    }
    case ValueType::kDoubleVector: {
      const auto& values = std::get<std::vector<double>>(value.value);
      if (values.size() > std::numeric_limits<uint32_t>::max()) {
        return Status::InvalidArgument("double vector too long for WAL encoding");
      }
      WriteUint32(out, static_cast<uint32_t>(values.size()));
      for (double d : values) {
        WriteDouble(out, d);
      }
      return Status::Ok();
    }
    case ValueType::kUnspecified:
      return Status::InvalidArgument("WAL value type cannot be unspecified");
  }

  return Status::InvalidArgument("unsupported WAL value type");
}

StatusOr<uint8_t> ReadUint8(const std::vector<uint8_t>& in, size_t* cursor) {
  if (*cursor + 1 > in.size()) {
    return Status::InvalidArgument("truncated WAL payload (u8)");
  }
  const uint8_t value = in[*cursor];
  *cursor += 1;
  return value;
}

StatusOr<uint32_t> ReadUint32(const std::vector<uint8_t>& in, size_t* cursor) {
  if (*cursor + 4 > in.size()) {
    return Status::InvalidArgument("truncated WAL payload (u32)");
  }
  uint32_t value = 0;
  for (int i = 0; i < 4; ++i) {
    value |= (static_cast<uint32_t>(in[*cursor + i]) << (8 * i));
  }
  *cursor += 4;
  return value;
}

StatusOr<uint64_t> ReadUint64(const std::vector<uint8_t>& in, size_t* cursor) {
  if (*cursor + 8 > in.size()) {
    return Status::InvalidArgument("truncated WAL payload (u64)");
  }
  uint64_t value = 0;
  for (int i = 0; i < 8; ++i) {
    value |= (static_cast<uint64_t>(in[*cursor + i]) << (8 * i));
  }
  *cursor += 8;
  return value;
}

StatusOr<int64_t> ReadInt64(const std::vector<uint8_t>& in, size_t* cursor) {
  auto bits = ReadUint64(in, cursor);
  if (!bits.ok()) {
    return bits.status();
  }
  return static_cast<int64_t>(bits.value());
}

StatusOr<double> ReadDouble(const std::vector<uint8_t>& in, size_t* cursor) {
  auto bits = ReadUint64(in, cursor);
  if (!bits.ok()) {
    return bits.status();
  }
  double value = 0;
  const uint64_t raw = bits.value();
  std::memcpy(&value, &raw, sizeof(value));
  return value;
}

StatusOr<float> ReadFloat(const std::vector<uint8_t>& in, size_t* cursor) {
  auto bits = ReadUint32(in, cursor);
  if (!bits.ok()) {
    return bits.status();
  }
  float value = 0;
  const uint32_t raw = bits.value();
  std::memcpy(&value, &raw, sizeof(value));
  return value;
}

StatusOr<std::string> ReadString(const std::vector<uint8_t>& in, size_t* cursor) {
  auto size_or = ReadUint32(in, cursor);
  if (!size_or.ok()) {
    return size_or.status();
  }

  const uint32_t size = size_or.value();
  if (*cursor + size > in.size()) {
    return Status::InvalidArgument("truncated WAL payload (string)");
  }
  std::string value(reinterpret_cast<const char*>(&in[*cursor]), size);
  *cursor += size;
  return value;
}

StatusOr<FeatureValue> ReadFeatureValue(const std::vector<uint8_t>& in,
                                        size_t* cursor) {
  auto value_type_or = ReadUint8(in, cursor);
  if (!value_type_or.ok()) {
    return value_type_or.status();
  }
  auto is_null_or = ReadUint8(in, cursor);
  if (!is_null_or.ok()) {
    return is_null_or.status();
  }

  FeatureValue value;
  value.type = static_cast<ValueType>(value_type_or.value());
  if (is_null_or.value() == 1U) {
    value.value = std::monostate{};
    return value;
  }

  switch (value.type) {
    case ValueType::kBool: {
      auto b = ReadUint8(in, cursor);
      if (!b.ok()) {
        return b.status();
      }
      value.value = (b.value() != 0U);
      return value;
    }
    case ValueType::kInt64: {
      auto v = ReadInt64(in, cursor);
      if (!v.ok()) {
        return v.status();
      }
      value.value = v.value();
      return value;
    }
    case ValueType::kDouble: {
      auto v = ReadDouble(in, cursor);
      if (!v.ok()) {
        return v.status();
      }
      value.value = v.value();
      return value;
    }
    case ValueType::kString: {
      auto v = ReadString(in, cursor);
      if (!v.ok()) {
        return v.status();
      }
      value.value = v.value();
      return value;
    }
    case ValueType::kFloatVector: {
      auto dim_or = ReadUint32(in, cursor);
      if (!dim_or.ok()) {
        return dim_or.status();
      }
      std::vector<float> values;
      values.reserve(dim_or.value());
      for (uint32_t i = 0; i < dim_or.value(); ++i) {
        auto f = ReadFloat(in, cursor);
        if (!f.ok()) {
          return f.status();
        }
        values.push_back(f.value());
      }
      value.value = std::move(values);
      return value;
    }
    case ValueType::kDoubleVector: {
      auto dim_or = ReadUint32(in, cursor);
      if (!dim_or.ok()) {
        return dim_or.status();
      }
      std::vector<double> values;
      values.reserve(dim_or.value());
      for (uint32_t i = 0; i < dim_or.value(); ++i) {
        auto d = ReadDouble(in, cursor);
        if (!d.ok()) {
          return d.status();
        }
        values.push_back(d.value());
      }
      value.value = std::move(values);
      return value;
    }
    case ValueType::kUnspecified:
      return Status::InvalidArgument("invalid WAL value type");
  }

  return Status::InvalidArgument("unsupported WAL value type");
}

}  // namespace

std::vector<uint8_t> SerializeWalBatch(const WalBatchPayload& payload) {
  std::vector<uint8_t> out;
  out.reserve(256 + payload.events.size() * 128);

  WriteInt64(&out, payload.commit_system_time_us);
  WriteUint32(&out, static_cast<uint32_t>(payload.events.size()));

  for (const auto& event : payload.events) {
    (void)WriteString(&out, event.entity.tenant_id);
    (void)WriteString(&out, event.entity.entity_type);
    (void)WriteString(&out, event.entity.entity_id);
    (void)WriteString(&out, event.feature_id);
    WriteInt64(&out, event.event_time_us);
    WriteInt64(&out, event.system_time_us);
    WriteUint64(&out, event.sequence_no);
    WriteUint64(&out, event.lsn);
    WriteUint8(&out, static_cast<uint8_t>(event.operation));
    (void)WriteString(&out, event.write_id);
    (void)WriteString(&out, event.source_id);
    (void)WriteFeatureValue(&out, event.value);
  }

  return out;
}

StatusOr<WalBatchPayload> ParseWalBatch(uint64_t lsn,
                                        const std::vector<uint8_t>& bytes) {
  size_t cursor = 0;
  WalBatchPayload payload;

  auto commit_or = ReadInt64(bytes, &cursor);
  if (!commit_or.ok()) {
    return commit_or.status();
  }
  payload.commit_system_time_us = commit_or.value();

  auto count_or = ReadUint32(bytes, &cursor);
  if (!count_or.ok()) {
    return count_or.status();
  }

  payload.events.reserve(count_or.value());
  for (uint32_t i = 0; i < count_or.value(); ++i) {
    FeatureEvent event;

    auto tenant = ReadString(bytes, &cursor);
    if (!tenant.ok()) {
      return tenant.status();
    }
    auto entity_type = ReadString(bytes, &cursor);
    if (!entity_type.ok()) {
      return entity_type.status();
    }
    auto entity_id = ReadString(bytes, &cursor);
    if (!entity_id.ok()) {
      return entity_id.status();
    }
    auto feature_id = ReadString(bytes, &cursor);
    if (!feature_id.ok()) {
      return feature_id.status();
    }

    event.entity.tenant_id = tenant.value();
    event.entity.entity_type = entity_type.value();
    event.entity.entity_id = entity_id.value();
    event.feature_id = feature_id.value();

    auto event_time = ReadInt64(bytes, &cursor);
    if (!event_time.ok()) {
      return event_time.status();
    }
    auto system_time = ReadInt64(bytes, &cursor);
    if (!system_time.ok()) {
      return system_time.status();
    }
    auto sequence = ReadUint64(bytes, &cursor);
    if (!sequence.ok()) {
      return sequence.status();
    }
    auto event_lsn = ReadUint64(bytes, &cursor);
    if (!event_lsn.ok()) {
      return event_lsn.status();
    }
    auto operation = ReadUint8(bytes, &cursor);
    if (!operation.ok()) {
      return operation.status();
    }
    auto write_id = ReadString(bytes, &cursor);
    if (!write_id.ok()) {
      return write_id.status();
    }
    auto source_id = ReadString(bytes, &cursor);
    if (!source_id.ok()) {
      return source_id.status();
    }
    auto feature_value = ReadFeatureValue(bytes, &cursor);
    if (!feature_value.ok()) {
      return feature_value.status();
    }

    event.event_time_us = event_time.value();
    event.system_time_us = system_time.value();
    event.sequence_no = sequence.value();
    event.lsn = event_lsn.value();
    event.operation = static_cast<OperationType>(operation.value());
    event.write_id = write_id.value();
    event.source_id = source_id.value();
    event.value = feature_value.value();
    if (event.lsn == 0) {
      event.lsn = lsn;
    }

    payload.events.push_back(std::move(event));
  }

  if (cursor != bytes.size()) {
    return Status::InvalidArgument("unexpected trailing bytes in WAL payload");
  }

  return payload;
}

}  // namespace mxdb
