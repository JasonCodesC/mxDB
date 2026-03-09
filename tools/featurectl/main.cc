#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <exception>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <optional>
#include <sstream>
#include <string>
#include <variant>
#include <vector>

#include "engine/admin/admin_service.h"
#include "engine/catalog/metadata_store.h"
#include "engine/common/config/config.h"
#include "engine/recovery/recovery_manager.h"
#include "engine/storage/feature_engine.h"

namespace {

mxdb::TimestampMicros NowMicros() {
  const auto now = std::chrono::system_clock::now();
  return std::chrono::duration_cast<std::chrono::microseconds>(
             now.time_since_epoch())
      .count();
}

void PrintUsage() {
  std::cerr
      << "usage: featurectl <config_path> <command> [args]\n"
      << "commands:\n"
      << "  health\n"
      << "  checkpoint\n"
      << "  compact\n"
      << "  readonly <on|off>\n"
      << "  backup <destination_dir>\n"
      << "  restore <source_dir> [readonly]\n"
      << "  register-feature <tenant> <entity_type> <feature_id> <feature_name> <value_type>\n"
      << "  ingest <tenant> <entity_type> <entity_id> <feature_id> <event_us> <system_us|auto> <value> <write_id> [upsert|delete]\n"
      << "  get <tenant> <entity_type> <entity_id>\n"
      << "  latest <tenant> <entity_type> <entity_id> <feature_id> [count]\n"
      << "  asof <tenant> <entity_type> <entity_id> <feature_id> <event_cutoff_us> <system_cutoff_us>\n";
}

mxdb::StatusOr<mxdb::ValueType> ParseValueType(const std::string& value_type) {
  if (value_type == "double") {
    return mxdb::ValueType::kDouble;
  }
  if (value_type == "int64") {
    return mxdb::ValueType::kInt64;
  }
  if (value_type == "string") {
    return mxdb::ValueType::kString;
  }
  if (value_type == "bool") {
    return mxdb::ValueType::kBool;
  }
  if (value_type == "float_vector") {
    return mxdb::ValueType::kFloatVector;
  }
  if (value_type == "double_vector") {
    return mxdb::ValueType::kDoubleVector;
  }
  return mxdb::Status::InvalidArgument("unsupported value_type for CLI");
}

std::string Trim(const std::string& value) {
  size_t begin = 0;
  while (begin < value.size() &&
         std::isspace(static_cast<unsigned char>(value[begin]))) {
    ++begin;
  }

  size_t end = value.size();
  while (end > begin &&
         std::isspace(static_cast<unsigned char>(value[end - 1]))) {
    --end;
  }

  return value.substr(begin, end - begin);
}

mxdb::StatusOr<bool> ParseBool(const std::string& value) {
  std::string lower;
  lower.reserve(value.size());
  std::transform(value.begin(), value.end(), std::back_inserter(lower),
                 [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
  if (lower == "true" || lower == "1") {
    return true;
  }
  if (lower == "false" || lower == "0") {
    return false;
  }
  return mxdb::Status::InvalidArgument("invalid bool literal: " + value);
}

mxdb::StatusOr<std::vector<double>> ParseDoubleVectorLiteral(
    const std::string& literal) {
  std::string body = Trim(literal);
  if (!body.empty() && body.front() == '[' && body.back() == ']') {
    body = body.substr(1, body.size() - 2);
  }
  body = Trim(body);
  if (body.empty()) {
    return std::vector<double>{};
  }

  std::vector<double> out;
  std::stringstream ss(body);
  std::string token;
  while (std::getline(ss, token, ',')) {
    token = Trim(token);
    if (token.empty()) {
      return mxdb::Status::InvalidArgument("invalid vector literal: empty element");
    }
    try {
      out.push_back(std::stod(token));
    } catch (const std::exception&) {
      return mxdb::Status::InvalidArgument("invalid vector literal element: " +
                                           token);
    }
  }
  return out;
}

std::string JsonEscape(const std::string& raw) {
  std::string out;
  out.reserve(raw.size() + 8);
  for (char c : raw) {
    switch (c) {
      case '\\':
        out += "\\\\";
        break;
      case '"':
        out += "\\\"";
        break;
      case '\n':
        out += "\\n";
        break;
      case '\r':
        out += "\\r";
        break;
      case '\t':
        out += "\\t";
        break;
      default:
        out += c;
        break;
    }
  }
  return out;
}

std::string Base64Encode(const std::string& input) {
  static constexpr char kTable[] =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

  std::string out;
  out.reserve(((input.size() + 2) / 3) * 4);

  size_t i = 0;
  while (i + 2 < input.size()) {
    const uint32_t n = (static_cast<uint8_t>(input[i]) << 16U) |
                       (static_cast<uint8_t>(input[i + 1]) << 8U) |
                       static_cast<uint8_t>(input[i + 2]);
    out.push_back(kTable[(n >> 18U) & 63U]);
    out.push_back(kTable[(n >> 12U) & 63U]);
    out.push_back(kTable[(n >> 6U) & 63U]);
    out.push_back(kTable[n & 63U]);
    i += 3;
  }

  const size_t rem = input.size() - i;
  if (rem == 1) {
    const uint32_t n = static_cast<uint8_t>(input[i]) << 16U;
    out.push_back(kTable[(n >> 18U) & 63U]);
    out.push_back(kTable[(n >> 12U) & 63U]);
    out.push_back('=');
    out.push_back('=');
  } else if (rem == 2) {
    const uint32_t n = (static_cast<uint8_t>(input[i]) << 16U) |
                       (static_cast<uint8_t>(input[i + 1]) << 8U);
    out.push_back(kTable[(n >> 18U) & 63U]);
    out.push_back(kTable[(n >> 12U) & 63U]);
    out.push_back(kTable[(n >> 6U) & 63U]);
    out.push_back('=');
  }

  return out;
}

mxdb::StatusOr<mxdb::FeatureValue> ParseLiteralForType(mxdb::ValueType type,
                                                       const std::string& literal) {
  mxdb::FeatureValue value;
  value.type = type;
  try {
    switch (type) {
      case mxdb::ValueType::kBool: {
        auto parsed = ParseBool(literal);
        if (!parsed.ok()) {
          return parsed.status();
        }
        value.value = parsed.value();
        return value;
      }
      case mxdb::ValueType::kInt64:
        value.value = static_cast<int64_t>(std::stoll(literal));
        return value;
      case mxdb::ValueType::kDouble:
        value.value = std::stod(literal);
        return value;
      case mxdb::ValueType::kString:
        value.value = literal;
        return value;
      case mxdb::ValueType::kFloatVector: {
        auto parsed = ParseDoubleVectorLiteral(literal);
        if (!parsed.ok()) {
          return parsed.status();
        }
        std::vector<float> casted;
        casted.reserve(parsed.value().size());
        for (double v : parsed.value()) {
          casted.push_back(static_cast<float>(v));
        }
        value.value = std::move(casted);
        return value;
      }
      case mxdb::ValueType::kDoubleVector: {
        auto parsed = ParseDoubleVectorLiteral(literal);
        if (!parsed.ok()) {
          return parsed.status();
        }
        value.value = parsed.value();
        return value;
      }
      default:
        return mxdb::Status::InvalidArgument("unsupported value_type for ingest");
    }
  } catch (const std::exception&) {
    return mxdb::Status::InvalidArgument("invalid value literal for feature type");
  }
}

mxdb::StatusOr<std::string> ValueAsJson(const mxdb::FeatureValue& value) {
  if (value.IsNull()) {
    return std::string("null");
  }

  try {
    switch (value.type) {
      case mxdb::ValueType::kBool:
        return std::string(std::get<bool>(value.value) ? "true" : "false");
      case mxdb::ValueType::kInt64:
        return std::to_string(std::get<int64_t>(value.value));
      case mxdb::ValueType::kDouble: {
        std::ostringstream out;
        out << std::setprecision(17) << std::get<double>(value.value);
        return out.str();
      }
      case mxdb::ValueType::kString:
        return std::string("\"") + JsonEscape(std::get<std::string>(value.value)) +
               "\"";
      case mxdb::ValueType::kFloatVector: {
        const auto& vec = std::get<std::vector<float>>(value.value);
        std::ostringstream out;
        out << "[";
        for (size_t i = 0; i < vec.size(); ++i) {
          if (i > 0) {
            out << ",";
          }
          out << std::setprecision(9) << vec[i];
        }
        out << "]";
        return out.str();
      }
      case mxdb::ValueType::kDoubleVector: {
        const auto& vec = std::get<std::vector<double>>(value.value);
        std::ostringstream out;
        out << "[";
        for (size_t i = 0; i < vec.size(); ++i) {
          if (i > 0) {
            out << ",";
          }
          out << std::setprecision(17) << vec[i];
        }
        out << "]";
        return out.str();
      }
      default:
        return mxdb::Status::InvalidArgument("unsupported value type for JSON");
    }
  } catch (const std::bad_variant_access&) {
    return mxdb::Status::Internal("feature value type/variant mismatch");
  }
}

mxdb::StatusOr<std::string> LatestEventsAsJson(
    const std::vector<mxdb::FeatureEvent>& events) {
  std::ostringstream out;
  out << "[";
  for (size_t i = 0; i < events.size(); ++i) {
    const auto& event = events[i];
    auto value_json = ValueAsJson(event.value);
    if (!value_json.ok()) {
      return value_json.status();
    }
    if (i > 0) {
      out << ",";
    }
    out << "{\"value_type\":\"" << JsonEscape(mxdb::ToString(event.value.type))
        << "\",\"value\":" << value_json.value()
        << ",\"event_time_us\":" << event.event_time_us
        << ",\"system_time_us\":" << event.system_time_us << ",\"lsn\":"
        << event.lsn << "}";
  }
  out << "]";
  return out.str();
}

mxdb::StatusOr<std::string> LatestSnapshotAsJson(
    const mxdb::LatestQueryResult& latest) {
  std::ostringstream out;
  out << "[";
  for (size_t i = 0; i < latest.features.size(); ++i) {
    const auto& feature = latest.features[i];
    if (i > 0) {
      out << ",";
    }

    out << "{\"feature_id\":\"" << JsonEscape(feature.feature_id) << "\"";
    if (!feature.found) {
      out << ",\"found\":false}";
      continue;
    }

    auto value_json = ValueAsJson(feature.value);
    if (!value_json.ok()) {
      return value_json.status();
    }

    out << ",\"found\":true"
        << ",\"value_type\":\"" << JsonEscape(mxdb::ToString(feature.value.type))
        << "\""
        << ",\"value\":" << value_json.value()
        << ",\"event_time_us\":" << feature.event_time_us
        << ",\"system_time_us\":" << feature.system_time_us << ",\"lsn\":"
        << latest.visible_commit.lsn << "}";
  }
  out << "]";
  return out.str();
}

}  // namespace

int main(int argc, char** argv) {
  if (argc < 3) {
    PrintUsage();
    return 1;
  }

  const std::string config_path = argv[1];
  const std::string command = argv[2];

  mxdb::EngineConfig config = mxdb::ConfigLoader::LoadFromFile(config_path);

  mxdb::MetadataStore metadata;
  mxdb::Status status = metadata.Open(config.metadata_path);
  if (!status.ok()) {
    std::cerr << "metadata open failed: " << status.message() << "\n";
    return 1;
  }

  mxdb::FeatureEngine engine(config, &metadata);
  status = engine.Start();
  if (!status.ok()) {
    std::cerr << "engine start failed: " << status.message() << "\n";
    return 1;
  }

  mxdb::RecoveryManager recovery(&engine);
  bool truncated_tail = false;
  status = recovery.RecoverFromWalDirectory(config.wal_dir, &truncated_tail);
  if (!status.ok()) {
    std::cerr << "recovery failed: " << status.message() << "\n";
    return 1;
  }

  mxdb::AdminService admin(&engine, &config, &metadata);

  if (command == "health") {
    mxdb::HealthStatus health = admin.GetHealth();
    std::cout << "state=" << health.state << " lsn=" << health.current_lsn
              << " manifest_version=" << health.manifest_version
              << " read_only=" << (health.read_only ? "true" : "false")
              << " truncated_tail=" << (truncated_tail ? "true" : "false")
              << "\n";
  } else if (command == "checkpoint") {
    status = admin.TriggerCheckpoint(/*wait=*/true);
  } else if (command == "compact") {
    status = admin.TriggerCompaction();
  } else if (command == "readonly") {
    if (argc < 4) {
      PrintUsage();
      return 1;
    }
    const std::string mode = argv[3];
    status = admin.SetReadOnlyMode(mode == "on");
  } else if (command == "backup") {
    if (argc < 4) {
      PrintUsage();
      return 1;
    }
    status = admin.StartBackup(argv[3]);
  } else if (command == "restore") {
    if (argc < 4) {
      PrintUsage();
      return 1;
    }
    const bool read_only = argc >= 5 && std::string(argv[4]) == "readonly";
    status = admin.RestoreBackup(argv[3], read_only);
  } else if (command == "register-feature") {
    if (argc < 8) {
      PrintUsage();
      return 1;
    }

    auto value_type = ParseValueType(argv[7]);
    if (!value_type.ok()) {
      std::cerr << value_type.status().message() << "\n";
      return 1;
    }

    mxdb::FeatureDefinition feature;
    feature.tenant_id = argv[3];
    feature.entity_type = argv[4];
    feature.feature_id = argv[5];
    feature.feature_name = argv[6];
    feature.value_type = value_type.value();
    feature.serving_enabled = true;
    feature.historical_enabled = true;
    feature.allow_external_system_time = true;
    feature.nullable = true;
    feature.description = "cli registered feature";
    feature.owner = "featurectl";
    feature.created_at_us = NowMicros();
    feature.updated_at_us = feature.created_at_us;

    status = metadata.CreateFeature(feature);
  } else if (command == "ingest") {
    if (argc < 11) {
      PrintUsage();
      return 1;
    }

    mxdb::OperationType operation = mxdb::OperationType::kUpsert;
    if (argc >= 12) {
      const std::string op = argv[11];
      if (op == "upsert") {
        operation = mxdb::OperationType::kUpsert;
      } else if (op == "delete") {
        operation = mxdb::OperationType::kDelete;
      } else {
        status =
            mxdb::Status::InvalidArgument("operation must be upsert or delete");
        goto done;
      }
    }

    auto feature = metadata.GetFeatureById(argv[3], argv[6]);
    if (!feature.ok()) {
      status = feature.status();
      goto done;
    }

    mxdb::FeatureValue value;
    if (operation == mxdb::OperationType::kDelete) {
      value.type = feature.value().value_type;
      value.value = std::monostate{};
    } else {
      auto parsed = ParseLiteralForType(feature.value().value_type, argv[9]);
      if (!parsed.ok()) {
        status = parsed.status();
        goto done;
      }
      value = parsed.value();
    }

    mxdb::EntityFeatureBatch batch;
    batch.entity = {.tenant_id = argv[3], .entity_type = argv[4], .entity_id = argv[5]};

    mxdb::FeatureEventInput event;
    event.feature_id = argv[6];
    event.event_time_us = std::stoll(argv[7]);
    if (std::string(argv[8]) != "auto") {
      event.system_time_us = std::stoll(argv[8]);
    }
    event.value = value;
    event.operation = operation;
    event.write_id = argv[10];
    event.source_id = "featurectl";
    batch.events.push_back(std::move(event));

    auto result = engine.WriteEntityBatch(
        batch, config.default_durability_mode,
        /*allow_trusted_system_time=*/batch.events[0].system_time_us.has_value());
    if (!result.ok()) {
      status = result.status();
    } else {
      std::cout << "accepted_events=" << result.value().accepted_events
                << " lsn=" << result.value().commit.lsn << "\n";
    }
  } else if (command == "get") {
    if (argc < 6) {
      PrintUsage();
      return 1;
    }

    auto features = metadata.ListFeatures(argv[3], std::string(argv[4]));
    if (!features.ok()) {
      status = features.status();
      goto done;
    }
    if (features.value().empty()) {
      std::cout << "found=0 count=0\n";
      goto done;
    }

    std::vector<std::string> feature_ids;
    feature_ids.reserve(features.value().size());
    for (const auto& feature : features.value()) {
      feature_ids.push_back(feature.feature_id);
    }

    mxdb::LatestQueryResult snapshot;
    auto latest = engine.GetLatest({.tenant_id = argv[3],
                                    .entity_type = argv[4],
                                    .entity_id = argv[5]},
                                   feature_ids);
    if (latest.ok()) {
      snapshot = latest.value();
    } else if (latest.status().code() == mxdb::StatusCode::kNotFound) {
      snapshot.entity = {.tenant_id = argv[3],
                         .entity_type = argv[4],
                         .entity_id = argv[5]};
      snapshot.visible_commit = {};
      snapshot.features.reserve(feature_ids.size());
      for (const auto& feature_id : feature_ids) {
        mxdb::FeatureValueResult value_result;
        value_result.feature_id = feature_id;
        value_result.found = false;
        snapshot.features.push_back(std::move(value_result));
      }
    } else {
      status = latest.status();
      goto done;
    }

    auto snapshot_json = LatestSnapshotAsJson(snapshot);
    if (!snapshot_json.ok()) {
      status = snapshot_json.status();
      goto done;
    }
    const std::string values_b64 = Base64Encode(snapshot_json.value());
    std::cout << "found=1"
              << " count=" << snapshot.features.size()
              << " values_b64=" << values_b64 << "\n";
  } else if (command == "latest") {
    if (argc < 7) {
      PrintUsage();
      return 1;
    }

    size_t latest_count = 1;
    if (argc >= 8) {
      try {
        latest_count = static_cast<size_t>(std::stoull(argv[7]));
      } catch (const std::exception&) {
        status = mxdb::Status::InvalidArgument("latest count must be a positive integer");
        goto done;
      }
      if (latest_count == 0) {
        status = mxdb::Status::InvalidArgument("latest count must be greater than zero");
        goto done;
      }
    }

    if (latest_count == 1) {
      auto latest = engine.GetLatest({.tenant_id = argv[3],
                                      .entity_type = argv[4],
                                      .entity_id = argv[5]},
                                     {argv[6]});
      if (!latest.ok()) {
        status = latest.status();
      } else {
        const auto& feature = latest.value().features.at(0);
        if (!feature.found) {
          std::cout << "found=0\n";
        } else {
          auto json = ValueAsJson(feature.value);
          if (!json.ok()) {
            status = json.status();
            goto done;
          }
          const std::string value_b64 = Base64Encode(json.value());
          std::cout << "found=1"
                    << " value_type=" << mxdb::ToString(feature.value.type)
                    << " value_b64=" << value_b64
                    << " event_time_us=" << feature.event_time_us
                    << " system_time_us=" << feature.system_time_us
                    << " lsn=" << latest.value().visible_commit.lsn << "\n";
        }
      }
    } else {
      auto latest_events = engine.GetLatestEvents({.tenant_id = argv[3],
                                                   .entity_type = argv[4],
                                                   .entity_id = argv[5]},
                                                  argv[6], latest_count);
      if (!latest_events.ok()) {
        status = latest_events.status();
      } else if (latest_events.value().empty()) {
        std::cout << "found=0 count=0\n";
      } else {
        auto events_json = LatestEventsAsJson(latest_events.value());
        if (!events_json.ok()) {
          status = events_json.status();
          goto done;
        }
        const std::string values_b64 = Base64Encode(events_json.value());
        std::cout << "found=1"
                  << " count=" << latest_events.value().size()
                  << " values_b64=" << values_b64 << "\n";
      }
    }
  } else if (command == "asof") {
    if (argc < 9) {
      PrintUsage();
      return 1;
    }

    auto as_of = engine.AsOfLookup({.entity = {.tenant_id = argv[3],
                                               .entity_type = argv[4],
                                               .entity_id = argv[5]},
                                    .feature_ids = {argv[6]},
                                    .event_cutoff_us = std::stoll(argv[7]),
                                    .system_cutoff_us = std::stoll(argv[8])});
    if (!as_of.ok()) {
      status = as_of.status();
    } else {
      const auto& feature = as_of.value().features.at(0);
      if (!feature.found) {
        std::cout << "found=0\n";
      } else {
        auto json = ValueAsJson(feature.value);
        if (!json.ok()) {
          status = json.status();
          goto done;
        }
        const std::string value_b64 = Base64Encode(json.value());
        std::cout << "found=1"
                  << " value_type=" << mxdb::ToString(feature.value.type)
                  << " value_b64=" << value_b64
                  << " event_time_us=" << feature.event_time_us
                  << " system_time_us=" << feature.system_time_us << "\n";
      }
    }
  } else {
    PrintUsage();
    return 1;
  }

done:
  if (!status.ok()) {
    std::cerr << "command failed: " << status.message() << "\n";
    return 1;
  }

  status = engine.Stop();
  if (!status.ok()) {
    std::cerr << "engine stop failed: " << status.message() << "\n";
    return 1;
  }

  status = metadata.Close();
  if (!status.ok()) {
    std::cerr << "metadata close failed: " << status.message() << "\n";
    return 1;
  }

  return 0;
}
