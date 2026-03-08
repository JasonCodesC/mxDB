#include <chrono>
#include <iostream>
#include <optional>
#include <string>

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
      << "  ingest <tenant> <entity_type> <entity_id> <feature_id> <event_us> <system_us|auto> <double_value> <write_id>\n"
      << "  latest <tenant> <entity_type> <entity_id> <feature_id>\n"
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
  return mxdb::Status::InvalidArgument("unsupported value_type for CLI");
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

  mxdb::AdminService admin(&engine, &config);

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

    mxdb::EntityFeatureBatch batch;
    batch.entity = {.tenant_id = argv[3], .entity_type = argv[4], .entity_id = argv[5]};

    mxdb::FeatureEventInput event;
    event.feature_id = argv[6];
    event.event_time_us = std::stoll(argv[7]);
    if (std::string(argv[8]) != "auto") {
      event.system_time_us = std::stoll(argv[8]);
    }
    event.value = {.type = mxdb::ValueType::kDouble, .value = std::stod(argv[9])};
    event.operation = mxdb::OperationType::kUpsert;
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
  } else if (command == "latest") {
    if (argc < 7) {
      PrintUsage();
      return 1;
    }

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
        std::cout << "found=1"
                  << " value=" << std::get<double>(feature.value.value)
                  << " event_time_us=" << feature.event_time_us
                  << " system_time_us=" << feature.system_time_us
                  << " lsn=" << latest.value().visible_commit.lsn << "\n";
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
        std::cout << "found=1"
                  << " value=" << std::get<double>(feature.value.value)
                  << " event_time_us=" << feature.event_time_us
                  << " system_time_us=" << feature.system_time_us << "\n";
      }
    }
  } else {
    PrintUsage();
    return 1;
  }

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
