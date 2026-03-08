#include <iostream>
#include <string>

#include "engine/catalog/metadata_store.h"
#include "engine/common/config/config.h"
#include "engine/common/logging/logging.h"
#include "engine/recovery/recovery_manager.h"
#include "engine/storage/feature_engine.h"

int main(int argc, char** argv) {
  std::string config_path = "featured.conf";
  if (argc > 1) {
    config_path = argv[1];
  }

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

  std::cout << "featured started"
            << " lsn=" << engine.CurrentLsn()
            << " truncated_tail=" << (truncated_tail ? "true" : "false")
            << "\n";

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
