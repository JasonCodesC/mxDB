#include <atomic>
#include <chrono>
#include <csignal>
#include <iostream>
#include <string>
#include <thread>

#include "engine/catalog/metadata_store.h"
#include "engine/common/config/config.h"
#include "engine/common/logging/logging.h"
#include "engine/common/process_lock/data_dir_lock.h"
#include "engine/recovery/recovery_manager.h"
#include "engine/storage/feature_engine.h"

namespace {

std::atomic<bool> g_shutdown_requested{false};

void HandleSignal(int /*signal*/) { g_shutdown_requested.store(true); }

}  // namespace

int main(int argc, char** argv) {
  std::string config_path = "featured.conf";
  if (argc > 1) {
    config_path = argv[1];
  }

  auto config_or = mxdb::ConfigLoader::LoadFromFile(config_path);
  if (!config_or.ok()) {
    std::cerr << "config load failed: " << config_or.status().message() << "\n";
    return 1;
  }
  mxdb::EngineConfig config = config_or.value();

  auto process_lock = mxdb::DataDirProcessLock::Acquire(config.data_dir, "featured");
  if (!process_lock.ok()) {
    std::cerr << "process lock failed: " << process_lock.status().message() << "\n";
    return 1;
  }

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

  std::signal(SIGINT, HandleSignal);
  std::signal(SIGTERM, HandleSignal);

  while (!g_shutdown_requested.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
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
