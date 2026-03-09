#include <cassert>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>

#include "engine/common/config/config.h"

namespace {

mxdb::TimestampMicros NowMicros() {
  const auto now = std::chrono::system_clock::now();
  return std::chrono::duration_cast<std::chrono::microseconds>(
             now.time_since_epoch())
      .count();
}

std::filesystem::path UniqueTmpDir(const std::string& name) {
  const auto base = std::filesystem::temp_directory_path();
  const auto stamp = std::to_string(NowMicros());
  std::filesystem::path path = base / ("mxdb-" + name + "-" + stamp);
  std::filesystem::create_directories(path);
  return path;
}

void WriteText(const std::filesystem::path& path, const std::string& content) {
  std::ofstream out(path);
  assert(out.is_open());
  out << content;
}

}  // namespace

int main() {
  // Missing config file should fail explicitly.
  {
    const auto tmp = UniqueTmpDir("config-missing");
    const auto missing = tmp / "does-not-exist.conf";
    auto config = mxdb::ConfigLoader::LoadFromFile(missing.string());
    assert(!config.ok());
    assert(config.status().code() == mxdb::StatusCode::kNotFound);
    std::filesystem::remove_all(tmp);
  }

  // Relative paths should resolve against config file directory, not caller CWD.
  {
    const auto tmp = UniqueTmpDir("config-relative");
    const auto config_dir = tmp / "cfgdir";
    const auto run_dir = tmp / "run-dir";
    std::filesystem::create_directories(config_dir);
    std::filesystem::create_directories(run_dir);

    const auto config_path = config_dir / "featured.conf";
    WriteText(config_path,
              "data_dir=./data\n"
              "metadata_path=./data/catalog/metadata.db\n"
              "partition_count=4\n");

    const auto original_cwd = std::filesystem::current_path();
    std::filesystem::current_path(run_dir);
    auto config = mxdb::ConfigLoader::LoadFromFile(config_path.string());
    std::filesystem::current_path(original_cwd);

    assert(config.ok());
    assert(config.value().data_dir ==
           (config_dir / "data").lexically_normal().string());
    assert(config.value().metadata_path ==
           (config_dir / "data/catalog/metadata.db").lexically_normal().string());

    std::filesystem::remove_all(tmp);
  }

  // Malformed numeric config should return InvalidArgument rather than throw.
  {
    const auto tmp = UniqueTmpDir("config-bad-number");
    const auto config_path = tmp / "featured.conf";
    WriteText(config_path, "partition_count=not-a-number\n");
    auto config = mxdb::ConfigLoader::LoadFromFile(config_path.string());
    assert(!config.ok());
    assert(config.status().code() == mxdb::StatusCode::kInvalidArgument);
    assert(config.status().message().find("partition_count") != std::string::npos);
    std::filesystem::remove_all(tmp);
  }

  return 0;
}
