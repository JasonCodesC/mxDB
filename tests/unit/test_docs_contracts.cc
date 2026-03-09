#include <cassert>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>

namespace {

std::string ReadFile(const std::filesystem::path& path) {
  std::ifstream in(path);
  assert(in.is_open());
  std::ostringstream out;
  out << in.rdbuf();
  return out.str();
}

}  // namespace

int main() {
  assert(std::filesystem::exists("AGENT.md"));

  const std::string docs_index = ReadFile("docs/README.md");
  const std::string dev_guide = ReadFile("docs/development-guide.md");

  assert(docs_index.find("Public API (Current v1)") != std::string::npos);
  assert(docs_index.find("Planned RPC Schemas (Not v1 Public API)") !=
         std::string::npos);

  assert(dev_guide.find(
             "The `proto/` files are the machine-readable public contract.") ==
         std::string::npos);
  assert(dev_guide.find(
             "Current v1 public API contract is `featurectl` + Python SDK behavior.") !=
         std::string::npos);

  return 0;
}
