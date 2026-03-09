#include <cassert>
#include <filesystem>
#include <fstream>
#include <regex>
#include <sstream>
#include <string>
#include <vector>

namespace {

std::string ReadFile(const std::filesystem::path& path) {
  std::ifstream in(path);
  assert(in.is_open());
  std::ostringstream out;
  out << in.rdbuf();
  return out.str();
}

std::vector<std::string> ExtractMarkdownLinks(const std::string& markdown) {
  static const std::regex kLinkPattern(R"(\[[^\]]+\]\(([^)]+)\))");
  std::vector<std::string> links;
  for (std::sregex_iterator it(markdown.begin(), markdown.end(), kLinkPattern);
       it != std::sregex_iterator(); ++it) {
    links.push_back((*it)[1].str());
  }
  return links;
}

bool IsExternalLink(const std::string& target) {
  return target.rfind("http://", 0) == 0 || target.rfind("https://", 0) == 0 ||
         target.rfind("mailto:", 0) == 0;
}

void AssertLocalMarkdownLinksResolve(const std::filesystem::path& markdown_path) {
  const std::string content = ReadFile(markdown_path);
  const auto links = ExtractMarkdownLinks(content);
  for (const std::string& raw_target : links) {
    if (raw_target.empty() || raw_target[0] == '#') {
      continue;
    }
    if (IsExternalLink(raw_target)) {
      continue;
    }

    const size_t anchor = raw_target.find('#');
    const std::string target =
        anchor == std::string::npos ? raw_target : raw_target.substr(0, anchor);
    if (target.empty()) {
      continue;
    }

    const std::filesystem::path resolved =
        (markdown_path.parent_path() / target).lexically_normal();
    assert(std::filesystem::exists(resolved));
  }
}

}  // namespace

int main() {
  assert(std::filesystem::exists("AGENT.md"));

  AssertLocalMarkdownLinksResolve("README.md");
  AssertLocalMarkdownLinksResolve("docs/README.md");
  AssertLocalMarkdownLinksResolve("CONTRIBUTING.md");

  const std::string root_readme = ReadFile("README.md");
  const std::string docs_index = ReadFile("docs/README.md");
  const std::string dev_guide = ReadFile("docs/development-guide.md");
  const std::string sdk_readme = ReadFile("sdk/python/README.md");

  assert(docs_index.find("Public API (Current v1)") != std::string::npos);
  assert(docs_index.find("Planned RPC Schemas (Not v1 Public API)") !=
         std::string::npos);

  assert(dev_guide.find(
             "The `proto/` files are the machine-readable public contract.") ==
         std::string::npos);
  assert(dev_guide.find(
             "Current v1 public API contract is `featurectl` + Python SDK behavior.") !=
         std::string::npos);

  assert(root_readme.find("`write_id` is optional in Python") != std::string::npos);
  assert(root_readme.find("historical **as-of** retrieval with both `event_time` and `system_time`") ==
         std::string::npos);
  assert(root_readme.find(
             "internal engine support for as-of/PIT primitives (not yet public CLI/SDK APIs)") !=
         std::string::npos);
  assert(root_readme.find("current public v1 read API: `latest`, `get`, `range`") !=
         std::string::npos);

  assert(sdk_readme.find(
             "upsert <namespace> <entity_name> <feature_id> <event_us> <value> [write_id]") !=
         std::string::npos);
  assert(sdk_readme.find(
             "delete <namespace> <entity_name> <feature_id> <event_us> [write_id]") !=
         std::string::npos);

  return 0;
}
