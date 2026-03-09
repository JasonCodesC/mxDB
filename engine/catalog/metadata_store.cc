#include "engine/catalog/metadata_store.h"

#include <filesystem>
#include <sstream>
#include <string>
#include <vector>

namespace mxdb {

namespace {

using StmtPtr = std::unique_ptr<sqlite3_stmt, decltype(&sqlite3_finalize)>;

Status Exec(sqlite3* db, const char* sql) {
  char* err = nullptr;
  const int rc = sqlite3_exec(db, sql, nullptr, nullptr, &err);
  if (rc != SQLITE_OK) {
    std::string message = err != nullptr ? err : "unknown sqlite error";
    sqlite3_free(err);
    return Status::Internal(message);
  }
  return Status::Ok();
}

StmtPtr Prepare(sqlite3* db, const char* sql, Status* status) {
  sqlite3_stmt* raw = nullptr;
  const int rc = sqlite3_prepare_v2(db, sql, -1, &raw, nullptr);
  if (rc != SQLITE_OK) {
    *status = Status::Internal(sqlite3_errmsg(db));
    return StmtPtr(nullptr, sqlite3_finalize);
  }
  *status = Status::Ok();
  return StmtPtr(raw, sqlite3_finalize);
}

FeatureDefinition ReadFeature(sqlite3_stmt* stmt) {
  FeatureDefinition feature;
  feature.tenant_id =
      reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
  feature.feature_id =
      reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1));
  feature.feature_name =
      reinterpret_cast<const char*>(sqlite3_column_text(stmt, 2));
  feature.entity_type =
      reinterpret_cast<const char*>(sqlite3_column_text(stmt, 3));

  const auto value_type = ParseValueType(
      reinterpret_cast<const char*>(sqlite3_column_text(stmt, 4)));
  if (value_type.ok()) {
    feature.value_type = value_type.value();
  }

  feature.serving_enabled = sqlite3_column_int(stmt, 5) != 0;
  feature.historical_enabled = sqlite3_column_int(stmt, 6) != 0;
  feature.allow_external_system_time = sqlite3_column_int(stmt, 7) != 0;
  feature.nullable = sqlite3_column_int(stmt, 8) != 0;

  feature.description =
      reinterpret_cast<const char*>(sqlite3_column_text(stmt, 9));
  feature.owner = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 10));
  const std::string raw_tags =
      reinterpret_cast<const char*>(sqlite3_column_text(stmt, 11));
  std::istringstream tags_in(raw_tags);
  std::string tag;
  while (std::getline(tags_in, tag, ',')) {
    if (!tag.empty()) {
      feature.tags.push_back(tag);
    }
  }
  feature.retention_policy_id =
      reinterpret_cast<const char*>(sqlite3_column_text(stmt, 12));
  feature.freshness_sla =
      reinterpret_cast<const char*>(sqlite3_column_text(stmt, 13));
  feature.created_at_us = sqlite3_column_int64(stmt, 14);
  feature.updated_at_us = sqlite3_column_int64(stmt, 15);
  return feature;
}

Status ValidateFeatureDefinitionShape(const FeatureDefinition& feature) {
  if (feature.tenant_id.empty()) {
    return Status::InvalidArgument("tenant_id is required");
  }
  if (feature.feature_id.empty()) {
    return Status::InvalidArgument("feature_id is required");
  }
  if (feature.feature_name.empty()) {
    return Status::InvalidArgument("feature_name is required");
  }
  if (feature.entity_type.empty()) {
    return Status::InvalidArgument("entity_type is required");
  }
  if (feature.value_type == ValueType::kUnspecified) {
    return Status::InvalidArgument("value_type is required");
  }
  return Status::Ok();
}

}  // namespace

MetadataStore::~MetadataStore() { Close(); }

Status MetadataStore::Open(const std::string& sqlite_path) {
  const auto parent = std::filesystem::path(sqlite_path).parent_path();
  if (!parent.empty()) {
    std::error_code ec;
    std::filesystem::create_directories(parent, ec);
    if (ec) {
      return Status::Internal("failed to create metadata directory: " +
                              ec.message());
    }
  }

  const int rc = sqlite3_open_v2(sqlite_path.c_str(), &db_,
                                 SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE |
                                     SQLITE_OPEN_FULLMUTEX,
                                 nullptr);
  if (rc != SQLITE_OK) {
    return SqliteError(db_, "sqlite open failed");
  }

  Status fk_status = Exec(db_, "PRAGMA foreign_keys = ON;");
  if (!fk_status.ok()) {
    return fk_status;
  }

  return RunMigrations();
}

Status MetadataStore::Close() {
  if (db_ == nullptr) {
    return Status::Ok();
  }
  if (fail_close_for_test_count_ > 0) {
    --fail_close_for_test_count_;
    return Status::Internal("injected metadata close failure");
  }

  const int rc = sqlite3_close(db_);
  if (rc != SQLITE_OK) {
    return Status::Internal("failed to close sqlite database");
  }
  db_ = nullptr;
  return Status::Ok();
}

void MetadataStore::InjectCloseFailureForTest(size_t count) {
  fail_close_for_test_count_ += count;
}

Status MetadataStore::RunMigrations() {
  if (db_ == nullptr) {
    return Status::FailedPrecondition("metadata store not opened");
  }

  const char* kSchema = R"SQL(
BEGIN TRANSACTION;

CREATE TABLE IF NOT EXISTS schema_migrations (
  version INTEGER PRIMARY KEY,
  applied_at_us INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS features (
  tenant_id TEXT NOT NULL,
  feature_id TEXT NOT NULL,
  feature_name TEXT NOT NULL,
  entity_type TEXT NOT NULL,
  value_type TEXT NOT NULL,
  serving_enabled INTEGER NOT NULL,
  historical_enabled INTEGER NOT NULL,
  allow_external_system_time INTEGER NOT NULL,
  nullable INTEGER NOT NULL,
  description TEXT NOT NULL,
  owner TEXT NOT NULL,
  tags_csv TEXT NOT NULL,
  retention_policy_id TEXT NOT NULL,
  freshness_sla TEXT NOT NULL,
  created_at_us INTEGER NOT NULL,
  updated_at_us INTEGER NOT NULL,
  PRIMARY KEY (tenant_id, feature_id),
  UNIQUE (tenant_id, entity_type, feature_name)
);

CREATE INDEX IF NOT EXISTS idx_features_tenant_entity
ON features(tenant_id, entity_type);

CREATE TABLE IF NOT EXISTS feature_groups (
  tenant_id TEXT NOT NULL,
  group_id TEXT NOT NULL,
  group_name TEXT NOT NULL,
  entity_type TEXT NOT NULL,
  description TEXT NOT NULL,
  created_at_us INTEGER NOT NULL,
  updated_at_us INTEGER NOT NULL,
  PRIMARY KEY (tenant_id, group_id),
  UNIQUE (tenant_id, entity_type, group_name)
);

CREATE TABLE IF NOT EXISTS feature_group_members (
  tenant_id TEXT NOT NULL,
  group_id TEXT NOT NULL,
  feature_id TEXT NOT NULL,
  PRIMARY KEY (tenant_id, group_id, feature_id),
  FOREIGN KEY (tenant_id, group_id) REFERENCES feature_groups(tenant_id, group_id)
    ON DELETE CASCADE,
  FOREIGN KEY (tenant_id, feature_id) REFERENCES features(tenant_id, feature_id)
    ON DELETE CASCADE
);

INSERT OR IGNORE INTO schema_migrations(version, applied_at_us) VALUES (1, strftime('%s','now') * 1000000);

COMMIT;
)SQL";

  return Exec(db_, kSchema);
}

Status MetadataStore::CreateFeature(const FeatureDefinition& feature) {
  if (db_ == nullptr) {
    return Status::FailedPrecondition("metadata store not opened");
  }
  Status validation = ValidateFeatureDefinitionForPersistence(feature);
  if (!validation.ok()) {
    return validation;
  }

  Status status;
  auto stmt = Prepare(
      db_,
      "INSERT INTO features(tenant_id, feature_id, feature_name, entity_type, "
      "value_type, serving_enabled, historical_enabled, allow_external_system_time, "
      "nullable, description, owner, tags_csv, retention_policy_id, freshness_sla, "
      "created_at_us, updated_at_us) "
      "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
      &status);
  if (!status.ok()) {
    return status;
  }

  sqlite3_bind_text(stmt.get(), 1, feature.tenant_id.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt.get(), 2, feature.feature_id.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt.get(), 3, feature.feature_name.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt.get(), 4, feature.entity_type.c_str(), -1, SQLITE_TRANSIENT);
  const std::string value_type = ToString(feature.value_type);
  sqlite3_bind_text(stmt.get(), 5, value_type.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_int(stmt.get(), 6, feature.serving_enabled ? 1 : 0);
  sqlite3_bind_int(stmt.get(), 7, feature.historical_enabled ? 1 : 0);
  sqlite3_bind_int(stmt.get(), 8, feature.allow_external_system_time ? 1 : 0);
  sqlite3_bind_int(stmt.get(), 9, feature.nullable ? 1 : 0);
  sqlite3_bind_text(stmt.get(), 10, feature.description.c_str(), -1,
                    SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt.get(), 11, feature.owner.c_str(), -1, SQLITE_TRANSIENT);
  const std::string tags = EncodeTags(feature.tags);
  sqlite3_bind_text(stmt.get(), 12, tags.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt.get(), 13, feature.retention_policy_id.c_str(), -1,
                    SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt.get(), 14, feature.freshness_sla.c_str(), -1,
                    SQLITE_TRANSIENT);
  sqlite3_bind_int64(stmt.get(), 15, feature.created_at_us);
  sqlite3_bind_int64(stmt.get(), 16, feature.updated_at_us);

  const int rc = sqlite3_step(stmt.get());
  if (rc == SQLITE_CONSTRAINT || rc == SQLITE_CONSTRAINT_PRIMARYKEY ||
      rc == SQLITE_CONSTRAINT_UNIQUE) {
    return Status::AlreadyExists("feature already exists");
  }
  if (rc != SQLITE_DONE) {
    return SqliteError(db_, "create feature failed");
  }

  return Status::Ok();
}

Status MetadataStore::UpdateFeature(const FeatureDefinition& feature) {
  if (db_ == nullptr) {
    return Status::FailedPrecondition("metadata store not opened");
  }
  Status validation = ValidateFeatureDefinitionForPersistence(feature);
  if (!validation.ok()) {
    return validation;
  }

  Status status;
  auto stmt = Prepare(
      db_,
      "UPDATE features SET serving_enabled = ?, historical_enabled = ?, "
      "allow_external_system_time = ?, nullable = ?, description = ?, owner = ?, "
      "tags_csv = ?, retention_policy_id = ?, freshness_sla = ?, updated_at_us = ? "
      "WHERE tenant_id = ? AND feature_id = ?",
      &status);
  if (!status.ok()) {
    return status;
  }

  sqlite3_bind_int(stmt.get(), 1, feature.serving_enabled ? 1 : 0);
  sqlite3_bind_int(stmt.get(), 2, feature.historical_enabled ? 1 : 0);
  sqlite3_bind_int(stmt.get(), 3, feature.allow_external_system_time ? 1 : 0);
  sqlite3_bind_int(stmt.get(), 4, feature.nullable ? 1 : 0);
  sqlite3_bind_text(stmt.get(), 5, feature.description.c_str(), -1,
                    SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt.get(), 6, feature.owner.c_str(), -1, SQLITE_TRANSIENT);
  const std::string tags = EncodeTags(feature.tags);
  sqlite3_bind_text(stmt.get(), 7, tags.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt.get(), 8, feature.retention_policy_id.c_str(), -1,
                    SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt.get(), 9, feature.freshness_sla.c_str(), -1,
                    SQLITE_TRANSIENT);
  sqlite3_bind_int64(stmt.get(), 10, feature.updated_at_us);
  sqlite3_bind_text(stmt.get(), 11, feature.tenant_id.c_str(), -1,
                    SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt.get(), 12, feature.feature_id.c_str(), -1,
                    SQLITE_TRANSIENT);

  const int rc = sqlite3_step(stmt.get());
  if (rc != SQLITE_DONE) {
    return SqliteError(db_, "update feature failed");
  }

  if (sqlite3_changes(db_) == 0) {
    return Status::NotFound("feature not found");
  }

  return Status::Ok();
}

StatusOr<FeatureDefinition> MetadataStore::GetFeatureById(
    const std::string& tenant_id, const std::string& feature_id) const {
  if (db_ == nullptr) {
    return Status::FailedPrecondition("metadata store not opened");
  }

  Status status;
  auto stmt = Prepare(
      db_,
      "SELECT tenant_id, feature_id, feature_name, entity_type, value_type, "
      "serving_enabled, historical_enabled, allow_external_system_time, nullable, "
      "description, owner, tags_csv, retention_policy_id, freshness_sla, "
      "created_at_us, updated_at_us "
      "FROM features WHERE tenant_id = ? AND feature_id = ?",
      &status);
  if (!status.ok()) {
    return status;
  }

  sqlite3_bind_text(stmt.get(), 1, tenant_id.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt.get(), 2, feature_id.c_str(), -1, SQLITE_TRANSIENT);

  const int rc = sqlite3_step(stmt.get());
  if (rc == SQLITE_ROW) {
    return ReadFeature(stmt.get());
  }
  if (rc == SQLITE_DONE) {
    return Status::NotFound("feature not found");
  }
  return SqliteError(db_, "get feature by id failed");
}

StatusOr<FeatureDefinition> MetadataStore::GetFeatureByName(
    const std::string& tenant_id, const std::string& entity_type,
    const std::string& feature_name) const {
  if (db_ == nullptr) {
    return Status::FailedPrecondition("metadata store not opened");
  }

  Status status;
  auto stmt = Prepare(
      db_,
      "SELECT tenant_id, feature_id, feature_name, entity_type, value_type, "
      "serving_enabled, historical_enabled, allow_external_system_time, nullable, "
      "description, owner, tags_csv, retention_policy_id, freshness_sla, "
      "created_at_us, updated_at_us "
      "FROM features WHERE tenant_id = ? AND entity_type = ? AND feature_name = ?",
      &status);
  if (!status.ok()) {
    return status;
  }

  sqlite3_bind_text(stmt.get(), 1, tenant_id.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt.get(), 2, entity_type.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt.get(), 3, feature_name.c_str(), -1, SQLITE_TRANSIENT);

  const int rc = sqlite3_step(stmt.get());
  if (rc == SQLITE_ROW) {
    return ReadFeature(stmt.get());
  }
  if (rc == SQLITE_DONE) {
    return Status::NotFound("feature not found");
  }
  return SqliteError(db_, "get feature by name failed");
}

StatusOr<std::vector<FeatureDefinition>> MetadataStore::ListFeatures(
    const std::string& tenant_id, std::optional<std::string> entity_type) const {
  if (db_ == nullptr) {
    return Status::FailedPrecondition("metadata store not opened");
  }

  const char* sql_all =
      "SELECT tenant_id, feature_id, feature_name, entity_type, value_type, "
      "serving_enabled, historical_enabled, allow_external_system_time, nullable, "
      "description, owner, tags_csv, retention_policy_id, freshness_sla, "
      "created_at_us, updated_at_us "
      "FROM features WHERE tenant_id = ? ORDER BY entity_type, feature_name";
  const char* sql_filtered =
      "SELECT tenant_id, feature_id, feature_name, entity_type, value_type, "
      "serving_enabled, historical_enabled, allow_external_system_time, nullable, "
      "description, owner, tags_csv, retention_policy_id, freshness_sla, "
      "created_at_us, updated_at_us "
      "FROM features WHERE tenant_id = ? AND entity_type = ? ORDER BY feature_name";

  Status status;
  auto stmt = Prepare(db_, entity_type.has_value() ? sql_filtered : sql_all, &status);
  if (!status.ok()) {
    return status;
  }

  sqlite3_bind_text(stmt.get(), 1, tenant_id.c_str(), -1, SQLITE_TRANSIENT);
  if (entity_type.has_value()) {
    sqlite3_bind_text(stmt.get(), 2, entity_type->c_str(), -1, SQLITE_TRANSIENT);
  }

  std::vector<FeatureDefinition> features;
  while (true) {
    const int rc = sqlite3_step(stmt.get());
    if (rc == SQLITE_DONE) {
      break;
    }
    if (rc != SQLITE_ROW) {
      return SqliteError(db_, "list features failed");
    }
    features.push_back(ReadFeature(stmt.get()));
  }
  return features;
}

Status MetadataStore::CreateFeatureGroup(const FeatureGroup& group) {
  if (db_ == nullptr) {
    return Status::FailedPrecondition("metadata store not opened");
  }

  Status status = Exec(db_, "BEGIN IMMEDIATE TRANSACTION");
  if (!status.ok()) {
    return status;
  }

  auto rollback = [&]() {
    (void)Exec(db_, "ROLLBACK");
  };

  Status stmt_status;
  auto group_stmt = Prepare(
      db_,
      "INSERT INTO feature_groups(tenant_id, group_id, group_name, entity_type, "
      "description, created_at_us, updated_at_us) VALUES (?, ?, ?, ?, ?, ?, ?)",
      &stmt_status);
  if (!stmt_status.ok()) {
    rollback();
    return stmt_status;
  }

  sqlite3_bind_text(group_stmt.get(), 1, group.tenant_id.c_str(), -1,
                    SQLITE_TRANSIENT);
  sqlite3_bind_text(group_stmt.get(), 2, group.group_id.c_str(), -1,
                    SQLITE_TRANSIENT);
  sqlite3_bind_text(group_stmt.get(), 3, group.group_name.c_str(), -1,
                    SQLITE_TRANSIENT);
  sqlite3_bind_text(group_stmt.get(), 4, group.entity_type.c_str(), -1,
                    SQLITE_TRANSIENT);
  sqlite3_bind_text(group_stmt.get(), 5, group.description.c_str(), -1,
                    SQLITE_TRANSIENT);
  sqlite3_bind_int64(group_stmt.get(), 6, group.created_at_us);
  sqlite3_bind_int64(group_stmt.get(), 7, group.updated_at_us);

  int rc = sqlite3_step(group_stmt.get());
  if (rc != SQLITE_DONE) {
    rollback();
    if (rc == SQLITE_CONSTRAINT || rc == SQLITE_CONSTRAINT_PRIMARYKEY ||
        rc == SQLITE_CONSTRAINT_UNIQUE) {
      return Status::AlreadyExists("feature group already exists");
    }
    return SqliteError(db_, "create feature group failed");
  }

  auto member_stmt = Prepare(
      db_,
      "INSERT INTO feature_group_members(tenant_id, group_id, feature_id) VALUES (?, ?, ?)",
      &stmt_status);
  if (!stmt_status.ok()) {
    rollback();
    return stmt_status;
  }

  for (const std::string& feature_id : group.feature_ids) {
    auto feature = GetFeatureById(group.tenant_id, feature_id);
    if (!feature.ok()) {
      rollback();
      return feature.status();
    }
    if (feature.value().entity_type != group.entity_type) {
      rollback();
      return Status::InvalidArgument(
          "feature group member entity_type mismatch");
    }

    sqlite3_reset(member_stmt.get());
    sqlite3_clear_bindings(member_stmt.get());
    sqlite3_bind_text(member_stmt.get(), 1, group.tenant_id.c_str(), -1,
                      SQLITE_TRANSIENT);
    sqlite3_bind_text(member_stmt.get(), 2, group.group_id.c_str(), -1,
                      SQLITE_TRANSIENT);
    sqlite3_bind_text(member_stmt.get(), 3, feature_id.c_str(), -1,
                      SQLITE_TRANSIENT);
    rc = sqlite3_step(member_stmt.get());
    if (rc != SQLITE_DONE) {
      rollback();
      return SqliteError(db_, "create feature group member failed");
    }
  }

  status = Exec(db_, "COMMIT");
  if (!status.ok()) {
    rollback();
    return status;
  }

  return Status::Ok();
}

StatusOr<FeatureGroup> MetadataStore::GetFeatureGroup(
    const std::string& tenant_id, const std::string& group_id) const {
  if (db_ == nullptr) {
    return Status::FailedPrecondition("metadata store not opened");
  }

  Status status;
  auto group_stmt = Prepare(
      db_,
      "SELECT tenant_id, group_id, group_name, entity_type, description, "
      "created_at_us, updated_at_us FROM feature_groups WHERE tenant_id = ? AND group_id = ?",
      &status);
  if (!status.ok()) {
    return status;
  }

  sqlite3_bind_text(group_stmt.get(), 1, tenant_id.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(group_stmt.get(), 2, group_id.c_str(), -1, SQLITE_TRANSIENT);

  const int rc = sqlite3_step(group_stmt.get());
  if (rc == SQLITE_DONE) {
    return Status::NotFound("feature group not found");
  }
  if (rc != SQLITE_ROW) {
    return SqliteError(db_, "get feature group failed");
  }

  FeatureGroup group;
  group.tenant_id = reinterpret_cast<const char*>(sqlite3_column_text(group_stmt.get(), 0));
  group.group_id = reinterpret_cast<const char*>(sqlite3_column_text(group_stmt.get(), 1));
  group.group_name = reinterpret_cast<const char*>(sqlite3_column_text(group_stmt.get(), 2));
  group.entity_type = reinterpret_cast<const char*>(sqlite3_column_text(group_stmt.get(), 3));
  group.description = reinterpret_cast<const char*>(sqlite3_column_text(group_stmt.get(), 4));
  group.created_at_us = sqlite3_column_int64(group_stmt.get(), 5);
  group.updated_at_us = sqlite3_column_int64(group_stmt.get(), 6);

  auto members_stmt = Prepare(
      db_,
      "SELECT feature_id FROM feature_group_members WHERE tenant_id = ? AND group_id = ? ORDER BY feature_id",
      &status);
  if (!status.ok()) {
    return status;
  }

  sqlite3_bind_text(members_stmt.get(), 1, tenant_id.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(members_stmt.get(), 2, group_id.c_str(), -1, SQLITE_TRANSIENT);

  while (true) {
    const int member_rc = sqlite3_step(members_stmt.get());
    if (member_rc == SQLITE_DONE) {
      break;
    }
    if (member_rc != SQLITE_ROW) {
      return SqliteError(db_, "get feature group members failed");
    }
    group.feature_ids.emplace_back(
        reinterpret_cast<const char*>(sqlite3_column_text(members_stmt.get(), 0)));
  }

  return group;
}

Status MetadataStore::SqliteError(sqlite3* db, const std::string& prefix) {
  if (db == nullptr) {
    return Status::Internal(prefix + ": sqlite db is null");
  }
  return Status::Internal(prefix + ": " + sqlite3_errmsg(db));
}

std::string MetadataStore::EncodeTags(const std::vector<std::string>& tags) {
  std::ostringstream out;
  bool first = true;
  for (const auto& tag : tags) {
    if (!first) {
      out << ',';
    }
    out << tag;
    first = false;
  }
  return out.str();
}

std::vector<std::string> MetadataStore::DecodeTags(const std::string& tags) {
  std::vector<std::string> out;
  std::string token;
  std::istringstream in(tags);
  while (std::getline(in, token, ',')) {
    if (!token.empty()) {
      out.push_back(token);
    }
  }
  return out;
}

Status MetadataStore::ValidateFeatureDefinitionForPersistence(
    const FeatureDefinition& feature) {
  return ValidateFeatureDefinitionShape(feature);
}

std::string ToString(ValueType value_type) {
  switch (value_type) {
    case ValueType::kBool:
      return "bool";
    case ValueType::kInt64:
      return "int64";
    case ValueType::kDouble:
      return "double";
    case ValueType::kString:
      return "string";
    case ValueType::kFloatVector:
      return "float_vector";
    case ValueType::kDoubleVector:
      return "double_vector";
    case ValueType::kUnspecified:
      return "unspecified";
  }
  return "unspecified";
}

StatusOr<ValueType> ParseValueType(const std::string& value_type) {
  if (value_type == "bool") {
    return ValueType::kBool;
  }
  if (value_type == "int64") {
    return ValueType::kInt64;
  }
  if (value_type == "double") {
    return ValueType::kDouble;
  }
  if (value_type == "string") {
    return ValueType::kString;
  }
  if (value_type == "float_vector") {
    return ValueType::kFloatVector;
  }
  if (value_type == "double_vector") {
    return ValueType::kDoubleVector;
  }
  if (value_type == "unspecified") {
    return ValueType::kUnspecified;
  }
  return Status::InvalidArgument("unknown value type: " + value_type);
}

}  // namespace mxdb
