#include "engine/storage/feature_engine.h"

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <functional>
#include <mutex>
#include <optional>
#include <unordered_set>
#include <utility>

namespace mxdb {

namespace {

bool ParseSegmentId(const std::string& segment_path, uint64_t* out_segment_id) {
  const auto file = std::filesystem::path(segment_path).filename().string();
  if (file.rfind("segment-", 0) != 0 || file.size() < 15 ||
      file.substr(file.size() - 4) != ".sgm") {
    return false;
  }
  *out_segment_id = std::stoull(file.substr(8, file.size() - 12));
  return true;
}

}  // namespace

FeatureEngine::FeatureEngine(const EngineConfig& config,
                             MetadataStore* metadata_store)
    : config_(config), validator_(*metadata_store) {
  partitions_.resize(std::max<size_t>(1, config_.partition_count));
}

Status FeatureEngine::Start() {
  std::error_code ec;
  std::filesystem::create_directories(config_.wal_dir, ec);
  if (ec) {
    return Status::Internal("failed to create wal dir: " + ec.message());
  }
  std::filesystem::create_directories(config_.segment_dir, ec);
  if (ec) {
    return Status::Internal("failed to create segment dir: " + ec.message());
  }

  Status status = wal_writer_.Open(config_.wal_dir, config_.wal_segment_target_bytes,
                                   config_.wal_group_commit_window_ms,
                                   config_.wal_group_commit_max_records);
  if (!status.ok()) {
    return status;
  }

  status = segment_store_.Open(config_.segment_dir);
  if (!status.ok()) {
    return status;
  }

  status = manifest_.Open(config_.manifest_path);
  if (!status.ok()) {
    return status;
  }

  status = checkpoint_manager_.Open(config_.checkpoint_path);
  if (!status.ok()) {
    return status;
  }

  auto checkpoint = checkpoint_manager_.LoadCheckpoint();
  if (!checkpoint.ok()) {
    return checkpoint.status();
  }
  checkpoint_state_ = checkpoint.value();

  return LoadImmutableSegments();
}

Status FeatureEngine::Stop() { return wal_writer_.Close(); }

StatusOr<EntityCommitResult> FeatureEngine::WriteEntityBatch(
    const EntityFeatureBatch& batch, DurabilityMode durability_mode,
    bool allow_trusted_system_time) {
  {
    std::shared_lock<std::shared_mutex> lock(mu_);
    if (read_only_) {
      return Status::FailedPrecondition("engine is in read-only mode");
    }
  }

  auto validation = validator_.ValidateWriteBatch(batch, allow_trusted_system_time);
  if (!validation.ok()) {
    return validation.status();
  }

  std::vector<FeatureEvent> new_events;
  new_events.reserve(batch.events.size());

  const TimestampMicros commit_system_time_us = NowMicros();
  {
    std::shared_lock<std::shared_mutex> lock(mu_);
    for (const auto& event_input : batch.events) {
      if (IsWriteIdSeenLocked(event_input.write_id)) {
        continue;
      }

      FeatureEvent event;
      event.entity = batch.entity;
      event.feature_id = event_input.feature_id;
      event.event_time_us = event_input.event_time_us;
      event.system_time_us = event_input.system_time_us.value_or(commit_system_time_us);
      event.sequence_no = next_sequence_no_.fetch_add(1);
      event.operation = event_input.operation;
      event.value = event_input.value;
      event.write_id = event_input.write_id;
      event.source_id = event_input.source_id;
      new_events.push_back(std::move(event));
    }
  }

  if (new_events.empty()) {
    EntityCommitResult deduped;
    deduped.entity = batch.entity;
    deduped.commit.lsn = CurrentLsn();
    deduped.commit.commit_system_time_us = commit_system_time_us;
    deduped.accepted_events = 0;
    return deduped;
  }

  const Lsn lsn = next_lsn_.fetch_add(1);

  for (auto& event : new_events) {
    event.lsn = lsn;
  }

  WalBatchPayload payload;
  payload.commit_system_time_us = commit_system_time_us;
  payload.events = new_events;

  Status status = wal_writer_.Append(lsn, payload, durability_mode);
  if (!status.ok()) {
    return status;
  }

  status = ApplyCommittedBatch(new_events, true);
  if (!status.ok()) {
    return status;
  }

  EntityCommitResult result;
  result.entity = batch.entity;
  result.commit.lsn = lsn;
  result.commit.commit_system_time_us = commit_system_time_us;
  result.accepted_events = static_cast<uint32_t>(new_events.size());
  return result;
}

StatusOr<std::vector<EntityCommitResult>> FeatureEngine::WriteEntityBatches(
    const std::vector<EntityFeatureBatch>& batches, DurabilityMode durability_mode,
    bool allow_trusted_system_time) {
  std::vector<EntityCommitResult> results;
  results.reserve(batches.size());

  for (const auto& batch : batches) {
    auto result = WriteEntityBatch(batch, durability_mode, allow_trusted_system_time);
    if (!result.ok()) {
      return result.status();
    }
    results.push_back(result.value());
  }

  return results;
}

Status FeatureEngine::RecoverFromWal(const WalReplayResult& replay) {
  for (const auto& record : replay.records) {
    if (checkpoint_state_.exists &&
        record.header.lsn <= checkpoint_state_.checkpoint_lsn) {
      continue;
    }

    Status status = ApplyCommittedBatch(record.payload.events, true);
    if (!status.ok()) {
      return status;
    }

    const Lsn next_candidate = record.header.lsn + 1;
    Lsn current = next_lsn_.load();
    while (current < next_candidate &&
           !next_lsn_.compare_exchange_weak(current, next_candidate)) {
    }

    uint64_t max_sequence = 0;
    for (const auto& event : record.payload.events) {
      max_sequence = std::max(max_sequence, event.sequence_no);
    }
    uint64_t seq_target = max_sequence + 1;
    uint64_t seq_current = next_sequence_no_.load();
    while (seq_current < seq_target &&
           !next_sequence_no_.compare_exchange_weak(seq_current, seq_target)) {
    }
  }

  return Status::Ok();
}

Status FeatureEngine::TriggerCheckpoint() {
  std::unique_lock<std::shared_mutex> lock(mu_);

  for (size_t partition_id = 0; partition_id < partitions_.size(); ++partition_id) {
    Status flush = FlushPartitionLocked(partition_id);
    if (!flush.ok()) {
      return flush;
    }
  }

  const TimestampMicros now_us = NowMicros();
  const uint64_t checkpoint_lsn = CurrentLsn();
  const uint64_t manifest_version = manifest_.LatestVersion();
  Status status = checkpoint_manager_.SaveCheckpoint(checkpoint_lsn,
                                                     manifest_version, now_us);
  if (!status.ok()) {
    return status;
  }

  checkpoint_state_.exists = true;
  checkpoint_state_.checkpoint_lsn = checkpoint_lsn;
  checkpoint_state_.manifest_version = manifest_version;
  checkpoint_state_.created_at_us = now_us;
  return Status::Ok();
}

Status FeatureEngine::CompactImmutableSegments() {
  std::unique_lock<std::shared_mutex> lock(mu_);

  for (size_t partition_id = 0; partition_id < partitions_.size(); ++partition_id) {
    PartitionState& partition = partitions_[partition_id];
    if (partition.immutable_segments.size() <= 1) {
      continue;
    }

    std::vector<FeatureEvent> merged_events;
    std::unordered_set<std::string> seen_write_ids;
    for (const auto& segment : partition.immutable_segments) {
      for (const auto& [_, timeline] : segment.timeline_by_entity_feature) {
        for (const auto& event : timeline) {
          if (!event.write_id.empty() &&
              seen_write_ids.find(event.write_id) != seen_write_ids.end()) {
            continue;
          }
          if (!event.write_id.empty()) {
            seen_write_ids.insert(event.write_id);
          }
          merged_events.push_back(event);
        }
      }
    }

    if (merged_events.empty()) {
      continue;
    }

    const uint64_t segment_id = next_segment_id_.fetch_add(1);
    auto compacted = segment_store_.WriteSegment(partition_id, segment_id, merged_events);
    if (!compacted.ok()) {
      return compacted.status();
    }

    partition.immutable_segments.clear();
    partition.immutable_segments.push_back(compacted.value());
  }

  std::vector<ManifestEntry> active_entries;
  for (const auto& partition : partitions_) {
    for (const auto& segment : partition.immutable_segments) {
      active_entries.push_back(segment.manifest);
    }
  }

  Status status = manifest_.RewriteEntries(active_entries);
  if (!status.ok()) {
    return status;
  }

  uint64_t version = 0;
  for (auto& partition : partitions_) {
    for (auto& segment : partition.immutable_segments) {
      segment.manifest.manifest_version = ++version;
    }
  }

  return Status::Ok();
}

Status FeatureEngine::SetReadOnly(bool read_only) {
  std::unique_lock<std::shared_mutex> lock(mu_);
  read_only_ = read_only;
  return Status::Ok();
}

bool FeatureEngine::IsReadOnly() const {
  std::shared_lock<std::shared_mutex> lock(mu_);
  return read_only_;
}

uint64_t FeatureEngine::ManifestVersion() const { return manifest_.LatestVersion(); }

size_t FeatureEngine::SegmentCount() const {
  std::shared_lock<std::shared_mutex> lock(mu_);
  size_t count = 0;
  for (const auto& partition : partitions_) {
    count += partition.immutable_segments.size();
  }
  return count;
}

StatusOr<LatestQueryResult> FeatureEngine::GetLatest(
    const EntityKey& entity, const std::vector<std::string>& feature_ids,
    std::optional<Lsn> min_visible_lsn) const {
  LatestQueryResult result;
  result.entity = entity;

  std::shared_lock<std::shared_mutex> lock(mu_);
  const PartitionState& partition = partitions_[PartitionForEntity(entity)];

  auto entity_it = partition.latest_by_entity.find(entity);
  if (entity_it == partition.latest_by_entity.end()) {
    return Status::NotFound("entity not found in latest cache");
  }

  for (const auto& feature_id : feature_ids) {
    FeatureValueResult value_result;
    value_result.feature_id = feature_id;

    const auto feature_it = entity_it->second.find(feature_id);
    if (feature_it == entity_it->second.end()) {
      value_result.found = false;
      result.features.push_back(std::move(value_result));
      continue;
    }

    const FeatureEvent& event = feature_it->second;
    if (min_visible_lsn.has_value() && event.lsn < min_visible_lsn.value()) {
      return Status::FailedPrecondition("latest value does not satisfy min_visible_lsn");
    }

    result.visible_commit.lsn = std::max(result.visible_commit.lsn, event.lsn);
    result.visible_commit.commit_system_time_us =
        std::max(result.visible_commit.commit_system_time_us, event.system_time_us);

    if (event.operation == OperationType::kDelete) {
      value_result.found = false;
      result.features.push_back(std::move(value_result));
      continue;
    }

    value_result.value = event.value;
    value_result.event_time_us = event.event_time_us;
    value_result.system_time_us = event.system_time_us;
    value_result.found = true;
    result.features.push_back(std::move(value_result));
  }

  return result;
}

StatusOr<AsOfLookupResult> FeatureEngine::AsOfLookup(
    const AsOfLookupInput& input) const {
  AsOfLookupResult result;
  result.entity = input.entity;
  result.event_cutoff_us = input.event_cutoff_us;
  result.system_cutoff_us = input.system_cutoff_us;

  std::shared_lock<std::shared_mutex> lock(mu_);
  const PartitionState& partition = partitions_[PartitionForEntity(input.entity)];

  for (const auto& feature_id : input.feature_ids) {
    FeatureValueResult value_result;
    value_result.feature_id = feature_id;

    std::optional<FeatureEvent> best;

    auto consider_timeline = [&](const std::vector<FeatureEvent>& timeline) {
      auto it = std::find_if(timeline.begin(), timeline.end(),
                             [&](const FeatureEvent& event) {
                               return IsVisible(event, input.event_cutoff_us,
                                                input.system_cutoff_us);
                             });
      if (it == timeline.end()) {
        return;
      }
      if (!best.has_value() || IsNewer(*it, *best)) {
        best = *it;
      }
    };

    EntityFeatureKey key{input.entity, feature_id};
    auto mem_it = partition.timeline_by_entity_feature.find(key);
    if (mem_it != partition.timeline_by_entity_feature.end()) {
      consider_timeline(mem_it->second);
    }

    for (const auto& segment : partition.immutable_segments) {
      if (segment.manifest.min_event_time_us > input.event_cutoff_us ||
          segment.manifest.min_system_time_us > input.system_cutoff_us) {
        continue;
      }

      auto segment_it = segment.timeline_by_entity_feature.find(key);
      if (segment_it == segment.timeline_by_entity_feature.end()) {
        continue;
      }
      consider_timeline(segment_it->second);
    }

    if (!best.has_value() || best->operation == OperationType::kDelete) {
      value_result.found = false;
      result.features.push_back(std::move(value_result));
      continue;
    }

    value_result.value = best->value;
    value_result.event_time_us = best->event_time_us;
    value_result.system_time_us = best->system_time_us;
    value_result.found = true;
    result.features.push_back(std::move(value_result));
  }

  return result;
}

StatusOr<std::vector<std::unordered_map<std::string, FeatureValueResult>>>
FeatureEngine::BuildTrainingDataset(const std::vector<DrivingRow>& rows,
                                    const std::vector<std::string>& feature_ids,
                                    TimestampMicros default_system_cutoff_us) const {
  std::vector<std::unordered_map<std::string, FeatureValueResult>> output;
  output.reserve(rows.size());

  for (const auto& row : rows) {
    AsOfLookupInput request;
    request.entity = row.entity;
    request.feature_ids = feature_ids;
    request.event_cutoff_us = row.label_time_us;
    request.system_cutoff_us = row.system_cutoff_us.value_or(default_system_cutoff_us);

    auto as_of = AsOfLookup(request);
    if (!as_of.ok()) {
      return as_of.status();
    }

    std::unordered_map<std::string, FeatureValueResult> projected;
    projected.emplace("__row_id__", FeatureValueResult{
                                        .feature_id = "__row_id__",
                                        .value = FeatureValue{.type = ValueType::kString,
                                                              .value = row.row_id},
                                        .event_time_us = row.label_time_us,
                                        .system_time_us = request.system_cutoff_us,
                                        .found = true,
                                    });

    for (const auto& feature : as_of.value().features) {
      projected.emplace(feature.feature_id, feature);
    }
    output.push_back(std::move(projected));
  }

  return output;
}

bool FeatureEngine::IsNewer(const FeatureEvent& lhs, const FeatureEvent& rhs) {
  if (lhs.event_time_us != rhs.event_time_us) {
    return lhs.event_time_us > rhs.event_time_us;
  }
  if (lhs.system_time_us != rhs.system_time_us) {
    return lhs.system_time_us > rhs.system_time_us;
  }
  return lhs.sequence_no > rhs.sequence_no;
}

bool FeatureEngine::IsVisible(const FeatureEvent& event,
                              TimestampMicros event_cutoff_us,
                              TimestampMicros system_cutoff_us) {
  return event.event_time_us <= event_cutoff_us &&
         event.system_time_us <= system_cutoff_us;
}

size_t FeatureEngine::PartitionForEntity(const EntityKey& entity) const {
  const size_t hash = EntityKeyHash{}(entity);
  return hash % partitions_.size();
}

Status FeatureEngine::ApplyCommittedBatch(const std::vector<FeatureEvent>& events,
                                          bool track_idempotency) {
  std::unique_lock<std::shared_mutex> lock(mu_);
  std::unordered_set<size_t> touched_partitions;

  for (const auto& event : events) {
    if (track_idempotency && IsWriteIdSeenLocked(event.write_id)) {
      continue;
    }

    const size_t partition_id = PartitionForEntity(event.entity);
    PartitionState& partition = partitions_[partition_id];

    EntityFeatureKey key{event.entity, event.feature_id};
    auto& timeline = partition.timeline_by_entity_feature[key];
    timeline.push_back(event);
    partition.memtable_event_count += 1;

    std::sort(timeline.begin(), timeline.end(),
              [](const FeatureEvent& lhs, const FeatureEvent& rhs) {
                return IsNewer(lhs, rhs);
              });

    ApplyEventToLatestLocked(event, &partition);

    if (track_idempotency && !event.write_id.empty()) {
      MarkWriteIdSeenLocked(event.write_id);
    }

    touched_partitions.insert(partition_id);
  }

  for (size_t partition_id : touched_partitions) {
    if (partitions_[partition_id].memtable_event_count >=
        config_.memtable_flush_event_threshold) {
      Status flush = FlushPartitionLocked(partition_id);
      if (!flush.ok()) {
        return flush;
      }
    }
  }

  return Status::Ok();
}

void FeatureEngine::ApplyEventToLatestLocked(const FeatureEvent& event,
                                             PartitionState* partition) {
  auto& latest_map = partition->latest_by_entity[event.entity];
  auto latest_it = latest_map.find(event.feature_id);
  if (latest_it == latest_map.end() || IsNewer(event, latest_it->second)) {
    latest_map[event.feature_id] = event;
  }

  const Lsn next_candidate = event.lsn + 1;
  Lsn current = next_lsn_.load();
  while (current < next_candidate &&
         !next_lsn_.compare_exchange_weak(current, next_candidate)) {
  }

  uint64_t seq_target = event.sequence_no + 1;
  uint64_t seq_current = next_sequence_no_.load();
  while (seq_current < seq_target &&
         !next_sequence_no_.compare_exchange_weak(seq_current, seq_target)) {
  }
}

void FeatureEngine::MarkWriteIdSeenLocked(const std::string& write_id) {
  if (!write_id.empty()) {
    seen_write_ids_.insert(write_id);
  }
}

bool FeatureEngine::IsWriteIdSeenLocked(const std::string& write_id) const {
  if (write_id.empty()) {
    return false;
  }
  return seen_write_ids_.find(write_id) != seen_write_ids_.end();
}

Status FeatureEngine::FlushPartitionLocked(size_t partition_id) {
  PartitionState& partition = partitions_[partition_id];
  if (partition.memtable_event_count == 0) {
    return Status::Ok();
  }

  std::vector<FeatureEvent> events;
  events.reserve(partition.memtable_event_count);
  for (const auto& [_, timeline] : partition.timeline_by_entity_feature) {
    events.insert(events.end(), timeline.begin(), timeline.end());
  }

  if (events.empty()) {
    partition.memtable_event_count = 0;
    partition.timeline_by_entity_feature.clear();
    return Status::Ok();
  }

  const uint64_t segment_id = next_segment_id_.fetch_add(1);
  auto segment = segment_store_.WriteSegment(partition_id, segment_id, events);
  if (!segment.ok()) {
    return segment.status();
  }

  auto manifest_entry = manifest_.AppendSegment(segment.value().manifest);
  if (!manifest_entry.ok()) {
    return manifest_entry.status();
  }

  SegmentData persisted_segment = segment.value();
  persisted_segment.manifest = manifest_entry.value();
  partition.immutable_segments.push_back(std::move(persisted_segment));

  partition.timeline_by_entity_feature.clear();
  partition.memtable_event_count = 0;
  return Status::Ok();
}

Status FeatureEngine::LoadImmutableSegments() {
  auto entries = manifest_.LoadEntries();
  if (!entries.ok()) {
    return entries.status();
  }

  for (const auto& entry : entries.value()) {
    auto segment = segment_store_.LoadSegment(entry);
    if (!segment.ok()) {
      return segment.status();
    }

    uint64_t parsed_segment_id = 0;
    if (ParseSegmentId(entry.segment_path, &parsed_segment_id)) {
      uint64_t current_segment_id = next_segment_id_.load();
      const uint64_t next_candidate = parsed_segment_id + 1;
      while (current_segment_id < next_candidate &&
             !next_segment_id_.compare_exchange_weak(current_segment_id,
                                                     next_candidate)) {
      }
    }

    std::unique_lock<std::shared_mutex> lock(mu_);
    PartitionState& partition = partitions_[entry.partition_id];
    partition.immutable_segments.push_back(segment.value());

    for (const auto& [_, timeline] : segment.value().timeline_by_entity_feature) {
      for (const auto& event : timeline) {
        ApplyEventToLatestLocked(event, &partition);
        MarkWriteIdSeenLocked(event.write_id);
      }
    }
  }

  return Status::Ok();
}

TimestampMicros FeatureEngine::NowMicros() const {
  const auto now = std::chrono::system_clock::now();
  return std::chrono::duration_cast<std::chrono::microseconds>(
             now.time_since_epoch())
      .count();
}

}  // namespace mxdb
