#pragma once

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "engine/catalog/metadata_store.h"
#include "engine/checkpoint/checkpoint_manager.h"
#include "engine/common/config/config.h"
#include "engine/common/status/status.h"
#include "engine/manifest/manifest.h"
#include "engine/segment/segment_store.h"
#include "engine/types/types.h"
#include "engine/validation/validator.h"
#include "engine/wal/wal_reader.h"
#include "engine/wal/wal_writer.h"

namespace mxdb {

struct LatestQueryResult {
  EntityKey entity;
  std::vector<FeatureValueResult> features;
  CommitToken visible_commit;
};

class FeatureEngine {
 public:
  FeatureEngine(const EngineConfig& config, MetadataStore* metadata_store);

  Status Start();
  Status Stop();

  StatusOr<EntityCommitResult> WriteEntityBatch(
      const EntityFeatureBatch& batch, DurabilityMode durability_mode,
      bool allow_trusted_system_time);

  StatusOr<std::vector<EntityCommitResult>> WriteEntityBatches(
      const std::vector<EntityFeatureBatch>& batches, DurabilityMode durability_mode,
      bool allow_trusted_system_time);

  Status RecoverFromWal(const WalReplayResult& replay);

  Status TriggerCheckpoint();
  Status CompactImmutableSegments();
  Status SetReadOnly(bool read_only);
  bool IsReadOnly() const;
  uint64_t ManifestVersion() const;
  size_t SegmentCount() const;
  void InjectFlushFailureForTest(size_t count = 1);
  void InjectStartFailureForTest(size_t count = 1);
  void InjectPauseAfterAdmissionForTest(size_t count = 1);
  void InjectPauseAfterWalAppendForTest(size_t count = 1);
  void InjectSkipApplyAfterWalAppendForTest(size_t count = 1);
  bool WaitForPausedWritesForTest(size_t min_paused_writers,
                                  uint64_t timeout_ms = 1000) const;
  void ReleasePausedWritesForTest();
  void ResetPausedWritesForTest();

  StatusOr<LatestQueryResult> GetLatest(
      const EntityKey& entity, const std::vector<std::string>& feature_ids,
      std::optional<Lsn> min_visible_lsn = std::nullopt) const;
  StatusOr<std::vector<FeatureEvent>> GetLatestEvents(
      const EntityKey& entity, const std::string& feature_id, size_t limit,
      bool include_disk = true,
      std::optional<Lsn> min_visible_lsn = std::nullopt) const;
  StatusOr<std::vector<FeatureEvent>> GetRangeEvents(
      const EntityKey& entity, const std::string& feature_id,
      TimestampMicros furthest_event_time_us,
      std::optional<TimestampMicros> latest_event_time_us = std::nullopt,
      bool include_disk = true,
      std::optional<Lsn> min_visible_lsn = std::nullopt) const;

  StatusOr<AsOfLookupResult> AsOfLookup(const AsOfLookupInput& input) const;

  StatusOr<std::vector<std::unordered_map<std::string, FeatureValueResult>>>
  BuildTrainingDataset(const std::vector<DrivingRow>& rows,
                       const std::vector<std::string>& feature_ids,
                       TimestampMicros default_system_cutoff_us) const;

  Lsn CurrentLsn() const { return next_lsn_.load() == 0 ? 0 : next_lsn_.load() - 1; }

 private:
  struct PartitionState {
    std::unordered_map<EntityFeatureKey, std::vector<FeatureEvent>,
                       EntityFeatureKeyHash>
        timeline_by_entity_feature;
    std::unordered_map<EntityKey,
                       std::unordered_map<std::string, FeatureEvent>, EntityKeyHash>
        latest_by_entity;
    std::vector<SegmentData> immutable_segments;
    size_t memtable_event_count = 0;
  };

  static bool IsNewer(const FeatureEvent& lhs, const FeatureEvent& rhs);
  static bool IsVisible(const FeatureEvent& event, TimestampMicros event_cutoff_us,
                        TimestampMicros system_cutoff_us);

  size_t PartitionForEntity(const EntityKey& entity) const;

  Status ApplyCommittedBatch(const std::vector<FeatureEvent>& events,
                             bool track_idempotency);
  void ApplyEventToLatestLocked(const FeatureEvent& event, PartitionState* partition);
  void MarkWriteIdSeenLocked(const std::string& write_id);
  bool IsWriteIdSeenLocked(const std::string& write_id) const;
  void MarkWriteIdInflightLocked(const std::string& write_id);
  bool IsWriteIdInflightLocked(const std::string& write_id) const;
  void ReleaseWriteIdsInflightLocked(const std::vector<std::string>& write_ids);
  void ResetRuntimeStateLocked();
  Status FlushPartitionLocked(size_t partition_id);
  Status LoadImmutableSegments();
  Status EnsureStartedForReadLocked() const;
  void MaybePauseWriterForTest(std::atomic<size_t>* counter);
  void NotifyWriteCompletedLocked();

  TimestampMicros NowMicros() const;

  EngineConfig config_;
  Validator validator_;
  WalWriter wal_writer_;
  ManifestLog manifest_;
  SegmentStore segment_store_;
  CheckpointManager checkpoint_manager_;

  mutable std::shared_mutex mu_;
  std::vector<PartitionState> partitions_;
  std::unordered_set<std::string> seen_write_ids_;
  std::unordered_set<std::string> inflight_write_ids_;
  CheckpointState checkpoint_state_;
  size_t fail_flush_for_test_count_ = 0;
  size_t fail_start_for_test_count_ = 0;
  bool read_only_ = false;
  bool started_ = false;
  size_t active_write_batches_ = 0;
  uint64_t write_admission_epoch_ = 0;
  std::condition_variable_any write_barrier_cv_;

  std::atomic<Lsn> next_lsn_{1};
  std::atomic<Lsn> highest_applied_lsn_{0};
  std::atomic<uint64_t> next_sequence_no_{1};
  std::atomic<uint64_t> next_segment_id_{1};

  // Test-only synchronization hooks for pausing write paths.
  mutable std::mutex test_pause_mu_;
  mutable std::condition_variable test_pause_cv_;
  std::atomic<size_t> pause_after_admission_for_test_count_{0};
  std::atomic<size_t> pause_after_wal_append_for_test_count_{0};
  std::atomic<size_t> skip_apply_after_wal_append_for_test_count_{0};
  size_t paused_writers_for_test_ = 0;
  bool release_paused_writers_for_test_ = false;
};

}  // namespace mxdb
