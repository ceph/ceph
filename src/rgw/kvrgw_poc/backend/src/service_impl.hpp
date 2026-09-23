// -*- mode:C++; tab-width:8; c-basic-offset:2;
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Author: Gabriel BenHanokh <gbenhano@ibm.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include "constants.hpp"
#include "data_store.hpp"
#include "err_insertion.hpp"
#include "error_codes.hpp"
#include "fdb_latency.hpp"
#include "ops_stats.hpp"
#include "gc_value.hpp"
#include "id_meta.hpp"
#include "id_tag.hpp"
#include "key_buf.hpp"
#include "kv_store.hpp"
#include "object_value.hpp"
#include "ref_tag.hpp"
#include "sweeper.hpp"
#include "tenant_value.hpp"
#include "typed_ids.hpp"
#include "byte_range.hpp"

#include <array>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <expected>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace kvrgw {

struct ErrorStats;

struct TierConfig {
  uint32_t max_inline{kDefaultMaxInline};
  uint32_t max_kv_store{kDefaultMaxKvStore};
  bool kv_store_coalescing{kDefaultKvStoreCoalescing};
  int batch_size{5};
  int batch_timeout_us{1000};
  int batch_threads{8};
};

class KvRgwServiceImpl;

struct BatchPutResult {
  KvrgwErrorCode error_code{KVRGW_ERR_OK};
  version_id_t version_id{};
  BatchPutResult() = default;
  BatchPutResult(KvrgwErrorCode ec) : error_code(ec) {}
  BatchPutResult(KvrgwErrorCode ec, version_id_t vid) : error_code(ec), version_id(vid) {}
};

struct BatchCommitEntry {
  tenant_id_t tenant_id;
  std::string bucket_name;
  std::string object_name;
  RefTag ref_tag{};
  ObjectValue value;
  std::string data;
  ChunkType chunk_type{CHUNK_INLINE};
  std::string if_match;
  std::string if_none_match;
  std::vector<uint8_t> tag_encoded;
  std::array<uint8_t, MAX_META_FRAME_BYTES> metadata_encoded{};
  size_t metadata_encoded_size{0};
  std::chrono::steady_clock::time_point enqueued_at;
  std::promise<BatchPutResult> promise;
};

struct BatchStats {
  std::atomic<int64_t> batch_commits{0};
  std::atomic<int64_t> total_entries_batched{0};
  std::atomic<int64_t> conflict_pushbacks{0};
  std::atomic<int64_t> min_batch_size{0};
  std::atomic<int64_t> max_batch_size{0};
  std::atomic<int64_t> total_wait_us{0};
  std::atomic<int64_t> min_wait_us{0};
  std::atomic<int64_t> max_wait_us{0};
  std::atomic<int64_t> total_queue_size_at_extract{0};

  void reset() {
    batch_commits.store(0);
    total_entries_batched.store(0);
    conflict_pushbacks.store(0);
    min_batch_size.store(0); max_batch_size.store(0);
    total_wait_us.store(0); min_wait_us.store(0); max_wait_us.store(0);
    total_queue_size_at_extract.store(0);
  }
};

class BatchCommitQueue {
 public:
  explicit BatchCommitQueue(KvRgwServiceImpl& service);
  ~BatchCommitQueue();

  void start(int num_threads);
  void stop();
  std::future<BatchPutResult> enqueue(BatchCommitEntry entry);
  BatchStats& stats() { return stats_; }

 private:
  struct InFlightBatch {
    enum Phase : uint8_t { PHASE_EMPTY = 0, PHASE_1 = 1, PHASE_2 = 2, PHASE_3 = 3 };
    enum Step : uint8_t { STEP_WORKING = 0, STEP_COMMIT_ISSUED = 1, STEP_IO_ISSUED = 2 };

    uint8_t phase{PHASE_EMPTY};
    uint8_t step{STEP_WORKING};
    uint8_t entry_count{0};
    uint8_t storage_entry_count{0};
    uint8_t p3_attempt{0};

    bucket_id_t group_bucket_id{};
    GroupPoEntry storage_entries[kMaxBatchSize];
    BatchCommitEntry entries[kMaxBatchSize];

    std::unique_ptr<KvTransaction> txn;
    FdbFuture commit_future;

    void reset() {
      phase = PHASE_EMPTY;
      step = STEP_WORKING;
      for (int i = 0; i < entry_count; ++i) {
        entries[i].bucket_name.clear();
        entries[i].object_name.clear();
        entries[i].data.clear();
        entries[i].if_match.clear();
        entries[i].if_none_match.clear();
        entries[i].tag_encoded.clear();
        entries[i].metadata_encoded_size = 0;
      }
      entry_count = 0;
      storage_entry_count = 0;
      p3_attempt = 0;
      group_bucket_id = kNullBucket;
      txn.reset();
      commit_future = FdbFuture();
    }
  };

  struct WorkerState {
    static constexpr int kMaxInflight = 4;
    InFlightBatch slots[kMaxInflight];
  };

  void run();
  void commit_batch(std::vector<BatchCommitEntry>& batch);
  void start_batch(InFlightBatch& ib);
  bool do_phase3_work(InFlightBatch& ib);

  KvRgwServiceImpl& service_;
  std::mutex mu_;
  std::condition_variable cv_;
  std::deque<BatchCommitEntry> pending_;
  std::atomic<bool> stop_{false};
  std::vector<std::thread> workers_;
  BatchStats stats_;
  static constexpr int kMaxInflight = 4;
  int max_inflight_{kMaxInflight};
};

class TierConfigState;

class KvRgwGrpcService;

bool write_object_value(OValueBuf& buf, const ObjectValue& value);
void apply_tags_to_value(ObjectValue& obj, std::span<const uint8_t> encoded,
                         KvTransaction& tr, bucket_id_t bucket_id,
                         std::string_view ref_tag);
void clear_object_tags(ObjectValue& obj, KvTransaction& tr, bucket_id_t bucket_id,
                       std::string_view ref_tag);

class KvRgwServiceImpl final {
 public:
  KvRgwServiceImpl(KvStore& store, DataStore& data_store, RefTagGenerator& ref_tags,
                   TierConfigState& tier_config, Sweeper& sweeper);

  LatencyStats& latency_stats() { return latency_stats_; }
  OpsStats& ops_stats() { return ops_stats_; }
  ErrorStats& error_stats() { return *error_stats_; }
  ErrInsertion& err_insertion() { return err_insertion_; }
  KvStore& store() { return store_; }
  TierConfigState& tier_config_state() { return tier_config_state_; }

  bool listing_disable_ryw() const { return listing_disable_ryw_; }
  void set_listing_disable_ryw(bool v) { listing_disable_ryw_ = v; }
  RefTagGenerator& ref_tags() { return ref_tags_; }
  BatchCommitQueue& batch_queue() { return batch_queue_; }

  bucket_id_t resolve_bucket_id(tenant_id_t tenant_id, const std::string& bucket_name);
  KvrgwErrorCode create_bucket(tenant_id_t tenant_id, std::string_view bucket_name);
  KvrgwErrorCode delete_bucket(tenant_id_t tenant_id, std::string_view bucket_name);
  KvrgwErrorCode add_tenant(std::string_view tenant_name, tenant_id_t* out_id);
  KvrgwErrorCode resolve_tenant(std::string_view tenant_name, bool* out_exists, tenant_id_t* out_id);
  KvrgwErrorCode bucket_exists(tenant_id_t tenant_id, std::string_view bucket_name,
                               bool* out_exists, bucket_id_t* out_id);
  KvrgwErrorCode bucket_exists_cached(tenant_id_t tenant_id, std::string_view bucket_name,
                               bool* out_exists, bucket_id_t* out_id);
  KvrgwErrorCode put_bucket_versioning(tenant_id_t tenant_id,
                                       std::string_view bucket_name,
                                       VersioningState state);
  KvrgwErrorCode get_bucket_versioning(tenant_id_t tenant_id,
                                       std::string_view bucket_name,
                                       VersioningState* out_state);
  KvrgwErrorCode put_bucket_policy(tenant_id_t tenant_id,
                                   std::string_view bucket_name,
                                   std::string_view policy_json);
  KvrgwErrorCode get_bucket_policy(tenant_id_t tenant_id,
                                   std::string_view bucket_name,
                                   std::string* out_policy_json);
  KvrgwErrorCode delete_bucket_policy(tenant_id_t tenant_id, std::string_view bucket_name);

  struct BucketListEntry {
    std::string name;
    int64_t created_at_unix = 0;
  };

  struct ListBucketsResult {
    std::vector<BucketListEntry> buckets;
    std::string continuation_token;
  };

  KvrgwErrorCode list_buckets(tenant_id_t tenant_id,
                              std::string_view prefix,
                              std::string_view continuation_token,
                              uint32_t max_buckets,
                              ListBucketsResult* out);

  struct ObjectListEntry {
    std::string key;
    uint64_t size = 0;
    std::string etag;
    int64_t last_modified_unix = 0;
  };

  struct ListObjectsResult {
    std::vector<ObjectListEntry> objects;
    std::vector<std::string> common_prefixes;
    bool is_truncated = false;
    std::string next_continuation_token;
  };

  KvrgwErrorCode list_objects(tenant_id_t tenant_id,
                              std::string_view bucket_name,
                              std::string_view prefix,
                              std::string_view delimiter,
                              uint32_t max_keys,
                              std::string_view continuation_token,
                              std::string_view marker,
                              ListObjectsResult* out);

  struct DeleteCondition {
    std::string if_match;
    int64_t if_match_last_modified_time = 0;
    int64_t if_match_size = 0;
    bool has_if_match_size = false;
  };

  KvrgwErrorCode delete_object_version(tenant_id_t tenant_id,
                                       std::string_view bucket_name,
                                       std::string_view key,
                                       version_id_t version_id,
                                       const DeleteCondition* cond);

  struct DeleteMultiObjectRef {
    std::string_view key;
    std::optional<version_id_t> version_id;
  };

  struct DeleteMultiKeyOutcome {
    std::string key;
    enum class Status { Deleted, Error } status{Status::Deleted};
    std::string error_code;
    std::string error_message;
    version_id_t version_id{};
    bool created_dm{false};
    version_id_t dm_version_id{};
  };

  KvrgwErrorCode delete_multi(tenant_id_t tenant_id,
                              std::string_view bucket_name,
                              std::span<const std::string> keys,
                              std::span<const DeleteMultiObjectRef> objects,
                              std::vector<DeleteMultiKeyOutcome>* out);

  struct CopyObjectRequest {
    tenant_id_t tenant_id{};
    std::string_view src_bucket_name;
    std::string_view src_key;
    std::string_view dst_bucket_name;
    std::string_view dst_key;
    std::optional<version_id_t> src_version_id;
    std::string_view if_match;
    std::string_view if_none_match;
    std::string_view dst_if_match;
    std::string_view dst_if_none_match;
    std::string_view content_type;
    bool replace_metadata = false;
    bool replace_tags = false;
    std::span<const uint8_t> tags;
    std::span<const uint8_t> metadata;
  };

  struct CopyObjectResult {
    std::string etag;
    int64_t last_modified_unix = 0;
    std::optional<version_id_t> version_id;
    version_id_t copy_source_version_id{};
  };

  KvrgwErrorCode copy_object(const CopyObjectRequest& req, CopyObjectResult* out);

  struct GetObjectResult {
    ObjectValue value;
    std::string body;
    std::string error_detail;
  };

  KvrgwErrorCode get_object(tenant_id_t tenant_id, std::string_view bucket_name,
                            std::string_view key, std::optional<version_id_t> version_id,
                            const ByteRange* range, GetObjectResult* out);

  KvrgwErrorCode head_object(tenant_id_t tenant_id, std::string_view bucket_name,
                             std::string_view key, std::optional<version_id_t> version_id,
                             ObjectValue* out, std::string* error_detail);

  struct ObjectVersionEntry {
    std::string key;
    version_id_t version_id{};
    bool is_latest = false;
    bool is_delete_marker = false;
    uint64_t size = 0;
    std::string etag;
    int64_t last_modified_unix = 0;
  };

  struct ListObjectVersionsResult {
    std::vector<ObjectVersionEntry> versions;
    bool is_truncated = false;
    std::string next_key_marker;
    version_id_t next_version_id_marker{};
  };

  KvrgwErrorCode list_object_versions(tenant_id_t tenant_id,
                                      std::string_view bucket_name,
                                      std::string_view prefix,
                                      uint32_t max_keys,
                                      std::string_view key_marker,
                                      version_id_t version_id_marker,
                                      ListObjectVersionsResult* out);

  struct DeleteResult {
    bool created_dm = false;
    version_id_t dm_version_id{};
  };

  KvrgwErrorCode delete_object(tenant_id_t tenant_id, std::string_view bucket_name,
                               std::string_view key, const DeleteCondition* cond,
                               DeleteResult* out);

  KvrgwErrorCode put_object_tagging(tenant_id_t tenant_id, std::string_view bucket_name,
                                    std::string_view key, std::span<const uint8_t> tags);
  KvrgwErrorCode get_object_tagging(tenant_id_t tenant_id, std::string_view bucket_name,
                                    std::string_view key,
                                    std::vector<uint8_t>& live,
                                    std::array<TagPair, MAX_TAG_COUNT>& out_tags,
                                    size_t* out_count);
  KvrgwErrorCode delete_object_tagging(tenant_id_t tenant_id, std::string_view bucket_name,
                                       std::string_view key);

  struct PutCondition {
    std::string if_match;
    std::string if_none_match;
  };

  struct PutObjectRequest {
    tenant_id_t tenant_id;
    std::string bucket_name;
    std::string object_name;
    RefTag ref_tag{};
    ObjectValue value;
    uint64_t estimated_size;
    PutCondition* cond;
    std::span<const uint8_t> tags;
    std::span<const uint8_t> metadata;
  };

  struct PutObjectResult {
    KvrgwErrorCode error_code{KVRGW_ERR_OK};
    std::string etag;
    version_id_t version_id{};
  };

  PutObjectResult put_object_route(PutObjectRequest& req, const uint8_t* data, size_t data_len);

  struct PutInTxnParams {
    tenant_id_t tenant_id;
    const std::string& bucket_name;
    bucket_id_t bucket_id;
    const std::string& object_name;
    const RefTag& ref_tag;
    ObjectValue& value;
    const std::string* data;
    const PutCondition* cond;
    std::span<const uint8_t> tags;
    bool skip_bucket_verify;
    bool is_storage_tier;
    std::span<const uint8_t> metadata{};
  };

  KvrgwErrorCode put_object_in_txn(KvTransaction& tr, PutInTxnParams& params, VersioningState* out_versioning_state);

  friend class KvRgwGrpcService;
  friend class BatchCommitQueue;
  friend int run_perf_driver(KvRgwServiceImpl&, KvStore&, const struct PerfConfig&);
  friend void put_worker(KvRgwServiceImpl&, uint32_t, const std::vector<std::string>&, uint64_t, int, int, std::atomic<bool>&, struct PutWorkerResult&, struct BenchResult&);
  friend void put_pad_worker(KvRgwServiceImpl&, uint32_t, const std::vector<std::string>&, uint64_t, int, uint64_t, struct PutWorkerResult&);
  friend void delete_multi_direct_worker(KvRgwServiceImpl&, tenant_id_t, const std::string&, int, int64_t, int64_t, struct BenchResult&);
  friend void copy_worker(KvRgwServiceImpl&, tenant_id_t, const struct ThreadRange&, struct BenchResult&);
  friend void put_overwrite_worker(KvRgwServiceImpl&, uint32_t, const struct ThreadRange&, uint64_t, struct BenchResult&);
  friend void put_overwrite_versioned_worker(KvRgwServiceImpl&, uint32_t, const struct ThreadRange&, uint64_t, int, struct BenchResult&);
  friend void delete_version_worker(KvRgwServiceImpl&, tenant_id_t, const struct ThreadRange&, uint32_t, int, struct BenchResult&);

 private:
  static constexpr auto kBucketCacheTtl = std::chrono::seconds(3);

  struct BucketCacheEntry {
    bucket_id_t bucket_id{};
    uint8_t access_flags{};
    VersioningState versioning_state{};
    std::chrono::steady_clock::time_point cached_at;
  };

  struct BucketCacheKey {
    tenant_id_t tenant_id{};
    std::string bucket_name;

    bool operator==(const BucketCacheKey& other) const {
      return tenant_id == other.tenant_id && bucket_name == other.bucket_name;
    }
  };

  struct BucketCacheKeyHash {
    size_t operator()(const BucketCacheKey& key) const {
      return std::hash<std::string>{}(key.bucket_name) ^ (static_cast<size_t>(key.tenant_id) << 1);
    }
  };


  struct BucketState {
    bucket_id_t bucket_id{};
    uint8_t access_flags{};
    VersioningState versioning_state{};
    int64_t created_at_unix{};
  };

  struct PutContext {
    FdbFuture f_bkt;
    FdbFuture f_obj;
    FdbFuture f_po;
    bool has_bucket_future{false};
    bool is_storage_tier{false};
    KeyBuf object_key;
  };

  struct VerifiedBucket {
    tenant_id_t tenant_id{};
    const std::string* bucket_name{nullptr};
    BucketState state;
  };

  PutContext put_prepare(KvTransaction& tr, PutInTxnParams& params, bool need_bucket);
  KvrgwErrorCode put_finalize(KvTransaction& tr, PutContext& ctx, PutInTxnParams& params,
                              VerifiedBucket* verified, int& verified_count,
                              VersioningState* out_versioning_state);

  std::expected<std::optional<BucketState>, fdb_error_t>
  read_bucket_state(tenant_id_t tenant_id, const std::string& bucket_name);

  std::expected<std::optional<BucketState>, fdb_error_t>
  get_bucket_id_cached(tenant_id_t tenant_id, const std::string& bucket_name);

  KvrgwErrorCode check_access(tenant_id_t tenant_id, const std::string& bucket_name,
                              const BucketState& cached, uint8_t deny_mask);

  void put_bucket_cache(tenant_id_t tenant_id, const std::string& bucket_name, bucket_id_t bucket_id, uint8_t access_flags = 0);
  void invalidate_bucket_cache(tenant_id_t tenant_id, const std::string& bucket_name);

  std::expected<std::optional<tenant_id_t>, fdb_error_t>
  resolve_tenant_id(std::string_view tenant_name);

  void put_tenant_cache(std::string_view tenant_name, tenant_id_t tenant_id);
  void invalidate_tenant_cache(const std::string& tenant_name);
  KvrgwErrorCode tenant_id_for_name(const std::string& tenant_name, tenant_id_t* tenant_id);

  KvrgwErrorCode resolve_bucket_error(tenant_id_t tenant_id,
                                      const std::string &bucket_name,
                                      KvrgwErrorCode tentative_err_code);

  struct LoadResult {
    ObjectValue value;
    std::string data;
  };

  std::expected<std::optional<ObjectValue>, fdb_error_t>
  load_object(bucket_id_t bucket_id, const std::string& object_name);

  std::expected<std::optional<LoadResult>, fdb_error_t>
  load_object_with_data(bucket_id_t bucket_id, const std::string& object_name);

  KvrgwErrorCode load_object_for_read(
      tenant_id_t tenant_id,
      const std::string& bucket_name,
      std::string_view key,
      std::optional<version_id_t> version_id,
      bool load_kv_data,
      ObjectValue* value,
      std::string* kv_data,
      std::string* error_detail);

  bool move_object_to_g(KvTransaction& tr, std::string_view object_key, const ObjectValue& value);

  struct NewVersionIds {
    version_id_t version_id;
    version_id_t next_vid;
  };

  NewVersionIds compute_new_version(VersioningState versioning_state, const ObjectValue* old_o);

  void displace_old_object(
      KvTransaction& tr,
      VersioningState versioning_state,
      std::string_view object_key,
      const ObjectValue& old_o);

  struct DeleteContext {
    FdbFuture f_obj;
    FdbFuture f_bkt;
    std::string object_key;
    bucket_id_t bucket_id{};
    tenant_id_t tenant_id{};
    std::string bucket_name;
  };

  std::expected<DeleteContext, KvrgwErrorCode> delete_prepare(
      KvTransaction& tr,
      tenant_id_t tenant_id,
      const std::string& bucket_name,
      const std::string& object_name,
      bool need_bucket,
      bucket_id_t bucket_id = kNullBucket);

  std::expected<BucketState, KvrgwErrorCode>
  delete_verify_bucket(DeleteContext& ctx);

  std::expected<DeleteResult, KvrgwErrorCode>
  delete_apply(
      KvTransaction& tr,
      DeleteContext& ctx,
      const BucketState& bucket_state,
      const DeleteCondition* cond);

  std::expected<DeleteResult, KvrgwErrorCode>
  delete_single(
      KvTransaction& tr,
      tenant_id_t tenant_id,
      const std::string& bucket_name,
      const std::string& object_name,
      const DeleteCondition* cond);

  bool delete_multi_try_commit(
      tenant_id_t tenant_id,
      const std::string& bucket_name,
      bucket_id_t bucket_id,
      const std::vector<std::string>& keys,
      std::vector<DeleteMultiKeyOutcome>& outcomes);
  void delete_multi_one_key(
      tenant_id_t tenant_id,
      const std::string& bucket_name,
      bucket_id_t bucket_id,
      const std::string& key,
      std::vector<DeleteMultiKeyOutcome>& outcomes);

  KvrgwErrorCode put_object_phase3(
      tenant_id_t tenant_id,
      const std::string& bucket_name,
      bucket_id_t bucket_id,
      const std::string& object_name,
      const RefTag& ref_tag,
      ObjectValue& new_value,
      VersioningState* out_versioning_state = nullptr,
      std::span<const uint8_t> tags = {},
      std::span<const uint8_t> metadata = {});

  KvrgwErrorCode put_object_single_txn(
      tenant_id_t tenant_id,
      const std::string& bucket_name,
      const std::string& object_name,
      const RefTag& ref_tag,
      ObjectValue& new_value,
      const std::string& data,
      VersioningState* out_versioning_state = nullptr,
      const PutCondition* cond = nullptr,
      std::span<const uint8_t> tags = {},
      std::span<const uint8_t> metadata = {});

  KvrgwErrorCode select_storage_tier(
      tenant_id_t tenant_id,
      const std::string& bucket_name,
      const std::string& object_name,
      const RefTag& ref_tag,
      ObjectValue& object_value,
      const std::string& data,
      uint64_t estimated_size,
      VersioningState* out_versioning_state = nullptr,
      const PutCondition* cond = nullptr,
      std::span<const uint8_t> tags = {},
      std::span<const uint8_t> metadata = {});

  std::expected<BucketState, KvrgwErrorCode>
  verify_bucket_in_txn(
      KvTransaction& tr,
      tenant_id_t tenant_id,
      const std::string& bucket_name,
      bucket_id_t expected_bucket_id,
      uint8_t deny_mask = 0);

  // Pipelined: issue the B: get without blocking, resolve later.
  FdbGetHolder issue_bucket_get(
      KvTransaction& tr,
      tenant_id_t tenant_id,
      const std::string& bucket_name);

  std::expected<BucketState, KvrgwErrorCode>
  resolve_bucket_verify(
      tenant_id_t tenant_id,
      const std::string& bucket_name,
      FdbGetHolder& holder,
      bucket_id_t expected_bucket_id,
      uint8_t deny_mask = 0);

  KvStore& store_;
  DataStore& data_store_;
  RefTagGenerator& ref_tags_;
  TierConfigState& tier_config_state_;
  Sweeper& sweeper_;
  LatencyStats latency_stats_;
  OpsStats ops_stats_;
  std::unique_ptr<ErrorStats> error_stats_;
  ErrInsertion err_insertion_;
  bool listing_disable_ryw_{true};
  BatchCommitQueue batch_queue_;
  std::mutex cache_mu_;
  std::unordered_map<std::string, tenant_id_t> tenant_cache_;
  std::unordered_map<BucketCacheKey, BucketCacheEntry, BucketCacheKeyHash> bucket_cache_;
};

}  // namespace kvrgw
