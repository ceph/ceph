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
#include "multipart_value.hpp"
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
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <shared_mutex>
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
  bucket_id_t bucket_id{};
  std::string bucket_name;
  std::string object_name;
  ObjectValue value;
  std::string data;
  ChunkType chunk_type{CHUNK_INLINE};
  PutCondition cond{};
  bool has_cond{false};
  std::vector<uint8_t> tag_encoded;
  std::array<uint8_t, MAX_META_FRAME_BYTES> metadata_encoded{};
  size_t metadata_encoded_size{0};
  std::chrono::steady_clock::time_point enqueued_at;
  std::promise<BatchPutResult> promise;
};

struct BatchStats {
  // attempted_* — recorded in start_batch (before Phase 1 commit)
  std::atomic<int64_t> attempted_batch_commits{0};
  std::atomic<int64_t> attempted_total_entries_batch{0};
  std::atomic<int64_t> attempted_single_entry_batches{0};
  std::atomic<int64_t> attempted_min_batch_size{0};
  std::atomic<int64_t> attempted_max_batch_size{0};
  std::atomic<int64_t> attempted_total_wait_us{0};
  std::atomic<int64_t> attempted_min_wait_us{0};
  std::atomic<int64_t> attempted_max_wait_us{0};
  std::atomic<int64_t> attempted_inline_only_batch_commits{0};
  std::atomic<int64_t> attempted_inline_only_batch_entries{0};

  // committed_* — recorded on successful Phase 3 commit
  std::atomic<int64_t> committed_batch_commits{0};
  std::atomic<int64_t> committed_total_entries{0};
  std::atomic<int64_t> committed_single_entry_batches{0};
  std::atomic<int64_t> committed_inline_only_batch_commits{0};
  std::atomic<int64_t> committed_inline_only_batch_entries{0};

  // misc
  std::atomic<int64_t> conflict_pushbacks{0};
  std::atomic<int64_t> total_queue_size_at_extract{0};
  std::atomic<int64_t> fallback_to_single_count{0};
  std::atomic<int64_t> fallback_to_single_entries{0};
  std::atomic<int64_t> single_txn_puts{0};

  void reset() {
    attempted_batch_commits.store(0);
    attempted_total_entries_batch.store(0);
    attempted_single_entry_batches.store(0);
    attempted_min_batch_size.store(0); attempted_max_batch_size.store(0);
    attempted_total_wait_us.store(0);
    attempted_min_wait_us.store(0); attempted_max_wait_us.store(0);
    attempted_inline_only_batch_commits.store(0);
    attempted_inline_only_batch_entries.store(0);
    committed_batch_commits.store(0);
    committed_total_entries.store(0);
    committed_single_entry_batches.store(0);
    committed_inline_only_batch_commits.store(0);
    committed_inline_only_batch_entries.store(0);
    conflict_pushbacks.store(0);
    total_queue_size_at_extract.store(0);
    fallback_to_single_count.store(0);
    fallback_to_single_entries.store(0);
    single_txn_puts.store(0);
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
    enum Phase : uint8_t { PHASE_EMPTY = 0, PHASE_1 = 1, PHASE_2 = 2, PHASE_3 = 3, PHASE_FALLBACK = 4 };
    enum Step : uint8_t { STEP_WORKING = 0, STEP_COMMIT_ISSUED = 1, STEP_IO_ISSUED = 2 };

    // Worst-case split: 20 storage entries / 2 per Group P:O = 10 keys.
    static constexpr int kMaxPhase1Keys = kMaxBatchSize / 2; // = 10

    uint8_t phase{PHASE_EMPTY};
    uint8_t step{STEP_WORKING};
    uint8_t entry_count{0};
    uint8_t phase1_key_count{0};
    uint8_t p3_attempt{0};
    uint8_t start_attempt{0};

    std::array<KeyBuf, kMaxPhase1Keys> phase1_keys{};
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
        entries[i].cond = PutCondition{};
        entries[i].has_cond = false;
        entries[i].tag_encoded.clear();
        entries[i].metadata_encoded_size = 0;
      }
      entry_count = 0;
      phase1_key_count = 0;
      p3_attempt = 0;
      start_attempt = 0;
      txn.reset();
      commit_future = FdbFuture();
    }
  };

  struct WorkerState {
    static constexpr int kMaxInflight = 4;
    InFlightBatch slots[kMaxInflight];
  };

  void run();
  void start_batch(InFlightBatch& ib);
  void fallback_to_single(InFlightBatch& ib);
  void record_batch_stats(InFlightBatch& ib);
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

  KvrgwErrorCode create_bucket(tenant_id_t tenant_id, std::string_view bucket_name);
  // Resolve bucket_name → bucket_id. Returns nullopt if the bucket doesn't
  // exist. Intended for tests that need bucket_id to inspect GC KV entries.
  std::expected<std::optional<bucket_id_t>, KvrgwErrorCode>
  resolve_bucket_id(tenant_id_t tenant_id, std::string_view bucket_name);
  KvrgwErrorCode delete_bucket(tenant_id_t tenant_id, std::string_view bucket_name);
  KvrgwErrorCode add_tenant(std::string_view tenant_name, tenant_id_t* out_id);
  KvrgwErrorCode resolve_tenant(std::string_view tenant_name, bool* out_exists, tenant_id_t* out_id);
  KvrgwErrorCode bucket_exists(tenant_id_t tenant_id, std::string_view bucket_name,
                               bool* out_exists);
  KvrgwErrorCode bucket_exists_cached(tenant_id_t tenant_id, std::string_view bucket_name,
                               bool* out_exists);
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

  struct AWSDeleteCondition {
    std::string if_match;
    int64_t if_match_last_modified_time = 0;
    int64_t if_match_size = 0;
    bool has_if_match_size = false;
  };

  KvrgwErrorCode delete_object_version(tenant_id_t tenant_id,
                                       std::string_view bucket_name,
                                       std::string_view key,
                                       version_id_t version_id,
                                       const AWSDeleteCondition* cond);

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

    DeleteMultiKeyOutcome() = default;
    DeleteMultiKeyOutcome(Status s, std::string k, std::string ec,
                          bool dm = false, version_id_t dm_vid = {})
        : key(std::move(k)), status(s), error_code(std::move(ec)),
          created_dm(dm), dm_version_id(dm_vid) {}
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

  // ---- Multipart upload

  struct InitiateMultipartResult {
    KvrgwErrorCode error_code{KVRGW_ERR_OK};
    std::string upload_id;  // opaque — hex-encoded 12-byte ref_tag
  };

  std::string_view make_part_filename(const RefTag& ref_tag,
                                      uint16_t part_number,
                                      uint32_t generation,
                                      std::span<char> out);

  InitiateMultipartResult initiate_multipart_upload(
      tenant_id_t tenant_id, std::string_view bucket_name,
      std::string_view object_name, std::string_view content_type,
      std::span<const TagPair> tags, std::span<const MetaPair> metadata);

  struct UploadPartResult {
    KvrgwErrorCode error_code{KVRGW_ERR_OK};
    std::string etag;
  };

  UploadPartResult upload_part(
      tenant_id_t tenant_id, std::string_view bucket_name,
      std::string_view object_name, std::string_view upload_id,
      uint16_t part_number, const uint8_t* data, size_t data_len);

  struct PartListEntry {
    uint16_t part_number{};
    std::string etag;
    uint64_t size{};
    int64_t last_modified_unix{};
  };

  struct ListPartsResult {
    std::vector<PartListEntry> parts;
    bool is_truncated{false};
    uint16_t next_part_number_marker{0};
  };

  KvrgwErrorCode list_parts(
      tenant_id_t tenant_id, std::string_view bucket_name,
      std::string_view object_name, std::string_view upload_id,
      uint16_t part_number_marker, uint32_t max_parts, ListPartsResult* out);

  struct MultipartUploadListEntry {
    std::string object_name;
    std::string upload_id;
    int64_t initiated_unix{};
  };

  struct ListMultipartUploadsResult {
    std::vector<MultipartUploadListEntry> uploads;
    bool is_truncated{false};
    std::string next_key_marker;
    std::string next_upload_id_marker;
  };

  KvrgwErrorCode list_multipart_uploads(
      tenant_id_t tenant_id, std::string_view bucket_name,
      std::string_view prefix, std::string_view key_marker,
      std::string_view upload_id_marker, uint32_t max_uploads,
      ListMultipartUploadsResult* out);

  struct CompletedPart {
    uint16_t part_number{};
    std::string etag;  // client-supplied part etag (hex, no dash)
  };

  struct CompleteMultipartResult {
    KvrgwErrorCode error_code{KVRGW_ERR_OK};
    std::string etag;  // composite etag ("32hex-N")
    version_id_t version_id{};
  };

  CompleteMultipartResult complete_multipart_upload(
      tenant_id_t tenant_id, std::string_view bucket_name,
      std::string_view object_name, std::string_view upload_id,
      std::span<const CompletedPart> client_parts,
      std::string_view if_match = {}, std::string_view if_none_match = {});

  KvrgwErrorCode abort_multipart_upload(
      tenant_id_t tenant_id, std::string_view bucket_name,
      std::string_view object_name, std::string_view upload_id);

  struct GetObjectResult {
    ObjectValue value;
    std::string body;
    std::string error_detail;
  };

  // part_number: 0 means "no ?partNumber= requested" (normal whole-object/
  // Range read). A non-zero value is validated against the object's
  // ObjectValueHeader::part_count (uniformly [1, part_count] for every
  // chunk type -- see object_value.hpp) and, for CHUNK_MULTIPART, resolved
  // via the manifest to read exactly that one part's bytes (whole part,
  // no range slicing), overriding GetObjectResult::value.hdr.size to the
  // part's own size. Mutually exclusive with `range` -- a non-zero
  // part_number takes precedence and `range` is ignored.
  KvrgwErrorCode get_object(tenant_id_t tenant_id, std::string_view bucket_name,
                            std::string_view key, std::optional<version_id_t> version_id,
                            const ByteRange* range, uint16_t part_number, GetObjectResult* out);

  // part_number: same contract as get_object's, minus any body read --
  // only overrides out->hdr.size to the resolved part's size.
  KvrgwErrorCode head_object(tenant_id_t tenant_id, std::string_view bucket_name,
                             std::string_view key, std::optional<version_id_t> version_id,
                             uint16_t part_number, ObjectValue* out, std::string* error_detail);

  // GetObjectAttributes -- ETag/size/version_id live on `value` (same
  // convention as head_object's own out-param). ObjectParts is reported
  // only when has_parts is true (CHUNK_MULTIPART/_REF); every other chunk
  // type leaves it unset entirely -- real AWS omits ObjectParts for
  // non-multipart objects, no synthetic "1 part" like PartNumber reads
  // use elsewhere. part_numbers/part_sizes carry one page (bounded by
  // max_parts, starting just after part_number_marker) of the manifest's
  // already-sorted included_parts/sizes -- same shape as list_parts'
  // pagination, just over the completed manifest instead of S:U rows.
  struct GetObjectAttributesResult {
    ObjectValue value;
    bool has_parts = false;
    uint32_t total_parts_count = 0;
    std::vector<uint16_t> part_numbers;
    std::vector<uint64_t> part_sizes;
    bool parts_truncated = false;
    uint16_t next_part_number_marker = 0;
    std::string error_detail;
  };

  KvrgwErrorCode get_object_attributes(
      tenant_id_t tenant_id, std::string_view bucket_name, std::string_view key,
      std::optional<version_id_t> version_id, uint16_t max_parts,
      uint16_t part_number_marker, GetObjectAttributesResult* out);

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
                               std::string_view key, const AWSDeleteCondition* cond,
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

  struct AWSPutCondition {
    std::string if_match;
    std::string if_none_match;
  };

  struct PutObjectRequest {
    tenant_id_t tenant_id;
    std::string bucket_name;
    std::string object_name;
    ObjectValue value;
    uint64_t estimated_size;
    const PutCondition* cond;
    std::span<const uint8_t> tags;
    std::span<const uint8_t> metadata;
  };

  struct PutObjectResult {
    KvrgwErrorCode error_code{KVRGW_ERR_OK};
    std::string etag;
    version_id_t version_id{};
  };

  PutObjectResult put_object_route(PutObjectRequest& req, const uint8_t* data, size_t data_len);

  PutObjectResult put_object(
      tenant_id_t tenant_id,
      std::string_view bucket_name,
      std::string_view object_name,
      const uint8_t* data, size_t data_len,
      std::string_view content_type,
      uint64_t estimated_size,
      const AWSPutCondition* cond,
      std::span<const TagPair> tags,
      std::span<const MetaPair> metadata);

  struct PutInTxnParams {
    tenant_id_t tenant_id;
    const std::string& bucket_name;
    bucket_id_t bucket_id;
    const std::string& object_name;
    ObjectValue& value;
    const std::string* data;
    const PutCondition* cond;
    std::span<const uint8_t> tags;
    bool skip_bucket_verify;
    bool is_storage_tier;
    std::span<const uint8_t> metadata{};
  };

  KvrgwErrorCode put_object_in_txn(KvTransaction& tr, PutInTxnParams& params, VersioningState* out_versioning_state);

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

  // Non-owning probe key for heterogeneous (allocation-free) unordered_map
  // lookups against bucket_cache_. Never stored -- only used transiently as
  // the argument to find()/erase().
  struct BucketCacheKeyView {
    tenant_id_t tenant_id{};
    std::string_view bucket_name;
  };

  struct BucketCacheKey {
    tenant_id_t tenant_id{};
    std::string bucket_name;

    // Hidden friends: appear in every comparison pairing this map performs
    // (Key==Key for insert's duplicate check, Key==View for heterogeneous
    // find()), so ADL finds all three regardless of which side is which.
    friend bool operator==(const BucketCacheKey& a, const BucketCacheKey& b) {
      return a.tenant_id == b.tenant_id && a.bucket_name == b.bucket_name;
    }
    friend bool operator==(const BucketCacheKey& a, const BucketCacheKeyView& b) {
      return a.tenant_id == b.tenant_id && a.bucket_name == b.bucket_name;
    }
    friend bool operator==(const BucketCacheKeyView& a, const BucketCacheKey& b) {
      return a.tenant_id == b.tenant_id && a.bucket_name == b.bucket_name;
    }
  };

  struct BucketCacheKeyHash {
    using is_transparent = void;  // opt-in to heterogeneous lookup
    size_t operator()(const BucketCacheKey& k) const {
      return std::hash<std::string_view>{}(k.bucket_name) ^ (static_cast<size_t>(k.tenant_id) << 1);
    }
    size_t operator()(const BucketCacheKeyView& k) const {
      return std::hash<std::string_view>{}(k.bucket_name) ^ (static_cast<size_t>(k.tenant_id) << 1);
    }
  };

  // Transparent hash for plain-std::string-keyed maps (e.g. tenant_cache_)
  // so find()/erase() can probe with a std::string_view, no allocation.
  struct TransparentStringHash {
    using is_transparent = void;
    size_t operator()(std::string_view s) const {
      return std::hash<std::string_view>{}(s);
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
    KeyBuf po_key;
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
  read_bucket_state(tenant_id_t tenant_id, std::string_view bucket_name);

  std::expected<std::optional<BucketState>, fdb_error_t>
  get_bucket_id_cached(tenant_id_t tenant_id, std::string_view bucket_name);

  KvrgwErrorCode check_access(tenant_id_t tenant_id, std::string_view bucket_name,
                              const BucketState& cached, uint8_t deny_mask);

  void put_bucket_cache(tenant_id_t tenant_id, std::string_view bucket_name, bucket_id_t bucket_id,
                        uint8_t access_flags = 0, VersioningState versioning_state = VERSIONING_DISABLED);
  void invalidate_bucket_cache(tenant_id_t tenant_id, std::string_view bucket_name);

  // Thread-local, lock-free front door for the opportunistic bucket-cache
  // refresh in resolve_bucket_verify().
  // Checks a small per-thread table to see whether this thread had
  // updated this bucket stgate within the last kBucketCacheTtl/2;
  // If not (stale or never seen by this thread), unconditionally calls
  // put_bucket_cache() (writer-lock) using data the caller already has on hand
  // and updates the local table.
  void bucket_cache_fast_refresh(tenant_id_t tenant_id, std::string_view bucket_name,
                                 bucket_id_t bucket_id, uint8_t access_flags,
                                 VersioningState versioning_state);

  std::expected<std::optional<tenant_id_t>, fdb_error_t>
  resolve_tenant_id(std::string_view tenant_name);

  void put_tenant_cache(std::string_view tenant_name, tenant_id_t tenant_id);
  void invalidate_tenant_cache(std::string_view tenant_name);

  KvrgwErrorCode resolve_bucket_error(tenant_id_t tenant_id,
                                      std::string_view bucket_name,
                                      KvrgwErrorCode tentative_err_code);

  struct LoadResult {
    ObjectValue value;
    std::string data;
  };

  std::expected<std::optional<ObjectValue>, fdb_error_t>
  load_object(bucket_id_t bucket_id, std::string_view object_name);

  std::expected<std::optional<LoadResult>, fdb_error_t>
  load_object_with_data(bucket_id_t bucket_id, std::string_view object_name);

  KvrgwErrorCode load_object_for_read(
      tenant_id_t tenant_id,
      std::string_view bucket_name,
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
  NewVersionIds compute_new_version(VersioningState versioning_state, const ObjectValueHeader* old_hdr);

  void displace_old_object(
      KvTransaction& tr,
      VersioningState versioning_state,
      std::string_view object_key,
      const ObjectValue& old_o);

  // Zero-copy overload: operates directly on the wire-format raw value.
  void displace_old_object(
      KvTransaction& tr,
      VersioningState versioning_state,
      std::string_view object_key,
      std::string_view raw_value);

  bool move_object_to_g(KvTransaction& tr, std::string_view object_key,
                        std::string_view raw_value);

  // Shared helper: displace the old object (if any) per versioning_state, then
  // compute the new object's (version_id, next_vid).
  NewVersionIds displace_and_version(
      KvTransaction& tr, VersioningState vs, std::string_view object_key,
      const ObjectValueHeader* old_hdr, std::string_view old_raw);

  // Pure, no writes. Evaluates If-Match / If-None-Match against the existing
  // object header (nullptr = absent). Returns KVRGW_ERR_OK, KVRGW_ERR_NO_SUCH_KEY
  // or KVRGW_ERR_PRECONDITION_FAILED. A null `cond` always passes.
  static KvrgwErrorCode check_put_condition(const PutCondition* cond,
                                            const ObjectValueHeader* old_hdr);

  // Shared "check condition -> displace -> version -> stamp" sequence used by
  // every tier of put_finalize and by complete_multipart_upload_step3.
  // `existing_raw` is the raw current :O: value (nullopt = absent). Nothing is
  // written on `tr` unless the result is a success. The caller writes the new
  // value itself (see write_o_value).
  std::expected<NewVersionIds, KvrgwErrorCode> install_object_value(
      KvTransaction& tr, VersioningState vs, std::string_view object_key,
      std::string_view old_raw, const PutCondition* cond,
      ObjectValue& new_value);

  // Serializes `value` (OValueBufMultipart for CHUNK_MULTIPART[_REF], OValueBuf
  // otherwise) and kv_put()s it at `object_key`. Returns false if the value
  // does not fit; on success optionally reports the written size.
  static bool write_o_value(KvTransaction& tr, std::string_view object_key,
                            const ObjectValue& value,
                            size_t* out_size = nullptr);

  // ---- Multipart upload private helpers (service_impl_multipart.cpp) ----

  // Read-only whole-upload manifest scan — see plans/fixing_agent_mess.md
  // Sections 6/8/10/11. Shared by AbortMultipartUpload (client_parts empty
  // -- only has_pending is meaningful) and CompleteMultipartUpload
  // (client_parts pre-validated and sorted by part_number -- builds the
  // full manifest/keep-list). Performs no KV writes at all -- Complete/
  // Abort's own TXN3 does the actual teardown writes based on this result.
  struct ManifestScanResult {
    bool abort{false};                          // Complete only: InvalidPart/EntityTooSmall
    KvrgwErrorCode abort_error{KVRGW_ERR_OK};    // valid only if abort
    bool has_pending{false};                    // any PENDING row anywhere in the scan
    std::vector<ETag> included;  // one per matched part, ascending part_number
                                 // order (parallels client_parts -- the
                                 // part_number itself lives there already)
    uint64_t total_size{0};
    // Generation chosen for each matched part, in the same ascending
    // part_number order as `included` / client_parts -- the GC keep-list
    // handed to G:M{MIXED}/P:M. A part can have more than one COMMITTED
    // generation; this is the one whose ETag the client sent (highest
    // generation wins on an ETag tie -- Section 8).
    std::vector<uint32_t> keep_list;
    // Size of the chosen generation for each matched part, same order as
    // `included`/`keep_list` -- Knob 3 (size) input for the keep-list's
    // compact encoding (plans/multipart_manifest.md). GC/Sweeper never
    // read this back (size is irrelevant to the keep/free decision); it
    // still gets encoded for real so the codec stays exercised uniformly
    // ahead of Phase 8's manifest, which will need it for real.
    std::vector<uint64_t> sizes;
  };

  KvrgwErrorCode complete_multipart_upload_step2(std::string_view head_key,
                                                 bucket_id_t bucket_id,
                                                 std::string_view rt_view,
                                                 std::span<const CompletedPart> client_parts,
                                                 ManifestScanResult *scan_result /*OUT*/);

  struct Step3Result {
    KvrgwErrorCode error_code{KVRGW_ERR_OK};
    std::string etag;
    version_id_t version_id{};
    bucket_id_t bucket_id{};  // may be updated on BUCKET_ID_MISMATCH retry
  };

  KvrgwErrorCode complete_multipart_upload_step3(tenant_id_t tenant_id,
                                                 std::string_view bucket_name,
                                                 std::string_view object_name,
                                                 std::string_view head_key,
                                                 std::string_view rt_view,
                                                 const RefTag& ref_tag,
                                                 const ETag& composite_etag,
                                                 std::span<const CompletedPart> client_parts,
                                                 const ManifestScanResult& scan_result,
                                                 const PutCondition *cond,
                                                 Step3Result *out /*OUT*/);

  std::expected<ManifestScanResult, KvrgwErrorCode> scan_upload_rows(
      KvTransaction& tr, bucket_id_t bucket_id, std::string_view ref_tag,
      std::span<const CompletedPart> client_parts);

  // Batch-level state shared across all keys in a single-bucket delete operation.
  // bucket_name is a reference — caller must ensure the string outlives the batch.
  struct DeleteBatchCtx {
    FdbFuture f_bkt;
    bucket_id_t bucket_id{};
    tenant_id_t tenant_id{};
    const std::string* bucket_name{nullptr};
  };

  // Per-key state for a single object within a delete batch.
  struct DeleteContext {
    FdbFuture f_obj;
    KeyBuf object_key;
    std::optional<std::string> resolved{};  // populated by delete_multi_try_commit before delete_apply
  };

  std::expected<DeleteBatchCtx, KvrgwErrorCode> delete_prepare_batch(
      KvTransaction& tr,
      tenant_id_t tenant_id,
      const std::string& bucket_name,
      bucket_id_t bucket_id);

  void delete_prepare_key(
      KvTransaction& tr,
      bucket_id_t bucket_id,
      std::string_view object_name,
      DeleteContext& ctx);

  std::expected<BucketState, KvrgwErrorCode>
  delete_verify_bucket(DeleteBatchCtx& batch);

  std::expected<DeleteResult, KvrgwErrorCode>
  delete_apply(
      KvTransaction& tr,
      DeleteContext& ctx,
      const BucketState& bucket_state,
      const GetCondition* cond);

  std::expected<DeleteResult, KvrgwErrorCode>
  delete_single(
      KvTransaction& tr,
      tenant_id_t tenant_id,
      const std::string& bucket_name,
      std::string_view object_name,
      const GetCondition* cond);

  // Extracts a GcDescriptor from a raw object if it needs deferred GC.
  // Returns true and populates `out` for STORAGE/STORAGE_REF/coalescing objects.
  // Returns false for INLINE, CHILD_D, etc. — no transaction side effects.
  bool pack_object_into_gc(std::string_view object_key,
                           std::string_view raw_value,
                           VersioningState versioning_state,
                           GcDescriptor& out) const;

  bool delete_multi_try_commit(
      tenant_id_t tenant_id,
      const std::string& bucket_name,
      bucket_id_t bucket_id,
      std::span<const std::string> keys,
      std::span<DeleteContext> ctxs,
      std::vector<DeleteMultiKeyOutcome>* out);
  void delete_multi_one_key(
      tenant_id_t tenant_id,
      const std::string& bucket_name,
      bucket_id_t bucket_id,
      std::string_view key,
      std::vector<DeleteMultiKeyOutcome>& outcomes);

  KvrgwErrorCode put_object_phase3(
      tenant_id_t tenant_id,
      const std::string& bucket_name,
      bucket_id_t bucket_id,
      const std::string& object_name,
      ObjectValue& new_value,
      VersioningState* out_versioning_state = nullptr,
      const PutCondition* cond = nullptr,
      std::span<const uint8_t> tags = {},
      std::span<const uint8_t> metadata = {});

  KvrgwErrorCode put_object_single_txn(
      tenant_id_t tenant_id,
      const std::string& bucket_name,
      const std::string& object_name,
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
      ObjectValue& object_value,
      const std::string& data,
      uint64_t estimated_size,
      VersioningState* out_versioning_state = nullptr,
      const PutCondition* cond = nullptr,
      std::span<const uint8_t> tags = {},
      std::span<const uint8_t> metadata = {});

  // Pipelined: issue the B: get without blocking, resolve later.
  FdbGetHolder issue_bucket_get(
      KvTransaction& tr,
      tenant_id_t tenant_id,
      std::string_view bucket_name);

  std::expected<BucketState, KvrgwErrorCode>
  resolve_bucket_verify(
      tenant_id_t tenant_id,
      std::string_view bucket_name,
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
  mutable std::shared_mutex cache_mu_;
  std::unordered_map<std::string, tenant_id_t, TransparentStringHash, std::equal_to<>> tenant_cache_;
  std::unordered_map<BucketCacheKey, BucketCacheEntry, BucketCacheKeyHash, std::equal_to<>> bucket_cache_;
};

}  // namespace kvrgw
