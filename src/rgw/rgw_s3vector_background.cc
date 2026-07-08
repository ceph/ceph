// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <memory>
#include <boost/functional/hash.hpp>
#include <boost/lockfree/queue.hpp>
#include <boost/asio/basic_waitable_timer.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/spawn.hpp>
#include <boost/context/protected_fixedsize_stack.hpp>
#include "common/ceph_time.h"
#include "common/dout.h"
#include "common/random_string.h"
#include <chrono>
#include <charconv>
#include <fmt/format.h>
#include "common/async/yield_waiter.h"
#include <future>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include "rgw_sal.h"
#include "rgw_common.h"
#include "rgw_acl.h"
#include "common/ceph_json.h"
#include "common/ceph_crypto.h"
#include "rgw_s3vector.h"
#include "lancedb.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw::s3vector {

// table metadata key for build coordination state
static constexpr const char* build_state_metadata_key = "s3v_index_state";

// lock object key prefix/suffix within the vector bucket
static constexpr const char* lock_key_prefix = ".s3v-lock-";
static constexpr const char* lock_key_suffix = ".lock";


struct build_state_t {
  bool build_in_progress = false;
  int64_t build_started_at = 0;
  int64_t build_lease_seconds = 600;
  std::string builder_id;

  void dump(ceph::Formatter *f) const {
    encode_json("build_in_progress", build_in_progress, f);
    encode_json("build_started_at", build_started_at, f);
    encode_json("build_lease_seconds", build_lease_seconds, f);
    encode_json("builder_id", builder_id, f);
  }

  void decode_json(JSONObj *obj) {
    JSONDecoder::decode_json("build_in_progress", build_in_progress, obj);
    JSONDecoder::decode_json("build_started_at", build_started_at, obj);
    JSONDecoder::decode_json("build_lease_seconds", build_lease_seconds, obj);
    JSONDecoder::decode_json("builder_id", builder_id, obj);
  }

  std::string to_json_str() const {
    JSONFormatter f;
    f.open_object_section("");
    dump(&f);
    f.close_section();
    std::ostringstream oss;
    f.flush(oss);
    return oss.str();
  }

  bool from_json_str(const char* str) {
    JSONParser parser;
    if (!parser.parse(str, strlen(str))) {
      return false;
    }
    decode_json(&parser);
    return true;
  }
};

class Manager : public DoutPrefixProvider {
public:
    //message_t -> pass in empty index name for session messages (can extend to per table sessions in the future if needed)
    using table_name_t = std::pair<std::string, std::string>; // pair of vector bucket name and index name
    struct message_t {
      enum class Op {
        UPDATE,
        REMOVE, 
        SESSION_CREATE, 
        SESSION_DELETE
      };
      message_t(const std::string& bucket_name, const std::string& index_name, Op _type) :
          table_name(bucket_name, index_name), type(_type) {}
      const table_name_t table_name;
      const Op type;
    };

private:
  // use mmap/mprotect to allocate 128k coroutine stacks
  auto make_stack_allocator() {
    // LanceDB's Rust/tokio runtime needs deep stacks for table open, index
    // stats, and index build operations
//note: without increasing the stack-size it may cause a crash.
    return boost::context::protected_fixedsize_stack{1024*1024};
  }
  using MessageQueue =  boost::lockfree::queue<message_t*, boost::lockfree::fixed_sized<true>>;
  using Executor = boost::asio::io_context::executor_type;
  bool shutdown = false;
  CephContext* const cct;
  boost::asio::io_context io_context;
  boost::asio::executor_work_guard<Executor> work_guard;
  std::vector<std::thread> workers;
  rgw::sal::Driver* const driver;
  struct LanceDBSessionDeleter {
    void operator()(LanceDBSession* session) const {
      if(session) {
        lancedb_session_free(session);
      }
    }
  };
  using SessionPtr = std::shared_ptr<LanceDBSession>;
  ceph::shared_mutex sessions_mutex = ceph::make_shared_mutex("s3vector::Manager::sessions_mutex"); 
  std::unordered_map<std::string, SessionPtr> sessions;
  std::unordered_map<table_name_t, ceph::coarse_real_time, boost::hash<table_name_t>> tables;
  std::unordered_set<table_name_t, boost::hash<table_name_t>> active_builds;
  MessageQueue messages;
  static constexpr auto idle_sleep = std::chrono::milliseconds(1000); // 1s

  CephContext *get_cct() const override { return cct; }
  unsigned get_subsys() const override { return dout_subsys; }
  std::ostream& gen_prefix(std::ostream& out) const override { return out << "s3vectors manager: "; }

  class tokens_waiter {
    size_t pending_tokens = 0;
    DoutPrefixProvider* const dpp;
    ceph::async::yield_waiter<void> waiter;

  public:
    class token{
      tokens_waiter* tw;
    public:
      token(const token& other) = delete;
      token(token&& other) : tw(other.tw) {
        other.tw = nullptr; // mark as moved
      }
      token& operator=(const token& other) = delete;
      token(tokens_waiter* _tw) : tw(_tw) {
        ++tw->pending_tokens;
      }

      ~token() {
        if (!tw) {
          return; // already moved
        }
        --tw->pending_tokens;
        if (tw->pending_tokens == 0 && tw->waiter) {
          tw->waiter.complete(boost::system::error_code{});
        }
      }
    };

    tokens_waiter(DoutPrefixProvider* _dpp) : dpp(_dpp) {}
    tokens_waiter(const tokens_waiter& other) = delete;
    tokens_waiter& operator=(const tokens_waiter& other) = delete;

    void async_wait(boost::asio::yield_context yield) {
      if (pending_tokens == 0) {
        return;
      }
      ldpp_dout(dpp, 20) << "INFO: tokens waiter is waiting on " <<
        pending_tokens << " tokens" << dendl;
      boost::system::error_code ec;
      waiter.async_wait(yield[ec]);
      ldpp_dout(dpp, 20) << "INFO: tokens waiter finished waiting for all tokens" << dendl;
    }
  };

  void async_sleep(boost::asio::yield_context yield, const std::chrono::milliseconds& duration) {
    using Clock = ceph::coarse_mono_clock;
    using Timer = boost::asio::basic_waitable_timer<Clock,
        boost::asio::wait_traits<Clock>, Executor>;
    Timer timer(io_context);
    timer.expires_after(duration);
    boost::system::error_code ec;
    timer.async_wait(yield[ec]);
    if (ec) {
      ldpp_dout(this, 1) << "ERROR: async_sleep failed with error: " << ec.message() << dendl;
    }
  }

  // ============================================================================
  // Build state management via LanceDB table metadata
  // ============================================================================

  int read_build_state(const LanceDBTable* table, build_state_t& state) {
    const char* key = build_state_metadata_key;
    char** keys_out = nullptr;
    char** values_out = nullptr;
    size_t count = 0;
    char* error_message = nullptr;

    if (const auto result = lancedb_table_get_metadata(
            table, &key, 1, &keys_out, &values_out, &count, &error_message);
        result != LANCEDB_SUCCESS) {
      ldpp_dout(this, 1) << "ERROR: failed to read build state from table metadata: "
          << (error_message ? error_message : "unknown") << dendl;
      lancedb_free_string(error_message);
      return -EIO;
    }

    if (count > 0 && values_out[0]) {
      state.from_json_str(values_out[0]);
    }
    lancedb_free_metadata(keys_out, values_out, count);
    return 0;
  }

  int write_build_state(const LanceDBTable* table, const build_state_t& state) {
    const std::string json_str = state.to_json_str();
    const char* key = build_state_metadata_key;
    const char* value = json_str.c_str();
    char* error_message = nullptr;

    if (const auto result = lancedb_table_set_metadata(
            table, &key, &value, 1, &error_message);
        result != LANCEDB_SUCCESS) {
      ldpp_dout(this, 1) << "ERROR: failed to write build state to table metadata: "
          << (error_message ? error_message : "unknown") << dendl;
      lancedb_free_string(error_message);
      return -EIO;
    }
    return 0;
  }

  // process all work items for tables and sessions
  void process_messages(boost::asio::yield_context yield) {
    ldpp_dout(this, 5) << "INFO: manager started. starting to process messages for background table and session operations" << dendl;
  }

  // ============================================================================
  // Index stats helper
  // ============================================================================

  struct index_stats_result {
    LanceDBIndexStats stats = {};
    bool ok = false;
  };

  index_stats_result get_vector_index_stats(const LanceDBTable* table) {
    index_stats_result result;
    char* error_message = nullptr;

    // query the vector index directly by its known name (data_idx).
    // LanceDB names indices as {column_name}_idx — our vector column is
    // always "data", so the vector index is always "data_idx".
    // this avoids picking up the scalar "key_idx" which reports misleading
    // unindexed counts (scalar BTree indices always show indexed=0).
    if (const auto err = lancedb_table_index_stats(
            table, vector_index_name, &result.stats, &error_message);
        err == LANCEDB_SUCCESS) {
      result.ok = true;
      return result;
    }

    if (error_message) {
      lancedb_free_string(error_message);
    }

    // vector index doesn't exist yet — all rows are unindexed
    result.stats.num_indexed_rows = 0;
    result.stats.num_unindexed_rows = lancedb_table_count_rows(table);
    result.stats.num_indices = 0;
    result.ok = true;
    return result;
  }

  // ============================================================================
  // Distributed lock via S3 conditional write (SAL)
  // ============================================================================

  static std::string make_lock_key(const std::string& index_name) {
    return std::string(lock_key_prefix) + index_name + lock_key_suffix;
  }

  std::string generate_lock_token() {
    //returns a random 32-character alphanumeric string as the lock token, to enable owenrship verification during release and prevent deleting another instance's lock.
    char buf[33];
    gen_rand_alphanumeric(cct, buf, sizeof(buf) - 1);
    buf[32] = '\0';
    return std::string(buf, 32);
  }

  struct lock_body_t {
    std::string token;
    int64_t timestamp = 0;

    void dump(ceph::Formatter *f) const {
      encode_json("token", token, f);
      encode_json("timestamp", timestamp, f);
    }

    void decode_json(JSONObj *obj) {
      JSONDecoder::decode_json("token", token, obj);
      JSONDecoder::decode_json("timestamp", timestamp, obj);
    }

    std::string to_json_str() const {
      JSONFormatter f;
      f.open_object_section("");
      dump(&f);
      f.close_section();
      std::ostringstream oss;
      f.flush(oss);
      return oss.str();
    }

    bool from_json_str(const std::string& str) {
      JSONParser parser;
      if (!parser.parse(str.c_str(), str.size())) {
        return false;
      }
      decode_json(&parser);
      return true;
    }
  };

  lock_body_t make_lock_body(const std::string& token) {
    lock_body_t body;
    body.token = token;
    body.timestamp = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    return body;
  }

  // load the vector bucket as a regular Bucket for lock object operations.
  // vector buckets have default-placement set at creation, so PUT/GET/DELETE
  // by exact key works (indexless only skips bucket index updates).
  int load_bucket_for_lock(const std::string& vector_bucket_name,
                           std::unique_ptr<rgw::sal::Bucket>& bucket,
                           optional_yield y) {
    rgw_bucket bucket_id;
    bucket_id.name = vector_bucket_name;
    std::unique_ptr<rgw::sal::VectorBucket> vbucket;
    int ret = driver->load_vector_bucket(this, bucket_id, &vbucket, y);
    if (ret < 0) {
      ldpp_dout(this, 1) << "ERROR: failed to load vector bucket for lock: "
          << vector_bucket_name << " ret=" << ret << dendl;
      return ret;
    }
    bucket = driver->get_bucket(vbucket->get_info());
    return 0;
  }

  // ---- low-level lock object operations ----

  // PUT lock object with if-none-match="*" (conditional create).
  // exactly one concurrent caller succeeds, others get -ERR_PRECONDITION_FAILED.
  int put_lock_object(const std::string& vector_bucket_name,
                      const std::string& lock_key,
                      const std::string& token,
                      optional_yield y) {
    std::unique_ptr<rgw::sal::Bucket> bucket;
    int ret = load_bucket_for_lock(vector_bucket_name, bucket, y);
    if (ret < 0) return ret;

    auto obj = bucket->get_object({lock_key});
    std::string req_id = driver->zone_unique_id(driver->get_new_req_id());
    ACLOwner owner;
    owner.id = bucket->get_owner();
    auto writer = driver->get_atomic_writer(this, y, obj.get(),
        owner, nullptr, 0, req_id);

    ret = writer->prepare(y);
    if (ret < 0) return ret;

    lock_body_t lock_body = make_lock_body(token);
    std::string body = lock_body.to_json_str();
    bufferlist bl;
    bl.append(body);
    const auto etag = TOPNSPC::crypto::digest<TOPNSPC::crypto::MD5>(bl).to_str();
    ret = writer->process(std::move(bl), 0);
    if (ret < 0) return ret;

    ret = writer->process({}, body.size());
    if (ret < 0) return ret;

    std::map<std::string, bufferlist> attrs;
    bufferlist etag_bl;
    etag_bl.append(etag.c_str(), etag.size());
    attrs[RGW_ATTR_ETAG] = std::move(etag_bl);

    const req_context rctx{this, y, nullptr};
    bool canceled = false;

    ret = writer->complete(body.size(), etag,
                           nullptr, ceph::real_clock::now(), attrs,
                           rgw::cksum::no_cksum, ceph::real_time(),
                           /*if_match=*/nullptr,
                           /*if_nomatch=*/"*",
                           nullptr, nullptr, &canceled,
                           rctx, 0);

    if (canceled) {
      return -ERR_PRECONDITION_FAILED;
    }
    return ret;
  }

  // DELETE lock object with conditional if_match (ETag).
  // if etag is non-empty, only deletes if the object's current ETag matches —
  // prevents deleting a lock that was reclaimed by another instance.
  // if etag is empty, performs unconditional delete.
  int delete_lock_object(const std::string& vector_bucket_name,
                         const std::string& lock_key,
                         const std::string& etag,
                         optional_yield y) {
    std::unique_ptr<rgw::sal::Bucket> bucket;
    int ret = load_bucket_for_lock(vector_bucket_name, bucket, y);
    if (ret < 0) return ret;

    auto obj = bucket->get_object({lock_key});
    auto del_op = obj->get_delete_op();
    if (!etag.empty()) {
      del_op->params.if_match = etag.c_str();
    } else {
      ldpp_dout(this, 0) << "CRITICAL: lock object " << lock_key
          << " has no ETag — conditional delete protection is disabled."
          << " This should not happen; the lock was likely created without"
          << " RGW_ATTR_ETAG. Proceeding with unconditional delete." << dendl;
    }
    return del_op->delete_obj(this, y, 0);
  }

  // GET lock object body and ETag.
  // returns {error_code, body, etag}. the ETag is used for conditional delete.
  struct lock_read_result {
    int ret = -1;
    std::string body;
    std::string etag;
  };

  lock_read_result get_lock_object(const std::string& vector_bucket_name,
                                   const std::string& lock_key,
                                   optional_yield y) {
    std::unique_ptr<rgw::sal::Bucket> bucket;
    int ret = load_bucket_for_lock(vector_bucket_name, bucket, y);
    if (ret < 0) return {ret, {}, {}};

    auto obj = bucket->get_object({lock_key});
    auto read_op = obj->get_read_op();
    ret = read_op->prepare(y, this);
    if (ret < 0) return {ret, {}, {}};

    bufferlist bl;
    const auto size = obj->get_size();
    if (size > 0) {
      ret = read_op->read(0, size - 1, bl, y, this);
      if (ret < 0) return {ret, {}, {}};
    }

    bufferlist etag_bl;
    ret = read_op->get_attr(this, RGW_ATTR_ETAG, etag_bl, y);
    std::string etag = (ret >= 0) ? etag_bl.to_str() : std::string{};

    return {0, bl.to_str(), std::move(etag)};
  }

  // ---- high-level lock protocol ----

  // Distributed lock acquisition protocol using S3 conditional operations.
  //
  // Step 1 — GET: read the existing lock object (body + ETag).
  //   - if no lock exists (ENOENT): proceed to step 3 (no lock to clear).
  //   - if lock exists and is fresh (age < TTL): return empty (build in progress).
  //   - if lock exists and is stale (age >= TTL): proceed to step 2.
  //
  // Step 2 — conditional DELETE (if_match=ETag from step 1):
  //   delete the stale lock only if its ETag still matches what we read.
  //   race condition: multiple instances may detect the same stale lock.
  //   - DELETE succeeds: we removed the stale lock. proceed to step 3.
  //   - ENOENT: another reclaimer already deleted it, but no new lock exists
  //     yet. proceed to step 3 to compete for the new lock.
  //   - PRECONDITION_FAILED: the lock was already reclaimed by another instance
  //     (D) which created a fresh lock with a different ETag. D holds the lock.
  //     return empty — no point attempting the PUT.
  //
  // Step 3 — conditional PUT (if_nomatch="*"):
  //   create the lock object with a fresh random token. RADOS exclusive-create
  //   guarantees exactly one concurrent caller succeeds.
  //   - if PUT succeeds: lock acquired, return the token.
  //   - if PUT fails (PRECONDITION_FAILED): another instance won, return empty.
  //
  // note: the lock TTL must exceed the maximum expected build duration.
  // if the build takes longer, the lock appears stale and another instance may
  // reclaim it. the release_lock token check prevents the original builder from
  // deleting the new holder's lock. a future improvement is to add a heartbeat
  // that refreshes the timestamp during long builds.
  std::string try_acquire_lock(const std::string& bucket_name,
                               const std::string& index_name,
                               optional_yield y) {
    const std::string lock_key = make_lock_key(index_name);

    // step 1: read existing lock
    auto lock_info = get_lock_object(bucket_name, lock_key, y);
    if (lock_info.ret == 0) {
      lock_body_t existing_lock;
      if (!existing_lock.from_json_str(lock_info.body)) {
        ldpp_dout(this, 1) << "ERROR: failed to parse lock body for " << bucket_name << "." << index_name << dendl;
        return {};
      }
      auto now = std::chrono::duration_cast<std::chrono::seconds>(
          std::chrono::system_clock::now().time_since_epoch()).count();
      int64_t lock_ttl = cct->_conf.get_val<uint64_t>("rgw_s3vector_index_lock_ttl_seconds");
      int64_t age = now - existing_lock.timestamp;

   //NOTE: (TODO) if the lock is stale, we could consider refreshing it (update timestamp), to protect against clock skew and transient delays that could cause false staleness detections.
      if (existing_lock.timestamp > 0 && age < lock_ttl) {
        ldpp_dout(this, 5) << "INFO: lock held for " << bucket_name << "." << index_name
            << " by " << existing_lock.token
            << " (age=" << age << "s)" << dendl;
        return {};
      }

      // step 2: stale lock — conditional delete using ETag from step 1
      ldpp_dout(this, 5) << "INFO: deleting stale lock for " << bucket_name
          << "." << index_name << " (age=" << age << "s, ttl=" << lock_ttl << "s)" << dendl;
      int del_ret = delete_lock_object(bucket_name, lock_key, lock_info.etag, y);
      if (del_ret == -ERR_PRECONDITION_FAILED) {
        ldpp_dout(this, 5) << "INFO: stale lock for " << bucket_name << "." << index_name
            << " was already reclaimed by another instance" << dendl;
        return {};
      }
    }

    // step 3: conditional create — only one caller wins
    // create-if-absent (atomic create)
    std::string token = generate_lock_token();
    int ret = put_lock_object(bucket_name, lock_key, token, y);
    if (ret == 0) {
      ldpp_dout(this, 5) << "INFO: acquired lock for " << bucket_name
          << "." << index_name << " token=" << token << dendl;
      return token;
    }

    if (ret == -ERR_PRECONDITION_FAILED) {
      ldpp_dout(this, 5) << "INFO: lock acquired by another instance for "
          << bucket_name << "." << index_name << dendl;
    } else {
      ldpp_dout(this, 1) << "ERROR: failed to acquire lock for " << bucket_name
          << "." << index_name << " ret=" << ret << dendl;
    }
    return {};
  }

  // Release the lock only if we still own it.
  //
  // Step 1 — GET: read the lock body and ETag.
  // Step 2 — token check: if the token doesn't match ours, another instance
  //   reclaimed the lock (e.g., our build exceeded the TTL). skip the delete.
  // Step 3 — conditional DELETE (if_match=ETag from step 1): delete only if the
  //   lock hasn't been replaced since we read it. this closes the TOCTOU window
  //   between the GET and DELETE — if another instance reclaimed the lock between
  //   our GET and DELETE, the ETag changed and the DELETE fails safely.
  //
  // note: in the normal case (build completes within TTL), no other instance
  // touches the lock, so the token check and conditional DELETE are redundant
  // safety. they matter only when the build exceeds TTL.
  void release_lock(const std::string& bucket_name,
                    const std::string& index_name,
                    const std::string& token,
                    optional_yield y) {
    const std::string lock_key = make_lock_key(index_name);

    // step 1: read lock
    auto lock_info = get_lock_object(bucket_name, lock_key, y);
    if (lock_info.ret < 0) return;

    // step 2: verify ownership
    lock_body_t existing_lock;
    if (!existing_lock.from_json_str(lock_info.body)) {
      ldpp_dout(this, 1) << "ERROR: failed to parse lock body for " << bucket_name << "." << index_name << dendl;
      return;
    }
    if (existing_lock.token != token) {
      ldpp_dout(this, 5) << "WARNING: lock for " << bucket_name << "." << index_name
          << " owned by another instance (ours=" << token
          << ", current=" << existing_lock.token << "), not releasing" << dendl;
      return;
    }

    // step 3: conditional delete using ETag from step 1
    // in the case some other instance reclaimed the lock between our GET and DELETE, 
    // the ETag changed and this delete fails safely without deleting the new holder's lock.
    int ret = delete_lock_object(bucket_name, lock_key, lock_info.etag, y);
    if (ret == -ERR_PRECONDITION_FAILED) {
      ldpp_dout(this, 5) << "INFO: lock for " << bucket_name << "." << index_name
          << " was reclaimed between read and delete, not releasing" << dendl;
    } else if (ret < 0 && ret != -ENOENT) {
      ldpp_dout(this, 1) << "ERROR: failed to release lock for " << bucket_name
          << "." << index_name << " ret=" << ret << dendl;
    }
  }

  // ============================================================================
  // Vector index build
  // ============================================================================

  static LanceDBIndexType string_to_index_type(const std::string& s) {
    if (s == "ivf_pq") return LANCEDB_INDEX_IVF_PQ;
    if (s == "ivf_hnsw_pq") return LANCEDB_INDEX_IVF_HNSW_PQ;
    if (s == "ivf_hnsw_sq") return LANCEDB_INDEX_IVF_HNSW_SQ;
    if (s == "ivf_flat") return LANCEDB_INDEX_IVF_FLAT;
    return LANCEDB_INDEX_AUTO;
  }

  int run_vector_index_build(LanceDBTable* table, LanceDBDistanceType distance_type) {
    LanceDBVectorIndexConfig vec_config = {};
    vec_config.num_partitions = -1;    // auto
    vec_config.num_sub_vectors = -1;   // auto
    vec_config.max_iterations = -1;    // default
    vec_config.sample_rate = 0.0f;     // default
    vec_config.distance_type = distance_type;
    vec_config.accelerator = nullptr;  // CPU
    vec_config.replace = 1;            // replace existing index

    const auto index_type_str = cct->_conf.get_val<std::string>("rgw_s3vector_index_type");
    const LanceDBIndexType index_type = string_to_index_type(index_type_str);

    const char* columns[] = {data_field};
    char* error_message = nullptr;

    const LanceDBError result = lancedb_table_create_vector_index(
        table, columns, 1, index_type, &vec_config, &error_message);

    if (result != LANCEDB_SUCCESS) {
      ldpp_dout(this, 0) << "ERROR: lancedb_table_create_vector_index failed"
          << " (error_code=" << result << "): "
          << (error_message ? error_message : "unknown") << dendl;
      lancedb_free_string(error_message);
      return -EIO;
    }
    return 0;
  }

  // ============================================================================
  // Core table processing: stats check → lock → build → cleanup
  // ============================================================================

  int process_table(const table_name_t& table_name, boost::asio::yield_context yield) {
    const auto& bucket_name = table_name.first;
    const auto& index_name = table_name.second;

    // step 1: check if this RGW is already building this table (cheapest check).
    // prevents the local background loop from seeing its own lock as stale
    // and reclaiming it when the build takes longer than the lock TTL.
    if (active_builds.count(table_name)) {
      ldpp_dout(this, 5) << "INFO: this RGW is already building "
          << bucket_name << "." << index_name << ", skipping" << dendl;
      return 0;
    }

    // step 2: read config
    const uint64_t threshold = cct->_conf.get_val<uint64_t>("rgw_s3vector_index_unindexed_threshold");
    if (threshold == 0) {
      ldpp_dout(this, 20) << "INFO: automatic index rebuild disabled for "
          << bucket_name << "." << index_name << dendl;
      return 0;
    }

    // step 3: open table
    LanceDBConnection* conn = s3vector::connect(this, bucket_name);
    if (!conn) {
      ldpp_dout(this, 5) << "WARNING: cannot connect to database for "
          << bucket_name << ", skipping" << dendl;
      return 0;
    }
    LanceDBTable* table = lancedb_connection_open_table(conn, index_name.c_str());
    if (!table) {
      ldpp_dout(this, 5) << "WARNING: cannot open table "
          << bucket_name << "." << index_name << ", may have been deleted" << dendl;
      lancedb_connection_free(conn);
      return 0;
    }

    // scope guard for cleanup
    struct table_guard_t {
      LanceDBTable* table;
      LanceDBConnection* conn;
      ~table_guard_t() {
        lancedb_table_free(table);
        lancedb_connection_free(conn);
      }
    } table_guard{table, conn};

    // step 4: get index stats
    auto stats_result = get_vector_index_stats(table);
    if (!stats_result.ok) {
      ldpp_dout(this, 1) << "ERROR: failed to get index stats for "
          << bucket_name << "." << index_name << dendl;
      return -EIO;
    }
    const auto& stats = stats_result.stats;

    ldpp_dout(this, 1) << "INFO: index stats for " << bucket_name << "." << index_name
        << ": indexed=" << stats.num_indexed_rows
        << " unindexed=" << stats.num_unindexed_rows
        << " num_indices=" << stats.num_indices << dendl;

    if (stats.num_unindexed_rows < threshold) {
      ldpp_dout(this, 1) << "INFO: " << bucket_name << "." << index_name
          << " below threshold (" << stats.num_unindexed_rows
          << " < " << threshold << "), skipping rebuild" << dendl;
      return 1; // below threshold — recheck on next notification
    }

    // step 5: try to acquire distributed lock (S3 conditional write)
    optional_yield y(yield);
    std::string lock_token = try_acquire_lock(bucket_name, index_name, y);
    if (lock_token.empty()) {
      ldpp_dout(this, 5) << "INFO: lock held by another process for "
          << bucket_name << "." << index_name << ", skipping rebuild" << dendl;
      return 0;
    }

    // track locally that this RGW is building this table
    active_builds.insert(table_name);

    // scope guard: release the distributed lock and remove from active builds
    struct lock_guard_t {
      Manager* mgr;
      const table_name_t& table_name;
      const std::string& bucket_name;
      const std::string& index_name;
      const std::string& token;
      optional_yield y;
      ~lock_guard_t() {
        mgr->active_builds.erase(table_name);
        mgr->release_lock(bucket_name, index_name, token, y);
      }
    } lock_guard{this, table_name, bucket_name, index_name, lock_token, y};

    // step 6: record build state in table metadata (observability, not a decision gate).
    // the distributed lock (step 5) controls mutual exclusion.
    // metadata records who is building and when, for diagnostics and monitoring.
    build_state_t state;
    state.build_in_progress = true;
    state.build_started_at = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    state.build_lease_seconds = cct->_conf.get_val<uint64_t>("rgw_s3vector_index_build_lease_seconds");
    state.builder_id = lock_token;
    if (int ret = write_build_state(table, state); ret < 0) {
      ldpp_dout(this, 1) << "ERROR: failed to record build state for "
          << bucket_name << "." << index_name
          << ", aborting rebuild (ret=" << ret << ")" << dendl;
      return ret;
    }

    // step 7: get distance metric for index config
    DistanceMetric metric = s3vector::get_distance_metric(table, this);
    LanceDBDistanceType distance_type = to_lancedb_distance(metric);

    // step 8: run the vector index build
    ldpp_dout(this, 1) << "INFO: starting vector index build for "
        << bucket_name << "." << index_name
        << " (unindexed=" << stats.num_unindexed_rows
        << ", threshold=" << threshold << ")" << dendl;

    int build_ret = run_vector_index_build(table, distance_type);

    // step 9: mark build complete in table metadata
    build_state_t post_state;
    if (int ret = read_build_state(table, post_state); ret < 0) {
      ldpp_dout(this, 1) << "WARNING: failed to read build state after build for "
          << bucket_name << "." << index_name << dendl;
    }
    post_state.build_in_progress = false;
    post_state.build_started_at = 0;
    if (int ret = write_build_state(table, post_state); ret < 0) {
      ldpp_dout(this, 1) << "WARNING: failed to clear build state for "
          << bucket_name << "." << index_name << dendl;
    }

    if (build_ret == 0) {
      auto post_stats = get_vector_index_stats(table);
      if (post_stats.ok) {
        ldpp_dout(this, 1) << "INFO: vector index build complete for "
            << bucket_name << "." << index_name
            << " (indexed=" << post_stats.stats.num_indexed_rows
            << ", unindexed=" << post_stats.stats.num_unindexed_rows << ")" << dendl;
      } else {
        ldpp_dout(this, 1) << "INFO: vector index build complete for "
            << bucket_name << "." << index_name << dendl;
      }
    } else {
      ldpp_dout(this, 1) << "ERROR: vector index build FAILED for "
          << bucket_name << "." << index_name << dendl;
    }

    return build_ret;
  }

  // ============================================================================
  // Main processing loop
  // ============================================================================

  void process_tables(boost::asio::yield_context yield) {
    ldpp_dout(this, 5) << "INFO: start processing tables" << dendl;
    while (!shutdown) {
      const uint64_t stats_interval_ms =
          cct->_conf.get_val<uint64_t>("rgw_s3vector_index_stats_interval") * 1000;

      std::vector<table_name_t> tables_to_process;
      const auto message_count = messages.consume_all([&tables_to_process, stats_interval_ms, this](auto message) {
        std::unique_ptr<message_t> message_guard(message);
        const auto table_name = std::move(message->table_name);
        switch(message->type) {
          case message_t::Op::REMOVE:
            ldpp_dout(this, 20) << "INFO: received remove message for table: " << table_name.first << "." << table_name.second << dendl;
            tables.erase(table_name);
            return;
          case message_t::Op::UPDATE:
            {
              ldpp_dout(this, 20) << "INFO: received update message for table: " << table_name.first << "." << table_name.second << dendl;
              auto [it, inserted] = tables.emplace(table_name, ceph::coarse_real_clock::now());
              if (inserted) {
                ldpp_dout(this, 20) << "INFO: will try to process new table: " << table_name.first << "." << table_name.second << dendl;
                tables_to_process.push_back(table_name);
                return;
              }
              const auto now = ceph::coarse_real_clock::now();
              const auto time_since_last_process = now - it->second;
              if (time_since_last_process > std::chrono::milliseconds(5000)) {
                ldpp_dout(this, 20) << "INFO: will try to process table: " << table_name.first << "." << table_name.second <<
                ". " << time_since_last_process << " passed since last processing" << dendl;
                it->second = now;
                tables_to_process.push_back(table_name);
              } else {
                ldpp_dout(this, 20) << "INFO: will skip processing table: " << table_name.first << "." << table_name.second <<
                ". only " << time_since_last_process << " passed since last processing" << dendl;
              }
              return;
            }            
          case message_t::Op::SESSION_CREATE:
            {
              ldpp_dout(this, 20) << "INFO: received session create message for bucket: " << table_name.first << dendl;
              std::unique_lock l(sessions_mutex);
              if (sessions.find(table_name.first) == sessions.end()) {
                //create session if not exist, otherwise just ignore
                //Can define session options in the future if needed, for now just create with default options for cache sizes
                LanceDBSession* session = lancedb_session_new(nullptr);
                if (session) {
                  sessions[table_name.first] = SessionPtr(session, LanceDBSessionDeleter());
                  ldpp_dout(this, 20) << "INFO: created session for bucket: " << table_name.first << dendl;
                }
                else {
                  ldpp_dout(this, 1) << "ERROR: failed to create session for bucket: " << table_name.first << dendl;
                }
                return;
              }
              ldpp_dout(this, 20) << "INFO: session already exists for bucket: " << table_name.first << dendl;
              return;
            }
          case message_t::Op::SESSION_DELETE:
            {
              ldpp_dout(this, 20) << "INFO: received session delete message for bucket: " << table_name.first << dendl; 
              std::unique_lock l(sessions_mutex);
              if (sessions.erase(table_name.first) > 0) {
                ldpp_dout(this, 20) << "INFO: deleted session for bucket: " << table_name.first << dendl;
              } else {
                ldpp_dout(this, 20) << "INFO: session doesn't exist for bucket: " << table_name.first << dendl;
              }
              return;
            }
          default:
            ldpp_dout(this, 1) << "ERROR: received message with unknown type for bucket: " << table_name.first << " index: " << table_name.second << dendl;
            return; 

        }
      });

      tokens_waiter tw(this);
      for (const auto& table_name : tables_to_process) {
        // start processing a table
        tokens_waiter::token token(&tw);
        boost::asio::spawn(make_strand(io_context), std::allocator_arg, make_stack_allocator(),
            [this, token = std::move(token), table_name](boost::asio::yield_context yield) {
          const int rc = process_table(table_name, yield);
          if (rc != 0) {
            // rc < 0: error, rc > 0: below threshold — allow recheck on next notification
            if (rc < 0) {
              ldpp_dout(this, 1) << "ERROR: failed to process table: " << table_name.first << "." << table_name.second << " with error code: " << rc << dendl;
            }
            const uint64_t stats_interval_ms =
                cct->_conf.get_val<uint64_t>("rgw_s3vector_index_stats_interval") * 1000;
            tables[table_name] = ceph::coarse_real_clock::now() - std::chrono::milliseconds(stats_interval_ms);
          }
        }, [] (std::exception_ptr eptr) {
          if (eptr) std::rethrow_exception(eptr);
        });
      }
      if (!tables_to_process.empty()) {
        // wait for all pending work to finish
        tw.async_wait(yield);
      }

      if (message_count == 0) {
        // if no messages, sleep for a while before checking again
        ldpp_dout(this, 20) << "INFO: no messages to process" << dendl;
        async_sleep(yield, idle_sleep);
      }
    }
    ldpp_dout(this, 5) << "INFO: manager stopped. done processing all table and session operations" << dendl;
  }
 
public:

  ~Manager() {
    messages.consume_all([](auto message) {
      std::unique_ptr<message_t> message_guard(message);
    });
  }

  void stop() {
    ldpp_dout(this, 5) << "INFO: manager received stop signal. shutting down..." << dendl;
    shutdown = true;
    work_guard.reset();
    for (auto& worker : workers) {
      if (worker.joinable()) {
        // try graceful shutdown first
        auto future = std::async(std::launch::async, [&worker]() {worker.join();});
        if (future.wait_for(idle_sleep*2) == std::future_status::timeout) {
          // force stop if graceful shutdown takes too long
          if (!io_context.stopped()) {
            ldpp_dout(this, 5) << "INFO: force shutdown of manager" << dendl;
            io_context.stop();
          }
          future.wait();
        }
      }
    }
    ldpp_dout(this, 5) << "INFO: manager shutdown ended" << dendl;
  }

  void init() {
    boost::asio::spawn(make_strand(io_context), std::allocator_arg, make_stack_allocator(),
        [this](boost::asio::yield_context yield) {
          process_tables(yield);
        }, [] (std::exception_ptr eptr) {
          if (eptr) std::rethrow_exception(eptr);
        });

    // start the worker threads to do the actual queue processing
    // TODO: use multiple threads
    workers.emplace_back(std::thread([this]() {
      ceph_pthread_setname("notif-worker");
      try {
        ldpp_dout(this, 10) << "INFO: worker started" << dendl;
        io_context.run();
        ldpp_dout(this, 10) << "INFO: worker ended" << dendl;
      } catch (const std::exception& err) {
        ldpp_dout(this, 1) << "ERROR: worker failed with error: " << err.what() << dendl;
        throw err;
      }
    }));
    ldpp_dout(this, 10) << "INFO: started manager" << dendl;
  }

  bool notify_index(const DoutPrefixProvider* dpp, const std::string& bucket_name, const std::string& index_name, message_t::Op op) {
    if (shutdown) {
      ldpp_dout(dpp, 1) << "ERROR: failed to notify s3vectors manager about index: manager is shutting down" << dendl;
      return false;
    }
    auto message_guard = std::make_unique<message_t>(bucket_name, index_name, op);
    if (messages.push(message_guard.get())) {
      std::ignore = message_guard.release(); // ownership transferred to the queue
      ldpp_dout(dpp, 20) << "INFO: notified s3vectors manager about index" << dendl;
      return true;
    }
    ldpp_dout(dpp, 1) << "ERROR: failed to notify s3vectors manager about index: queue is full" << dendl;
    return false;
  }

  bool notify_session(const DoutPrefixProvider* dpp, const std::string& bucket_name, message_t::Op op) {
    if (shutdown) {
      ldpp_dout(dpp, 1) << "ERROR: failed to notify s3vectors manager about session: manager is shutting down" << dendl;
      return false;
    }
    auto message_guard = std::make_unique<message_t>(bucket_name, "", op);
    if (messages.push(message_guard.get())) {
      std::ignore = message_guard.release(); // ownership transferred to the queue
      ldpp_dout(dpp, 20) << "INFO: notified s3vectors manager about session" << dendl;
      return true;
    }
    ldpp_dout(dpp, 1) << "ERROR: failed to notify s3vectors manager about session: queue is full" << dendl;
    return false;
  }

  std::shared_ptr<const LanceDBSession> get_session(const std::string& bucket_name) {
    std::shared_lock l(sessions_mutex);
    auto it = sessions.find(bucket_name);
    if (it == sessions.end()) {
      return nullptr;
    }
    return it->second;
  }
  
  Manager(CephContext* _cct, rgw::sal::Driver* _driver) :
    cct(_cct),
    work_guard(boost::asio::make_work_guard(io_context)),
    driver(_driver),
    messages(8192)
    {}
};

std::unique_ptr<Manager> s_manager;

bool init(const DoutPrefixProvider* dpp, rgw::sal::Driver* driver) {
  if (s_manager) {
    ldpp_dout(dpp, 1) << "ERROR: failed to init s3vectors manager: already exists" << dendl;
    return false;
  }
  s_manager = std::make_unique<Manager>(dpp->get_cct(), driver);
  s_manager->init();
  return true;
}

void shutdown() {
  if (!s_manager) return;
  s_manager->stop();
  s_manager.reset();
}

void pause() {
  shutdown();
}

void resume(const DoutPrefixProvider* dpp, rgw::sal::Driver* driver) {
  init(dpp, driver);
}

bool notify_index_update(const DoutPrefixProvider* dpp, const std::string& bucket_name, const std::string& index_name) {
  if (!s_manager) {
    ldpp_dout(dpp, 1) << "ERROR: failed to notify s3vectors manager about table update: manager is not initialized" << dendl;
    return false;
  }
  return s_manager->notify_index(dpp, bucket_name, index_name, Manager::message_t::Op::UPDATE);
}

bool notify_index_remove(const DoutPrefixProvider* dpp, const std::string& bucket_name, const std::string& index_name) {
  if (!s_manager) {
    ldpp_dout(dpp, 1) << "ERROR: failed to notify s3vectors manager about table remove: manager is not initialized" << dendl;
    return false;
  }
  return s_manager->notify_index(dpp, bucket_name, index_name, Manager::message_t::Op::REMOVE);
}

std::shared_ptr<const LanceDBSession> get_session(const DoutPrefixProvider* dpp, const std::string& bucket_name) {
  if (!s_manager) {
    ldpp_dout(dpp, 1) << "ERROR: failed to get LanceDB session for bucket: manager is not initialized" << dendl;
    return nullptr;
  }
  return s_manager->get_session(bucket_name);
}

bool notify_session_create(const DoutPrefixProvider* dpp, const std::string& bucket_name) {
  if (!s_manager) {
    ldpp_dout(dpp, 1) << "ERROR: failed to notify s3vectors manager about session creation: manager is not initialized" << dendl;
    return false; 
  }
  return s_manager->notify_session(dpp, bucket_name, Manager::message_t::Op::SESSION_CREATE);
}

bool notify_session_delete(const DoutPrefixProvider* dpp, const std::string& bucket_name) {
  if (!s_manager) {
    ldpp_dout(dpp, 1) << "ERROR: failed to notify s3vectors manager about session deletion: manager is not initialized" << dendl;
    return false;
  }
  return s_manager->notify_session(dpp, bucket_name, Manager::message_t::Op::SESSION_DELETE);
}

} // namespace rgw::s3vector
