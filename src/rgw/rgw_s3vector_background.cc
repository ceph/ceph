// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <atomic>
#include <compare>
#include <deque>
#include <memory>
#include <mutex>
#include <shared_mutex>
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
#include <future>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include "rgw_sal.h"
#include "rgw_common.h"
#include "rgw_acl.h"
#include "common/ceph_json.h"
#include "common/ceph_crypto.h"
#include "common/perf_counters.h"
#include "common/perf_counters_collection.h"
#include "rgw_s3vector.h"
#include "rgw_s3vector_background.h"
#include "lancedb.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw::s3vector {

// table metadata key for build coordination state
static constexpr const char* build_state_metadata_key = "s3v_index_state";

// lock object key prefix/suffix within the vector bucket
static constexpr const char* lock_key_prefix = ".s3v-lock-";
static constexpr const char* lock_key_suffix = ".lock";

// Perf counters for the background rebuild subsystem. These replace the previous
// bespoke "counters since boot" in the admin reply: exposing them through Ceph's
// PerfCounters interface lets the existing collector/prometheus stack scrape them
// and present rates/history. `rebuilds_active` is a gauge (current in-flight
// rebuilds); its peak is derivable via a prometheus max_over_time().
enum {
  l_rgw_s3v_bg_first = 20000,
  l_rgw_s3v_bg_rebuilds_started,
  l_rgw_s3v_bg_rebuilds_completed,
  l_rgw_s3v_bg_rebuilds_failed,
  l_rgw_s3v_bg_rebuilds_active,
  l_rgw_s3v_bg_limit_reached,
  l_rgw_s3v_bg_lock_refresh,
  l_rgw_s3v_bg_lock_lost,
  l_rgw_s3v_bg_lock_refresh_fail,
  l_rgw_s3v_bg_last,
};


struct build_state_t {
  int64_t build_started_at = 0;
  std::string builder_id;
  uint64_t global_delete_count = 0;
  // per-index rebuild timing, persisted across RGW restarts via table metadata
  int64_t last_rebuild_completed_at = 0;  // epoch seconds
  int64_t last_rebuild_duration_ms = 0;

  // Persisted in LanceDB table metadata using Ceph's versioned bufferlist
  // encoding (struct_v / compat_v via ENCODE_START / DECODE_START). To evolve:
  // append a new field, bump the ENCODE_START version, and read it back under
  // `if (struct_v >= N)`. DECODE_FINISH skips trailing bytes written by a newer
  // RGW (forward compatible), and compat_v lets an older reader reject a record
  // it cannot understand (it throws buffer::error, surfaced as a decode failure).
  // The metadata store holds null-terminated strings, so the binary encoding is
  // base64-wrapped for storage — see to_base64_str() / from_base64_str().
  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(build_started_at, bl);
    encode(builder_id, bl);
    encode(global_delete_count, bl);
    encode(last_rebuild_completed_at, bl);
    encode(last_rebuild_duration_ms, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(build_started_at, bl);
    decode(builder_id, bl);
    decode(global_delete_count, bl);
    decode(last_rebuild_completed_at, bl);
    decode(last_rebuild_duration_ms, bl);
    DECODE_FINISH(bl);
  }

  // base64 of the versioned bufferlist, so the binary encoding survives the
  // string-typed LanceDB metadata API.
  std::string to_base64_str() const {
    ceph::buffer::list bl;
    encode(bl);
    ceph::buffer::list b64;
    bl.encode_base64(b64);
    return b64.to_str();
  }

  // Returns false on malformed base64 or a corrupt / incompatible-version record.
  bool from_base64_str(const char* str) {
    try {
      ceph::buffer::list b64;
      b64.append(str);
      ceph::buffer::list bl;
      bl.decode_base64(b64);
      auto it = bl.cbegin();
      decode(it);
    } catch (const ceph::buffer::error&) {
      return false;
    }
    return true;
  }
};
WRITE_CLASS_ENCODER(build_state_t)

// ============================================================================
// Background rebuild observability (per-instance)
// ============================================================================

// A significant background action, recorded to the in-memory ring buffer.
// These correspond 1:1 to the log points the integration tests used to scrape.
struct rebuild_event_t {
  enum class Type {
    SPAWN, FINISH, LIMIT_REACHED,
    LOCK_REFRESH, LOCK_LOST, LOCK_REFRESH_FAIL
  };
  // Outcome of a FINISH event; NONE for every other event type.
  enum class Result { NONE, SUCCESS, FAILURE, SKIPPED };
  Type type;
  ceph::coarse_real_time timestamp;
  std::string tenant;
  std::string bucket;
  std::string index;
  int active_rebuilds = 0;
  int max_concurrent = 0;
  int duration_ms = 0;
  Result result = Result::NONE;  // meaningful only for FINISH
};

static const char* event_type_to_str(rebuild_event_t::Type t) {
  switch (t) {
    case rebuild_event_t::Type::SPAWN:             return "spawn";
    case rebuild_event_t::Type::FINISH:            return "finish";
    case rebuild_event_t::Type::LIMIT_REACHED:     return "limit_reached";
    case rebuild_event_t::Type::LOCK_REFRESH:      return "lock_refresh";
    case rebuild_event_t::Type::LOCK_LOST:         return "lock_lost";
    case rebuild_event_t::Type::LOCK_REFRESH_FAIL: return "lock_refresh_fail";
  }
  return "unknown";
}

// External string values are part of the admin status/report API — keep them
// exactly as consumers (tests, tooling) expect: "success"/"failure"/"skipped".
static const char* event_result_to_str(rebuild_event_t::Result r) {
  switch (r) {
    case rebuild_event_t::Result::NONE:    return "";
    case rebuild_event_t::Result::SUCCESS: return "success";
    case rebuild_event_t::Result::FAILURE: return "failure";
    case rebuild_event_t::Result::SKIPPED: return "skipped";
  }
  return "";
}

class Manager : public DoutPrefixProvider {
public:
    //message_t -> pass in empty index name for session messages (can extend to per table sessions in the future if needed)
    // A vector index is identified cluster-wide by (tenant, bucket, index): two
    // tenants can own same-named buckets/indexes, so tenant MUST be part of the key
    // — otherwise their mutation counters, local build dedup, distributed lock, and
    // event log all collide. Named fields (not a nested pair) keep every access site
    // unambiguous and compiler-checked.
    struct table_name_t {
      std::string tenant;
      std::string bucket;
      std::string index;
      auto operator<=>(const table_name_t&) const = default;  // ordering for std::map
      bool operator==(const table_name_t&) const = default;   // equality for unordered_*
    };
    using session_name_t = std::pair<std::string, std::string>; // pair of tenant and vector bucket name
    struct message_t {
      enum class Op {
        UPDATE,
        REMOVE,
        SESSION_CREATE,
        SESSION_DELETE
      };
      message_t(const std::string& tenant, const std::string& bucket_name, const std::string& index_name, Op _type) :
          session_name(tenant, bucket_name), table_name{tenant, bucket_name, index_name}, type(_type) {}
      const session_name_t session_name;
      const table_name_t table_name;
      const Op type;
    };
    // boost::hash<table_name_t> extension point (ADL), so the existing
    // boost::hash<table_name_t> hashers on the maps below keep working unchanged.
    friend std::size_t hash_value(const table_name_t& t) {
      std::size_t seed = 0;
      boost::hash_combine(seed, t.tenant);
      boost::hash_combine(seed, t.bucket);
      boost::hash_combine(seed, t.index);
      return seed;
    }

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
  std::atomic<bool> shutdown{false};
  CephContext* const cct;
  boost::asio::io_context io_context;
  boost::asio::executor_work_guard<Executor> work_guard;
  std::vector<std::thread> workers;
  // The main loop (process_tables) — which refreshes the distributed locks — runs
  // on its OWN io_context serviced by a single dedicated thread, separate from the
  // `io_context` build-worker pool above. This guarantees lock refresh a thread
  // regardless of how many builds are in flight: the synchronous LanceDB FFI build
  // blocks its worker thread for the whole build, so without a dedicated main-loop
  // thread N concurrent builds could consume all N workers and stall refresh,
  // letting another instance reclaim the (now-stale) lock and start a duplicate
  // rebuild of the same index.
  boost::asio::io_context main_loop_io_context;
  boost::asio::executor_work_guard<Executor> main_loop_work_guard;
  std::thread main_loop_thread;
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
  std::unordered_map<session_name_t, SessionPtr, boost::hash<session_name_t>> sessions;

  struct table_state_t {
    // track local insert/delete counts for each table, last_rebuild_time, to determine if a rebuild is needed.
    // data is saved into table metadata to persist across RGW restarts and allow other RGW instances to see the global state.
    std::atomic<uint64_t> insert_count{0};
    std::atomic<uint64_t> delete_count{0};
    ceph::coarse_real_time last_rebuild_time;
    table_state_t() = default;
    table_state_t(table_state_t&& o) noexcept
      : insert_count(o.insert_count.load()),
        delete_count(o.delete_count.load()),
        last_rebuild_time(o.last_rebuild_time) {}
  };
  // guards the `tables` map: shared (read) while the scan loop iterates and while
  // notify_index_mutation bumps a counter; exclusive only to insert a new entry.
  std::shared_mutex tables_mutex;
  std::unordered_map<table_name_t, table_state_t, boost::hash<table_name_t>> tables;
  // guards both `active_builds` and `active_locks` together (they are updated as a
  // pair when a build starts/ends). A short-held plain mutex — never held across I/O.
  std::mutex active_builds_mutex;
  std::unordered_set<table_name_t, boost::hash<table_name_t>> active_builds;//to check locally if a table is already being rebuilt by this RGW instance(a cheap check before acquiring the distributed lock)
  std::atomic<int> active_rebuild_count{0};//how many rebuilds are currently active, in order to control the number of concurrent tasks.

  struct active_lock_t {
    std::string token;
    std::string etag;
    ceph::coarse_real_clock::time_point last_refresh;
    bool lock_lost = false;
    ceph::coarse_real_time start_time;  // when the build acquired the lock
    int refresh_count = 0;              // successful lock refreshes so far
    // true while a refresh_one_lock() coroutine is refreshing THIS lock. The scan
    // skips in-flight locks, so a single lock is never refreshed concurrently with
    // itself (that would break the if_match=etag chain); refreshes of DIFFERENT
    // locks still run in parallel. Always cleared on write-back (see
    // refresh_one_lock). A default member initializer keeps active_lock_t an
    // aggregate, so the 6-field brace-init at the build-start site still compiles.
    bool refresh_in_flight = false;
  };
  std::map<table_name_t, active_lock_t> active_locks; // protected by active_builds_mutex
  MessageQueue messages;
  static constexpr auto idle_sleep = std::chrono::milliseconds(1000); // 1s

  // ---- background rebuild observability (per-instance) ----
  // cached daemon identity so every status report is unambiguously scoped.
  std::string instance_id_;
  std::string host_id_;
  // in-memory ring buffer of significant rebuild actions + aggregate counters.
  std::deque<rebuild_event_t> event_log_;
  // guards the event ring buffer; touched by both the rebuild coroutines (writers)
  // and the admin status/report path (reader).
  mutable std::mutex event_log_mutex_;
  // aggregate counters are published via Ceph PerfCounters (owned by the perf
  // collection on cct); created in the constructor, removed in the destructor.
  PerfCounters* perf_counters_ = nullptr;

  CephContext *get_cct() const override { return cct; }
  unsigned get_subsys() const override { return dout_subsys; }
  std::ostream& gen_prefix(std::ostream& out) const override { return out << "s3vectors manager: "; }

  void async_sleep(boost::asio::yield_context yield, const std::chrono::milliseconds& duration) {
    using Clock = ceph::coarse_mono_clock;
    // Default (any_io_executor) timer so it can bind to any caller's executor.
    using Timer = boost::asio::basic_waitable_timer<Clock,
        boost::asio::wait_traits<Clock>>;
    // Bind the timer to the CALLING coroutine's own executor (via yield), so the
    // sleep is serviced by whatever context that coroutine runs on:
    //   - process_tables   -> dedicated main-loop thread (lock refresh never has to
    //                         wait for a free build-worker thread)
    //   - process_messages -> build-worker pool
    // Binding to a fixed io_context would re-couple the sleep to that pool's threads.
    Timer timer(yield.get_executor());
    timer.expires_after(duration);
    boost::system::error_code ec;
    timer.async_wait(yield[ec]);
    if (ec) {
      ldpp_dout(this, 1) << "ERROR: async_sleep failed with error: " << ec.message() << dendl;
    }
  }

  // Record a significant rebuild action: bump the matching perf counter and
  // append to the in-memory ring buffer (evicting the oldest when full).
  // Additive to the existing ldpp_dout() log lines — never replaces them.
  void record_event(rebuild_event_t::Type type,
                    const std::string& tenant = "",
                    const std::string& bucket = "",
                    const std::string& index = "",
                    int active_rebuilds = 0,
                    int max_concurrent = 0,
                    int duration_ms = 0,
                    rebuild_event_t::Result result = rebuild_event_t::Result::NONE) {
    // Bump the matching perf counter. The `rebuilds_active` gauge is maintained at
    // the active_rebuild_count inc/dec sites (not here), so its value is exact
    // (record_event(FINISH) runs before the decrement).
    switch (type) {
      case rebuild_event_t::Type::SPAWN:
        perf_counters_->inc(l_rgw_s3v_bg_rebuilds_started);
        break;
      case rebuild_event_t::Type::FINISH:
        if (result == rebuild_event_t::Result::SUCCESS) {
          perf_counters_->inc(l_rgw_s3v_bg_rebuilds_completed);
        } else if (result == rebuild_event_t::Result::FAILURE) {
          perf_counters_->inc(l_rgw_s3v_bg_rebuilds_failed);
        }
        break;
      case rebuild_event_t::Type::LIMIT_REACHED:
        perf_counters_->inc(l_rgw_s3v_bg_limit_reached);
        break;
      case rebuild_event_t::Type::LOCK_REFRESH:
        perf_counters_->inc(l_rgw_s3v_bg_lock_refresh);
        break;
      case rebuild_event_t::Type::LOCK_LOST:
        perf_counters_->inc(l_rgw_s3v_bg_lock_lost);
        break;
      case rebuild_event_t::Type::LOCK_REFRESH_FAIL:
        perf_counters_->inc(l_rgw_s3v_bg_lock_refresh_fail);
        break;
    }

    rebuild_event_t ev;
    ev.type = type;
    ev.timestamp = ceph::coarse_real_clock::now();
    ev.tenant = tenant;
    ev.bucket = bucket;
    ev.index = index;
    ev.active_rebuilds = active_rebuilds;
    ev.max_concurrent = max_concurrent;
    ev.duration_ms = duration_ms;
    ev.result = result;

    const size_t max_events = std::max<uint64_t>(
        1, cct->_conf.get_val<uint64_t>("rgw_s3vector_event_log_size"));
    std::lock_guard lg(event_log_mutex_);
    event_log_.push_back(std::move(ev));
    while (event_log_.size() > max_events) {
      event_log_.pop_front();
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

    int ret = 0;
    if (count > 0 && values_out[0]) {
      if (!state.from_base64_str(values_out[0])) {
        // corrupt / unparseable metadata, or a record whose incompatible (compat)
        // version this build cannot decode: do not silently proceed with a
        // half-populated struct. Reset to clean defaults (treated as "no prior
        // state"; the next write repairs the record) and report the error.
        ldpp_dout(this, 0) << "ERROR: failed to decode build state metadata (corrupt "
            "or incompatible version), resetting to defaults" << dendl;
        state = build_state_t{};
        ret = -EINVAL;
      }
    }
    lancedb_free_metadata(keys_out, values_out, count);
    return ret;
  }

  int write_build_state(const LanceDBTable* table, const build_state_t& state) {
    const std::string encoded = state.to_base64_str();
    const char* key = build_state_metadata_key;
    const char* value = encoded.c_str();
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

  // ============================================================================
  // Index stats helper
  // ============================================================================

  // Success holds the stats; failure holds the LanceDBError that occurred.
  using index_stats_result = tl::expected<LanceDBIndexStats, LanceDBError>;

  index_stats_result get_vector_index_stats(const LanceDBTable* table) {
    LanceDBIndexStats stats = {};
    char* error_message = nullptr;

    // query the vector index directly by its known name (data_idx).
    // LanceDB names indices as {column_name}_idx — our vector column is
    // always "data", so the vector index is always "data_idx".
    // this avoids picking up the scalar "key_idx" which reports misleading
    // unindexed counts (scalar BTree indices always show indexed=0).
    const auto err = lancedb_table_index_stats(
        table, vector_index_name, &stats, &error_message);
    if (err == LANCEDB_SUCCESS) {
      return stats;
    }

    // Distinguish "no vector index yet" (benign, expected before the first
    // rebuild) from a genuine backend failure. Only the former should be
    // reported as "all rows unindexed"; a real error (IO, timeout, corruption,
    // ...) must NOT be masked behind fabricated stats, or a transient failure
    // would spuriously trigger a full index rebuild.
    const bool index_absent = (err == LANCEDB_INDEX_NOT_FOUND);
    ldpp_dout(this, index_absent ? 10 : 1)
        << (index_absent ? "INFO: no vector index yet for '"
                         : "ERROR: lancedb_table_index_stats failed for '")
        << vector_index_name << "': "
        << (error_message ? error_message : "unknown") << dendl;

    if (error_message) {
      lancedb_free_string(error_message);
    }

    if (!index_absent) {
      // genuine failure — let the caller handle it (returns -EIO)
      return tl::make_unexpected(err);
    }

    // vector index doesn't exist yet — all rows are unindexed
    stats.num_indexed_rows = 0;
    stats.num_unindexed_rows = lancedb_table_count_rows(table);
    stats.num_indices = 0;
    return stats;
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
  // Load the regular S3 backing bucket (not the vector bucket) for lock operations.
  // The vector bucket is created with BucketIndexType::Indexless and has no bucket
  // index shards, so direct SAL writes (get_atomic_writer) fail with -ENOENT on
  // get_bucket_shard(). The regular S3 bucket (same name) has normal index shards
  // and is the same bucket that LanceDB uses for data storage via rgw_sal_wrapper.
  // TODO: the backing S3 bucket is currently created externally (e.g. by tests);
  // CreateVectorBucket should create it automatically for the RGW backend.
  int load_bucket_for_lock(const std::string& tenant,
                           const std::string& vector_bucket_name,
                           std::unique_ptr<rgw::sal::Bucket>& bucket,
                           optional_yield y) {
    rgw_bucket bucket_id;
    bucket_id.tenant = tenant;  // scope the lock object to the owning tenant
    bucket_id.name = vector_bucket_name;
    int ret = driver->load_bucket(this, bucket_id, &bucket, y);
    if (ret < 0) {
      ldpp_dout(this, 1) << "ERROR: failed to load bucket for lock: "
          << vector_bucket_name << " ret=" << ret << dendl;
      return ret;
    }
    return 0;
  }

  // ---- low-level lock object operations ----

  // PUT lock object with if-none-match="*" (conditional create).
  // exactly one concurrent caller succeeds, others get -ERR_PRECONDITION_FAILED.
  int put_lock_object(const std::string& tenant,
                      const std::string& vector_bucket_name,
                      const std::string& lock_key,
                      const std::string& token,
                      optional_yield y,
                      std::string* etag_out = nullptr) {
    std::unique_ptr<rgw::sal::Bucket> bucket;
    int ret = load_bucket_for_lock(tenant, vector_bucket_name, bucket, y);
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
    if (ret == 0 && etag_out) {
      *etag_out = etag;
    }
    return ret;
  }

  struct refresh_result {
    int ret;
    std::string new_etag;
  };

  // Refresh (conditional overwrite) the lock object with a fresh timestamp.
  // Uses if_match=current_etag to ensure only the lock holder can refresh.
  // Returns the new ETag on success for subsequent refresh calls.
  refresh_result refresh_lock_object(const std::string& tenant,
                                     const std::string& vector_bucket_name,
                                     const std::string& lock_key,
                                     const std::string& token,
                                     const std::string& current_etag,
                                     optional_yield y) {
    std::unique_ptr<rgw::sal::Bucket> bucket;
    int ret = load_bucket_for_lock(tenant, vector_bucket_name, bucket, y);
    if (ret < 0) return {ret, {}};

    auto obj = bucket->get_object({lock_key});
    std::string req_id = driver->zone_unique_id(driver->get_new_req_id());
    ACLOwner owner;
    owner.id = bucket->get_owner();
    auto writer = driver->get_atomic_writer(this, y, obj.get(),
        owner, nullptr, 0, req_id);

    ret = writer->prepare(y);
    if (ret < 0) return {ret, {}};

    lock_body_t lock_body = make_lock_body(token);
    std::string body = lock_body.to_json_str();
    bufferlist bl;
    bl.append(body);
    const auto etag = TOPNSPC::crypto::digest<TOPNSPC::crypto::MD5>(bl).to_str();
    ret = writer->process(std::move(bl), 0);
    if (ret < 0) return {ret, {}};

    ret = writer->process({}, body.size());
    if (ret < 0) return {ret, {}};

    std::map<std::string, bufferlist> attrs;
    bufferlist etag_bl;
    etag_bl.append(etag.c_str(), etag.size());
    attrs[RGW_ATTR_ETAG] = std::move(etag_bl);

    const req_context rctx{this, y, nullptr};
    bool canceled = false;

    ret = writer->complete(body.size(), etag,
                           nullptr, ceph::real_clock::now(), attrs,
                           rgw::cksum::no_cksum, ceph::real_time(),
                           /*if_match=*/current_etag.c_str(),
                           /*if_nomatch=*/nullptr,
                           nullptr, nullptr, &canceled,
                           rctx, 0);

    if (canceled) {
      return {-ERR_PRECONDITION_FAILED, {}};
    }
    return {ret, etag};
  }

  // DELETE lock object with conditional if_match (ETag).
  // if etag is non-empty, only deletes if the object's current ETag matches —
  // prevents deleting a lock that was reclaimed by another instance.
  // if etag is empty, performs unconditional delete.
  int delete_lock_object(const std::string& tenant,
                         const std::string& vector_bucket_name,
                         const std::string& lock_key,
                         const std::string& etag,
                         optional_yield y) {
    std::unique_ptr<rgw::sal::Bucket> bucket;
    int ret = load_bucket_for_lock(tenant, vector_bucket_name, bucket, y);
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

  lock_read_result get_lock_object(const std::string& tenant,
                                   const std::string& vector_bucket_name,
                                   const std::string& lock_key,
                                   optional_yield y) {
    std::unique_ptr<rgw::sal::Bucket> bucket;
    int ret = load_bucket_for_lock(tenant, vector_bucket_name, bucket, y);
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
  // note: the main loop periodically refreshes the lock timestamp during builds,
  // so the TTL does not need to exceed the build duration. if the builder crashes,
  // refreshes stop and the lock becomes reclaimable after TTL expires.
  struct lock_acquire_result {
    std::string token;
    std::string etag;
  };

  lock_acquire_result try_acquire_lock(const std::string& tenant,
                                       const std::string& bucket_name,
                                       const std::string& index_name,
                                       optional_yield y) {
    const std::string lock_key = make_lock_key(index_name);

    // step 1: read existing lock
    auto lock_info = get_lock_object(tenant, bucket_name, lock_key, y);
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

      if (existing_lock.timestamp > 0 && age < lock_ttl) {
        ldpp_dout(this, 5) << "INFO: lock held for " << bucket_name << "." << index_name
            << " by " << existing_lock.token
            << " (age=" << age << "s)" << dendl;
        return {};
      }

      // step 2: stale lock — conditional delete using ETag from step 1
      ldpp_dout(this, 5) << "INFO: deleting stale lock for " << bucket_name
          << "." << index_name << " (age=" << age << "s, ttl=" << lock_ttl << "s)" << dendl;
      int del_ret = delete_lock_object(tenant, bucket_name, lock_key, lock_info.etag, y);
      if (del_ret == -ERR_PRECONDITION_FAILED) {
        ldpp_dout(this, 5) << "INFO: stale lock for " << bucket_name << "." << index_name
            << " was already reclaimed by another instance" << dendl;
        return {};
      }
    }

    // step 3: conditional create — only one caller wins
    // create-if-absent (atomic create)
    std::string token = generate_lock_token();
    std::string etag;
    int ret = put_lock_object(tenant, bucket_name, lock_key, token, y, &etag);
    if (ret == 0) {
      ldpp_dout(this, 5) << "INFO: acquired lock for " << bucket_name
          << "." << index_name << " token=" << token << dendl;
      return {std::move(token), std::move(etag)};
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
  void release_lock(const std::string& tenant,
                    const std::string& bucket_name,
                    const std::string& index_name,
                    const std::string& token,
                    optional_yield y) {
    const std::string lock_key = make_lock_key(index_name);

    // step 1: read lock
    auto lock_info = get_lock_object(tenant, bucket_name, lock_key, y);
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
    int ret = delete_lock_object(tenant, bucket_name, lock_key, lock_info.etag, y);
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
    vec_config.replace = 1;            // replace existing index

    const auto index_type_str = cct->_conf.get_val<std::string>("rgw_s3vector_index_type");
    const LanceDBIndexType index_type = string_to_index_type(index_type_str);

    const char* columns[] = {data_field};
    char* error_message = nullptr;

    const LanceDBError result = lancedb_table_create_vector_index(
        table, columns, 1, index_type, &vec_config, &error_message);

    if (result != LANCEDB_SUCCESS) {
      const bool permanent = (result == LANCEDB_LANCE ||
                              result == LANCEDB_INVALID_INPUT ||
                              result == LANCEDB_NOT_SUPPORTED);
      ldpp_dout(this, 0) << "ERROR: lancedb_table_create_vector_index failed"
          << " (error_code=" << result
          << ", " << (permanent ? "permanent" : "transient") << "): "
          << (error_message ? error_message : "unknown") << dendl;
      lancedb_free_string(error_message);
      return permanent ? -ENOTSUP : -EIO;
    }
    return 0;
  }

  // ============================================================================
  // Core table processing: stats check → lock → build → cleanup
  // ============================================================================

  static constexpr int RESULT_SUCCESS = 0;
  static constexpr int RESULT_SKIPPED = 1;
  static constexpr int RESULT_ALREADY_BUILDING = 2;
  static constexpr int RESULT_REBUILD_DISABLED = 3;

  void restore_counters(const table_name_t& table_name,
                        uint64_t inserts, uint64_t deletes) {
    std::shared_lock sl(tables_mutex);
    auto it = tables.find(table_name);
    if (it != tables.end()) {
      if (inserts > 0) {
        it->second.insert_count.fetch_add(inserts, std::memory_order_relaxed);
      }
      if (deletes > 0) {
        it->second.delete_count.fetch_add(deletes, std::memory_order_relaxed);
      }
    }
  }

  int process_table(const table_name_t& table_name,
                    uint64_t local_inserts, uint64_t local_deletes,
                    boost::asio::yield_context yield) {
    const auto& tenant = table_name.tenant;
    const auto& bucket_name = table_name.bucket;
    const auto& index_name = table_name.index;

    // step 1: check if this RGW is already building this table (cheapest check).
    {
      std::lock_guard lg(active_builds_mutex);
      if (active_builds.count(table_name)) {
        ldpp_dout(this, 5) << "INFO: this RGW is already building "
            << bucket_name << "." << index_name << ", skipping"
            << " (local_inserts=" << local_inserts
            << ", local_deletes=" << local_deletes << ")" << dendl;
        restore_counters(table_name, local_inserts, local_deletes);
        return RESULT_ALREADY_BUILDING;
      }
    }

    // step 2: read ratio-based config
    const double insert_ratio_threshold = cct->_conf.get_val<double>("rgw_s3vector_index_insert_rebuild_ratio");
    const double delete_ratio_threshold = cct->_conf.get_val<double>("rgw_s3vector_index_delete_rebuild_ratio");
    if (insert_ratio_threshold <= 0.0 && delete_ratio_threshold <= 0.0) {
      ldpp_dout(this, 20) << "INFO: automatic index rebuild disabled for "
          << bucket_name << "." << index_name << dendl;
      return RESULT_REBUILD_DISABLED;
    }

    // step 3: open table (tenant-scoped; &tenant with an empty string behaves like
    // nullptr for the default tenant, see tenant_name())
    int connect_result = 0;
    LanceDBConnection* conn = s3vector::connect(this, driver, &tenant, bucket_name, connect_result);
    if (!conn) {
      ldpp_dout(this, 5) << "WARNING: cannot connect to database for "
          << bucket_name << ", skipping" << dendl;
      return -EIO;
    }
    LanceDBTable* table = nullptr;
    char* open_error_message = nullptr;
    if (const LanceDBError oerr = lancedb_connection_open_table(conn, index_name.c_str(), &table, &open_error_message);
        oerr != LANCEDB_SUCCESS || !table) {
      ldpp_dout(this, 5) << "WARNING: cannot open table "
          << bucket_name << "." << index_name << ", may have been deleted, error: "
          << (open_error_message ? open_error_message : "unknown") << dendl;
      lancedb_free_string(open_error_message);
      lancedb_connection_free(conn);
      return -ENOENT;
    }

    struct table_guard_t {
      LanceDBTable* table;
      LanceDBConnection* conn;
      ~table_guard_t() {
        lancedb_table_free(table);
        lancedb_connection_free(conn);
      }
    } table_guard{table, conn};

    // step 4: get index stats(lanceDB index stats are global ground truth, no lock needed, it does not reflect deleted rows)
    auto stats_result = get_vector_index_stats(table);
    if (!stats_result) {
      ldpp_dout(this, 1) << "ERROR: failed to get index stats for "
          << bucket_name << "." << index_name << dendl;
      return -EIO;
    }
    const auto& vector_index_status = *stats_result;

    ldpp_dout(this, 1) << "INFO: index stats for " << bucket_name << "." << index_name
        << ": indexed=" << vector_index_status.num_indexed_rows
        << " unindexed=" << vector_index_status.num_unindexed_rows
        << " num_indices=" << vector_index_status.num_indices
        << " local_inserts=" << local_inserts
        << " local_deletes=" << local_deletes << dendl;

    // step 4b: minimum row count check
    const uint64_t min_rows = cct->_conf.get_val<uint64_t>("rgw_s3vector_index_min_rows");
    const uint64_t total_rows_int = vector_index_status.num_indexed_rows + vector_index_status.num_unindexed_rows;
    if (total_rows_int < min_rows) {
      ldpp_dout(this, 5) << "INFO: " << bucket_name << "." << index_name
          << " has " << total_rows_int << " rows, below minimum " << min_rows
          << " for index build" << dendl;
      return RESULT_SKIPPED;
    }

    // step 5: insert ratio pre-check (stats are global ground truth, no lock needed)
    const double total_rows = static_cast<double>(total_rows_int);
    bool insert_rebuild = false;
    if (total_rows > 0 && insert_ratio_threshold > 0.0) {
      const double insert_ratio = static_cast<double>(vector_index_status.num_unindexed_rows) / total_rows;
      insert_rebuild = (insert_ratio >= insert_ratio_threshold);
      ldpp_dout(this, 5) << "INFO: insert ratio for " << bucket_name << "." << index_name
          << ": " << insert_ratio << " (threshold=" << insert_ratio_threshold << ")" << dendl;
    }

    if (!insert_rebuild && local_deletes == 0) {
      ldpp_dout(this, 1) << "INFO: " << bucket_name << "." << index_name
          << " below insert threshold and no local deletes, skipping" << dendl;
      return RESULT_SKIPPED;
    }

    // step 6: acquire distributed lock.
    // needed to read/write global_delete_count atomically (LanceDB metadata
    // commits are not mutual-exclusive — concurrent writes cause CommitConflict)
    // and to protect the rebuild itself.
    optional_yield y(yield);
    auto lock_result = try_acquire_lock(tenant, bucket_name, index_name, y);
    if (lock_result.token.empty()) {
      ldpp_dout(this, 5) << "INFO: lock held by another process for "
          << bucket_name << "." << index_name << ", skipping" << dendl;
      restore_counters(table_name, local_inserts, local_deletes);
      return RESULT_SKIPPED;
    }
    const std::string& lock_token = lock_result.token;

    {
      std::lock_guard lg(active_builds_mutex);
      const auto now = ceph::coarse_real_clock::now();
      active_builds.insert(table_name);
      active_locks[table_name] = {lock_token, lock_result.etag,
                                   now, false, now, 0};
    }

    struct lock_guard_t {
      Manager* mgr;
      const table_name_t& table_name;
      const std::string& tenant;
      const std::string& bucket_name;
      const std::string& index_name;
      const std::string& token;
      optional_yield y;
      ~lock_guard_t() {
        {
          std::lock_guard lg(mgr->active_builds_mutex);
          mgr->active_builds.erase(table_name);
          mgr->active_locks.erase(table_name);
        }
        mgr->release_lock(tenant, bucket_name, index_name, token, y);
      }
    } lock_guard{this, table_name, tenant, bucket_name, index_name, lock_token, y};

    // step 7: read build state under lock — fresh global_delete_count
    build_state_t prev_state;
    read_build_state(table, prev_state);
    const uint64_t global_delete_count = prev_state.global_delete_count + local_deletes;

    // step 8: delete ratio check (requires global counter, must be under lock)
    bool delete_rebuild = false;
    if (vector_index_status.num_indexed_rows > 0 && delete_ratio_threshold > 0.0 && global_delete_count > 0) {
      const double delete_ratio = static_cast<double>(global_delete_count)
          / (static_cast<double>(vector_index_status.num_indexed_rows) + global_delete_count);
      delete_rebuild = (delete_ratio >= delete_ratio_threshold);
      ldpp_dout(this, 5) << "INFO: delete ratio for " << bucket_name << "." << index_name
          << ": " << delete_ratio << " (threshold=" << delete_ratio_threshold
          << ", global_delete_count=" << global_delete_count << ")" << dendl;
    }

    if (!insert_rebuild && !delete_rebuild) {
      ldpp_dout(this, 1) << "INFO: " << bucket_name << "." << index_name
          << " below rebuild thresholds, skipping" << dendl;
      if (local_deletes > 0) {
        prev_state.global_delete_count = global_delete_count;
        write_build_state(table, prev_state);
      }
      return 1;
    }

    // step 9: record build state
    build_state_t state;
    state.build_started_at = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    state.builder_id = lock_token;
    state.global_delete_count = global_delete_count;
    if (int ret = write_build_state(table, state); ret < 0) {
      ldpp_dout(this, 1) << "ERROR: failed to record build state for "
          << bucket_name << "." << index_name
          << ", aborting rebuild (ret=" << ret << ")" << dendl;
      return ret;
    }

    // step 10: get distance metric for index config
    DistanceMetric metric = s3vector::get_distance_metric(table, this);
    LanceDBDistanceType distance_type = to_lancedb_distance(metric);

    // step 11: run the vector index build
    ldpp_dout(this, 1) << "INFO: starting vector index build for "
        << bucket_name << "." << index_name
        << " (unindexed=" << vector_index_status.num_unindexed_rows
        << ", insert_rebuild=" << insert_rebuild
        << ", delete_rebuild=" << delete_rebuild << ")" << dendl;

    int build_ret = run_vector_index_build(table, distance_type);

    // step 12: check if the lock was lost during the build (detected by main-loop refresh)
    bool lock_lost = false;
    {
      std::lock_guard lg(active_builds_mutex);
      auto it = active_locks.find(table_name);
      if (it != active_locks.end()) {
        lock_lost = it->second.lock_lost;
      }
    }

    if (lock_lost) {
      ldpp_dout(this, 1) << "WARNING: lock was lost during build for "
          << bucket_name << "." << index_name
          << " — skipping metadata update" << dendl;
    } else {
      // step 13: mark build complete in table metadata
      build_state_t post_state;
      if (int ret = read_build_state(table, post_state); ret < 0) {
        ldpp_dout(this, 1) << "WARNING: failed to read build state after build for "
            << bucket_name << "." << index_name << dendl;
      }
      post_state.build_started_at = 0;
      if (build_ret == 0 && delete_rebuild) {
        post_state.global_delete_count = 0;
      }
      if (build_ret == 0) {
        const int64_t now_s = std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        post_state.last_rebuild_completed_at = now_s;
        post_state.last_rebuild_duration_ms =
            (state.build_started_at > 0) ? (now_s - state.build_started_at) * 1000 : 0;
      }
      if (int ret = write_build_state(table, post_state); ret < 0) {
        ldpp_dout(this, 1) << "WARNING: failed to clear build state for "
            << bucket_name << "." << index_name << dendl;
      }
    }

    if (build_ret == 0) {
      auto post_stats = get_vector_index_stats(table);
      if (post_stats) {
        ldpp_dout(this, 1) << "INFO: vector index build complete for "
            << bucket_name << "." << index_name
            << " (indexed=" << post_stats->num_indexed_rows
            << ", unindexed=" << post_stats->num_unindexed_rows << ")" << dendl;
      } else {
        ldpp_dout(this, 1) << "INFO: vector index build complete for "
            << bucket_name << "." << index_name << dendl;
      }
    } else if (build_ret == -ENOTSUP) {
      ldpp_dout(this, 1) << "WARNING: vector index build not possible for "
          << bucket_name << "." << index_name
          << " (permanent error, not retrying)" << dendl;
    } else {
      ldpp_dout(this, 1) << "ERROR: vector index build FAILED for "
          << bucket_name << "." << index_name
          << " (transient error, will retry)" << dendl;
      restore_counters(table_name, local_inserts, local_deletes);
    }

    return build_ret;
  }

  // ============================================================================
  // Lock refresh: keep distributed locks alive during long builds
  // ============================================================================

  // Scan the active locks and refresh any whose interval (TTL/3) has elapsed.
  //
  // The scan holds active_builds_mutex only for a short, I/O-free critical section
  // to snapshot the due locks and claim each one (refresh_in_flight=true). The
  // actual lock-refresh I/O is then performed by one refresh_one_lock() coroutine
  // PER lock, spawned on the main-loop io_context (never the blocking build-worker
  // pool). So:
  //   - the mutex is never held across I/O -> build start/finish on OTHER tables
  //     is not blocked by refresh I/O;
  //   - refreshes of different locks run concurrently -> a whole pass costs ~1 RTT
  //     instead of N*RTT, so the TTL/3 deadline holds even at a high
  //     max_concurrent_rebuilds (many simultaneous active locks).
  void refresh_active_locks(boost::asio::yield_context /*yield*/) {
    if (shutdown) return;

    const uint64_t lock_ttl = cct->_conf.get_val<uint64_t>("rgw_s3vector_index_lock_ttl_seconds");
    const auto refresh_interval = std::chrono::seconds(lock_ttl / 3);
    const auto now = ceph::coarse_real_clock::now();

    // snapshot the locks due for refresh under a short, I/O-free lock.
    struct due_lock_t { table_name_t name; std::string token; std::string etag; };
    std::vector<due_lock_t> lockers_in_work;
    {
      std::lock_guard lg(active_builds_mutex);
      for (auto& [name, lock] : active_locks) {
        if (lock.lock_lost || lock.refresh_in_flight) continue;
        if (now - lock.last_refresh < refresh_interval) continue;
        lock.refresh_in_flight = true;  // claim: the next scan skips this lock until
                                        // refresh_one_lock writes back and clears it.
                                        
        //the vector contain only the locks that are due for refresh, i.e. locks that have not been refreshed in the last TTL/3 seconds and are not already being refreshed.  
        //so we can spawn one coroutine per lock to refresh them concurrently.
        lockers_in_work.push_back({name, lock.token, lock.etag});
      }
    }

    // spawn one independent refresh coroutine per due lock. per-lock serialization
    // is guaranteed by refresh_in_flight (above); cross-lock parallelism comes from
    // these running concurrently on the main-loop io_context.
    for (const auto& d : lockers_in_work) {
      boost::asio::spawn(make_strand(main_loop_io_context), std::allocator_arg, make_stack_allocator(),
          [this, d](boost::asio::yield_context y) {
            refresh_one_lock(d.name, d.token, d.etag, y);
          },
          [this, name = d.name, token = d.token](std::exception_ptr eptr) {
            if (!eptr) return;
            // on exception the normal write-back did not run: release our in-flight
            // claim (only if still our generation) so the lock is retried next scan.
            {
              std::lock_guard lg(active_builds_mutex);
              auto it = active_locks.find(name);
              if (it != active_locks.end() && it->second.token == token) {
                it->second.refresh_in_flight = false;
              }
            }
            try {
              std::rethrow_exception(eptr);
            } catch (const std::exception& e) {
              ldpp_dout(this, 0) << "ERROR: lock refresh coroutine exception for "
                  << name.bucket << "." << name.index << ": " << e.what() << dendl;
            }
          });
    }
  }

  // Refresh a single distributed lock. Runs OUTSIDE active_builds_mutex: the RADOS
  // round-trip (refresh_lock_object) is done unlocked, and the mutex is re-acquired
  // only briefly to write the result back. On write-back the entry is re-validated
  // — it may have been erased by a finished build, or released and re-acquired by a
  // new build under a different token — before the result is applied.
  void refresh_one_lock(const table_name_t& name,
                        const std::string& token,
                        const std::string& etag,
                        boost::asio::yield_context yield) {
    const std::string lock_key = make_lock_key(name.index);
    //the actual refresh I/O is done outside the lock, so it doesn't block other builds or refreshes.
    const auto result = refresh_lock_object(name.tenant, name.bucket, lock_key, token, etag,
                                            optional_yield(yield));
    const auto now = ceph::coarse_real_clock::now();

    std::lock_guard lg(active_builds_mutex);//this lock is only held for a short time to update the lock state, so it doesn't block other builds or refreshes.
    auto it = active_locks.find(name);
    if (it == active_locks.end()) {
      // the build finished and erased the lock while we were refreshing it.
      return;
    }
    auto& lock = it->second;
    if (lock.token != token) {
      // stale generation: the lock was released and re-acquired by a new build.
      // Leave the new generation's state (incl. its own refresh_in_flight) alone.
      return;
    }
    lock.refresh_in_flight = false;  // release our claim on this generation
    if (lock.lock_lost) return;      // already marked lost by an earlier pass

    if (result.ret == 0) {
      // a new etag means the lock was successfully refreshed. 
      lock.etag = result.new_etag;
      // the lock timestamp is updated to the current time, the lock is still for another TTL seconds.
      lock.last_refresh = now;
      ++lock.refresh_count;
      ldpp_dout(this, 10) << "INFO: refreshed lock for "
          << name.bucket << "." << name.index << dendl;
      record_event(rebuild_event_t::Type::LOCK_REFRESH, name.tenant, name.bucket, name.index);
    } else if (result.ret == -ERR_PRECONDITION_FAILED) {
      // lock_lost: another instance reclaimed the lock (stolen) while we were refreshing it. mark it lost so the build can skip metadata updates.
      lock.lock_lost = true;
      ldpp_dout(this, 1) << "WARNING: lock lost (stolen) for "
          << name.bucket << "." << name.index
          << " during active build" << dendl;
      record_event(rebuild_event_t::Type::LOCK_LOST, name.tenant, name.bucket, name.index);
    } else {
      ldpp_dout(this, 1) << "WARNING: failed to refresh lock for "
          << name.bucket << "." << name.index
          << " (ret=" << result.ret << "), will retry" << dendl;
      record_event(rebuild_event_t::Type::LOCK_REFRESH_FAIL, name.tenant, name.bucket, name.index);
    }
  }

  // ============================================================================
  // Control-message / session loop
  // ============================================================================
  //
  // Runs on the build-worker io_context (the multi-threaded pool), NOT on the
  // dedicated main-loop thread. SESSION_CREATE/SESSION_DELETE perform synchronous
  // blocking LanceDB FFI (create / free a session); keeping them off the main loop
  // means that blocking can never stall the main loop's periodic lock refresh. A
  // single coroutine drains the whole queue, so all control messages (REMOVE /
  // SESSION_CREATE / SESSION_DELETE) stay strictly ordered without an extra strand.
  void process_messages(boost::asio::yield_context yield) {
    ldpp_dout(this, 5) << "INFO: start processing control messages" << dendl;
    while (!shutdown) {
      // consume control messages (REMOVE / SESSION_CREATE / SESSION_DELETE)
      messages.consume_all([this](auto message) {
        std::unique_ptr<message_t> message_guard(message);
        const auto table_name = std::move(message->table_name);
        const auto session_name = std::move(message->session_name);
        switch(message->type) {
          case message_t::Op::REMOVE:
            {
              ldpp_dout(this, 20) << "INFO: received remove message for table: " << table_name.bucket << "." << table_name.index << dendl;
              std::unique_lock ul(tables_mutex);//exclusive lock to erase the table from the map, since we don't want any other thread to be reading or writing to this table while it's being removed.(short time)
              tables.erase(table_name);
              return;
            }
          case message_t::Op::SESSION_CREATE:
            {
              ldpp_dout(this, 20) << "INFO: received session create message for bucket: " << session_name.second <<
                " of tenant: " << session_name.first << dendl;
              std::unique_lock l(sessions_mutex);
              if (sessions.find(session_name) != sessions.end()) {
                ldpp_dout(this, 20) << "INFO: session already exists for bucket: " << session_name.second <<
                  " of tenant: " << session_name.first << dendl;
                return;
              }

              const std::string backend_str = cct->_conf.get_val<std::string>("rgw_s3vector_backend");
              BackendType backend_type;
              if (int ret = get_backend_type(backend_str, backend_type); ret < 0) {
                ldpp_dout(this, 1) << "ERROR: unrecognized backend type: " << backend_str << dendl;
                return;
              }
              LanceDBSession* session = nullptr;

              // To pass custom LanceDBSessionOptions for cache sizes etc.
              const LanceDBSessionOptions* options = nullptr;

              // NOTE: this is a synchronous blocking FFI call; it runs here on the
              // worker pool precisely so it cannot block the main loop's refresh.
              if (is_rgw_backend(backend_type)) {
                session = create_rgw_session(this, driver, session_name.first, options);
              } else {
                session = lancedb_session_new(options);
              }
              if (!session) {
                ldpp_dout(this, 1) << "ERROR: failed to create session for bucket: " << session_name.second <<
                  " of tenant: " << session_name.first << dendl;
                return;
              }
              ldpp_dout(this, 20) << "INFO: created session for bucket: " << session_name.second <<
                " of tenant: " << session_name.first << dendl;

              sessions[session_name] = SessionPtr(session, LanceDBSessionDeleter());
              return;
            }
          case message_t::Op::SESSION_DELETE:
            {
              ldpp_dout(this, 20) << "INFO: received session delete message for bucket: " << session_name.second <<
                " of tenant: " << session_name.first << dendl;
              std::unique_lock l(sessions_mutex);
              if (sessions.erase(session_name) > 0) {
                ldpp_dout(this, 20) << "INFO: deleted session for bucket: " << session_name.second <<
                  " of tenant: " << session_name.first << dendl;
              } else {
                ldpp_dout(this, 20) << "INFO: session doesn't exist for bucket: " << session_name.second <<
                  " of tenant: " << session_name.first << dendl;
              }
              return;
            }
          default:
            return;
        }
      });

      async_sleep(yield, idle_sleep);
    }
    ldpp_dout(this, 5) << "INFO: stopped processing control messages" << dendl;
  }

  // ============================================================================
  // Main processing loop
  // ============================================================================

  void process_tables(boost::asio::yield_context yield) {
    ldpp_dout(this, 5) << "INFO: start processing tables" << dendl;
    while (!shutdown) {
      const int max_concurrent = cct->_conf.get_val<int64_t>("rgw_s3vector_max_concurrent_rebuilds");
      const auto cooldown = std::chrono::seconds(
          cct->_conf.get_val<uint64_t>("rgw_s3vector_index_rebuild_cooldown"));

      // 1. pause switch: rgw_s3vector_max_concurrent_rebuilds == 0 stops the
      // worker from starting new rebuilds. Control messages keep being consumed by
      // the separate process_messages loop (so table/session bookkeeping stays
      // current), and here we skip the table scan below - so pending mutation
      // counters are NOT consumed (no rebuild signal is lost) and no LIMIT_REACHED
      // event is emitted on every scan while paused. We still fall through to
      // refresh_active_locks() so that any build already in flight when the pause
      // took effect keeps its distributed lock alive until it finishes. Raising the
      // value resumes rebuilds normally.
      const bool paused = (max_concurrent <= 0);
      if (paused) {
        ldpp_dout(this, 20) << "INFO: background rebuilds paused "
            "(rgw_s3vector_max_concurrent_rebuilds=0), skipping table scan" << dendl;
      }

      // 2. scan tables for pending mutations
      if (!paused) {
        std::shared_lock sl(tables_mutex);//this lock is held only for the duration of scanning the tables map, not for the entire processing of each table.
        const auto now = ceph::coarse_real_clock::now();
        for (auto& [name, state] : tables) {
          if (active_rebuild_count.load(std::memory_order_relaxed) >= max_concurrent) {
            ldpp_dout(this, 1) << "INFO: rebuild concurrency limit reached"
                << " (active_rebuilds=" << active_rebuild_count.load(std::memory_order_relaxed)
                << ", max_concurrent=" << max_concurrent
                << "), deferring remaining tables" << dendl;
            record_event(rebuild_event_t::Type::LIMIT_REACHED, "", "", "",
                         active_rebuild_count.load(std::memory_order_relaxed),
                         max_concurrent);
            break;
          }

          const uint64_t inserts = state.insert_count.load(std::memory_order_relaxed);
          const uint64_t deletes = state.delete_count.load(std::memory_order_relaxed);
          if (inserts == 0 && deletes == 0) {
            continue;
          }
          if (now - state.last_rebuild_time < cooldown) {
            ldpp_dout(this, 20) << "INFO: table " << name.bucket << "." << name.index
                << " under cooldown, deferring (inserts=" << inserts
                << ", deletes=" << deletes << ")" << dendl;
            continue;
          }
          {
            std::lock_guard lg(active_builds_mutex);
            if (active_builds.count(name)) {
              continue;
            }
          }

          // Consume the counts we observed so the next tick won't re-select this
          // table for the same mutations. NOTE: this and the local `active_builds`
          // set are only fast-path dedup; they do NOT prevent a duplicate build on
          // their own. Between here and the coroutine registering in `active_builds`
          // (process_table step 6, after the lock round-trip yields), a fast re-scan
          // with fresh mutations could spawn a second coroutine for this same table.
          // The AUTHORITATIVE dedup — across that window and across RGW instances —
          // is the distributed lock (try_acquire_lock); the loser returns SKIPPED and
          // restores its counts. Do not rely on active_builds for correctness.
          state.insert_count.fetch_sub(inserts, std::memory_order_relaxed);
          state.delete_count.fetch_sub(deletes, std::memory_order_relaxed);
          perf_counters_->set(l_rgw_s3v_bg_rebuilds_active,
                              active_rebuild_count.fetch_add(1, std::memory_order_relaxed) + 1);

          ldpp_dout(this, 1) << "INFO: spawning rebuild coroutine for "
              << name.bucket << "." << name.index
              << " (active_rebuilds=" << active_rebuild_count.load(std::memory_order_relaxed)
              << "/" << max_concurrent
              << ", inserts=" << inserts
              << ", deletes=" << deletes << ")" << dendl;

          record_event(rebuild_event_t::Type::SPAWN, name.tenant, name.bucket, name.index,
                       active_rebuild_count.load(std::memory_order_relaxed),
                       max_concurrent);

          boost::asio::spawn(make_strand(io_context), std::allocator_arg, make_stack_allocator(),
              [this, table_name = name, inserts, deletes, max_concurrent](boost::asio::yield_context yield) {
            const auto build_start = ceph::coarse_real_clock::now();
            const int rc = process_table(table_name, inserts, deletes, yield);
            if (rc == 0) {
              std::shared_lock sl(tables_mutex);//short time lock to update last_rebuild_time after successful rebuild
              auto it = tables.find(table_name);
              if (it != tables.end()) {
                it->second.last_rebuild_time = ceph::coarse_real_clock::now();
              }
            } else if (rc < 0) {
              ldpp_dout(this, 1) << "ERROR: failed to process table: " << table_name.bucket
                  << "." << table_name.index << " with error code: " << rc << dendl;
            }
            ldpp_dout(this, 1) << "INFO: rebuild coroutine finished for "
                << table_name.bucket << "." << table_name.index
                << " (active_rebuilds=" << active_rebuild_count.load(std::memory_order_relaxed)
                << ", rc=" << rc << ")" << dendl;
            const int duration_ms = static_cast<int>(
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    ceph::coarse_real_clock::now() - build_start).count());
            const auto result = (rc == 0) ? rebuild_event_t::Result::SUCCESS
                              : (rc < 0)  ? rebuild_event_t::Result::FAILURE
                                          : rebuild_event_t::Result::SKIPPED;
            record_event(rebuild_event_t::Type::FINISH, table_name.tenant,
                         table_name.bucket, table_name.index,
                         active_rebuild_count.load(std::memory_order_relaxed),
                         max_concurrent, duration_ms, result);
          }, [this, table_name = name, inserts, deletes] (std::exception_ptr eptr) {
            if (eptr) {
              try {
                std::rethrow_exception(eptr);
              } catch (const std::exception& e) {
                ldpp_dout(this, 0) << "ERROR: rebuild coroutine exception for "
                    << table_name.bucket << "." << table_name.index
                    << ": " << e.what() << dendl;
              }
              restore_counters(table_name, inserts, deletes);
            }
            perf_counters_->set(l_rgw_s3v_bg_rebuilds_active,
                                active_rebuild_count.fetch_sub(1, std::memory_order_relaxed) - 1);
          });
        }
      }// end of scan tables for pending mutations

      // 3. refresh distributed lock timestamps for active builds
      refresh_active_locks(yield);

      async_sleep(yield, idle_sleep);
    }
    ldpp_dout(this, 5) << "INFO: manager stopped. done processing all table and session operations" << dendl;
  }
 
public:

  ~Manager() {
    if (perf_counters_) {
      cct->get_perfcounters_collection()->remove(perf_counters_);
      delete perf_counters_;
      perf_counters_ = nullptr;
    }
    messages.consume_all([](auto message) {
      std::unique_ptr<message_t> message_guard(message);
    });
  }

  void stop() {
    ldpp_dout(this, 5) << "INFO: manager received stop signal. shutting down..." << dendl;
    shutdown = true;
    work_guard.reset();
    main_loop_work_guard.reset();

    // Stop the dedicated main-loop thread first: once it exits it spawns no new
    // builds and stops refreshing locks. In-flight builds keep running on the
    // worker pool and are drained below.
    if (main_loop_thread.joinable()) {
      auto future = std::async(std::launch::async, [this]() { main_loop_thread.join(); });
      if (future.wait_for(idle_sleep*2) == std::future_status::timeout) {
        if (!main_loop_io_context.stopped()) {
          ldpp_dout(this, 5) << "INFO: force shutdown of main loop" << dendl;
          main_loop_io_context.stop();
        }
        future.wait();
      }
    }

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
    // cache the daemon identity once so every status report is unambiguously
    // scoped to this instance. host_id is "<instance_id>-<zone>-<zonegroup>";
    // instance_id is the monitor-assigned global_id (leading, dash-free field).
    host_id_ = driver->get_host_id();
    const auto dash = host_id_.find('-');
    instance_id_ = (dash == std::string::npos) ? host_id_ : host_id_.substr(0, dash);

    // Launch the main loop FIRST, before the build-worker pool, on its own
    // dedicated thread (main_loop_io_context, run by a single thread). This is the
    // one coroutine that scans for rebuilds and — critically — refreshes the
    // distributed locks; giving it a private thread means lock refresh can never be
    // starved by the synchronous LanceDB builds that occupy the worker pool below.
    boost::asio::spawn(make_strand(main_loop_io_context), std::allocator_arg, make_stack_allocator(),
        [this](boost::asio::yield_context yield) {
          process_tables(yield);
        }, [] (std::exception_ptr eptr) {
          if (eptr) std::rethrow_exception(eptr);
        });
    main_loop_thread = std::thread([this]() {
      ceph_pthread_setname("s3v-mainloop");
      try {
        ldpp_dout(this, 10) << "INFO: main loop thread started" << dendl;
        main_loop_io_context.run();
        ldpp_dout(this, 10) << "INFO: main loop thread ended" << dendl;
      } catch (const std::exception& err) {
        ldpp_dout(this, 1) << "ERROR: main loop thread failed with error: " << err.what() << dendl;
        throw err;
      }
    });

    // Control-message / session loop: runs on the build-worker pool (below), so its
    // blocking session FFI (SESSION_CREATE/DELETE) never stalls the main loop's lock
    // refresh. A single coroutine keeps all control messages ordered.
    boost::asio::spawn(make_strand(io_context), std::allocator_arg, make_stack_allocator(),
        [this](boost::asio::yield_context yield) {
          process_messages(yield);
        }, [] (std::exception_ptr eptr) {
          if (eptr) std::rethrow_exception(eptr);
        });

    // Build-worker pool: runs the per-table build coroutines (process_table) — which
    // block their thread for the whole synchronous LanceDB FFI build — plus the
    // single process_messages coroutine spawned above. The main loop has its own
    // dedicated thread, so these workers no longer need to leave one free for lock
    // refresh; the floor is therefore 1. For full build parallelism, size the pool
    // to rgw_s3vector_max_concurrent_rebuilds — a smaller pool just serializes some
    // builds (and may briefly defer session processing behind a build), it is not a
    // correctness issue.
    const int configured_workers =
        static_cast<int>(cct->_conf.get_val<int64_t>("rgw_s3vector_background_workers"));
    const int num_workers = std::max(1, configured_workers);
    if (configured_workers < 1) {
      ldpp_dout(this, 0) << "WARNING: rgw_s3vector_background_workers="
          << configured_workers << " is below the required minimum of 1; "
          << "using " << num_workers << dendl;
    }
    for (int i = 0; i < num_workers; ++i) {
      workers.emplace_back(std::thread([this, i]() {
        const auto name = fmt::format("s3v-worker-{}", i);
        ceph_pthread_setname(name.c_str());
        try {
          ldpp_dout(this, 10) << "INFO: worker " << i << " started" << dendl;
          io_context.run();
          ldpp_dout(this, 10) << "INFO: worker " << i << " ended" << dendl;
        } catch (const std::exception& err) {
          ldpp_dout(this, 1) << "ERROR: worker " << i << " failed with error: " << err.what() << dendl;
          throw err;
        }
      }));
    }
    ldpp_dout(this, 10) << "INFO: started manager with 1 dedicated main-loop thread + "
        << num_workers << " build-worker threads" << dendl;
  }

  bool notify_index(const DoutPrefixProvider* dpp, const std::string& tenant, const std::string& bucket_name,
      const std::string& index_name, message_t::Op op) {
    if (shutdown) {
      ldpp_dout(dpp, 1) << "ERROR: failed to notify s3vectors manager about index: manager is shutting down" << dendl;
      return false;
    }
    auto message_guard = std::make_unique<message_t>(tenant, bucket_name, index_name, op);
    if (messages.push(message_guard.get())) {
      std::ignore = message_guard.release(); // ownership transferred to the queue
      ldpp_dout(dpp, 20) << "INFO: notified s3vectors manager about index" << dendl;
      return true;
    }
    ldpp_dout(dpp, 1) << "ERROR: failed to notify s3vectors manager about index: queue is full" << dendl;
    return false;
  }

  bool notify_index_mutation(const DoutPrefixProvider* dpp,
                             const std::string& tenant,
                             const std::string& bucket_name,
                             const std::string& index_name,
                             uint64_t row_count,
                             bool is_delete) {
    if (shutdown) {
      ldpp_dout(dpp, 1) << "ERROR: failed to notify s3vectors manager about index mutation: manager is shutting down" << dendl;
      return false;
    }
    const table_name_t table_name{tenant, bucket_name, index_name};

    {
      std::shared_lock sl(tables_mutex);
      auto it = tables.find(table_name);
      if (it != tables.end()) {
        if (is_delete) {
          it->second.delete_count.fetch_add(row_count, std::memory_order_relaxed);
        } else {
          it->second.insert_count.fetch_add(row_count, std::memory_order_relaxed);
        }
        ldpp_dout(dpp, 20) << "INFO: incremented " << (is_delete ? "delete" : "insert")
            << " counter by " << row_count << " for " << tenant << "/" << bucket_name << "." << index_name << dendl;
        return true;
      }
    }

    {
      std::unique_lock ul(tables_mutex);//exclusive lock to create a new entry in the tables map if it doesn't exist
      auto [it, inserted] = tables.emplace(table_name, table_state_t{});
      if (is_delete) {
        it->second.delete_count.fetch_add(row_count, std::memory_order_relaxed);
      } else {
        it->second.insert_count.fetch_add(row_count, std::memory_order_relaxed);
      }
      ldpp_dout(dpp, 20) << "INFO: " << (inserted ? "created entry and incremented" : "incremented")
          << " " << (is_delete ? "delete" : "insert")
          << " counter by " << row_count << " for " << tenant << "/" << bucket_name << "." << index_name << dendl;
    }
    return true;
  }

  bool notify_session(const DoutPrefixProvider* dpp, const std::string& tenant, const std::string& bucket_name, message_t::Op op) {
    if (shutdown) {
      ldpp_dout(dpp, 1) << "ERROR: failed to notify s3vectors manager about session: manager is shutting down" << dendl;
      return false;
    }
    auto message_guard = std::make_unique<message_t>(tenant, bucket_name, "", op);
    if (messages.push(message_guard.get())) {
      std::ignore = message_guard.release(); // ownership transferred to the queue
      ldpp_dout(dpp, 20) << "INFO: notified s3vectors manager about session" << dendl;
      return true;
    }
    ldpp_dout(dpp, 1) << "ERROR: failed to notify s3vectors manager about session: queue is full" << dendl;
    return false;
  }

  std::shared_ptr<const LanceDBSession> get_session(const std::string& tenant, const std::string& bucket_name) {
    std::shared_lock l(sessions_mutex);
    auto it = sessions.find(session_name_t(tenant, bucket_name));
    if (it == sessions.end()) {
      return nullptr;
    }
    return it->second;
  }

  // Snapshot of this instance's rebuild status + aggregate counters.
  background_status_t get_background_status() {
    background_status_t st;
    st.instance_id = instance_id_;
    st.host_id = host_id_;
    st.active_rebuilds = active_rebuild_count.load(std::memory_order_relaxed);
    st.max_concurrent_rebuilds =
        cct->_conf.get_val<int64_t>("rgw_s3vector_max_concurrent_rebuilds");
    st.num_workers = static_cast<int>(workers.size());
    {
      std::shared_lock sl(tables_mutex);
      st.tables_tracked = static_cast<int>(tables.size());
    }
    // aggregate counters are exposed via PerfCounters (see the perf collection),
    // not in this admin reply.
    {
      std::lock_guard lg(active_builds_mutex);
      for (const auto& [name, lock] : active_locks) {
        active_build_info_t info;
        info.tenant = name.tenant;
        info.bucket = name.bucket;
        info.index = name.index;
        info.start_time = lock.start_time;
        info.lock_refreshes = lock.refresh_count;
        st.active_builds_list.push_back(std::move(info));
      }
    }
    return st;
  }

  // Return recorded events, optionally filtered by timestamp and bucket.
  std::vector<rebuild_event_info_t> get_rebuild_events(uint64_t since_epoch,
                                                       const std::string& tenant_filter,
                                                       const std::string& bucket_filter) {
    std::vector<rebuild_event_info_t> out;
    std::lock_guard lg(event_log_mutex_);
    out.reserve(event_log_.size());
    for (const auto& e : event_log_) {
      if (since_epoch > 0) {
        const auto ev_epoch = static_cast<uint64_t>(
            ceph::coarse_real_clock::to_time_t(e.timestamp));
        if (ev_epoch < since_epoch) continue;
      }
      if (!tenant_filter.empty() && e.tenant != tenant_filter) continue;
      if (!bucket_filter.empty() && e.bucket != bucket_filter) continue;
      rebuild_event_info_t info;
      info.type = event_type_to_str(e.type);
      info.timestamp = e.timestamp;
      info.tenant = e.tenant;
      info.bucket = e.bucket;
      info.index = e.index;
      info.active_rebuilds = e.active_rebuilds;
      info.max_concurrent = e.max_concurrent;
      info.duration_ms = e.duration_ms;
      info.result = event_result_to_str(e.result);
      out.push_back(std::move(info));
    }
    return out;
  }

  Manager(CephContext* _cct, rgw::sal::Driver* _driver) :
    cct(_cct),
    work_guard(boost::asio::make_work_guard(io_context)),
    main_loop_work_guard(boost::asio::make_work_guard(main_loop_io_context)),
    driver(_driver),
    messages(8192)
  {
    PerfCountersBuilder pcb(cct, "rgw_s3vector_background",
                            l_rgw_s3v_bg_first, l_rgw_s3v_bg_last);
    pcb.set_prio_default(PerfCountersBuilder::PRIO_USEFUL);
    pcb.add_u64_counter(l_rgw_s3v_bg_rebuilds_started, "rebuilds_started",
                        "Background index rebuilds started");
    pcb.add_u64_counter(l_rgw_s3v_bg_rebuilds_completed, "rebuilds_completed",
                        "Background index rebuilds completed successfully");
    pcb.add_u64_counter(l_rgw_s3v_bg_rebuilds_failed, "rebuilds_failed",
                        "Background index rebuilds that failed");
    pcb.add_u64(l_rgw_s3v_bg_rebuilds_active, "rebuilds_active",
                "Background index rebuilds currently in flight");
    pcb.add_u64_counter(l_rgw_s3v_bg_limit_reached, "limit_reached",
                        "Scans that hit the max concurrent rebuild limit");
    pcb.add_u64_counter(l_rgw_s3v_bg_lock_refresh, "lock_refresh",
                        "Distributed lock refreshes during active builds");
    pcb.add_u64_counter(l_rgw_s3v_bg_lock_lost, "lock_lost",
                        "Distributed locks lost (stolen) during active builds");
    pcb.add_u64_counter(l_rgw_s3v_bg_lock_refresh_fail, "lock_refresh_fail",
                        "Distributed lock refresh failures (transient)");
    perf_counters_ = pcb.create_perf_counters();
    cct->get_perfcounters_collection()->add(perf_counters_);
  }
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

bool notify_index_update(const DoutPrefixProvider* dpp, const std::string& tenant, const std::string& bucket_name, const std::string& index_name, uint64_t row_count) {
  if (!s_manager) {
    ldpp_dout(dpp, 1) << "ERROR: failed to notify s3vectors manager about table update: manager is not initialized" << dendl;
    return false;
  }
  return s_manager->notify_index_mutation(dpp, tenant, bucket_name, index_name, row_count, false);
}

bool notify_index_delete(const DoutPrefixProvider* dpp, const std::string& tenant, const std::string& bucket_name, const std::string& index_name, uint64_t row_count) {
  if (!s_manager) {
    ldpp_dout(dpp, 1) << "ERROR: failed to notify s3vectors manager about table delete: manager is not initialized" << dendl;
    return false;
  }
  return s_manager->notify_index_mutation(dpp, tenant, bucket_name, index_name, row_count, true);
}

bool notify_index_remove(const DoutPrefixProvider* dpp, const std::string& tenant, const std::string& bucket_name, const std::string& index_name) {
  if (!s_manager) {
    ldpp_dout(dpp, 1) << "ERROR: failed to notify s3vectors manager about table remove: manager is not initialized" << dendl;
    return false;
  }
  return s_manager->notify_index(dpp, tenant, bucket_name, index_name, Manager::message_t::Op::REMOVE);
}

std::shared_ptr<const LanceDBSession> get_session(const DoutPrefixProvider* dpp, const std::string& tenant, const std::string& bucket_name) {
  if (!s_manager) {
    ldpp_dout(dpp, 1) << "ERROR: failed to get LanceDB session for bucket: manager is not initialized" << dendl;
    return nullptr;
  }
  return s_manager->get_session(tenant, bucket_name);
}

bool notify_session_create(const DoutPrefixProvider* dpp, const std::string& tenant, const std::string& bucket_name) {
  if (!s_manager) {
    ldpp_dout(dpp, 1) << "ERROR: failed to notify s3vectors manager about session creation: manager is not initialized" << dendl;
    return false; 
  }
  return s_manager->notify_session(dpp, tenant, bucket_name, Manager::message_t::Op::SESSION_CREATE);
}

bool notify_session_delete(const DoutPrefixProvider* dpp, const std::string& tenant, const std::string& bucket_name) {
  if (!s_manager) {
    ldpp_dout(dpp, 1) << "ERROR: failed to notify s3vectors manager about session deletion: manager is not initialized" << dendl;
    return false;
  }
  return s_manager->notify_session(dpp, tenant, bucket_name, Manager::message_t::Op::SESSION_DELETE);
}

background_status_t get_background_status() {
  if (!s_manager) {
    return {};
  }
  return s_manager->get_background_status();
}

std::vector<rebuild_event_info_t> get_rebuild_events(uint64_t since_epoch,
                                                     const std::string& tenant_filter,
                                                     const std::string& bucket_filter) {
  if (!s_manager) {
    return {};
  }
  return s_manager->get_rebuild_events(since_epoch, tenant_filter, bucket_filter);
}



} // namespace rgw::s3vector
