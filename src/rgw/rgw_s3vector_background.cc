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
#include "common/hostname.h"
#include <chrono>
#include <charconv>
#include <fmt/format.h>
#include "common/async/yield_waiter.h"
#include <future>
#include <string>
#include <unordered_map>
#include "rgw_sal.h"
#include "rgw_common.h"
#include "rgw_acl.h"
#include "rgw_s3vector.h"
#include "lancedb.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw::s3vector {

// table metadata key for build coordination state
static constexpr const char* build_state_metadata_key = "s3v_index_state";

// lock object key prefix/suffix within the vector bucket
static constexpr const char* lock_key_prefix = ".s3v-lock-";
static constexpr const char* lock_key_suffix = ".lock";

// vector data column name used for vector index
static constexpr const char* vector_data_column = "data";

struct build_state_t {
  bool build_in_progress = false;
  int64_t build_started_at = 0;
  int64_t build_lease_seconds = 600;
  std::string builder_id;

  bool is_lease_expired() const {
    if (!build_in_progress || build_started_at == 0) return true;
    auto now = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    return (now - build_started_at) > build_lease_seconds;
  }

  std::string to_json() const {
    return fmt::format(
        R"({{"build_in_progress":{},"build_started_at":{},"build_lease_seconds":{},"builder_id":"{}"}})",
        build_in_progress ? "true" : "false",
        build_started_at,
        build_lease_seconds,
        builder_id);
  }

  static build_state_t from_json(const std::string& json_str) {
    build_state_t state;
    // simple field extraction without a JSON library
    auto extract_bool = [&](const char* key) -> bool {
      auto pos = json_str.find(key);
      if (pos == std::string::npos) return false;
      pos = json_str.find(':', pos);
      if (pos == std::string::npos) return false;
      return json_str.find("true", pos) < json_str.find(',', pos);
    };
    auto extract_int64 = [&](const char* key) -> int64_t {
      auto pos = json_str.find(key);
      if (pos == std::string::npos) return 0;
      pos = json_str.find(':', pos);
      if (pos == std::string::npos) return 0;
      ++pos;
      while (pos < json_str.size() && json_str[pos] == ' ') ++pos;
      int64_t val = 0;
      std::from_chars(json_str.data() + pos,
                      json_str.data() + json_str.size(), val);
      return val;
    };
    auto extract_string = [&](const char* key) -> std::string {
      auto pos = json_str.find(key);
      if (pos == std::string::npos) return {};
      pos = json_str.find('"', pos + strlen(key) + 2); // skip key + ":
      if (pos == std::string::npos) return {};
      ++pos; // skip opening quote
      auto end = json_str.find('"', pos);
      if (end == std::string::npos) return {};
      return json_str.substr(pos, end - pos);
    };

    state.build_in_progress = extract_bool("build_in_progress");
    state.build_started_at = extract_int64("build_started_at");
    state.build_lease_seconds = extract_int64("build_lease_seconds");
    state.builder_id = extract_string("builder_id");
    return state;
  }
};

class Manager : public DoutPrefixProvider {
public:
    using table_name_t = std::pair<std::string, std::string>; // pair of vector bucket name and index name
    struct message_t {
      enum class Op {
        UPDATE,
        REMOVE
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
  std::unordered_map<table_name_t, ceph::coarse_real_time, boost::hash<table_name_t>> tables;
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

  build_state_t read_build_state(const LanceDBTable* table) {
    const char* key = build_state_metadata_key;
    char** keys_out = nullptr;
    char** values_out = nullptr;
    size_t count = 0;
    char* error_message = nullptr;

    if (const auto result = lancedb_table_get_metadata(
            table, &key, 1, &keys_out, &values_out, &count, &error_message);
        result != LANCEDB_SUCCESS) {
      ldpp_dout(this, 5) << "WARNING: failed to read build state from table metadata: "
          << (error_message ? error_message : "unknown") << dendl;
      lancedb_free_string(error_message);
      return {};
    }

    build_state_t state;
    if (count > 0 && values_out[0]) {
      state = build_state_t::from_json(values_out[0]);
    }
    lancedb_free_metadata(keys_out, values_out, count);
    return state;
  }

  int write_build_state(const LanceDBTable* table, const build_state_t& state) {
    const std::string json_str = state.to_json();
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
>>>>>>> 08f3a7a1fa8 (Background vector-index rebuild for S3 Vector tables. When put-vector or delete-vector operations cause the number of unindexed rows to exceed a configurable)
    return 0;
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

    // list all indices to find the vector index name
    char** indices = nullptr;
    size_t index_count = 0;
    char* error_message = nullptr;

    if (const auto err = lancedb_table_list_indices(
            table, &indices, &index_count, &error_message);
        err != LANCEDB_SUCCESS) {
      ldpp_dout(this, 1) << "ERROR: failed to list indices: "
          << (error_message ? error_message : "unknown") << dendl;
      lancedb_free_string(error_message);
      return result;
    }

    if (index_count == 0) {
      // no index exists yet — all rows are unindexed
      result.stats.num_indexed_rows = 0;
      result.stats.num_unindexed_rows = lancedb_table_count_rows(table);
      result.stats.num_indices = 0;
      result.ok = true;
      return result;
    }

    // try each index name to find a vector index with stats
    bool found = false;
    for (size_t i = 0; i < index_count && !found; ++i) {
      error_message = nullptr;
      if (const auto err = lancedb_table_index_stats(
              table, indices[i], &result.stats, &error_message);
          err == LANCEDB_SUCCESS) {
        found = true;
        result.ok = true;
      } else {
        lancedb_free_string(error_message);
      }
    }

    lancedb_free_index_list(indices, index_count);

    if (!found) {
      // indices exist but none have stats — treat all rows as unindexed
      result.stats.num_indexed_rows = 0;
      result.stats.num_unindexed_rows = lancedb_table_count_rows(table);
      result.stats.num_indices = 0;
      result.ok = true;
    }

    return result;
  }

  // ============================================================================
  // Distance metric conversion
  // ============================================================================

  static LanceDBDistanceType to_lancedb_distance(DistanceMetric metric) {
    switch (metric) {
      case DistanceMetric::COSINE: return LANCEDB_DISTANCE_COSINE;
      case DistanceMetric::EUCLIDEAN: return LANCEDB_DISTANCE_L2;
      default: return LANCEDB_DISTANCE_L2;
    }
  }

  // ============================================================================
  // Distributed lock via S3 conditional write (SAL)
  // ============================================================================

  static std::string make_lock_key(const std::string& index_name) {
    return std::string(lock_key_prefix) + index_name + lock_key_suffix;
  }

  std::string make_lock_body() {
    auto now = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    return fmt::format(R"({{"builder_id":"{}","timestamp":{}}})",
        builder_id(), now);
  }

  std::string builder_id() {
    return fmt::format("{}-{}", ceph_get_short_hostname(), getpid());
  }

  int64_t parse_lock_timestamp(const std::string& body) {
    auto pos = body.find("\"timestamp\":");
    if (pos == std::string::npos) return 0;
    pos += 12; // length of "\"timestamp\":"
    int64_t ts = 0;
    std::from_chars(body.data() + pos, body.data() + body.size(), ts);
    return ts;
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

  // PUT lock object with if-none-match="*" (fails if already exists)
  int put_lock_object(const std::string& vector_bucket_name,
                      const std::string& lock_key,
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

    std::string body = make_lock_body();
    bufferlist bl;
    bl.append(body);
    ret = writer->process(std::move(bl), 0);
    if (ret < 0) return ret;

    // flush
    ret = writer->process({}, body.size());
    if (ret < 0) return ret;

    std::map<std::string, bufferlist> attrs;
    const req_context rctx{this, y, nullptr};
    bool canceled = false;

    ret = writer->complete(body.size(), /*etag=*/"",
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

  // DELETE lock object
  int delete_lock_object(const std::string& vector_bucket_name,
                         const std::string& lock_key,
                         optional_yield y) {
    std::unique_ptr<rgw::sal::Bucket> bucket;
    int ret = load_bucket_for_lock(vector_bucket_name, bucket, y);
    if (ret < 0) return ret;

    auto obj = bucket->get_object({lock_key});
    return obj->delete_object(this, y, 0, nullptr, nullptr);
  }

  // GET lock object body
  std::pair<int, std::string> get_lock_object(const std::string& vector_bucket_name,
                                              const std::string& lock_key,
                                              optional_yield y) {
    std::unique_ptr<rgw::sal::Bucket> bucket;
    int ret = load_bucket_for_lock(vector_bucket_name, bucket, y);
    if (ret < 0) return {ret, {}};

    auto obj = bucket->get_object({lock_key});
    auto read_op = obj->get_read_op();
    ret = read_op->prepare(y, this);
    if (ret < 0) return {ret, {}};

    bufferlist bl;
    ret = read_op->read(0, obj->get_size() - 1, bl, y, this);
    if (ret < 0) return {ret, {}};
    return {0, bl.to_str()};
  }

  // Check if lock is stale and reclaim it
  bool check_stale_and_reclaim(const std::string& bucket_name,
                               const std::string& lock_key,
                               optional_yield y) {
    auto [ret, body] = get_lock_object(bucket_name, lock_key, y);
    if (ret < 0) return false;

    int64_t lock_ts = parse_lock_timestamp(body);
    if (lock_ts == 0) return false;

    auto now = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    int64_t lock_ttl = cct->_conf.get_val<uint64_t>("rgw_s3vector_index_lock_ttl_seconds");
    int64_t age = now - lock_ts;

    if (age < lock_ttl) {
      return false; // lock is still fresh
    }

    ldpp_dout(this, 5) << "INFO: reclaiming stale lock (age=" << age
        << "s, ttl=" << lock_ttl << "s)" << dendl;
    delete_lock_object(bucket_name, lock_key, y);
    return put_lock_object(bucket_name, lock_key, y) == 0;
  }

  // try to acquire the distributed lock (non-blocking)
  bool try_acquire_lock(const std::string& bucket_name,
                        const std::string& index_name,
                        optional_yield y) {
    const std::string lock_key = make_lock_key(index_name);
    int ret = put_lock_object(bucket_name, lock_key, y);
    if (ret == 0) {
      return true; // lock acquired
    }

    if (ret == -ERR_PRECONDITION_FAILED) {
      // lock exists — check if it's stale
      return check_stale_and_reclaim(bucket_name, lock_key, y);
    }

    ldpp_dout(this, 1) << "ERROR: failed to acquire lock for " << bucket_name
        << "." << index_name << " ret=" << ret << dendl;
    return false;
  }

  void release_lock(const std::string& bucket_name,
                    const std::string& index_name,
                    optional_yield y) {
    const std::string lock_key = make_lock_key(index_name);
    int ret = delete_lock_object(bucket_name, lock_key, y);
    if (ret < 0 && ret != -ENOENT) {
      ldpp_dout(this, 1) << "ERROR: failed to release lock for " << bucket_name
          << "." << index_name << " ret=" << ret << dendl;
    }
  }

  // ============================================================================
  // Vector index build
  // ============================================================================

  int run_vector_index_build(LanceDBTable* table, LanceDBDistanceType distance_type) {
    LanceDBVectorIndexConfig vec_config = {};
    vec_config.num_partitions = -1;    // auto
    vec_config.num_sub_vectors = -1;   // auto
    vec_config.max_iterations = -1;    // default
    vec_config.sample_rate = 0.0f;     // default
    vec_config.distance_type = distance_type;
    vec_config.accelerator = nullptr;  // CPU
    vec_config.replace = 1;            // replace existing index

    const char* columns[] = {vector_data_column};
    char* error_message = nullptr;

    const LanceDBError result = lancedb_table_create_vector_index(
        table, columns, 1, LANCEDB_INDEX_AUTO, &vec_config, &error_message);

    if (result != LANCEDB_SUCCESS) {
      ldpp_dout(this, 1) << "ERROR: vector index build failed: "
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

    // step 1: read config
    const uint64_t threshold = cct->_conf.get_val<uint64_t>("rgw_s3vector_index_unindexed_threshold");
    if (threshold == 0) {
      ldpp_dout(this, 20) << "INFO: automatic index rebuild disabled for "
          << bucket_name << "." << index_name << dendl;
      return 0;
    }

    // step 2: open table
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

    // step 3: get index stats
    auto stats_result = get_vector_index_stats(table);
    if (!stats_result.ok) {
      ldpp_dout(this, 1) << "ERROR: failed to get index stats for "
          << bucket_name << "." << index_name << dendl;
      return -EIO;
    }
    const auto& stats = stats_result.stats;

    ldpp_dout(this, 5) << "INFO: index stats for " << bucket_name << "." << index_name
        << ": indexed=" << stats.num_indexed_rows
        << " unindexed=" << stats.num_unindexed_rows
        << " num_indices=" << stats.num_indices << dendl;

    if (stats.num_unindexed_rows < threshold) {
      ldpp_dout(this, 20) << "INFO: " << bucket_name << "." << index_name
          << " below threshold (" << stats.num_unindexed_rows
          << " < " << threshold << "), skipping rebuild" << dendl;
      return 0;
    }

    // step 4: try to acquire distributed lock
    optional_yield y(yield);
    if (!try_acquire_lock(bucket_name, index_name, y)) {
      ldpp_dout(this, 5) << "INFO: lock held by another process for "
          << bucket_name << "." << index_name << ", skipping rebuild" << dendl;
      return 0;
    }

    // scope guard for lock release
    struct lock_guard_t {
      Manager* mgr;
      const std::string& bucket_name;
      const std::string& index_name;
      optional_yield y;
      ~lock_guard_t() {
        mgr->release_lock(bucket_name, index_name, y);
      }
    } lock_guard{this, bucket_name, index_name, y};

    // step 5: read build state from table metadata (double-check after lock)
    build_state_t state = read_build_state(table);
    if (state.build_in_progress) {
      if (!state.is_lease_expired()) {
        ldpp_dout(this, 5) << "INFO: build already in progress for "
            << bucket_name << "." << index_name
            << " by " << state.builder_id
            << " (started at " << state.build_started_at << "), skipping" << dendl;
        return 0;
      }
      ldpp_dout(this, 5) << "INFO: detected stale build for "
          << bucket_name << "." << index_name
          << " by " << state.builder_id
          << " (lease expired), resetting state" << dendl;
    }

    // step 6: mark build started
    const uint64_t build_lease = cct->_conf.get_val<uint64_t>("rgw_s3vector_index_build_lease_seconds");
    state.build_in_progress = true;
    state.build_started_at = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    state.build_lease_seconds = build_lease;
    state.builder_id = builder_id();
    write_build_state(table, state);

    // step 7: get distance metric for index config
    DistanceMetric metric = s3vector::get_table_distance_metric(table, this);
    LanceDBDistanceType distance_type = to_lancedb_distance(metric);

    // step 8: run the vector index build
    ldpp_dout(this, 1) << "INFO: starting vector index build for "
        << bucket_name << "." << index_name
        << " (unindexed=" << stats.num_unindexed_rows
        << ", threshold=" << threshold << ")" << dendl;

    int build_ret = run_vector_index_build(table, distance_type);

    // step 9: mark build complete (always, even on failure)
    state = read_build_state(table);
    state.build_in_progress = false;
    state.build_started_at = 0;
    write_build_state(table, state);

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
        if (message->type == message_t::Op::REMOVE) {
          ldpp_dout(this, 20) << "INFO: received remove message for table: " << table_name.first << "." << table_name.second << dendl;
          tables.erase(table_name);
          return;
        }
        auto [it, inserted] = tables.emplace(table_name, ceph::coarse_real_clock::now());
        if (inserted) {
          ldpp_dout(this, 20) << "INFO: will try to process new table: " << table_name.first << "." << table_name.second << dendl;
          tables_to_process.push_back(table_name);
          return;
        }
        const auto now = ceph::coarse_real_clock::now();
        const auto time_since_last_process = now - it->second;
        if (time_since_last_process > std::chrono::milliseconds(stats_interval_ms)) {
          ldpp_dout(this, 20) << "INFO: will try to process table: " << table_name.first << "." << table_name.second <<
          ". " << time_since_last_process << " passed since last processing" << dendl;
          it->second = now;
          tables_to_process.push_back(table_name);
        } else {
          ldpp_dout(this, 20) << "INFO: will skip processing table: " << table_name.first << "." << table_name.second <<
          ". only " << time_since_last_process << " passed since last processing" << dendl;
        }
      });
      tokens_waiter tw(this);
      for (const auto& table_name : tables_to_process) {
        // start processing a table
        tokens_waiter::token token(&tw);
        boost::asio::spawn(make_strand(io_context), std::allocator_arg, make_stack_allocator(),
            [this, token = std::move(token), table_name](boost::asio::yield_context yield) {
          const int rc = process_table(table_name, yield);
          if (rc < 0) {
            ldpp_dout(this, 1) << "ERROR: failed to process table: " << table_name.first << "." << table_name.second << " with error code: " << rc << dendl;
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
        ldpp_dout(this, 20) << "INFO: no tables to process" << dendl;
        async_sleep(yield, idle_sleep);
      }
    }
    ldpp_dout(this, 5) << "INFO: manager stopped. done processing all tables" << dendl;
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

} // namespace rgw::s3vector
