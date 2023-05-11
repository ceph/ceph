// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <fmt/format.h>
#include <fmt/ostream.h>

#include "include/encoding.h"

#include "common/ceph_json.h"
#include "common/likely.h"

#include "rgw_coroutine.h"
#include "rgw_cr_rados.h"
#include "rgw_http_client.h"
#include "rgw_sal_rados.h"

#include "rgw_datalog.h"
#include "rgw_sync.h"
#include "rgw_sync_module.h"
#include "rgw_sync_trace.h"
#include "rgw_sync_policy.h"

#include "rgw_bucket_sync.h"
#include "sync_fairness.h"

// represents an obligation to sync an entry up a given time
struct rgw_data_sync_obligation {
  rgw_bucket_shard bs;
  std::optional<uint64_t> gen;
  std::string marker;
  ceph::real_time timestamp;
  bool retry = false;
};

inline std::ostream& operator<<(std::ostream& out, const rgw_data_sync_obligation& o) {
  out << "key=" << o.bs;
  if (o.gen) {
    out << '[' << *o.gen << ']';
  }
  if (!o.marker.empty()) {
    out << " marker=" << o.marker;
  }
  if (o.timestamp != ceph::real_time{}) {
    out << " timestamp=" << o.timestamp;
  }
  if (o.retry) {
    out << " retry";
  }
  return out;
}

class JSONObj;
struct rgw_sync_bucket_pipe;

struct rgw_bucket_sync_pair_info {
  RGWBucketSyncFlowManager::pipe_handler handler; /* responsible for sync filters */
  rgw_bucket_shard source_bs;
  rgw_bucket dest_bucket;
};

inline std::ostream& operator<<(std::ostream& out, const rgw_bucket_sync_pair_info& p) {
  if (p.source_bs.bucket == p.dest_bucket) {
    return out << p.source_bs;
  }
  return out << p.source_bs << "->" << p.dest_bucket;
}

struct rgw_bucket_sync_pipe {
  rgw_bucket_sync_pair_info info;
  RGWBucketInfo source_bucket_info;
  std::map<std::string, bufferlist> source_bucket_attrs;
  RGWBucketInfo dest_bucket_info;
  std::map<std::string, bufferlist> dest_bucket_attrs;

  RGWBucketSyncFlowManager::pipe_rules_ref& get_rules() {
    return info.handler.rules;
  }
};

inline std::ostream& operator<<(std::ostream& out, const rgw_bucket_sync_pipe& p) {
  return out << p.info;
}

struct rgw_datalog_info {
  uint32_t num_shards;

  rgw_datalog_info() : num_shards(0) {}

  void decode_json(JSONObj *obj);
};

struct rgw_data_sync_info {
  enum SyncState {
    StateInit = 0,
    StateBuildingFullSyncMaps = 1,
    StateSync = 2,
  };

  uint16_t state;
  uint32_t num_shards;

  uint64_t instance_id{0};

  void encode(bufferlist& bl) const {
    ENCODE_START(2, 1, bl);
    encode(state, bl);
    encode(num_shards, bl);
    encode(instance_id, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
     DECODE_START(2, bl);
     decode(state, bl);
     decode(num_shards, bl);
     if (struct_v >= 2) {
       decode(instance_id, bl);
     }
     DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const {
    std::string s;
    switch ((SyncState)state) {
      case StateInit:
	s = "init";
	break;
      case StateBuildingFullSyncMaps:
	s = "building-full-sync-maps";
	break;
      case StateSync:
	s = "sync";
	break;
      default:
	s = "unknown";
	break;
    }
    encode_json("status", s, f);
    encode_json("num_shards", num_shards, f);
    encode_json("instance_id", instance_id, f);
  }
  void decode_json(JSONObj *obj) {
    std::string s;
    JSONDecoder::decode_json("status", s, obj);
    if (s == "building-full-sync-maps") {
      state = StateBuildingFullSyncMaps;
    } else if (s == "sync") {
      state = StateSync;
    } else {
      state = StateInit;
    }
    JSONDecoder::decode_json("num_shards", num_shards, obj);
    JSONDecoder::decode_json("instance_id", instance_id, obj);
  }
  static void generate_test_instances(std::list<rgw_data_sync_info*>& o);

  rgw_data_sync_info() : state((int)StateInit), num_shards(0) {}
};
WRITE_CLASS_ENCODER(rgw_data_sync_info)

struct rgw_data_sync_marker {
  enum SyncState {
    FullSync = 0,
    IncrementalSync = 1,
  };
  uint16_t state;
  std::string marker;
  std::string next_step_marker;
  uint64_t total_entries;
  uint64_t pos;
  real_time timestamp;

  rgw_data_sync_marker() : state(FullSync), total_entries(0), pos(0) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(state, bl);
    encode(marker, bl);
    encode(next_step_marker, bl);
    encode(total_entries, bl);
    encode(pos, bl);
    encode(timestamp, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
     DECODE_START(1, bl);
    decode(state, bl);
    decode(marker, bl);
    decode(next_step_marker, bl);
    decode(total_entries, bl);
    decode(pos, bl);
    decode(timestamp, bl);
     DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const {
    const char *s{nullptr};
    switch ((SyncState)state) {
      case FullSync:
        s = "full-sync";
        break;
      case IncrementalSync:
        s = "incremental-sync";
        break;
      default:
        s = "unknown";
        break;
    }
    encode_json("status", s, f);
    encode_json("marker", marker, f);
    encode_json("next_step_marker", next_step_marker, f);
    encode_json("total_entries", total_entries, f);
    encode_json("pos", pos, f);
    encode_json("timestamp", utime_t(timestamp), f);
  }
  void decode_json(JSONObj *obj) {
    std::string s;
    JSONDecoder::decode_json("status", s, obj);
    if (s == "full-sync") {
      state = FullSync;
    } else if (s == "incremental-sync") {
      state = IncrementalSync;
    }
    JSONDecoder::decode_json("marker", marker, obj);
    JSONDecoder::decode_json("next_step_marker", next_step_marker, obj);
    JSONDecoder::decode_json("total_entries", total_entries, obj);
    JSONDecoder::decode_json("pos", pos, obj);
    utime_t t;
    JSONDecoder::decode_json("timestamp", t, obj);
    timestamp = t.to_real_time();
  }
  static void generate_test_instances(std::list<rgw_data_sync_marker*>& o);
};
WRITE_CLASS_ENCODER(rgw_data_sync_marker)

struct rgw_data_sync_status {
  rgw_data_sync_info sync_info;
  std::map<uint32_t, rgw_data_sync_marker> sync_markers;

  rgw_data_sync_status() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(sync_info, bl);
    /* sync markers are encoded separately */
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
     DECODE_START(1, bl);
    decode(sync_info, bl);
    /* sync markers are decoded separately */
     DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const {
    encode_json("info", sync_info, f);
    encode_json("markers", sync_markers, f);
  }
  void decode_json(JSONObj *obj) {
    JSONDecoder::decode_json("info", sync_info, obj);
    JSONDecoder::decode_json("markers", sync_markers, obj);
  }
  static void generate_test_instances(std::list<rgw_data_sync_status*>& o);
};
WRITE_CLASS_ENCODER(rgw_data_sync_status)

struct rgw_datalog_entry {
  std::string key;
  ceph::real_time timestamp;

  void decode_json(JSONObj *obj);
};

struct rgw_datalog_shard_data {
  std::string marker;
  bool truncated;
  std::vector<rgw_datalog_entry> entries;

  void decode_json(JSONObj *obj);
};

class RGWAsyncRadosProcessor;
class RGWDataSyncControlCR;

struct rgw_bucket_entry_owner {
  std::string id;
  std::string display_name;

  rgw_bucket_entry_owner() {}
  rgw_bucket_entry_owner(const std::string& _id, const std::string& _display_name) : id(_id), display_name(_display_name) {}

  void decode_json(JSONObj *obj);
};

class RGWSyncErrorLogger;
class RGWRESTConn;
class RGWServices;

struct RGWDataSyncEnv {
  const DoutPrefixProvider *dpp{nullptr};
  CephContext *cct{nullptr};
  rgw::sal::RadosStore* driver{nullptr};
  RGWServices *svc{nullptr};
  RGWAsyncRadosProcessor *async_rados{nullptr};
  RGWHTTPManager *http_manager{nullptr};
  RGWSyncErrorLogger *error_logger{nullptr};
  RGWSyncTraceManager *sync_tracer{nullptr};
  RGWSyncModuleInstanceRef sync_module{nullptr};
  PerfCounters* counters{nullptr};
  rgw::sync_fairness::BidManager* bid_manager{nullptr};

  RGWDataSyncEnv() {}

  void init(const DoutPrefixProvider *_dpp, CephContext *_cct, rgw::sal::RadosStore* _driver, RGWServices *_svc,
            RGWAsyncRadosProcessor *_async_rados, RGWHTTPManager *_http_manager,
            RGWSyncErrorLogger *_error_logger, RGWSyncTraceManager *_sync_tracer,
            RGWSyncModuleInstanceRef& _sync_module,
            PerfCounters* _counters) {
     dpp = _dpp;
    cct = _cct;
    driver = _driver;
    svc = _svc;
    async_rados = _async_rados;
    http_manager = _http_manager;
    error_logger = _error_logger;
    sync_tracer = _sync_tracer;
    sync_module = _sync_module;
    counters = _counters;
  }

  std::string shard_obj_name(int shard_id);
  std::string status_oid();

  std::ostream* ostr{nullptr}; // For pretty printing progress
};

// pretty ostream output for `radosgw-admin bucket sync run`
#if FMT_VERSION >= 90000
template<typename ...T>
void pretty_print(const RGWDataSyncEnv* env, fmt::format_string<T...> fmt, T&& ...t) {
#else
template<typename S, typename ...T>
void pretty_print(const RGWDataSyncEnv* env, const S& fmt, T&& ...t) {
#endif
  if (unlikely(!!env->ostr)) {
    fmt::print(*env->ostr, fmt, std::forward<T>(t)...);
    env->ostr->flush();
  }
}

/// \brief Adjust concurrency based on latency
///
/// Keep a running average of operation latency and scale concurrency
/// down when latency rises.
class LatencyConcurrencyControl : public LatencyMonitor {
  static constexpr auto dout_subsys = ceph_subsys_rgw;
  ceph::coarse_mono_time last_warning;
public:
  CephContext* cct;

  LatencyConcurrencyControl(CephContext* cct)
    : cct(cct)  {}

  /// \brief Lower concurrency when latency rises
  ///
  /// Since we have multiple spawn windows (data sync overall and
  /// bucket), accept a number of concurrent operations to spawn and,
  /// if latency is high, cut it in half. If latency is really high,
  /// cut it to 1.
  int64_t adj_concurrency(int64_t concurrency) {
    using namespace std::literals;
    auto threshold = (cct->_conf->rgw_sync_lease_period * 1s) / 12;

    if (avg_latency() >= 2 * threshold) [[unlikely]] {
      auto now = ceph::coarse_mono_clock::now();
      if (now - last_warning > 5min) {
        ldout(cct, -1)
            << "WARNING: The OSD cluster is overloaded and struggling to "
            << "complete ops. You need more capacity to serve this level "
	    << "of demand." << dendl;
	last_warning = now;
      }
      return 1;
    } else if (avg_latency() >= threshold) [[unlikely]] {
      return concurrency / 2;
    } else [[likely]] {
      return concurrency;
    }
  }
};

struct RGWDataSyncCtx {
  RGWDataSyncEnv *env{nullptr};
  CephContext *cct{nullptr};

  RGWRESTConn *conn{nullptr};
  rgw_zone_id source_zone;

  LatencyConcurrencyControl lcc{nullptr};

  RGWDataSyncCtx() = default;

  RGWDataSyncCtx(RGWDataSyncEnv* env,
		 RGWRESTConn* conn,
		 const rgw_zone_id& source_zone)
    : env(env), cct(env->cct), conn(conn), source_zone(source_zone), lcc(cct) {}

  void init(RGWDataSyncEnv *_env,
            RGWRESTConn *_conn,
            const rgw_zone_id& _source_zone) {
    cct = _env->cct;
    env = _env;
    conn = _conn;
    source_zone = _source_zone;
    lcc.cct = cct;
  }
};

class RGWRados;

class RGWRemoteDataLog : public RGWCoroutinesManager {
  const DoutPrefixProvider *dpp;
  rgw::sal::RadosStore* driver;
  CephContext *cct;
  RGWCoroutinesManagerRegistry *cr_registry;
  RGWAsyncRadosProcessor *async_rados;
  RGWHTTPManager http_manager;

  RGWDataSyncEnv sync_env;
  RGWDataSyncCtx sc;

  ceph::shared_mutex lock = ceph::make_shared_mutex("RGWRemoteDataLog::lock");
  RGWDataSyncControlCR *data_sync_cr;

  RGWSyncTraceNodeRef tn;

  bool initialized;

public:
  RGWRemoteDataLog(const DoutPrefixProvider *dpp,
                   rgw::sal::RadosStore* _store,
                   RGWAsyncRadosProcessor *async_rados);
  int init(const rgw_zone_id& _source_zone, RGWRESTConn *_conn, RGWSyncErrorLogger *_error_logger,
           RGWSyncTraceManager *_sync_tracer, RGWSyncModuleInstanceRef& module,
           PerfCounters* _counters);
  void finish();

  int read_log_info(const DoutPrefixProvider *dpp, rgw_datalog_info *log_info);
  int read_source_log_shards_info(const DoutPrefixProvider *dpp, std::map<int, RGWDataChangesLogInfo> *shards_info);
  int read_source_log_shards_next(const DoutPrefixProvider *dpp, std::map<int, std::string> shard_markers, std::map<int, rgw_datalog_shard_data> *result);
  int read_sync_status(const DoutPrefixProvider *dpp, rgw_data_sync_status *sync_status);
  int read_recovering_shards(const DoutPrefixProvider *dpp, const int num_shards, std::set<int>& recovering_shards);
  int read_shard_status(const DoutPrefixProvider *dpp, int shard_id, std::set<std::string>& lagging_buckets,std::set<std::string>& recovering_buckets, rgw_data_sync_marker* sync_marker, const int max_entries);
  int init_sync_status(const DoutPrefixProvider *dpp, int num_shards);
  int run_sync(const DoutPrefixProvider *dpp, int num_shards);

  void wakeup(int shard_id, bc::flat_set<rgw_data_notify_entry>& entries);
};

class RGWDataSyncStatusManager : public DoutPrefixProvider {
  rgw::sal::RadosStore* driver;

  rgw_zone_id source_zone;
  RGWRESTConn *conn;
  RGWSyncErrorLogger *error_logger;
  RGWSyncModuleInstanceRef sync_module;
  PerfCounters* counters;

  RGWRemoteDataLog source_log;

  std::string source_status_oid;
  std::string source_shard_status_oid_prefix;

  std::map<int, rgw_raw_obj> shard_objs;

  int num_shards;

public:
  RGWDataSyncStatusManager(rgw::sal::RadosStore* _driver, RGWAsyncRadosProcessor *async_rados,
                           const rgw_zone_id& _source_zone, PerfCounters* counters)
    : driver(_driver), source_zone(_source_zone), conn(NULL), error_logger(NULL),
      sync_module(nullptr), counters(counters),
      source_log(this, driver, async_rados), num_shards(0) {}
  RGWDataSyncStatusManager(rgw::sal::RadosStore* _driver, RGWAsyncRadosProcessor *async_rados,
                           const rgw_zone_id& _source_zone, PerfCounters* counters,
                           const RGWSyncModuleInstanceRef& _sync_module)
    : driver(_driver), source_zone(_source_zone), conn(NULL), error_logger(NULL),
      sync_module(_sync_module), counters(counters),
      source_log(this, driver, async_rados), num_shards(0) {}
  ~RGWDataSyncStatusManager() {
    finalize();
  }
  int init(const DoutPrefixProvider *dpp);
  void finalize();

  static std::string shard_obj_name(const rgw_zone_id& source_zone, int shard_id);
  static std::string sync_status_oid(const rgw_zone_id& source_zone);

  int read_sync_status(const DoutPrefixProvider *dpp, rgw_data_sync_status *sync_status) {
    return source_log.read_sync_status(dpp, sync_status);
  }

  int read_recovering_shards(const DoutPrefixProvider *dpp, const int num_shards, std::set<int>& recovering_shards) {
    return source_log.read_recovering_shards(dpp, num_shards, recovering_shards);
  }

  int read_shard_status(const DoutPrefixProvider *dpp, int shard_id, std::set<std::string>& lagging_buckets, std::set<std::string>& recovering_buckets, rgw_data_sync_marker *sync_marker, const int max_entries) {
    return source_log.read_shard_status(dpp, shard_id, lagging_buckets, recovering_buckets,sync_marker, max_entries);
  }
  int init_sync_status(const DoutPrefixProvider *dpp) { return source_log.init_sync_status(dpp, num_shards); }

  int read_log_info(const DoutPrefixProvider *dpp, rgw_datalog_info *log_info) {
    return source_log.read_log_info(dpp, log_info);
  }
  int read_source_log_shards_info(const DoutPrefixProvider *dpp, std::map<int, RGWDataChangesLogInfo> *shards_info) {
    return source_log.read_source_log_shards_info(dpp, shards_info);
  }
  int read_source_log_shards_next(const DoutPrefixProvider *dpp, std::map<int, std::string> shard_markers, std::map<int, rgw_datalog_shard_data> *result) {
    return source_log.read_source_log_shards_next(dpp, shard_markers, result);
  }

  int run(const DoutPrefixProvider *dpp) { return source_log.run_sync(dpp, num_shards); }

  void wakeup(int shard_id, bc::flat_set<rgw_data_notify_entry>& entries) { return source_log.wakeup(shard_id, entries); }

  void stop() {
    source_log.finish();
  }

  // implements DoutPrefixProvider
  CephContext *get_cct() const override;
  unsigned get_subsys() const override;
  std::ostream& gen_prefix(std::ostream& out) const override;
};

class RGWBucketPipeSyncStatusManager;
class RGWBucketSyncCR;

struct rgw_bucket_shard_full_sync_marker {
  rgw_obj_key position;
  uint64_t count;

  rgw_bucket_shard_full_sync_marker() : count(0) {}

  void encode_attr(std::map<std::string, bufferlist>& attrs);

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(position, bl);
    encode(count, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
     DECODE_START(1, bl);
    decode(position, bl);
    decode(count, bl);
     DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(rgw_bucket_shard_full_sync_marker)

struct rgw_bucket_shard_inc_sync_marker {
  std::string position;
  ceph::real_time timestamp;

  void encode_attr(std::map<std::string, bufferlist>& attrs);

  void encode(bufferlist& bl) const {
    ENCODE_START(2, 1, bl);
    encode(position, bl);
    encode(timestamp, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(2, bl);
    decode(position, bl);
    if (struct_v >= 2) {
      decode(timestamp, bl);
    }
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(rgw_bucket_shard_inc_sync_marker)

struct rgw_bucket_shard_sync_info {
  enum SyncState {
    StateInit = 0,
    StateFullSync = 1,
    StateIncrementalSync = 2,
    StateStopped = 3,
  };

  uint16_t state;
  rgw_bucket_shard_inc_sync_marker inc_marker;

  void decode_from_attrs(CephContext *cct, std::map<std::string, bufferlist>& attrs);
  void encode_all_attrs(std::map<std::string, bufferlist>& attrs);
  void encode_state_attr(std::map<std::string, bufferlist>& attrs);

  void encode(bufferlist& bl) const {
    ENCODE_START(2, 1, bl);
    encode(state, bl);
    encode(inc_marker, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
     DECODE_START(2, bl);
     decode(state, bl);
     if (struct_v <= 1) {
       rgw_bucket_shard_full_sync_marker full_marker;
       decode(full_marker, bl);
     }
     decode(inc_marker, bl);
     DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);

  rgw_bucket_shard_sync_info() : state((int)StateInit) {}

};
WRITE_CLASS_ENCODER(rgw_bucket_shard_sync_info)

struct rgw_bucket_full_sync_status {
  rgw_obj_key position;
  uint64_t count = 0;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(position, bl);
    encode(count, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(position, bl);
    decode(count, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(rgw_bucket_full_sync_status)

enum class BucketSyncState : uint8_t {
  Init = 0,
  Full,
  Incremental,
  Stopped,
};
inline std::ostream& operator<<(std::ostream& out, const BucketSyncState& s) {
  switch (s) {
  case BucketSyncState::Init: out << "init"; break;
  case BucketSyncState::Full: out << "full"; break;
  case BucketSyncState::Incremental: out << "incremental"; break;
  case BucketSyncState::Stopped: out << "stopped"; break;
  }
  return out;
}

void encode_json(const char *name, BucketSyncState state, Formatter *f);
void decode_json_obj(BucketSyncState& state, JSONObj *obj);

struct rgw_bucket_sync_status {
  BucketSyncState state = BucketSyncState::Init;
  rgw_bucket_full_sync_status full;
  uint64_t incremental_gen = 0;
  std::vector<bool> shards_done_with_gen;

  void encode(bufferlist& bl) const {
    ENCODE_START(2, 1, bl);
    encode(state, bl);
    encode(full, bl);
    encode(incremental_gen, bl);
    encode(shards_done_with_gen, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(2, bl);
    decode(state, bl);
    decode(full, bl);
    if (struct_v > 1) {
      decode(incremental_gen, bl);
      decode(shards_done_with_gen, bl);
    }
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(rgw_bucket_sync_status)

struct bilog_status_v2 {
  rgw_bucket_sync_status sync_status;
  std::vector<rgw_bucket_shard_sync_info> inc_status;

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
};

struct store_gen_shards {
  uint64_t gen = 0;
  uint32_t num_shards = 0;

  void dump(Formatter *f) const {
    encode_json("gen", gen, f);
    encode_json("num_shards", num_shards, f);
  }

  void decode_json(JSONObj *obj) {
    JSONDecoder::decode_json("gen", gen, obj);
    JSONDecoder::decode_json("num_shards", num_shards, obj);
  }
};

struct rgw_bucket_index_marker_info {
  std::string bucket_ver;
  std::string master_ver;
  std::string max_marker;
  bool syncstopped{false};
  uint64_t oldest_gen = 0;
  uint64_t latest_gen = 0;
  std::vector<store_gen_shards> generations;

  void decode_json(JSONObj *obj) {
    JSONDecoder::decode_json("bucket_ver", bucket_ver, obj);
    JSONDecoder::decode_json("master_ver", master_ver, obj);
    JSONDecoder::decode_json("max_marker", max_marker, obj);
    JSONDecoder::decode_json("syncstopped", syncstopped, obj);
    JSONDecoder::decode_json("oldest_gen", oldest_gen, obj);
    JSONDecoder::decode_json("latest_gen", latest_gen, obj);
    JSONDecoder::decode_json("generations", generations, obj);
  }
};


class BucketIndexShardsManager;

int rgw_read_remote_bilog_info(const DoutPrefixProvider *dpp,
                               RGWRESTConn* conn,
                               const rgw_bucket& bucket,
                               rgw_bucket_index_marker_info& info,
                               BucketIndexShardsManager& markers,
                               optional_yield y);

class RGWBucketPipeSyncStatusManager : public DoutPrefixProvider {
  rgw::sal::RadosStore* driver;

  RGWDataSyncEnv sync_env;

  RGWCoroutinesManager cr_mgr{driver->ctx(),
                              driver->getRados()->get_cr_registry()};

  RGWHTTPManager http_manager{driver->ctx(), cr_mgr.get_completion_mgr()};

  std::optional<rgw_zone_id> source_zone;
  std::optional<rgw_bucket> source_bucket;

  std::unique_ptr<RGWSyncErrorLogger> error_logger =
    std::make_unique<RGWSyncErrorLogger>(driver, RGW_SYNC_ERROR_LOG_SHARD_PREFIX,
					 ERROR_LOGGER_SHARDS);
  RGWSyncModuleInstanceRef sync_module;

  rgw_bucket dest_bucket;

  struct source {
    RGWDataSyncCtx sc;
    RGWBucketInfo info;
    rgw_bucket dest;
    RGWBucketSyncFlowManager::pipe_handler handler;
    std::string zone_name;

    source(RGWDataSyncEnv* env, const rgw_zone_id& zone, RGWRESTConn* conn,
	   const RGWBucketInfo& info, const rgw_bucket& dest,
	   const RGWBucketSyncFlowManager::pipe_handler& handler,
	   const std::string& zone_name)
      : sc(env, conn, zone), info(info), dest(dest), handler(handler),
	zone_name(zone_name) {}
  };
  std::vector<source> sources;

  int do_init(const DoutPrefixProvider *dpp, std::ostream* ostr);
  RGWBucketPipeSyncStatusManager(rgw::sal::RadosStore* driver,
				 std::optional<rgw_zone_id> source_zone,
				 std::optional<rgw_bucket> source_bucket,
				 const rgw_bucket& dest_bucket)
    : driver(driver), source_zone(source_zone), source_bucket(source_bucket),
      dest_bucket(dest_bucket) {}

  int remote_info(const DoutPrefixProvider *dpp, source& s,
		  uint64_t* oldest_gen, uint64_t* latest_gen,
		  uint64_t* num_shards);
public:
  static tl::expected<std::unique_ptr<RGWBucketPipeSyncStatusManager>, int>
  construct(const DoutPrefixProvider* dpp, rgw::sal::RadosStore* driver,
	    std::optional<rgw_zone_id> source_zone,
	    std::optional<rgw_bucket> source_bucket,
	    const rgw_bucket& dest_bucket, std::ostream *ostream);
  ~RGWBucketPipeSyncStatusManager() = default;


  static std::string full_status_oid(const rgw_zone_id& source_zone,
				     const rgw_bucket& source_bucket,
				     const rgw_bucket& dest_bucket);
  static std::string inc_status_oid(const rgw_zone_id& source_zone,
				    const rgw_bucket_sync_pair_info& bs,
				    uint64_t gen);
  // specific source obj sync status, can be used by sync modules
  static std::string obj_status_oid(const rgw_bucket_sync_pipe& sync_pipe,
				    const rgw_zone_id& source_zone,
				    const rgw_obj& obj);

  // implements DoutPrefixProvider
  CephContext *get_cct() const override;
  unsigned get_subsys() const override;
  std::ostream& gen_prefix(std::ostream& out) const override;

  int init_sync_status(const DoutPrefixProvider *dpp);
  tl::expected<std::map<int, rgw_bucket_shard_sync_info>, int> read_sync_status(
    const DoutPrefixProvider *dpp);
  int run(const DoutPrefixProvider *dpp);
};

/// read the full sync status with respect to a source bucket
int rgw_read_bucket_full_sync_status(const DoutPrefixProvider *dpp,
                                     rgw::sal::RadosStore *driver,
                                     const rgw_sync_bucket_pipe& pipe,
                                     rgw_bucket_sync_status *status,
                                     optional_yield y);

/// read the incremental sync status of all bucket shards from the given source zone
int rgw_read_bucket_inc_sync_status(const DoutPrefixProvider *dpp,
                                    rgw::sal::RadosStore *driver,
                                    const rgw_sync_bucket_pipe& pipe,
                                    uint64_t gen,
                                    std::vector<rgw_bucket_shard_sync_info> *status);

class RGWDefaultSyncModule : public RGWSyncModule {
public:
  RGWDefaultSyncModule() {}
  bool supports_writes() override { return true; }
  bool supports_data_export() override { return true; }
  int create_instance(const DoutPrefixProvider *dpp, CephContext *cct, const JSONFormattable& config, RGWSyncModuleInstanceRef *instance) override;
};

class RGWArchiveSyncModule : public RGWDefaultSyncModule {
public:
  RGWArchiveSyncModule() {}
  bool supports_writes() override { return true; }
  bool supports_data_export() override { return false; }
  int create_instance(const DoutPrefixProvider *dpp, CephContext *cct, const JSONFormattable& config, RGWSyncModuleInstanceRef *instance) override;
};
