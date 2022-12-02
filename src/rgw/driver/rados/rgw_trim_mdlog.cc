// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "common/errno.h"

#include "rgw_trim_mdlog.h"
#include "rgw_sync.h"
#include "rgw_cr_rados.h"
#include "rgw_cr_rest.h"
#include "rgw_zone.h"
#include "services/svc_zone.h"
#include "services/svc_meta.h"
#include "services/svc_mdlog.h"
#include "services/svc_cls.h"

#include <boost/asio/yield.hpp>

#define dout_subsys ceph_subsys_rgw

#undef dout_prefix
#define dout_prefix (*_dout << "meta trim: ")

/// purge all log shards for the given mdlog
class PurgeLogShardsCR : public RGWShardCollectCR {
  rgw::sal::RadosStore* const store;
  const RGWMetadataLog* mdlog;
  const int num_shards;
  rgw_raw_obj obj;
  int i{0};

  static constexpr int max_concurrent = 16;

  int handle_result(int r) override {
    if (r == -ENOENT) { // ENOENT is not a fatal error
      return 0;
    }
    if (r < 0) {
      ldout(cct, 4) << "failed to remove mdlog shard: " << cpp_strerror(r) << dendl;
    }
    return r;
  }
 public:
  PurgeLogShardsCR(rgw::sal::RadosStore* store, const RGWMetadataLog* mdlog,
                   const rgw_pool& pool, int num_shards)
    : RGWShardCollectCR(store->ctx(), max_concurrent),
      store(store), mdlog(mdlog), num_shards(num_shards), obj(pool, "")
  {}

  bool spawn_next() override {
    if (i == num_shards) {
      return false;
    }
    mdlog->get_shard_oid(i++, obj.oid);
    spawn(new RGWRadosRemoveCR(store, obj), false);
    return true;
  }
};

using Cursor = RGWPeriodHistory::Cursor;

/// purge mdlogs from the oldest up to (but not including) the given realm_epoch
class PurgePeriodLogsCR : public RGWCoroutine {
  struct Svc {
    RGWSI_Zone *zone;
    RGWSI_MDLog *mdlog;
  } svc;
  const DoutPrefixProvider *dpp;
  rgw::sal::RadosStore* const store;
  RGWMetadataManager *const metadata;
  RGWObjVersionTracker objv;
  Cursor cursor;
  epoch_t realm_epoch;
  epoch_t *last_trim_epoch; //< update last trim on success

 public:
  PurgePeriodLogsCR(const DoutPrefixProvider *dpp, rgw::sal::RadosStore* store, epoch_t realm_epoch, epoch_t *last_trim)
    : RGWCoroutine(store->ctx()), dpp(dpp), store(store), metadata(store->ctl()->meta.mgr),
      realm_epoch(realm_epoch), last_trim_epoch(last_trim) {
    svc.zone = store->svc()->zone;
    svc.mdlog = store->svc()->mdlog;
  }

  int operate(const DoutPrefixProvider *dpp) override;
};

int PurgePeriodLogsCR::operate(const DoutPrefixProvider *dpp)
{
  reenter(this) {
    // read our current oldest log period
    yield call(svc.mdlog->read_oldest_log_period_cr(dpp, &cursor, &objv));
    if (retcode < 0) {
      return set_cr_error(retcode);
    }
    ceph_assert(cursor);
    ldpp_dout(dpp, 20) << "oldest log realm_epoch=" << cursor.get_epoch()
        << " period=" << cursor.get_period().get_id() << dendl;

    // trim -up to- the given realm_epoch
    while (cursor.get_epoch() < realm_epoch) {
      ldpp_dout(dpp, 4) << "purging log shards for realm_epoch=" << cursor.get_epoch()
          << " period=" << cursor.get_period().get_id() << dendl;
      yield {
        const auto mdlog = svc.mdlog->get_log(cursor.get_period().get_id());
        const auto& pool = svc.zone->get_zone_params().log_pool;
        auto num_shards = cct->_conf->rgw_md_log_max_shards;
        call(new PurgeLogShardsCR(store, mdlog, pool, num_shards));
      }
      if (retcode < 0) {
        ldpp_dout(dpp, 1) << "failed to remove log shards: "
            << cpp_strerror(retcode) << dendl;
        return set_cr_error(retcode);
      }
      ldpp_dout(dpp, 10) << "removed log shards for realm_epoch=" << cursor.get_epoch()
          << " period=" << cursor.get_period().get_id() << dendl;

      // update our mdlog history
      yield call(svc.mdlog->trim_log_period_cr(dpp, cursor, &objv));
      if (retcode == -ENOENT) {
        // must have raced to update mdlog history. return success and allow the
        // winner to continue purging
        ldpp_dout(dpp, 10) << "already removed log shards for realm_epoch=" << cursor.get_epoch()
            << " period=" << cursor.get_period().get_id() << dendl;
        return set_cr_done();
      } else if (retcode < 0) {
        ldpp_dout(dpp, 1) << "failed to remove log shards for realm_epoch="
            << cursor.get_epoch() << " period=" << cursor.get_period().get_id()
            << " with: " << cpp_strerror(retcode) << dendl;
        return set_cr_error(retcode);
      }

      if (*last_trim_epoch < cursor.get_epoch()) {
        *last_trim_epoch = cursor.get_epoch();
      }

      ceph_assert(cursor.has_next()); // get_current() should always come after
      cursor.next();
    }
    return set_cr_done();
  }
  return 0;
}

namespace {

using connection_map = std::map<std::string, std::unique_ptr<RGWRESTConn>>;

/// construct a RGWRESTConn for each zone in the realm
template <typename Zonegroups>
connection_map make_peer_connections(rgw::sal::RadosStore* store,
                                     const Zonegroups& zonegroups)
{
  connection_map connections;
  for (auto& g : zonegroups) {
    for (auto& z : g.second.zones) {
      std::unique_ptr<RGWRESTConn> conn{
        new RGWRESTConn(store->ctx(), store, z.first.id, z.second.endpoints, g.second.api_name)};
      connections.emplace(z.first.id, std::move(conn));
    }
  }
  return connections;
}

/// return the marker that it's safe to trim up to
const std::string& get_stable_marker(const rgw_meta_sync_marker& m)
{
  return m.state == m.FullSync ? m.next_step_marker : m.marker;
}

/// comparison operator for take_min_status()
bool operator<(const rgw_meta_sync_marker& lhs, const rgw_meta_sync_marker& rhs)
{
  // sort by stable marker
  return get_stable_marker(lhs) < get_stable_marker(rhs);
}

/// populate the status with the minimum stable marker of each shard for any
/// peer whose realm_epoch matches the minimum realm_epoch in the input
template <typename Iter>
int take_min_status(CephContext *cct, Iter first, Iter last,
                    rgw_meta_sync_status *status)
{
  if (first == last) {
    return -EINVAL;
  }
  const size_t num_shards = cct->_conf->rgw_md_log_max_shards;

  status->sync_info.realm_epoch = std::numeric_limits<epoch_t>::max();
  for (auto p = first; p != last; ++p) {
    // validate peer's shard count
    if (p->sync_markers.size() != num_shards) {
      ldout(cct, 1) << "take_min_status got peer status with "
          << p->sync_markers.size() << " shards, expected "
          << num_shards << dendl;
      return -EINVAL;
    }
    if (p->sync_info.realm_epoch < status->sync_info.realm_epoch) {
      // earlier epoch, take its entire status
      *status = std::move(*p);
    } else if (p->sync_info.realm_epoch == status->sync_info.realm_epoch) {
      // same epoch, take any earlier markers
      auto m = status->sync_markers.begin();
      for (auto& shard : p->sync_markers) {
        if (shard.second < m->second) {
          m->second = std::move(shard.second);
        }
        ++m;
      }
    }
  }
  return 0;
}

struct TrimEnv {
  const DoutPrefixProvider *dpp;
  rgw::sal::RadosStore* const store;
  RGWHTTPManager *const http;
  int num_shards;
  const rgw_zone_id& zone;
  Cursor current; //< cursor to current period
  epoch_t last_trim_epoch{0}; //< epoch of last mdlog that was purged

  TrimEnv(const DoutPrefixProvider *dpp, rgw::sal::RadosStore* store, RGWHTTPManager *http, int num_shards)
    : dpp(dpp), store(store), http(http), num_shards(num_shards),
      zone(store->svc()->zone->zone_id()),
      current(store->svc()->mdlog->get_period_history()->get_current())
  {}
};

struct MasterTrimEnv : public TrimEnv {
  connection_map connections; //< peer connections
  std::vector<rgw_meta_sync_status> peer_status; //< sync status for each peer
  /// last trim marker for each shard, only applies to current period's mdlog
  std::vector<std::string> last_trim_markers;

  MasterTrimEnv(const DoutPrefixProvider *dpp, rgw::sal::RadosStore* store, RGWHTTPManager *http, int num_shards)
    : TrimEnv(dpp, store, http, num_shards),
      last_trim_markers(num_shards)
  {
    auto& period = current.get_period();
    connections = make_peer_connections(store, period.get_map().zonegroups);
    connections.erase(zone.id);
    peer_status.resize(connections.size());
  }
};

struct PeerTrimEnv : public TrimEnv {
  /// last trim timestamp for each shard, only applies to current period's mdlog
  std::vector<ceph::real_time> last_trim_timestamps;

  PeerTrimEnv(const DoutPrefixProvider *dpp, rgw::sal::RadosStore* store, RGWHTTPManager *http, int num_shards)
    : TrimEnv(dpp, store, http, num_shards),
      last_trim_timestamps(num_shards)
  {}

  void set_num_shards(int num_shards) {
    this->num_shards = num_shards;
    last_trim_timestamps.resize(num_shards);
  }
};

} // anonymous namespace


/// spawn a trim cr for each shard that needs it, while limiting the number
/// of concurrent shards
class MetaMasterTrimShardCollectCR : public RGWShardCollectCR {
 private:
  static constexpr int MAX_CONCURRENT_SHARDS = 16;

  MasterTrimEnv& env;
  RGWMetadataLog *mdlog;
  int shard_id{0};
  std::string oid;
  const rgw_meta_sync_status& sync_status;

  int handle_result(int r) override {
    if (r == -ENOENT) { // ENOENT is not a fatal error
      return 0;
    }
    if (r < 0) {
      ldout(cct, 4) << "failed to trim mdlog shard: " << cpp_strerror(r) << dendl;
    }
    return r;
  }
 public:
  MetaMasterTrimShardCollectCR(MasterTrimEnv& env, RGWMetadataLog *mdlog,
                               const rgw_meta_sync_status& sync_status)
    : RGWShardCollectCR(env.store->ctx(), MAX_CONCURRENT_SHARDS),
      env(env), mdlog(mdlog), sync_status(sync_status)
  {}

  bool spawn_next() override;
};

bool MetaMasterTrimShardCollectCR::spawn_next()
{
  while (shard_id < env.num_shards) {
    auto m = sync_status.sync_markers.find(shard_id);
    if (m == sync_status.sync_markers.end()) {
      shard_id++;
      continue;
    }
    auto& stable = get_stable_marker(m->second);
    auto& last_trim = env.last_trim_markers[shard_id];

    if (stable <= last_trim) {
      // already trimmed
      ldpp_dout(env.dpp, 20) << "skipping log shard " << shard_id
          << " at marker=" << stable
          << " last_trim=" << last_trim
          << " realm_epoch=" << sync_status.sync_info.realm_epoch << dendl;
      shard_id++;
      continue;
    }

    mdlog->get_shard_oid(shard_id, oid);

    ldpp_dout(env.dpp, 10) << "trimming log shard " << shard_id
        << " at marker=" << stable
        << " last_trim=" << last_trim
        << " realm_epoch=" << sync_status.sync_info.realm_epoch << dendl;
    spawn(new RGWSyncLogTrimCR(env.dpp, env.store, oid, stable, &last_trim), false);
    shard_id++;
    return true;
  }
  return false;
}

/// spawn rest requests to read each peer's sync status
class MetaMasterStatusCollectCR : public RGWShardCollectCR {
  static constexpr int MAX_CONCURRENT_SHARDS = 16;

  MasterTrimEnv& env;
  connection_map::iterator c;
  std::vector<rgw_meta_sync_status>::iterator s;

  int handle_result(int r) override {
    if (r == -ENOENT) { // ENOENT is not a fatal error
      return 0;
    }
    if (r < 0) {
      ldout(cct, 4) << "failed to fetch metadata sync status: "
          << cpp_strerror(r) << dendl;
    }
    return r;
  }
 public:
  explicit MetaMasterStatusCollectCR(MasterTrimEnv& env)
    : RGWShardCollectCR(env.store->ctx(), MAX_CONCURRENT_SHARDS),
      env(env), c(env.connections.begin()), s(env.peer_status.begin())
  {}

  bool spawn_next() override {
    if (c == env.connections.end()) {
      return false;
    }
    static rgw_http_param_pair params[] = {
      { "type", "metadata" },
      { "status", nullptr },
      { nullptr, nullptr }
    };

    ldout(cct, 20) << "query sync status from " << c->first << dendl;
    auto conn = c->second.get();
    using StatusCR = RGWReadRESTResourceCR<rgw_meta_sync_status>;
    spawn(new StatusCR(cct, conn, env.http, "/admin/log/", params, &*s),
          false);
    ++c;
    ++s;
    return true;
  }
};

class MetaMasterTrimCR : public RGWCoroutine {
  MasterTrimEnv& env;
  rgw_meta_sync_status min_status; //< minimum sync status of all peers
  int ret{0};

 public:
  explicit MetaMasterTrimCR(MasterTrimEnv& env)
    : RGWCoroutine(env.store->ctx()), env(env)
  {}

  int operate(const DoutPrefixProvider *dpp) override;
};

int MetaMasterTrimCR::operate(const DoutPrefixProvider *dpp)
{
  reenter(this) {
    // TODO: detect this and fail before we spawn the trim thread?
    if (env.connections.empty()) {
      ldpp_dout(dpp, 4) << "no peers, exiting" << dendl;
      return set_cr_done();
    }

    ldpp_dout(dpp, 10) << "fetching sync status for zone " << env.zone << dendl;
    // query mdlog sync status from peers
    yield call(new MetaMasterStatusCollectCR(env));

    // must get a successful reply from all peers to consider trimming
    if (ret < 0) {
      ldpp_dout(dpp, 4) << "failed to fetch sync status from all peers" << dendl;
      return set_cr_error(ret);
    }

    // determine the minimum epoch and markers
    ret = take_min_status(env.store->ctx(), env.peer_status.begin(),
                          env.peer_status.end(), &min_status);
    if (ret < 0) {
      ldpp_dout(dpp, 4) << "failed to calculate min sync status from peers" << dendl;
      return set_cr_error(ret);
    }
    yield {
      auto store = env.store;
      auto epoch = min_status.sync_info.realm_epoch;
      ldpp_dout(dpp, 4) << "realm epoch min=" << epoch
          << " current=" << env.current.get_epoch()<< dendl;
      if (epoch > env.last_trim_epoch + 1) {
        // delete any prior mdlog periods
        spawn(new PurgePeriodLogsCR(dpp, store, epoch, &env.last_trim_epoch), true);
      } else {
        ldpp_dout(dpp, 10) << "mdlogs already purged up to realm_epoch "
            << env.last_trim_epoch << dendl;
      }

      // if realm_epoch == current, trim mdlog based on markers
      if (epoch == env.current.get_epoch()) {
        auto mdlog = store->svc()->mdlog->get_log(env.current.get_period().get_id());
        spawn(new MetaMasterTrimShardCollectCR(env, mdlog, min_status), true);
      }
    }
    // ignore any errors during purge/trim because we want to hold the lock open
    return set_cr_done();
  }
  return 0;
}


/// read the first entry of the master's mdlog shard and trim to that position
class MetaPeerTrimShardCR : public RGWCoroutine {
  RGWMetaSyncEnv& env;
  RGWMetadataLog *mdlog;
  const std::string& period_id;
  const int shard_id;
  RGWMetadataLogInfo info;
  ceph::real_time stable; //< safe timestamp to trim, according to master
  ceph::real_time *last_trim; //< last trimmed timestamp, updated on trim
  rgw_mdlog_shard_data result; //< result from master's mdlog listing

 public:
  MetaPeerTrimShardCR(RGWMetaSyncEnv& env, RGWMetadataLog *mdlog,
                      const std::string& period_id, int shard_id,
                      ceph::real_time *last_trim)
    : RGWCoroutine(env.store->ctx()), env(env), mdlog(mdlog),
      period_id(period_id), shard_id(shard_id), last_trim(last_trim)
  {}

  int operate(const DoutPrefixProvider *dpp) override;
};

int MetaPeerTrimShardCR::operate(const DoutPrefixProvider *dpp)
{
  reenter(this) {
    // query master's first mdlog entry for this shard
    yield call(create_list_remote_mdlog_shard_cr(&env, period_id, shard_id,
                                                 "", 1, &result));
    if (retcode < 0) {
      ldpp_dout(dpp, 5) << "failed to read first entry from master's mdlog shard "
          << shard_id << " for period " << period_id
          << ": " << cpp_strerror(retcode) << dendl;
      return set_cr_error(retcode);
    }
    if (result.entries.empty()) {
      // if there are no mdlog entries, we don't have a timestamp to compare. we
      // can't just trim everything, because there could be racing updates since
      // this empty reply. query the mdlog shard info to read its max timestamp,
      // then retry the listing to make sure it's still empty before trimming to
      // that
      ldpp_dout(dpp, 10) << "empty master mdlog shard " << shard_id
          << ", reading last timestamp from shard info" << dendl;
      // read the mdlog shard info for the last timestamp
      yield call(create_read_remote_mdlog_shard_info_cr(&env, period_id, shard_id, &info));
      if (retcode < 0) {
        ldpp_dout(dpp, 5) << "failed to read info from master's mdlog shard "
            << shard_id << " for period " << period_id
            << ": " << cpp_strerror(retcode) << dendl;
        return set_cr_error(retcode);
      }
      if (ceph::real_clock::is_zero(info.last_update)) {
        return set_cr_done(); // nothing to trim
      }
      ldpp_dout(dpp, 10) << "got mdlog shard info with last update="
          << info.last_update << dendl;
      // re-read the master's first mdlog entry to make sure it hasn't changed
      yield call(create_list_remote_mdlog_shard_cr(&env, period_id, shard_id,
                                                   "", 1, &result));
      if (retcode < 0) {
        ldpp_dout(dpp, 5) << "failed to read first entry from master's mdlog shard "
            << shard_id << " for period " << period_id
            << ": " << cpp_strerror(retcode) << dendl;
        return set_cr_error(retcode);
      }
      // if the mdlog is still empty, trim to max marker
      if (result.entries.empty()) {
        stable = info.last_update;
      } else {
        stable = result.entries.front().timestamp;

        // can only trim -up to- master's first timestamp, so subtract a second.
        // (this is why we use timestamps instead of markers for the peers)
        stable -= std::chrono::seconds(1);
      }
    } else {
      stable = result.entries.front().timestamp;
      stable -= std::chrono::seconds(1);
    }

    if (stable <= *last_trim) {
      ldpp_dout(dpp, 10) << "skipping log shard " << shard_id
          << " at timestamp=" << stable
          << " last_trim=" << *last_trim << dendl;
      return set_cr_done();
    }

    ldpp_dout(dpp, 10) << "trimming log shard " << shard_id
        << " at timestamp=" << stable
        << " last_trim=" << *last_trim << dendl;
    yield {
      std::string oid;
      mdlog->get_shard_oid(shard_id, oid);
      call(new RGWRadosTimelogTrimCR(dpp, env.store, oid, real_time{}, stable, "", ""));
    }
    if (retcode < 0 && retcode != -ENODATA) {
      ldpp_dout(dpp, 1) << "failed to trim mdlog shard " << shard_id
          << ": " << cpp_strerror(retcode) << dendl;
      return set_cr_error(retcode);
    }
    *last_trim = stable;
    return set_cr_done();
  }
  return 0;
}

class MetaPeerTrimShardCollectCR : public RGWShardCollectCR {
  static constexpr int MAX_CONCURRENT_SHARDS = 16;

  PeerTrimEnv& env;
  RGWMetadataLog *mdlog;
  const std::string& period_id;
  RGWMetaSyncEnv meta_env; //< for RGWListRemoteMDLogShardCR
  int shard_id{0};

  int handle_result(int r) override {
    if (r == -ENOENT) { // ENOENT is not a fatal error
      return 0;
    }
    if (r < 0) {
      ldout(cct, 4) << "failed to trim mdlog shard: " << cpp_strerror(r) << dendl;
    }
    return r;
  }
 public:
  MetaPeerTrimShardCollectCR(PeerTrimEnv& env, RGWMetadataLog *mdlog)
    : RGWShardCollectCR(env.store->ctx(), MAX_CONCURRENT_SHARDS),
      env(env), mdlog(mdlog), period_id(env.current.get_period().get_id())
  {
    meta_env.init(env.dpp, cct, env.store, env.store->svc()->zone->get_master_conn(),
                  env.store->svc()->async_processor, env.http, nullptr,
                  env.store->getRados()->get_sync_tracer());
  }

  bool spawn_next() override;
};

bool MetaPeerTrimShardCollectCR::spawn_next()
{
  if (shard_id >= env.num_shards) {
    return false;
  }
  auto& last_trim = env.last_trim_timestamps[shard_id];
  spawn(new MetaPeerTrimShardCR(meta_env, mdlog, period_id, shard_id, &last_trim),
        false);
  shard_id++;
  return true;
}

class MetaPeerTrimCR : public RGWCoroutine {
  PeerTrimEnv& env;
  rgw_mdlog_info mdlog_info; //< master's mdlog info

 public:
  explicit MetaPeerTrimCR(PeerTrimEnv& env) : RGWCoroutine(env.store->ctx()), env(env) {}

  int operate(const DoutPrefixProvider *dpp) override;
};

int MetaPeerTrimCR::operate(const DoutPrefixProvider *dpp)
{
  reenter(this) {
    ldpp_dout(dpp, 10) << "fetching master mdlog info" << dendl;
    yield {
      // query mdlog_info from master for oldest_log_period
      rgw_http_param_pair params[] = {
        { "type", "metadata" },
        { nullptr, nullptr }
      };

      using LogInfoCR = RGWReadRESTResourceCR<rgw_mdlog_info>;
      call(new LogInfoCR(cct, env.store->svc()->zone->get_master_conn(), env.http,
                         "/admin/log/", params, &mdlog_info));
    }
    if (retcode < 0) {
      ldpp_dout(dpp, 4) << "failed to read mdlog info from master" << dendl;
      return set_cr_error(retcode);
    }
    // use master's shard count instead
    env.set_num_shards(mdlog_info.num_shards);

    if (mdlog_info.realm_epoch > env.last_trim_epoch + 1) {
      // delete any prior mdlog periods
      yield call(new PurgePeriodLogsCR(dpp, env.store, mdlog_info.realm_epoch,
                                       &env.last_trim_epoch));
    } else {
      ldpp_dout(dpp, 10) << "mdlogs already purged through realm_epoch "
          << env.last_trim_epoch << dendl;
    }

    // if realm_epoch == current, trim mdlog based on master's markers
    if (mdlog_info.realm_epoch == env.current.get_epoch()) {
      yield {
        auto mdlog = env.store->svc()->mdlog->get_log(env.current.get_period().get_id());
        call(new MetaPeerTrimShardCollectCR(env, mdlog));
        // ignore any errors during purge/trim because we want to hold the lock open
      }
    }
    return set_cr_done();
  }
  return 0;
}

class MetaTrimPollCR : public RGWCoroutine {
  rgw::sal::RadosStore* const store;
  const utime_t interval; //< polling interval
  const rgw_raw_obj obj;
  const std::string name{"meta_trim"}; //< lock name
  const std::string cookie;

 protected:
  /// allocate the coroutine to run within the lease
  virtual RGWCoroutine* alloc_cr() = 0;

 public:
  MetaTrimPollCR(rgw::sal::RadosStore* store, utime_t interval)
    : RGWCoroutine(store->ctx()), store(store), interval(interval),
      obj(store->svc()->zone->get_zone_params().log_pool, RGWMetadataLogHistory::oid),
      cookie(RGWSimpleRadosLockCR::gen_random_cookie(cct))
  {}

  int operate(const DoutPrefixProvider *dpp) override;
};

int MetaTrimPollCR::operate(const DoutPrefixProvider *dpp)
{
  reenter(this) {
    for (;;) {
      set_status("sleeping");
      wait(interval);

      // prevent others from trimming for our entire wait interval
      set_status("acquiring trim lock");

      yield call(new RGWSimpleRadosLockCR(store->svc()->async_processor, store,
                                          obj, name, cookie, 
                                          // interval is a small number and unlikely to overflow
                                          // coverity[store_truncates_time_t:SUPPRESS]
                                          interval.sec()));
      if (retcode < 0) {
        ldout(cct, 4) << "failed to lock: " << cpp_strerror(retcode) << dendl;
        continue;
      }

      set_status("trimming");
      yield call(alloc_cr());

      if (retcode < 0) {
        // on errors, unlock so other gateways can try
        set_status("unlocking");
        yield call(new RGWSimpleRadosUnlockCR(store->svc()->async_processor, store,
                                              obj, name, cookie));
      }
    }
  }
  return 0;
}

class MetaMasterTrimPollCR : public MetaTrimPollCR  {
  MasterTrimEnv env; //< trim state to share between calls
  RGWCoroutine* alloc_cr() override {
    return new MetaMasterTrimCR(env);
  }
 public:
  MetaMasterTrimPollCR(const DoutPrefixProvider *dpp, rgw::sal::RadosStore* store, RGWHTTPManager *http,
                       int num_shards, utime_t interval)
    : MetaTrimPollCR(store, interval),
      env(dpp, store, http, num_shards)
  {}
};

class MetaPeerTrimPollCR : public MetaTrimPollCR {
  PeerTrimEnv env; //< trim state to share between calls
  RGWCoroutine* alloc_cr() override {
    return new MetaPeerTrimCR(env);
  }
 public:
  MetaPeerTrimPollCR(const DoutPrefixProvider *dpp, rgw::sal::RadosStore* store, RGWHTTPManager *http,
                     int num_shards, utime_t interval)
    : MetaTrimPollCR(store, interval),
      env(dpp, store, http, num_shards)
  {}
};

namespace {
bool sanity_check_endpoints(const DoutPrefixProvider *dpp, rgw::sal::RadosStore* store) {
  bool retval = true;
  auto current = store->svc()->mdlog->get_period_history()->get_current();
  const auto& period = current.get_period();
  for (const auto& [_, zonegroup] : period.get_map().zonegroups) {
    if (zonegroup.endpoints.empty()) {
      ldpp_dout(dpp, -1)
	<< __PRETTY_FUNCTION__ << ":" << __LINE__
	<< " WARNING: Cluster is is misconfigured! "
	<< " Zonegroup " << zonegroup.get_name()
	<< " (" << zonegroup.get_id() << ") in Realm id ( "
  << period.get_realm() << ") "
	<< " has no endpoints!" << dendl;
    }
    for (const auto& [_, zone] : zonegroup.zones) {
      if (zone.endpoints.empty()) {
	ldpp_dout(dpp, -1)
	  << __PRETTY_FUNCTION__ << ":" << __LINE__
	  << " ERROR: Cluster is is misconfigured! "
	  << " Zone " << zone.name << " (" << zone.id << ") in Zonegroup "
	  << zonegroup.get_name() << " ( " << zonegroup.get_id()
	  << ") in Realm id ( " << period.get_realm() << ") "
	  << " has no endpoints! Trimming is impossible." << dendl;
	retval = false;
      }
    }
  }
  return retval;
}
}

RGWCoroutine* create_meta_log_trim_cr(const DoutPrefixProvider *dpp, rgw::sal::RadosStore* store, RGWHTTPManager *http,
                                      int num_shards, utime_t interval)
{
  if (!sanity_check_endpoints(dpp, store)) {
    ldpp_dout(dpp, -1)
      << __PRETTY_FUNCTION__ << ":" << __LINE__
      << " ERROR: Cluster is is misconfigured! Refusing to trim." << dendl;
      return nullptr;
  }
  if (store->svc()->zone->is_meta_master()) {
    return new MetaMasterTrimPollCR(dpp, store, http, num_shards, interval);
  }
  return new MetaPeerTrimPollCR(dpp, store, http, num_shards, interval);
}


struct MetaMasterAdminTrimCR : private MasterTrimEnv, public MetaMasterTrimCR {
  MetaMasterAdminTrimCR(const DoutPrefixProvider *dpp, rgw::sal::RadosStore* store, RGWHTTPManager *http, int num_shards)
    : MasterTrimEnv(dpp, store, http, num_shards),
      MetaMasterTrimCR(*static_cast<MasterTrimEnv*>(this))
  {}
};

struct MetaPeerAdminTrimCR : private PeerTrimEnv, public MetaPeerTrimCR {
  MetaPeerAdminTrimCR(const DoutPrefixProvider *dpp, rgw::sal::RadosStore* store, RGWHTTPManager *http, int num_shards)
    : PeerTrimEnv(dpp, store, http, num_shards),
      MetaPeerTrimCR(*static_cast<PeerTrimEnv*>(this))
  {}
};

RGWCoroutine* create_admin_meta_log_trim_cr(const DoutPrefixProvider *dpp, rgw::sal::RadosStore* store,
                                            RGWHTTPManager *http,
                                            int num_shards)
{
  if (!sanity_check_endpoints(dpp, store)) {
    ldpp_dout(dpp, -1)
      << __PRETTY_FUNCTION__ << ":" << __LINE__
      << " ERROR: Cluster is is misconfigured! Refusing to trim." << dendl;
      return nullptr;
  }
  if (store->svc()->zone->is_meta_master()) {
    return new MetaMasterAdminTrimCR(dpp, store, http, num_shards);
  }
  return new MetaPeerAdminTrimCR(dpp, store, http, num_shards);
}
