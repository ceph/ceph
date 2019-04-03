// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "pg.h"

#include <functional>

#include <boost/range/adaptor/filtered.hpp>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/algorithm/copy.hpp>
#include <boost/range/algorithm/max_element.hpp>
#include <boost/range/numeric.hpp>

#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
#include "messages/MOSDPGInfo.h"
#include "messages/MOSDPGLog.h"
#include "messages/MOSDPGNotify.h"
#include "messages/MOSDPGQuery.h"

#include "osd/OSDMap.h"

#include "crimson/net/Connection.h"
#include "crimson/net/Messenger.h"
#include "crimson/os/cyan_collection.h"
#include "crimson/os/cyan_store.h"
#include "crimson/osd/exceptions.h"
#include "crimson/osd/pg_meta.h"

#include "pg_backend.h"
#include "recovery_events.h"
#include "recovery_state.h"

namespace {
  seastar::logger& logger() {
    return ceph::get_logger(ceph_subsys_osd);
  }
  template<typename Message, typename... Args>
  Ref<Message> make_message(Args&&... args)
  {
    return {new Message{std::forward<Args>(args)...}, false};
  }
}

using recovery::AdvMap;
using recovery::ActMap;
using recovery::Initialize;

PG::PG(spg_t pgid,
       pg_shard_t pg_shard,
       pg_pool_t&& pool,
       std::string&& name,
       std::unique_ptr<PGBackend> backend,
       cached_map_t osdmap,
       ceph::net::Messenger& msgr)
  : pgid{pgid},
    whoami{pg_shard},
    pool{std::move(pool)},
    recovery_state{*this},
    info{pgid},
    backend{std::move(backend)},
    osdmap{osdmap},
    msgr{msgr}
{
  // TODO
}

seastar::future<> PG::read_state(ceph::os::CyanStore* store)
{
  return PGMeta{store, pgid}.load().then(
    [this](pg_info_t pg_info_, PastIntervals past_intervals_) {
      info = std::move(pg_info_);
      last_written_info = info;
      past_intervals = std::move(past_intervals_);
      // initialize current mapping
      {
        vector<int> new_up, new_acting;
        int new_up_primary, new_acting_primary;
        osdmap->pg_to_up_acting_osds(pgid.pgid,
                                     &new_up, &new_up_primary,
                                     &new_acting, &new_acting_primary);
        update_primary_state(new_up, new_up_primary,
                             new_acting, new_acting_primary);
      }
      info.stats.up = up;
      info.stats.up_primary = up_primary.osd;
      info.stats.acting = acting;
      info.stats.acting_primary = primary.osd;
      info.stats.mapping_epoch = info.history.same_interval_since;
      recovery_state.handle_event(Initialize{});
      // note: we don't activate here because we know the OSD will advance maps
      // during boot.
      return seastar::now();
    });
}

void
PG::update_primary_state(const std::vector<int>& new_up,
                         int new_up_primary,
                         const std::vector<int>& new_acting,
                         int new_acting_primary)
{
  auto collect_pg_shards =
    [is_erasure=pool.is_erasure()](const std::vector<int>& osds,
                                   int osd_primary) {
      int8_t index = 0;
      pg_shard_set_t collected;
      pg_shard_t pg_primary;
      for (auto osd : osds) {
        if (osd != CRUSH_ITEM_NONE) {
          pg_shard_t pg_shard{
            osd, is_erasure ? shard_id_t{index} : shard_id_t::NO_SHARD};
          if (osd == osd_primary) {
            pg_primary = pg_shard;
          }
          collected.insert(pg_shard);
        }
        index++;
      }
      return std::make_pair(collected, pg_primary);
    };
  acting = new_acting;
  std::tie(actingset, primary) = collect_pg_shards(acting, new_acting_primary);
  ceph_assert(primary.osd == new_acting_primary);
  up = new_up;
  std::tie(upset, up_primary) = collect_pg_shards(up, new_up_primary);
  ceph_assert(up_primary.osd == new_up_primary);
}

epoch_t PG::get_osdmap_epoch() const
{
  return osdmap->get_epoch();
}

pg_shard_t PG::get_whoami() const
{
  return whoami;
}

const pg_info_t& PG::get_info() const
{
  return info;
}

const pg_stat_t& PG::get_stats() const
{
  return info.stats;
}

void PG::clear_state(uint64_t mask)
{
  if (!test_state(mask))
    return;
  info.stats.state &= ~mask;
  const auto now = utime_t{coarse_real_clock::now()};
  info.stats.last_change = now;
  if (mask & PG_STATE_ACTIVE) {
    info.stats.last_active = now;
  }
}

bool PG::test_state(uint64_t mask) const
{
  return info.stats.state & mask;
}

void PG::set_state(uint64_t mask)
{
  if (test_state(mask)) {
    return;
  }
  info.stats.state |= mask;
  const auto now = utime_t{coarse_real_clock::now()};
  info.stats.last_change = now;
  if (mask & PG_STATE_ACTIVE) {
    info.stats.last_became_active = now;
    if (active_promise) {
      std::move(active_promise)->set_value();
      active_promise.reset();
    }
  }
  if (mask & (PG_STATE_ACTIVE | PG_STATE_PEERED) &&
      test_state(PG_STATE_ACTIVE | PG_STATE_PEERED)) {
    info.stats.last_became_peered = now;
  }
  if (mask & PG_STATE_CLEAN) {
    info.stats.last_epoch_clean = get_osdmap_epoch();
  }
}

const PastIntervals& PG::get_past_intervals() const
{
  return past_intervals;
}

pg_shard_t PG::get_primary() const
{
  return primary;
}

bool PG::is_primary() const
{
  return whoami == primary;
}


namespace {
  bool has_shard(bool ec, const vector<int>& osds, pg_shard_t pg_shard)
  {
    if (ec) {
      return (osds.size() > static_cast<unsigned>(pg_shard.shard) &&
              osds[pg_shard.shard] == pg_shard.osd);
    } else {
      return std::find(osds.begin(), osds.end(), pg_shard.osd) != osds.end();
    }
  }
}

bool PG::is_acting(pg_shard_t pg_shard) const
{
  return has_shard(pool.is_erasure(), acting, pg_shard);
}

bool PG::is_up(pg_shard_t pg_shard) const
{
  return has_shard(pool.is_erasure(), up, pg_shard);
}

epoch_t PG::get_last_peering_reset() const
{
  return last_peering_reset;
}

void PG::update_last_peering_reset()
{
  last_peering_reset = get_osdmap_epoch();
}

epoch_t PG::get_need_up_thru() const
{
  return need_up_thru;
}

void PG::update_need_up_thru(const OSDMap* o)
{
  if (!o) {
    o = osdmap.get();
  }
  if (auto up_thru = o->get_up_thru(whoami.osd);
      up_thru < info.history.same_interval_since) {
    logger().info("up_thru {} < same_since {}, must notify monitor",
                  up_thru, info.history.same_interval_since);
    need_up_thru = info.history.same_interval_since;
  } else {
    logger().info("up_thru {} >= same_since {}, all is well",
                  up_thru, info.history.same_interval_since);
    need_up_thru = 0;
  }
}

std::vector<int>
PG::calc_acting(pg_shard_t auth_shard,
                const vector<int>& acting,
                const map<pg_shard_t, pg_info_t>& all_info) const
{
  // select primary
  auto auth_log_shard = all_info.find(auth_shard);
  auto primary = all_info.find(up_primary);
  if (up.empty() ||
      primary->second.is_incomplete() ||
      primary->second.last_update < auth_log_shard->second.log_tail) {
    ceph_assert(!auth_log_shard->second.is_incomplete());
    logger().info("up[0] needs backfill, osd.{} selected as primary instead",
                  auth_shard);
    primary = auth_log_shard;
  }
  auto& [primary_shard_id, primary_info] = *primary;
  logger().info("primary is osd.{} with {}",
                primary_shard_id.osd, primary_info);

  vector<int> want{primary_shard_id.osd};
  // We include auth_log_shard->second.log_tail because in GetLog,
  // we will request logs back to the min last_update over our
  // acting_backfill set, which will result in our log being extended
  // as far backwards as necessary to pick up any peers which can
  // be log recovered by auth_log_shard's log
  auto oldest_auth_log_entry =
    std::min(primary_info.log_tail, auth_log_shard->second.log_tail);
  // select replicas that have log contiguity with primary.
  // prefer up, then acting, then any peer_info osds
  auto get_shard = [](int osd) {
    return pg_shard_t{osd, shard_id_t::NO_SHARD}; };
  auto get_info = [&](int osd) -> const pg_info_t& {
    return all_info.find(get_shard(osd))->second; };
  auto is_good = [&, oldest_auth_log_entry](int osd) {
    auto& info = get_info(osd);
    return (!info.is_incomplete() &&
            info.last_update >= oldest_auth_log_entry);
  };
  auto is_enough = [size=pool.get_size(), &want](int) {
    return want.size() >= size;
  };
  std::vector<std::reference_wrapper<const vector<int>>> covered;
  auto has_covered = [primary=primary_shard_id.osd, &covered](int osd) {
    if (osd == primary)
      return true;
    for (auto& c : covered) {
      if (std::find(c.get().begin(), c.get().end(), osd) != c.get().end()) {
        return true;
      }
    }
    return false;
  };
  boost::copy((up |
               boost::adaptors::filtered(std::not_fn(is_enough)) |
               boost::adaptors::filtered(std::not_fn(has_covered)) |
               boost::adaptors::filtered(is_good)),
              std::back_inserter(want));
  if (is_enough(0))
    return want;
  covered.push_back(std::cref(up));
  // let's select from acting. the later "last_update" is, the better
  // sort by last_update, in descending order.
  using cands_sorted_by_eversion_t = std::map<eversion_t,
                                              pg_shard_t,
                                              std::greater<eversion_t>>;
  auto shard_to_osd = [](const pg_shard_t& shard) { return shard.osd; };
  {
    // This no longer has backfill OSDs, as they are covered above.
    auto cands = boost::accumulate(
      (acting |
       boost::adaptors::filtered(std::not_fn(is_enough)) |
       boost::adaptors::filtered(std::not_fn(has_covered)) |
       boost::adaptors::filtered(is_good)),
      cands_sorted_by_eversion_t{},
      [&](cands_sorted_by_eversion_t& cands, int osd) {
        cands.emplace(get_info(osd).last_update, get_shard(osd));
        return std::move(cands);
      });
    boost::copy(cands |
                boost::adaptors::map_values |
                boost::adaptors::transformed(shard_to_osd),
                std::back_inserter(want));
    if (is_enough(0)) {
      return want;
    }
    covered.push_back(std::cref(acting));
  }
  // continue to search stray for more suitable peers
  {
    auto pi_to_osd = [](const peer_info_t::value_type& pi) {
      return pi.first.osd; };
    auto cands = boost::accumulate(
      (all_info |
       boost::adaptors::transformed(pi_to_osd) |
       boost::adaptors::filtered(std::not_fn(is_enough)) |
       boost::adaptors::filtered(std::not_fn(has_covered)) |
       boost::adaptors::filtered(is_good)),
      cands_sorted_by_eversion_t{},
      [&](cands_sorted_by_eversion_t& cands, int osd) {
        cands.emplace(get_info(osd).last_update, get_shard(osd));
        return cands;
      });
    boost::copy(cands |
                boost::adaptors::map_values |
                boost::adaptors::transformed(shard_to_osd),
                std::back_inserter(want));
  }
  return want;
}

bool PG::proc_replica_info(pg_shard_t from,
                           const pg_info_t& pg_info,
                           epoch_t send_epoch)
{

  if (auto found = peer_info.find(from);
      found != peer_info.end() &&
      found->second.last_update == pg_info.last_update) {
    logger().info("got info {} from osd.{}, identical to ours",
                  info, from);
    return false;
  } else if (!osdmap->has_been_up_since(from.osd, send_epoch)) {
    logger().info("got info {} from down osd.{}. discarding",
                  info, from);
    return false;
  } else {
    logger().info("got info {} from osd.{}", info, from);
    peer_info.emplace(from, pg_info);
    return true;
  }
}

void PG::proc_replica_log(pg_shard_t from,
                          const pg_info_t& pg_info,
                          const pg_log_t& pg_log,
                          const pg_missing_t& pg_missing)
{

  logger().info("{} for osd.{}: {} {} {}", from, pg_info, pg_log, pg_missing);
  peer_info.insert_or_assign(from, pg_info);
}

// Returns an iterator to the best info in infos sorted by:
//  1) Prefer newer last_update
//  2) Prefer longer tail if it brings another info into contiguity
//  3) Prefer current primary
pg_shard_t
PG::find_best_info(const PG::peer_info_t& infos) const
{
  // See doc/dev/osd_internals/last_epoch_started.rst before attempting
  // to make changes to this process.  Also, make sure to update it
  // when you find bugs!
  auto min_last_update_acceptable = eversion_t::max();
  epoch_t max_les = 0;
  for (auto& [shard, info] : infos) {
    if (max_les < info.history.last_epoch_started) {
      max_les = info.history.last_epoch_started;
    }
    if (!info.is_incomplete() &&
        max_les < info.last_epoch_started) {
      max_les = info.last_epoch_started;
    }
  }
  for (auto& [shard, info] : infos) {
    if (max_les <= info.last_epoch_started &&
        min_last_update_acceptable > info.last_update) {
      min_last_update_acceptable = info.last_update;
    }
  }
  if (min_last_update_acceptable == eversion_t::max()) {
    return pg_shard_t{};
  }
  // find osd with newest last_update (oldest for ec_pool).
  // if there are multiples, prefer
  //  - a longer tail, if it brings another peer into log contiguity
  //  - the current primary
  struct is_good {
    // boost::max_element() copies the filter function, so make it copyable
    eversion_t min_last_update_acceptable;
    epoch_t max_les;
    const PG* thiz;
    is_good(eversion_t min_lua, epoch_t max_les, const PG* thiz)
      : min_last_update_acceptable{min_lua}, max_les{max_les}, thiz{thiz} {}
    is_good(const is_good& rhs) = default;
    is_good& operator=(const is_good& rhs) = default;
    bool operator()(const PG::peer_info_t::value_type& pi) const {
      auto& [shard, info] = pi;
      if (!thiz->is_up(shard) && !thiz->is_acting(shard)) {
        return false;
        // Only consider peers with last_update >= min_last_update_acceptable
      } else if (info.last_update < min_last_update_acceptable) {
        return false;
        // Disqualify anyone with a too old last_epoch_started
      } else if (info.last_epoch_started < max_les) {
        return false;
        // Disqualify anyone who is incomplete (not fully backfilled)
      } else if (info.is_incomplete()) {
        return false;
      } else {
        return true;
      }
    }
  };
  auto better = [require_rollback=pool.require_rollback(), this]
    (const PG::peer_info_t::value_type& lhs,
     const PG::peer_info_t::value_type& rhs) {
    if (require_rollback) {
      // prefer older last_update for ec_pool
      if (lhs.second.last_update > rhs.second.last_update) {
        return true;
      } else if (lhs.second.last_update < rhs.second.last_update) {
        return false;
      }
    } else {
      // prefer newer last_update for replica pool
      if (lhs.second.last_update > rhs.second.last_update) {
        return false;
      } else if (lhs.second.last_update < rhs.second.last_update) {
        return true;
      }
    }
    // Prefer longer tail
    if (lhs.second.log_tail > rhs.second.log_tail) {
      return true;
    } else if (lhs.second.log_tail < rhs.second.log_tail) {
      return false;
    }
    // prefer complete to missing
    if (lhs.second.has_missing() && !rhs.second.has_missing()) {
      return true;
    } else if (!lhs.second.has_missing() && rhs.second.has_missing()) {
      return false;
    }
    // prefer current primary (usually the caller), all things being equal
    if (rhs.first == whoami) {
      return true;
    } else if (lhs.first == whoami) {
      return false;
    }
    return false;
  };
  auto good_infos =
    (infos | boost::adaptors::filtered(is_good{min_last_update_acceptable,
                                               max_les, this}));
  if (good_infos.empty()) {
    return pg_shard_t{};
  } else {
    return boost::max_element(good_infos, better)->first;
  }
}

std::pair<PG::choose_acting_t, pg_shard_t> PG::choose_acting()
{
  auto all_info = peer_info;
  all_info.emplace(whoami, info);

  auto auth_log_shard = find_best_info(all_info);
  if (auth_log_shard.is_undefined()) {
    if (up != acting) {
      logger().info("{} no suitable info found (incomplete backfills?), "
                    "reverting to up", __func__);
      want_acting = up;
      // todo: reset pg_temp
      return {choose_acting_t::should_change, auth_log_shard};
    } else {
      logger().info("{} failed ", __func__);
      ceph_assert(want_acting.empty());
      return {choose_acting_t::pg_incomplete, auth_log_shard};
    }
  }

  auto want = calc_acting(auth_log_shard, acting, all_info);
  if (want != acting) {
    logger().info("{} want {} != acting {}, requesting pg_temp change",
                  __func__, want, acting);
    want_acting = std::move(want);
    // todo: update pg temp
    return {choose_acting_t::should_change, auth_log_shard};
  } else {
    logger().info("{} want={}", __func__, want);
    want_acting.clear();
    acting_recovery_backfill.clear();
    std::transform(want.begin(), want.end(),
      std::inserter(acting_recovery_backfill, acting_recovery_backfill.end()),
      [](int osd) { return pg_shard_t{osd, shard_id_t::NO_SHARD}; });
    return {choose_acting_t::dont_change, auth_log_shard};
  }
}

bool PG::should_send_notify() const
{
  return should_notify_primary && primary.osd >= 0;
}

pg_notify_t PG::get_notify(epoch_t query_epoch) const
{
  return pg_notify_t{primary.shard,
                     whoami.shard,
                     query_epoch,
                     get_osdmap_epoch(),
                     info};
}

bool PG::is_last_activated_peer(pg_shard_t peer)
{
  if (!acting_recovery_backfill.count(peer))
    return false;
  if (!peer_activated.insert(peer).second)
    return false;
  logger().info("peer osd.{} activated and committed", peer);
  return peer_activated.size() == acting_recovery_backfill.size();
}


void PG::clear_primary_state()
{
  peer_info.clear();
  want_acting.clear();
  need_up_thru = 0;
  peer_activated.clear();
}

bool PG::should_restart_peering(int new_up_primary,
                                int new_acting_primary,
                                const std::vector<int>& new_up,
                                const std::vector<int>& new_acting,
                                cached_map_t last_map,
                                cached_map_t osd_map) const
{
  auto pgid = info.pgid.pgid;
  auto pool = last_map->get_pg_pool(pgid.pool());
  if (!pool) {
    return false;
  }
  auto new_pool = osd_map->get_pg_pool(pgid.pool());
  if (!new_pool) {
    return true;
  }
  if (PastIntervals::is_new_interval(
        primary.osd,
        new_acting_primary,
        acting,
        new_acting,
        up_primary.osd,
        new_up_primary,
        up,
        new_up,
        pool->size,
        new_pool->size,
        pool->min_size,
        new_pool->min_size,
        pool->get_pg_num(),
        new_pool->get_pg_num(),
        pool->get_pg_num_pending(),
        new_pool->get_pg_num_pending(),
        last_map->test_flag(CEPH_OSDMAP_SORTBITWISE),
        osd_map->test_flag(CEPH_OSDMAP_SORTBITWISE),
        last_map->test_flag(CEPH_OSDMAP_RECOVERY_DELETES),
        osd_map->test_flag(CEPH_OSDMAP_RECOVERY_DELETES),
        pgid)) {
    logger().info("new interval new_up {} new_acting {}",
                  new_up, new_acting);
    return true;
  }
  if (!last_map->is_up(whoami.osd) && osd_map->is_up(whoami.osd)) {
    logger().info(" osd transitioned from down -> up");
    return true;
  }
  return false;
}

template<class T>
bool compare_n_set(T& v, const T& new_v)
{
  if (v != new_v) {
    v = new_v;
    return true;
  } else {
    return false;
  }
}

void PG::start_peering_interval(int new_up_primary,
                                int new_acting_primary,
                                const std::vector<int>& new_up,
                                const std::vector<int>& new_acting,
                                cached_map_t last_map)
{
  // todo
  update_last_peering_reset();

  auto old_acting_primary = primary;
  auto old_acting = std::move(acting);
  auto old_up_primary = up_primary;
  auto old_up = std::move(up);

  update_primary_state(new_up, new_up_primary,
                       new_acting, new_acting_primary);
  if (compare_n_set(info.stats.up, up) +
      compare_n_set(info.stats.up_primary, up_primary.osd) +
      compare_n_set(info.stats.acting, acting) +
      compare_n_set(info.stats.acting_primary, primary.osd)) {
    info.stats.mapping_epoch = osdmap->get_epoch();
  }
  if (old_up_primary != up_primary || old_up != up) {
    info.history.same_up_since = osdmap->get_epoch();
  }
  // this comparison includes primary rank via pg_shard_t
  if (old_acting_primary != get_primary()) {
    info.history.same_primary_since = osdmap->get_epoch();
  }
  // todo: always start a new interval
  info.history.same_interval_since = osdmap->get_epoch();
  // This will now be remapped during a backfill in cases
  // that it would not have been before.
  if (up != acting) {
    set_state(PG_STATE_REMAPPED);
  } else {
    clear_state(PG_STATE_REMAPPED);
  }
  // deactivate.
  clear_state(PG_STATE_ACTIVE);
  clear_state(PG_STATE_PEERED);
  clear_state(PG_STATE_DOWN);

  acting_recovery_backfill.clear();
  // should we tell the primary we are here?
  should_notify_primary = !is_primary();
}

void PG::activate(epoch_t activation_epoch)
{
  clear_state(PG_STATE_DOWN);

  if (is_primary()) {
    // only update primary last_epoch_started if we will go active
    if (acting.size() >= pool.min_size) {
      info.last_epoch_started = activation_epoch;
      info.last_interval_started = info.history.same_interval_since;
    }
  } else if (is_acting(whoami)) {
    // update last_epoch_started on acting replica to whatever the primary sent
    // unless it's smaller (could happen if we are going peered rather than
    // active, see doc/dev/osd_internals/last_epoch_started.rst)
    if (info.last_epoch_started < activation_epoch) {
      info.last_epoch_started = activation_epoch;
      info.last_interval_started = info.history.same_interval_since;
    }
  }
  if (is_primary()) {
    // start up replicas
    seastar::do_for_each(
      acting_recovery_backfill.begin(),
      acting_recovery_backfill.end(),
      [this](pg_shard_t peer) { return activate_peer(peer); });
    set_state(PG_STATE_ACTIVATING);
  } else {
    // todo: write/commit pg log, activate myself, and then tell primary
    on_activated();
    pg_notify_t notify{get_primary().shard,
                       whoami.shard,
                       get_osdmap_epoch(),
                       get_osdmap_epoch(),
                       info};
    auto m = make_message<MOSDPGInfo>(
      get_osdmap_epoch(),
      MOSDPGInfo::pg_list_t{make_pair(std::move(notify), PastIntervals{})});
    send_to_osd(get_primary().osd, std::move(m), get_osdmap_epoch());
  }
  // todo:
  info.last_complete = info.last_update;
  update_need_up_thru();
}

void PG::on_activated()
{
  if (acting.size() >= pool.min_size) {
    set_state(PG_STATE_ACTIVE);
  } else {
    set_state(PG_STATE_PEERED);
  }
}

seastar::future<> PG::activate_peer(pg_shard_t peer)
{
  if (peer == whoami) {
    // todo: write/commit pg log
    peer_activated.insert(whoami);
    return seastar::now();
  }
  auto& pi = peer_info[peer];
  MOSDPGLog* m = nullptr;
  if (pi.last_update == info.last_update) {
    // empty log
    logger().info("activate peer osd.{} is up to date, "
                  "but sending pg_log anyway", peer);
    m = new MOSDPGLog{peer.shard,
                      whoami.shard,
                      get_osdmap_epoch(),
                      get_info(),
                      get_last_peering_reset()};
  } else if (pi.last_backfill.is_min()) {
    logger().info("starting backfill to osd.{} from ({},{}] {} to {}", peer,
                  pi.log_tail, pi.last_update,
                  pi.last_backfill, info.last_update);
    // backfill
    pi.last_update = info.last_update;
    pi.last_complete = info.last_update;
    pi.last_epoch_started = info.last_epoch_started;
    pi.last_interval_started = info.last_interval_started;
    pi.history = info.history;
    pi.hit_set = info.hit_set;
    pi.stats.stats.clear();
    pi.purged_snaps = info.purged_snaps;
    m = new MOSDPGLog{peer.shard,
                      whoami.shard,
                      get_osdmap_epoch(),
                      pi,
                      get_last_peering_reset()};
  } else {
    // catch up
    logger().info("send missing log to osd.{}", peer);
    m = new MOSDPGLog{peer.shard,
                      whoami.shard,
                      get_osdmap_epoch(),
                      get_info(),
                      get_last_peering_reset()};
    // todo. send pg_log
    pi.last_update = info.last_update;
  }
  return send_to_osd(peer.osd, Ref<Message>{m, false}, get_osdmap_epoch());
}

void PG::maybe_mark_clean()
{
  if (actingset.size() == osdmap->get_pg_size(pgid.pgid)) {
    set_state(PG_STATE_CLEAN);
    info.history.last_epoch_clean = get_osdmap_epoch();
    info.history.last_interval_clean = info.history.same_interval_since;
  }
}

seastar::future<> PG::do_peering_event(std::unique_ptr<PGPeeringEvent> evt)
{
  return dispatch_context(recovery_state.handle_event(evt->get_event()));
}

seastar::future<> PG::dispatch_context(recovery::Context&& ctx)
{
  return seastar::do_with(recovery::Context{ctx}, [this](auto& todo) {
    return seastar::when_all_succeed(
      seastar::parallel_for_each(std::move(todo.notifies),
        [this](auto& osd_notifies) {
          auto& [peer, notifies] = osd_notifies;
          auto m = make_message<MOSDPGNotify>(get_osdmap_epoch(),
                                              std::move(notifies));
          return send_to_osd(peer, m, get_osdmap_epoch());
        }),
      seastar::parallel_for_each(std::move(todo.queries),
        [this](auto& osd_queries) {
          auto& [peer, queries] = osd_queries;
          auto m = make_message<MOSDPGQuery>(get_osdmap_epoch(),
                                             std::move(queries));
          return send_to_osd(peer, m, get_osdmap_epoch());
        }),
      seastar::parallel_for_each(std::move(todo.infos),
        [this](auto& osd_infos) {
          auto& [peer, infos] = osd_infos;
          auto m = make_message<MOSDPGInfo>(get_osdmap_epoch(),
                                            std::move(infos));
          return send_to_osd(peer, m, get_osdmap_epoch());
        })
    );
  });
}

seastar::future<> PG::handle_advance_map(cached_map_t next_map)
{
  auto last_map = std::move(osdmap);
  osdmap = std::move(next_map);
  vector<int> new_up, new_acting;
  int up_primary, acting_primary;
  osdmap->pg_to_up_acting_osds(pgid.pgid,
                               &new_up,
                               &up_primary,
                               &new_acting,
                               &acting_primary);
  logger().info("handle_advance_map {}/{} -- {}/{}",
                new_up, new_acting, up_primary, acting_primary);
  recovery_state.handle_event(AdvMap{osdmap, last_map,
                                     std::move(new_up), up_primary,
                                     std::move(new_acting), acting_primary});
  return seastar::now();
}

seastar::future<> PG::handle_activate_map()
{
  recovery_state.handle_event(ActMap{});
  return seastar::now();
}

void PG::print(ostream& out) const
{
  out << "pg[" << info
      << " " << up;
  if (acting != up)
    out << "/" << acting;
  out << " lpr=" << last_peering_reset
      << " " << pg_state_string(info.stats.state)
      << "]";
}


std::ostream& operator<<(std::ostream& os, const PG& pg)
{
  pg.print(os);
  return os;
}

void PG::reply_pg_query(const MQuery& query, recovery::Context* ctx)
{
  switch (query.query.type) {
  case pg_query_t::INFO:
    return reply_pg_query_for_info(query, ctx);
  case pg_query_t::LOG:
    return reply_pg_query_for_log(query, false);
  case pg_query_t::FULLLOG:
    return reply_pg_query_for_log(query, true);
  }
}

void PG::reply_pg_query_for_info(const MQuery& query, recovery::Context* ctx)
{
  recovery::Context::notify_t notify{pg_notify_t{query.from.shard,
                                                 whoami.shard,
                                                 query.query_epoch,
                                                 get_osdmap_epoch(),
                                                 info},
                                     past_intervals};
  ctx->notifies[query.from.osd].push_back(std::move(notify));
}

void PG::reply_pg_query_for_log(const MQuery& query, bool full)
{
  auto m = make_message<MOSDPGLog>(query.from.shard,
                                   whoami.shard,
                                   get_osdmap_epoch(),
                                   info,
                                   query.query_epoch);
  // todo:
  // m->missing = pg_log.get_missing();
  if (full) {
    // m->log = pg_log.get_log();
  } else {
    // maybe partial
  }
  send_to_osd(query.from.osd, m, get_osdmap_epoch());
}

seastar::future<> PG::send_to_osd(int peer, Ref<Message> m, epoch_t from_epoch)
{
  if (osdmap->is_down(peer) || osdmap->get_info(peer).up_from > from_epoch) {
    return seastar::now();
  } else {
    return msgr.connect(osdmap->get_cluster_addrs(peer).legacy_addr(),
                        CEPH_ENTITY_TYPE_OSD)
     .then([m, this] (auto xconn) {
       return (*xconn)->send(m);
     });
  }
}

seastar::future<> PG::share_pg_info()
{
  return seastar::do_for_each(
    acting_recovery_backfill.begin(),
    acting_recovery_backfill.end(),
    [this](pg_shard_t peer) {
      if (peer == whoami) return seastar::now();
      if (auto pi = peer_info.find(peer); pi != peer_info.end()) {
        pi->second.last_epoch_started = info.last_epoch_started;
        pi->second.last_interval_started = info.last_interval_started;
        pi->second.history.merge(info.history);
      }
      pg_notify_t notify{peer.shard,
                         whoami.shard,
                         get_osdmap_epoch(),
                         get_osdmap_epoch(),
                         info};
      auto m = make_message<MOSDPGInfo>(
        get_osdmap_epoch(),
        MOSDPGInfo::pg_list_t{make_pair(std::move(notify),
                                        past_intervals)});
      return send_to_osd(peer.osd, m, get_osdmap_epoch());
    });
}

seastar::future<> PG::wait_for_active()
{
  logger().info("wait_for_active: {}", pg_state_string(info.stats.state));
  if (test_state(PG_STATE_ACTIVE)) {
    return seastar::now();
  } else {
    if (!active_promise) {
      active_promise = seastar::shared_promise<>();
    }
    return active_promise->get_shared_future();
  }
}

seastar::future<>
PG::do_osd_op(const object_info_t& oi, OSDOp* osd_op)
{
  switch (const auto& op = osd_op->op; op.op) {
  case CEPH_OSD_OP_SYNC_READ:
    [[fallthrough]];
  case CEPH_OSD_OP_READ:
    return backend->read(oi,
                         op.extent.offset,
                         op.extent.length,
                         op.extent.truncate_size,
                         op.extent.truncate_seq,
                         op.flags).then([osd_op](bufferlist bl) {
      osd_op->rval = bl.length();
      osd_op->outdata = std::move(bl);
      return seastar::now();
    });
  default:
    return seastar::now();
  }
}

seastar::future<Ref<MOSDOpReply>> PG::do_osd_ops(Ref<MOSDOp> m)
{
  // todo: issue requests in parallel if they don't write,
  // with writes being basically a synchronization barrier
  return seastar::do_with(std::move(m), [this](auto& m) {
    return seastar::do_for_each(begin(m->ops), end(m->ops),
                                [m,this](OSDOp& osd_op) {
      const auto oid = (m->get_snapid() == CEPH_SNAPDIR ?
                        m->get_hobj().get_head() :
                        m->get_hobj());
      return backend->get_object(oid).then([&osd_op,this](auto oi) {
        return do_osd_op(*oi, &osd_op);
      }).handle_exception_type([&osd_op](const object_not_found&) {
        osd_op.rval = -ENOENT;
        throw;
      });
    }).then([=] {
      auto reply = make_message<MOSDOpReply>(m.get(), 0, get_osdmap_epoch(),
                                             0, false);
      reply->add_flags(CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK);
      return seastar::make_ready_future<Ref<MOSDOpReply>>(std::move(reply));
    }).handle_exception_type([=](const object_not_found& dne) {
      auto reply = make_message<MOSDOpReply>(m.get(), -ENOENT, get_osdmap_epoch(),
                                             0, false);
      reply->set_enoent_reply_versions(info.last_update,
                                       info.last_user_version);
      return seastar::make_ready_future<Ref<MOSDOpReply>>(std::move(reply));
    });
  });
}

seastar::future<> PG::handle_op(ceph::net::ConnectionRef conn,
                                Ref<MOSDOp> m)
{
  return wait_for_active().then([conn, m, this] {
    if (m->finish_decode()) {
      m->clear_payload();
    }
    return do_osd_ops(m);
  }).then([conn](Ref<MOSDOpReply> reply) {
    return conn->send(reply);
  });
}
