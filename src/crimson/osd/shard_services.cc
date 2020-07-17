// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/osd/shard_services.h"

#include "messages/MOSDAlive.h"

#include "osd/osd_perf_counters.h"
#include "osd/PeeringState.h"
#include "crimson/common/config_proxy.h"
#include "crimson/mgr/client.h"
#include "crimson/mon/MonClient.h"
#include "crimson/net/Messenger.h"
#include "crimson/net/Connection.h"
#include "crimson/os/cyanstore/cyan_store.h"
#include "crimson/osd/osdmap_service.h"
#include "messages/MOSDPGTemp.h"
#include "messages/MOSDPGCreated.h"
#include "messages/MOSDPGNotify.h"
#include "messages/MOSDPGInfo.h"
#include "messages/MOSDPGQuery.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

namespace crimson::osd {

ShardServices::ShardServices(
  OSDMapService &osdmap_service,
  const int whoami,
  crimson::net::Messenger &cluster_msgr,
  crimson::net::Messenger &public_msgr,
  crimson::mon::Client &monc,
  crimson::mgr::Client &mgrc,
  crimson::os::FuturizedStore &store)
    : osdmap_service(osdmap_service),
      whoami(whoami),
      cluster_msgr(cluster_msgr),
      public_msgr(public_msgr),
      monc(monc),
      mgrc(mgrc),
      store(store),
      throttler(crimson::common::local_conf()),
      obc_registry(crimson::common::local_conf()),
      local_reserver(
	&cct,
	&finisher,
	crimson::common::local_conf()->osd_max_backfills,
	crimson::common::local_conf()->osd_min_recovery_priority),
      remote_reserver(
	&cct,
	&finisher,
	crimson::common::local_conf()->osd_max_backfills,
	crimson::common::local_conf()->osd_min_recovery_priority)
{
  perf = build_osd_logger(&cct);
  cct.get_perfcounters_collection()->add(perf);

  recoverystate_perf = build_recoverystate_perf(&cct);
  cct.get_perfcounters_collection()->add(recoverystate_perf);

  crimson::common::local_conf().add_observer(this);
}

const char** ShardServices::get_tracked_conf_keys() const
{
  static const char* KEYS[] = {
    "osd_max_backfills",
    "osd_min_recovery_priority",
    nullptr
  };
  return KEYS;
}

void ShardServices::handle_conf_change(const ConfigProxy& conf,
				       const std::set <std::string> &changed)
{
  if (changed.count("osd_max_backfills")) {
    local_reserver.set_max(conf->osd_max_backfills);
    remote_reserver.set_max(conf->osd_max_backfills);
  }
  if (changed.count("osd_min_recovery_priority")) {
    local_reserver.set_min_priority(conf->osd_min_recovery_priority);
    remote_reserver.set_min_priority(conf->osd_min_recovery_priority);
  }
}

seastar::future<> ShardServices::send_to_osd(
  int peer, Ref<Message> m, epoch_t from_epoch) {
  if (osdmap->is_down(peer)) {
    logger().info("{}: osd.{} is_down", __func__, peer);
    return seastar::now();
  } else if (osdmap->get_info(peer).up_from > from_epoch) {
    logger().info("{}: osd.{} {} > {}", __func__, peer,
		    osdmap->get_info(peer).up_from, from_epoch);
    return seastar::now();
  } else {
    auto conn = cluster_msgr.connect(
        osdmap->get_cluster_addrs(peer).front(), CEPH_ENTITY_TYPE_OSD);
    return conn->send(m);
  }
}

seastar::future<> ShardServices::dispatch_context_transaction(
  crimson::os::CollectionRef col, PeeringCtx &ctx) {
  auto ret = store.do_transaction(
    col,
    std::move(ctx.transaction));
  ctx.reset_transaction();
  return ret;
}

seastar::future<> ShardServices::dispatch_context_messages(
  BufferedRecoveryMessages &&ctx)
{
  auto ret = seastar::parallel_for_each(std::move(ctx.message_map),
    [this](auto& osd_messages) {
      auto& [peer, messages] = osd_messages;
      logger().debug("dispatch_context_messages sending messages to {}", peer);
      return seastar::parallel_for_each(
        std::move(messages), [=, peer=peer](auto& m) {
        return send_to_osd(peer, m, osdmap->get_epoch());
      });
    });
  ctx.message_map.clear();
  return ret;
}

seastar::future<> ShardServices::dispatch_context(
  crimson::os::CollectionRef col,
  PeeringCtx &&ctx)
{
  ceph_assert(col || ctx.transaction.empty());
  return seastar::when_all_succeed(
    dispatch_context_messages(
      BufferedRecoveryMessages{ceph_release_t::octopus, ctx}),
    col ? dispatch_context_transaction(col, ctx) : seastar::now());
}

void ShardServices::queue_want_pg_temp(pg_t pgid,
				    const vector<int>& want,
				    bool forced)
{
  auto p = pg_temp_pending.find(pgid);
  if (p == pg_temp_pending.end() ||
      p->second.acting != want ||
      forced) {
    pg_temp_wanted[pgid] = {want, forced};
  }
}

void ShardServices::remove_want_pg_temp(pg_t pgid)
{
  pg_temp_wanted.erase(pgid);
  pg_temp_pending.erase(pgid);
}

void ShardServices::_sent_pg_temp()
{
#ifdef HAVE_STDLIB_MAP_SPLICING
  pg_temp_pending.merge(pg_temp_wanted);
#else
  pg_temp_pending.insert(make_move_iterator(begin(pg_temp_wanted)),
			 make_move_iterator(end(pg_temp_wanted)));
#endif
  pg_temp_wanted.clear();
}

void ShardServices::requeue_pg_temp()
{
  unsigned old_wanted = pg_temp_wanted.size();
  unsigned old_pending = pg_temp_pending.size();
  _sent_pg_temp();
  pg_temp_wanted.swap(pg_temp_pending);
  logger().debug(
    "{}: {} + {} -> {}",
    __func__ ,
    old_wanted,
    old_pending,
    pg_temp_wanted.size());
}

std::ostream& operator<<(
  std::ostream& out,
  const ShardServices::pg_temp_t& pg_temp)
{
  out << pg_temp.acting;
  if (pg_temp.forced) {
    out << " (forced)";
  }
  return out;
}

seastar::future<> ShardServices::send_pg_temp()
{
  if (pg_temp_wanted.empty())
    return seastar::now();
  logger().debug("{}: {}", __func__, pg_temp_wanted);
  boost::intrusive_ptr<MOSDPGTemp> ms[2] = {nullptr, nullptr};
  for (auto& [pgid, pg_temp] : pg_temp_wanted) {
    auto& m = ms[pg_temp.forced];
    if (!m) {
      m = make_message<MOSDPGTemp>(osdmap->get_epoch());
      m->forced = pg_temp.forced;
    }
    m->pg_temp.emplace(pgid, pg_temp.acting);
  }
  return seastar::parallel_for_each(std::begin(ms), std::end(ms),
    [this](auto m) {
      if (m) {
	return monc.send_message(m);
      } else {
	return seastar::now();
      }
    }).then([this] {
      _sent_pg_temp();
    });
}

void ShardServices::update_map(cached_map_t new_osdmap)
{
  osdmap = std::move(new_osdmap);
}

ShardServices::cached_map_t &ShardServices::get_osdmap()
{
  return osdmap;
}

seastar::future<> ShardServices::send_pg_created(pg_t pgid)
{
  logger().debug(__func__);
  auto o = get_osdmap();
  ceph_assert(o->require_osd_release >= ceph_release_t::luminous);
  pg_created.insert(pgid);
  return monc.send_message(make_message<MOSDPGCreated>(pgid));
}

seastar::future<> ShardServices::send_pg_created()
{
  logger().debug(__func__);
  auto o = get_osdmap();
  ceph_assert(o->require_osd_release >= ceph_release_t::luminous);
  return seastar::parallel_for_each(pg_created,
    [this](auto &pgid) {
      return monc.send_message(make_message<MOSDPGCreated>(pgid));
    });
}

void ShardServices::prune_pg_created()
{
  logger().debug(__func__);
  auto o = get_osdmap();
  auto i = pg_created.begin();
  while (i != pg_created.end()) {
    auto p = o->get_pg_pool(i->pool());
    if (!p || !p->has_flag(pg_pool_t::FLAG_CREATING)) {
      logger().debug("{} pruning {}", __func__, *i);
      i = pg_created.erase(i);
    } else {
      logger().debug(" keeping {}", __func__, *i);
      ++i;
    }
  }
}

seastar::future<> ShardServices::osdmap_subscribe(version_t epoch, bool force_request)
{
  logger().info("{}({})", __func__, epoch);
  if (monc.sub_want_increment("osdmap", epoch, CEPH_SUBSCRIBE_ONETIME) ||
      force_request) {
    return monc.renew_subs();
  } else {
    return seastar::now();
  }
}

HeartbeatStampsRef ShardServices::get_hb_stamps(int peer)
{
  auto [stamps, added] = heartbeat_stamps.try_emplace(peer);
  if (added) {
    stamps->second = ceph::make_ref<HeartbeatStamps>(peer);
  }
  return stamps->second;
}

seastar::future<> ShardServices::send_alive(const epoch_t want)
{
  logger().info(
    "{} want={} up_thru_wanted={}",
    __func__,
    want,
    up_thru_wanted);

  if (want > up_thru_wanted) {
    up_thru_wanted = want;
  } else {
    logger().debug("{} want={} <= up_thru_wanted={}; skipping",
                   __func__, want, up_thru_wanted);
    return seastar::now();
  }
  if (!osdmap->exists(whoami)) {
    logger().warn("{} DNE", __func__);
    return seastar::now();
  } if (const epoch_t up_thru = osdmap->get_up_thru(whoami);
        up_thru_wanted > up_thru) {
    logger().debug("{} up_thru_wanted={} up_thru={}", __func__, want, up_thru);
    return monc.send_message(
      make_message<MOSDAlive>(osdmap->get_epoch(), want));
  } else {
    logger().debug("{} {} <= {}", __func__, want, osdmap->get_up_thru(whoami));
    return seastar::now();
  }
}

};
