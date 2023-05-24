// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <boost/smart_ptr/make_local_shared.hpp>

#include "crimson/osd/shard_services.h"

#include "messages/MOSDAlive.h"
#include "messages/MOSDMap.h"
#include "messages/MOSDPGCreated.h"
#include "messages/MOSDPGTemp.h"

#include "osd/osd_perf_counters.h"
#include "osd/PeeringState.h"
#include "crimson/common/config_proxy.h"
#include "crimson/mgr/client.h"
#include "crimson/mon/MonClient.h"
#include "crimson/net/Messenger.h"
#include "crimson/net/Connection.h"
#include "crimson/os/cyanstore/cyan_store.h"
#include "crimson/osd/osdmap_service.h"
#include "crimson/osd/osd_operations/pg_advance_map.h"
#include "crimson/osd/pg.h"
#include "crimson/osd/pg_meta.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

using std::vector;

namespace crimson::osd {

PerShardState::PerShardState(
  int whoami,
  ceph::mono_time startup_time,
  PerfCounters *perf,
  PerfCounters *recoverystate_perf,
  crimson::os::FuturizedStore &store)
  : whoami(whoami),
    store(store.get_sharded_store()),
    perf(perf), recoverystate_perf(recoverystate_perf),
    throttler(crimson::common::local_conf()),
    next_tid(
      static_cast<ceph_tid_t>(seastar::this_shard_id()) <<
      (std::numeric_limits<ceph_tid_t>::digits - 8)),
    startup_time(startup_time)
{}

seastar::future<> PerShardState::dump_ops_in_flight(Formatter *f) const
{
  registry.for_each_op([f](const auto &op) {
    op.dump(f);
  });
  return seastar::now();
}

seastar::future<> PerShardState::stop_pgs()
{
  assert_core();
  return seastar::parallel_for_each(
    pg_map.get_pgs(),
    [](auto& p) {
      return p.second->stop();
    });
}

std::map<pg_t, pg_stat_t> PerShardState::get_pg_stats() const
{
  assert_core();
  std::map<pg_t, pg_stat_t> ret;
  for (auto [pgid, pg] : pg_map.get_pgs()) {
    if (pg->is_primary()) {
      auto stats = pg->get_stats();
      // todo: update reported_epoch,reported_seq,last_fresh
      stats.reported_epoch = osdmap->get_epoch();
      ret.emplace(pgid.pgid, std::move(stats));
    }
  }
  return ret;
}

seastar::future<> PerShardState::broadcast_map_to_pgs(
  ShardServices &shard_services,
  epoch_t from,
  epoch_t to)
{
  assert_core();
  auto &pgs = pg_map.get_pgs();
  return seastar::parallel_for_each(
    pgs.begin(), pgs.end(),
    [=, &shard_services](auto& pg) {
      return shard_services.start_operation<PGAdvanceMap>(
	shard_services,
	pg.second, from, to,
	PeeringCtx{}, false).second;
    });
}

Ref<PG> PerShardState::get_pg(spg_t pgid)
{
  assert_core();
  return pg_map.get_pg(pgid);
}

HeartbeatStampsRef PerShardState::get_hb_stamps(int peer)
{
  assert_core();
  auto [stamps, added] = heartbeat_stamps.try_emplace(peer);
  if (added) {
    stamps->second = ceph::make_ref<HeartbeatStamps>(peer);
  }
  return stamps->second;
}

OSDSingletonState::OSDSingletonState(
  int whoami,
  crimson::net::Messenger &cluster_msgr,
  crimson::net::Messenger &public_msgr,
  crimson::mon::Client &monc,
  crimson::mgr::Client &mgrc)
  : whoami(whoami),
    osdmap_gate("OSDSingletonState::osdmap_gate"),
    cluster_msgr(cluster_msgr),
    public_msgr(public_msgr),
    monc(monc),
    mgrc(mgrc),
    local_reserver(
      &cct,
      &finisher,
      crimson::common::local_conf()->osd_max_backfills,
      crimson::common::local_conf()->osd_min_recovery_priority),
    remote_reserver(
      &cct,
      &finisher,
      crimson::common::local_conf()->osd_max_backfills,
      crimson::common::local_conf()->osd_min_recovery_priority),
    snap_reserver(
      &cct,
      &finisher,
      crimson::common::local_conf()->osd_max_trimming_pgs)
{
  crimson::common::local_conf().add_observer(this);
  osdmaps[0] = boost::make_local_shared<OSDMap>();

  perf = build_osd_logger(&cct);
  cct.get_perfcounters_collection()->add(perf);

  recoverystate_perf = build_recoverystate_perf(&cct);
  cct.get_perfcounters_collection()->add(recoverystate_perf);
}

seastar::future<> OSDSingletonState::send_to_osd(
  int peer, MessageURef m, epoch_t from_epoch)
{
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
    return conn->send(std::move(m));
  }
}

seastar::future<> OSDSingletonState::osdmap_subscribe(
  version_t epoch, bool force_request)
{
  logger().info("{}({})", __func__, epoch);
  if (monc.sub_want_increment("osdmap", epoch, CEPH_SUBSCRIBE_ONETIME) ||
      force_request) {
    return monc.renew_subs();
  } else {
    return seastar::now();
  }
}

void OSDSingletonState::queue_want_pg_temp(
  pg_t pgid,
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

void OSDSingletonState::remove_want_pg_temp(pg_t pgid)
{
  pg_temp_wanted.erase(pgid);
  pg_temp_pending.erase(pgid);
}

void OSDSingletonState::requeue_pg_temp()
{
  unsigned old_wanted = pg_temp_wanted.size();
  unsigned old_pending = pg_temp_pending.size();
  pg_temp_wanted.merge(pg_temp_pending);
  pg_temp_pending.clear();
  logger().debug(
    "{}: {} + {} -> {}",
    __func__ ,
    old_wanted,
    old_pending,
    pg_temp_wanted.size());
}

seastar::future<> OSDSingletonState::send_pg_temp()
{
  if (pg_temp_wanted.empty())
    return seastar::now();
  logger().debug("{}: {}", __func__, pg_temp_wanted);
  MURef<MOSDPGTemp> ms[2] = {nullptr, nullptr};
  for (auto& [pgid, pg_temp] : pg_temp_wanted) {
    auto& m = ms[pg_temp.forced];
    if (!m) {
      m = crimson::make_message<MOSDPGTemp>(osdmap->get_epoch());
      m->forced = pg_temp.forced;
    }
    m->pg_temp.emplace(pgid, pg_temp.acting);
  }
  pg_temp_pending.merge(pg_temp_wanted);
  pg_temp_wanted.clear();
  return seastar::parallel_for_each(std::begin(ms), std::end(ms),
    [this](auto& m) {
      if (m) {
	return monc.send_message(std::move(m));
      } else {
	return seastar::now();
      }
    });
}

std::ostream& operator<<(
  std::ostream& out,
  const OSDSingletonState::pg_temp_t& pg_temp)
{
  out << pg_temp.acting;
  if (pg_temp.forced) {
    out << " (forced)";
  }
  return out;
}

seastar::future<> OSDSingletonState::send_pg_created(pg_t pgid)
{
  logger().debug(__func__);
  auto o = get_osdmap();
  ceph_assert(o->require_osd_release >= ceph_release_t::luminous);
  pg_created.insert(pgid);
  return monc.send_message(crimson::make_message<MOSDPGCreated>(pgid));
}

seastar::future<> OSDSingletonState::send_pg_created()
{
  logger().debug(__func__);
  auto o = get_osdmap();
  ceph_assert(o->require_osd_release >= ceph_release_t::luminous);
  return seastar::parallel_for_each(pg_created,
    [this](auto &pgid) {
      return monc.send_message(crimson::make_message<MOSDPGCreated>(pgid));
    });
}

void OSDSingletonState::prune_pg_created()
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

seastar::future<> OSDSingletonState::send_alive(const epoch_t want)
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
      crimson::make_message<MOSDAlive>(osdmap->get_epoch(), want));
  } else {
    logger().debug("{} {} <= {}", __func__, want, osdmap->get_up_thru(whoami));
    return seastar::now();
  }
}

const char** OSDSingletonState::get_tracked_conf_keys() const
{
  static const char* KEYS[] = {
    "osd_max_backfills",
    "osd_min_recovery_priority",
    "osd_max_trimming_pgs",
    nullptr
  };
  return KEYS;
}

void OSDSingletonState::handle_conf_change(
  const ConfigProxy& conf,
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
  if (changed.count("osd_max_trimming_pgs")) {
    snap_reserver.set_max(conf->osd_max_trimming_pgs);
  }
}

seastar::future<OSDSingletonState::local_cached_map_t>
OSDSingletonState::get_local_map(epoch_t e)
{
  // TODO: use LRU cache for managing osdmap, fallback to disk if we have to
  if (auto found = osdmaps.find(e); found) {
    logger().debug("{} osdmap.{} found in cache", __func__, e);
    return seastar::make_ready_future<local_cached_map_t>(std::move(found));
  } else {
    logger().debug("{} loading osdmap.{} from disk", __func__, e);
    return load_map(e).then([e, this](std::unique_ptr<OSDMap> osdmap) {
      return seastar::make_ready_future<local_cached_map_t>(
	osdmaps.insert(e, std::move(osdmap)));
    });
  }
}

void OSDSingletonState::store_map_bl(
  ceph::os::Transaction& t,
  epoch_t e, bufferlist&& bl)
{
  meta_coll->store_map(t, e, bl);
  map_bl_cache.insert(e, std::move(bl));
}

seastar::future<bufferlist> OSDSingletonState::load_map_bl(
  epoch_t e)
{
  if (std::optional<bufferlist> found = map_bl_cache.find(e); found) {
    logger().debug("{} osdmap.{} found in cache", __func__, e);
    return seastar::make_ready_future<bufferlist>(*found);
  } else {
    logger().debug("{} loading osdmap.{} from disk", __func__, e);
    return meta_coll->load_map(e);
  }
}

seastar::future<std::map<epoch_t, bufferlist>> OSDSingletonState::load_map_bls(
  epoch_t first,
  epoch_t last)
{
  logger().debug("{} loading maps [{},{}]",
                 __func__, first, last);
  ceph_assert(first <= last);
  return seastar::map_reduce(boost::make_counting_iterator<epoch_t>(first),
			     boost::make_counting_iterator<epoch_t>(last + 1),
			     [this](epoch_t e) {
    return load_map_bl(e).then([e](auto&& bl) {
      return seastar::make_ready_future<std::pair<epoch_t, bufferlist>>(
	std::make_pair(e, std::move(bl)));
    });
  },
  std::map<epoch_t, bufferlist>{},
  [](auto&& bls, auto&& epoch_bl) {
    bls.emplace(std::move(epoch_bl));
    return std::move(bls);
  });
}

seastar::future<std::unique_ptr<OSDMap>> OSDSingletonState::load_map(epoch_t e)
{
  auto o = std::make_unique<OSDMap>();
  logger().info("{} osdmap.{}", __func__, e);
  if (e == 0) {
    return seastar::make_ready_future<std::unique_ptr<OSDMap>>(std::move(o));
  }
  return load_map_bl(e).then([o=std::move(o)](bufferlist bl) mutable {
    o->decode(bl);
    return seastar::make_ready_future<std::unique_ptr<OSDMap>>(std::move(o));
  });
}

seastar::future<> OSDSingletonState::store_maps(ceph::os::Transaction& t,
                                  epoch_t start, epoch_t last, Ref<MOSDMap> m)
{
  return seastar::do_for_each(
    boost::make_counting_iterator(start),
    boost::make_counting_iterator(last + 1),
    [&t, m, this](epoch_t e) {
      if (auto p = m->maps.find(e); p != m->maps.end()) {
	auto o = std::make_unique<OSDMap>();
	o->decode(p->second);
	logger().info("store_maps storing osdmap.{}", e);
	store_map_bl(t, e, std::move(std::move(p->second)));
	osdmaps.insert(e, std::move(o));
	return seastar::now();
      } else if (auto p = m->incremental_maps.find(e);
		 p != m->incremental_maps.end()) {
	ceph_assert (std::cmp_greater(e, 0u));
	logger().info("store_maps found osdmap.{} incremental map, "
	              "loading osdmap.{}", e, e - 1);
	return load_map(e - 1).then([e, bl=p->second, &t, this](auto o) {
	  OSDMap::Incremental inc;
	  auto i = bl.cbegin();
	  inc.decode(i);
	  o->apply_incremental(inc);
	  bufferlist fbl;
	  o->encode(fbl, inc.encode_features | CEPH_FEATURE_RESERVED);
	  logger().info("store_maps storing osdmap.{}", o->get_epoch());
	  store_map_bl(t, e, std::move(fbl));
	  osdmaps.insert(e, std::move(o));
	  return seastar::now();
	});
      } else {
	logger().error("MOSDMap lied about what maps it had?");
	return seastar::now();
      }
    });
}

seastar::future<Ref<PG>> ShardServices::make_pg(
  OSDMapService::cached_map_t create_map,
  spg_t pgid,
  bool do_create)
{
  using ec_profile_t = std::map<std::string, std::string>;
  auto get_pool_info_for_pg = [create_map, pgid, this] {
    if (create_map->have_pg_pool(pgid.pool())) {
      pg_pool_t pi = *create_map->get_pg_pool(pgid.pool());
      std::string name = create_map->get_pool_name(pgid.pool());
      ec_profile_t ec_profile;
      if (pi.is_erasure()) {
	ec_profile = create_map->get_erasure_code_profile(
	  pi.erasure_code_profile);
      }
      return seastar::make_ready_future<
	std::tuple<pg_pool_t,std::string, ec_profile_t>
	>(std::make_tuple(
	    std::move(pi),
	    std::move(name),
	    std::move(ec_profile)));
    } else {
      // pool was deleted; grab final pg_pool_t off disk.
      return get_pool_info(pgid.pool());
    }
  };
  auto get_collection = [pgid, do_create, this] {
    const coll_t cid{pgid};
    if (do_create) {
      return get_store().create_new_collection(cid);
    } else {
      return get_store().open_collection(cid);
    }
  };
  return seastar::when_all(
    std::move(get_pool_info_for_pg),
    std::move(get_collection)
  ).then([pgid, create_map, this](auto &&ret) {
    auto [pool, name, ec_profile] = std::move(std::get<0>(ret).get0());
    auto coll = std::move(std::get<1>(ret).get0());
    return seastar::make_ready_future<Ref<PG>>(
      new PG{
	pgid,
	pg_shard_t{local_state.whoami, pgid.shard},
	std::move(coll),
	std::move(pool),
	std::move(name),
	create_map,
	*this,
	ec_profile});
  });
}

seastar::future<Ref<PG>> ShardServices::handle_pg_create_info(
  std::unique_ptr<PGCreateInfo> info) {
  return seastar::do_with(
    std::move(info),
    [this](auto &info)
    -> seastar::future<Ref<PG>> {
      return get_map(info->epoch).then(
	[&info, this](cached_map_t startmap)
	-> seastar::future<std::tuple<Ref<PG>, cached_map_t>> {
	  const spg_t &pgid = info->pgid;
	  if (info->by_mon) {
	    int64_t pool_id = pgid.pgid.pool();
	    const pg_pool_t *pool = get_map()->get_pg_pool(pool_id);
	    if (!pool) {
	      logger().debug(
		"{} ignoring pgid {}, pool dne",
		__func__,
		pgid);
	      local_state.pg_map.pg_creation_canceled(pgid);
	      return seastar::make_ready_future<
		std::tuple<Ref<PG>, OSDMapService::cached_map_t>
		>(std::make_tuple(Ref<PG>(), startmap));
	    } else if (!pool->is_crimson()) {
	      logger().debug(
		"{} ignoring pgid {}, pool lacks crimson flag",
		__func__,
		pgid);
	      local_state.pg_map.pg_creation_canceled(pgid);
	      return seastar::make_ready_future<
		std::tuple<Ref<PG>, OSDMapService::cached_map_t>
		>(std::make_tuple(Ref<PG>(), startmap));
	    }
	    ceph_assert(get_map()->require_osd_release >=
			ceph_release_t::octopus);
	    if (!pool->has_flag(pg_pool_t::FLAG_CREATING)) {
	      // this ensures we do not process old creating messages after the
	      // pool's initial pgs have been created (and pg are subsequently
	      // allowed to split or merge).
	      logger().debug(
		"{} dropping {} create, pool does not have CREATING flag set",
		__func__,
		pgid);
	      local_state.pg_map.pg_creation_canceled(pgid);
	      return seastar::make_ready_future<
		std::tuple<Ref<PG>, OSDMapService::cached_map_t>
		>(std::make_tuple(Ref<PG>(), startmap));
	    }
	  }
	  return make_pg(
	    startmap, pgid, true
	  ).then([startmap=std::move(startmap)](auto pg) mutable {
	    return seastar::make_ready_future<
	      std::tuple<Ref<PG>, OSDMapService::cached_map_t>
	      >(std::make_tuple(std::move(pg), std::move(startmap)));
	  });
	}).then([this, &info](auto &&ret)
		->seastar::future<Ref<PG>> {
	  auto [pg, startmap] = std::move(ret);
	  if (!pg)
	    return seastar::make_ready_future<Ref<PG>>(Ref<PG>());
	  const pg_pool_t* pp = startmap->get_pg_pool(info->pgid.pool());

	  int up_primary, acting_primary;
	  vector<int> up, acting;
	  startmap->pg_to_up_acting_osds(
	    info->pgid.pgid, &up, &up_primary, &acting, &acting_primary);

	  int role = startmap->calc_pg_role(
	    pg_shard_t(local_state.whoami, info->pgid.shard),
	    acting);

	  PeeringCtx rctx;
	  create_pg_collection(
	    rctx.transaction,
	    info->pgid,
	    info->pgid.get_split_bits(pp->get_pg_num()));
	  init_pg_ondisk(
	    rctx.transaction,
	    info->pgid,
	    pp);

	  pg->init(
	    role,
	    up,
	    up_primary,
	    acting,
	    acting_primary,
	    info->history,
	    info->past_intervals,
	    rctx.transaction);

	  return start_operation<PGAdvanceMap>(
	    *this, pg, pg->get_osdmap_epoch(), get_map()->get_epoch(), std::move(rctx), true
	  ).second.then([pg=pg] {
	    return seastar::make_ready_future<Ref<PG>>(pg);
	  });
	});
    });
}


ShardServices::get_or_create_pg_ret
ShardServices::get_or_create_pg(
  PGMap::PGCreationBlockingEvent::TriggerI&& trigger,
  spg_t pgid,
  std::unique_ptr<PGCreateInfo> info)
{
  if (info) {
    auto [fut, creating] = local_state.pg_map.wait_for_pg(
      std::move(trigger), pgid);
    if (!creating) {
      local_state.pg_map.set_creating(pgid);
      (void)handle_pg_create_info(
	std::move(info));
    }
    return std::move(fut);
  } else {
    return get_or_create_pg_ret(
      get_or_create_pg_ertr::ready_future_marker{},
      local_state.pg_map.get_pg(pgid));
  }
}

ShardServices::wait_for_pg_ret
ShardServices::wait_for_pg(
  PGMap::PGCreationBlockingEvent::TriggerI&& trigger, spg_t pgid)
{
  return local_state.pg_map.wait_for_pg(std::move(trigger), pgid).first;
}

seastar::future<Ref<PG>> ShardServices::load_pg(spg_t pgid)

{
  logger().debug("{}: {}", __func__, pgid);

  return seastar::do_with(PGMeta(get_store(), pgid), [](auto& pg_meta) {
    return pg_meta.get_epoch();
  }).then([this](epoch_t e) {
    return get_map(e);
  }).then([pgid, this](auto&& create_map) {
    return make_pg(std::move(create_map), pgid, false);
  }).then([this](Ref<PG> pg) {
    return pg->read_state(&get_store()).then([pg] {
	return seastar::make_ready_future<Ref<PG>>(std::move(pg));
    });
  }).handle_exception([pgid](auto ep) {
    logger().info("pg {} saw exception on load {}", pgid, ep);
    ceph_abort("Could not load pg" == 0);
    return seastar::make_exception_future<Ref<PG>>(ep);
  });
}

seastar::future<> ShardServices::dispatch_context_transaction(
  crimson::os::CollectionRef col, PeeringCtx &ctx) {
  if (ctx.transaction.empty()) {
    logger().debug("ShardServices::dispatch_context_transaction: empty transaction");
    return seastar::now();
  }

  logger().debug("ShardServices::dispatch_context_transaction: do_transaction ...");
  auto ret = get_store().do_transaction(
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
        std::move(messages), [=, peer=peer, this](auto& m) {
        return send_to_osd(peer, std::move(m), local_state.osdmap->get_epoch());
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
      BufferedRecoveryMessages{ctx}),
    col ? dispatch_context_transaction(col, ctx) : seastar::now()
  ).then_unpack([] {
    return seastar::now();
  });
}

seastar::future<> OSDSingletonState::send_incremental_map(
  crimson::net::Connection &conn,
  epoch_t first)
{
  logger().info("{}: first osdmap: {} "
                "superblock's oldest map: {}",
                __func__, first, superblock.oldest_map);
  if (first >= superblock.oldest_map) {
    return load_map_bls(
      first, superblock.newest_map
    ).then([this, &conn, first](auto&& bls) {
      auto m = crimson::make_message<MOSDMap>(
	monc.get_fsid(),
	osdmap->get_encoding_features());
      m->cluster_osdmap_trim_lower_bound = first;
      m->newest_map = superblock.newest_map;
      m->maps = std::move(bls);
      return conn.send(std::move(m));
    });
  } else {
    return load_map_bl(osdmap->get_epoch()
    ).then([this, &conn](auto&& bl) mutable {
      auto m = crimson::make_message<MOSDMap>(
	monc.get_fsid(),
	osdmap->get_encoding_features());
      /* TODO: once we support the tracking of superblock's
       *       cluster_osdmap_trim_lower_bound, the MOSDMap should
       *       be populated with this value instead of the oldest_map.
       *       See: OSD::handle_osd_map for how classic updates the
       *       cluster's trim lower bound.
       */
      m->cluster_osdmap_trim_lower_bound = superblock.oldest_map;
      m->newest_map = superblock.newest_map;
      m->maps.emplace(osdmap->get_epoch(), std::move(bl));
      return conn.send(std::move(m));
    });
  }
}

seastar::future<> OSDSingletonState::send_incremental_map_to_osd(
  int osd,
  epoch_t first)
{
  if (osdmap->is_down(osd)) {
    logger().info("{}: osd.{} is_down", __func__, osd);
    return seastar::now();
  } else {
    auto conn = cluster_msgr.connect(
      osdmap->get_cluster_addrs(osd).front(), CEPH_ENTITY_TYPE_OSD);
    return send_incremental_map(*conn, first);
  }
}

};
