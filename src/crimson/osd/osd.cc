#include "osd.h"

#include <boost/iterator/counting_iterator.hpp>
#include <boost/range/join.hpp>
#include <boost/smart_ptr/make_local_shared.hpp>
#include <fmt/format.h>
#include <fmt/ostream.h>
#include "common/pick_address.h"

#include "messages/MOSDAlive.h"
#include "messages/MOSDBeacon.h"
#include "messages/MOSDBoot.h"
#include "messages/MOSDMap.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDPGInfo.h"
#include "messages/MOSDPGLog.h"
#include "messages/MOSDPGNotify.h"
#include "messages/MOSDPGQuery.h"
#include "messages/MPGStats.h"

#include "crimson/mon/MonClient.h"
#include "crimson/net/Connection.h"
#include "crimson/net/Messenger.h"
#include "crimson/os/cyan_collection.h"
#include "crimson/os/cyan_object.h"
#include "crimson/os/cyan_store.h"
#include "crimson/os/Transaction.h"
#include "crimson/osd/heartbeat.h"
#include "crimson/osd/osd_meta.h"
#include "crimson/osd/pg.h"
#include "crimson/osd/pg_backend.h"
#include "crimson/osd/pg_meta.h"

#include "osd/PGPeeringEvent.h"

namespace {
  seastar::logger& logger() {
    return ceph::get_logger(ceph_subsys_osd);
  }

  template<typename Message, typename... Args>
  Ref<Message> make_message(Args&&... args)
  {
    return {new Message{std::forward<Args>(args)...}, false};
  }
  static constexpr int TICK_INTERVAL = 1;
}

using ceph::common::local_conf;
using ceph::os::CyanStore;

OSD::OSD(int id, uint32_t nonce,
         ceph::net::Messenger& cluster_msgr,
         ceph::net::Messenger& public_msgr,
         ceph::net::Messenger& hb_front_msgr,
         ceph::net::Messenger& hb_back_msgr)
  : whoami{id},
    nonce{nonce},
    beacon_timer{[this] { send_beacon(); }},
    cluster_msgr{cluster_msgr},
    public_msgr{public_msgr},
    monc{new ceph::mon::Client{public_msgr}},
    mgrc{new ceph::mgr::Client{public_msgr, *this}},
    heartbeat{new Heartbeat{*this, *monc, hb_front_msgr, hb_back_msgr}},
    heartbeat_timer{[this] { update_heartbeat_peers(); }},
    store{std::make_unique<ceph::os::CyanStore>(
      local_conf().get_val<std::string>("osd_data"))}
{
  osdmaps[0] = boost::make_local_shared<OSDMap>();
}

OSD::~OSD() = default;

namespace {
// Initial features in new superblock.
// Features here are also automatically upgraded
CompatSet get_osd_initial_compat_set()
{
  CompatSet::FeatureSet ceph_osd_feature_compat;
  CompatSet::FeatureSet ceph_osd_feature_ro_compat;
  CompatSet::FeatureSet ceph_osd_feature_incompat;
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_BASE);
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_PGINFO);
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_OLOC);
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_LEC);
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_CATEGORIES);
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_HOBJECTPOOL);
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_BIGINFO);
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_LEVELDBINFO);
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_LEVELDBLOG);
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_SNAPMAPPER);
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_HINTS);
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_PGMETA);
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_MISSING);
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_FASTINFO);
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_RECOVERY_DELETES);
  return CompatSet(ceph_osd_feature_compat,
                   ceph_osd_feature_ro_compat,
                   ceph_osd_feature_incompat);
}
}

seastar::future<> OSD::mkfs(uuid_d cluster_fsid)
{
  return store->mkfs().then([this] {
    return store->mount();
  }).then([cluster_fsid, this] {
    superblock.cluster_fsid = cluster_fsid;
    superblock.osd_fsid = store->get_fsid();
    superblock.whoami = whoami;
    superblock.compat_features = get_osd_initial_compat_set();

    meta_coll = make_unique<OSDMeta>(
      store->create_new_collection(coll_t::meta()), store.get());
    ceph::os::Transaction t;
    meta_coll->create(t);
    meta_coll->store_superblock(t, superblock);
    return store->do_transaction(meta_coll->collection(), std::move(t));
  }).then([cluster_fsid, this] {
    store->write_meta("ceph_fsid", cluster_fsid.to_string());
    store->write_meta("whoami", std::to_string(whoami));
    fmt::print("created object store {} for osd.{} fsid {}\n",
               local_conf().get_val<std::string>("osd_data"),
               whoami, cluster_fsid);
    return seastar::now();
  });
}

namespace {
  entity_addrvec_t pick_addresses(int what) {
    entity_addrvec_t addrs;
    CephContext cct;
    if (int r = ::pick_addresses(&cct, what, &addrs, -1); r < 0) {
      throw std::runtime_error("failed to pick address");
    }
    // TODO: v2: ::pick_addresses() returns v2 addresses, but crimson-msgr does
    // not support v2 yet. remove following set_type() once v2 support is ready.
    for (auto addr : addrs.v) {
      addr.set_type(addr.TYPE_LEGACY);
      logger().info("picked address {}", addr);
    }
    return addrs;
  }
  std::pair<entity_addrvec_t, bool>
  replace_unknown_addrs(entity_addrvec_t maybe_unknowns,
                        const entity_addrvec_t& knowns) {
    bool changed = false;
    auto maybe_replace = [&](entity_addr_t addr) {
      if (!addr.is_blank_ip()) {
        return addr;
      }
      for (auto& b : knowns.v) {
        if (addr.get_family() == b.get_family()) {
          auto a = b;
          a.set_nonce(addr.get_nonce());
          a.set_type(addr.get_type());
          a.set_port(addr.get_port());
          changed = true;
          return a;
        }
      }
      throw std::runtime_error("failed to replace unknown address");
    };
    entity_addrvec_t replaced;
    std::transform(maybe_unknowns.v.begin(),
                   maybe_unknowns.v.end(),
                   std::back_inserter(replaced.v),
                   maybe_replace);
    return {replaced, changed};
  }
}

seastar::future<> OSD::start()
{
  logger().info("start");

  return store->mount().then([this] {
    meta_coll = make_unique<OSDMeta>(store->open_collection(coll_t::meta()),
                                     store.get());
    return meta_coll->load_superblock();
  }).then([this](OSDSuperblock&& sb) {
    superblock = std::move(sb);
    return get_map(superblock.current_epoch);
  }).then([this](cached_map_t&& map) {
    osdmap = std::move(map);
    return load_pgs();
  }).then([this] {
    for (auto msgr : {std::ref(cluster_msgr), std::ref(public_msgr)}) {
      if (local_conf()->ms_crc_data) {
        msgr.get().set_crc_data();
      }
      if (local_conf()->ms_crc_header) {
        msgr.get().set_crc_header();
      }
    }
    dispatchers.push_front(this);
    dispatchers.push_front(monc.get());
    dispatchers.push_front(mgrc.get());
    return seastar::when_all_succeed(
      cluster_msgr.try_bind(pick_addresses(CEPH_PICK_ADDRESS_CLUSTER),
                            local_conf()->ms_bind_port_min,
                            local_conf()->ms_bind_port_max)
        .then([this] { return cluster_msgr.start(&dispatchers); }),
      public_msgr.try_bind(pick_addresses(CEPH_PICK_ADDRESS_PUBLIC),
                           local_conf()->ms_bind_port_min,
                           local_conf()->ms_bind_port_max)
        .then([this] { return public_msgr.start(&dispatchers); }));
  }).then([this] {
    return seastar::when_all_succeed(monc->start(),
                                     mgrc->start());
  }).then([this] {
    monc->sub_want("osd_pg_creates", last_pg_create_epoch, 0);
    monc->sub_want("mgrmap", 0, 0);
    monc->sub_want("osdmap", 0, 0);
    return monc->renew_subs();
  }).then([this] {
    if (auto [addrs, changed] =
        replace_unknown_addrs(cluster_msgr.get_myaddrs(),
                              public_msgr.get_myaddrs()); changed) {
      cluster_msgr.set_myaddrs(addrs);
    }
    return heartbeat->start(public_msgr.get_myaddrs(),
                            cluster_msgr.get_myaddrs());
  }).then([this] {
    return start_boot();
  });
}

seastar::future<> OSD::start_boot()
{
  state.set_preboot();
  return monc->get_version("osdmap").then([this](version_t newest, version_t oldest) {
    return _preboot(oldest, newest);
  });
}

seastar::future<> OSD::_preboot(version_t oldest, version_t newest)
{
  logger().info("osd.{}: _preboot", whoami);
  if (osdmap->get_epoch() == 0) {
    logger().warn("waiting for initial osdmap");
  } else if (osdmap->is_destroyed(whoami)) {
    logger().warn("osdmap says I am destroyed");
    // provide a small margin so we don't livelock seeing if we
    // un-destroyed ourselves.
    if (osdmap->get_epoch() > newest - 1) {
      throw std::runtime_error("i am destroyed");
    }
  } else if (osdmap->test_flag(CEPH_OSDMAP_NOUP) || osdmap->is_noup(whoami)) {
    logger().warn("osdmap NOUP flag is set, waiting for it to clear");
  } else if (!osdmap->test_flag(CEPH_OSDMAP_SORTBITWISE)) {
    logger().error("osdmap SORTBITWISE OSDMap flag is NOT set; please set it");
  } else if (osdmap->require_osd_release < CEPH_RELEASE_LUMINOUS) {
    logger().error("osdmap require_osd_release < luminous; please upgrade to luminous");
  } else if (false) {
    // TODO: update mon if current fullness state is different from osdmap
  } else if (version_t n = local_conf()->osd_map_message_max;
             osdmap->get_epoch() >= oldest - 1 &&
             osdmap->get_epoch() + n > newest) {
    return _send_boot();
  }
  // get all the latest maps
  if (osdmap->get_epoch() + 1 >= oldest) {
    return osdmap_subscribe(osdmap->get_epoch() + 1, false);
  } else {
    return osdmap_subscribe(oldest - 1, true);
  }
}

seastar::future<> OSD::_send_boot()
{
  state.set_booting();

  logger().info("hb_back_msgr: {}", heartbeat->get_back_addrs());
  logger().info("hb_front_msgr: {}", heartbeat->get_front_addrs());
  logger().info("cluster_msgr: {}", cluster_msgr.get_myaddr());
  auto m = make_message<MOSDBoot>(superblock,
                                  osdmap->get_epoch(),
                                  osdmap->get_epoch(),
                                  heartbeat->get_back_addrs(),
                                  heartbeat->get_front_addrs(),
                                  cluster_msgr.get_myaddrs(),
                                  CEPH_FEATURES_ALL);
  return monc->send_message(m);
}

seastar::future<> OSD::_send_alive(epoch_t want)
{
  if (!osdmap->exists(whoami)) {
    return seastar::now();
  } else if (want <= up_thru_wanted){
    return seastar::now();
  } else {
    up_thru_wanted = want;
    auto m = make_message<MOSDAlive>(osdmap->get_epoch(), want);
    return monc->send_message(std::move(m));
  }
}

seastar::future<> OSD::stop()
{
  logger().info("stop");
  // see also OSD::shutdown()
  state.set_stopping();
  return gate.close().then([this] {
    return heartbeat->stop();
  }).then([this] {
    return monc->stop();
  }).then([this] {
    return store->umount();
  });
}

seastar::future<> OSD::load_pgs()
{
  return seastar::parallel_for_each(store->list_collections(),
    [this](auto coll) {
      spg_t pgid;
      if (coll.is_pg(&pgid)) {
        return load_pg(pgid).then([pgid, this](auto&& pg) {
          logger().info("load_pgs: loaded {}", pgid);
          pgs.emplace(pgid, std::move(pg));
          return seastar::now();
        });
      } else if (coll.is_temp(&pgid)) {
        // TODO: remove the collection
        return seastar::now();
      } else {
        logger().warn("ignoring unrecognized collection: {}", coll);
        return seastar::now();
      }
    });
}

seastar::future<Ref<PG>> OSD::load_pg(spg_t pgid)
{
  using ec_profile_t = map<string,string>;
  return PGMeta{store.get(), pgid}.get_epoch().then([this](epoch_t e) {
    return get_map(e);
  }).then([pgid, this] (auto&& create_map) {
    if (create_map->have_pg_pool(pgid.pool())) {
      pg_pool_t pi = *create_map->get_pg_pool(pgid.pool());
      string name = create_map->get_pool_name(pgid.pool());
      ec_profile_t ec_profile;
      if (pi.is_erasure()) {
        ec_profile = create_map->get_erasure_code_profile(pi.erasure_code_profile);
      }
      return seastar::make_ready_future<pg_pool_t,
                                        string,
                                        ec_profile_t>(std::move(pi),
                                                      std::move(name),
                                                      std::move(ec_profile));
    } else {
      // pool was deleted; grab final pg_pool_t off disk.
      return meta_coll->load_final_pool_info(pgid.pool());
    }
  }).then([pgid, this](pg_pool_t&& pool,
                       string&& name,
                       ec_profile_t&& ec_profile) {
    auto backend = PGBackend::create(pgid, pool, store.get(), ec_profile);
    Ref<PG> pg{new PG{pgid,
                      pg_shard_t{whoami, pgid.shard},
                      std::move(pool),
                      std::move(name),
                      std::move(backend),
                      osdmap,
                      cluster_msgr}};
    return pg->read_state(store.get()).then([pg] {
      return seastar::make_ready_future<Ref<PG>>(std::move(pg));
    });
  });
}

seastar::future<> OSD::ms_dispatch(ceph::net::ConnectionRef conn, MessageRef m)
{
  if (state.is_stopping()) {
    return seastar::now();
  }

  switch (m->get_type()) {
  case CEPH_MSG_OSD_MAP:
    return handle_osd_map(conn, boost::static_pointer_cast<MOSDMap>(m));
  case CEPH_MSG_OSD_OP:
    return handle_osd_op(conn, boost::static_pointer_cast<MOSDOp>(m));
  case MSG_OSD_PG_NOTIFY:
    return handle_pg_notify(conn, boost::static_pointer_cast<MOSDPGNotify>(m));
  case MSG_OSD_PG_INFO:
    return handle_pg_info(conn, boost::static_pointer_cast<MOSDPGInfo>(m));
  case MSG_OSD_PG_QUERY:
    return handle_pg_query(conn, boost::static_pointer_cast<MOSDPGQuery>(m));
  case MSG_OSD_PG_LOG:
    return handle_pg_log(conn, boost::static_pointer_cast<MOSDPGLog>(m));
  default:
    return seastar::now();
  }
}

seastar::future<> OSD::ms_handle_connect(ceph::net::ConnectionRef conn)
{
  if (conn->get_peer_type() != CEPH_ENTITY_TYPE_MON) {
    return seastar::now();
  } else {
    return seastar::now();
  }
}

seastar::future<> OSD::ms_handle_reset(ceph::net::ConnectionRef conn)
{
  // TODO: cleanup the session attached to this connection
  logger().warn("ms_handle_reset");
  return seastar::now();
}

seastar::future<> OSD::ms_handle_remote_reset(ceph::net::ConnectionRef conn)
{
  logger().warn("ms_handle_remote_reset");
  return seastar::now();
}

MessageRef OSD::get_stats()
{
  // todo: m-to-n: collect stats using map-reduce
  // MPGStats::had_map_for is not used since PGMonitor was removed
  auto m = make_message<MPGStats>(monc->get_fsid(), osdmap->get_epoch());

  for (auto [pgid, pg] : pgs) {
    if (pg->is_primary()) {
      auto stats = pg->get_stats();
      // todo: update reported_epoch,reported_seq,last_fresh
      stats.reported_epoch = osdmap->get_epoch();
      m->pg_stat.emplace(pgid.pgid, std::move(stats));
    }
  }
  return m;
}

OSD::cached_map_t OSD::get_map() const
{
  return osdmap;
}

seastar::future<OSD::cached_map_t> OSD::get_map(epoch_t e)
{
  // TODO: use LRU cache for managing osdmap, fallback to disk if we have to
  if (auto found = osdmaps.find(e); found) {
    return seastar::make_ready_future<cached_map_t>(std::move(found));
  } else {
    return load_map(e).then([e, this](unique_ptr<OSDMap> osdmap) {
      return seastar::make_ready_future<cached_map_t>(
        osdmaps.insert(e, std::move(osdmap)));
    });
  }
}

void OSD::store_map_bl(ceph::os::Transaction& t,
                       epoch_t e, bufferlist&& bl)
{
  meta_coll->store_map(t, e, bl);
  map_bl_cache.insert(e, std::move(bl));
}

seastar::future<bufferlist> OSD::load_map_bl(epoch_t e)
{
  if (std::optional<bufferlist> found = map_bl_cache.find(e); found) {
    return seastar::make_ready_future<bufferlist>(*found);
  } else {
    return meta_coll->load_map(e);
  }
}

seastar::future<std::unique_ptr<OSDMap>> OSD::load_map(epoch_t e)
{
  auto o = std::make_unique<OSDMap>();
  if (e > 0) {
    return load_map_bl(e).then([e, o=std::move(o), this](bufferlist bl) mutable {
      o->decode(bl);
      return seastar::make_ready_future<unique_ptr<OSDMap>>(std::move(o));
    });
  } else {
    return seastar::make_ready_future<unique_ptr<OSDMap>>(std::move(o));
  }
}

seastar::future<> OSD::store_maps(ceph::os::Transaction& t,
                                  epoch_t start, Ref<MOSDMap> m)
{
  return seastar::do_for_each(boost::make_counting_iterator(start),
                              boost::make_counting_iterator(m->get_last() + 1),
                              [&t, m, this](epoch_t e) {
    if (auto p = m->maps.find(e); p != m->maps.end()) {
      auto o = std::make_unique<OSDMap>();
      o->decode(p->second);
      logger().info("store_maps osdmap.{}", e);
      store_map_bl(t, e, std::move(std::move(p->second)));
      osdmaps.insert(e, std::move(o));
      return seastar::now();
    } else if (auto p = m->incremental_maps.find(e);
               p != m->incremental_maps.end()) {
      return load_map(e - 1).then([e, bl=p->second, &t, this](auto o) {
        OSDMap::Incremental inc;
        auto i = bl.cbegin();
        inc.decode(i);
        o->apply_incremental(inc);
        bufferlist fbl;
        o->encode(fbl, inc.encode_features | CEPH_FEATURE_RESERVED);
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

seastar::future<> OSD::osdmap_subscribe(version_t epoch, bool force_request)
{
  logger().info("{}({})", __func__, epoch);
  if (monc->sub_want_increment("osdmap", epoch, CEPH_SUBSCRIBE_ONETIME) ||
      force_request) {
    return monc->renew_subs();
  } else {
    return seastar::now();
  }
}

seastar::future<> OSD::handle_osd_map(ceph::net::ConnectionRef conn,
                                      Ref<MOSDMap> m)
{
  logger().info("handle_osd_map {}", *m);
  if (m->fsid != superblock.cluster_fsid) {
    logger().warn("fsid mismatched");
    return seastar::now();
  }
  if (state.is_initializing()) {
    logger().warn("i am still initializing");
    return seastar::now();
  }

  const auto first = m->get_first();
  const auto last = m->get_last();
  logger().info("handle_osd_map epochs [{}..{}], i have {}, src has [{}..{}]",
                first, last, superblock.newest_map, m->oldest_map, m->newest_map);
  // make sure there is something new, here, before we bother flushing
  // the queues and such
  if (last <= superblock.newest_map) {
    return seastar::now();
  }
  // missing some?
  bool skip_maps = false;
  epoch_t start = superblock.newest_map + 1;
  if (first > start) {
    logger().info("handle_osd_map message skips epochs {}..{}",
                  start, first - 1);
    if (m->oldest_map <= start) {
      return osdmap_subscribe(start, false);
    }
    // always try to get the full range of maps--as many as we can.  this
    //  1- is good to have
    //  2- is at present the only way to ensure that we get a *full* map as
    //     the first map!
    if (m->oldest_map < first) {
      return osdmap_subscribe(m->oldest_map - 1, true);
    }
    skip_maps = true;
    start = first;
  }

  return seastar::do_with(ceph::os::Transaction{},
                          [=](auto& t) {
    return store_maps(t, start, m).then([=, &t] {
      // even if this map isn't from a mon, we may have satisfied our subscription
      monc->sub_got("osdmap", last);
      if (!superblock.oldest_map || skip_maps) {
        superblock.oldest_map = first;
      }
      superblock.newest_map = last;
      superblock.current_epoch = last;

      // note in the superblock that we were clean thru the prior epoch
      if (boot_epoch && boot_epoch >= superblock.mounted) {
        superblock.mounted = boot_epoch;
        superblock.clean_thru = last;
      }
      meta_coll->store_superblock(t, superblock);
      return store->do_transaction(meta_coll->collection(), std::move(t));
    });
  }).then([=] {
    // TODO: write to superblock and commit the transaction
    return committed_osd_maps(start, last, m);
  });
}

seastar::future<> OSD::committed_osd_maps(version_t first,
                                          version_t last,
                                          Ref<MOSDMap> m)
{
  logger().info("osd.{}: committed_osd_maps({}, {})", whoami, first, last);
  // advance through the new maps
  return seastar::do_for_each(boost::make_counting_iterator(first),
                              boost::make_counting_iterator(last + 1),
                              [this](epoch_t cur) {
    return get_map(cur).then([this](cached_map_t&& o) {
      osdmap = std::move(o);
      if (up_epoch != 0 &&
          osdmap->is_up(whoami) &&
          osdmap->get_addrs(whoami) == public_msgr.get_myaddrs()) {
        up_epoch = osdmap->get_epoch();
        if (!boot_epoch) {
          boot_epoch = osdmap->get_epoch();
        }
      }
    });
  }).then([m, this] {
    if (osdmap->is_up(whoami) &&
        osdmap->get_addrs(whoami) == public_msgr.get_myaddrs() &&
        bind_epoch < osdmap->get_up_from(whoami)) {
      if (state.is_booting()) {
        logger().info("osd.{}: activating...", whoami);
        state.set_active();
        beacon_timer.arm_periodic(
          std::chrono::seconds(local_conf()->osd_beacon_report_interval));
        heartbeat_timer.arm_periodic(
          std::chrono::seconds(TICK_INTERVAL));
      }
    }
    // yay!
    return consume_map(osdmap->get_epoch());
  }).then([m, this] {
    if (state.is_active()) {
      logger().info("osd.{}: now active", whoami);
      if (!osdmap->exists(whoami)) {
        return shutdown();
      }
      if (should_restart()) {
        return restart();
      } else {
        return seastar::now();
      }
    } else if (state.is_preboot()) {
      logger().info("osd.{}: now preboot", whoami);

      if (m->get_source().is_mon()) {
        return _preboot(m->oldest_map, m->newest_map);
      } else {
        logger().info("osd.{}: start_boot", whoami);
        return start_boot();
      }
    } else {
      logger().info("osd.{}: now {}", whoami, state);
      // XXX
      return seastar::now();
    }
  });
}

seastar::future<> OSD::handle_osd_op(ceph::net::ConnectionRef conn,
                                     Ref<MOSDOp> m)
{
  return wait_for_map(m->get_map_epoch()).then([=](epoch_t epoch) {
    if (auto found = pgs.find(m->get_spg()); found != pgs.end()) {
      return found->second->handle_op(conn, std::move(m));
    } else if (osdmap->is_up_acting_osd_shard(m->get_spg(), whoami)) {
      logger().info("no pg, should exist e{}, will wait", epoch);
      // todo, wait for peering, etc
      return seastar::now();
    } else {
      logger().info("no pg, shouldn't exist e{}, dropping", epoch);
      // todo: share map with client
      return seastar::now();
    }
  });
}

bool OSD::should_restart() const
{
  if (!osdmap->is_up(whoami)) {
    logger().info("map e {} marked osd.{} down",
                  osdmap->get_epoch(), whoami);
    return true;
  } else if (osdmap->get_addrs(whoami) != public_msgr.get_myaddrs()) {
    logger().error("map e {} had wrong client addr ({} != my {})",
                   osdmap->get_epoch(),
                   osdmap->get_addrs(whoami),
                   public_msgr.get_myaddrs());
    return true;
  } else if (osdmap->get_cluster_addrs(whoami) != cluster_msgr.get_myaddrs()) {
    logger().error("map e {} had wrong cluster addr ({} != my {})",
                   osdmap->get_epoch(),
                   osdmap->get_cluster_addrs(whoami),
                   cluster_msgr.get_myaddrs());
    return true;
  } else {
    return false;
  }
}

seastar::future<> OSD::restart()
{
  beacon_timer.cancel();
  heartbeat_timer.cancel();
  up_epoch = 0;
  bind_epoch = osdmap->get_epoch();
  // TODO: promote to shutdown if being marked down for multiple times
  // rebind messengers
  return start_boot();
}

seastar::future<> OSD::shutdown()
{
  // TODO
  superblock.mounted = boot_epoch;
  superblock.clean_thru = osdmap->get_epoch();
  return seastar::now();
}

seastar::future<> OSD::send_beacon()
{
  // FIXME: min lec should be calculated from pg_stat
  //        and should set m->pgs
  epoch_t min_last_epoch_clean = osdmap->get_epoch();
  auto m = make_message<MOSDBeacon>(osdmap->get_epoch(),
                                    min_last_epoch_clean);
  return monc->send_message(m);
}

void OSD::update_heartbeat_peers()
{
  if (!state.is_active()) {
    return;
  }
  for (auto& pg : pgs) {
    vector<int> up, acting;
    osdmap->pg_to_up_acting_osds(pg.first.pgid,
                                 &up, nullptr,
                                 &acting, nullptr);
    for (auto osd : boost::join(up, acting)) {
      if (osd != CRUSH_ITEM_NONE && osd != whoami) {
        heartbeat->add_peer(osd, osdmap->get_epoch());
      }
    }
  }
  heartbeat->update_peers(whoami);
}

seastar::future<> OSD::handle_pg_notify(ceph::net::ConnectionRef conn,
                                        Ref<MOSDPGNotify> m)
{
  // assuming all pgs reside in a single shard
  // see OSD::dequeue_peering_evt()
  const int from = m->get_source().num();
  return seastar::parallel_for_each(m->get_pg_list(),
    [from, this](pair<pg_notify_t, PastIntervals> p) {
      auto& [pg_notify, past_intervals] = p;
      spg_t pgid{pg_notify.info.pgid.pgid, pg_notify.to};
      MNotifyRec notify{pgid,
                        pg_shard_t{from, pg_notify.from},
                        pg_notify,
                        0, // the features is not used
                        past_intervals};
      auto create_info = new PGCreateInfo{pgid,
                                          pg_notify.query_epoch,
                                          pg_notify.info.history,
                                          past_intervals,
                                          false};
      auto evt = std::make_unique<PGPeeringEvent>(pg_notify.epoch_sent,
                                                  pg_notify.query_epoch,
                                                  notify,
                                                  true, // requires_pg
                                                  create_info);
      return do_peering_event(pgid, std::move(evt));
  });
}

seastar::future<> OSD::handle_pg_info(ceph::net::ConnectionRef conn,
                                      Ref<MOSDPGInfo> m)
{
  // assuming all pgs reside in a single shard
  // see OSD::dequeue_peering_evt()
  const int from = m->get_source().num();
  return seastar::parallel_for_each(m->pg_list,
    [from, this](pair<pg_notify_t, PastIntervals> p) {
      auto& pg_notify = p.first;
      spg_t pgid{pg_notify.info.pgid.pgid, pg_notify.to};
      MInfoRec info{pg_shard_t{from, pg_notify.from},
                    pg_notify.info,
                    pg_notify.epoch_sent};
      auto evt = std::make_unique<PGPeeringEvent>(pg_notify.epoch_sent,
                                                  pg_notify.query_epoch,
                                                  std::move(info));
      return do_peering_event(pgid, std::move(evt));
  });
}

seastar::future<> OSD::handle_pg_query(ceph::net::ConnectionRef conn,
                                       Ref<MOSDPGQuery> m)
{
  const int from = m->get_source().num();
  return seastar::parallel_for_each(m->pg_list,
    [from, this](pair<spg_t, pg_query_t> p) {
      auto& [pgid, pg_query] = p;
      MQuery query{pgid, pg_shard_t{from, pg_query.from},
                   pg_query, pg_query.epoch_sent};
      auto evt = std::make_unique<PGPeeringEvent>(pg_query.epoch_sent,
                                                  pg_query.epoch_sent,
                                                  std::move(query));
      return do_peering_event(pgid, std::move(evt));
  });
}

seastar::future<> OSD::handle_pg_log(ceph::net::ConnectionRef conn,
                                       Ref<MOSDPGLog> m)
{
  const int from = m->get_source().num();
  MLogRec log{pg_shard_t{from, m->from}, m.get()};
  auto create_info = new PGCreateInfo{m->get_spg(),
                                      m->get_query_epoch(),
                                      m->info.history,
                                      m->past_intervals,
                                      false};
  auto evt = std::make_unique<PGPeeringEvent>(m->get_epoch(),
                                              m->get_query_epoch(),
                                              std::move(log),
                                              true,
                                              create_info);
  return do_peering_event(m->get_spg(), std::move(evt));
}

seastar::future<> OSD::consume_map(epoch_t epoch)
{
  // todo: m-to-n: broadcast this news to all shards
  return seastar::parallel_for_each(pgs.begin(), pgs.end(), [=](auto& pg) {
    return advance_pg_to(pg.second, epoch);
  }).then([epoch, this] {
    auto first = waiting_peering.lower_bound(epoch);
    auto last = waiting_peering.end();
    std::for_each(first, last, [epoch, this](auto& blocked_requests) {
      blocked_requests.second.set_value(epoch);
    });
    waiting_peering.erase(first, last);
    return seastar::now();
  });
}

seastar::future<>
OSD::do_peering_event(spg_t pgid,
                      std::unique_ptr<PGPeeringEvent> evt)
{
  if (auto pg = pgs.find(pgid); pg != pgs.end()) {
    return wait_for_map(evt->get_epoch_sent()).then(
      [pg=pg->second, this](epoch_t epoch) {
        return advance_pg_to(pg, epoch);
    }).then([pg, evt=std::move(evt)]() mutable {
        return pg->second->do_peering_event(std::move(evt));
    }).then([pg=pg->second, this] {
        return _send_alive(pg->get_need_up_thru());
    });
  } else {
    logger().warn("pg not found: {}", pgid);
    // todo: handle_pg_query_nopg()
    return seastar::now();
  }
}

seastar::future<epoch_t> OSD::wait_for_map(epoch_t epoch)
{
  const auto mine = osdmap->get_epoch();
  if (mine >= epoch) {
    return seastar::make_ready_future<epoch_t>(mine);
  } else {
    logger().info("evt epoch is {}, i have {}, will wait", epoch, mine);
    return waiting_peering[epoch].get_shared_future();
  }
}

seastar::future<> OSD::advance_pg_to(Ref<PG> pg, epoch_t to)
{
  auto from = pg->get_osdmap_epoch();
  // todo: merge/split support
  return seastar::do_for_each(boost::make_counting_iterator(from + 1),
                              boost::make_counting_iterator(to + 1),
    [pg, this](epoch_t next_epoch) {
      return get_map(next_epoch).then([pg, this] (cached_map_t&& next_map) {
        return pg->handle_advance_map(next_map);
      }).then([pg, this] {
        return pg->handle_activate_map();
      });
    });
}
