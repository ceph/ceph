#include "osd.h"

#include "messages/MOSDBeacon.h"
#include "messages/MOSDBoot.h"
#include "messages/MOSDMap.h"
#include "crimson/net/Connection.h"
#include "crimson/net/SocketMessenger.h"
#include "crimson/os/cyan_collection.h"
#include "crimson/os/cyan_object.h"
#include "crimson/os/cyan_store.h"
#include "crimson/os/Transaction.h"

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

using ceph::common::local_conf;
using ceph::os::CyanStore;

OSD::OSD(int id, uint32_t nonce)
  : whoami{id},
    cluster_msgr{new ceph::net::SocketMessenger{entity_name_t::OSD(whoami),
                                                "cluster", nonce}},
    client_msgr{new ceph::net::SocketMessenger{entity_name_t::OSD(whoami),
                                               "client", nonce}},
    monc{*client_msgr}
{
  for (auto msgr : {cluster_msgr.get(), client_msgr.get()}) {
    if (local_conf()->ms_crc_data) {
      msgr->set_crc_data();
    }
    if (local_conf()->ms_crc_header) {
      msgr->set_crc_header();
    }
  }
  dispatchers.push_front(this);
  dispatchers.push_front(&monc);
  osdmaps[0] = seastar::make_lw_shared<OSDMap>();
  beacon_timer.set_callback([this] {
    send_beacon();
  });
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
  const auto data_path = local_conf().get_val<std::string>("osd_data");
  store = std::make_unique<ceph::os::CyanStore>(data_path);
  uuid_d osd_fsid;
  osd_fsid.generate_random();
  return store->mkfs(osd_fsid).then([this] {
    return store->mount();
  }).then([cluster_fsid, osd_fsid, this] {
    superblock.cluster_fsid = cluster_fsid;
    superblock.osd_fsid = osd_fsid;
    superblock.whoami = whoami;
    superblock.compat_features = get_osd_initial_compat_set();

    bufferlist bl;
    encode(superblock, bl);

    auto ch = store->create_new_collection(coll_t::meta());
    ceph::os::Transaction t;
    t.create_collection(coll_t::meta(), 0);
    t.write(coll_t::meta(), OSD_SUPERBLOCK_GOBJECT, 0, bl.length(), bl);
    return store->do_transaction(ch, std::move(t));
  }).then([cluster_fsid, this] {
    store->write_meta("ceph_fsid", cluster_fsid.to_string());
    store->write_meta("whoami", std::to_string(whoami));
    return seastar::now();
  });
}

seastar::future<> OSD::start()
{
  logger().info("start");
  const auto data_path = local_conf().get_val<std::string>("osd_data");
  store = std::make_unique<ceph::os::CyanStore>(data_path);
  return store->mount().then([this] {
    meta_coll = store->open_collection(coll_t::meta());
    return read_superblock();
  }).then([this] {
    osdmap = get_map(superblock.current_epoch);
    return client_msgr->start(&dispatchers);
  }).then([this] {
    return monc.start();
  }).then([this] {
    monc.sub_want("osd_pg_creates", last_pg_create_epoch, 0);
    monc.sub_want("mgrmap", 0, 0);
    monc.sub_want("osdmap", 0, 0);
    return monc.renew_subs();
  }).then([this] {
    return start_boot();
  });
}

seastar::future<> OSD::start_boot()
{
  state.set_preboot();
  return monc.get_version("osdmap").then([this](version_t newest, version_t oldest) {
    return _preboot(newest, oldest);
  });
}

seastar::future<> OSD::_preboot(version_t newest, version_t oldest)
{
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

  entity_addrvec_t hb_back_addrs;
  entity_addrvec_t hb_front_addrs;
  entity_addrvec_t cluster_addrs;

  auto m = make_message<MOSDBoot>(superblock,
                                  osdmap->get_epoch(),
                                  osdmap->get_epoch(),
                                  hb_back_addrs,
                                  hb_front_addrs,
                                  cluster_addrs,
                                  CEPH_FEATURES_ALL);
  return monc.send_message(m);
}

seastar::future<> OSD::stop()
{
  // see also OSD::shutdown()
  state.set_stopping();
  return gate.close().then([this] {
    return monc.stop();
  }).then([this] {
    return client_msgr->shutdown();
  });
}

seastar::future<> OSD::ms_dispatch(ceph::net::ConnectionRef conn, MessageRef m)
{
  logger().info("ms_dispatch {}", *m);
  if (state.is_stopping()) {
    return seastar::now();
  }

  switch (m->get_type()) {
  case CEPH_MSG_OSD_MAP:
    return handle_osd_map(conn, boost::static_pointer_cast<MOSDMap>(m));
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

seastar::lw_shared_ptr<OSDMap> OSD::get_map(epoch_t e)
{
  // TODO: use LRU cache for managing osdmap, fallback to disk if we have to
  return osdmaps[e];
}

void OSD::store_maps(epoch_t start, Ref<MOSDMap> m)
{
  for (epoch_t e = start; e <= m->get_last(); e++) {
    seastar::lw_shared_ptr<OSDMap> o;
    if (auto p = m->maps.find(e); p != m->maps.end()) {
      o = seastar::make_lw_shared<OSDMap>();
      o->decode(p->second);
    } else if (auto p = m->incremental_maps.find(e);
               p != m->incremental_maps.end()) {
      o = get_map(e - 1);
      OSDMap::Incremental inc;
      auto i = p->second.cbegin();
      inc.decode(i);
      o->apply_incremental(inc);
    } else {
      logger().error("MOSDMap lied about what maps it had?");
    }
    osdmaps[e] = std::move(o);
  }
}

seastar::future<> OSD::osdmap_subscribe(version_t epoch, bool force_request)
{
  if (monc.sub_want_increment("osdmap", epoch, CEPH_SUBSCRIBE_ONETIME) ||
      force_request) {
    return monc.renew_subs();
  } else {
    return seastar::now();
  }
}

void OSD::write_superblock(ceph::os::Transaction& t)
{
  bufferlist bl;
  encode(superblock, bl);
  t.write(meta_coll->cid, OSD_SUPERBLOCK_GOBJECT, 0, bl.length(), bl);
}

seastar::future<> OSD::read_superblock()
{
  // just-enough superblock so mon can ack my MOSDBoot
  return store->read(meta_coll, OSD_SUPERBLOCK_GOBJECT, 0, 0)
    .then([this] (bufferlist&& bl) {
      auto p = bl.cbegin();
      decode(superblock, p);
      return seastar::now();
  });
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
  // TODO: store new maps: queue for disk and put in the osdmap cache
  store_maps(start, m);

  // even if this map isn't from a mon, we may have satisfied our subscription
  monc.sub_got("osdmap", last);
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
  // TODO: write to superblock and commit the transaction
  return committed_osd_maps(start, last, m);
}

seastar::future<> OSD::committed_osd_maps(version_t first,
                                          version_t last,
                                          Ref<MOSDMap> m)
{
  logger().info("osd.{}: committed_osd_maps({}, {})", whoami, first, last);
  // advance through the new maps
  for (epoch_t cur = first; cur <= last; cur++) {
    osdmap = get_map(cur);
    if (up_epoch != 0 &&
        osdmap->is_up(whoami) &&
        osdmap->get_addrs(whoami) == client_msgr->get_myaddrs()) {
      up_epoch = osdmap->get_epoch();
      if (!boot_epoch) {
        boot_epoch = osdmap->get_epoch();
      }
    }
  }

  if (osdmap->is_up(whoami) &&
      osdmap->get_addrs(whoami) == client_msgr->get_myaddrs() &&
      bind_epoch < osdmap->get_up_from(whoami)) {
    if (state.is_booting()) {
      logger().info("osd.{}: activating...", whoami);
      state.set_active();
      beacon_timer.arm_periodic(
         std::chrono::seconds(local_conf()->osd_beacon_report_interval));
    }
  }

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
      logger().info("osd.{}: _preboot", whoami);
      return _preboot(m->oldest_map, m->newest_map);
    } else {
      logger().info("osd.{}: start_boot", whoami);
      return start_boot();
    }
  } else {
    logger().info("osd.{}: now ???", whoami);
    // XXX
    return seastar::now();
  }
}

bool OSD::should_restart() const
{
  if (!osdmap->is_up(whoami)) {
    logger().info("map e {} marked osd.{} down",
                  osdmap->get_epoch(), whoami);
    return true;
  } else if (osdmap->get_addrs(whoami) != client_msgr->get_myaddrs()) {
    logger().error("map e {} had wrong client addr ({} != my {})",
                   osdmap->get_epoch(),
                   osdmap->get_addrs(whoami),
                   client_msgr->get_myaddrs());
    return true;
  } else if (osdmap->get_cluster_addrs(whoami) != cluster_msgr->get_myaddrs()) {
    logger().error("map e {} had wrong cluster addr ({} != my {})",
                   osdmap->get_epoch(),
                   osdmap->get_cluster_addrs(whoami),
                   cluster_msgr->get_myaddrs());
    return true;
  } else {
    return false;
  }
}

seastar::future<> OSD::restart()
{
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
  return monc.send_message(m);
}
