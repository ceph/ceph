// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 sts=2 smarttab

/*
 * Fake OSD for ceph-mgr scale testing.
 *
 * Boots into the osdmap like a real OSD (MOSDBoot with metadata, beacons)
 * and reports to the mgr through MgrClient (MMgrOpen, MMgrReport,
 * MPGStats), but has no object store, no peering and no heartbeats.
 * PG stats are reported as active+clean for every PG this OSD is
 * acting primary for.
 *
 * The OSD id must exist in the osdmap with a matching uuid and have an
 * osd key, e.g.:
 *   uuid=$(uuidgen); id=$(ceph osd new $uuid)
 *   ceph auth get-or-create osd.$id mon 'allow profile osd' \
 *     mgr 'allow profile osd' osd 'allow *' -o fake-osd.$id.keyring
 *   ceph_test_mgr_fake_osd -c ceph.conf -i $id --osd-uuid $uuid \
 *     --keyring fake-osd.$id.keyring
 */

#include <iostream>
#include <memory>

#include "common/Timer.h"
#include "common/async/context_pool.h"
#include "common/ceph_argparse.h"
#include "common/ceph_mutex.h"
#include "common/errno.h"
#include "common/pick_address.h"
#include "common/strtol.h"
#include "global/global_init.h"
#include "global/signal_handler.h"
#include "include/util.h"
#include "messages/MOSDBeacon.h"
#include "messages/MOSDBoot.h"
#include "messages/MOSDMap.h"
#include "messages/MOSDPGCreate2.h"
#include "messages/MOSDPGCreated.h"
#include "messages/MPGStats.h"
#include "mgr/MgrClient.h"
#include "mon/MonClient.h"
#include "msg/Dispatcher.h"
#include "msg/Messenger.h"
#include "osd/OSDMap.h"
#include "osd/osd_types.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix *_dout << "fake_osd." << whoami << " "

static Messenger *create_messenger(CephContext *cct, int whoami)
{
  std::string type = cct->_conf.get_val<std::string>("ms_public_type");
  if (type.empty()) {
    type = cct->_conf.get_val<std::string>("ms_type");
  }
  return Messenger::create(cct, type, entity_name_t::OSD(whoami), "client",
			   Messenger::get_random_nonce());
}

class FakeOSD : public Dispatcher {
  const int whoami;
  ceph::async::io_context_pool poolctx;
  std::unique_ptr<Messenger> messenger;
  MonClient monc;
  MgrClient mgrc;

  ceph::mutex lock = ceph::make_mutex("FakeOSD::lock");
  SafeTimer timer;
  OSDMap osdmap;
  OSDSuperblock sb;
  utime_t last_boot_attempt;
  utime_t last_beacon;
  uint64_t stat_seq = 0;
  std::map<pg_t, pg_stat_t> primary_pgs;
  epoch_t mapped_epoch = 0;

  static constexpr double BOOT_INTERVAL = 10.0;
  static constexpr uint64_t FAKE_TOTAL_BYTES = 1ull << 40;

public:
  FakeOSD(CephContext *cct, int whoami)
    : Dispatcher(cct),
      whoami(whoami),
      messenger(create_messenger(cct, whoami)),
      monc(cct, poolctx),
      mgrc(cct, messenger.get(), &monc.monmap),
      timer(cct, lock) {}

  int init() {
    poolctx.start(1);

    uint64_t required = CEPH_FEATURE_UID | CEPH_FEATURE_PGID64 |
      CEPH_FEATURE_OSDENC;
    messenger->set_default_policy(
      Messenger::Policy::stateless_registered_server(0));
    messenger->set_policy(entity_name_t::TYPE_MON,
			  Messenger::Policy::lossy_client(required));
    messenger->set_policy(entity_name_t::TYPE_MGR,
			  Messenger::Policy::lossy_client(required));

    entity_addrvec_t addrs;
    int r = pick_addresses(cct, CEPH_PICK_ADDRESS_PUBLIC, &addrs);
    if (r < 0) {
      derr << "failed to pick public address: " << cpp_strerror(r) << dendl;
      return r;
    }
    r = messenger->bindv(addrs);
    if (r < 0) {
      derr << "failed to bind " << addrs << ": " << cpp_strerror(r) << dendl;
      return r;
    }

    r = monc.build_initial_monmap();
    if (r < 0) {
      derr << "build_initial_monmap: " << cpp_strerror(r) << dendl;
      return r;
    }
    monc.set_messenger(messenger.get());
    messenger->set_auth_server(&monc);
    messenger->add_dispatcher_tail(&mgrc);
    messenger->add_dispatcher_tail(this);
    messenger->start();

    monc.set_want_keys(CEPH_ENTITY_TYPE_MON | CEPH_ENTITY_TYPE_OSD |
		       CEPH_ENTITY_TYPE_MGR);
    r = monc.init();
    if (r < 0) {
      derr << "monc init: " << cpp_strerror(r) << dendl;
      return r;
    }
    mgrc.set_pgstats_cb([this]() { return collect_pg_stats(); });
    mgrc.init();

    r = monc.authenticate();
    if (r < 0) {
      derr << "monc authenticate: " << cpp_strerror(r) << dendl;
      return r;
    }
    monc.wait_auth_rotating(30.0);

    sb.cluster_fsid = monc.get_fsid();
    sb.osd_fsid = cct->_conf->osd_uuid;
    sb.whoami = whoami;

    monc.sub_want("osdmap", 0, 0);
    monc.sub_want("mgrmap", 0, 0);
    monc.sub_want("osd_pg_creates", 0, 0);
    monc.renew_subs();

    std::lock_guard l{lock};
    timer.init();
    schedule_tick();
    dout(1) << "started, addrs " << messenger->get_myaddrs() << dendl;
    return 0;
  }

  void shutdown() {
    dout(1) << "shutting down" << dendl;
    {
      std::lock_guard l{lock};
      timer.shutdown();
    }
    mgrc.shutdown();
    monc.shutdown();
    messenger->shutdown();
    messenger->wait();
    poolctx.stop();
  }

private:
  bool is_booted() const {
    return osdmap.is_up(whoami) &&
      osdmap.get_addrs(whoami) == messenger->get_myaddrs();
  }

  void schedule_tick() {
    timer.add_event_after(1.0, new LambdaContext([this](int r) {
      if (r != -ECANCELED) {
	tick();
      }
    }));
  }

  // called with lock held
  void tick() {
    utime_t now = ceph_clock_now();
    if (osdmap.get_epoch() > 0 && !is_booted() &&
	now - last_boot_attempt > BOOT_INTERVAL) {
      send_boot();
      last_boot_attempt = now;
    }
    double interval = cct->_conf->osd_beacon_report_interval;
    if (is_booted() && now - last_beacon > interval) {
      send_beacon();
      last_beacon = now;
    }
    schedule_tick();
  }

  void send_boot() {
    const entity_addrvec_t addrs = messenger->get_myaddrs();
    sb.current_epoch = osdmap.get_epoch();
    auto m = new MOSDBoot(sb, osdmap.get_epoch(), 0, addrs, addrs, addrs,
			  CEPH_FEATURES_ALL);
    collect_sys_info(&m->metadata, cct);
    m->metadata["front_addr"] = stringify(addrs);
    m->metadata["back_addr"] = stringify(addrs);
    m->metadata["hb_front_addr"] = stringify(addrs);
    m->metadata["hb_back_addr"] = stringify(addrs);
    std::string dev = "fake" + stringify(whoami);
    m->metadata["osd_objectstore"] = "bluestore";
    m->metadata["osd_data"] = "/var/lib/ceph/osd/ceph-" + stringify(whoami);
    m->metadata["bluestore_bdev_dev_node"] = "/dev/" + dev;
    m->metadata["bluestore_bdev_type"] = "ssd";
    m->metadata["bluefs"] = "1";
    m->metadata["bluefs_dedicated_db"] = "0";
    m->metadata["bluefs_dedicated_wal"] = "0";
    m->metadata["devices"] = dev;
    m->metadata["device_ids"] = dev + "=FAKE_DISK_" + stringify(whoami);
    m->metadata["rotational"] = "0";
    m->metadata["default_device_class"] = "ssd";
    dout(1) << "sending boot at e" << osdmap.get_epoch() << dendl;
    monc.send_mon_message(m);
  }

  void send_beacon() {
    epoch_t e = osdmap.get_epoch();
    dout(10) << "sending beacon at e" << e << dendl;
    monc.send_mon_message(new MOSDBeacon(
      e, e, utime_t(), cct->_conf->osd_beacon_report_interval));
  }

  // recompute the PGs we are acting primary for, once per osdmap epoch
  void update_primary_pgs() {
    epoch_t e = osdmap.get_epoch();
    if (mapped_epoch == e) {
      return;
    }
    utime_t now = ceph_clock_now();
    std::map<pg_t, pg_stat_t> pgs;
    for (const auto &[pool_id, pool] : osdmap.get_pools()) {
      for (ps_t ps = 0; ps < pool.get_pg_num(); ++ps) {
	pg_t pgid(ps, pool_id);
	std::vector<int> up, acting;
	int up_primary, acting_primary;
	osdmap.pg_to_up_acting_osds(pgid, &up, &up_primary, &acting,
				    &acting_primary);
	if (acting_primary != whoami) {
	  continue;
	}
	pg_stat_t &s = pgs[pgid];
	if (auto p = primary_pgs.find(pgid); p != primary_pgs.end()) {
	  s = p->second;
	} else {
	  s.created = e;
	  s.last_change = now;
	}
	s.state = PG_STATE_ACTIVE | PG_STATE_CLEAN;
	s.up = up;
	s.acting = acting;
	s.up_primary = up_primary;
	s.acting_primary = acting_primary;
      }
    }
    primary_pgs.swap(pgs);
    mapped_epoch = e;
    dout(1) << "primary for " << primary_pgs.size() << " pgs at e" << e
	    << dendl;
  }

  MPGStats *collect_pg_stats() {
    std::lock_guard l{lock};
    auto m = new MPGStats(monc.get_fsid(), osdmap.get_epoch());
    utime_t now = ceph_clock_now();
    epoch_t e = osdmap.get_epoch();
    ++stat_seq;

    update_primary_pgs();
    for (auto &[pgid, s] : primary_pgs) {
      s.reported_epoch = e;
      s.reported_seq = stat_seq;
      s.last_epoch_clean = e;
      s.last_fresh = now;
      s.last_active = now;
      s.last_peered = now;
      s.last_clean = now;
      s.last_unstale = now;
      s.last_undegraded = now;
      s.last_fullsized = now;
      s.last_scrub_stamp = now;
      s.last_deep_scrub_stamp = now;
    }
    m->pg_stat = primary_pgs;

    osd_stat_t &st = m->osd_stat;
    st.up_from = osdmap.get_up_from(whoami);
    st.seq = ((uint64_t)st.up_from << 32) + stat_seq;
    st.num_pgs = m->pg_stat.size();
    st.num_osds = 1;
    st.statfs.total = FAKE_TOTAL_BYTES;
    st.statfs.allocated = FAKE_TOTAL_BYTES / 10;
    st.statfs.data_stored = st.statfs.allocated;
    st.statfs.available = st.statfs.total - st.statfs.allocated;
    dout(20) << "reporting " << st.num_pgs << " pgs seq " << st.seq << dendl;
    return m;
  }

  // called with lock held
  void handle_osd_map(const ceph::ref_t<MOSDMap> &m) {
    if (m->fsid != monc.get_fsid()) {
      derr << "osdmap fsid " << m->fsid << " != " << monc.get_fsid() << dendl;
      return;
    }
    epoch_t first = m->get_first();
    epoch_t last = m->get_last();
    if (last <= osdmap.get_epoch()) {
      return;
    }

    epoch_t start = std::max(osdmap.get_epoch() + 1, first);
    if (!m->maps.empty() && m->maps.rbegin()->first >= start) {
      osdmap.decode(m->maps.rbegin()->second);
      start = osdmap.get_epoch() + 1;
    }
    for (epoch_t e = start; e <= last; ++e) {
      auto p = m->incremental_maps.find(e);
      if (p == m->incremental_maps.end() || osdmap.get_epoch() != e - 1) {
	dout(10) << "missing e" << e << ", requesting from e"
		 << osdmap.get_epoch() + 1 << dendl;
	monc.sub_want("osdmap", osdmap.get_epoch() + 1,
		      CEPH_SUBSCRIBE_ONETIME);
	monc.renew_subs();
	return;
      }
      OSDMap::Incremental inc;
      auto bp = p->second.cbegin();
      inc.decode(bp);
      int r = osdmap.apply_incremental(inc);
      ceph_assert(r == 0);
    }
    dout(10) << "osdmap now e" << osdmap.get_epoch()
	     << (is_booted() ? " (up)" : "") << dendl;

    if (m->newest_map > osdmap.get_epoch()) {
      monc.sub_want("osdmap", osdmap.get_epoch() + 1, CEPH_SUBSCRIBE_ONETIME);
      monc.renew_subs();
    }
  }

  // pretend to create the pgs so the mon clears them from creating_pgs
  void handle_pg_create(const ceph::ref_t<MOSDPGCreate2> &m) {
    dout(10) << "acking " << m->pgs.size() << " pg creates" << dendl;
    for (const auto &[spgid, created] : m->pgs) {
      monc.send_mon_message(new MOSDPGCreated(spgid.pgid));
    }
  }

  dispatch_result_t ms_dispatch2(const MessageRef &m) override {
    switch (m->get_type()) {
    case CEPH_MSG_OSD_MAP: {
      std::lock_guard l{lock};
      handle_osd_map(ref_cast<MOSDMap>(m));
      return true;
    }
    case MSG_OSD_PG_CREATE2:
      handle_pg_create(ref_cast<MOSDPGCreate2>(m));
      return true;
    }
    return false;
  }

  bool ms_handle_reset(Connection *con) override { return false; }
  void ms_handle_remote_reset(Connection *con) override {}
  bool ms_handle_refused(Connection *con) override { return false; }
};

static ceph::mutex stop_lock = ceph::make_mutex("fake_osd::stop_lock");
static ceph::condition_variable stop_cond;
static bool stop_requested = false;

static void handle_stop_signal(int signum)
{
  std::lock_guard l{stop_lock};
  stop_requested = true;
  stop_cond.notify_all();
}

int main(int argc, const char **argv)
{
  auto args = argv_to_vec(argc, argv);
  auto cct = global_init(nullptr, args, CEPH_ENTITY_TYPE_OSD,
			 CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  std::string err;
  int whoami = strict_strtol(g_conf()->name.get_id().c_str(), 10, &err);
  if (!err.empty() || whoami < 0) {
    std::cerr << "must specify '-i #' where # is the osd id" << std::endl;
    return 1;
  }
  if (g_conf()->osd_uuid.is_zero()) {
    std::cerr << "must specify --osd-uuid matching 'ceph osd new'"
	      << std::endl;
    return 1;
  }

  FakeOSD osd(g_ceph_context, whoami);
  int r = osd.init();
  if (r < 0) {
    std::cerr << "fake osd." << whoami << " init failed: "
	      << cpp_strerror(r) << std::endl;
    return 1;
  }

  init_async_signal_handler();
  register_async_signal_handler_oneshot(SIGINT, handle_stop_signal);
  register_async_signal_handler_oneshot(SIGTERM, handle_stop_signal);
  {
    std::unique_lock l{stop_lock};
    stop_cond.wait(l, [] { return stop_requested; });
  }
  unregister_async_signal_handler(SIGINT, handle_stop_signal);
  unregister_async_signal_handler(SIGTERM, handle_stop_signal);
  shutdown_async_signal_handler();

  osd.shutdown();
  return 0;
}
