// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */
#include "acconfig.h"

#ifdef HAVE_SYS_MOUNT_H
#include <sys/mount.h>
#endif

#ifdef HAVE_SYS_PARAM_H
#include <sys/param.h>
#endif

#include <iostream>
#include <string>
#include <map>

#include <boost/scoped_ptr.hpp>
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int.hpp>


#include "osd/osd_types.h"
#include "osd/OSD.h"
#include "osdc/Objecter.h"
#include "mon/MonClient.h"
#include "msg/Dispatcher.h"
#include "msg/Messenger.h"
#include "common/Timer.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "global/signal_handler.h"
#include "common/config.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/Cond.h"
#include "common/Mutex.h"
#include "common/strtol.h"
#include "common/LogEntry.h"
#include "auth/KeyRing.h"
#include "auth/AuthAuthorizeHandler.h"
#include "include/uuid.h"
#include "include/assert.h"

#include "messages/MOSDBoot.h"
#include "messages/MOSDAlive.h"
#include "messages/MOSDPGCreate.h"
#include "messages/MOSDPGRemove.h"
#include "messages/MOSDMap.h"
#include "messages/MPGStats.h"
#include "messages/MLog.h"
#include "messages/MOSDPGTemp.h"

using namespace std;

#define dout_subsys ceph_subsys_
#undef dout_prefix
#define dout_prefix _prefix(_dout, get_name())
static ostream& _prefix(std::ostream *_dout, string n) {
  return *_dout << " stub(" << n << ") ";
}


typedef boost::mt11213b rngen_t;
typedef boost::scoped_ptr<Messenger> MessengerRef;
typedef boost::scoped_ptr<Objecter> ObjecterRef;

class TestStub : public Dispatcher
{
 protected:
  MessengerRef messenger;
  MonClient monc;

  Mutex lock;
  Cond cond;
  SafeTimer timer;

  bool do_shutdown;
  double tick_seconds;

  struct C_Tick : public Context {
    TestStub *s;
    explicit C_Tick(TestStub *stub) : s(stub) {}
    void finish(int r) {
      generic_dout(20) << "C_Tick::" << __func__ << dendl;
      if (r == -ECANCELED) {
	generic_dout(20) << "C_Tick::" << __func__
			<< " shutdown" << dendl;
	return;
      }
      s->tick();
    }
  };

  virtual bool ms_dispatch(Message *m) = 0;
  virtual void ms_handle_connect(Connection *con) = 0;
  virtual void ms_handle_remote_reset(Connection *con) = 0;
  virtual int _shutdown() = 0;
  // courtesy method to be implemented by the stubs at their
  // own discretion
  virtual void _tick() { }
  // different stubs may have different needs; if a stub needs
  // to tick, then it must call this function.
  void start_ticking(double t=1.0) {
    tick_seconds = t;
    if (t <= 0) {
      stop_ticking();
      return;
    }
    dout(20) << __func__ << " adding tick timer" << dendl;
    timer.add_event_after(tick_seconds, new C_Tick(this));
  }
  // If we have a function to start ticking that the stubs can
  // use at their own discretion, then we should also have a
  // function to disable said ticking to be used the same way.
  // Just in case.
  // For simplicity's sake, we don't cancel the tick right off
  // the bat; instead, we wait for the next tick to kick in and
  // disable itself.
  void stop_ticking() {
    dout(20) << __func__ << " disable tick" << dendl;
    tick_seconds = 0;
  }

 public:
  void tick() {
    std::cout << __func__ << std::endl;
    if (do_shutdown || (tick_seconds <= 0)) {
      std::cout << __func__ << " "
		<< (do_shutdown ? "shutdown" : "stop ticking")
		<< std::endl;
      return;
    }
    _tick();
    timer.add_event_after(tick_seconds, new C_Tick(this));
  }

  virtual const string get_name() = 0;
  virtual int init() = 0;

  virtual int shutdown() {
    Mutex::Locker l(lock);
    do_shutdown = true;
    int r = _shutdown();
    if (r < 0) {
      dout(10) << __func__ << " error shutting down: "
	       << cpp_strerror(-r) << dendl;
      return r;
    }
    monc.shutdown();
    timer.shutdown();
    messenger->shutdown();
    return 0;
  }

  virtual void print(ostream &out) {
    out << "stub(" << get_name() << ")";
  }

  void wait() {
    if (messenger != NULL)
      messenger->wait();
  }

  TestStub(CephContext *cct, string who)
    : Dispatcher(cct),
      monc(cct),
      lock(who.append("::lock").c_str()),
      timer(cct, lock),
      do_shutdown(false),
      tick_seconds(0.0) { }
};

class ClientStub : public TestStub
{
  ObjecterRef objecter;
  rngen_t gen;

 protected:
  bool ms_dispatch(Message *m) {
    Mutex::Locker l(lock);
    dout(1) << "client::" << __func__ << " " << *m << dendl;
    switch (m->get_type()) {
    case CEPH_MSG_OSD_MAP:
      objecter->handle_osd_map((MOSDMap*)m);
      cond.Signal();
      break;
    }
    return true;
  }

  void ms_handle_connect(Connection *con) {
    dout(1) << "client::" << __func__ << " " << con << dendl;
    Mutex::Locker l(lock);
    objecter->ms_handle_connect(con);
  }

  void ms_handle_remote_reset(Connection *con) {
    dout(1) << "client::" << __func__ << " " << con << dendl;
    Mutex::Locker l(lock);
    objecter->ms_handle_remote_reset(con);
  }

  bool ms_handle_reset(Connection *con) {
    dout(1) << "client::" << __func__ << dendl;
    Mutex::Locker l(lock);
    objecter->ms_handle_reset(con);
    return false;
  }

  const string get_name() {
    return "client";
  }

  virtual int _shutdown() {
    if (objecter) {
      objecter->shutdown();
    }
    return 0;
  }

 public:
  explicit ClientStub(CephContext *cct)
    : TestStub(cct, "client"),
      gen((int) time(NULL))
  { }

  int init() {
    int err;
    err = monc.build_initial_monmap();
    if (err < 0) {
      derr << "ClientStub::" << __func__ << " ERROR: build initial monmap: "
	   << cpp_strerror(err) << dendl;
      return err;
    }

    messenger.reset(Messenger::create_client_messenger(cct, "stubclient"));
    assert(messenger.get() != NULL);

    messenger->set_default_policy(
	Messenger::Policy::lossy_client(0, CEPH_FEATURE_OSDREPLYMUX));
    dout(10) << "ClientStub::" << __func__ << " starting messenger at "
	    << messenger->get_myaddr() << dendl;

    objecter.reset(new Objecter(cct, messenger.get(), &monc, NULL, 0, 0));
    assert(objecter.get() != NULL);
    objecter->set_balanced_budget();

    monc.set_messenger(messenger.get());
    objecter->init();
    messenger->add_dispatcher_head(this);
    messenger->start();
    monc.set_want_keys(CEPH_ENTITY_TYPE_MON|CEPH_ENTITY_TYPE_OSD);

    err = monc.init();
    if (err < 0) {
      derr << "ClientStub::" << __func__ << " monc init error: "
	   << cpp_strerror(-err) << dendl;
      return err;
    }

    err = monc.authenticate();
    if (err < 0) {
      derr << "ClientStub::" << __func__ << " monc authenticate error: "
	   << cpp_strerror(-err) << dendl;
      monc.shutdown();
      return err;
    }
    monc.wait_auth_rotating(30.0);

    objecter->set_client_incarnation(0);
    objecter->start();

    lock.Lock();
    timer.init();
    monc.renew_subs();

    lock.Unlock();

    objecter->wait_for_osd_map();

    dout(10) << "ClientStub::" << __func__ << " done" << dendl;
    return 0;
  }
};

typedef boost::scoped_ptr<AuthAuthorizeHandlerRegistry> AuthHandlerRef;
class OSDStub : public TestStub
{
  AuthHandlerRef auth_handler_registry;
  int whoami;
  OSDSuperblock sb;
  OSDMap osdmap;
  osd_stat_t osd_stat;

  map<pg_t,pg_stat_t> pgs;
  set<pg_t> pgs_changes;

  rngen_t gen;
  boost::uniform_int<> mon_osd_rng;

  utime_t last_boot_attempt;
  static const double STUB_BOOT_INTERVAL;


 public:

  enum {
    STUB_MON_OSD_ALIVE	  = 1,
    STUB_MON_OSD_PGTEMP	  = 2,
    STUB_MON_OSD_FAILURE  = 3,
    STUB_MON_OSD_PGSTATS  = 4,
    STUB_MON_LOG	  = 5,

    STUB_MON_OSD_FIRST	  = STUB_MON_OSD_ALIVE,
    STUB_MON_OSD_LAST	  = STUB_MON_LOG,
  };

  struct C_CreatePGs : public Context {
    OSDStub *s;
    explicit C_CreatePGs(OSDStub *stub) : s(stub) {}
    void finish(int r) {
      if (r == -ECANCELED) {
	generic_dout(20) << "C_CreatePGs::" << __func__
			<< " shutdown" << dendl;
	return;
      }
      generic_dout(20) << "C_CreatePGs::" << __func__ << dendl;
      s->auto_create_pgs();
    }
  };


  OSDStub(int _whoami, CephContext *cct)
    : TestStub(cct, "osd"),
      auth_handler_registry(new AuthAuthorizeHandlerRegistry(
				  cct,
				  cct->_conf->auth_cluster_required.length() ?
				  cct->_conf->auth_cluster_required :
				  cct->_conf->auth_supported)),
      whoami(_whoami),
      gen(whoami),
      mon_osd_rng(STUB_MON_OSD_FIRST, STUB_MON_OSD_LAST)
  {
    dout(20) << __func__ << " auth supported: "
	     << cct->_conf->auth_supported << dendl;
    stringstream ss;
    ss << "client-osd" << whoami;
    messenger.reset(Messenger::create(cct, cct->_conf->ms_type, entity_name_t::OSD(whoami),
				      ss.str().c_str(), getpid()));

    Throttle throttler(g_ceph_context, "osd_client_bytes",
	g_conf->osd_client_message_size_cap);
    uint64_t supported =
      CEPH_FEATURE_UID |
      CEPH_FEATURE_NOSRCADDR |
      CEPH_FEATURE_PGID64;

    messenger->set_default_policy(
	Messenger::Policy::stateless_server(supported, 0));
    messenger->set_policy_throttlers(entity_name_t::TYPE_CLIENT,
				    &throttler, NULL);
    messenger->set_policy(entity_name_t::TYPE_MON,
	Messenger::Policy::lossy_client(supported, CEPH_FEATURE_UID |
	  CEPH_FEATURE_PGID64 |
	  CEPH_FEATURE_OSDENC));
    messenger->set_policy(entity_name_t::TYPE_OSD,
	Messenger::Policy::stateless_server(0,0));

    dout(10) << __func__ << " public addr " << g_conf->public_addr << dendl;
    int err = messenger->bind(g_conf->public_addr);
    if (err < 0)
      exit(1);

    if (monc.build_initial_monmap() < 0)
      exit(1);

    messenger->start();
    monc.set_messenger(messenger.get());
  }

  int init() {
    dout(10) << __func__ << dendl;
    Mutex::Locker l(lock);

    dout(1) << __func__ << " fsid " << monc.monmap.fsid
	    << " osd_fsid " << g_conf->osd_uuid << dendl;
    dout(1) << __func__ << " name " << g_conf->name << dendl;

    timer.init();
    messenger->add_dispatcher_head(this);
    monc.set_want_keys(CEPH_ENTITY_TYPE_MON | CEPH_ENTITY_TYPE_OSD);

    int err = monc.init();
    if (err < 0) {
      derr << __func__ << " monc init error: "
	   << cpp_strerror(-err) << dendl;
      return err;
    }

    err = monc.authenticate();
    if (err < 0) {
      derr << __func__ << " monc authenticate error: "
	   << cpp_strerror(-err) << dendl;
      monc.shutdown();
      return err;
    }
    assert(!monc.get_fsid().is_zero());

    monc.wait_auth_rotating(30.0);


    dout(10) << __func__ << " creating osd superblock" << dendl;
    sb.cluster_fsid = monc.monmap.fsid;
    sb.osd_fsid.generate_random();
    sb.whoami = whoami;
    sb.compat_features = CompatSet();
    dout(20) << __func__ << " " << sb << dendl;
    dout(20) << __func__ << " osdmap " << osdmap << dendl;

    update_osd_stat();

    start_ticking();
    // give a chance to the mons to inform us of what PGs we should create
    timer.add_event_after(30.0, new C_CreatePGs(this));

    return 0;
  }

  int _shutdown() {

    return 0;
  }

  void boot() {
    dout(1) << __func__ << " boot?" << dendl;

    utime_t now = ceph_clock_now(messenger->cct);
    if ((last_boot_attempt > 0.0)
	&& ((now - last_boot_attempt)) <= STUB_BOOT_INTERVAL) {
      dout(1) << __func__ << " backoff and try again later." << dendl;
      return;
    }

    dout(1) << __func__ << " boot!" << dendl;
    MOSDBoot *mboot = new MOSDBoot;
    mboot->sb = sb;
    last_boot_attempt = now;
    monc.send_mon_message(mboot);
  }

  void add_pg(pg_t pgid, epoch_t epoch, pg_t parent) {

    utime_t now = ceph_clock_now(messenger->cct);

    pg_stat_t s;
    s.created = epoch;
    s.last_epoch_clean = epoch;
    s.parent = parent;
    s.state |= PG_STATE_CLEAN | PG_STATE_ACTIVE;
    s.last_fresh = now;
    s.last_change = now;
    s.last_clean = now;
    s.last_active = now;
    s.last_unstale = now;

    pgs[pgid] = s;
    pgs_changes.insert(pgid);
  }

  void auto_create_pgs() {
    bool has_pgs = !pgs.empty();
    dout(10) << __func__
	     << ": " << (has_pgs ? "has pgs; ignore" : "create pgs") << dendl;
    if (has_pgs)
      return;

    if (!osdmap.get_epoch()) {
      dout(1) << __func__
	      << " still don't have osdmap; reschedule pg creation" << dendl;
      timer.add_event_after(10.0, new C_CreatePGs(this));
      return;
    }

    const map<int64_t,pg_pool_t> &osdmap_pools = osdmap.get_pools();
    map<int64_t,pg_pool_t>::const_iterator pit;
    for (pit = osdmap_pools.begin(); pit != osdmap_pools.end(); ++pit) {
      const int64_t pool_id = pit->first;
      const pg_pool_t &pool = pit->second;
      int ruleno = pool.get_crush_ruleset();

      if (!osdmap.crush->rule_exists(ruleno)) {
	dout(20) << __func__
		 << " no crush rule for pool id " << pool_id
		 << " rule no " << ruleno << dendl;
	continue;
      }

      epoch_t pool_epoch = pool.get_last_change();
      dout(20) << __func__
	       << " pool num pgs " << pool.get_pg_num()
	       << " epoch " << pool_epoch << dendl;

      for (ps_t ps = 0; ps < pool.get_pg_num(); ++ps) {
	pg_t pgid(ps, pool_id, -1);
	pg_t parent;
	dout(20) << __func__
		 << " pgid " << pgid << " parent " << parent << dendl;
	add_pg(pgid, pool_epoch, parent);
      }
    }
  }

  void update_osd_stat() {
    struct statfs stbuf;
    int ret = statfs(".", &stbuf);
    if (ret < 0) {
      ret = -errno;
      dout(0) << __func__ 
              << " cannot statfs ." << cpp_strerror(ret) << dendl;
      return;
    }

    osd_stat.kb = stbuf.f_blocks * stbuf.f_bsize / 1024;
    osd_stat.kb_used = (stbuf.f_blocks - stbuf.f_bfree) * stbuf.f_bsize / 1024;
    osd_stat.kb_avail = stbuf.f_bavail * stbuf.f_bsize / 1024;
  }

  void send_pg_stats() {
    dout(10) << __func__
	     << " pgs " << pgs.size() << " osdmap " << osdmap << dendl;
    utime_t now = ceph_clock_now(messenger->cct);
    MPGStats *mstats = new MPGStats(monc.get_fsid(), osdmap.get_epoch(), now);

    mstats->set_tid(1);
    mstats->osd_stat = osd_stat;

    set<pg_t>::iterator it;
    for (it = pgs_changes.begin(); it != pgs_changes.end(); ++it) {
      pg_t pgid = (*it);
      if (pgs.count(pgid) == 0) {
	derr << __func__
	     << " pgid " << pgid << " not on our map" << dendl;
	assert(0 == "pgid not on our map");
      }
      pg_stat_t &s = pgs[pgid];
      mstats->pg_stat[pgid] = s;

      JSONFormatter f(true);
      s.dump(&f);
      dout(20) << __func__
	       << " pg " << pgid << " stats:\n";
      f.flush(*_dout);
      *_dout << dendl;

    }
    dout(10) << __func__ << " send " << *mstats << dendl;
    monc.send_mon_message(mstats);
  }

  void modify_pg(pg_t pgid) {
    dout(10) << __func__ << " pg " << pgid << dendl;
    assert(pgs.count(pgid) > 0);

    pg_stat_t &s = pgs[pgid];
    utime_t now = ceph_clock_now(messenger->cct);

    if (now - s.last_change < 10.0) {
      dout(10) << __func__
	       << " pg " << pgid << " changed in the last 10s" << dendl;
      return;
    }

    s.state ^= PG_STATE_CLEAN;
    if (s.state & PG_STATE_CLEAN)
      s.last_clean = now;
    s.last_change = now;
    s.reported_seq++;

    pgs_changes.insert(pgid);
  }

  void modify_pgs() {
    dout(10) << __func__ << dendl;

    if (pgs.empty()) {
      dout(1) << __func__
	      << " no pgs available! don't attempt to modify." << dendl;
      return;
    }

    boost::uniform_int<> pg_rng(0, pgs.size()-1);
    set<int> pgs_pos;

    int num_pgs = pg_rng(gen);
    while ((int)pgs_pos.size() < num_pgs)
      pgs_pos.insert(pg_rng(gen));

    map<pg_t,pg_stat_t>::iterator it = pgs.begin();
    set<int>::iterator pos_it = pgs_pos.begin();

    int pgs_at = 0;
    while (pos_it != pgs_pos.end()) {
      int at = *pos_it;
      dout(20) << __func__ << " pg at pos " << at << dendl;
      while ((pgs_at != at) && (it != pgs.end())) {
	++it;
	++pgs_at;
      }
      assert(it != pgs.end());
      dout(20) << __func__
	       << " pg at pos " << at << ": " << it->first << dendl;
      modify_pg(it->first);
      ++pos_it;
    }
  }

  void op_alive() {
    dout(10) << __func__ << dendl;
    if (!osdmap.exists(whoami)) {
      dout(0) << __func__ << " I'm not in the osdmap!!\n";
      JSONFormatter f(true);
      osdmap.dump(&f);
      f.flush(*_dout);
      *_dout << dendl;
    }
    if (osdmap.get_epoch() == 0) {
      dout(1) << __func__ << " wait for osdmap" << dendl;
      return;
    }
    epoch_t up_thru = osdmap.get_up_thru(whoami);
    dout(10) << __func__ << "up_thru: " << osdmap.get_up_thru(whoami) << dendl;

    monc.send_mon_message(new MOSDAlive(osdmap.get_epoch(), up_thru));
  }

  void op_pgtemp() {
    if (osdmap.get_epoch() == 0) {
      dout(1) << __func__ << " wait for osdmap" << dendl;
      return;
    }
    dout(10) << __func__ << dendl;
    MOSDPGTemp *m = new MOSDPGTemp(osdmap.get_epoch());
    monc.send_mon_message(m);
  }

  void op_failure() {
    dout(10) << __func__ << dendl;
  }

  void op_pgstats() {
    dout(10) << __func__ << dendl;

    modify_pgs();
    if (!pgs_changes.empty())
      send_pg_stats();
    monc.sub_want("osd_pg_creates", 0, CEPH_SUBSCRIBE_ONETIME);
    monc.renew_subs();

    dout(20) << __func__ << " pg pools:\n";

    JSONFormatter f(true);
    f.open_array_section("pools");
    const map<int64_t,pg_pool_t> &osdmap_pools = osdmap.get_pools();
    map<int64_t,pg_pool_t>::const_iterator pit;
    for (pit = osdmap_pools.begin(); pit != osdmap_pools.end(); ++pit) {
      const int64_t pool_id = pit->first;
      const pg_pool_t &pool = pit->second;
      f.open_object_section("pool");
      f.dump_int("pool_id", pool_id);
      f.open_object_section("pool_dump");
      pool.dump(&f);
      f.close_section();
      f.close_section();
    }
    f.close_section();
    f.flush(*_dout);
    *_dout << dendl;
  }

  void op_log() {
    dout(10) << __func__ << dendl;

    MLog *m = new MLog(monc.get_fsid());

    boost::uniform_int<> log_rng(1, 10);
    size_t num_entries = log_rng(gen);
    dout(10) << __func__
	     << " send " << num_entries << " log messages" << dendl;

    utime_t now = ceph_clock_now(messenger->cct);
    int seq = 0;
    for (; num_entries > 0; --num_entries) {
      LogEntry e;
      e.who = messenger->get_myinst();
      e.stamp = now;
      e.seq = seq++;
      e.prio = CLOG_DEBUG;
      e.msg = "OSDStub::op_log";
      m->entries.push_back(e);
    }

    monc.send_mon_message(m);
  }

  void _tick() {
    if (!osdmap.exists(whoami)) {
      std::cout << __func__ << " not in the cluster; boot!" << std::endl;
      boot();
      return;
    }

    update_osd_stat();

    boost::uniform_int<> op_rng(STUB_MON_OSD_FIRST, STUB_MON_OSD_LAST);
    int op = op_rng(gen);
    switch (op) {
    case STUB_MON_OSD_ALIVE:
      op_alive();
      break;
    case STUB_MON_OSD_PGTEMP:
      op_pgtemp();
      break;
    case STUB_MON_OSD_FAILURE:
      op_failure();
      break;
    case STUB_MON_OSD_PGSTATS:
      op_pgstats();
      break;
    case STUB_MON_LOG:
      op_log();
      break;
    }
  }

  void handle_pg_create(MOSDPGCreate *m) {
    assert(m != NULL);
    if (m->epoch < osdmap.get_epoch()) {
      std::cout << __func__ << " epoch " << m->epoch << " < "
	       << osdmap.get_epoch() << "; dropping" << std::endl;
      m->put();
      return;
    }

    for (map<pg_t,pg_create_t>::iterator it = m->mkpg.begin();
	 it != m->mkpg.end(); ++it) {
      pg_create_t &c = it->second;
      std::cout << __func__ << " pg " << it->first
	      << " created " << c.created
	      << " parent " << c.parent << std::endl;
      if (pgs.count(it->first)) {
	std::cout << __func__ << " pg " << it->first
		 << " exists; skipping" << std::endl;
	continue;
      }

      pg_t pgid = it->first;
      add_pg(pgid, c.created, c.parent);
    }
    send_pg_stats();
  }

  void handle_osd_map(MOSDMap *m) {
    dout(1) << __func__ << dendl;
    if (m->fsid != monc.get_fsid()) {
      dout(0) << __func__
              << " message fsid " << m->fsid << " != " << monc.get_fsid()
              << dendl;
      dout(0) << __func__ << " " << m
              << " from " << m->get_source_inst()
              << dendl;
      dout(0) << monc.get_monmap() << dendl;
    }
    assert(m->fsid == monc.get_fsid());

    epoch_t first = m->get_first();
    epoch_t last = m->get_last();
    dout(5) << __func__
	    << " epochs [" << first << "," << last << "]"
	    << " current " << osdmap.get_epoch() << dendl;

    if (last <= osdmap.get_epoch()) {
      dout(5) << __func__ << " no new maps here; dropping" << dendl;
      m->put();
      return;
    }

    if (first > osdmap.get_epoch() + 1) {
      dout(5) << __func__
	      << osdmap.get_epoch() + 1 << ".." << (first-1) << dendl;
      if ((m->oldest_map < first && osdmap.get_epoch() == 0) ||
	  m->oldest_map <= osdmap.get_epoch()) {
	monc.sub_want("osdmap", osdmap.get_epoch()+1,
		       CEPH_SUBSCRIBE_ONETIME);
	monc.renew_subs();
	m->put();
	return;
      }
    }

    epoch_t start_full = MAX(osdmap.get_epoch() + 1, first);

    if (m->maps.size() > 0) {
      map<epoch_t,bufferlist>::reverse_iterator rit;
      rit = m->maps.rbegin();
      if (start_full <= rit->first) {
	start_full = rit->first;
	dout(5) << __func__
		<< " full epoch " << start_full << dendl;
	bufferlist &bl = rit->second;
	bufferlist::iterator p = bl.begin();
	osdmap.decode(p);
      }
    }

    for (epoch_t e = start_full; e <= last; e++) {
      map<epoch_t,bufferlist>::iterator it;
      it = m->incremental_maps.find(e);
      if (it == m->incremental_maps.end())
	continue;

      dout(20) << __func__
	       << " incremental epoch " << e
	       << " on full epoch " << start_full << dendl;
      OSDMap::Incremental inc;
      bufferlist &bl = it->second;
      bufferlist::iterator p = bl.begin();
      inc.decode(p);

      int err = osdmap.apply_incremental(inc);
      if (err < 0) {
	derr << "osd." << whoami << "::" << __func__
	     << "** ERROR: applying incremental: "
	     << cpp_strerror(err) << dendl;
	assert(0 == "error applying incremental");
      }
    }
    dout(30) << __func__ << "\nosdmap:\n";
    JSONFormatter f(true);
    osdmap.dump(&f);
    f.flush(*_dout);
    *_dout << dendl;

    if (osdmap.is_up(whoami) &&
	osdmap.get_addr(whoami) == messenger->get_myaddr()) {
      dout(1) << __func__
	      << " got into the osdmap and we're up!" << dendl;
    }

    if (m->newest_map && m->newest_map > last) {
      dout(1) << __func__
	      << " they have more maps; requesting them!" << dendl;
      monc.sub_want("osdmap", osdmap.get_epoch()+1, CEPH_SUBSCRIBE_ONETIME);
      monc.renew_subs();
    }

    dout(10) << __func__ << " done" << dendl;
    m->put();
  }

  bool ms_dispatch(Message *m) {
    dout(1) << __func__ << " " << *m << dendl;

    switch (m->get_type()) {
    case MSG_OSD_PG_CREATE:
      handle_pg_create((MOSDPGCreate*)m);
      break;
    case CEPH_MSG_OSD_MAP:
      handle_osd_map((MOSDMap*)m);
      break;
    default:
      m->put();
      break;
    }
    return true;
  }

  void ms_handle_connect(Connection *con) {
    dout(1) << __func__ << " " << con << dendl;
    if (con->get_peer_type() == CEPH_ENTITY_TYPE_MON) {
      dout(10) << __func__ << " on mon" << dendl;
    }
  }

  void ms_handle_remote_reset(Connection *con) {}

  bool ms_handle_reset(Connection *con) {
    dout(1) << __func__ << dendl;
    OSD::Session *session = (OSD::Session *)con->get_priv();
    if (!session)
      return false;
    session->put();
    return true;
  }

  const string get_name() {
    stringstream ss;
    ss << "osd." << whoami;
    return ss.str();
  }
};

double const OSDStub::STUB_BOOT_INTERVAL = 10.0;

#undef dout_prefix
#define dout_prefix *_dout << "main "

const char *our_name = NULL;
vector<TestStub*> stubs;
Mutex shutdown_lock("main::shutdown_lock");
Cond shutdown_cond;
Context *shutdown_cb = NULL;
SafeTimer *shutdown_timer = NULL;

struct C_Shutdown : public Context
{
  void finish(int r) {
    generic_dout(10) << "main::shutdown time has ran out" << dendl;
    shutdown_cond.Signal();
  }
};

void handle_test_signal(int signum)
{
  if ((signum != SIGINT) && (signum != SIGTERM))
    return;

  std::cerr << "*** Got signal " << sig_str(signum) << " ***" << std::endl;
  Mutex::Locker l(shutdown_lock);
  if (shutdown_timer) {
    shutdown_timer->cancel_all_events();
    shutdown_cond.Signal();
  }
}

void usage() {
  assert(our_name != NULL);

  std::cout << "usage: " << our_name
	    << " <--stub-id ID> [--stub-id ID...]"
	    << std::endl;
  std::cout << "\n\
Global Options:\n\
  -c FILE                   Read configuration from FILE\n\
  --keyring FILE            Read keyring from FILE\n\
  --help                    This message\n\
\n\
Test-specific Options:\n\
  --stub-id ID1..ID2        Interval of OSD ids for multiple stubs to mimic.\n\
  --stub-id ID              OSD id a stub will mimic to be\n\
                            (same as --stub-id ID..ID)\n\
" << std::endl;
}

int get_id_interval(int &first, int &last, string &str)
{
  size_t found = str.find("..");
  string first_str, last_str;
  if (found == string::npos) {
    first_str = last_str = str;
  } else {
    first_str = str.substr(0, found);
    last_str = str.substr(found+2);
  }

  string err;
  first = strict_strtol(first_str.c_str(), 10, &err);
  if ((first == 0) && (!err.empty())) {
    std::cerr << err << std::endl;
    return -1;
  }

  last = strict_strtol(last_str.c_str(), 10, &err);
  if ((last == 0) && (!err.empty())) {
    std::cerr << err << std::endl;
    return -1;
  }
  return 0;
}

int main(int argc, const char *argv[])
{
  vector<const char*> def_args;
  vector<const char*> args;
  our_name = argv[0];
  argv_to_vec(argc, argv, args);

  global_init(&def_args, args,
	      CEPH_ENTITY_TYPE_OSD, CODE_ENVIRONMENT_UTILITY,
	      0);

  common_init_finish(g_ceph_context);
  g_ceph_context->_conf->apply_changes(NULL);

  set<int> stub_ids;
  double duration = 300.0;

  for (std::vector<const char*>::iterator i = args.begin(); i != args.end();) {
    string val;

    if (ceph_argparse_double_dash(args, i)) {
      break;
    } else if (ceph_argparse_witharg(args, i, &val,
        "--stub-id", (char*) NULL)) {
      int first = -1, last = -1;
      if (get_id_interval(first, last, val) < 0) {
	std::cerr << "** error parsing stub id '" << val << "'" << std::endl;
	exit(1);
      }

      for (; first <= last; ++first)
	stub_ids.insert(first);
    } else if (ceph_argparse_witharg(args, i, &val,
	"--duration", (char*) NULL)) {
      string err;
      duration = (double) strict_strtol(val.c_str(), 10, &err);
      if ((duration == 0) && (!err.empty())) {
	std::cerr << "** error parsing '--duration " << val << "': '"
		  << err << std::endl;
	exit(1);
      }
    } else if (ceph_argparse_flag(args, i, "--help", (char*) NULL)) {
      usage();
      exit(0);
    } else {
      std::cerr << "unknown argument '" << *i << "'" << std::endl;
      return 1;
    }
  }

  if (stub_ids.empty()) {
    std::cerr << "** error: must specify at least one '--stub-id <ID>'"
         << std::endl;
    usage();
    return 1;
  }

  for (set<int>::iterator i = stub_ids.begin(); i != stub_ids.end(); ++i) {
    int whoami = *i;

    std::cout << __func__ << " starting stub." << whoami << std::endl;
    OSDStub *stub = new OSDStub(whoami, g_ceph_context);
    int err = stub->init();
    if (err < 0) {
      std::cerr << "** osd stub error: " << cpp_strerror(-err) << std::endl;
      return 1;
    }
    stubs.push_back(stub);
  }

  std::cout << __func__ << " starting client stub" << std::endl;
  ClientStub *cstub = new ClientStub(g_ceph_context);
  int err = cstub->init();
  if (err < 0) {
    std::cerr << "** client stub error: " << cpp_strerror(-err) << std::endl;
    return 1;
  }
  stubs.push_back(cstub);

  init_async_signal_handler();
  register_async_signal_handler_oneshot(SIGINT, handle_test_signal);
  register_async_signal_handler_oneshot(SIGTERM, handle_test_signal);

  shutdown_lock.Lock();
  shutdown_timer = new SafeTimer(g_ceph_context, shutdown_lock);
  shutdown_timer->init();
  if (duration != 0) {
    std::cout << __func__
	    << " run test for " << duration << " seconds" << std::endl;
    shutdown_timer->add_event_after((double) duration, new C_Shutdown);
  }
  shutdown_cond.Wait(shutdown_lock);

  shutdown_timer->shutdown();
  delete shutdown_timer;
  shutdown_timer = NULL;
  shutdown_lock.Unlock();

  unregister_async_signal_handler(SIGINT, handle_test_signal);
  unregister_async_signal_handler(SIGTERM, handle_test_signal);

  std::cout << __func__ << " waiting for stubs to finish" << std::endl;
  vector<TestStub*>::iterator it;
  int i;
  for (i = 0, it = stubs.begin(); it != stubs.end(); ++it, ++i) {
    if (*it != NULL) {
      (*it)->shutdown();
      (*it)->wait();
      std::cout << __func__ << " finished " << (*it)->get_name() << std::endl;
      delete (*it);
      (*it) = NULL;
    }
  }

  return 0;
}
