// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common/debug.h"
#include "common/errno.h"

#include "messages/MClientRequestForward.h"
#include "messages/MMDSLoadTargets.h"
#include "messages/MMDSMap.h"
#include "messages/MMDSTableRequest.h"
#include "messages/MCommand.h"
#include "messages/MCommandReply.h"

#include "MDSDaemon.h"
#include "MDSMap.h"
#include "SnapClient.h"
#include "SnapServer.h"
#include "MDBalancer.h"
#include "Locker.h"
#include "Server.h"
#include "InoTable.h"
#include "mon/MonClient.h"
#include "common/HeartbeatMap.h"
#include "ScrubStack.h"


#include "MDSRank.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds." << whoami << '.' << incarnation << ' '

MDSRank::MDSRank(
    mds_rank_t whoami_,
    Mutex &mds_lock_,
    LogChannelRef &clog_,
    SafeTimer &timer_,
    Beacon &beacon_,
    MDSMap *& mdsmap_,
    Messenger *msgr,
    MonClient *monc_,
    Context *respawn_hook_,
    Context *suicide_hook_)
  :
    whoami(whoami_), incarnation(0),
    mds_lock(mds_lock_), clog(clog_), timer(timer_),
    mdsmap(mdsmap_),
    objecter(new Objecter(g_ceph_context, msgr, monc_, nullptr, 0, 0)),
    server(NULL), mdcache(NULL), locker(NULL), mdlog(NULL),
    balancer(NULL), scrubstack(NULL),
    damage_table(whoami_),
    inotable(NULL), snapserver(NULL), snapclient(NULL),
    sessionmap(this), logger(NULL), mlogger(NULL),
    op_tracker(g_ceph_context, g_conf->mds_enable_op_tracker,
               g_conf->osd_num_op_tracker_shard),
    last_state(MDSMap::STATE_BOOT),
    state(MDSMap::STATE_BOOT),
    cluster_degraded(false), stopping(false),
    purge_queue(g_ceph_context, whoami_,
      mdsmap_->get_metadata_pool(), objecter,
      new FunctionContext(
          [this](int r){
          // Purge Queue operates inside mds_lock when we're calling into
          // it, and outside when in background, so must handle both cases.
          if (mds_lock.is_locked_by_me()) {
            damaged();
          } else {
            damaged_unlocked();
          }
        }
      )
    ),
    progress_thread(this), dispatch_depth(0),
    hb(NULL), last_tid(0), osd_epoch_barrier(0), beacon(beacon_),
    mds_slow_req_count(0),
    last_client_mdsmap_bcast(0),
    messenger(msgr), monc(monc_),
    respawn_hook(respawn_hook_),
    suicide_hook(suicide_hook_),
    standby_replaying(false)
{
  hb = g_ceph_context->get_heartbeat_map()->add_worker("MDSRank", pthread_self());

  purge_queue.update_op_limit(*mdsmap);

  objecter->unset_honor_osdmap_full();

  finisher = new Finisher(msgr->cct);

  mdcache = new MDCache(this, purge_queue);
  mdlog = new MDLog(this);
  balancer = new MDBalancer(this, messenger, monc);

  scrubstack = new ScrubStack(mdcache, finisher);

  inotable = new InoTable(this);
  snapserver = new SnapServer(this, monc);
  snapclient = new SnapClient(this);

  server = new Server(this);
  locker = new Locker(this, mdcache);

  op_tracker.set_complaint_and_threshold(msgr->cct->_conf->mds_op_complaint_time,
                                         msgr->cct->_conf->mds_op_log_threshold);
  op_tracker.set_history_size_and_duration(msgr->cct->_conf->mds_op_history_size,
                                           msgr->cct->_conf->mds_op_history_duration);
}

MDSRank::~MDSRank()
{
  if (hb) {
    g_ceph_context->get_heartbeat_map()->remove_worker(hb);
  }

  if (scrubstack) { delete scrubstack; scrubstack = NULL; }
  if (mdcache) { delete mdcache; mdcache = NULL; }
  if (mdlog) { delete mdlog; mdlog = NULL; }
  if (balancer) { delete balancer; balancer = NULL; }
  if (inotable) { delete inotable; inotable = NULL; }
  if (snapserver) { delete snapserver; snapserver = NULL; }
  if (snapclient) { delete snapclient; snapclient = NULL; }
  if (mdsmap) { delete mdsmap; mdsmap = 0; }

  if (server) { delete server; server = 0; }
  if (locker) { delete locker; locker = 0; }

  if (logger) {
    g_ceph_context->get_perfcounters_collection()->remove(logger);
    delete logger;
    logger = 0;
  }
  if (mlogger) {
    g_ceph_context->get_perfcounters_collection()->remove(mlogger);
    delete mlogger;
    mlogger = 0;
  }

  delete finisher;
  finisher = NULL;

  delete suicide_hook;
  suicide_hook = NULL;

  delete respawn_hook;
  respawn_hook = NULL;

  delete objecter;
  objecter = nullptr;
}

void MDSRankDispatcher::init()
{
  objecter->init();
  messenger->add_dispatcher_head(objecter);

  objecter->start();

  update_log_config();
  create_logger();

  // Expose the OSDMap (already populated during MDS::init) to anyone
  // who is interested in it.
  handle_osd_map();

  progress_thread.create("mds_rank_progr");

  purge_queue.init();

  finisher->start();
}

void MDSRank::update_targets(utime_t now)
{
  // get MonMap's idea of my export_targets
  const set<mds_rank_t>& map_targets = mdsmap->get_mds_info(get_nodeid()).export_targets;

  dout(20) << "updating export targets, currently " << map_targets.size() << " ranks are targets" << dendl;

  bool send = false;
  set<mds_rank_t> new_map_targets;

  auto it = export_targets.begin();
  while (it != export_targets.end()) {
    mds_rank_t rank = it->first;
    double val = it->second.get(now);
    dout(20) << "export target mds." << rank << " value is " << val << " @ " << now << dendl;

    if (val <= 0.01) {
      dout(15) << "export target mds." << rank << " is no longer an export target" << dendl;
      export_targets.erase(it++);
      send = true;
      continue;
    }
    if (!map_targets.count(rank)) {
      dout(15) << "export target mds." << rank << " not in map's export_targets" << dendl;
      send = true;
    }
    new_map_targets.insert(rank);
    it++;
  }
  if (new_map_targets.size() < map_targets.size()) {
    dout(15) << "export target map holds stale targets, sending update" << dendl;
    send = true;
  }

  if (send) {
    dout(15) << "updating export_targets, now " << new_map_targets.size() << " ranks are targets" << dendl;
    MMDSLoadTargets* m = new MMDSLoadTargets(mds_gid_t(monc->get_global_id()), new_map_targets);
    monc->send_mon_message(m);
  }
}

void MDSRank::hit_export_target(utime_t now, mds_rank_t rank, double amount)
{
  double rate = g_conf->mds_bal_target_decay;
  if (amount < 0.0) {
    amount = 100.0/g_conf->mds_bal_target_decay; /* a good default for "i am trying to keep this export_target active" */
  }
  auto em = export_targets.emplace(std::piecewise_construct, std::forward_as_tuple(rank), std::forward_as_tuple(now, DecayRate(rate)));
  if (em.second) {
    dout(15) << "hit export target (new) " << amount << " @ " << now << dendl;
  } else {
    dout(15) << "hit export target " << amount << " @ " << now << dendl;
  }
  em.first->second.hit(now, amount);
}

void MDSRankDispatcher::tick()
{
  heartbeat_reset();

  if (beacon.is_laggy()) {
    dout(5) << "tick bailing out since we seem laggy" << dendl;
    return;
  }

  check_ops_in_flight();

  // Wake up thread in case we use to be laggy and have waiting_for_nolaggy
  // messages to progress.
  progress_thread.signal();

  // make sure mds log flushes, trims periodically
  mdlog->flush();

  if (is_active() || is_stopping()) {
    mdcache->trim();
    mdcache->trim_client_leases();
    mdcache->check_memory_usage();
    mdlog->trim();  // NOT during recovery!
  }

  // log
  mds_load_t load = balancer->get_load(ceph_clock_now());

  if (logger) {
    logger->set(l_mds_load_cent, 100 * load.mds_load());
    logger->set(l_mds_dispatch_queue_len, messenger->get_dispatch_queue_len());
    logger->set(l_mds_subtrees, mdcache->num_subtrees());

    mdcache->log_stat();
  }

  // ...
  if (is_clientreplay() || is_active() || is_stopping()) {
    server->find_idle_sessions();
    locker->tick();
  }

  if (is_reconnect())
    server->reconnect_tick();

  if (is_active()) {
    balancer->tick();
    mdcache->find_stale_fragment_freeze();
    mdcache->migrator->find_stale_export_freeze();
    if (snapserver)
      snapserver->check_osd_map(false);
  }

  if (is_active() || is_stopping()) {
    update_targets(ceph_clock_now());
  }

  // shut down?
  if (is_stopping()) {
    mdlog->trim();
    if (mdcache->shutdown_pass()) {
      uint64_t pq_progress = 0 ;
      uint64_t pq_total = 0;
      size_t pq_in_flight = 0;
      if (!purge_queue.drain(&pq_progress, &pq_total, &pq_in_flight)) {
        dout(7) << "shutdown_pass=true, but still waiting for purge queue"
                << dendl;
        // This takes unbounded time, so we must indicate progress
        // to the administrator: we do it in a slightly imperfect way
        // by sending periodic (tick frequency) clog messages while
        // in this state.
        clog->info() << "MDS rank " << whoami << " waiting for purge queue ("
          << std::dec << pq_progress << "/" << pq_total << " " << pq_in_flight
          << " files purging" << ")";
      } else {
        dout(7) << "shutdown_pass=true, finished w/ shutdown, moving to "
                   "down:stopped" << dendl;
        stopping_done();
      }
    }
    else {
      dout(7) << "shutdown_pass=false" << dendl;
    }
  }

  // Expose ourselves to Beacon to update health indicators
  beacon.notify_health(this);
}

void MDSRankDispatcher::shutdown()
{
  // It should never be possible for shutdown to get called twice, because
  // anyone picking up mds_lock checks if stopping is true and drops
  // out if it is.
  assert(stopping == false);
  stopping = true;

  dout(1) << __func__ << ": shutting down rank " << whoami << dendl;

  timer.shutdown();

  // MDLog has to shut down before the finisher, because some of its
  // threads block on IOs that require finisher to complete.
  mdlog->shutdown();

  // shut down cache
  mdcache->shutdown();

  purge_queue.shutdown();

  mds_lock.Unlock();
  finisher->stop(); // no flushing
  mds_lock.Lock();

  if (objecter->initialized)
    objecter->shutdown();

  monc->shutdown();

  op_tracker.on_shutdown();

  progress_thread.shutdown();

  // release mds_lock for finisher/messenger threads (e.g.
  // MDSDaemon::ms_handle_reset called from Messenger).
  mds_lock.Unlock();

  // shut down messenger
  messenger->shutdown();

  mds_lock.Lock();

  // Workaround unclean shutdown: HeartbeatMap will assert if
  // worker is not removed (as we do in ~MDS), but ~MDS is not
  // always called after suicide.
  if (hb) {
    g_ceph_context->get_heartbeat_map()->remove_worker(hb);
    hb = NULL;
  }
}

/**
 * Helper for simple callbacks that call a void fn with no args.
 */
class C_MDS_VoidFn : public MDSInternalContext
{
  typedef void (MDSRank::*fn_ptr)();
  protected:
   fn_ptr fn;
  public:
  C_MDS_VoidFn(MDSRank *mds_, fn_ptr fn_)
    : MDSInternalContext(mds_), fn(fn_)
  {
    assert(mds_);
    assert(fn_);
  }

  void finish(int r) override
  {
    (mds->*fn)();
  }
};

int64_t MDSRank::get_metadata_pool()
{
    return mdsmap->get_metadata_pool();
}

MDSTableClient *MDSRank::get_table_client(int t)
{
  switch (t) {
  case TABLE_ANCHOR: return NULL;
  case TABLE_SNAP: return snapclient;
  default: ceph_abort();
  }
}

MDSTableServer *MDSRank::get_table_server(int t)
{
  switch (t) {
  case TABLE_ANCHOR: return NULL;
  case TABLE_SNAP: return snapserver;
  default: ceph_abort();
  }
}

void MDSRank::suicide()
{
  if (suicide_hook) {
    suicide_hook->complete(0);
    suicide_hook = NULL;
  }
}

void MDSRank::respawn()
{
  if (respawn_hook) {
    respawn_hook->complete(0);
    respawn_hook = NULL;
  }
}

void MDSRank::damaged()
{
  assert(whoami != MDS_RANK_NONE);
  assert(mds_lock.is_locked_by_me());

  beacon.set_want_state(mdsmap, MDSMap::STATE_DAMAGED);
  monc->flush_log();  // Flush any clog error from before we were called
  beacon.notify_health(this);  // Include latest status in our swan song
  beacon.send_and_wait(g_conf->mds_mon_shutdown_timeout);

  // It's okay if we timed out and the mon didn't get our beacon, because
  // another daemon (or ourselves after respawn) will eventually take the
  // rank and report DAMAGED again when it hits same problem we did.

  respawn();  // Respawn into standby in case mon has other work for us
}

void MDSRank::damaged_unlocked()
{
  Mutex::Locker l(mds_lock);
  damaged();
}

void MDSRank::handle_write_error(int err)
{
  if (err == -EBLACKLISTED) {
    derr << "we have been blacklisted (fenced), respawning..." << dendl;
    respawn();
    return;
  }

  if (g_conf->mds_action_on_write_error >= 2) {
    derr << "unhandled write error " << cpp_strerror(err) << ", suicide..." << dendl;
    respawn();
  } else if (g_conf->mds_action_on_write_error == 1) {
    derr << "unhandled write error " << cpp_strerror(err) << ", force readonly..." << dendl;
    mdcache->force_readonly();
  } else {
    // ignore;
    derr << "unhandled write error " << cpp_strerror(err) << ", ignore..." << dendl;
  }
}

void *MDSRank::ProgressThread::entry()
{
  Mutex::Locker l(mds->mds_lock);
  while (true) {
    while (!mds->stopping &&
	   mds->finished_queue.empty() &&
	   (mds->waiting_for_nolaggy.empty() || mds->beacon.is_laggy())) {
      cond.Wait(mds->mds_lock);
    }

    if (mds->stopping) {
      break;
    }

    mds->_advance_queues();
  }

  return NULL;
}


void MDSRank::ProgressThread::shutdown()
{
  assert(mds->mds_lock.is_locked_by_me());
  assert(mds->stopping);

  if (am_self()) {
    // Stopping is set, we will fall out of our main loop naturally
  } else {
    // Kick the thread to notice mds->stopping, and join it
    cond.Signal();
    mds->mds_lock.Unlock();
    if (is_started())
      join();
    mds->mds_lock.Lock();
  }
}

bool MDSRankDispatcher::ms_dispatch(Message *m)
{
  bool ret;
  inc_dispatch_depth();
  ret = _dispatch(m, true);
  dec_dispatch_depth();
  return ret;
}

/* If this function returns true, it recognizes the message and has taken the
 * reference. If it returns false, it has done neither. */
bool MDSRank::_dispatch(Message *m, bool new_msg)
{
  if (is_stale_message(m)) {
    m->put();
    return true;
  }

  if (beacon.is_laggy()) {
    dout(10) << " laggy, deferring " << *m << dendl;
    waiting_for_nolaggy.push_back(m);
  } else if (new_msg && !waiting_for_nolaggy.empty()) {
    dout(10) << " there are deferred messages, deferring " << *m << dendl;
    waiting_for_nolaggy.push_back(m);
  } else {
    if (!handle_deferrable_message(m)) {
      dout(0) << "unrecognized message " << *m << dendl;
      return false;
    }

    heartbeat_reset();
  }

  if (dispatch_depth > 1)
    return true;

  // finish any triggered contexts
  _advance_queues();

  if (beacon.is_laggy()) {
    // We've gone laggy during dispatch, don't do any
    // more housekeeping
    return true;
  }

  // done with all client replayed requests?
  if (is_clientreplay() &&
      mdcache->is_open() &&
      replay_queue.empty() &&
      beacon.get_want_state() == MDSMap::STATE_CLIENTREPLAY) {
    int num_requests = mdcache->get_num_client_requests();
    dout(10) << " still have " << num_requests << " active replay requests" << dendl;
    if (num_requests == 0)
      clientreplay_done();
  }

  // hack: thrash exports
  static utime_t start;
  utime_t now = ceph_clock_now();
  if (start == utime_t())
    start = now;
  /*double el = now - start;
  if (el > 30.0 &&
    el < 60.0)*/
  for (int i=0; i<g_conf->mds_thrash_exports; i++) {
    set<mds_rank_t> s;
    if (!is_active()) break;
    mdsmap->get_mds_set(s, MDSMap::STATE_ACTIVE);
    if (s.size() < 2 || CInode::count() < 10)
      break;  // need peers for this to work.
    if (mdcache->migrator->get_num_exporting() > g_conf->mds_thrash_exports * 5 ||
	mdcache->migrator->get_export_queue_size() > g_conf->mds_thrash_exports * 10)
      break;

    dout(7) << "mds thrashing exports pass " << (i+1) << "/" << g_conf->mds_thrash_exports << dendl;

    // pick a random dir inode
    CInode *in = mdcache->hack_pick_random_inode();

    list<CDir*> ls;
    in->get_dirfrags(ls);
    if (!ls.empty()) {	// must be an open dir.
      list<CDir*>::iterator p = ls.begin();
      int n = rand() % ls.size();
      while (n--)
        ++p;
      CDir *dir = *p;
      if (!dir->get_parent_dir()) continue;    // must be linked.
      if (!dir->is_auth()) continue;           // must be auth.

      mds_rank_t dest;
      do {
        int k = rand() % s.size();
        set<mds_rank_t>::iterator p = s.begin();
        while (k--) ++p;
        dest = *p;
      } while (dest == whoami);
      mdcache->migrator->export_dir_nicely(dir,dest);
    }
  }
  // hack: thrash fragments
  for (int i=0; i<g_conf->mds_thrash_fragments; i++) {
    if (!is_active()) break;
    if (mdcache->get_num_fragmenting_dirs() > 5 * g_conf->mds_thrash_fragments) break;
    dout(7) << "mds thrashing fragments pass " << (i+1) << "/" << g_conf->mds_thrash_fragments << dendl;

    // pick a random dir inode
    CInode *in = mdcache->hack_pick_random_inode();

    list<CDir*> ls;
    in->get_dirfrags(ls);
    if (ls.empty()) continue;                // must be an open dir.
    CDir *dir = ls.front();
    if (!dir->get_parent_dir()) continue;    // must be linked.
    if (!dir->is_auth()) continue;           // must be auth.
    frag_t fg = dir->get_frag();
    if (mdsmap->allows_dirfrags()) {
      if ((fg == frag_t() || (rand() % (1 << fg.bits()) == 0))) {
        mdcache->split_dir(dir, 1);
      } else {
        balancer->queue_merge(dir);
      }
    }
  }

  // hack: force hash root?
  /*
  if (false &&
      mdcache->get_root() &&
      mdcache->get_root()->dir &&
      !(mdcache->get_root()->dir->is_hashed() ||
        mdcache->get_root()->dir->is_hashing())) {
    dout(0) << "hashing root" << dendl;
    mdcache->migrator->hash_dir(mdcache->get_root()->dir);
  }
  */

  update_mlogger();
  return true;
}

void MDSRank::update_mlogger()
{
  if (mlogger) {
    mlogger->set(l_mdm_ino, CInode::count());
    mlogger->set(l_mdm_dir, CDir::count());
    mlogger->set(l_mdm_dn, CDentry::count());
    mlogger->set(l_mdm_cap, Capability::count());
    mlogger->set(l_mdm_inoa, CInode::increments());
    mlogger->set(l_mdm_inos, CInode::decrements());
    mlogger->set(l_mdm_dira, CDir::increments());
    mlogger->set(l_mdm_dirs, CDir::decrements());
    mlogger->set(l_mdm_dna, CDentry::increments());
    mlogger->set(l_mdm_dns, CDentry::decrements());
    mlogger->set(l_mdm_capa, Capability::increments());
    mlogger->set(l_mdm_caps, Capability::decrements());
    mlogger->set(l_mdm_buf, buffer::get_total_alloc());
  }
}

/*
 * lower priority messages we defer if we seem laggy
 */
bool MDSRank::handle_deferrable_message(Message *m)
{
  int port = m->get_type() & 0xff00;

  switch (port) {
  case MDS_PORT_CACHE:
    ALLOW_MESSAGES_FROM(CEPH_ENTITY_TYPE_MDS);
    mdcache->dispatch(m);
    break;

  case MDS_PORT_MIGRATOR:
    ALLOW_MESSAGES_FROM(CEPH_ENTITY_TYPE_MDS);
    mdcache->migrator->dispatch(m);
    break;

  default:
    switch (m->get_type()) {
      // SERVER
    case CEPH_MSG_CLIENT_SESSION:
    case CEPH_MSG_CLIENT_RECONNECT:
      ALLOW_MESSAGES_FROM(CEPH_ENTITY_TYPE_CLIENT);
      // fall-thru
    case CEPH_MSG_CLIENT_REQUEST:
      server->dispatch(m);
      break;
    case MSG_MDS_SLAVE_REQUEST:
      ALLOW_MESSAGES_FROM(CEPH_ENTITY_TYPE_MDS);
      server->dispatch(m);
      break;

    case MSG_MDS_HEARTBEAT:
      ALLOW_MESSAGES_FROM(CEPH_ENTITY_TYPE_MDS);
      balancer->proc_message(m);
      break;

    case MSG_MDS_TABLE_REQUEST:
      ALLOW_MESSAGES_FROM(CEPH_ENTITY_TYPE_MDS);
      {
	MMDSTableRequest *req = static_cast<MMDSTableRequest*>(m);
	if (req->op < 0) {
	  MDSTableClient *client = get_table_client(req->table);
	      client->handle_request(req);
	} else {
	  MDSTableServer *server = get_table_server(req->table);
	  server->handle_request(req);
	}
      }
      break;

    case MSG_MDS_LOCK:
    case MSG_MDS_INODEFILECAPS:
      ALLOW_MESSAGES_FROM(CEPH_ENTITY_TYPE_MDS);
      locker->dispatch(m);
      break;

    case CEPH_MSG_CLIENT_CAPS:
    case CEPH_MSG_CLIENT_CAPRELEASE:
    case CEPH_MSG_CLIENT_LEASE:
      ALLOW_MESSAGES_FROM(CEPH_ENTITY_TYPE_CLIENT);
      locker->dispatch(m);
      break;

    default:
      return false;
    }
  }

  return true;
}

/**
 * Advance finished_queue and waiting_for_nolaggy.
 *
 * Usually drain both queues, but may not drain waiting_for_nolaggy
 * if beacon is currently laggy.
 */
void MDSRank::_advance_queues()
{
  assert(mds_lock.is_locked_by_me());

  while (!finished_queue.empty()) {
    dout(7) << "mds has " << finished_queue.size() << " queued contexts" << dendl;
    dout(10) << finished_queue << dendl;
    list<MDSInternalContextBase*> ls;
    ls.swap(finished_queue);
    while (!ls.empty()) {
      dout(10) << " finish " << ls.front() << dendl;
      ls.front()->complete(0);
      ls.pop_front();

      heartbeat_reset();
    }
  }

  while (!waiting_for_nolaggy.empty()) {
    // stop if we're laggy now!
    if (beacon.is_laggy())
      break;

    Message *old = waiting_for_nolaggy.front();
    waiting_for_nolaggy.pop_front();

    if (is_stale_message(old)) {
      old->put();
    } else {
      dout(7) << " processing laggy deferred " << *old << dendl;
      if (!handle_deferrable_message(old)) {
        dout(0) << "unrecognized message " << *old << dendl;
        old->put();
      }
    }

    heartbeat_reset();
  }
}

/**
 * Call this when you take mds_lock, or periodically if you're going to
 * hold the lock for a long time (e.g. iterating over clients/inodes)
 */
void MDSRank::heartbeat_reset()
{
  // Any thread might jump into mds_lock and call us immediately
  // after a call to suicide() completes, in which case MDSRank::hb
  // has been freed and we are a no-op.
  if (!hb) {
      assert(stopping);
      return;
  }

  // NB not enabling suicide grace, because the mon takes care of killing us
  // (by blacklisting us) when we fail to send beacons, and it's simpler to
  // only have one way of dying.
  g_ceph_context->get_heartbeat_map()->reset_timeout(hb, g_conf->mds_beacon_grace, 0);
}

bool MDSRank::is_stale_message(Message *m) const
{
  // from bad mds?
  if (m->get_source().is_mds()) {
    mds_rank_t from = mds_rank_t(m->get_source().num());
    if (!mdsmap->have_inst(from) ||
	mdsmap->get_inst(from) != m->get_source_inst() ||
	mdsmap->is_down(from)) {
      // bogus mds?
      if (m->get_type() == CEPH_MSG_MDS_MAP) {
	dout(5) << "got " << *m << " from old/bad/imposter mds " << m->get_source()
		<< ", but it's an mdsmap, looking at it" << dendl;
      } else if (m->get_type() == MSG_MDS_CACHEEXPIRE &&
		 mdsmap->get_inst(from) == m->get_source_inst()) {
	dout(5) << "got " << *m << " from down mds " << m->get_source()
		<< ", but it's a cache_expire, looking at it" << dendl;
      } else {
	dout(5) << "got " << *m << " from down/old/bad/imposter mds " << m->get_source()
		<< ", dropping" << dendl;
	return true;
      }
    }
  }
  return false;
}


void MDSRank::send_message(Message *m, Connection *c)
{
  assert(c);
  c->send_message(m);
}


void MDSRank::send_message_mds(Message *m, mds_rank_t mds)
{
  if (!mdsmap->is_up(mds)) {
    dout(10) << "send_message_mds mds." << mds << " not up, dropping " << *m << dendl;
    m->put();
    return;
  }

  // send mdsmap first?
  if (mds != whoami && peer_mdsmap_epoch[mds] < mdsmap->get_epoch()) {
    messenger->send_message(new MMDSMap(monc->get_fsid(), mdsmap),
			    mdsmap->get_inst(mds));
    peer_mdsmap_epoch[mds] = mdsmap->get_epoch();
  }

  // send message
  messenger->send_message(m, mdsmap->get_inst(mds));
}

void MDSRank::forward_message_mds(Message *m, mds_rank_t mds)
{
  assert(mds != whoami);

  // client request?
  if (m->get_type() == CEPH_MSG_CLIENT_REQUEST &&
      (static_cast<MClientRequest*>(m))->get_source().is_client()) {
    MClientRequest *creq = static_cast<MClientRequest*>(m);
    creq->inc_num_fwd();    // inc forward counter

    /*
     * don't actually forward if non-idempotent!
     * client has to do it.  although the MDS will ignore duplicate requests,
     * the affected metadata may migrate, in which case the new authority
     * won't have the metareq_id in the completed request map.
     */
    // NEW: always make the client resend!
    bool client_must_resend = true;  //!creq->can_forward();

    // tell the client where it should go
    messenger->send_message(new MClientRequestForward(creq->get_tid(), mds, creq->get_num_fwd(),
						      client_must_resend),
			    creq->get_source_inst());

    if (client_must_resend) {
      m->put();
      return;
    }
  }

  // these are the only types of messages we should be 'forwarding'; they
  // explicitly encode their source mds, which gets clobbered when we resend
  // them here.
  assert(m->get_type() == MSG_MDS_DIRUPDATE ||
	 m->get_type() == MSG_MDS_EXPORTDIRDISCOVER);

  // send mdsmap first?
  if (peer_mdsmap_epoch[mds] < mdsmap->get_epoch()) {
    messenger->send_message(new MMDSMap(monc->get_fsid(), mdsmap),
			    mdsmap->get_inst(mds));
    peer_mdsmap_epoch[mds] = mdsmap->get_epoch();
  }

  messenger->send_message(m, mdsmap->get_inst(mds));
}



void MDSRank::send_message_client_counted(Message *m, client_t client)
{
  Session *session =  sessionmap.get_session(entity_name_t::CLIENT(client.v));
  if (session) {
    send_message_client_counted(m, session);
  } else {
    dout(10) << "send_message_client_counted no session for client." << client << " " << *m << dendl;
  }
}

void MDSRank::send_message_client_counted(Message *m, Connection *connection)
{
  Session *session = static_cast<Session *>(connection->get_priv());
  if (session) {
    session->put();  // do not carry ref
    send_message_client_counted(m, session);
  } else {
    dout(10) << "send_message_client_counted has no session for " << m->get_source_inst() << dendl;
    // another Connection took over the Session
  }
}

void MDSRank::send_message_client_counted(Message *m, Session *session)
{
  version_t seq = session->inc_push_seq();
  dout(10) << "send_message_client_counted " << session->info.inst.name << " seq "
	   << seq << " " << *m << dendl;
  if (session->connection) {
    session->connection->send_message(m);
  } else {
    session->preopen_out_queue.push_back(m);
  }
}

void MDSRank::send_message_client(Message *m, Session *session)
{
  dout(10) << "send_message_client " << session->info.inst << " " << *m << dendl;
  if (session->connection) {
    session->connection->send_message(m);
  } else {
    session->preopen_out_queue.push_back(m);
  }
}

/**
 * This is used whenever a RADOS operation has been cancelled
 * or a RADOS client has been blacklisted, to cause the MDS and
 * any clients to wait for this OSD epoch before using any new caps.
 *
 * See doc/cephfs/eviction
 */
void MDSRank::set_osd_epoch_barrier(epoch_t e)
{
  dout(4) << __func__ << ": epoch=" << e << dendl;
  osd_epoch_barrier = e;
}

void MDSRank::retry_dispatch(Message *m)
{
  inc_dispatch_depth();
  _dispatch(m, false);
  dec_dispatch_depth();
}

utime_t MDSRank::get_laggy_until() const
{
  return beacon.get_laggy_until();
}

bool MDSRank::is_daemon_stopping() const
{
  return stopping;
}

void MDSRank::request_state(MDSMap::DaemonState s)
{
  dout(3) << "request_state " << ceph_mds_state_name(s) << dendl;
  beacon.set_want_state(mdsmap, s);
  beacon.send();
}


class C_MDS_BootStart : public MDSInternalContext {
  MDSRank::BootStep nextstep;
public:
  C_MDS_BootStart(MDSRank *m, MDSRank::BootStep n)
    : MDSInternalContext(m), nextstep(n) {}
  void finish(int r) override {
    mds->boot_start(nextstep, r);
  }
};


void MDSRank::boot_start(BootStep step, int r)
{
  // Handle errors from previous step
  if (r < 0) {
    if (is_standby_replay() && (r == -EAGAIN)) {
      dout(0) << "boot_start encountered an error EAGAIN"
              << ", respawning since we fell behind journal" << dendl;
      respawn();
    } else if (r == -EINVAL || r == -ENOENT) {
      // Invalid or absent data, indicates damaged on-disk structures
      clog->error() << "Error loading MDS rank " << whoami << ": "
        << cpp_strerror(r);
      damaged();
      assert(r == 0);  // Unreachable, damaged() calls respawn()
    } else {
      // Completely unexpected error, give up and die
      dout(0) << "boot_start encountered an error, failing" << dendl;
      suicide();
      return;
    }
  }

  assert(is_starting() || is_any_replay());

  switch(step) {
    case MDS_BOOT_INITIAL:
      {
        mdcache->init_layouts();

        MDSGatherBuilder gather(g_ceph_context,
            new C_MDS_BootStart(this, MDS_BOOT_OPEN_ROOT));
        dout(2) << "boot_start " << step << ": opening inotable" << dendl;
        inotable->set_rank(whoami);
        inotable->load(gather.new_sub());

        dout(2) << "boot_start " << step << ": opening sessionmap" << dendl;
        sessionmap.set_rank(whoami);
        sessionmap.load(gather.new_sub());

        dout(2) << "boot_start " << step << ": opening mds log" << dendl;
        mdlog->open(gather.new_sub());

        if (mdsmap->get_tableserver() == whoami) {
          dout(2) << "boot_start " << step << ": opening snap table" << dendl;
          snapserver->set_rank(whoami);
          snapserver->load(gather.new_sub());
        }

        gather.activate();
      }
      break;
    case MDS_BOOT_OPEN_ROOT:
      {
        dout(2) << "boot_start " << step << ": loading/discovering base inodes" << dendl;

        MDSGatherBuilder gather(g_ceph_context,
            new C_MDS_BootStart(this, MDS_BOOT_PREPARE_LOG));

        mdcache->open_mydir_inode(gather.new_sub());

        purge_queue.open(new C_IO_Wrapper(this, gather.new_sub()));

        if (is_starting() ||
            whoami == mdsmap->get_root()) {  // load root inode off disk if we are auth
          mdcache->open_root_inode(gather.new_sub());
        } else {
          // replay.  make up fake root inode to start with
          (void)mdcache->create_root_inode();
        }
        gather.activate();
      }
      break;
    case MDS_BOOT_PREPARE_LOG:
      if (is_any_replay()) {
        dout(2) << "boot_start " << step << ": replaying mds log" << dendl;
        mdlog->replay(new C_MDS_BootStart(this, MDS_BOOT_REPLAY_DONE));
      } else {
        dout(2) << "boot_start " << step << ": positioning at end of old mds log" << dendl;
        mdlog->append();
        starting_done();
      }
      break;
    case MDS_BOOT_REPLAY_DONE:
      assert(is_any_replay());

      // Sessiontable and inotable should be in sync after replay, validate
      // that they are consistent.
      validate_sessions();

      replay_done();
      break;
  }
}

void MDSRank::validate_sessions()
{
  assert(mds_lock.is_locked_by_me());
  std::vector<Session*> victims;

  // Identify any sessions which have state inconsistent with other,
  // after they have been loaded from rados during startup.
  // Mitigate bugs like: http://tracker.ceph.com/issues/16842
  const auto &sessions = sessionmap.get_sessions();
  for (const auto &i : sessions) {
    Session *session = i.second;
    interval_set<inodeno_t> badones;
    if (inotable->intersects_free(session->info.prealloc_inos, &badones)) {
      clog->error() << "Client session loaded with invalid preallocated "
                          "inodes, evicting session " << *session;

      // Make the session consistent with inotable so that it can
      // be cleanly torn down
      session->info.prealloc_inos.subtract(badones);

      victims.push_back(session);
    }
  }

  for (const auto &session: victims) {
    server->kill_session(session, nullptr);
  }
}

void MDSRank::starting_done()
{
  dout(3) << "starting_done" << dendl;
  assert(is_starting());
  request_state(MDSMap::STATE_ACTIVE);

  mdcache->open_root();

  if (mdcache->is_open()) {
    mdlog->start_new_segment();
  } else {
    mdcache->wait_for_open(new MDSInternalContextWrapper(this,
			   new FunctionContext([this] (int r) {
			       mdlog->start_new_segment();
			   })));
  }
}


void MDSRank::calc_recovery_set()
{
  // initialize gather sets
  set<mds_rank_t> rs;
  mdsmap->get_recovery_mds_set(rs);
  rs.erase(whoami);
  mdcache->set_recovery_set(rs);

  dout(1) << " recovery set is " << rs << dendl;
}


void MDSRank::replay_start()
{
  dout(1) << "replay_start" << dendl;

  if (is_standby_replay())
    standby_replaying = true;

  calc_recovery_set();

  // Check if we need to wait for a newer OSD map before starting
  Context *fin = new C_IO_Wrapper(this, new C_MDS_BootStart(this, MDS_BOOT_INITIAL));
  bool const ready = objecter->wait_for_map(
      mdsmap->get_last_failure_osd_epoch(),
      fin);

  if (ready) {
    delete fin;
    boot_start();
  } else {
    dout(1) << " waiting for osdmap " << mdsmap->get_last_failure_osd_epoch()
	    << " (which blacklists prior instance)" << dendl;
  }
}


class MDSRank::C_MDS_StandbyReplayRestartFinish : public MDSIOContext {
  uint64_t old_read_pos;
public:
  C_MDS_StandbyReplayRestartFinish(MDSRank *mds_, uint64_t old_read_pos_) :
    MDSIOContext(mds_), old_read_pos(old_read_pos_) {}
  void finish(int r) override {
    mds->_standby_replay_restart_finish(r, old_read_pos);
  }
};

void MDSRank::_standby_replay_restart_finish(int r, uint64_t old_read_pos)
{
  if (old_read_pos < mdlog->get_journaler()->get_trimmed_pos()) {
    dout(0) << "standby MDS fell behind active MDS journal's expire_pos, restarting" << dendl;
    respawn(); /* we're too far back, and this is easier than
		  trying to reset everything in the cache, etc */
  } else {
    mdlog->standby_trim_segments();
    boot_start(MDS_BOOT_PREPARE_LOG, r);
  }
}

inline void MDSRank::standby_replay_restart()
{
  if (standby_replaying) {
    /* Go around for another pass of replaying in standby */
    dout(4) << "standby_replay_restart (as standby)" << dendl;
    mdlog->get_journaler()->reread_head_and_probe(
      new C_MDS_StandbyReplayRestartFinish(
        this,
	mdlog->get_journaler()->get_read_pos()));
  } else {
    /* We are transitioning out of standby: wait for OSD map update
       before making final pass */
    dout(1) << "standby_replay_restart (final takeover pass)" << dendl;
    Context *fin = new C_IO_Wrapper(this, new C_MDS_BootStart(this, MDS_BOOT_PREPARE_LOG));
    bool const ready =
      objecter->wait_for_map(mdsmap->get_last_failure_osd_epoch(), fin);
    if (ready) {
      delete fin;
      mdlog->get_journaler()->reread_head_and_probe(
        new C_MDS_StandbyReplayRestartFinish(
          this,
	  mdlog->get_journaler()->get_read_pos()));
    } else {
      dout(1) << " waiting for osdmap " << mdsmap->get_last_failure_osd_epoch()
              << " (which blacklists prior instance)" << dendl;
    }
  }
}

class MDSRank::C_MDS_StandbyReplayRestart : public MDSInternalContext {
public:
  explicit C_MDS_StandbyReplayRestart(MDSRank *m) : MDSInternalContext(m) {}
  void finish(int r) override {
    assert(!r);
    mds->standby_replay_restart();
  }
};

void MDSRank::replay_done()
{
  dout(1) << "replay_done" << (standby_replaying ? " (as standby)" : "") << dendl;

  if (is_standby_replay()) {
    // The replay was done in standby state, and we are still in that state
    assert(standby_replaying);
    dout(10) << "setting replay timer" << dendl;
    timer.add_event_after(g_conf->mds_replay_interval,
                          new C_MDS_StandbyReplayRestart(this));
    return;
  } else if (standby_replaying) {
    // The replay was done in standby state, we have now _left_ that state
    dout(10) << " last replay pass was as a standby; making final pass" << dendl;
    standby_replaying = false;
    standby_replay_restart();
    return;
  } else {
    // Replay is complete, journal read should be up to date
    assert(mdlog->get_journaler()->get_read_pos() == mdlog->get_journaler()->get_write_pos());
    assert(!is_standby_replay());

    // Reformat and come back here
    if (mdlog->get_journaler()->get_stream_format() < g_conf->mds_journal_format) {
        dout(4) << "reformatting journal on standbyreplay->replay transition" << dendl;
        mdlog->reopen(new C_MDS_BootStart(this, MDS_BOOT_REPLAY_DONE));
        return;
    }
  }

  dout(1) << "making mds journal writeable" << dendl;
  mdlog->get_journaler()->set_writeable();
  mdlog->get_journaler()->trim_tail();

  if (g_conf->mds_wipe_sessions) {
    dout(1) << "wiping out client sessions" << dendl;
    sessionmap.wipe();
    sessionmap.save(new C_MDSInternalNoop);
  }
  if (g_conf->mds_wipe_ino_prealloc) {
    dout(1) << "wiping out ino prealloc from sessions" << dendl;
    sessionmap.wipe_ino_prealloc();
    sessionmap.save(new C_MDSInternalNoop);
  }
  if (g_conf->mds_skip_ino) {
    inodeno_t i = g_conf->mds_skip_ino;
    dout(1) << "skipping " << i << " inodes" << dendl;
    inotable->skip_inos(i);
    inotable->save(new C_MDSInternalNoop);
  }

  if (mdsmap->get_num_in_mds() == 1 &&
      mdsmap->get_num_failed_mds() == 0) { // just me!
    dout(2) << "i am alone, moving to state reconnect" << dendl;
    request_state(MDSMap::STATE_RECONNECT);
  } else {
    dout(2) << "i am not alone, moving to state resolve" << dendl;
    request_state(MDSMap::STATE_RESOLVE);
  }
}

void MDSRank::reopen_log()
{
  dout(1) << "reopen_log" << dendl;
  mdcache->rollback_uncommitted_fragments();
}


void MDSRank::resolve_start()
{
  dout(1) << "resolve_start" << dendl;

  reopen_log();

  mdcache->resolve_start(new C_MDS_VoidFn(this, &MDSRank::resolve_done));
  finish_contexts(g_ceph_context, waiting_for_resolve);
}
void MDSRank::resolve_done()
{
  dout(1) << "resolve_done" << dendl;
  request_state(MDSMap::STATE_RECONNECT);
}

void MDSRank::reconnect_start()
{
  dout(1) << "reconnect_start" << dendl;

  if (last_state == MDSMap::STATE_REPLAY) {
    reopen_log();
  }

  // Drop any blacklisted clients from the SessionMap before going
  // into reconnect, so that we don't wait for them.
  objecter->enable_blacklist_events();
  std::set<entity_addr_t> blacklist;
  epoch_t epoch = 0;
  objecter->with_osdmap([this, &blacklist, &epoch](const OSDMap& o) {
      o.get_blacklist(&blacklist);
      epoch = o.get_epoch();
  });
  auto killed = server->apply_blacklist(blacklist);
  dout(4) << "reconnect_start: killed " << killed << " blacklisted sessions ("
          << blacklist.size() << " blacklist entries, "
          << sessionmap.get_sessions().size() << ")" << dendl;
  if (killed) {
    set_osd_epoch_barrier(epoch);
  }

  server->reconnect_clients(new C_MDS_VoidFn(this, &MDSRank::reconnect_done));
  finish_contexts(g_ceph_context, waiting_for_reconnect);
}
void MDSRank::reconnect_done()
{
  dout(1) << "reconnect_done" << dendl;
  request_state(MDSMap::STATE_REJOIN);    // move to rejoin state
}

void MDSRank::rejoin_joint_start()
{
  dout(1) << "rejoin_joint_start" << dendl;
  mdcache->rejoin_send_rejoins();
}
void MDSRank::rejoin_start()
{
  dout(1) << "rejoin_start" << dendl;
  mdcache->rejoin_start(new C_MDS_VoidFn(this, &MDSRank::rejoin_done));
}
void MDSRank::rejoin_done()
{
  dout(1) << "rejoin_done" << dendl;
  mdcache->show_subtrees();
  mdcache->show_cache();

  // funny case: is our cache empty?  no subtrees?
  if (!mdcache->is_subtrees()) {
    if (whoami == 0) {
      // The root should always have a subtree!
      clog->error() << "No subtrees found for root MDS rank!";
      damaged();
      assert(mdcache->is_subtrees());
    } else {
      dout(1) << " empty cache, no subtrees, leaving cluster" << dendl;
      request_state(MDSMap::STATE_STOPPED);
    }
    return;
  }

  if (replay_queue.empty())
    request_state(MDSMap::STATE_ACTIVE);
  else
    request_state(MDSMap::STATE_CLIENTREPLAY);
}

void MDSRank::clientreplay_start()
{
  dout(1) << "clientreplay_start" << dendl;
  finish_contexts(g_ceph_context, waiting_for_replay);  // kick waiters
  mdcache->start_files_to_recover();
  queue_one_replay();
}

bool MDSRank::queue_one_replay()
{
  if (replay_queue.empty()) {
    mdlog->wait_for_safe(new C_MDS_VoidFn(this, &MDSRank::clientreplay_done));
    return false;
  }
  queue_waiter(replay_queue.front());
  replay_queue.pop_front();
  return true;
}

void MDSRank::clientreplay_done()
{
  dout(1) << "clientreplay_done" << dendl;
  request_state(MDSMap::STATE_ACTIVE);
}

void MDSRank::active_start()
{
  dout(1) << "active_start" << dendl;

  if (last_state == MDSMap::STATE_CREATING) {
    mdcache->open_root();
  }

  mdcache->clean_open_file_lists();
  mdcache->export_remaining_imported_caps();
  finish_contexts(g_ceph_context, waiting_for_replay);  // kick waiters
  mdcache->start_files_to_recover();

  mdcache->reissue_all_caps();
  mdcache->activate_stray_manager();

  finish_contexts(g_ceph_context, waiting_for_active);  // kick waiters
}

void MDSRank::recovery_done(int oldstate)
{
  dout(1) << "recovery_done -- successful recovery!" << dendl;
  assert(is_clientreplay() || is_active());

  // kick snaptable (resent AGREEs)
  if (mdsmap->get_tableserver() == whoami) {
    set<mds_rank_t> active;
    mdsmap->get_clientreplay_or_active_or_stopping_mds_set(active);
    snapserver->finish_recovery(active);
  }

  if (oldstate == MDSMap::STATE_CREATING)
    return;

  mdcache->start_recovered_truncates();
  mdcache->do_file_recover();

  // tell connected clients
  //bcast_mds_map();     // not anymore, they get this from the monitor

  mdcache->populate_mydir();
}

void MDSRank::creating_done()
{
  dout(1)<< "creating_done" << dendl;
  request_state(MDSMap::STATE_ACTIVE);
}

void MDSRank::boot_create()
{
  dout(3) << "boot_create" << dendl;

  MDSGatherBuilder fin(g_ceph_context, new C_MDS_VoidFn(this, &MDSRank::creating_done));

  mdcache->init_layouts();

  snapserver->set_rank(whoami);
  inotable->set_rank(whoami);
  sessionmap.set_rank(whoami);

  // start with a fresh journal
  dout(10) << "boot_create creating fresh journal" << dendl;
  mdlog->create(fin.new_sub());

  // open new journal segment, but do not journal subtree map (yet)
  mdlog->prepare_new_segment();

  if (whoami == mdsmap->get_root()) {
    dout(3) << "boot_create creating fresh hierarchy" << dendl;
    mdcache->create_empty_hierarchy(fin.get());
  }

  dout(3) << "boot_create creating mydir hierarchy" << dendl;
  mdcache->create_mydir_hierarchy(fin.get());

  // fixme: fake out inotable (reset, pretend loaded)
  dout(10) << "boot_create creating fresh inotable table" << dendl;
  inotable->reset();
  inotable->save(fin.new_sub());

  // write empty sessionmap
  sessionmap.save(fin.new_sub());

  // Create empty purge queue
  purge_queue.create(new C_IO_Wrapper(this, fin.new_sub()));

  // initialize tables
  if (mdsmap->get_tableserver() == whoami) {
    dout(10) << "boot_create creating fresh snaptable" << dendl;
    snapserver->reset();
    snapserver->save(fin.new_sub());
  }

  assert(g_conf->mds_kill_create_at != 1);

  // ok now journal it
  mdlog->journal_segment_subtree_map(fin.new_sub());
  mdlog->flush();

  // Usually we do this during reconnect, but creation skips that.
  objecter->enable_blacklist_events();

  fin.activate();
}

void MDSRank::stopping_start()
{
  dout(2) << "stopping_start" << dendl;

  if (mdsmap->get_num_in_mds() == 1 && !sessionmap.empty()) {
    // we're the only mds up!
    dout(0) << "we are the last MDS, and have mounted clients: we cannot flush our journal.  suicide!" << dendl;
    suicide();
  }

  mdcache->shutdown_start();
}

void MDSRank::stopping_done()
{
  dout(2) << "stopping_done" << dendl;

  // tell monitor we shut down cleanly.
  request_state(MDSMap::STATE_STOPPED);
}

void MDSRankDispatcher::handle_mds_map(
    MMDSMap *m,
    MDSMap *oldmap)
{
  // I am only to be passed MDSMaps in which I hold a rank
  assert(whoami != MDS_RANK_NONE);

  MDSMap::DaemonState oldstate = state;
  mds_gid_t mds_gid = mds_gid_t(monc->get_global_id());
  state = mdsmap->get_state_gid(mds_gid);
  if (state != oldstate) {
    last_state = oldstate;
    incarnation = mdsmap->get_inc_gid(mds_gid);
  }

  version_t epoch = m->get_epoch();

  // note source's map version
  if (m->get_source().is_mds() &&
      peer_mdsmap_epoch[mds_rank_t(m->get_source().num())] < epoch) {
    dout(15) << " peer " << m->get_source()
	     << " has mdsmap epoch >= " << epoch
	     << dendl;
    peer_mdsmap_epoch[mds_rank_t(m->get_source().num())] = epoch;
  }

  // Validate state transitions while I hold a rank
  if (!MDSMap::state_transition_valid(oldstate, state)) {
    derr << "Invalid state transition " << ceph_mds_state_name(oldstate)
      << "->" << ceph_mds_state_name(state) << dendl;
    respawn();
  }

  if (oldstate != state) {
    // update messenger.
    if (state == MDSMap::STATE_STANDBY_REPLAY) {
      dout(1) << "handle_mds_map i am now mds." << mds_gid << "." << incarnation
	      << " replaying mds." << whoami << "." << incarnation << dendl;
      messenger->set_myname(entity_name_t::MDS(mds_gid));
    } else {
      dout(1) << "handle_mds_map i am now mds." << whoami << "." << incarnation << dendl;
      messenger->set_myname(entity_name_t::MDS(whoami));
    }
  }

  // tell objecter my incarnation
  if (objecter->get_client_incarnation() != incarnation)
    objecter->set_client_incarnation(incarnation);

  // for debug
  if (g_conf->mds_dump_cache_on_map)
    mdcache->dump_cache();

  // did it change?
  if (oldstate != state) {
    dout(1) << "handle_mds_map state change "
	    << ceph_mds_state_name(oldstate) << " --> "
	    << ceph_mds_state_name(state) << dendl;
    beacon.set_want_state(mdsmap, state);

    if (oldstate == MDSMap::STATE_STANDBY_REPLAY) {
        dout(10) << "Monitor activated us! Deactivating replay loop" << dendl;
        assert (state == MDSMap::STATE_REPLAY);
    } else {
      // did i just recover?
      if ((is_active() || is_clientreplay()) &&
          (oldstate == MDSMap::STATE_CREATING ||
	   oldstate == MDSMap::STATE_REJOIN ||
	   oldstate == MDSMap::STATE_RECONNECT))
        recovery_done(oldstate);

      if (is_active()) {
        active_start();
      } else if (is_any_replay()) {
        replay_start();
      } else if (is_resolve()) {
        resolve_start();
      } else if (is_reconnect()) {
        reconnect_start();
      } else if (is_rejoin()) {
	rejoin_start();
      } else if (is_clientreplay()) {
        clientreplay_start();
      } else if (is_creating()) {
        boot_create();
      } else if (is_starting()) {
        boot_start();
      } else if (is_stopping()) {
        assert(oldstate == MDSMap::STATE_ACTIVE);
        stopping_start();
      }
    }
  }

  // RESOLVE
  // is someone else newly resolving?
  if (is_resolve() || is_reconnect() || is_rejoin() ||
      is_clientreplay() || is_active() || is_stopping()) {
    if (!oldmap->is_resolving() && mdsmap->is_resolving()) {
      set<mds_rank_t> resolve;
      mdsmap->get_mds_set(resolve, MDSMap::STATE_RESOLVE);
      dout(10) << " resolve set is " << resolve << dendl;
      calc_recovery_set();
      mdcache->send_resolves();
    }
  }

  // REJOIN
  // is everybody finally rejoining?
  if (is_starting() || is_rejoin() || is_clientreplay() || is_active() || is_stopping()) {
    // did we start?
    if (!oldmap->is_rejoining() && mdsmap->is_rejoining())
      rejoin_joint_start();

    // did we finish?
    if (g_conf->mds_dump_cache_after_rejoin &&
	oldmap->is_rejoining() && !mdsmap->is_rejoining())
      mdcache->dump_cache();      // for DEBUG only

    if (oldstate >= MDSMap::STATE_REJOIN ||
	oldstate == MDSMap::STATE_STARTING) {
      // ACTIVE|CLIENTREPLAY|REJOIN => we can discover from them.
      set<mds_rank_t> olddis, dis;
      oldmap->get_mds_set(olddis, MDSMap::STATE_ACTIVE);
      oldmap->get_mds_set(olddis, MDSMap::STATE_CLIENTREPLAY);
      oldmap->get_mds_set(olddis, MDSMap::STATE_REJOIN);
      mdsmap->get_mds_set(dis, MDSMap::STATE_ACTIVE);
      mdsmap->get_mds_set(dis, MDSMap::STATE_CLIENTREPLAY);
      mdsmap->get_mds_set(dis, MDSMap::STATE_REJOIN);
      for (set<mds_rank_t>::iterator p = dis.begin(); p != dis.end(); ++p)
	if (*p != whoami &&            // not me
	    olddis.count(*p) == 0) {  // newly so?
	  mdcache->kick_discovers(*p);
	  mdcache->kick_open_ino_peers(*p);
	}
    }
  }

  cluster_degraded = mdsmap->is_degraded();
  if (oldmap->is_degraded() && !cluster_degraded && state >= MDSMap::STATE_ACTIVE) {
    dout(1) << "cluster recovered." << dendl;
    auto it = waiting_for_active_peer.find(MDS_RANK_NONE);
    if (it != waiting_for_active_peer.end()) {
      queue_waiters(it->second);
      waiting_for_active_peer.erase(it);
    }
  }

  // did someone go active?
  if (oldstate >= MDSMap::STATE_CLIENTREPLAY &&
      (is_clientreplay() || is_active() || is_stopping())) {
    set<mds_rank_t> oldactive, active;
    oldmap->get_mds_set(oldactive, MDSMap::STATE_ACTIVE);
    oldmap->get_mds_set(oldactive, MDSMap::STATE_CLIENTREPLAY);
    mdsmap->get_mds_set(active, MDSMap::STATE_ACTIVE);
    mdsmap->get_mds_set(active, MDSMap::STATE_CLIENTREPLAY);
    for (set<mds_rank_t>::iterator p = active.begin(); p != active.end(); ++p)
      if (*p != whoami &&            // not me
	  oldactive.count(*p) == 0)  // newly so?
	handle_mds_recovery(*p);
  }

  // did someone fail?
  //   new down?
  {
    set<mds_rank_t> olddown, down;
    oldmap->get_down_mds_set(&olddown);
    mdsmap->get_down_mds_set(&down);
    for (set<mds_rank_t>::iterator p = down.begin(); p != down.end(); ++p) {
      if (oldmap->have_inst(*p) && olddown.count(*p) == 0) {
        messenger->mark_down(oldmap->get_inst(*p).addr);
        handle_mds_failure(*p);
      }
    }
  }

  // did someone fail?
  //   did their addr/inst change?
  {
    set<mds_rank_t> up;
    mdsmap->get_up_mds_set(up);
    for (set<mds_rank_t>::iterator p = up.begin(); p != up.end(); ++p) {
      if (oldmap->have_inst(*p) &&
         oldmap->get_inst(*p) != mdsmap->get_inst(*p)) {
        messenger->mark_down(oldmap->get_inst(*p).addr);
        handle_mds_failure(*p);
      }
    }
  }

  if (is_clientreplay() || is_active() || is_stopping()) {
    // did anyone stop?
    set<mds_rank_t> oldstopped, stopped;
    oldmap->get_stopped_mds_set(oldstopped);
    mdsmap->get_stopped_mds_set(stopped);
    for (set<mds_rank_t>::iterator p = stopped.begin(); p != stopped.end(); ++p)
      if (oldstopped.count(*p) == 0)      // newly so?
	mdcache->migrator->handle_mds_failure_or_stop(*p);
  }

  {
    map<epoch_t,list<MDSInternalContextBase*> >::iterator p = waiting_for_mdsmap.begin();
    while (p != waiting_for_mdsmap.end() && p->first <= mdsmap->get_epoch()) {
      list<MDSInternalContextBase*> ls;
      ls.swap(p->second);
      waiting_for_mdsmap.erase(p++);
      finish_contexts(g_ceph_context, ls);
    }
  }

  if (is_active()) {
    // Before going active, set OSD epoch barrier to latest (so that
    // we don't risk handing out caps to clients with old OSD maps that
    // might not include barriers from the previous incarnation of this MDS)
    set_osd_epoch_barrier(objecter->with_osdmap(
			    std::mem_fn(&OSDMap::get_epoch)));
  }

  if (is_active()) {
    bool found = false;
    MDSMap::mds_info_t info = mdsmap->get_info(whoami);

    for (map<mds_gid_t,MDSMap::mds_info_t>::const_iterator p = mdsmap->get_mds_info().begin();
       p != mdsmap->get_mds_info().end();
       ++p) {
      if (p->second.state == MDSMap::STATE_STANDBY_REPLAY &&
	  (p->second.standby_for_rank == whoami ||(info.name.length() && p->second.standby_for_name == info.name))) {
	found = true;
	break;
      }
      if (found)
	mdlog->set_write_iohint(0);
      else
	mdlog->set_write_iohint(CEPH_OSD_OP_FLAG_FADVISE_DONTNEED);
    }
  }

  if (oldmap->get_max_mds() != mdsmap->get_max_mds()) {
    purge_queue.update_op_limit(*mdsmap);
  }
}

void MDSRank::handle_mds_recovery(mds_rank_t who)
{
  dout(5) << "handle_mds_recovery mds." << who << dendl;

  mdcache->handle_mds_recovery(who);

  if (mdsmap->get_tableserver() == whoami) {
    snapserver->handle_mds_recovery(who);
  }

  queue_waiters(waiting_for_active_peer[who]);
  waiting_for_active_peer.erase(who);
}

void MDSRank::handle_mds_failure(mds_rank_t who)
{
  if (who == whoami) {
    dout(5) << "handle_mds_failure for myself; not doing anything" << dendl;
    return;
  }
  dout(5) << "handle_mds_failure mds." << who << dendl;

  mdcache->handle_mds_failure(who);

  snapclient->handle_mds_failure(who);
}

bool MDSRankDispatcher::handle_asok_command(
    std::string command, cmdmap_t& cmdmap, Formatter *f,
		    std::ostream& ss)
{
  if (command == "dump_ops_in_flight" ||
             command == "ops") {
    if (!op_tracker.dump_ops_in_flight(f)) {
      ss << "op_tracker tracking is not enabled now, so no ops are tracked currently, even those get stuck. \
	  please enable \"osd_enable_op_tracker\", and the tracker will start to track new ops received afterwards.";
    }
  } else if (command == "dump_blocked_ops") {
    if (!op_tracker.dump_ops_in_flight(f, true)) {
      ss << "op_tracker tracking is not enabled now, so no ops are tracked currently, even those get stuck. \
	Please enable \"osd_enable_op_tracker\", and the tracker will start to track new ops received afterwards.";
    }
  } else if (command == "dump_historic_ops") {
    if (!op_tracker.dump_historic_ops(f)) {
      ss << "op_tracker tracking is not enabled now, so no ops are tracked currently, even those get stuck. \
	  please enable \"osd_enable_op_tracker\", and the tracker will start to track new ops received afterwards.";
    }
  } else if (command == "dump_historic_ops_by_duration") {
    if (!op_tracker.dump_historic_ops(f, true)) {
      ss << "op_tracker tracking is not enabled now, so no ops are tracked currently, even those get stuck. \
	  please enable \"osd_enable_op_tracker\", and the tracker will start to track new ops received afterwards.";
    }
  } else if (command == "osdmap barrier") {
    int64_t target_epoch = 0;
    bool got_val = cmd_getval(g_ceph_context, cmdmap, "target_epoch", target_epoch);

    if (!got_val) {
      ss << "no target epoch given";
      return true;
    }

    mds_lock.Lock();
    set_osd_epoch_barrier(target_epoch);
    mds_lock.Unlock();

    C_SaferCond cond;
    bool already_got = objecter->wait_for_map(target_epoch, &cond);
    if (!already_got) {
      dout(4) << __func__ << ": waiting for OSD epoch " << target_epoch << dendl;
      cond.wait();
    }
  } else if (command == "session ls") {
    Mutex::Locker l(mds_lock);

    heartbeat_reset();

    dump_sessions(SessionFilter(), f);
  } else if (command == "session evict") {
    std::string client_id;
    const bool got_arg = cmd_getval(g_ceph_context, cmdmap, "client_id", client_id);
    if(!got_arg) {
      ss << "Invalid client_id specified";
      return true;
    }

    mds_lock.Lock();
    std::stringstream dss;
    bool evicted = evict_client(strtol(client_id.c_str(), 0, 10), true,
        g_conf->mds_session_blacklist_on_evict, dss);
    if (!evicted) {
      dout(15) << dss.str() << dendl;
      ss << dss.str();
    }
    mds_lock.Unlock();
  } else if (command == "scrub_path") {
    string path;
    vector<string> scrubop_vec;
    cmd_getval(g_ceph_context, cmdmap, "scrubops", scrubop_vec);
    cmd_getval(g_ceph_context, cmdmap, "path", path);
    command_scrub_path(f, path, scrubop_vec);
  } else if (command == "tag path") {
    string path;
    cmd_getval(g_ceph_context, cmdmap, "path", path);
    string tag;
    cmd_getval(g_ceph_context, cmdmap, "tag", tag);
    command_tag_path(f, path, tag);
  } else if (command == "flush_path") {
    string path;
    cmd_getval(g_ceph_context, cmdmap, "path", path);
    command_flush_path(f, path);
  } else if (command == "flush journal") {
    command_flush_journal(f);
  } else if (command == "get subtrees") {
    command_get_subtrees(f);
  } else if (command == "export dir") {
    string path;
    if(!cmd_getval(g_ceph_context, cmdmap, "path", path)) {
      ss << "malformed path";
      return true;
    }
    int64_t rank;
    if(!cmd_getval(g_ceph_context, cmdmap, "rank", rank)) {
      ss << "malformed rank";
      return true;
    }
    command_export_dir(f, path, (mds_rank_t)rank);
  } else if (command == "dump cache") {
    Mutex::Locker l(mds_lock);
    string path;
    int r;
    if(!cmd_getval(g_ceph_context, cmdmap, "path", path)) {
      r = mdcache->dump_cache(f);
    } else {
      r = mdcache->dump_cache(path);
    }

    if (r != 0) {
      ss << "Failed to dump cache: " << cpp_strerror(r);
      f->reset();
    }
  } else if (command == "cache status") {
    Mutex::Locker l(mds_lock);
    int r = mdcache->cache_status(f);
    if (r != 0) {
      ss << "Failed to get cache status: " << cpp_strerror(r);
    }
  } else if (command == "dump tree") {
    string root;
    int64_t depth;
    cmd_getval(g_ceph_context, cmdmap, "root", root);
    if (!cmd_getval(g_ceph_context, cmdmap, "depth", depth))
      depth = -1;
    {
      Mutex::Locker l(mds_lock);
      int r = mdcache->dump_cache(root, depth, f);
      if (r != 0) {
        ss << "Failed to dump tree: " << cpp_strerror(r);
        f->reset();
      }
    }
  } else if (command == "force_readonly") {
    Mutex::Locker l(mds_lock);
    mdcache->force_readonly();
  } else if (command == "dirfrag split") {
    command_dirfrag_split(cmdmap, ss);
  } else if (command == "dirfrag merge") {
    command_dirfrag_merge(cmdmap, ss);
  } else if (command == "dirfrag ls") {
    command_dirfrag_ls(cmdmap, ss, f);
  } else {
    return false;
  }

  return true;
}

class C_MDS_Send_Command_Reply : public MDSInternalContext
{
protected:
  MCommand *m;
public:
  C_MDS_Send_Command_Reply(MDSRank *_mds, MCommand *_m) :
    MDSInternalContext(_mds), m(_m) { m->get(); }
  void send (int r, const std::string& out_str) {
    bufferlist bl;
    MDSDaemon::send_command_reply(m, mds, r, bl, out_str);
    m->put();
  }
  void finish (int r) override {
    send(r, "");
  }
};

/**
 * This function drops the mds_lock, so don't do anything with
 * MDSRank after calling it (we could have gone into shutdown): just
 * send your result back to the calling client and finish.
 */
void MDSRankDispatcher::evict_clients(const SessionFilter &filter, MCommand *m)
{
  C_MDS_Send_Command_Reply *reply = new C_MDS_Send_Command_Reply(this, m);

  if (is_any_replay()) {
    reply->send(-EAGAIN, "MDS is replaying log");
    delete reply;
    return;
  }

  std::list<Session*> victims;
  const auto sessions = sessionmap.get_sessions();
  for (const auto p : sessions)  {
    if (!p.first.is_client()) {
      continue;
    }

    Session *s = p.second;

    if (filter.match(*s, std::bind(&Server::waiting_for_reconnect, server, std::placeholders::_1))) {
      victims.push_back(s);
    }
  }

  dout(20) << __func__ << " matched " << victims.size() << " sessions" << dendl;

  if (victims.empty()) {
    reply->send(0, "");
    delete reply;
    return;
  }

  C_GatherBuilder gather(g_ceph_context, reply);
  for (const auto s : victims) {
    std::stringstream ss;
    evict_client(s->info.inst.name.num(), false,
                 g_conf->mds_session_blacklist_on_evict, ss, gather.new_sub());
  }
  gather.activate();
}

void MDSRankDispatcher::dump_sessions(const SessionFilter &filter, Formatter *f) const
{
  // Dump sessions, decorated with recovery/replay status
  f->open_array_section("sessions");
  const ceph::unordered_map<entity_name_t, Session*> session_map = sessionmap.get_sessions();
  for (ceph::unordered_map<entity_name_t,Session*>::const_iterator p = session_map.begin();
       p != session_map.end();
       ++p)  {
    if (!p->first.is_client()) {
      continue;
    }

    Session *s = p->second;

    if (!filter.match(*s, std::bind(&Server::waiting_for_reconnect, server, std::placeholders::_1))) {
      continue;
    }

    f->open_object_section("session");
    f->dump_int("id", p->first.num());

    f->dump_int("num_leases", s->leases.size());
    f->dump_int("num_caps", s->caps.size());

    f->dump_string("state", s->get_state_name());
    f->dump_int("replay_requests", is_clientreplay() ? s->get_request_count() : 0);
    f->dump_unsigned("completed_requests", s->get_num_completed_requests());
    f->dump_bool("reconnecting", server->waiting_for_reconnect(p->first.num()));
    f->dump_stream("inst") << s->info.inst;
    f->open_object_section("client_metadata");
    for (map<string, string>::const_iterator i = s->info.client_metadata.begin();
         i != s->info.client_metadata.end(); ++i) {
      f->dump_string(i->first.c_str(), i->second);
    }
    f->close_section(); // client_metadata
    f->close_section(); //session
  }
  f->close_section(); //sessions
}

void MDSRank::command_scrub_path(Formatter *f, const string& path, vector<string>& scrubop_vec)
{
  bool force = false;
  bool recursive = false;
  bool repair = false;
  for (vector<string>::iterator i = scrubop_vec.begin() ; i != scrubop_vec.end(); ++i) {
    if (*i == "force")
      force = true;
    else if (*i == "recursive")
      recursive = true;
    else if (*i == "repair")
      repair = true;
  }
  C_SaferCond scond;
  {
    Mutex::Locker l(mds_lock);
    mdcache->enqueue_scrub(path, "", force, recursive, repair, f, &scond);
  }
  scond.wait();
  // scrub_dentry() finishers will dump the data for us; we're done!
}

void MDSRank::command_tag_path(Formatter *f,
    const string& path, const std::string &tag)
{
  C_SaferCond scond;
  {
    Mutex::Locker l(mds_lock);
    mdcache->enqueue_scrub(path, tag, true, true, false, f, &scond);
  }
  scond.wait();
}

void MDSRank::command_flush_path(Formatter *f, const string& path)
{
  C_SaferCond scond;
  {
    Mutex::Locker l(mds_lock);
    mdcache->flush_dentry(path, &scond);
  }
  int r = scond.wait();
  f->open_object_section("results");
  f->dump_int("return_code", r);
  f->close_section(); // results
}

/**
 * Wrapper around _command_flush_journal that
 * handles serialization of result
 */
void MDSRank::command_flush_journal(Formatter *f)
{
  assert(f != NULL);

  std::stringstream ss;
  const int r = _command_flush_journal(&ss);
  f->open_object_section("result");
  f->dump_string("message", ss.str());
  f->dump_int("return_code", r);
  f->close_section();
}

/**
 * Implementation of "flush journal" asok command.
 *
 * @param ss
 * Optionally populate with a human readable string describing the
 * reason for any unexpected return status.
 */
int MDSRank::_command_flush_journal(std::stringstream *ss)
{
  assert(ss != NULL);

  Mutex::Locker l(mds_lock);

  if (mdcache->is_readonly()) {
    dout(5) << __func__ << ": read-only FS" << dendl;
    return -EROFS;
  }

  if (!is_active()) {
    dout(5) << __func__ << ": MDS not active, no-op" << dendl;
    return 0;
  }

  // I need to seal off the current segment, and then mark all previous segments
  // for expiry
  mdlog->start_new_segment();
  int r = 0;

  // Flush initially so that all the segments older than our new one
  // will be elegible for expiry
  {
    C_SaferCond mdlog_flushed;
    mdlog->flush();
    mdlog->wait_for_safe(new MDSInternalContextWrapper(this, &mdlog_flushed));
    mds_lock.Unlock();
    r = mdlog_flushed.wait();
    mds_lock.Lock();
    if (r != 0) {
      *ss << "Error " << r << " (" << cpp_strerror(r) << ") while flushing journal";
      return r;
    }
  }

  // Because we may not be the last wait_for_safe context on MDLog, and
  // subsequent contexts might wake up in the middle of our later trim_all
  // and interfere with expiry (by e.g. marking dirs/dentries dirty
  // on previous log segments), we run a second wait_for_safe here.
  // See #10368
  {
    C_SaferCond mdlog_cleared;
    mdlog->wait_for_safe(new MDSInternalContextWrapper(this, &mdlog_cleared));
    mds_lock.Unlock();
    r = mdlog_cleared.wait();
    mds_lock.Lock();
    if (r != 0) {
      *ss << "Error " << r << " (" << cpp_strerror(r) << ") while flushing journal";
      return r;
    }
  }

  // Put all the old log segments into expiring or expired state
  dout(5) << __func__ << ": beginning segment expiry" << dendl;
  r = mdlog->trim_all();
  if (r != 0) {
    *ss << "Error " << r << " (" << cpp_strerror(r) << ") while trimming log";
    return r;
  }

  // Attach contexts to wait for all expiring segments to expire
  MDSGatherBuilder expiry_gather(g_ceph_context);

  const std::set<LogSegment*> &expiring_segments = mdlog->get_expiring_segments();
  for (std::set<LogSegment*>::const_iterator i = expiring_segments.begin();
       i != expiring_segments.end(); ++i) {
    (*i)->wait_for_expiry(expiry_gather.new_sub());
  }
  dout(5) << __func__ << ": waiting for " << expiry_gather.num_subs_created()
          << " segments to expire" << dendl;

  if (expiry_gather.has_subs()) {
    C_SaferCond cond;
    expiry_gather.set_finisher(new MDSInternalContextWrapper(this, &cond));
    expiry_gather.activate();

    // Drop mds_lock to allow progress until expiry is complete
    mds_lock.Unlock();
    int r = cond.wait();
    mds_lock.Lock();

    assert(r == 0);  // MDLog is not allowed to raise errors via wait_for_expiry
  }

  dout(5) << __func__ << ": expiry complete, expire_pos/trim_pos is now " << std::hex <<
    mdlog->get_journaler()->get_expire_pos() << "/" <<
    mdlog->get_journaler()->get_trimmed_pos() << dendl;

  // Now everyone I'm interested in is expired
  mdlog->trim_expired_segments();

  dout(5) << __func__ << ": trim complete, expire_pos/trim_pos is now " << std::hex <<
    mdlog->get_journaler()->get_expire_pos() << "/" <<
    mdlog->get_journaler()->get_trimmed_pos() << dendl;

  // Flush the journal header so that readers will start from after the flushed region
  C_SaferCond wrote_head;
  mdlog->get_journaler()->write_head(&wrote_head);
  mds_lock.Unlock();  // Drop lock to allow messenger dispatch progress
  r = wrote_head.wait();
  mds_lock.Lock();
  if (r != 0) {
      *ss << "Error " << r << " (" << cpp_strerror(r) << ") while writing header";
      return r;
  }

  dout(5) << __func__ << ": write_head complete, all done!" << dendl;

  return 0;
}


void MDSRank::command_get_subtrees(Formatter *f)
{
  assert(f != NULL);
  Mutex::Locker l(mds_lock);

  std::list<CDir*> subtrees;
  mdcache->list_subtrees(subtrees);

  f->open_array_section("subtrees");
  for (std::list<CDir*>::iterator i = subtrees.begin(); i != subtrees.end(); ++i) {
    const CDir *dir = *i;

    f->open_object_section("subtree");
    {
      f->dump_bool("is_auth", dir->is_auth());
      f->dump_int("auth_first", dir->get_dir_auth().first);
      f->dump_int("auth_second", dir->get_dir_auth().second);
      f->dump_int("export_pin", dir->inode->get_export_pin());
      f->open_object_section("dir");
      dir->dump(f);
      f->close_section();
    }
    f->close_section();
  }
  f->close_section();
}


void MDSRank::command_export_dir(Formatter *f,
    const std::string &path,
    mds_rank_t target)
{
  int r = _command_export_dir(path, target);
  f->open_object_section("results");
  f->dump_int("return_code", r);
  f->close_section(); // results
}

int MDSRank::_command_export_dir(
    const std::string &path,
    mds_rank_t target)
{
  Mutex::Locker l(mds_lock);
  filepath fp(path.c_str());

  if (target == whoami || !mdsmap->is_up(target) || !mdsmap->is_in(target)) {
    derr << "bad MDS target " << target << dendl;
    return -ENOENT;
  }

  CInode *in = mdcache->cache_traverse(fp);
  if (!in) {
    derr << "Bath path '" << path << "'" << dendl;
    return -ENOENT;
  }
  CDir *dir = in->get_dirfrag(frag_t());
  if (!dir || !(dir->is_auth())) {
    derr << "bad export_dir path dirfrag frag_t() or dir not auth" << dendl;
    return -EINVAL;
  }

  mdcache->migrator->export_dir(dir, target);
  return 0;
}

CDir *MDSRank::_command_dirfrag_get(
    const cmdmap_t &cmdmap,
    std::ostream &ss)
{
  std::string path;
  bool got = cmd_getval(g_ceph_context, cmdmap, "path", path);
  if (!got) {
    ss << "missing path argument";
    return NULL;
  }

  std::string frag_str;
  if (!cmd_getval(g_ceph_context, cmdmap, "frag", frag_str)) {
    ss << "missing frag argument";
    return NULL;
  }

  CInode *in = mdcache->cache_traverse(filepath(path.c_str()));
  if (!in) {
    // TODO really we should load something in if it's not in cache,
    // but the infrastructure is harder, and we might still be unable
    // to act on it if someone else is auth.
    ss << "directory '" << path << "' inode not in cache";
    return NULL;
  }

  frag_t fg;

  if (!fg.parse(frag_str.c_str())) {
    ss << "frag " << frag_str << " failed to parse";
    return NULL;
  }

  CDir *dir = in->get_dirfrag(fg);
  if (!dir) {
    ss << "frag 0x" << std::hex << in->ino() << "/" << fg << " not in cache ("
          "use `dirfrag ls` to see if it should exist)";
    return NULL;
  }

  if (!dir->is_auth()) {
    ss << "frag " << dir->dirfrag() << " not auth (auth = "
       << dir->authority() << ")";
    return NULL;
  }

  return dir;
}

bool MDSRank::command_dirfrag_split(
    cmdmap_t cmdmap,
    std::ostream &ss)
{
  Mutex::Locker l(mds_lock);
  if (!mdsmap->allows_dirfrags()) {
    ss << "dirfrags are disallowed by the mds map!";
    return false;
  }

  int64_t by = 0;
  if (!cmd_getval(g_ceph_context, cmdmap, "bits", by)) {
    ss << "missing bits argument";
    return false;
  }

  if (by <= 0) {
    ss << "must split by >0 bits";
    return false;
  }

  CDir *dir = _command_dirfrag_get(cmdmap, ss);
  if (!dir) {
    return false;
  }

  mdcache->split_dir(dir, by);

  return true;
}

bool MDSRank::command_dirfrag_merge(
    cmdmap_t cmdmap,
    std::ostream &ss)
{
  Mutex::Locker l(mds_lock);
  std::string path;
  bool got = cmd_getval(g_ceph_context, cmdmap, "path", path);
  if (!got) {
    ss << "missing path argument";
    return false;
  }

  std::string frag_str;
  if (!cmd_getval(g_ceph_context, cmdmap, "frag", frag_str)) {
    ss << "missing frag argument";
    return false;
  }

  CInode *in = mdcache->cache_traverse(filepath(path.c_str()));
  if (!in) {
    ss << "directory '" << path << "' inode not in cache";
    return false;
  }

  frag_t fg;
  if (!fg.parse(frag_str.c_str())) {
    ss << "frag " << frag_str << " failed to parse";
    return false;
  }

  mdcache->merge_dir(in, fg);

  return true;
}

bool MDSRank::command_dirfrag_ls(
    cmdmap_t cmdmap,
    std::ostream &ss,
    Formatter *f)
{
  Mutex::Locker l(mds_lock);
  std::string path;
  bool got = cmd_getval(g_ceph_context, cmdmap, "path", path);
  if (!got) {
    ss << "missing path argument";
    return false;
  }

  CInode *in = mdcache->cache_traverse(filepath(path.c_str()));
  if (!in) {
    ss << "directory inode not in cache";
    return false;
  }

  f->open_array_section("frags");
  std::list<frag_t> frags;
  // NB using get_leaves_under instead of get_dirfrags to give
  // you the list of what dirfrags may exist, not which are in cache
  in->dirfragtree.get_leaves_under(frag_t(), frags);
  for (std::list<frag_t>::iterator i = frags.begin();
       i != frags.end(); ++i) {
    f->open_object_section("frag");
    f->dump_int("value", i->value());
    f->dump_int("bits", i->bits());
    std::ostringstream frag_str;
    frag_str << std::hex << i->value() << "/" << std::dec << i->bits();
    f->dump_string("str", frag_str.str());
    f->close_section();
  }
  f->close_section();

  return true;
}

void MDSRank::dump_status(Formatter *f) const
{
  if (state == MDSMap::STATE_REPLAY ||
      state == MDSMap::STATE_STANDBY_REPLAY) {
    mdlog->dump_replay_status(f);
  } else if (state == MDSMap::STATE_RESOLVE) {
    mdcache->dump_resolve_status(f);
  } else if (state == MDSMap::STATE_RECONNECT) {
    server->dump_reconnect_status(f);
  } else if (state == MDSMap::STATE_REJOIN) {
    mdcache->dump_rejoin_status(f);
  } else if (state == MDSMap::STATE_CLIENTREPLAY) {
    dump_clientreplay_status(f);
  }
}

void MDSRank::dump_clientreplay_status(Formatter *f) const
{
  f->open_object_section("clientreplay_status");
  f->dump_unsigned("clientreplay_queue", replay_queue.size());
  f->dump_unsigned("active_replay", mdcache->get_num_client_requests());
  f->close_section();
}

void MDSRankDispatcher::update_log_config()
{
  map<string,string> log_to_monitors;
  map<string,string> log_to_syslog;
  map<string,string> log_channel;
  map<string,string> log_prio;
  map<string,string> log_to_graylog;
  map<string,string> log_to_graylog_host;
  map<string,string> log_to_graylog_port;
  uuid_d fsid;
  string host;

  if (parse_log_client_options(g_ceph_context, log_to_monitors, log_to_syslog,
			       log_channel, log_prio, log_to_graylog,
			       log_to_graylog_host, log_to_graylog_port,
			       fsid, host) == 0)
    clog->update_config(log_to_monitors, log_to_syslog,
			log_channel, log_prio, log_to_graylog,
			log_to_graylog_host, log_to_graylog_port,
			fsid, host);
  dout(10) << __func__ << " log_to_monitors " << log_to_monitors << dendl;
}

void MDSRank::create_logger()
{
  dout(10) << "create_logger" << dendl;
  {
    PerfCountersBuilder mds_plb(g_ceph_context, "mds", l_mds_first, l_mds_last);

    mds_plb.add_u64_counter(
      l_mds_request, "request", "Requests", "req",
      PerfCountersBuilder::PRIO_CRITICAL);
    mds_plb.add_u64_counter(l_mds_reply, "reply", "Replies");
    mds_plb.add_time_avg(
      l_mds_reply_latency, "reply_latency", "Reply latency", "rlat",
      PerfCountersBuilder::PRIO_CRITICAL);
    mds_plb.add_u64_counter(
      l_mds_forward, "forward", "Forwarding request", "fwd",
      PerfCountersBuilder::PRIO_INTERESTING);
    mds_plb.add_u64_counter(l_mds_dir_fetch, "dir_fetch", "Directory fetch");
    mds_plb.add_u64_counter(l_mds_dir_commit, "dir_commit", "Directory commit");
    mds_plb.add_u64_counter(l_mds_dir_split, "dir_split", "Directory split");
    mds_plb.add_u64_counter(l_mds_dir_merge, "dir_merge", "Directory merge");

    mds_plb.add_u64(l_mds_inode_max, "inode_max", "Max inodes, cache size");
    mds_plb.add_u64(l_mds_inodes, "inodes", "Inodes", "inos",
		    PerfCountersBuilder::PRIO_CRITICAL);
    mds_plb.add_u64(l_mds_inodes_top, "inodes_top", "Inodes on top");
    mds_plb.add_u64(l_mds_inodes_bottom, "inodes_bottom", "Inodes on bottom");
    mds_plb.add_u64(
      l_mds_inodes_pin_tail, "inodes_pin_tail", "Inodes on pin tail");
    mds_plb.add_u64(l_mds_inodes_pinned, "inodes_pinned", "Inodes pinned");
    mds_plb.add_u64(l_mds_inodes_expired, "inodes_expired", "Inodes expired");
    mds_plb.add_u64(
      l_mds_inodes_with_caps, "inodes_with_caps", "Inodes with capabilities");
    mds_plb.add_u64(l_mds_caps, "caps", "Capabilities", "caps",
		    PerfCountersBuilder::PRIO_INTERESTING);
    mds_plb.add_u64(l_mds_subtrees, "subtrees", "Subtrees");

    mds_plb.add_u64_counter(l_mds_traverse, "traverse", "Traverses");
    mds_plb.add_u64_counter(l_mds_traverse_hit, "traverse_hit", "Traverse hits");
    mds_plb.add_u64_counter(l_mds_traverse_forward, "traverse_forward",
			    "Traverse forwards");
    mds_plb.add_u64_counter(l_mds_traverse_discover, "traverse_discover",
			    "Traverse directory discovers");
    mds_plb.add_u64_counter(l_mds_traverse_dir_fetch, "traverse_dir_fetch",
			    "Traverse incomplete directory content fetchings");
    mds_plb.add_u64_counter(l_mds_traverse_remote_ino, "traverse_remote_ino",
			    "Traverse remote dentries");
    mds_plb.add_u64_counter(l_mds_traverse_lock, "traverse_lock",
			    "Traverse locks");

    mds_plb.add_u64(l_mds_load_cent, "load_cent", "Load per cent");
    mds_plb.add_u64(l_mds_dispatch_queue_len, "q", "Dispatch queue length");

    mds_plb.add_u64_counter(l_mds_exported, "exported", "Exports");
    mds_plb.add_u64_counter(
      l_mds_exported_inodes, "exported_inodes", "Exported inodes", "exi",
      PerfCountersBuilder::PRIO_INTERESTING);
    mds_plb.add_u64_counter(l_mds_imported, "imported", "Imports");
    mds_plb.add_u64_counter(
      l_mds_imported_inodes, "imported_inodes", "Imported inodes", "imi",
      PerfCountersBuilder::PRIO_INTERESTING);
    logger = mds_plb.create_perf_counters();
    g_ceph_context->get_perfcounters_collection()->add(logger);
  }

  {
    PerfCountersBuilder mdm_plb(g_ceph_context, "mds_mem", l_mdm_first, l_mdm_last);
    mdm_plb.add_u64(l_mdm_ino, "ino", "Inodes");
    mdm_plb.add_u64_counter(l_mdm_inoa, "ino+", "Inodes opened");
    mdm_plb.add_u64_counter(l_mdm_inos, "ino-", "Inodes closed");
    mdm_plb.add_u64(l_mdm_dir, "dir", "Directories");
    mdm_plb.add_u64_counter(l_mdm_dira, "dir+", "Directories opened");
    mdm_plb.add_u64_counter(l_mdm_dirs, "dir-", "Directories closed");
    mdm_plb.add_u64(l_mdm_dn, "dn", "Dentries");
    mdm_plb.add_u64_counter(l_mdm_dna, "dn+", "Dentries opened");
    mdm_plb.add_u64_counter(l_mdm_dns, "dn-", "Dentries closed");
    mdm_plb.add_u64(l_mdm_cap, "cap", "Capabilities");
    mdm_plb.add_u64_counter(l_mdm_capa, "cap+", "Capabilities added");
    mdm_plb.add_u64_counter(l_mdm_caps, "cap-", "Capabilities removed");
    mdm_plb.add_u64(l_mdm_rss, "rss", "RSS");
    mdm_plb.add_u64(l_mdm_heap, "heap", "Heap size");
    mdm_plb.add_u64(l_mdm_buf, "buf", "Buffer size");
    mlogger = mdm_plb.create_perf_counters();
    g_ceph_context->get_perfcounters_collection()->add(mlogger);
  }

  mdlog->create_logger();
  server->create_logger();
  purge_queue.create_logger();
  sessionmap.register_perfcounters();
  mdcache->register_perfcounters();
}

void MDSRank::check_ops_in_flight()
{
  vector<string> warnings;
  int slow = 0;
  if (op_tracker.check_ops_in_flight(warnings, &slow)) {
    for (vector<string>::iterator i = warnings.begin();
        i != warnings.end();
        ++i) {
      clog->warn() << *i;
    }
  }
 
  // set mds slow request count 
  mds_slow_req_count = slow;
  return;
}

void MDSRankDispatcher::handle_osd_map()
{
  if (is_active() && snapserver) {
    snapserver->check_osd_map(true);
  }

  server->handle_osd_map();

  purge_queue.update_op_limit(*mdsmap);

  std::set<entity_addr_t> newly_blacklisted;
  objecter->consume_blacklist_events(&newly_blacklisted);
  auto epoch = objecter->with_osdmap([](const OSDMap &o){return o.get_epoch();});
  dout(4) << "handle_osd_map epoch " << epoch << ", "
          << newly_blacklisted.size() << " new blacklist entries" << dendl;
  auto victims = server->apply_blacklist(newly_blacklisted);
  if (victims) {
    set_osd_epoch_barrier(epoch);
  }


  // By default the objecter only requests OSDMap updates on use,
  // we would like to always receive the latest maps in order to
  // apply policy based on the FULL flag.
  objecter->maybe_request_map();
}

bool MDSRank::evict_client(int64_t session_id,
    bool wait, bool blacklist, std::stringstream& err_ss,
    Context *on_killed)
{
  assert(mds_lock.is_locked_by_me());

  // Mutually exclusive args
  assert(!(wait && on_killed != nullptr));

  if (is_any_replay()) {
    err_ss << "MDS is replaying log";
    return false;
  }

  Session *session = sessionmap.get_session(
      entity_name_t(CEPH_ENTITY_TYPE_CLIENT, session_id));
  if (!session) {
    err_ss << "session " << session_id << " not in sessionmap!";
    return false;
  }

  dout(4) << "Preparing blacklist command... (wait=" << wait << ")" << dendl;
  stringstream ss;
  ss << "{\"prefix\":\"osd blacklist\", \"blacklistop\":\"add\",";
  ss << "\"addr\":\"";
  ss << session->info.inst.addr;
  ss << "\"}";
  std::string tmp = ss.str();
  std::vector<std::string> cmd = {tmp};

  auto kill_mds_session = [this, session_id, on_killed](){
    assert(mds_lock.is_locked_by_me());
    Session *session = sessionmap.get_session(
        entity_name_t(CEPH_ENTITY_TYPE_CLIENT, session_id));
    if (session) {
      if (on_killed) {
        server->kill_session(session, on_killed);
      } else {
        C_SaferCond on_safe;
        server->kill_session(session, &on_safe);

        mds_lock.Unlock();
        on_safe.wait();
        mds_lock.Lock();
      }
    } else {
      dout(1) << "session " << session_id << " was removed while we waited "
      "for blacklist" << dendl;

      // Even though it wasn't us that removed it, kick our completion
      // as the session has been removed.
      if (on_killed) {
        on_killed->complete(0);
      }
    }
  };

  auto background_blacklist = [this, session_id, cmd](std::function<void ()> fn){
    assert(mds_lock.is_locked_by_me());

    Context *on_blacklist_done = new FunctionContext([this, session_id, fn](int r) {
      objecter->wait_for_latest_osdmap(
       new C_OnFinisher(
         new FunctionContext([this, session_id, fn](int r) {
              Mutex::Locker l(mds_lock);
              auto epoch = objecter->with_osdmap([](const OSDMap &o){
                  return o.get_epoch();
              });

              set_osd_epoch_barrier(epoch);

              fn();
            }), finisher)
       );
    });

    dout(4) << "Sending mon blacklist command: " << cmd[0] << dendl;
    monc->start_mon_command(cmd, {}, nullptr, nullptr, on_blacklist_done);
  };

  auto blocking_blacklist = [this, cmd, &err_ss, background_blacklist](){
    C_SaferCond inline_ctx;
    background_blacklist([&inline_ctx](){inline_ctx.complete(0);});
    mds_lock.Unlock();
    inline_ctx.wait();
    mds_lock.Lock();
  };

  if (wait) {
    if (blacklist) {
      blocking_blacklist();
    }

    // We dropped mds_lock, so check that session still exists
    session = sessionmap.get_session(entity_name_t(CEPH_ENTITY_TYPE_CLIENT,
          session_id));
    if (!session) {
      dout(1) << "session " << session_id << " was removed while we waited "
                 "for blacklist" << dendl;
      return true;
    }
    kill_mds_session();
  } else {
    if (blacklist) {
      background_blacklist(kill_mds_session);
    } else {
      kill_mds_session();
    }
  }

  return true;
}

void MDSRank::bcast_mds_map()
{
  dout(7) << "bcast_mds_map " << mdsmap->get_epoch() << dendl;

  // share the map with mounted clients
  set<Session*> clients;
  sessionmap.get_client_session_set(clients);
  for (set<Session*>::const_iterator p = clients.begin();
       p != clients.end();
       ++p)
    (*p)->connection->send_message(new MMDSMap(monc->get_fsid(), mdsmap));
  last_client_mdsmap_bcast = mdsmap->get_epoch();
}

MDSRankDispatcher::MDSRankDispatcher(
    mds_rank_t whoami_,
    Mutex &mds_lock_,
    LogChannelRef &clog_,
    SafeTimer &timer_,
    Beacon &beacon_,
    MDSMap *& mdsmap_,
    Messenger *msgr,
    MonClient *monc_,
    Context *respawn_hook_,
    Context *suicide_hook_)
  : MDSRank(whoami_, mds_lock_, clog_, timer_, beacon_, mdsmap_,
      msgr, monc_, respawn_hook_, suicide_hook_)
{}

bool MDSRankDispatcher::handle_command(
  const cmdmap_t &cmdmap,
  MCommand *m,
  int *r,
  std::stringstream *ds,
  std::stringstream *ss,
  bool *need_reply)
{
  assert(r != nullptr);
  assert(ds != nullptr);
  assert(ss != nullptr);

  *need_reply = true;

  std::string prefix;
  cmd_getval(g_ceph_context, cmdmap, "prefix", prefix);

  if (prefix == "session ls" || prefix == "client ls") {
    std::vector<std::string> filter_args;
    cmd_getval(g_ceph_context, cmdmap, "filters", filter_args);

    SessionFilter filter;
    *r = filter.parse(filter_args, ss);
    if (*r != 0) {
      return true;
    }

    Formatter *f = new JSONFormatter(true);
    dump_sessions(filter, f);
    f->flush(*ds);
    delete f;
    return true;
  } else if (prefix == "session evict" || prefix == "client evict") {
    std::vector<std::string> filter_args;
    cmd_getval(g_ceph_context, cmdmap, "filters", filter_args);

    SessionFilter filter;
    *r = filter.parse(filter_args, ss);
    if (*r != 0) {
      return true;
    }

    evict_clients(filter, m);

    *need_reply = false;
    return true;
  } else if (prefix == "damage ls") {
    Formatter *f = new JSONFormatter(true);
    damage_table.dump(f);
    f->flush(*ds);
    delete f;
    return true;
  } else if (prefix == "damage rm") {
    damage_entry_id_t id = 0;
    bool got = cmd_getval(g_ceph_context, cmdmap, "damage_id", (int64_t&)id);
    if (!got) {
      *r = -EINVAL;
      return true;
    }

    damage_table.erase(id);
    return true;
  } else {
    return false;
  }
}

epoch_t MDSRank::get_osd_epoch() const
{
  return objecter->with_osdmap(std::mem_fn(&OSDMap::get_epoch));  
}

