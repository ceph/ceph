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

#include "MDCache.h"
#include "Server.h"
#include "Locker.h"
#include "MDSMap.h"
#include "Mutation.h"

#include "MDLog.h"
#include "SessionMap.h"
#include "InoTable.h"
#include "mon/MonClient.h"
#include "osdc/Objecter.h"
#include "osdc/Journaler.h"
#include "common/HeartbeatMap.h"

#include "messages/MMDSMap.h"
#include "messages/MCommand.h"
#include "messages/MCommandReply.h"

#include "MDSRank.h"
#include "MDSDaemon.h"

#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds." << whoami << '.' << incarnation << ' '

struct MDRequestImpl;
typedef ceph::shared_ptr<MDRequestImpl> MDRequestRef;

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
    server(NULL), locker(NULL), mdcache(NULL),
    mdlog(NULL), sessionmap(NULL), inotable(NULL),
    last_state(MDSMap::STATE_BOOT),
    state(MDSMap::STATE_BOOT),
    stopping(false),
    hb(NULL), beacon(beacon_),
    last_client_mdsmap_bcast(0),
    messenger(msgr), monc(monc_),
    respawn_hook(respawn_hook_),
    suicide_hook(suicide_hook_),
    op_tp(msgr->cct, "MDSRank::op_tp", "tp_op",  g_conf->osd_op_threads * 2, "mds_op_threads"),
    msg_tp(msgr->cct, "MDSRank::msg_tp", "tp_msg",  g_conf->osd_op_threads, "mds_msg_threads"),
    ctx_wq(this, g_conf->osd_op_thread_timeout, &op_tp),
    req_wq(this, g_conf->osd_op_thread_timeout, &op_tp),
    msg_wq(this, g_conf->osd_op_thread_timeout, &msg_tp)
{
  hb = g_ceph_context->get_heartbeat_map()->add_worker("MDSRank", pthread_self());

  objecter->unset_honor_osdmap_full();

  mdcache = new MDCache(this);
  server = new Server(this);
  locker = new Locker(this);

  mdlog = new MDLog(this);
  sessionmap = new SessionMap(this);
  inotable = new InoTable(this);

  log_finisher = new Finisher(msgr->cct);
}

MDSRank::~MDSRank()
{
  if (hb) {
    g_ceph_context->get_heartbeat_map()->remove_worker(hb);
  }

  if (mdsmap) { delete mdsmap; mdsmap = NULL; }
  if (mdcache) { delete mdcache; mdcache = NULL; }
  if (server) { delete server; server = NULL; }
  if (mdlog) { delete mdlog; mdlog = NULL; }
  if (sessionmap) { delete sessionmap; sessionmap = NULL; }
  if (inotable) { delete inotable; inotable = NULL; }

  delete log_finisher;
  log_finisher = NULL;

  delete suicide_hook;
  suicide_hook = NULL;

  delete respawn_hook;
  respawn_hook = NULL;

  delete objecter;
  objecter = nullptr;
}

void MDSRank::create_logger()
{
  mdlog->create_logger();
}

void MDSRankDispatcher::init()
{
  objecter->init();
  messenger->add_dispatcher_head(objecter);

  objecter->start();

  update_log_config();
  create_logger();

  // Expose the OSDMap (already populated during MDSRank::init) to anyone
  // who is interested in it.
  handle_osd_map();

  op_tp.start();
  msg_tp.start();
  log_finisher->start();
}

void MDSRankDispatcher::tick()
{
  heartbeat_reset();

  if (beacon.is_laggy()) {
    dout(5) << "tick bailing out since we seem laggy" << dendl;
    return;
  }

  mdlog->flush();

  // Expose ourselves to Beacon to update health indicators
  beacon.notify_health(this);

  if (is_active() || is_stopping()) {
    mdlog->trim();  // NOT during recovery!
  }

  if (is_clientreplay() || is_active() || is_stopping()) {
    mds_lock.Unlock();
    // FIXME: use seperate timer to do this
    server->find_idle_sessions();
    locker->tick();
    mds_lock.Lock();
  }
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

  mdlog->shutdown();

  mdcache->shutdown();

  monc->shutdown();
  
  if (objecter->initialized.read())
    objecter->shutdown();

  // release mds_lock for messenger threads (e.g.
  // MDSDaemon::ms_handle_reset called from Messenger).
  mds_lock.Unlock();

  op_tp.stop();
  msg_tp.stop();
  log_finisher->stop();

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
class C_MDS_VoidFn : public MDSAsyncContextBase
{
  typedef void (MDSRank::*fn_ptr)();
protected:
  MDSRank *mds;
  MDSRank* get_mds() { return mds; }
  fn_ptr fn;
public:
  C_MDS_VoidFn(MDSRank *mds_, fn_ptr fn_) : mds(mds_), fn(fn_)
  {
    assert(mds);
    assert(fn);
  }
  void finish(int r)
  {
    Mutex::Locker l(mds->mds_lock);
    assert(r >= 0);
    (mds->*fn)();
  }
};

int64_t MDSRank::get_metadata_pool()
{
    return mdsmap->get_metadata_pool();
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


bool MDSRankDispatcher::ms_dispatch(Message *m)
{
  if (is_deferrable_message(m)) {
    msg_wq.queue(m);
    return true;
  }
  return false;
}

bool MDSRank::is_deferrable_message(Message *m)
{
  int port = m->get_type() & 0xff00;
  switch (port) {
    // mdcache
    case MDS_PORT_CACHE:
    // migrator
    case MDS_PORT_MIGRATOR:
      return true;
    default:
      switch (m->get_type()) {
	// server
	case CEPH_MSG_CLIENT_SESSION:
	case CEPH_MSG_CLIENT_RECONNECT:
	case CEPH_MSG_CLIENT_REQUEST:
	case MSG_MDS_SLAVE_REQUEST:
	// balancer
	case MSG_MDS_HEARTBEAT:
	// table client/server
	case MSG_MDS_TABLE_REQUEST:
	// locker
	case MSG_MDS_LOCK:
	case MSG_MDS_INODEFILECAPS:
	case CEPH_MSG_CLIENT_CAPS:
	case CEPH_MSG_CLIENT_CAPRELEASE:
	case CEPH_MSG_CLIENT_LEASE:
	  return true;
      }
  }
  return false;
}

/*
 * lower priority messages we defer if we seem laggy
 */
void MDSRank::handle_deferrable_message(Message *m)
{
  int port = m->get_type() & 0xff00;
  switch (port) {
  case MDS_PORT_CACHE:
    ALLOW_MESSAGES_FROM(CEPH_ENTITY_TYPE_MDS);
    mdcache->dispatch(m);
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
      derr << "mds unknown message " << m->get_type() << dendl;
      m->put();
    }
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

Session *MDSRank::get_session(Message *m)
{
  Session *session = static_cast<Session *>(m->get_connection()->get_priv());
  if (session) {
    dout(20) << "get_session have " << session << " " << session->info.inst
	     << " state " << session->get_state_name() << dendl;
    session->put();  // not carry ref
  } else {
    dout(20) << "get_session dne for " << m->get_source_inst() << dendl;
  }
  return session;
}

void MDSRank::bcast_mds_map()
{
  dout(7) << "bcast_mds_map " << mdsmap->get_epoch() << dendl;
  // share the map with mounted clients
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

class C_MDS_BootStart : public MDSAsyncContextBase {
protected:
  MDSRank *mds;
  MDSRank* get_mds() { return mds; }
  MDSRank::BootStep nextstep;
public:
  C_MDS_BootStart(MDSRank *m, MDSRank::BootStep n) : mds(m), nextstep(n) {
    assert(mds);
  }
  void finish(int r) {
    Mutex::Locker l(mds->mds_lock);
    mds->boot_start(nextstep, r);
  }
};

void MDSRank::boot_start(BootStep step, int r)
{
  dout(3) << "boot_start" << dendl;

  assert(r >= 0);

  assert(is_starting() || is_any_replay());

  switch(step) {
  case MDS_BOOT_INITIAL:
    {
      mdcache->init_layouts();

      MDSGatherBuilder gather(g_ceph_context, new C_MDS_BootStart(this, MDS_BOOT_PREPARE_LOG));

      dout(3) << "boot_start " << step << ": opening inotable" << dendl;
      inotable->set_rank(whoami);
      inotable->load(gather.new_sub());

      dout(3) << "boot_start " << step << ": opening sessionmap" << dendl;
      sessionmap->set_rank(whoami);
      sessionmap->load(gather.new_sub());

      dout(3) << "boot_start " << step << ": opening mds log" << dendl;
      mdlog->open(gather.new_sub());

      dout(3) << "boot_start " << step << ": create root and mydir" << dendl;
      mdcache->create_empty_hierarchy(NULL);
      mdcache->create_mydir_hierarchy(NULL);

      gather.activate();
    }
    break;
  case MDS_BOOT_PREPARE_LOG:
    if (is_any_replay()) {
      mdlog->replay(new C_MDS_BootStart(this, MDS_BOOT_REPLAY_DONE));
    } else {
      mdlog->append();
      starting_done();
    }
    break;
  case MDS_BOOT_REPLAY_DONE:
    assert(is_any_replay());
    replay_done();
    break;
  }
}

void MDSRank::starting_done()
{
  dout(3) << "starting_done" << dendl;
  assert(is_starting());
  request_state(MDSMap::STATE_ACTIVE);
  mdcache->open_root_and_mydir();
  mdlog->start_new_segment();
}

void MDSRank::replay_start()
{
  dout(1) << "replay_start" << dendl;

  boot_start();
}

void MDSRank::replay_done()
{
  dout(1) << "replay_done" << dendl;

  if (mdcache->has_replay_undef_inodes()) {
    dout(1) << "opening replay undef inodes" << dendl;
    mdcache->open_replay_undef_inodes(new C_MDS_VoidFn(this, &MDSRank::replay_done));
    return;
  }

  dout(1) << "making mds journal writeable" << dendl;
  mdlog->get_journaler()->set_writeable();
  mdlog->get_journaler()->trim_tail();

  request_state(MDSMap::STATE_RECONNECT);
}

void MDSRank::resolve_start()
{
  dout(1) << "resolve_start" << dendl;
  assert(0);
}
void MDSRank::resolve_done()
{
  dout(1) << "resolve_done" << dendl;
  request_state(MDSMap::STATE_RECONNECT);
}

void MDSRank::reconnect_start()
{
  dout(1) << "reconnect_start" << dendl;
  reconnect_done();
}
void MDSRank::reconnect_done()
{
  dout(1) << "reconnect_done" << dendl;
  request_state(MDSMap::STATE_REJOIN);
}

void MDSRank::rejoin_start()
{
  dout(1) << "rejoin_start" << dendl;
  rejoin_done();
}
void MDSRank::rejoin_done()
{
  dout(1) << "rejoin_done" << dendl;
  request_state(MDSMap::STATE_ACTIVE);
}

void MDSRank::clientreplay_start()
{
  dout(1) << "clientreplay_start" << dendl;
  assert(0);
}

void MDSRank::active_start()
{
  dout(1) << "active_start" << dendl;
}

void MDSRank::recovery_done(int oldstate)
{
  dout(1) << "recovery_done -- successful recovery!" << dendl;
  assert(is_clientreplay() || is_active());

  if (oldstate == MDSMap::STATE_CREATING)
    return;

  mdcache->open_root_and_mydir();
}

void MDSRank::creating_done()
{
  dout(1)<< "creating_done" << dendl;
  request_state(MDSMap::STATE_ACTIVE);
}

void MDSRank::boot_create()
{
  dout(3) << "boot_create" << dendl;

  MDSGatherBuilder gather(g_ceph_context, new C_MDS_VoidFn(this, &MDSRank::creating_done));

  mdcache->init_layouts();

  // start with a fresh journal
  dout(3) << "boot_create creating fresh journal" << dendl;
  mdlog->create(gather.new_sub());

  // open new journal segment, but do not journal subtree map (yet)
  mdlog->prepare_new_segment();

  dout(3) << "boot_create creating fresh hierarchy" << dendl;
  mdcache->create_empty_hierarchy(gather.get());

  dout(3) << "boot_create creating mydir hierarchy" << dendl;
  mdcache->create_mydir_hierarchy(gather.get());

  dout(10) << "boot_create creating fresh inotable table" << dendl;
  inotable->mutex_lock();
  inotable->set_rank(whoami);
  inotable->reset();
  inotable->save(gather.new_sub());
  inotable->mutex_unlock();

  dout(10) << "boot_create creating fresh session map" << dendl;
  sessionmap->mutex_lock();
  sessionmap->set_rank(whoami);
  sessionmap->save(gather.new_sub());
  sessionmap->mutex_unlock();

  mdlog->journal_segment_subtree_map(gather.new_sub());

  gather.activate();
}

void MDSRank::stopping_start()
{
  dout(2) << "stopping_start" << dendl;
  assert(0);
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

  if (oldmap->is_degraded() && !mdsmap->is_degraded() && state >= MDSMap::STATE_ACTIVE)
    dout(1) << "cluster recovered." << dendl;
}

void MDSRank::dump_status(Formatter *f) const
{
}

MDSRank::CtxWQ::CtxWQ(MDSRank *m, time_t ti, ThreadPool *tp)
  : ThreadPool::WorkQueueVal<pair<MDSContextBase*,int> >("MDSRank::CtxWQ", ti, ti*10, tp), mds(m)
{
}

MDSRank::ReqWQ::ReqWQ(MDSRank *m, time_t ti, ThreadPool *tp)
  : ThreadPool::WorkQueueVal<const MDRequestRef&, entity_name_t>("MDSRank::MsgWQ", ti, ti*10, tp),
    mds(m), qlock("ReqWQ::qlock"),
    pqueue(g_conf->osd_op_pq_max_tokens_per_priority,
           g_conf->osd_op_pq_min_cost)
{}

void MDSRank::ReqWQ::_enqueue(const MDRequestRef& mdr)
{
  if (mdr->retries > 0)
    pqueue.enqueue_strict(mdr->reqid.name, CEPH_MSG_PRIO_HIGH, mdr);
  else
    pqueue.enqueue_strict(mdr->reqid.name, CEPH_MSG_PRIO_DEFAULT, mdr);
}

void MDSRank::ReqWQ::_enqueue_front(const MDRequestRef& mdr)
{
  assert(0);

  MDRequestRef other;
  {
    Mutex::Locker l(qlock);
    auto it = req_for_processing.find(mdr->reqid.name);
    if (it != req_for_processing.end()) {
      it->second.push_front(mdr);
      other = it->second.back();
      it->second.pop_back();
    }
  }
  pqueue.enqueue_strict_front(mdr->reqid.name, CEPH_MSG_PRIO_DEFAULT,
			      other ? other : mdr);
}

entity_name_t MDSRank::ReqWQ::_dequeue()
{
  assert(!pqueue.empty());
  entity_name_t name;
  {
    Mutex::Locker l(qlock);
    MDRequestRef mdr = pqueue.dequeue();
    name = mdr->reqid.name;
    req_for_processing[name].push_back(mdr);
  }
  return name;
}

void MDSRank::ReqWQ::_process(entity_name_t name, ThreadPool::TPHandle &handle)
{
  MDRequestRef mdr;
  {
    Mutex::Locker l(qlock);
    auto it = req_for_processing.find(name);
    if (it == req_for_processing.end())
      return;

    mdr = it->second.front();
    it->second.pop_front();
    if (it->second.empty())
      req_for_processing.erase(it);
  }
  mds->mdcache->dispatch_request(mdr);
}

MDSRank::MsgWQ::MsgWQ(MDSRank *m, time_t ti, ThreadPool *tp)
  : ThreadPool::WorkQueueVal<Message*, entity_inst_t>("MDS::MsgWQ", ti, ti*10, tp),
    mds(m), qlock("MsgWQ::qlock"),
    pqueue(g_conf->osd_op_pq_max_tokens_per_priority,
	   g_conf->osd_op_pq_min_cost)
{}

void MDSRank::MsgWQ::_enqueue(Message *m)
{ 
  unsigned priority = m->get_priority();
  unsigned cost = m->get_cost();
  if (priority >= CEPH_MSG_PRIO_LOW)
    pqueue.enqueue_strict(m->get_source_inst(), priority, m);
  else
    pqueue.enqueue(m->get_source_inst(), priority, cost, m);
}

void MDSRank::MsgWQ::_enqueue_front(Message *m)
{
  entity_inst_t inst = m->get_source_inst();
  {
    Mutex::Locker l(qlock);
    auto it = msg_for_processing.find(inst);
    if (it != msg_for_processing.end() && !it->second.second.empty()) {
      it->second.second.push_front(m);
      m = it->second.second.back();
      it->second.second.pop_back();
    }
  }
  unsigned priority = m->get_priority();
  unsigned cost = m->get_cost();
  if (priority >= CEPH_MSG_PRIO_LOW)
    pqueue.enqueue_strict_front(inst, priority, m);
  else
    pqueue.enqueue_front(inst, priority, cost, m);
}

entity_inst_t MDSRank::MsgWQ::_dequeue()
{
  entity_inst_t inst;
  if (!pqueue.empty()) {
    Message *m = pqueue.dequeue();
    Mutex::Locker l(qlock);
    inst = m->get_source_inst();
    msg_for_processing[inst].second.push_back(m);
  } else {
    Mutex::Locker l(qlock);
    assert(!more_to_process.empty());
    inst = more_to_process.front();
    more_to_process.pop_front();
  }
  return inst;
}

void MDSRank::MsgWQ::_process(entity_inst_t inst, ThreadPool::TPHandle &handle)
{
  Message *m;
  {
    Mutex::Locker l(qlock);
    auto it = msg_for_processing.find(inst);
    if (it == msg_for_processing.end() || it->second.second.empty())
      return;

    if (it->second.first)
      return;

    it->second.first = true;
    m = it->second.second.front();
    it->second.second.pop_front();
  }

  mds->handle_deferrable_message(m);

  {
    Mutex::Locker l(qlock);
    auto it = msg_for_processing.find(inst);
    assert(it != msg_for_processing.end());

    if (it->second.second.empty()) {
      msg_for_processing.erase(it);
    } else {
      it->second.first = false;
      more_to_process.push_back(inst);
    }
  }
}

void MDSRank::retry_dispatch(const MDRequestRef &mdr)
{
  mdr->retries++;
  req_wq.queue(mdr);
}



// --- MDSRankDispatcher ---
bool MDSRankDispatcher::handle_asok_command(
    std::string command, cmdmap_t& cmdmap, Formatter *f,
		    std::ostream& ss)
{
  return false;
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

void MDSRankDispatcher::handle_osd_map()
{
  // By default the objecter only requests OSDMap updates on use,
  // we would like to always receive the latest maps in order to
  // apply policy based on the FULL flag.
  objecter->maybe_request_map();
}

bool MDSRankDispatcher::handle_command_legacy(std::vector<std::string> args)
{
  return false;
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

  return false;
}
