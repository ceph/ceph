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
#include "messages/MMDSMap.h"

#include "MDSMap.h"
#include "MDS.h"
#include "mds_table_types.h"
#include "SnapClient.h"
#include "SnapServer.h"
#include "MDBalancer.h"
#include "messages/MMDSTableRequest.h"
#include "Locker.h"
#include "Server.h"
#include "InoTable.h"
#include "mon/MonClient.h"
#include "common/HeartbeatMap.h"


#include "MDSRank.h"

#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds." << whoami << '.' << incarnation << ' '

MDSRank::MDSRank(
    Mutex &mds_lock_,
    LogChannelRef &clog_,
    SafeTimer &timer_,
    Beacon &beacon_,
    MDSMap *& mdsmap_,
    Finisher *finisher_,
    MDS *mds_daemon_,
    Messenger *msgr,
    MonClient *monc_)
  :
    whoami(MDS_RANK_NONE),
    incarnation(0),
    mds_lock(mds_lock_), clog(clog_), timer(timer_),
    mdsmap(mdsmap_),
    objecter(NULL),
    server(NULL), mdcache(NULL), locker(NULL), mdlog(NULL),
    balancer(NULL), inotable(NULL), snapserver(NULL), snapclient(NULL),
    sessionmap(this), logger(NULL), mlogger(NULL),
    op_tracker(g_ceph_context, g_conf->mds_enable_op_tracker, 
               g_conf->osd_num_op_tracker_shard),
    last_state(MDSMap::STATE_NULL), want_state(MDSMap::STATE_NULL),
    state(MDSMap::STATE_NULL),
    progress_thread(this), dispatch_depth(0),
    hb(NULL), last_tid(0), osd_epoch_barrier(0), beacon(beacon_),
    finisher(finisher_), mds_daemon(mds_daemon_), messenger(msgr), monc(monc_),

    standby_for_rank(MDSMap::MDS_NO_STANDBY_PREF),
    standby_type(MDSMap::STATE_NULL),
    standby_replaying(false)
{
  hb = g_ceph_context->get_heartbeat_map()->add_worker("MDSRank");
}

MDSRank::~MDSRank()
{
  if (hb) {
    g_ceph_context->get_heartbeat_map()->remove_worker(hb);
  }
}

/**
 * Helper for simple callbacks that call a void fn with no args.
 */
class C_VoidFn : public MDSInternalContext
{
  typedef void (MDSRank::*fn_ptr)();
  protected:
   MDSRank *mds_daemon;
   fn_ptr fn; 
  public:
  C_VoidFn(MDSRank *mds_, fn_ptr fn_)
    : MDSInternalContext(mds_), mds_daemon(mds_), fn(fn_)
  {
    assert(mds_);
    assert(fn_);
  }

  void finish(int r)
  {
    (mds_daemon->*fn)();
  }
};



uint64_t MDSRank::get_metadata_pool()
{
    return mdsmap->get_metadata_pool();
}

MDSTableClient *MDSRank::get_table_client(int t)
{
  switch (t) {
  case TABLE_ANCHOR: return NULL;
  case TABLE_SNAP: return snapclient;
  default: assert(0);
  }
}

MDSTableServer *MDSRank::get_table_server(int t)
{
  switch (t) {
  case TABLE_ANCHOR: return NULL;
  case TABLE_SNAP: return snapserver;
  default: assert(0);
  }
}

void MDSRank::suicide(bool fast)
{
  mds_daemon->suicide(fast);
}

void MDSRank::respawn()
{
  mds_daemon->respawn();
}

void MDSRank::damaged()
{
  assert(whoami != MDS_RANK_NONE);
  assert(mds_lock.is_locked_by_me());

  set_want_state(MDSMap::STATE_DAMAGED);
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
    mds_daemon->respawn();
    return;
  }

  if (g_conf->mds_action_on_write_error >= 2) {
    derr << "unhandled write error " << cpp_strerror(err) << ", suicide..." << dendl;
    mds_daemon->suicide();
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
    while (!mds->mds_daemon->stopping &&
	   mds->finished_queue.empty() &&
	   (mds->waiting_for_nolaggy.empty() || mds->beacon.is_laggy())) {
      cond.Wait(mds->mds_lock);
    }

    if (mds->mds_daemon->stopping) {
      break;
    }

    mds->_advance_queues();
  }

  return NULL;
}


void MDSRank::ProgressThread::shutdown()
{
  assert(mds->mds_lock.is_locked_by_me());
  assert(mds->mds_daemon->stopping);

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

bool MDSRank::handle_rank_message(Message *m)
{
  bool ret;
  inc_dispatch_depth();
  ret = _dispatch(m, true);
  dec_dispatch_depth();
  return ret;
}

/* If this function returns true, it has put the message. If it returns false,
 * it has not put the message. */
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
      m->put();
      return false;
    }
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
      want_state == MDSMap::STATE_CLIENTREPLAY) {
    int num_requests = mdcache->get_num_client_requests();
    dout(10) << " still have " << num_requests << " active replay requests" << dendl;
    if (num_requests == 0)
      clientreplay_done();
  }

  // hack: thrash exports
  static utime_t start;
  utime_t now = ceph_clock_now(g_ceph_context);
  if (start == utime_t()) 
    start = now;
  /*double el = now - start;
  if (el > 30.0 &&
    el < 60.0)*/
  for (int i=0; i<g_conf->mds_thrash_exports; i++) {
    set<mds_rank_t> s;
    if (!is_active()) break;
    mdsmap->get_mds_set(s, MDSMap::STATE_ACTIVE);
    if (s.size() < 2 || mdcache->get_num_inodes() < 10) 
      break;  // need peers for this to work.

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
    if (mdcache->get_num_fragmenting_dirs() > 5) break;
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
    if (fg == frag_t() || (rand() % (1 << fg.bits()) == 0))
      mdcache->split_dir(dir, 1);
    else
      balancer->queue_merge(dir);
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

  if (mlogger) {
    mlogger->set(l_mdm_ino, g_num_ino);
    mlogger->set(l_mdm_dir, g_num_dir);
    mlogger->set(l_mdm_dn, g_num_dn);
    mlogger->set(l_mdm_cap, g_num_cap);

    mlogger->inc(l_mdm_inoa, g_num_inoa);  g_num_inoa = 0;
    mlogger->inc(l_mdm_inos, g_num_inos);  g_num_inos = 0;
    mlogger->inc(l_mdm_dira, g_num_dira);  g_num_dira = 0;
    mlogger->inc(l_mdm_dirs, g_num_dirs);  g_num_dirs = 0;
    mlogger->inc(l_mdm_dna, g_num_dna);  g_num_dna = 0;
    mlogger->inc(l_mdm_dns, g_num_dns);  g_num_dns = 0;
    mlogger->inc(l_mdm_capa, g_num_capa);  g_num_capa = 0;
    mlogger->inc(l_mdm_caps, g_num_caps);  g_num_caps = 0;

    mlogger->set(l_mdm_buf, buffer::get_total_alloc());
  }

  // shut down?
  if (is_stopping()) {
    mdlog->trim();
    if (mdcache->shutdown_pass()) {
      dout(7) << "shutdown_pass=true, finished w/ shutdown, moving to down:stopped" << dendl;
      stopping_done();
    }
    else {
      dout(7) << "shutdown_pass=false" << dendl;
    }
  }
  return true;
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
      handle_deferrable_message(old);
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
      assert(want_state == CEPH_MDS_STATE_DNE);
      return;
  }

  // NB not enabling suicide grace, because the mon takes care of killing us
  // (by blacklisting us) when we fail to send beacons, and it's simpler to
  // only have one way of dying.
  g_ceph_context->get_heartbeat_map()->reset_timeout(hb, g_conf->mds_beacon_grace, 0);
}

bool MDSRank::is_stale_message(Message *m)
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

/**
 * FIXME ugly call up to MDS daemon until the dispatching is separated out
 */
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

// FIXME maybe instead of exposing this to the world, we should just
// share a MonClient reference with the guys who need it (balancer+snapserver)
void MDSRank::send_mon_message(Message *m)
{
  monc->send_mon_message(m);
}

uint64_t MDSRank::get_global_id() const
{
  return monc->get_global_id();
}

bool MDSRank::is_daemon_stopping() const
{
  return mds_daemon->stopping;
}

// FIXME>> this fns are state-machiney, not dispatchy
// >>>>>

void MDSRank::request_state(MDSMap::DaemonState s)
{
  dout(3) << "request_state " << ceph_mds_state_name(s) << dendl;
  set_want_state(s);
  beacon.send();
}


class C_MDS_BootStart : public MDSInternalContext {
  MDSRank::BootStep nextstep;
public:
  C_MDS_BootStart(MDSRank *m, MDSRank::BootStep n)
    : MDSInternalContext(m), nextstep(n) {}
  void finish(int r) {
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

        if (is_starting() ||
            whoami == mdsmap->get_root()) {  // load root inode off disk if we are auth
          mdcache->open_root_inode(gather.new_sub());
        } else {
          // replay.  make up fake root inode to start with
          mdcache->create_root_inode();
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
      replay_done();
      break;
  }
}

void MDSRank::starting_done()
{
  dout(3) << "starting_done" << dendl;
  assert(is_starting());
  request_state(MDSMap::STATE_ACTIVE);

  mdcache->open_root();

  // start new segment
  mdlog->start_new_segment();
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
  
  standby_type = MDSMap::STATE_NULL;

  calc_recovery_set();

  // Check if we need to wait for a newer OSD map before starting
  Context *fin = new C_OnFinisher(new C_IO_Wrapper(this, new C_MDS_BootStart(this, MDS_BOOT_INITIAL)), finisher);
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
  void finish(int r) {
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
  dout(1) << "standby_replay_restart"
	  << (standby_replaying ? " (as standby)":" (final takeover pass)")
	  << dendl;
  if (standby_replaying) {
    /* Go around for another pass of replaying in standby */
    mdlog->get_journaler()->reread_head_and_probe(
      new C_MDS_StandbyReplayRestartFinish(
        this,
	mdlog->get_journaler()->get_read_pos()));
  } else {
    /* We are transitioning out of standby: wait for OSD map update
       before making final pass */
    Context *fin = new C_OnFinisher(new C_IO_Wrapper(this,
          new C_MDS_BootStart(this, MDS_BOOT_PREPARE_LOG)),
      finisher);
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
  C_MDS_StandbyReplayRestart(MDSRank *m) : MDSInternalContext(m) {}
  void finish(int r) {
    assert(!r);
    mds->standby_replay_restart();
  }
};

void MDSRank::replay_done()
{
  dout(1) << "replay_done" << (standby_replaying ? " (as standby)" : "") << dendl;

  if (is_oneshot_replay()) {
    dout(2) << "hack.  journal looks ok.  shutting down." << dendl;
    suicide();
    return;
  }

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

  mdcache->resolve_start(new C_VoidFn(this, &MDSRank::resolve_done));
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

  server->reconnect_clients(new C_VoidFn(this, &MDSRank::reconnect_done));
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
  mdcache->rejoin_start(new C_VoidFn(this, &MDSRank::rejoin_done));
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
  queue_one_replay();
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

  mdcache->reissue_all_caps();
  
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

  MDSGatherBuilder fin(g_ceph_context, new C_VoidFn(this, &MDSRank::creating_done));

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

void MDSRank::set_want_state(MDSMap::DaemonState newstate)
{
  if (want_state != newstate) {
    dout(10) << __func__ << " "
      << ceph_mds_state_name(want_state) << " -> "
      << ceph_mds_state_name(newstate) << dendl;
    want_state = newstate;
    beacon.notify_want_state(newstate);
  }
}

// <<<<<<<<

void MDSRank::handle_mds_map_rank(
    MMDSMap *m,
    MDSMap *oldmap,
    int oldwhoami,
    MDSMap::DaemonState oldstate)
{
  // I am only to be passed MDSMaps in which I hold a rank
  assert(whoami != MDS_RANK_NONE);

  version_t epoch = m->get_epoch();

  // note source's map version
  if (m->get_source().is_mds() && 
      peer_mdsmap_epoch[mds_rank_t(m->get_source().num())] < epoch) {
    dout(15) << " peer " << m->get_source()
	     << " has mdsmap epoch >= " << epoch
	     << dendl;
    peer_mdsmap_epoch[mds_rank_t(m->get_source().num())] = epoch;
  }

  // Once I hold a rank it can't be taken away without
  // restarting this daemon
  if (whoami != oldwhoami && oldwhoami != MDS_RANK_NONE) {
    derr << "Invalid rank transition " << oldwhoami << "->" << whoami << dendl;
    respawn();
  }

  // Validate state transitions while I hold a rank
  bool state_valid = true;
  if (state != oldstate) {
    if (oldstate == MDSMap::STATE_REPLAY) {
      if (state != MDSMap::STATE_RESOLVE && state != MDSMap::STATE_RECONNECT) {
        state_valid = false;
      }
    } else if (oldstate == MDSMap::STATE_REJOIN) {
      if (state != MDSMap::STATE_ACTIVE
          && state != MDSMap::STATE_CLIENTREPLAY
          && state != MDSMap::STATE_STOPPED) {
        state_valid = false;
      }
    } else if (oldstate >= MDSMap::STATE_RECONNECT && oldstate < MDSMap::STATE_ACTIVE) {
      // Once I have entered replay, the only allowable transitions are to
      // the next state along in the sequence.
      if (state != oldstate + 1) {
        state_valid = false;
      }
    }
  }

  if (!state_valid) {
    derr << "Invalid state transition " << ceph_mds_state_name(oldstate)
      << "->" << ceph_mds_state_name(state) << dendl;
    respawn();
  }

  if (oldwhoami != whoami || oldstate != state) {
    // update messenger.
    if (state == MDSMap::STATE_STANDBY_REPLAY || state == MDSMap::STATE_ONESHOT_REPLAY) {
      dout(1) << "handle_mds_map i am now mds." << monc->get_global_id() << "." << incarnation
	      << "replaying mds." << whoami << "." << incarnation << dendl;
      messenger->set_myname(entity_name_t::MDS(monc->get_global_id()));
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
    set_want_state(state);

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
  if (is_rejoin() || is_clientreplay() || is_active() || is_stopping()) {
    // did we start?
    if (!oldmap->is_rejoining() && mdsmap->is_rejoining())
      rejoin_joint_start();

    // did we finish?
    if (g_conf->mds_dump_cache_after_rejoin &&
	oldmap->is_rejoining() && !mdsmap->is_rejoining()) 
      mdcache->dump_cache();      // for DEBUG only

    if (oldstate >= MDSMap::STATE_REJOIN) {
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

  if (oldmap->is_degraded() && !mdsmap->is_degraded() && state >= MDSMap::STATE_ACTIVE)
    dout(1) << "cluster recovered." << dendl;

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
      if (olddown.count(*p) == 0) {
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

  if (!is_any_replay())
    balancer->try_rebalance();

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
    const OSDMap *osdmap = objecter->get_osdmap_read();
    const epoch_t osd_epoch = osdmap->get_epoch();
    objecter->put_osdmap_read();
    set_osd_epoch_barrier(osd_epoch);
  }

  mdcache->notify_mdsmap_changed();
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

