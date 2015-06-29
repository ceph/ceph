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
    progress_thread(this),
    hb(NULL), last_tid(0), osd_epoch_barrier(0), beacon(beacon_),
    finisher(finisher_), mds_daemon(mds_daemon_), messenger(msgr), monc(monc_)
{
  hb = g_ceph_context->get_heartbeat_map()->add_worker("MDSRank");
}

MDSRank::~MDSRank()
{
  if (hb) {
    g_ceph_context->get_heartbeat_map()->remove_worker(hb);
  }
}

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
  mds_daemon->damaged();
}

void MDSRank::damaged_unlocked()
{
  mds_daemon->damaged_unlocked();
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
void MDSRank::dispatch(Message *m)
{
  mds_daemon->inc_dispatch_depth();
  mds_daemon->_dispatch(m, false);
  mds_daemon->dec_dispatch_depth();
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
