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

#include "MDS.h"
#include "Server.h"
#include "Locker.h"
#include "MDCache.h"
#include "MDLog.h"
#include "Migrator.h"
#include "MDBalancer.h"
#include "AnchorClient.h"
#include "IdAllocator.h"

#include "msg/Messenger.h"

#include "messages/MClientSession.h"
#include "messages/MClientRequest.h"
#include "messages/MClientReply.h"
#include "messages/MClientReconnect.h"
#include "messages/MClientFileCaps.h"

#include "messages/MMDSSlaveRequest.h"

#include "messages/MLock.h"

#include "messages/MDentryUnlink.h"

#include "events/EString.h"
#include "events/EUpdate.h"
#include "events/ESlaveUpdate.h"
#include "events/ESession.h"
#include "events/EOpen.h"

#include "include/filepath.h"
#include "common/Timer.h"
#include "common/Logger.h"
#include "common/LogType.h"

#include <errno.h>
#include <fcntl.h>

#include <list>
#include <iostream>
using namespace std;

#include "config.h"

#define  dout(l)    if (l<=g_conf.debug || l <= g_conf.debug_mds) *_dout << dbeginl << g_clock.now() << " mds" << mds->get_nodeid() << ".server "
#define  derr(l)    if (l<=g_conf.debug || l <= g_conf.debug_mds) *_derr << dbeginl << g_clock.now() << " mds" << mds->get_nodeid() << ".server "


void Server::reopen_logger(utime_t start, bool append)
{
  static LogType mdserver_logtype;
  static bool didit = false;
  if (!didit) {
    didit = true;
    mdserver_logtype.add_inc("hcreq"); // handle client req
    mdserver_logtype.add_inc("hsreq"); // slave
    mdserver_logtype.add_inc("hcsess");    // client session
    mdserver_logtype.add_inc("dcreq"); // dispatch client req
    mdserver_logtype.add_inc("dsreq"); // slave
  }

  if (logger) 
    delete logger;

  // logger
  char name[80];
  sprintf(name, "mds%d.server", mds->get_nodeid());
  logger = new Logger(name, &mdserver_logtype, append);
  logger->set_start(start);
}


void Server::dispatch(Message *m) 
{
  switch (m->get_type()) {
  case CEPH_MSG_CLIENT_RECONNECT:
    handle_client_reconnect((MClientReconnect*)m);
    return;
  }

  // active?
  if (!mds->is_active() && !mds->is_stopping()) {
    dout(3) << "not active yet, waiting" << dendl;
    mds->wait_for_active(new C_MDS_RetryMessage(mds, m));
    return;
  }

  switch (m->get_type()) {
  case CEPH_MSG_CLIENT_SESSION:
    handle_client_session((MClientSession*)m);
    delete m;
    return;
  case CEPH_MSG_CLIENT_REQUEST:
    handle_client_request((MClientRequest*)m);
    return;
  case MSG_MDS_SLAVE_REQUEST:
    handle_slave_request((MMDSSlaveRequest*)m);
    return;
  }

  dout(1) << "server unknown message " << m->get_type() << dendl;
  assert(0);
}



// ----------------------------------------------------------
// SESSION management

class C_MDS_session_finish : public Context {
  MDS *mds;
  Session *session;
  bool open;
  version_t cmapv;
public:
  C_MDS_session_finish(MDS *m, Session *se, bool s, version_t mv) :
    mds(m), session(se), open(s), cmapv(mv) { }
  void finish(int r) {
    assert(r == 0);
    mds->server->_session_logged(session, open, cmapv);
  }
};


void Server::handle_client_session(MClientSession *m)
{
  version_t pv;
  Session *session = mds->sessionmap.get_session(m->get_source());

  dout(3) << "handle_client_session " << *m << " from " << m->get_source() << dendl;
  assert(m->get_source().is_client()); // should _not_ come from an mds!

  switch (m->op) {
  case CEPH_SESSION_REQUEST_OPEN:
    if (session && (session->is_opening() || session->is_open())) {
      dout(10) << "already open|opening, dropping this req" << dendl;
      return;
    }
    assert(!session);  // ?
    session = mds->sessionmap.get_or_add_session(m->get_source_inst());
    mds->sessionmap.set_state(session, Session::STATE_OPENING);
    mds->sessionmap.touch_session(session);
    pv = ++mds->sessionmap.projected;
    mdlog->submit_entry(new ESession(m->get_source_inst(), true, pv),
			new C_MDS_session_finish(mds, session, true, pv));
    break;

  case CEPH_SESSION_REQUEST_RENEWCAPS:
    if (!session) {
      dout(10) << "dne, dropping this req" << dendl;
      return;
    }
    mds->sessionmap.touch_session(session);
    mds->messenger->send_message(new MClientSession(CEPH_SESSION_RENEWCAPS, m->stamp), session->inst);
    break;

  case CEPH_SESSION_REQUEST_RESUME:
    if (!session) {
      dout(10) << "dne, replying with close" << dendl;
      mds->messenger->send_message(new MClientSession(CEPH_SESSION_CLOSE), m->get_source_inst());      
      return;
    }
    if (!session->is_stale()) {
      dout(10) << "hmm, got request_resume on non-stale session for " << session->inst << dendl;
      assert(0);
      return;
    }
    mds->sessionmap.set_state(session, Session::STATE_OPEN);
    mds->sessionmap.touch_session(session);
    mds->locker->resume_stale_caps(session);
    mds->messenger->send_message(new MClientSession(CEPH_SESSION_RESUME, m->stamp), session->inst);
    break;

  case CEPH_SESSION_REQUEST_CLOSE:
    if (!session || session->is_closing()) {
      dout(10) << "already closing|dne, dropping this req" << dendl;
      return;
    }
    if (m->seq < session->get_push_seq()) {
      dout(10) << "old push seq " << m->seq << " < " << session->get_push_seq() 
	       << ", dropping" << dendl;
      return;
    }
    assert(m->seq == session->get_push_seq());
    mds->sessionmap.set_state(session, Session::STATE_CLOSING);
    pv = ++mds->sessionmap.projected;
    mdlog->submit_entry(new ESession(m->get_source_inst(), false, pv),
			new C_MDS_session_finish(mds, session, false, pv));
    break;

  default:
    assert(0);
  }
}

void Server::_session_logged(Session *session, bool open, version_t pv)
{
  dout(10) << "_session_logged " << session->inst << " " << (open ? "open":"close")
	   << " " << pv << dendl;

  // apply
  if (open) {
    assert(session->is_opening());
    mds->sessionmap.set_state(session, Session::STATE_OPEN);
    mds->messenger->send_message(new MClientSession(CEPH_SESSION_OPEN), session->inst);
  } else if (session->is_closing() || session->is_stale_closing()) {
    // kill any lingering capabilities
    while (!session->caps.empty()) {
      Capability *cap = session->caps.front();
      CInode *in = cap->get_inode();
      dout(10) << " killing capability on " << *in << dendl;
      in->remove_client_cap(session->inst.name.num());
    }

    if (session->is_closing())
      mds->messenger->send_message(new MClientSession(CEPH_SESSION_CLOSE), session->inst);
    else if (session->is_stale_closing())
      mds->messenger->mark_down(session->inst.addr); // kill connection
    mds->sessionmap.remove_session(session);
  } else {
    // close must have been canceled (by an import?) ...
    assert(!open);
  }
  mds->sessionmap.version++;  // noop
}

void Server::prepare_force_open_sessions(map<int,entity_inst_t>& cm)
{
  version_t pv = ++mds->sessionmap.projected;
  dout(10) << "prepare_force_open_sessions " << pv 
	   << " on " << cm.size() << " clients"
	   << dendl;
  for (map<int,entity_inst_t>::iterator p = cm.begin(); p != cm.end(); ++p) {
    Session *session = mds->sessionmap.get_or_add_session(p->second);
    if (session->is_undef() || session->is_closing())
      mds->sessionmap.set_state(session, Session::STATE_OPENING);
  }
}

void Server::finish_force_open_sessions(map<int,entity_inst_t>& cm)
{
  /*
   * FIXME: need to carefully consider the race conditions between a
   * client trying to close a session and an MDS doing an import
   * trying to force open a session...  
   */
  dout(10) << "finish_force_open_sessions on " << cm.size() << " clients,"
	   << " v " << mds->sessionmap.version << " -> " << (mds->sessionmap.version+1) << dendl;
  for (map<int,entity_inst_t>::iterator p = cm.begin(); p != cm.end(); ++p) {
    Session *session = mds->sessionmap.get_session(p->second.name);
    assert(session);
    if (session->is_opening()) {
      dout(10) << "force_open_sessions opening " << session->inst << dendl;
      mds->sessionmap.set_state(session, Session::STATE_OPEN);
      mds->messenger->send_message(new MClientSession(CEPH_SESSION_OPEN), session->inst);
    }
  }
  mds->sessionmap.version++;
}


void Server::terminate_sessions()
{
  dout(2) << "terminate_sessions" << dendl;

  // kill them off.  clients will retry etc.
  set<Session*> sessions;
  mds->sessionmap.get_client_session_set(sessions);
  for (set<Session*>::const_iterator p = sessions.begin();
       p != sessions.end();
       ++p) {
    Session *session = *p;
    if (session->is_closing()) continue;
    mds->sessionmap.set_state(session, Session::STATE_CLOSING);
    version_t pv = ++mds->sessionmap.projected;
    mdlog->submit_entry(new ESession(session->inst, false, pv),
			new C_MDS_session_finish(mds, session, false, pv));
  }
}


void Server::find_idle_sessions()
{
  dout(10) << "find_idle_sessions" << dendl;
  
  // stale
  utime_t now = g_clock.now();
  utime_t cutoff = now;
  cutoff -= g_conf.mds_cap_timeout;  
  while (1) {
    Session *session = mds->sessionmap.get_oldest_session(Session::STATE_OPEN);
    if (!session) break;
    dout(20) << "laggiest active session is " << session->inst << dendl;
    if (session->last_cap_renew >= cutoff) {
      dout(20) << "laggiest active session is " << session->inst << " and sufficiently new (" 
	       << session->last_cap_renew << ")" << dendl;
      break;
    }

    dout(10) << "new stale session " << session->inst << " last " << session->last_cap_renew << dendl;
    mds->sessionmap.set_state(session, Session::STATE_STALE);
    mds->locker->revoke_stale_caps(session);
    mds->messenger->send_message(new MClientSession(CEPH_SESSION_STALE, session->get_push_seq()),
				 session->inst);
  }

  // dead
  cutoff = now;
  cutoff -= g_conf.mds_session_autoclose;
  while (1) {
    Session *session = mds->sessionmap.get_oldest_session(Session::STATE_STALE);
    if (!session) break;
    assert(session->is_stale());
    dout(20) << "oldest stale session is " << session->inst << dendl;
    if (session->last_cap_renew >= cutoff) {
      dout(20) << "oldest stale session is " << session->inst << " and sufficiently new (" 
	       << session->last_cap_renew << ")" << dendl;
      break;
    }

    dout(10) << "autoclosing stale session " << session->inst << " last " << session->last_cap_renew << dendl;
    mds->sessionmap.set_state(session, Session::STATE_STALE_CLOSING);
    version_t pv = ++mds->sessionmap.projected;
    mdlog->submit_entry(new ESession(session->inst, false, pv),
			new C_MDS_session_finish(mds, session, false, pv));
  }

}


void Server::reconnect_clients()
{
  mds->sessionmap.get_client_set(client_reconnect_gather);

  if (client_reconnect_gather.empty()) {
    dout(7) << "reconnect_clients -- no sessions, doing nothing." << dendl;
    reconnect_gather_finish();
    return;
  }
  
  dout(7) << "reconnect_clients -- sending mdsmap to clients with sessions" << dendl;
  mds->bcast_mds_map();  // send mdsmap to all client sessions
  reconnect_start = g_clock.now();
  dout(1) << "reconnect_clients -- " << client_reconnect_gather.size() << " sessions" << dendl;
}

void Server::handle_client_reconnect(MClientReconnect *m)
{
  dout(7) << "handle_client_reconnect " << m->get_source() << dendl;
  int from = m->get_source().num();
  Session *session = mds->sessionmap.get_session(m->get_source());

  if (m->closed) {
    dout(7) << " client had no session, removing from session map" << dendl;
    assert(session);  // ?
    version_t pv = ++mds->sessionmap.projected;
    mdlog->submit_entry(new ESession(session->inst, false, pv),
			new C_MDS_session_finish(mds, session, false, pv));
  } else {
    
    // caps
    for (map<inodeno_t, inode_caps_reconnect_t>::iterator p = m->inode_caps.begin();
	 p != m->inode_caps.end();
	 ++p) {
      CInode *in = mdcache->get_inode(p->first);
      if (in && in->is_auth()) {
	// we recovered it, and it's ours.  take note.
	dout(15) << "open caps on " << *in << dendl;
	in->reconnect_cap(from, p->second, session->caps);
	reconnected_caps.insert(in);
	continue;
      }
      
      filepath path = m->inode_path[p->first];
      if ((in && !in->is_auth()) ||
	  !mds->mdcache->path_is_mine(path)) {
	// not mine.
	dout(0) << "non-auth " << p->first << " " << m->inode_path[p->first]
		<< ", will pass off to authority" << dendl;
	
	// mark client caps stale.
	inode_t fake_inode;
	memset(&fake_inode, 0, sizeof(fake_inode));
	fake_inode.ino = p->first;
	MClientFileCaps *stale = new MClientFileCaps(CEPH_CAP_OP_EXPORT,
						     fake_inode, 
						     0,
						     0,                // doesn't matter.
						     p->second.wanted); // doesn't matter.
	mds->send_message_client(stale, m->get_source_inst());

	// add to cap export list.
	mdcache->rejoin_export_caps(p->first, m->inode_path[p->first], from, p->second);
      } else {
	// mine.  fetch later.
	dout(0) << "missing " << p->first << " " << m->inode_path[p->first]
		<< " (mine), will load later" << dendl;
	mdcache->rejoin_recovered_caps(p->first, m->inode_path[p->first], from, p->second, 
				       -1);  // "from" me.
      }
    }
  }

  // remove from gather set
  client_reconnect_gather.erase(from);
  if (client_reconnect_gather.empty()) reconnect_gather_finish();

  delete m;
}

/*
 * called by mdcache, late in rejoin (right before acks are sent)
 */
void Server::process_reconnected_caps()
{
  dout(10) << "process_reconnected_caps" << dendl;

  // adjust filelock state appropriately
  for (set<CInode*>::iterator p = reconnected_caps.begin();
       p != reconnected_caps.end();
       ++p) {
    CInode *in = *p;
    int issued = in->get_caps_issued();
    if (in->is_auth()) {
      // wr?
      if (issued & (CEPH_CAP_WR|CEPH_CAP_WRBUFFER)) {
	if (issued & (CEPH_CAP_RDCACHE|CEPH_CAP_WRBUFFER)) {
	  in->filelock.set_state(LOCK_LONER);
	} else {
	  in->filelock.set_state(LOCK_MIXED);
	}
      }
    } else {
      // note that client should perform stale/reap cleanup during reconnect.
      assert(issued & (CEPH_CAP_WR|CEPH_CAP_WRBUFFER) == 0);   // ????
      if (in->filelock.is_xlocked())
	in->filelock.set_state(LOCK_LOCK);
      else
	in->filelock.set_state(LOCK_SYNC);  // might have been lock, previously
    }
    dout(15) << " issued " << cap_string(issued)
	     << " chose " << in->filelock
	     << " on " << *in << dendl;
  }
  reconnected_caps.clear();  // clean up
}


void Server::client_reconnect_failure(int from) 
{
  dout(5) << "client_reconnect_failure on client" << from << dendl;
  if (mds->is_reconnect() &&
      client_reconnect_gather.count(from)) {
    client_reconnect_gather.erase(from);
    if (client_reconnect_gather.empty()) 
      reconnect_gather_finish();
  }
}

void Server::reconnect_gather_finish()
{
  dout(7) << "reconnect_gather_finish" << dendl;
  mds->reconnect_done();
}



/*******
 * some generic stuff for finishing off requests
 */


/*
 * send generic response (just and error code)
 */
void Server::reply_request(MDRequest *mdr, int r, CInode *tracei)
{
  reply_request(mdr, new MClientReply(mdr->client_request, r), tracei);
}


/*
 * send given reply
 * include a trace to tracei
 */
void Server::reply_request(MDRequest *mdr, MClientReply *reply, CInode *tracei) 
{
  MClientRequest *req = mdr->client_request;
  
  dout(10) << "reply_request " << reply->get_result() 
	   << " (" << strerror(-reply->get_result())
	   << ") " << *req << dendl;

  // note result code in session map?
  if (!req->is_idempotent())
    mdr->session->add_completed_request(mdr->reqid.tid);

  /*
  if (tracei && !tracei->hack_accessed) {
    tracei->hack_accessed = true;
    mds->logger->inc("newt");
    if (tracei->parent &&
	tracei->parent->dir->hack_num_accessed >= 0) {
      tracei->parent->dir->hack_num_accessed++;
      if (tracei->parent->dir->hack_num_accessed == 1)
	mds->logger->inc("dirt1");
      if (tracei->parent->dir->hack_num_accessed == 2)
	mds->logger->inc("dirt2");
      if (tracei->parent->dir->hack_num_accessed == 3)
	mds->logger->inc("dirt3");
      if (tracei->parent->dir->hack_num_accessed == 4)
	mds->logger->inc("dirt4");
      if (tracei->parent->dir->hack_num_accessed == 5)
	mds->logger->inc("dirt5");
    }
  }
  */

  // include trace
  if (tracei)
    reply->set_trace_dist( tracei, mds->get_nodeid() );
  
  reply->set_mdsmap_epoch(mds->mdsmap->get_epoch());
  
  // send reply
  if (req->get_client_inst().name.is_mds())
    delete reply;   // mds doesn't need a reply
  else
    messenger->send_message(reply, req->get_client_inst());
  
  // finish request
  mdcache->request_finish(mdr);

  if (tracei && 
      tracei->get_parent_dn() &&
      tracei->get_parent_dn()->is_remote())
    mdcache->eval_remote(tracei->get_parent_dn());
}





/***
 * process a client request
 */
void Server::handle_client_request(MClientRequest *req)
{
  dout(4) << "handle_client_request " << *req << dendl;

  if (logger) logger->inc("hcreq");

  if (!mds->is_active() &&
      !(mds->is_stopping() && req->get_client_inst().name.is_mds())) {
    dout(5) << " not active (or stopping+mds), discarding request." << dendl;
    delete req;
    return;
  }
  
  if (!mdcache->get_root()) {
    dout(5) << "need to open root" << dendl;
    mdcache->open_root(new C_MDS_RetryMessage(mds, req));
    return;
  }

  // active session?
  Session *session = 0;
  if (req->get_client_inst().name.is_client()) {
    session = mds->sessionmap.get_session(req->get_client_inst().name);
    if (!session) {
      dout(5) << "no session for " << req->get_client_inst().name << ", dropping" << dendl;
      delete req;
      return;
    }
  }

  // old mdsmap?
  if (req->get_mdsmap_epoch() < mds->mdsmap->get_epoch()) {
    // send it?  hrm, this isn't ideal; they may get a lot of copies if
    // they have a high request rate.
  }

  // okay, i want
  CInode *ref = 0;

  // retry?
  if (req->get_retry_attempt()) {
    assert(session);
    if (session->have_completed_request(req->get_reqid().tid)) {
      dout(5) << "already completed " << req->get_reqid() << dendl;
      mds->messenger->send_message(new MClientReply(req, 0), req->get_client_inst());
      delete req;
      return;
    }
  }
  // trim completed_request list
  if (req->get_oldest_client_tid() > 0) {
    dout(15) << " oldest_client_tid=" << req->get_oldest_client_tid() << dendl;
    session->trim_completed_requests(req->get_oldest_client_tid());
  }

  // register + dispatch
  MDRequest *mdr = mdcache->request_start(req);
  if (!mdr) return;
  mdr->session = session;

  if (ref) {
    dout(10) << "inode op on ref " << *ref << dendl;
    mdr->ref = ref;
    mdr->pin(ref);
  }

  dispatch_client_request(mdr);
  return;
}


void Server::dispatch_client_request(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;

  if (logger) logger->inc("dcreq");

  if (mdr->ref) {
    dout(7) << "dispatch_client_request " << *req << " ref " << *mdr->ref << dendl;
  } else {
    dout(7) << "dispatch_client_request " << *req << dendl;
  }

  // we shouldn't be waiting on anyone.
  assert(mdr->more()->waiting_on_slave.empty());
  
  switch (req->get_op()) {

    // inodes ops.
  case CEPH_MDS_OP_STAT:
  case CEPH_MDS_OP_LSTAT:
  case CEPH_MDS_OP_FSTAT:
    handle_client_stat(mdr);
    break;
  case CEPH_MDS_OP_UTIME:
    handle_client_utime(mdr);
    break;
  case CEPH_MDS_OP_CHMOD:
    handle_client_chmod(mdr);
    break;
  case CEPH_MDS_OP_CHOWN:
    handle_client_chown(mdr);
    break;
  case CEPH_MDS_OP_TRUNCATE:
    handle_client_truncate(mdr);
    break;
  case CEPH_MDS_OP_READDIR:
    handle_client_readdir(mdr);
    break;
  case CEPH_MDS_OP_FSYNC:
    //handle_client_fsync(req);
    break;

    // funky.
  case CEPH_MDS_OP_OPEN:
    if ((req->head.args.open.flags & O_CREAT) == 0) {
      handle_client_open(mdr);
      break;
    }
  case CEPH_MDS_OP_CREATE:
    handle_client_openc(mdr);
    break;

    // namespace.
    // no prior locks.
  case CEPH_MDS_OP_MKNOD:
    handle_client_mknod(mdr);
    break;
  case CEPH_MDS_OP_LINK:
    handle_client_link(mdr);
    break;
  case CEPH_MDS_OP_UNLINK:
  case CEPH_MDS_OP_RMDIR:
    handle_client_unlink(mdr);
    break;
  case CEPH_MDS_OP_RENAME:
    handle_client_rename(mdr);
    break;
  case CEPH_MDS_OP_MKDIR:
    handle_client_mkdir(mdr);
    break;
  case CEPH_MDS_OP_SYMLINK:
    handle_client_symlink(mdr);
    break;


  default:
    dout(1) << " unknown client op " << req->get_op() << dendl;
    assert(0);
  }
}


// ---------------------------------------
// SLAVE REQUESTS

void Server::handle_slave_request(MMDSSlaveRequest *m)
{
  dout(4) << "handle_slave_request " << m->get_reqid() << " from " << m->get_source() << dendl;
  int from = m->get_source().num();

  if (logger) logger->inc("hsreq");

  // reply?
  if (m->is_reply()) {

    switch (m->get_op()) {
    case MMDSSlaveRequest::OP_XLOCKACK:
      {
	// identify lock, master request
	SimpleLock *lock = mds->locker->get_lock(m->get_lock_type(),
						 m->get_object_info());
	MDRequest *mdr = mdcache->request_get(m->get_reqid());
	mdr->more()->slaves.insert(from);
	dout(10) << "got remote xlock on " << *lock << " on " << *lock->get_parent() << dendl;
	mdr->xlocks.insert(lock);
	mdr->locks.insert(lock);
	lock->get_xlock(mdr);
	lock->finish_waiters(SimpleLock::WAIT_REMOTEXLOCK);
      }
      break;

    case MMDSSlaveRequest::OP_AUTHPINACK:
      {
	MDRequest *mdr = mdcache->request_get(m->get_reqid());
	handle_slave_auth_pin_ack(mdr, m);
      }
      break;

    case MMDSSlaveRequest::OP_LINKPREPACK:
      {
	MDRequest *mdr = mdcache->request_get(m->get_reqid());
	handle_slave_link_prep_ack(mdr, m);
      }
      break;

    case MMDSSlaveRequest::OP_RENAMEPREPACK:
      {
	MDRequest *mdr = mdcache->request_get(m->get_reqid());
	handle_slave_rename_prep_ack(mdr, m);
      }
      break;

    default:
      assert(0);
    }

    // done with reply.
    delete m;
    return;

  } else {
    // am i a new slave?
    MDRequest *mdr;
    if (mdcache->have_request(m->get_reqid())) {
      // existing?
      mdr = mdcache->request_get(m->get_reqid());
      if (mdr->slave_to_mds != from) {   // may not even be a slave! (e.g. forward race)
	dout(10) << "local request " << *mdr << " not slave to mds" << from
		 << ", ignoring " << *m << dendl;
	delete m;
	return;
      }
    } else {
      // new?
      if (m->get_op() == MMDSSlaveRequest::OP_FINISH) {
	dout(10) << "missing slave request for " << m->get_reqid() 
		 << " OP_FINISH, must have lost race with a forward" << dendl;
	delete m;
	return;
      }
      mdr = mdcache->request_start_slave(m->get_reqid(), m->get_source().num());
    }
    assert(mdr->slave_request == 0);     // only one at a time, please!  
    mdr->slave_request = m;
    
    dispatch_slave_request(mdr);
  }
}

void Server::dispatch_slave_request(MDRequest *mdr)
{
  dout(7) << "dispatch_slave_request " << *mdr << " " << *mdr->slave_request << dendl;

  if (mdr->aborted) {
    dout(7) << " abort flag set, finishing" << dendl;
    mdcache->request_finish(mdr);
    return;
  }

  if (logger) logger->inc("dsreq");

  switch (mdr->slave_request->get_op()) {
  case MMDSSlaveRequest::OP_XLOCK:
    {
      // identify object
      SimpleLock *lock = mds->locker->get_lock(mdr->slave_request->get_lock_type(),
					       mdr->slave_request->get_object_info());

      if (lock && lock->get_parent()->is_auth()) {
	// xlock.
	// use acquire_locks so that we get auth_pinning.
	set<SimpleLock*> rdlocks;
	set<SimpleLock*> wrlocks;
	set<SimpleLock*> xlocks = mdr->xlocks;
	xlocks.insert(lock);
	
	if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
	  return;
	
	// ack
	MMDSSlaveRequest *r = new MMDSSlaveRequest(mdr->reqid, MMDSSlaveRequest::OP_XLOCKACK);
	r->set_lock_type(lock->get_type());
	lock->get_parent()->set_object_info(r->get_object_info());
	mds->send_message_mds(r, mdr->slave_request->get_source().num());
      } else {
	if (lock) {
	  dout(10) << "not auth for remote xlock attempt, dropping on " 
		   << *lock << " on " << *lock->get_parent() << dendl;
	} else {
	  dout(10) << "don't have object, dropping" << dendl;
	  assert(0); // can this happen, if we auth pinned properly.
	}
      }

      // done.
      delete mdr->slave_request;
      mdr->slave_request = 0;
    }
    break;

  case MMDSSlaveRequest::OP_UNXLOCK:
    {  
      SimpleLock *lock = mds->locker->get_lock(mdr->slave_request->get_lock_type(),
					       mdr->slave_request->get_object_info());
      assert(lock);
      mds->locker->xlock_finish(lock, mdr);
      
      // done.  no ack necessary.
      delete mdr->slave_request;
      mdr->slave_request = 0;
    }
    break;

  case MMDSSlaveRequest::OP_AUTHPIN:
    handle_slave_auth_pin(mdr);
    break;

  case MMDSSlaveRequest::OP_LINKPREP:
  case MMDSSlaveRequest::OP_UNLINKPREP:
    handle_slave_link_prep(mdr);
    break;

  case MMDSSlaveRequest::OP_RENAMEPREP:
    handle_slave_rename_prep(mdr);
    break;

  case MMDSSlaveRequest::OP_FINISH:
    // finish off request.
    mdcache->request_finish(mdr);
    break;

  default: 
    assert(0);
  }
}


void Server::handle_slave_auth_pin(MDRequest *mdr)
{
  dout(10) << "handle_slave_auth_pin " << *mdr << dendl;

  // build list of objects
  list<MDSCacheObject*> objects;
  bool fail = false;

  for (list<MDSCacheObjectInfo>::iterator p = mdr->slave_request->get_authpins().begin();
       p != mdr->slave_request->get_authpins().end();
       ++p) {
    MDSCacheObject *object = mdcache->get_object(*p);
    if (!object) {
      dout(10) << " don't have " << *p << dendl;
      fail = true;
      break;
    }

    objects.push_back(object);
  }
  
  // can we auth pin them?
  if (!fail) {
    for (list<MDSCacheObject*>::iterator p = objects.begin();
	 p != objects.end();
	 ++p) {
      if (!(*p)->is_auth()) {
	dout(10) << " not auth for " << **p << dendl;
	fail = true;
	break;
      }
      if (!mdr->is_auth_pinned(*p) &&
	  !(*p)->can_auth_pin()) {
	// wait
	dout(10) << " waiting for authpinnable on " << **p << dendl;
	(*p)->add_waiter(CDir::WAIT_UNFREEZE, new C_MDS_RetryRequest(mdcache, mdr));
	mdr->drop_local_auth_pins();
	return;
      }
    }
  }

  // auth pin!
  if (fail) {
    mdr->drop_local_auth_pins();  // just in case
  } else {
    for (list<MDSCacheObject*>::iterator p = objects.begin();
	 p != objects.end();
	 ++p) {
      dout(10) << "auth_pinning " << **p << dendl;
      mdr->auth_pin(*p);
    }
  }

  // ack!
  MMDSSlaveRequest *reply = new MMDSSlaveRequest(mdr->reqid, MMDSSlaveRequest::OP_AUTHPINACK);
  
  // return list of my auth_pins (if any)
  for (set<MDSCacheObject*>::iterator p = mdr->auth_pins.begin();
       p != mdr->auth_pins.end();
       ++p) {
    MDSCacheObjectInfo info;
    (*p)->set_object_info(info);
    reply->get_authpins().push_back(info);
  }

  mds->send_message_mds(reply, mdr->slave_to_mds);
  
  // clean up this request
  delete mdr->slave_request;
  mdr->slave_request = 0;
  return;
}

void Server::handle_slave_auth_pin_ack(MDRequest *mdr, MMDSSlaveRequest *ack)
{
  dout(10) << "handle_slave_auth_pin_ack on " << *mdr << " " << *ack << dendl;
  int from = ack->get_source().num();

  // added auth pins?
  set<MDSCacheObject*> pinned;
  for (list<MDSCacheObjectInfo>::iterator p = ack->get_authpins().begin();
       p != ack->get_authpins().end();
       ++p) {
    MDSCacheObject *object = mdcache->get_object(*p);
    assert(object);  // we pinned it
    dout(10) << " remote has pinned " << *object << dendl;
    if (!mdr->is_auth_pinned(object))
      mdr->remote_auth_pins.insert(object);
    pinned.insert(object);
  }

  // removed auth pins?
  set<MDSCacheObject*>::iterator p = mdr->remote_auth_pins.begin();
  while (p != mdr->remote_auth_pins.end()) {
    if ((*p)->authority().first == from &&
	pinned.count(*p) == 0) {
      dout(10) << " remote has unpinned " << **p << dendl;
      set<MDSCacheObject*>::iterator o = p;
      ++p;
      mdr->remote_auth_pins.erase(o);
    } else {
      ++p;
    }
  }
  
  // note slave
  mdr->more()->slaves.insert(from);

  // clear from waiting list
  assert(mdr->more()->waiting_on_slave.count(from));
  mdr->more()->waiting_on_slave.erase(from);

  // go again?
  if (mdr->more()->waiting_on_slave.empty())
    dispatch_client_request(mdr);
  else 
    dout(10) << "still waiting on slaves " << mdr->more()->waiting_on_slave << dendl;
}


// ---------------------------------------
// HELPERS


/** validate_dentry_dir
 *
 * verify that the dir exists and would own the dname.
 * do not check if the dentry exists.
 */
CDir *Server::validate_dentry_dir(MDRequest *mdr, CInode *diri, const string& dname)
{
  // make sure parent is a dir?
  if (!diri->is_dir()) {
    dout(7) << "validate_dentry_dir: not a dir" << dendl;
    reply_request(mdr, -ENOTDIR);
    return false;
  }

  // which dirfrag?
  frag_t fg = diri->pick_dirfrag(dname);
  CDir *dir = try_open_auth_dirfrag(diri, fg, mdr);
  if (!dir)
    return 0;

  // frozen?
  if (dir->is_frozen()) {
    dout(7) << "dir is frozen " << *dir << dendl;
    dir->add_waiter(CDir::WAIT_UNFREEZE, new C_MDS_RetryRequest(mdcache, mdr));
    return false;
  }
  
  return dir;
}


/** prepare_null_dentry
 * prepare a null (or existing) dentry in given dir. 
 * wait for any dn lock.
 */
CDentry* Server::prepare_null_dentry(MDRequest *mdr, CDir *dir, const string& dname, bool okexist)
{
  dout(10) << "prepare_null_dentry " << dname << " in " << *dir << dendl;
  assert(dir->is_auth());
  
  // does it already exist?
  CDentry *dn = dir->lookup(dname);
  if (dn) {
    if (dn->lock.is_xlocked_by_other(mdr)) {
      dout(10) << "waiting on xlocked dentry " << *dn << dendl;
      dn->lock.add_waiter(SimpleLock::WAIT_RD, new C_MDS_RetryRequest(mdcache, mdr));
      return 0;
    }

    if (!dn->is_null()) {
      // name already exists
      dout(10) << "dentry " << dname << " exists in " << *dir << dendl;
      if (!okexist) {
        reply_request(mdr, -EEXIST);
        return 0;
      }
    }

    return dn;
  }

  // make sure dir is complete
  if (!dir->is_complete()) {
    dout(7) << " incomplete dir contents for " << *dir << ", fetching" << dendl;
    dir->fetch(new C_MDS_RetryRequest(mdcache, mdr));
    return 0;
  }
  
  // create
  dn = dir->add_null_dentry(dname);
  dn->mark_new();
  dout(10) << "prepare_null_dentry added " << *dn << dendl;

  return dn;
}


/** prepare_new_inode
 *
 * create a new inode.  set c/m/atime.  hit dir pop.
 */
CInode* Server::prepare_new_inode(MDRequest *mdr, CDir *dir) 
{
  CInode *in = mdcache->create_inode();
  in->inode.uid = mdr->client_request->get_caller_uid();
  in->inode.gid = mdr->client_request->get_caller_gid();
  in->inode.ctime = in->inode.mtime = in->inode.atime = mdr->now;   // now
  dout(10) << "prepare_new_inode " << *in << dendl;

  return in;
}



CDir *Server::traverse_to_auth_dir(MDRequest *mdr, vector<CDentry*> &trace, filepath refpath)
{
  // figure parent dir vs dname
  if (refpath.depth() == 0) {
    dout(7) << "can't do that to root" << dendl;
    reply_request(mdr, -EINVAL);
    return 0;
  }
  string dname = refpath.last_dentry();
  refpath.pop_dentry();
  
  dout(10) << "traverse_to_auth_dir dirpath " << refpath << " dname " << dname << dendl;

  // traverse to parent dir
  int r = mdcache->path_traverse(mdr, mdr->client_request,
				 refpath, trace, true,
				 MDS_TRAVERSE_FORWARD);
  if (r > 0) return 0; // delayed
  if (r < 0) {
    reply_request(mdr, r);
    return 0;
  }

  // open inode
  CInode *diri;
  if (trace.empty())
    diri = mdcache->get_inode(refpath.get_ino());
  else
    diri = mdcache->get_dentry_inode(trace[trace.size()-1], mdr);
  if (!diri) 
    return 0; // opening inode.

  // is it an auth dir?
  CDir *dir = validate_dentry_dir(mdr, diri, dname);
  if (!dir)
    return 0; // forwarded or waiting for freeze

  dout(10) << "traverse_to_auth_dir " << *dir << dendl;
  return dir;
}



CInode* Server::rdlock_path_pin_ref(MDRequest *mdr, bool want_auth)
{
  // already got ref?
  if (mdr->ref) 
    return mdr->ref;

  MClientRequest *req = mdr->client_request;

  // traverse
  filepath refpath = req->get_filepath();
  vector<CDentry*> trace;
  int r = mdcache->path_traverse(mdr, req,
				 refpath, 
				 trace, req->follow_trailing_symlink(),
				 MDS_TRAVERSE_FORWARD);
  if (r > 0) return false; // delayed
  if (r < 0) {  // error
    reply_request(mdr, r);
    return 0;
  }

  // open ref inode
  CInode *ref = 0;
  if (trace.empty())
    ref = mdcache->get_inode(refpath.get_ino());
  else {
    CDentry *dn = trace[trace.size()-1];

    // if no inode (null or unattached remote), fw to dentry auth?
    if (want_auth && !dn->is_auth() &&
	(dn->is_null() ||
	 (dn->is_remote() && dn->inode))) {
      if (dn->is_ambiguous_auth()) {
	dout(10) << "waiting for single auth on " << *dn << dendl;
	dn->dir->add_waiter(CInode::WAIT_SINGLEAUTH, new C_MDS_RetryRequest(mdcache, mdr));
      } else {
	dout(10) << "fw to auth for " << *dn << dendl;
	mdcache->request_forward(mdr, dn->authority().first);
	return 0;
      }
    }

    // open ref inode
    ref = mdcache->get_dentry_inode(dn, mdr);
    if (!ref) return 0;
  }
  dout(10) << "ref is " << *ref << dendl;

  // fw to inode auth?
  if (want_auth && !ref->is_auth()) {
    if (ref->is_ambiguous_auth()) {
      dout(10) << "waiting for single auth on " << *ref << dendl;
      ref->add_waiter(CInode::WAIT_SINGLEAUTH, new C_MDS_RetryRequest(mdcache, mdr));
    } else {
      dout(10) << "fw to auth for " << *ref << dendl;
      mdcache->request_forward(mdr, ref->authority().first);
    }
    return 0;
  }

  // auth_pin?
  if (want_auth) {
    if (ref->is_frozen()) {
      dout(7) << "waiting for !frozen/authpinnable on " << *ref << dendl;
      ref->add_waiter(CInode::WAIT_UNFREEZE, new C_MDS_RetryRequest(mdcache, mdr));
      return 0;
    }
    mdr->auth_pin(ref);
  }

  // lock the path
  set<SimpleLock*> rdlocks, empty;

  for (int i=0; i<(int)trace.size(); i++) 
    rdlocks.insert(&trace[i]->lock);

  if (!mds->locker->acquire_locks(mdr, rdlocks, empty, empty))
    return 0;
  
  // set and pin ref
  mdr->pin(ref);
  mdr->ref = ref;

  // save the locked trace.
  mdr->trace.swap(trace);
  
  return ref;
}


/** rdlock_path_xlock_dentry
 * traverse path to the directory that could/would contain dentry.
 * make sure i am auth for that dentry, forward as necessary.
 * create null dentry in place (or use existing if okexist).
 * get rdlocks on traversed dentries, xlock on new dentry.
 */
CDentry* Server::rdlock_path_xlock_dentry(MDRequest *mdr, bool okexist, bool mustexist)
{
  MClientRequest *req = mdr->client_request;

  vector<CDentry*> trace;
  CDir *dir = traverse_to_auth_dir(mdr, trace, req->get_filepath());
  if (!dir) return 0;
  dout(10) << "rdlock_path_xlock_dentry dir " << *dir << dendl;

  // make sure we can auth_pin (or have already authpinned) dir
  if (dir->is_frozen()) {
    dout(7) << "waiting for !frozen/authpinnable on " << *dir << dendl;
    dir->add_waiter(CInode::WAIT_UNFREEZE, new C_MDS_RetryRequest(mdcache, mdr));
    return 0;
  }

  // make a null dentry?
  const string &dname = req->get_filepath().last_dentry();
  CDentry *dn;
  if (mustexist) {
    dn = dir->lookup(dname);

    // make sure dir is complete
    if (!dn && !dir->is_complete()) {
      dout(7) << " incomplete dir contents for " << *dir << ", fetching" << dendl;
      dir->fetch(new C_MDS_RetryRequest(mdcache, mdr));
      return 0;
    }

    // readable?
    if (dn && dn->lock.is_xlocked_by_other(mdr)) {
      dout(10) << "waiting on xlocked dentry " << *dn << dendl;
      dn->lock.add_waiter(SimpleLock::WAIT_RD, new C_MDS_RetryRequest(mdcache, mdr));
      return 0;
    }
      
    // exists?
    if (!dn || dn->is_null()) {
      dout(7) << "dentry " << dname << " dne in " << *dir << dendl;
      reply_request(mdr, -ENOENT);
      return 0;
    }    
  } else {
    dn = prepare_null_dentry(mdr, dir, dname, okexist);
    if (!dn) 
      return 0;
  }

  // -- lock --
  set<SimpleLock*> rdlocks, wrlocks, xlocks;

  for (int i=0; i<(int)trace.size(); i++) 
    rdlocks.insert(&trace[i]->lock);
  if (dn->is_null()) {
    xlocks.insert(&dn->lock);                 // new dn, xlock
    wrlocks.insert(&dn->dir->inode->dirlock); // also, wrlock on dir mtime
  } else
    rdlocks.insert(&dn->lock);  // existing dn, rdlock

  if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
    return 0;

  // save the locked trace.
  mdr->trace.swap(trace);

  return dn;
}





/**
 * try_open_auth_dirfrag -- open dirfrag, or forward to dirfrag auth
 *
 * @diri base indoe
 * @fg the exact frag we want
 * @mdr request
 */
CDir* Server::try_open_auth_dirfrag(CInode *diri, frag_t fg, MDRequest *mdr)
{
  CDir *dir = diri->get_dirfrag(fg);

  // not open and inode not mine?
  if (!dir && !diri->is_auth()) {
    int inauth = diri->authority().first;
    dout(7) << "try_open_auth_dirfrag: not open, not inode auth, fw to mds" << inauth << dendl;
    mdcache->request_forward(mdr, inauth);
    return 0;
  }

  // not open and inode frozen?
  if (!dir && diri->is_frozen_dir()) {
    dout(10) << "try_open_auth_dirfrag: dir inode is frozen, waiting " << *diri << dendl;
    assert(diri->get_parent_dir());
    diri->get_parent_dir()->add_waiter(CDir::WAIT_UNFREEZE, new C_MDS_RetryRequest(mdcache, mdr));
    return 0;
  }

  // invent?
  if (!dir) 
    dir = diri->get_or_open_dirfrag(mds->mdcache, fg);
 
  // am i auth for the dirfrag?
  if (!dir->is_auth()) {
    int auth = dir->authority().first;
    dout(7) << "try_open_auth_dirfrag: not auth for " << *dir
	    << ", fw to mds" << auth << dendl;
    mdcache->request_forward(mdr, auth);
    return 0;
  }

  return dir;
}



/** predirty_dn_diri
 * predirty the directory inode for a new dentry, if it is auth (and not root)
 * BUG: root inode doesn't get dirtied properly, currently.  blech.
 */
version_t Server::predirty_dn_diri(MDRequest *mdr, CDentry *dn, EMetaBlob *blob)
{
  version_t dirpv = 0;
  CInode *diri = dn->dir->inode;
  
  if (diri->is_base()) return 0;

  if (diri->is_auth()) {
    assert(mdr->wrlocks.count(&diri->dirlock));
    
    dirpv = diri->pre_dirty();
    dout(10) << "predirty_dn_diri ctime/mtime " << mdr->now << " pv " << dirpv << " on " << *diri << dendl;
    
    // predirty+journal
    inode_t *pi = diri->project_inode();
    if (dirpv) pi->version = dirpv;
    pi->ctime = pi->mtime = mdr->now;
    blob->add_dir_context(diri->get_parent_dn()->get_dir());
    blob->add_primary_dentry(diri->get_parent_dn(), true, 0, pi);
  } else {
    // journal the mtime change anyway.
    inode_t *ji = blob->add_primary_dentry(diri->get_parent_dn(), true);    
    ji->ctime = ji->mtime = mdr->now;

    dout(10) << "predirty_dn_diri (non-auth) ctime/mtime " << mdr->now << " on " << *diri << dendl;
    
    blob->add_dirtied_inode_mtime(diri->ino(), mdr->now);
    assert(mdr->ls);
    mdr->ls->dirty_inode_mtimes.push_back(&diri->xlist_dirty_inode_mtime);
  }
  
  return dirpv;
}

/** dirty_dn_diri
 * follow-up with actual dirty of inode after journal entry commits.
 */
void Server::dirty_dn_diri(MDRequest *mdr, CDentry *dn, version_t dirpv)
{
  CInode *diri = dn->dir->inode;
  
  if (diri->is_root()) return;

  if (dirpv) {
    // we journaled and predirtied.
    assert(diri->is_auth() && !diri->is_root());
    diri->pop_and_dirty_projected_inode(mdr->ls);
    dout(10) << "dirty_dn_diri ctime/mtime " << mdr->now << " v " << diri->inode.version << " on " << *diri << dendl;
  } else {
    // dirlock scatterlock will propagate the update.
    diri->inode.ctime = diri->inode.mtime = mdr->now;
    diri->dirlock.set_updated();
    dout(10) << "dirty_dn_diri (non-dirty) ctime/mtime " << mdr->now << " on " << *diri << dendl;
  }
}






// ===============================================================================
// STAT

void Server::handle_client_stat(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;
  CInode *ref = rdlock_path_pin_ref(mdr, false);
  if (!ref) return;

  // which inode locks do I want?
  /* note: this works because we include existing locks in our lists,
     and because all new locks are on inodes and sort to the right of
     the dentry rdlocks previous acquired by rdlock_path_pin_ref(). */
  set<SimpleLock*> rdlocks = mdr->rdlocks;
  set<SimpleLock*> wrlocks = mdr->wrlocks;
  set<SimpleLock*> xlocks = mdr->xlocks;
  
  int mask = req->head.args.stat.mask;
  if (mask & STAT_MASK_LINK) rdlocks.insert(&ref->linklock);
  if (mask & STAT_MASK_AUTH) rdlocks.insert(&ref->authlock);
  if (ref->is_file() && 
      mask & STAT_MASK_FILE) rdlocks.insert(&ref->filelock);
  if (ref->is_dir() &&
      mask & STAT_MASK_MTIME) rdlocks.insert(&ref->dirlock);

  if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
    return;

  mds->balancer->hit_inode(g_clock.now(), ref, META_POP_IRD,
			   mdr->client_request->get_client_inst().name.num());

  // reply
  dout(10) << "reply to stat on " << *req << dendl;
  MClientReply *reply = new MClientReply(req);
  reply_request(mdr, reply, ref);
}




// ===============================================================================
// INODE UPDATES


/* 
 * finisher for basic inode updates
 */
class C_MDS_inode_update_finish : public Context {
  MDS *mds;
  MDRequest *mdr;
  CInode *in;
public:
  C_MDS_inode_update_finish(MDS *m, MDRequest *r, CInode *i) :
    mds(m), mdr(r), in(i) { }
  void finish(int r) {
    assert(r == 0);

    // apply
    in->pop_and_dirty_projected_inode(mdr->ls);

    mds->balancer->hit_inode(mdr->now, in, META_POP_IWR);   

    // reply
    MClientReply *reply = new MClientReply(mdr->client_request, 0);
    reply->set_result(0);
    mds->server->reply_request(mdr, reply, in);
  }
};


// utime

void Server::handle_client_utime(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;
  CInode *cur = rdlock_path_pin_ref(mdr, true);
  if (!cur) return;

  if (cur->is_root()) {
    reply_request(mdr, -EINVAL);   // for now
    return;
  }

  // xlock inode
  set<SimpleLock*> rdlocks = mdr->rdlocks;
  set<SimpleLock*> wrlocks = mdr->wrlocks;
  set<SimpleLock*> xlocks = mdr->xlocks;
  xlocks.insert(&cur->filelock);
  if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
    return;

  // project update
  inode_t *pi = cur->project_inode();
  pi->mtime = req->head.args.utime.mtime;
  pi->atime = req->head.args.utime.atime;
  pi->version = cur->pre_dirty();
  pi->ctime = g_clock.real_now();

  // log + wait
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "utime");
  le->metablob.add_client_req(req->get_reqid());
  le->metablob.add_dir_context(cur->get_parent_dir());
  le->metablob.add_primary_dentry(cur->parent, true, 0, pi);
  
  mdlog->submit_entry(le, new C_MDS_inode_update_finish(mds, mdr, cur));
}


// chmod

void Server::handle_client_chmod(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;
  CInode *cur = rdlock_path_pin_ref(mdr, true);
  if (!cur) return;

  if (cur->is_root()) {
    reply_request(mdr, -EINVAL);   // for now
    return;
  }

  // write
  set<SimpleLock*> rdlocks = mdr->rdlocks;
  set<SimpleLock*> wrlocks = mdr->wrlocks;
  set<SimpleLock*> xlocks = mdr->xlocks;
  xlocks.insert(&cur->authlock);
  if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
    return;

  // project update
  inode_t *pi = cur->project_inode();
  pi->mode = 
    (pi->mode & ~04777) | 
    (req->head.args.chmod.mode & 04777);
  pi->version = cur->pre_dirty();
  pi->ctime = g_clock.real_now();

  // log + wait
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "chmod");
  le->metablob.add_client_req(req->get_reqid());
  le->metablob.add_dir_context(cur->get_parent_dir());
  le->metablob.add_primary_dentry(cur->parent, true, 0, pi);

  mdlog->submit_entry(le, new C_MDS_inode_update_finish(mds, mdr, cur));
}


// chown

void Server::handle_client_chown(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;
  CInode *cur = rdlock_path_pin_ref(mdr, true);
  if (!cur) return;

  if (cur->is_root()) {
    reply_request(mdr, -EINVAL);   // for now
    return;
  }

  // write
  set<SimpleLock*> rdlocks = mdr->rdlocks;
  set<SimpleLock*> wrlocks = mdr->wrlocks;
  set<SimpleLock*> xlocks = mdr->xlocks;
  xlocks.insert(&cur->authlock);
  if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
    return;

  // project update
  inode_t *pi = cur->project_inode();
  if (req->head.args.chown.uid != -1)
    pi->uid = req->head.args.chown.uid;
  if (req->head.args.chown.gid != -1)
    pi->gid = req->head.args.chown.gid;
  pi->version = cur->pre_dirty();
  pi->ctime = g_clock.real_now();
  
  // log + wait
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "chown");
  le->metablob.add_client_req(req->get_reqid());
  le->metablob.add_dir_context(cur->get_parent_dir());
  le->metablob.add_primary_dentry(cur->parent, true, 0, pi);
  
  mdlog->submit_entry(le);
  mdlog->wait_for_sync(new C_MDS_inode_update_finish(mds, mdr, cur));
}




// =================================================================
// DIRECTORY and NAMESPACE OPS

// READDIR

void Server::handle_client_readdir(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;
  CInode *diri = rdlock_path_pin_ref(mdr, false);
  if (!diri) return;

  // it's a directory, right?
  if (!diri->is_dir()) {
    // not a dir
    dout(10) << "reply to " << *req << " readdir -ENOTDIR" << dendl;
    reply_request(mdr, -ENOTDIR, diri);
    return;
  }

  // which frag?
  frag_t fg = req->head.args.readdir.frag;

  // does the frag exist?
  if (diri->dirfragtree[fg] != fg) {
    dout(10) << "frag " << fg << " doesn't appear in fragtree " << diri->dirfragtree << dendl;
    reply_request(mdr, -EAGAIN, diri);
    return;
  }
  
  CDir *dir = try_open_auth_dirfrag(diri, fg, mdr);
  if (!dir) return;

  // ok!
  assert(dir->is_auth());

  // check perm
  /*
  if (!mds->locker->inode_hard_rdlock_start(diri, mdr))
    return;
  mds->locker->inode_hard_rdlock_finish(diri, mdr);
  */

  if (!dir->is_complete()) {
    // fetch
    dout(10) << " incomplete dir contents for readdir on " << *dir << ", fetching" << dendl;
    dir->fetch(new C_MDS_RetryRequest(mdcache, mdr));
    return;
  }

  // build dir contents
  bufferlist dirbl, dnbl;
  DirStat::_encode(dirbl, dir, mds->get_nodeid());

  __u32 numfiles = 0;
  for (CDir::map_t::iterator it = dir->begin(); 
       it != dir->end(); 
       it++) {
    CDentry *dn = it->second;
    if (dn->is_null()) continue;

    CInode *in = dn->inode;

    // remote link?
    // better for the MDS to do the work, if we think the client will stat any of these files.
    if (dn->is_remote() && !in) {
      in = mdcache->get_inode(dn->get_remote_ino());
      if (in) {
	dn->link_remote(in);
      } else {
	mdcache->open_remote_ino(dn->get_remote_ino(),
				 mdr,
				 new C_MDS_RetryRequest(mdcache, mdr));

	// touch everything i _do_ have
	for (it = dir->begin(); 
	     it != dir->end(); 
	     it++) 
	  if (!it->second->is_null())
	    mdcache->lru.lru_touch(it->second);
	return;
      }
    }
    assert(in);

    dout(12) << "including inode " << *in << dendl;
    
    // add this dentry + inodeinfo
    ::_encode(it->first, dnbl);
    InodeStat::_encode(dnbl, in);
    numfiles++;

    // touch it
    mdcache->lru.lru_touch(dn);
  }
  ::_encode_simple(numfiles, dirbl);
  dirbl.claim_append(dnbl);
  
  // yay, reply
  MClientReply *reply = new MClientReply(req);
  reply->take_dir_items(dirbl);
  
  dout(10) << "reply to " << *req << " readdir " << numfiles << " files" << dendl;
  reply->set_result(0);

  // bump popularity.  NOTE: this doesn't quite capture it.
  mds->balancer->hit_dir(g_clock.now(), dir, META_POP_IRD, -1, numfiles);
  
  // reply
  reply_request(mdr, reply, diri);
}



// ------------------------------------------------

// MKNOD

class C_MDS_mknod_finish : public Context {
  MDS *mds;
  MDRequest *mdr;
  CDentry *dn;
  CInode *newi;
  version_t dirpv;
  version_t newdirpv;
public:
  C_MDS_mknod_finish(MDS *m, MDRequest *r, CDentry *d, CInode *ni, version_t dirpv_, version_t newdirpv_=0) :
    mds(m), mdr(r), dn(d), newi(ni),
    dirpv(dirpv_), newdirpv(newdirpv_) {}
  void finish(int r) {
    assert(r == 0);

    // link the inode
    dn->get_dir()->link_primary_inode(dn, newi);
    
    // dirty inode, dn, dir
    newi->mark_dirty(newi->inode.version + 1, mdr->ls);

    // mkdir?
    if (newdirpv) { 
      CDir *dir = newi->get_dirfrag(frag_t());
      assert(dir);
      dir->mark_dirty(newdirpv, mdr->ls);
    }

    // dir inode's mtime
    mds->server->dirty_dn_diri(mdr, dn, dirpv);
    
    // hit pop
    mds->balancer->hit_inode(mdr->now, newi, META_POP_IWR);
    //mds->balancer->hit_dir(mdr->now, dn->get_dir(), META_POP_DWR);

    // reply
    MClientReply *reply = new MClientReply(mdr->client_request, 0);
    reply->set_result(0);
    mds->server->reply_request(mdr, reply, newi);
  }
};


void Server::handle_client_mknod(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;
  
  CDentry *dn = rdlock_path_xlock_dentry(mdr, false, false);
  if (!dn) return;

  mdr->now = g_clock.real_now();
  CInode *newi = prepare_new_inode(mdr, dn->dir);
  assert(newi);

  // it's a file.
  newi->inode.rdev = req->head.args.mknod.rdev;
  newi->inode.mode = req->head.args.mknod.mode;
  newi->inode.version = dn->pre_dirty() - 1;
  
  dout(10) << "mknod mode " << newi->inode.mode << " rdev " << newi->inode.rdev << dendl;

  // prepare finisher
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "mknod");
  le->metablob.add_client_req(req->get_reqid());
  le->metablob.add_allocated_ino(newi->ino(), mds->idalloc->get_version());
  version_t dirpv = predirty_dn_diri(mdr, dn, &le->metablob);  // dir mtime too
  le->metablob.add_dir_context(dn->dir);
  le->metablob.add_primary_dentry(dn, true, newi, &newi->inode);
  
  // log + wait
  mdlog->submit_entry(le, new C_MDS_mknod_finish(mds, mdr, dn, newi, dirpv));
}



// MKDIR

void Server::handle_client_mkdir(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;
  
  CDentry *dn = rdlock_path_xlock_dentry(mdr, false, false);
  if (!dn) return;

  // new inode
  mdr->now = g_clock.real_now();
  CInode *newi = prepare_new_inode(mdr, dn->dir);  
  assert(newi);

  // it's a directory.
  newi->inode.mode = req->head.args.mkdir.mode;
  newi->inode.mode &= ~S_IFMT;
  newi->inode.mode |= S_IFDIR;
  newi->inode.layout = g_OSD_MDDirLayout;
  newi->inode.version = dn->pre_dirty() - 1;

  // ...and that new dir is empty.
  CDir *newdir = newi->get_or_open_dirfrag(mds->mdcache, frag_t());
  newdir->mark_complete();
  version_t newdirpv = newdir->pre_dirty();

  //if (mds->logger) mds->logger->inc("mkdir");

  // prepare finisher
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "mkdir");
  le->metablob.add_client_req(req->get_reqid());
  le->metablob.add_allocated_ino(newi->ino(), mds->idalloc->get_version());
  version_t dirpv = predirty_dn_diri(mdr, dn, &le->metablob);  // dir mtime too
  le->metablob.add_dir_context(dn->dir);
  le->metablob.add_primary_dentry(dn, true, newi, &newi->inode);
  le->metablob.add_dir(newdir, true, true); // dirty AND complete
  
  // log + wait
  mdlog->submit_entry(le, new C_MDS_mknod_finish(mds, mdr, dn, newi, dirpv, newdirpv));

  /* old export heuristic.  pbly need to reimplement this at some point.    
  if (
      diri->dir->is_auth() &&
      diri->dir->is_rep() &&
      newdir->is_auth() &&
      !newdir->is_hashing()) {
    int dest = rand() % mds->mdsmap->get_num_mds();
    if (dest != whoami) {
      dout(10) << "exporting new dir " << *newdir << " in replicated parent " << *diri->dir << dendl;
      mdcache->migrator->export_dir(newdir, dest);
    }
  }
  */
}


// SYMLINK

void Server::handle_client_symlink(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;
  
  CDentry *dn = rdlock_path_xlock_dentry(mdr, false, false);
  if (!dn) return;

  mdr->now = g_clock.real_now();

  CInode *newi = prepare_new_inode(mdr, dn->dir);
  assert(newi);

  // it's a symlink
  newi->inode.mode &= ~S_IFMT;
  newi->inode.mode |= S_IFLNK;
  newi->inode.mode |= 0777;     // ?
  newi->symlink = req->get_path2();
  newi->inode.size = newi->symlink.length();
  newi->inode.version = dn->pre_dirty() - 1;

  // prepare finisher
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "symlink");
  le->metablob.add_client_req(req->get_reqid());
  le->metablob.add_allocated_ino(newi->ino(), mds->idalloc->get_version());
  version_t dirpv = predirty_dn_diri(mdr, dn, &le->metablob);  // dir mtime too
  le->metablob.add_dir_context(dn->dir);
  le->metablob.add_primary_dentry(dn, true, newi, &newi->inode);
  
  // log + wait
  mdlog->submit_entry(le, new C_MDS_mknod_finish(mds, mdr, dn, newi, dirpv));
}





// LINK

void Server::handle_client_link(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;

  dout(7) << "handle_client_link " << req->get_filepath()
	  << " to " << req->get_filepath2()
	  << dendl;

  // traverse to dest dir, make sure it's ours.
  const filepath &linkpath = req->get_filepath();
  const string &dname = linkpath.last_dentry();
  vector<CDentry*> linktrace;
  CDir *dir = traverse_to_auth_dir(mdr, linktrace, linkpath);
  if (!dir) return;
  dout(7) << "handle_client_link link " << dname << " in " << *dir << dendl;
  
  // traverse to link target
  filepath targetpath = req->get_filepath2();
  dout(7) << "handle_client_link discovering target " << targetpath << dendl;
  vector<CDentry*> targettrace;
  int r = mdcache->path_traverse(mdr, req,
				 targetpath, targettrace, false,
				 MDS_TRAVERSE_DISCOVER);
  if (r > 0) return; // wait
  if (targettrace.empty()) r = -EINVAL; 
  if (r < 0) {
    reply_request(mdr, r);
    return;
  }
  
  // identify target inode
  CInode *targeti = targettrace[targettrace.size()-1]->inode;
  assert(targeti);

  // dir?
  dout(7) << "target is " << *targeti << dendl;
  if (targeti->is_dir()) {
    dout(7) << "target is a dir, failing..." << dendl;
    reply_request(mdr, -EINVAL);
    return;
  }
  
  // get/make null link dentry
  CDentry *dn = prepare_null_dentry(mdr, dir, dname, false);
  if (!dn) return;

  // create lock lists
  set<SimpleLock*> rdlocks, wrlocks, xlocks;

  for (int i=0; i<(int)linktrace.size(); i++)
    rdlocks.insert(&linktrace[i]->lock);
  xlocks.insert(&dn->lock);
  wrlocks.insert(&dn->dir->inode->dirlock);
  for (int i=0; i<(int)targettrace.size(); i++)
    rdlocks.insert(&targettrace[i]->lock);
  xlocks.insert(&targeti->linklock);

  if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
    return;

  mdr->done_locking = true;  // avoid wrlock moving target issues.
  
  // pick mtime
  if (mdr->now == utime_t())
    mdr->now = g_clock.real_now();

  // does the target need an anchor?
  if (targeti->is_auth()) {
    /*if (targeti->get_parent_dir() == dn->dir) {
      dout(7) << "target is in the same dirfrag, sweet" << dendl;
    } 
    else 
      */
    if (targeti->is_anchored() && !targeti->is_unanchoring()) {
      dout(7) << "target anchored already (nlink=" << targeti->inode.nlink << "), sweet" << dendl;
    } 
    else {
      dout(7) << "target needs anchor, nlink=" << targeti->inode.nlink << ", creating anchor" << dendl;
      
      mdcache->anchor_create(mdr, targeti,
			     new C_MDS_RetryRequest(mdcache, mdr));
      return;
    }
  }

  // go!

  // local or remote?
  if (targeti->is_auth()) 
    _link_local(mdr, dn, targeti);
  else 
    _link_remote(mdr, dn, targeti);
}


class C_MDS_link_local_finish : public Context {
  MDS *mds;
  MDRequest *mdr;
  CDentry *dn;
  CInode *targeti;
  version_t dnpv;
  version_t tipv;
  version_t dirpv;
public:
  C_MDS_link_local_finish(MDS *m, MDRequest *r, CDentry *d, CInode *ti, 
			  version_t dnpv_, version_t tipv_, version_t dirpv_) :
    mds(m), mdr(r), dn(d), targeti(ti),
    dnpv(dnpv_), tipv(tipv_), dirpv(dirpv_) { }
  void finish(int r) {
    assert(r == 0);
    mds->server->_link_local_finish(mdr, dn, targeti, dnpv, tipv, dirpv);
  }
};


void Server::_link_local(MDRequest *mdr, CDentry *dn, CInode *targeti)
{
  dout(10) << "_link_local " << *dn << " to " << *targeti << dendl;

  mdr->ls = mdlog->get_current_segment();

  // predirty NEW dentry
  version_t dnpv = dn->pre_dirty();
  version_t tipv = targeti->pre_dirty();
  
  // project inode update
  inode_t *pi = targeti->project_inode();
  pi->nlink++;
  pi->ctime = mdr->now;
  pi->version = tipv;

  // log + wait
  EUpdate *le = new EUpdate(mdlog, "link_local");
  le->metablob.add_client_req(mdr->reqid);
  version_t dirpv = predirty_dn_diri(mdr, dn, &le->metablob);   // dir inode's mtime
  le->metablob.add_dir_context(dn->get_dir());
  le->metablob.add_remote_dentry(dn, true, targeti->ino());  // new remote
  le->metablob.add_dir_context(targeti->get_parent_dir());
  le->metablob.add_primary_dentry(targeti->parent, true, targeti, pi);  // update old primary

  mdlog->submit_entry(le, new C_MDS_link_local_finish(mds, mdr, dn, targeti, dnpv, tipv, dirpv));
}

void Server::_link_local_finish(MDRequest *mdr, CDentry *dn, CInode *targeti,
				version_t dnpv, version_t tipv, version_t dirpv)
{
  dout(10) << "_link_local_finish " << *dn << " to " << *targeti << dendl;

  // link and unlock the NEW dentry
  dn->dir->link_remote_inode(dn, targeti->ino(), MODE_TO_DT(targeti->inode.mode));
  dn->mark_dirty(dnpv, mdr->ls);

  // target inode
  targeti->pop_and_dirty_projected_inode(mdr->ls);

  // new dentry dir mtime
  dirty_dn_diri(mdr, dn, dirpv);
  
  // bump target popularity
  mds->balancer->hit_inode(mdr->now, targeti, META_POP_IWR);
  //mds->balancer->hit_dir(mdr->now, dn->get_dir(), META_POP_DWR);

  // reply
  MClientReply *reply = new MClientReply(mdr->client_request, 0);
  reply_request(mdr, reply, dn->get_dir()->get_inode());  // FIXME: imprecise ref
}


// remote

class C_MDS_link_remote_finish : public Context {
  MDS *mds;
  MDRequest *mdr;
  CDentry *dn;
  CInode *targeti;
  version_t dpv;
  version_t dirpv;
public:
  C_MDS_link_remote_finish(MDS *m, MDRequest *r, CDentry *d, CInode *ti, version_t dirpv_) :
    mds(m), mdr(r), dn(d), targeti(ti),
    dpv(d->get_projected_version()),
    dirpv(dirpv_) { }
  void finish(int r) {
    assert(r == 0);
    mds->server->_link_remote_finish(mdr, dn, targeti, dpv, dirpv);
  }
};

void Server::_link_remote(MDRequest *mdr, CDentry *dn, CInode *targeti)
{
  dout(10) << "_link_remote " << *dn << " to " << *targeti << dendl;
    
  // 1. send LinkPrepare to dest (journal nlink++ prepare)
  int linkauth = targeti->authority().first;
  if (mdr->more()->witnessed.count(linkauth) == 0) {
    dout(10) << " targeti auth must prepare nlink++" << dendl;

    MMDSSlaveRequest *req = new MMDSSlaveRequest(mdr->reqid, MMDSSlaveRequest::OP_LINKPREP);
    targeti->set_object_info(req->get_object_info());
    req->now = mdr->now;
    mds->send_message_mds(req, linkauth);

    assert(mdr->more()->waiting_on_slave.count(linkauth) == 0);
    mdr->more()->waiting_on_slave.insert(linkauth);
    return;
  }
  dout(10) << " targeti auth has prepared nlink++" << dendl;

  // go.
  // predirty dentry
  dn->pre_dirty();
  
  // add to event
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "link_remote");
  le->metablob.add_client_req(mdr->reqid);
  version_t dirpv = predirty_dn_diri(mdr, dn, &le->metablob);   // dir inode's mtime
  le->metablob.add_dir_context(dn->get_dir());
  le->metablob.add_remote_dentry(dn, true, targeti->ino());  // new remote

  // mark committing (needed for proper recovery)
  mdr->committing = true;

  // log + wait
  mdlog->submit_entry(le, new C_MDS_link_remote_finish(mds, mdr, dn, targeti, dirpv));
}

void Server::_link_remote_finish(MDRequest *mdr, CDentry *dn, CInode *targeti,
				 version_t dpv, version_t dirpv)
{
  dout(10) << "_link_remote_finish " << *dn << " to " << *targeti << dendl;

  // link the new dentry
  dn->dir->link_remote_inode(dn, targeti->ino(), MODE_TO_DT(targeti->inode.mode));
  dn->mark_dirty(dpv, mdr->ls);

  // dir inode's mtime
  dirty_dn_diri(mdr, dn, dirpv);
  
  // bump target popularity
  mds->balancer->hit_inode(mdr->now, targeti, META_POP_IWR);
  //mds->balancer->hit_dir(mdr->now, dn->get_dir(), META_POP_DWR);

  // reply
  MClientReply *reply = new MClientReply(mdr->client_request, 0);
  reply_request(mdr, reply, dn->get_dir()->get_inode());  // FIXME: imprecise ref
}


// remote linking/unlinking

class C_MDS_SlaveLinkPrep : public Context {
  Server *server;
  MDRequest *mdr;
  CInode *targeti;
  utime_t old_ctime;
  bool inc;
public:
  C_MDS_SlaveLinkPrep(Server *s, MDRequest *r, CInode *t, utime_t oct, bool in) :
    server(s), mdr(r), targeti(t), old_ctime(oct), inc(in) { }
  void finish(int r) {
    assert(r == 0);
    server->_logged_slave_link(mdr, targeti, old_ctime, inc);
  }
};

void Server::handle_slave_link_prep(MDRequest *mdr)
{
  dout(10) << "handle_slave_link_prep " << *mdr 
	   << " on " << mdr->slave_request->get_object_info() 
	   << dendl;

  CInode *targeti = mdcache->get_inode(mdr->slave_request->get_object_info().ino);
  assert(targeti);
  dout(10) << "targeti " << *targeti << dendl;
  CDentry *dn = targeti->get_parent_dn();
  assert(dn->is_primary());

  mdr->now = mdr->slave_request->now;

  // anchor?
  if (mdr->slave_request->get_op() == MMDSSlaveRequest::OP_LINKPREP) {
    if (targeti->is_anchored() && !targeti->is_unanchoring()) {
      dout(7) << "target anchored already (nlink=" << targeti->inode.nlink << "), sweet" << dendl;
    } 
    else {
      dout(7) << "target needs anchor, nlink=" << targeti->inode.nlink << ", creating anchor" << dendl;
      mdcache->anchor_create(mdr, targeti,
			     new C_MDS_RetryRequest(mdcache, mdr));
      return;
    }
  }

  // journal it
  mdr->ls = mdlog->get_current_segment();
  ESlaveUpdate *le = new ESlaveUpdate(mdlog, "slave_link_prep", mdr->reqid, mdr->slave_to_mds, ESlaveUpdate::OP_PREPARE);

  inode_t *pi = dn->inode->project_inode();

  // rollback case
  le->rollback.add_dir_context(targeti->get_parent_dir());
  le->rollback.add_primary_dentry(dn, true, targeti, pi);  // update old primary

  // update journaled target inode
  bool inc;
  if (mdr->slave_request->get_op() == MMDSSlaveRequest::OP_LINKPREP) {
    inc = true;
    pi->nlink++;
  } else {
    inc = false;
    pi->nlink--;
  }
  utime_t old_ctime = pi->ctime;
  pi->ctime = mdr->now;
  pi->version = targeti->pre_dirty();

  dout(10) << " projected inode " << pi << " v " << pi->version << dendl;

  // commit case
  le->commit.add_dir_context(targeti->get_parent_dir());
  le->commit.add_primary_dentry(dn, true, targeti, pi);  // update old primary

  mdlog->submit_entry(le, new C_MDS_SlaveLinkPrep(this, mdr, targeti, old_ctime, inc));
}

class C_MDS_SlaveLinkCommit : public Context {
  Server *server;
  MDRequest *mdr;
  CInode *targeti;
  utime_t old_ctime;
  version_t old_version;
  bool inc;
public:
  C_MDS_SlaveLinkCommit(Server *s, MDRequest *r, CInode *t, utime_t oct, version_t ov, bool in) :
    server(s), mdr(r), targeti(t), old_ctime(oct), old_version(ov), inc(in) { }
  void finish(int r) {
    server->_commit_slave_link(mdr, r, targeti, 
			       old_ctime, old_version, inc);
  }
};

void Server::_logged_slave_link(MDRequest *mdr, CInode *targeti, utime_t old_ctime, bool inc) 
{
  dout(10) << "_logged_slave_link " << *mdr
	   << " inc=" << inc
	   << " " << *targeti << dendl;

  version_t old_version = targeti->inode.version;

  // update the target
  targeti->pop_and_dirty_projected_inode(mdr->ls);

  // hit pop
  mds->balancer->hit_inode(mdr->now, targeti, META_POP_IWR);

  // ack
  MMDSSlaveRequest *reply = new MMDSSlaveRequest(mdr->reqid, MMDSSlaveRequest::OP_LINKPREPACK);
  mds->send_message_mds(reply, mdr->slave_to_mds);
  
  // set up commit waiter
  mdr->more()->slave_commit = new C_MDS_SlaveLinkCommit(this, mdr, targeti, old_ctime, old_version, inc);

  // done.
  delete mdr->slave_request;
  mdr->slave_request = 0;
}


void Server::_commit_slave_link(MDRequest *mdr, int r, CInode *targeti, 
				utime_t old_ctime, version_t old_version, bool inc)
{  
  dout(10) << "_commit_slave_link " << *mdr
	   << " r=" << r
	   << " inc=" << inc
	   << " " << *targeti << dendl;

  ESlaveUpdate *le;
  if (r == 0) {
    // write a commit to the journal
    le  = new ESlaveUpdate(mdlog, "slave_link_commit", mdr->reqid, mdr->slave_to_mds, ESlaveUpdate::OP_COMMIT);
  } else {
    le  = new ESlaveUpdate(mdlog, "slave_link_rollback", mdr->reqid, mdr->slave_to_mds, ESlaveUpdate::OP_ROLLBACK);

    // -- rollback in memory --
    assert(targeti->inode.ctime == mdr->now);
    assert(targeti->projected_inode.empty());  // we're holding the version lock.

    targeti->inode.ctime = old_ctime;
    targeti->inode.version = old_version;
    if (inc) 
      targeti->inode.nlink++;
    else
      targeti->inode.nlink--;
  }

  mdlog->submit_entry(le);
}



void Server::handle_slave_link_prep_ack(MDRequest *mdr, MMDSSlaveRequest *m)
{
  dout(10) << "handle_slave_link_prep_ack " << *mdr 
	   << " " << *m << dendl;
  int from = m->get_source().num();

  // note slave
  mdr->more()->slaves.insert(from);
  
  // witnessed!
  assert(mdr->more()->witnessed.count(from) == 0);
  mdr->more()->witnessed.insert(from);
  
  // remove from waiting list
  assert(mdr->more()->waiting_on_slave.count(from));
  mdr->more()->waiting_on_slave.erase(from);

  assert(mdr->more()->waiting_on_slave.empty());

  dispatch_client_request(mdr);  // go again!
}





// UNLINK

void Server::handle_client_unlink(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;

  // traverse to path
  vector<CDentry*> trace;
  int r = mdcache->path_traverse(mdr, req, 
				 req->get_filepath(), trace, false,
				 MDS_TRAVERSE_FORWARD);
  if (r > 0) return;
  if (trace.empty()) r = -EINVAL;   // can't unlink root
  if (r < 0) {
    reply_request(mdr, r);
    return;
  }

  CDentry *dn = trace[trace.size()-1];
  assert(dn);
  
  // is it my dentry?
  if (!dn->is_auth()) {
    // fw to auth
    mdcache->request_forward(mdr, dn->authority().first);
    return;
  }

  // rmdir or unlink?
  bool rmdir = false;
  if (req->get_op() == CEPH_MDS_OP_RMDIR) rmdir = true;
  
  if (rmdir) {
    dout(7) << "handle_client_rmdir on " << *dn << dendl;
  } else {
    dout(7) << "handle_client_unlink on " << *dn << dendl;
  }

  // readable?
  if (dn->lock.is_xlocked_by_other(mdr)) {
    dout(10) << "waiting on xlocked dentry " << *dn << dendl;
    dn->lock.add_waiter(SimpleLock::WAIT_RD, new C_MDS_RetryRequest(mdcache, mdr));
    return;
  }

  // dn looks ok.

  // get/open inode.
  mdr->trace.swap(trace);
  CInode *in = mdcache->get_dentry_inode(dn, mdr);
  if (!in) return;
  dout(7) << "dn links to " << *in << dendl;

  // rmdir vs is_dir 
  if (in->is_dir()) {
    if (rmdir) {
      // do empty directory checks
      if (!_verify_rmdir(mdr, in))
	return;
    } else {
      dout(7) << "handle_client_unlink on dir " << *in << ", returning error" << dendl;
      reply_request(mdr, -EISDIR);
      return;
    }
  } else {
    if (rmdir) {
      // unlink
      dout(7) << "handle_client_rmdir on non-dir " << *in << ", returning error" << dendl;
      reply_request(mdr, -ENOTDIR);
      return;
    }
  }

  // lock
  set<SimpleLock*> rdlocks, wrlocks, xlocks;

  for (int i=0; i<(int)trace.size()-1; i++)
    rdlocks.insert(&trace[i]->lock);
  xlocks.insert(&dn->lock);
  wrlocks.insert(&dn->dir->inode->dirlock);
  xlocks.insert(&in->linklock);
  
  if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
    return;

  // yay!
  mdr->done_locking = true;  // avoid wrlock racing
  if (mdr->now == utime_t())
    mdr->now = g_clock.real_now();

  // get stray dn ready?
  CDentry *straydn = 0;
  if (dn->is_primary()) {
    straydn = mdcache->get_or_create_stray_dentry(dn->inode);
    mdr->pin(straydn);  // pin it.
    dout(10) << " straydn is " << *straydn << dendl;
    assert(straydn->is_null());

    if (!mdr->more()->dst_reanchor_atid &&
	dn->inode->is_anchored()) {
      dout(10) << "reanchoring to stray " << *dn->inode << dendl;
      vector<Anchor> trace;
      straydn->make_anchor_trace(trace, dn->inode);
      mds->anchorclient->prepare_update(dn->inode->ino(), trace, &mdr->more()->dst_reanchor_atid, 
					new C_MDS_RetryRequest(mdcache, mdr));
      return;
    }
  }

  // ok!
  if (dn->is_remote() && !dn->inode->is_auth()) 
    _unlink_remote(mdr, dn);
  else
    _unlink_local(mdr, dn, straydn);
}



class C_MDS_unlink_local_finish : public Context {
  MDS *mds;
  MDRequest *mdr;
  CDentry *dn;
  CDentry *straydn;
  version_t dnpv;  // deleted dentry
  version_t dirpv;
public:
  C_MDS_unlink_local_finish(MDS *m, MDRequest *r, CDentry *d, CDentry *sd,
			    version_t dirpv_) :
    mds(m), mdr(r), dn(d), straydn(sd),
    dnpv(d->get_projected_version()), dirpv(dirpv_) { }
  void finish(int r) {
    assert(r == 0);
    mds->server->_unlink_local_finish(mdr, dn, straydn, dnpv, dirpv);
  }
};


void Server::_unlink_local(MDRequest *mdr, CDentry *dn, CDentry *straydn)
{
  dout(10) << "_unlink_local " << *dn << dendl;

  // ok, let's do it.
  mdr->ls = mdlog->get_current_segment();

  // prepare log entry
  EUpdate *le = new EUpdate(mdlog, "unlink_local");
  le->metablob.add_client_req(mdr->reqid);

  version_t ipv = 0;  // dirty inode version
  inode_t *ji = 0;     // journaled projected inode
  if (dn->is_primary()) {
    // primary link.  add stray dentry.
    assert(straydn);
    ipv = straydn->pre_dirty(dn->inode->inode.version);
    le->metablob.add_dir_context(straydn->dir);
    ji = le->metablob.add_primary_dentry(straydn, true, dn->inode);
  } else {
    // remote link.  update remote inode.
    ipv = dn->inode->pre_dirty();
    le->metablob.add_dir_context(dn->inode->get_parent_dir());
    ji = le->metablob.add_primary_dentry(dn->inode->parent, true, dn->inode);
  }
  
  // update journaled target inode
  inode_t *pi = dn->inode->project_inode();
  pi->nlink--;
  pi->ctime = mdr->now;
  pi->version = ipv;
  *ji = *pi;  // copy into journal
  
  // the unlinked dentry
  dn->pre_dirty();
  version_t dirpv = predirty_dn_diri(mdr, dn, &le->metablob);
  le->metablob.add_dir_context(dn->get_dir());
  le->metablob.add_null_dentry(dn, true);

  if (mdr->more()->dst_reanchor_atid)
    le->metablob.add_anchor_transaction(mdr->more()->dst_reanchor_atid);

  // log + wait
  journal_opens();  // journal pending opens, just in case
  mdlog->submit_entry(le, new C_MDS_unlink_local_finish(mds, mdr, dn, straydn, 
							dirpv));
}

void Server::_unlink_local_finish(MDRequest *mdr, 
				  CDentry *dn, CDentry *straydn,
				  version_t dnpv, version_t dirpv) 
{
  dout(10) << "_unlink_local_finish " << *dn << dendl;

  // unlink main dentry
  CInode *in = dn->inode;
  dn->dir->unlink_inode(dn);

  // relink as stray?  (i.e. was primary link?)
  if (straydn) {
    dout(20) << " straydn is " << *straydn << dendl;
    straydn->dir->link_primary_inode(straydn, in);
  }

  // nlink--, dirty old dentry
  in->pop_and_dirty_projected_inode(mdr->ls);
  dn->mark_dirty(dnpv, mdr->ls);  

  // dir inode's mtime
  dirty_dn_diri(mdr, dn, dirpv);
  
  // share unlink news with replicas
  for (map<int,int>::iterator it = dn->replicas_begin();
       it != dn->replicas_end();
       it++) {
    dout(7) << "_unlink_local_finish sending MDentryUnlink to mds" << it->first << dendl;
    MDentryUnlink *unlink = new MDentryUnlink(dn->dir->dirfrag(), dn->name);
    if (straydn) {
      unlink->strayin = straydn->dir->inode->replicate_to(it->first);
      unlink->straydir = straydn->dir->replicate_to(it->first);
      unlink->straydn = straydn->replicate_to(it->first);
    }
    mds->send_message_mds(unlink, it->first);
  }
  
  // commit anchor update?
  if (mdr->more()->dst_reanchor_atid) 
    mds->anchorclient->commit(mdr->more()->dst_reanchor_atid, mdr->ls);

  // bump pop
  //mds->balancer->hit_dir(mdr->now, dn->dir, META_POP_DWR);

  // reply
  MClientReply *reply = new MClientReply(mdr->client_request, 0);
  reply_request(mdr, reply, dn->dir->get_inode());  // FIXME: imprecise ref
  
  // clean up?
  if (straydn)
    mdcache->eval_stray(straydn);

  // removing a new dn?
  dn->dir->try_remove_unlinked_dn(dn);
}


class C_MDS_unlink_remote_finish : public Context {
  MDS *mds;
  MDRequest *mdr;
  CDentry *dn;
  version_t dnpv;  // deleted dentry
  version_t dirpv;
public:
  C_MDS_unlink_remote_finish(MDS *m, MDRequest *r, CDentry *d, 
			     version_t dirpv_) :
    mds(m), mdr(r), dn(d), 
    dnpv(d->get_projected_version()), dirpv(dirpv_) { }
  void finish(int r) {
    assert(r == 0);
    mds->server->_unlink_remote_finish(mdr, dn, dnpv, dirpv);
  }
};

void Server::_unlink_remote(MDRequest *mdr, CDentry *dn) 
{
  dout(10) << "_unlink_remote " << *dn << " " << *dn->inode << dendl;

  // 1. send LinkPrepare to dest (journal nlink-- prepare)
  int inauth = dn->inode->authority().first;
  if (mdr->more()->witnessed.count(inauth) == 0) {
    dout(10) << " inode auth must prepare nlink--" << dendl;

    MMDSSlaveRequest *req = new MMDSSlaveRequest(mdr->reqid, MMDSSlaveRequest::OP_UNLINKPREP);
    dn->inode->set_object_info(req->get_object_info());
    req->now = mdr->now;
    mds->send_message_mds(req, inauth);

    assert(mdr->more()->waiting_on_slave.count(inauth) == 0);
    mdr->more()->waiting_on_slave.insert(inauth);
    return;
  }
  dout(10) << " inode auth has prepared nlink--" << dendl;

  // ok, let's do it.
  // prepare log entry
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "unlink_remote");
  le->metablob.add_client_req(mdr->reqid);

  // the unlinked dentry
  dn->pre_dirty();
  version_t dirpv = predirty_dn_diri(mdr, dn, &le->metablob);
  le->metablob.add_dir_context(dn->get_dir());
  le->metablob.add_null_dentry(dn, true);

  if (mdr->more()->dst_reanchor_atid)
    le->metablob.add_anchor_transaction(mdr->more()->dst_reanchor_atid);

  // finisher
  C_MDS_unlink_remote_finish *fin = new C_MDS_unlink_remote_finish(mds, mdr, dn, dirpv);
  
  journal_opens();  // journal pending opens, just in case
  
  // mark committing (needed for proper recovery)
  mdr->committing = true;

  // log + wait
  mdlog->submit_entry(le, fin);
}

void Server::_unlink_remote_finish(MDRequest *mdr, 
				   CDentry *dn, 
				   version_t dnpv, version_t dirpv) 
{
  dout(10) << "_unlink_remote_finish " << *dn << dendl;

  // unlink main dentry
  dn->dir->unlink_inode(dn);
  dn->mark_dirty(dnpv, mdr->ls);  // dirty old dentry

  // dir inode's mtime
  dirty_dn_diri(mdr, dn, dirpv);
    
  // share unlink news with replicas
  for (map<int,int>::iterator it = dn->replicas_begin();
       it != dn->replicas_end();
       it++) {
    dout(7) << "_unlink_remote_finish sending MDentryUnlink to mds" << it->first << dendl;
    MDentryUnlink *unlink = new MDentryUnlink(dn->dir->dirfrag(), dn->name);
    mds->send_message_mds(unlink, it->first);
  }

  // commit anchor update?
  if (mdr->more()->dst_reanchor_atid) 
    mds->anchorclient->commit(mdr->more()->dst_reanchor_atid, mdr->ls);

  //mds->balancer->hit_dir(mdr->now, dn->dir, META_POP_DWR);

  // reply
  MClientReply *reply = new MClientReply(mdr->client_request, 0);
  reply_request(mdr, reply, dn->dir->get_inode());  // FIXME: imprecise ref

  // removing a new dn?
  dn->dir->try_remove_unlinked_dn(dn);
}




/** _verify_rmdir
 *
 * verify that a directory is empty (i.e. we can rmdir it),
 * and make sure it is part of the same subtree (i.e. local)
 * so that rmdir will occur locally.
 *
 * @param in is the inode being rmdir'd.
 */
bool Server::_verify_rmdir(MDRequest *mdr, CInode *in)
{
  dout(10) << "_verify_rmdir " << *in << dendl;
  assert(in->is_auth());

  list<frag_t> frags;
  in->dirfragtree.get_leaves(frags);

  for (list<frag_t>::iterator p = frags.begin();
       p != frags.end();
       ++p) {
    CDir *dir = in->get_or_open_dirfrag(mdcache, *p);
    assert(dir);

    // dir looks empty but incomplete?
    if (dir->is_auth() &&
	dir->get_size() == 0 && 
	!dir->is_complete()) {
      dout(7) << "_verify_rmdir fetching incomplete dir " << *dir << dendl;
      dir->fetch(new C_MDS_RetryRequest(mdcache, mdr));
      return false;
    }
    
    // does the frag _look_ empty?
    if (dir->get_size()) {
      dout(10) << "_verify_rmdir still " << dir->get_size() << " items in frag " << *dir << dendl;
      reply_request(mdr, -ENOTEMPTY);
      return false;
    }
    
    // not dir auth?
    if (!dir->is_auth()) {
      dout(10) << "_verify_rmdir not auth for " << *dir << ", FIXME BUG" << dendl;
      reply_request(mdr, -ENOTEMPTY);
      return false;
    }
  }

  return true;
}
/*
      // export sanity check
      if (!in->is_auth()) {
        // i should be exporting this now/soon, since the dir is empty.
        dout(7) << "handle_client_rmdir dir is auth, but not inode." << dendl;
	mdcache->migrator->export_empty_import(in->dir);          
        in->dir->add_waiter(CDir::WAIT_UNFREEZE, new C_MDS_RetryRequest(mds, req, diri));
        return;
      }
*/




// ======================================================


class C_MDS_rename_finish : public Context {
  MDS *mds;
  MDRequest *mdr;
  CDentry *srcdn;
  CDentry *destdn;
  CDentry *straydn;
public:
  C_MDS_rename_finish(MDS *m, MDRequest *r,
		      CDentry *sdn, CDentry *ddn, CDentry *stdn) :
    mds(m), mdr(r),
    srcdn(sdn), destdn(ddn), straydn(stdn) { }
  void finish(int r) {
    assert(r == 0);
    mds->server->_rename_finish(mdr, srcdn, destdn, straydn);
  }
};


/** handle_client_rename
 *
 */
void Server::handle_client_rename(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;
  dout(7) << "handle_client_rename " << *req << dendl;

  // src+dest _must_ share commont root for locking to prevent orphans
  filepath destpath = req->get_filepath2();
  filepath srcpath = req->get_filepath();
  if (destpath.get_ino() != srcpath.get_ino()) {
    // error out for now; eventually, we should find the deepest common root
    derr(0) << "rename src + dst must share common root; fix client or fix me" << dendl;
    assert(0);
  }

  // traverse to dest dir (not dest)
  //  we do this FIRST, because the rename should occur on the 
  //  destdn's auth.
  const string &destname = destpath.last_dentry();
  vector<CDentry*> desttrace;
  CDir *destdir = traverse_to_auth_dir(mdr, desttrace, destpath);
  if (!destdir) return;  // fw or error out
  dout(10) << "dest will be " << destname << " in " << *destdir << dendl;
  assert(destdir->is_auth());

  // traverse to src
  vector<CDentry*> srctrace;
  int r = mdcache->path_traverse(mdr, req,
				 srcpath, srctrace, false,
				 MDS_TRAVERSE_DISCOVER);
  if (r > 0) return;
  if (srctrace.empty()) r = -EINVAL;  // can't rename root
  if (r < 0) {
    reply_request(mdr, r);
    return;
  }
  CDentry *srcdn = srctrace[srctrace.size()-1];
  dout(10) << " srcdn " << *srcdn << dendl;
  CInode *srci = mdcache->get_dentry_inode(srcdn, mdr);
  dout(10) << " srci " << *srci << dendl;
  mdr->pin(srci);

  // -- some sanity checks --
  // src == dest?
  if (srcdn->get_dir() == destdir && srcdn->name == destname) {
    dout(7) << "rename src=dest, noop" << dendl;
    reply_request(mdr, 0);
    return;
  }

  // dest a child of src?
  // e.g. mv /usr /usr/foo
  CDentry *pdn = destdir->inode->parent;
  while (pdn) {
    if (pdn == srcdn) {
      dout(7) << "cannot rename item to be a child of itself" << dendl;
      reply_request(mdr, -EINVAL);
      return;
    }
    pdn = pdn->dir->inode->parent;
  }


  // identify/create dest dentry
  CDentry *destdn = destdir->lookup(destname);
  if (destdn && destdn->lock.is_xlocked_by_other(mdr)) {
    destdn->lock.add_waiter(SimpleLock::WAIT_RD, new C_MDS_RetryRequest(mdcache, mdr));
    return;
  }

  CInode *oldin = 0;
  if (destdn && !destdn->is_null()) {
    //dout(10) << "dest dn exists " << *destdn << dendl;
    oldin = mdcache->get_dentry_inode(destdn, mdr);
    if (!oldin) return;
    dout(10) << " oldin " << *oldin << dendl;
    
    // mv /some/thing /to/some/existing_other_thing
    if (oldin->is_dir() && !srci->is_dir()) {
      reply_request(mdr, -EISDIR);
      return;
    }
    if (!oldin->is_dir() && srci->is_dir()) {
      reply_request(mdr, -ENOTDIR);
      return;
    }

    // non-empty dir?
    if (oldin->is_dir() && !_verify_rmdir(mdr, oldin))      
      return;
  }
  if (!destdn) {
    // mv /some/thing /to/some/non_existent_name
    destdn = prepare_null_dentry(mdr, destdir, destname);
    if (!destdn) return;
  }

  dout(10) << " destdn " << *destdn << dendl;

  // -- locks --
  set<SimpleLock*> rdlocks, wrlocks, xlocks;

  // rdlock sourcedir path, xlock src dentry
  for (int i=0; i<(int)srctrace.size()-1; i++) 
    rdlocks.insert(&srctrace[i]->lock);
  xlocks.insert(&srcdn->lock);
  wrlocks.insert(&srcdn->dir->inode->dirlock);
  /*
   * no, this causes problems if the dftlock is scattered...
   *  and what was i thinking anyway? 
   * rdlocks.insert(&srcdn->dir->inode->dirfragtreelock);  // rd lock on srci dirfragtree.
   */

  // rdlock destdir path, xlock dest dentry
  for (int i=0; i<(int)desttrace.size(); i++)
    rdlocks.insert(&desttrace[i]->lock);
  xlocks.insert(&destdn->lock);
  wrlocks.insert(&destdn->dir->inode->dirlock);

  // xlock versionlock on srci if remote?
  //  this ensures it gets safely remotely auth_pinned, avoiding deadlock;
  //  strictly speaking, having the slave node freeze the inode is 
  //  otherwise sufficient for avoiding conflicts with inode locks, etc.
  if (!srcdn->is_auth() && srcdn->is_primary())
    xlocks.insert(&srcdn->inode->versionlock);

  // xlock oldin (for nlink--)
  if (oldin) xlocks.insert(&oldin->linklock);
  
  if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
    return;
  
  // set done_locking flag, to avoid problems with wrlock moving auth target
  mdr->done_locking = true;

  // -- open all srcdn inode frags, if any --
  // we need these open so that auth can properly delegate from inode to dirfrags
  // after the inode is _ours_.
  if (srcdn->is_primary() && 
      !srcdn->is_auth() && 
      srci->is_dir()) {
    dout(10) << "srci is remote dir, setting stickydirs and opening all frags" << dendl;
    mdr->set_stickydirs(srci);

    list<frag_t> frags;
    srci->dirfragtree.get_leaves(frags);
    for (list<frag_t>::iterator p = frags.begin();
	 p != frags.end();
	 ++p) {
      CDir *dir = srci->get_dirfrag(*p);
      if (!dir) {
	dout(10) << " opening " << *p << " under " << *srci << dendl;
	mdcache->open_remote_dirfrag(srci, *p, new C_MDS_RetryRequest(mdcache, mdr));
	return;
      }
    }
  }

  // -- declare now --
  if (mdr->now == utime_t())
    mdr->now = g_clock.real_now();

  // -- create stray dentry? --
  CDentry *straydn = 0;
  if (destdn->is_primary()) {
    straydn = mdcache->get_or_create_stray_dentry(destdn->inode);
    mdr->pin(straydn);
    dout(10) << "straydn is " << *straydn << dendl;
  }

  // -- prepare witnesses --
  /*
   * NOTE: we use _all_ replicas as witnesses.
   * this probably isn't totally necessary (esp for file renames),
   * but if/when we change that, we have to make sure rejoin is
   * sufficiently robust to handle strong rejoins from survivors
   * with totally wrong dentry->inode linkage.
   * (currently, it can ignore rename effects, because the resolve
   * stage will sort them out.)
   */
  set<int> witnesses = mdr->more()->extra_witnesses;
  if (srcdn->is_auth())
    srcdn->list_replicas(witnesses);
  else
    witnesses.insert(srcdn->authority().first);
  destdn->list_replicas(witnesses);
  dout(10) << " witnesses " << witnesses << ", have " << mdr->more()->witnessed << dendl;

  // do srcdn auth last
  int last = -1;
  if (!srcdn->is_auth())
    last = srcdn->authority().first;
  
  for (set<int>::iterator p = witnesses.begin();
       p != witnesses.end();
       ++p) {
    if (*p == last) continue;  // do it last!
    if (mdr->more()->witnessed.count(*p)) {
      dout(10) << " already witnessed by mds" << *p << dendl;
    } else if (mdr->more()->waiting_on_slave.count(*p)) {
      dout(10) << " already waiting on witness mds" << *p << dendl;      
    } else {
      _rename_prepare_witness(mdr, *p, srcdn, destdn, straydn);
    }
  }
  if (!mdr->more()->waiting_on_slave.empty())
    return;  // we're waiting for a witness.

  if (last >= 0 &&
      mdr->more()->witnessed.count(last) == 0 &&
      mdr->more()->waiting_on_slave.count(last) == 0) {
    dout(10) << " preparing last witness (srcdn auth)" << dendl;
    _rename_prepare_witness(mdr, last, srcdn, destdn, straydn);
    return;
  }
  
  // -- prepare anchor updates -- 
  bool linkmerge = (srcdn->inode == destdn->inode &&
		    (srcdn->is_primary() || destdn->is_primary()));

  if (!linkmerge) {
    C_Gather *anchorgather = 0;

    if (srcdn->is_primary() && srcdn->inode->is_anchored() &&
	srcdn->dir != destdn->dir &&
	!mdr->more()->src_reanchor_atid) {
      dout(10) << "reanchoring src->dst " << *srcdn->inode << dendl;
      vector<Anchor> trace;
      destdn->make_anchor_trace(trace, srcdn->inode);
      
      anchorgather = new C_Gather(new C_MDS_RetryRequest(mdcache, mdr));
      mds->anchorclient->prepare_update(srcdn->inode->ino(), trace, &mdr->more()->src_reanchor_atid, 
					anchorgather->new_sub());
    }
    if (destdn->is_primary() &&
	destdn->inode->is_anchored() &&
	!mdr->more()->dst_reanchor_atid) {
      dout(10) << "reanchoring dst->stray " << *destdn->inode << dendl;

      assert(straydn);
      vector<Anchor> trace;
      straydn->make_anchor_trace(trace, destdn->inode);
      
      if (!anchorgather)
	anchorgather = new C_Gather(new C_MDS_RetryRequest(mdcache, mdr));
      mds->anchorclient->prepare_update(destdn->inode->ino(), trace, &mdr->more()->dst_reanchor_atid, 
					anchorgather->new_sub());
    }

    if (anchorgather) 
      return;  // waiting for anchor prepares
  }

  // -- prepare journal entry --
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "rename");
  le->metablob.add_client_req(mdr->reqid);
  
  _rename_prepare(mdr, &le->metablob, &le->client_map, srcdn, destdn, straydn);

  if (!srcdn->is_auth() && srcdn->is_primary()) {
    // importing inode; also journal imported client map
    
    // ** DER FIXME **
  }

  // -- commit locally --
  C_MDS_rename_finish *fin = new C_MDS_rename_finish(mds, mdr, srcdn, destdn, straydn);

  journal_opens();  // journal pending opens, just in case

  // mark committing (needed for proper recovery)
  mdr->committing = true;
  
  // log + wait
  mdlog->submit_entry(le, fin);
}


void Server::_rename_finish(MDRequest *mdr, CDentry *srcdn, CDentry *destdn, CDentry *straydn)
{
  dout(10) << "_rename_finish " << *mdr << dendl;

  // apply
  _rename_apply(mdr, srcdn, destdn, straydn);
  
  // commit anchor updates?
  if (mdr->more()->src_reanchor_atid) 
    mds->anchorclient->commit(mdr->more()->src_reanchor_atid, mdr->ls);
  if (mdr->more()->dst_reanchor_atid) 
    mds->anchorclient->commit(mdr->more()->dst_reanchor_atid, mdr->ls);

  // bump popularity
  //if (srcdn->is_auth())
  //mds->balancer->hit_dir(mdr->now, srcdn->get_dir(), META_POP_DWR);
  //  mds->balancer->hit_dir(mdr->now, destdn->get_dir(), META_POP_DWR);
  if (destdn->is_remote() &&
      destdn->inode->is_auth())
    mds->balancer->hit_inode(mdr->now, destdn->get_inode(), META_POP_IWR);

  // reply
  MClientReply *reply = new MClientReply(mdr->client_request, 0);
  reply_request(mdr, reply, destdn->get_inode());  // FIXME: imprecise ref
  
  // clean up?
  if (straydn) 
    mdcache->eval_stray(straydn);
}



// helpers

void Server::_rename_prepare_witness(MDRequest *mdr, int who, CDentry *srcdn, CDentry *destdn, CDentry *straydn)
{
  dout(10) << "_rename_prepare_witness mds" << who << dendl;
  MMDSSlaveRequest *req = new MMDSSlaveRequest(mdr->reqid, MMDSSlaveRequest::OP_RENAMEPREP);
  srcdn->make_path(req->srcdnpath);
  destdn->make_path(req->destdnpath);
  req->now = mdr->now;
  
  if (straydn) {
    CInodeDiscover *indis = straydn->dir->inode->replicate_to(who);
    CDirDiscover *dirdis = straydn->dir->replicate_to(who);
    CDentryDiscover *dndis = straydn->replicate_to(who);
    indis->_encode(req->stray);
    dirdis->_encode(req->stray);
    dndis->_encode(req->stray);
    delete indis;
    delete dirdis;
    delete dndis;
  }
  
  // srcdn auth will verify our current witness list is sufficient
  req->witnesses = mdr->more()->witnessed;

  mds->send_message_mds(req, who);
  
  assert(mdr->more()->waiting_on_slave.count(who) == 0);
  mdr->more()->waiting_on_slave.insert(who);
}


void Server::_rename_prepare(MDRequest *mdr,
			     EMetaBlob *metablob, bufferlist *client_map_bl,
			     CDentry *srcdn, CDentry *destdn, CDentry *straydn)
{
  dout(10) << "_rename_prepare " << *mdr << " " << *srcdn << " " << *destdn << dendl;

  // primary+remote link merge?
  bool linkmerge = (srcdn->inode == destdn->inode &&
		    (srcdn->is_primary() || destdn->is_primary()));

  if (mdr->is_master()) {
    mdr->more()->pvmap[destdn->dir->inode] = predirty_dn_diri(mdr, destdn, metablob); 
    if (destdn->dir != srcdn->dir)
      mdr->more()->pvmap[srcdn->dir->inode] = predirty_dn_diri(mdr, srcdn, metablob); 
  }

  inode_t *ji = 0;     // journaled inode getting nlink--
  version_t ipv = 0;   // it's version
  
  if (linkmerge) {
    dout(10) << "will merge remote+primary links" << dendl;

    // destdn -> primary
    metablob->add_dir_context(destdn->dir);
    if (destdn->is_auth())
      ipv = mdr->more()->pvmap[destdn] = destdn->pre_dirty(destdn->inode->inode.version);
    ji = metablob->add_primary_dentry(destdn, true, destdn->inode); 
    
    // do src dentry
    metablob->add_dir_context(srcdn->dir);
    if (srcdn->is_auth())
      mdr->more()->pvmap[srcdn] = srcdn->pre_dirty();
    metablob->add_null_dentry(srcdn, true);

  } else {
    // move to stray?
    if (destdn->is_primary()) {
      // primary.  we'll move inode to stray dir.
      assert(straydn);

      // link-- inode, move to stray dir.
      metablob->add_dir_context(straydn->dir);
      if (straydn->is_auth())
	ipv = mdr->more()->pvmap[straydn] = straydn->pre_dirty(destdn->inode->inode.version);
      ji = metablob->add_primary_dentry(straydn, true, destdn->inode);
    } 
    else if (destdn->is_remote()) {
      // remote.
      // nlink-- targeti
      metablob->add_dir_context(destdn->inode->get_parent_dir());
      if (destdn->inode->is_auth())
	ipv = mdr->more()->pvmap[destdn->inode] = destdn->inode->pre_dirty();
      ji = metablob->add_primary_dentry(destdn->inode->parent, true, destdn->inode);  // update primary
      dout(10) << "remote targeti (nlink--) is " << *destdn->inode << dendl;
    }
    else {
      assert(destdn->is_null());
    }

    // add dest dentry
    metablob->add_dir_context(destdn->dir);
    if (srcdn->is_primary()) {
      dout(10) << "src is a primary dentry" << dendl;
      if (destdn->is_auth()) {
	version_t siv;
	if (srcdn->is_auth())
	  siv = srcdn->inode->get_projected_version();
	else {
	  siv = mdr->more()->inode_import_v;

	  /* import node */
	  bufferlist::iterator blp = mdr->more()->inode_import.begin();
	  
	  // imported caps
	  ::_decode_simple(mdr->more()->imported_client_map, blp);
	  ::_encode_simple(mdr->more()->imported_client_map, *client_map_bl);
	  prepare_force_open_sessions(mdr->more()->imported_client_map);

	  list<ScatterLock*> updated_scatterlocks;  // we clear_updated explicitly below
	  
	  mdcache->migrator->decode_import_inode(srcdn, blp, 
						 srcdn->authority().first,
						 mdr->ls,
						 mdr->more()->cap_imports, updated_scatterlocks);
	  srcdn->inode->dirlock.clear_updated();  


	  // hack: force back to !auth and clean, temporarily
	  srcdn->inode->state_clear(CInode::STATE_AUTH);
	  srcdn->inode->mark_clean();
	}
	mdr->more()->pvmap[destdn] = destdn->pre_dirty(siv+1);
      }
      metablob->add_primary_dentry(destdn, true, srcdn->inode); 

    } else {
      assert(srcdn->is_remote());
      dout(10) << "src is a remote dentry" << dendl;
      if (destdn->is_auth())
	mdr->more()->pvmap[destdn] = destdn->pre_dirty();
      metablob->add_remote_dentry(destdn, true, srcdn->get_remote_ino()); 
    }
    
    // remove src dentry
    metablob->add_dir_context(srcdn->dir);
    if (srcdn->is_auth())
      mdr->more()->pvmap[srcdn] = srcdn->pre_dirty();
    metablob->add_null_dentry(srcdn, true);

    // new subtree?
    if (srcdn->is_primary() &&
	srcdn->inode->is_dir()) {
      list<CDir*> ls;
      srcdn->inode->get_nested_dirfrags(ls);
      int auth = srcdn->authority().first;
      for (list<CDir*>::iterator p = ls.begin(); p != ls.end(); ++p) 
	mdcache->adjust_subtree_auth(*p, auth, auth);
    }       
  }

  if (ipv) {
    // update journaled target inode
    inode_t *pi = destdn->inode->project_inode();
    pi->nlink--;
    pi->ctime = mdr->now;
    pi->version = ipv;
    *ji = *pi;  // copy into journal
  }

  // anchor updates?
  if (mdr->more()->src_reanchor_atid)
    metablob->add_anchor_transaction(mdr->more()->src_reanchor_atid);
  if (mdr->more()->dst_reanchor_atid)
    metablob->add_anchor_transaction(mdr->more()->dst_reanchor_atid);
}


void Server::_rename_apply(MDRequest *mdr, CDentry *srcdn, CDentry *destdn, CDentry *straydn)
{
  dout(10) << "_rename_apply " << *mdr << " " << *srcdn << " " << *destdn << dendl;
  dout(10) << " pvs " << mdr->more()->pvmap << dendl;

  CInode *oldin = destdn->inode;
  
  // primary+remote link merge?
  bool linkmerge = (srcdn->inode == destdn->inode &&
		    (srcdn->is_primary() || destdn->is_primary()));

  // dir mtimes
  if (mdr->is_master()) {
    dirty_dn_diri(mdr, destdn, mdr->more()->pvmap[destdn->dir->inode]);
    if (destdn->dir != srcdn->dir)
      dirty_dn_diri(mdr, srcdn, mdr->more()->pvmap[srcdn->dir->inode]);
  }

  if (linkmerge) {
    if (destdn->is_primary()) {
      dout(10) << "merging remote onto primary link" << dendl;

      // nlink-- in place
      destdn->inode->inode.nlink--;
      destdn->inode->inode.ctime = mdr->now;
      if (destdn->inode->is_auth())
	destdn->inode->mark_dirty(mdr->more()->pvmap[destdn], mdr->ls);

      // unlink srcdn
      srcdn->dir->unlink_inode(srcdn);
      if (srcdn->is_auth())
	srcdn->mark_dirty(mdr->more()->pvmap[srcdn], mdr->ls);
    } else {
      dout(10) << "merging primary onto remote link" << dendl;
      assert(srcdn->is_primary());
      
      // move inode to dest
      srcdn->dir->unlink_inode(srcdn);
      destdn->dir->unlink_inode(destdn);
      destdn->dir->link_primary_inode(destdn, oldin);
      
      // nlink--
      destdn->inode->inode.nlink--;
      destdn->inode->inode.ctime = mdr->now;
      if (destdn->inode->is_auth())
	destdn->inode->mark_dirty(mdr->more()->pvmap[destdn], mdr->ls);
      
      // mark src dirty
      if (srcdn->is_auth())
	srcdn->mark_dirty(mdr->more()->pvmap[srcdn], mdr->ls);
    }
  } 
  else {
    // unlink destdn?
    if (!destdn->is_null())
      destdn->dir->unlink_inode(destdn);

    if (straydn) {
      dout(10) << "straydn is " << *straydn << dendl;

      // relink oldin to stray dir.  destdn was primary.
      assert(oldin);
      straydn->dir->link_primary_inode(straydn, oldin);
      //assert(straypv == ipv);

      // nlink-- in stray dir.
      oldin->inode.nlink--;
      oldin->inode.ctime = mdr->now;
      if (oldin->is_auth())
	oldin->pop_and_dirty_projected_inode(mdr->ls);
    }
    else if (oldin) {
      // nlink-- remote.  destdn was remote.
      oldin->inode.nlink--;
      oldin->inode.ctime = mdr->now;
      if (oldin->is_auth())
	oldin->pop_and_dirty_projected_inode(mdr->ls);
    }
    
    CInode *in = srcdn->inode;
    assert(in);
    if (srcdn->is_remote()) {
      // srcdn was remote.
      srcdn->dir->unlink_inode(srcdn);
      destdn->dir->link_remote_inode(destdn, in->ino(), MODE_TO_DT(in->inode.mode));    
      destdn->link_remote(in);
      if (destdn->is_auth())
	destdn->mark_dirty(mdr->more()->pvmap[destdn], mdr->ls);
    } else {
      // srcdn was primary.
      srcdn->dir->unlink_inode(srcdn);
      destdn->dir->link_primary_inode(destdn, in);

      // srcdn inode import?
      if (!srcdn->is_auth() && destdn->is_auth()) {
	assert(mdr->more()->inode_import.length() > 0);

	// finish cap imports
	finish_force_open_sessions(mdr->more()->imported_client_map);
	if (mdr->more()->cap_imports.count(destdn->inode))
	  mds->mdcache->migrator->finish_import_inode_caps(destdn->inode, srcdn->authority().first, 
							   mdr->more()->cap_imports[destdn->inode]);
	
	// hack: fix auth bit
	destdn->inode->state_set(CInode::STATE_AUTH);
      }
      if (destdn->inode->is_auth())
	destdn->inode->mark_dirty(mdr->more()->pvmap[destdn], mdr->ls);
    }

    if (srcdn->is_auth())
      srcdn->mark_dirty(mdr->more()->pvmap[srcdn], mdr->ls);
  }

  // update subtree map?
  if (destdn->is_primary() && destdn->inode->is_dir()) 
    mdcache->adjust_subtree_after_rename(destdn->inode, srcdn->dir);

  // removing a new dn?
  if (srcdn->is_auth())
    srcdn->dir->try_remove_unlinked_dn(srcdn);
}





// ------------
// SLAVE

class C_MDS_SlaveRenamePrep : public Context {
  Server *server;
  MDRequest *mdr;
  CDentry *srcdn, *destdn, *straydn;
public:
  C_MDS_SlaveRenamePrep(Server *s, MDRequest *m, CDentry *sr, CDentry *de, CDentry *st) :
    server(s), mdr(m), srcdn(sr), destdn(de), straydn(st) {}
  void finish(int r) {
    server->_logged_slave_rename(mdr, srcdn, destdn, straydn);
  }
};

class C_MDS_SlaveRenameCommit : public Context {
  Server *server;
  MDRequest *mdr;
  CDentry *srcdn, *destdn, *straydn;
public:
  C_MDS_SlaveRenameCommit(Server *s, MDRequest *m, CDentry *sr, CDentry *de, CDentry *st) :
    server(s), mdr(m), srcdn(sr), destdn(de), straydn(st) {}
  void finish(int r) {
    server->_commit_slave_rename(mdr, r, srcdn, destdn, straydn);
  }
};

void Server::handle_slave_rename_prep(MDRequest *mdr)
{
  dout(10) << "handle_slave_rename_prep " << *mdr 
	   << " " << mdr->slave_request->srcdnpath 
	   << " to " << mdr->slave_request->destdnpath
	   << dendl;
  
  // discover destdn
  filepath destpath(mdr->slave_request->destdnpath);
  dout(10) << " dest " << destpath << dendl;
  vector<CDentry*> trace;
  int r = mdcache->path_traverse(mdr, mdr->slave_request, 
				 destpath, trace, false,
				 MDS_TRAVERSE_DISCOVERXLOCK);
  if (r > 0) return;
  assert(r == 0);  // we shouldn't get an error here!
      
  CDentry *destdn = trace[trace.size()-1];
  dout(10) << " destdn " << *destdn << dendl;
  mdr->pin(destdn);
  
  // discover srcdn
  filepath srcpath(mdr->slave_request->srcdnpath);
  dout(10) << " src " << srcpath << dendl;
  r = mdcache->path_traverse(mdr, mdr->slave_request,
			     srcpath, trace, false,  
			     MDS_TRAVERSE_DISCOVERXLOCK);
  if (r > 0) return;
  assert(r == 0);
      
  CDentry *srcdn = trace[trace.size()-1];
  dout(10) << " srcdn " << *srcdn << dendl;
  mdr->pin(srcdn);
  assert(srcdn->inode);
  mdr->pin(srcdn->inode);

  // stray?
  CDentry *straydn = 0;
  if (destdn->is_primary()) {
    assert(mdr->slave_request->stray.length() > 0);
    straydn = mdcache->add_replica_stray(mdr->slave_request->stray, 
					 destdn->inode, mdr->slave_to_mds);
    assert(straydn);
    mdr->pin(straydn);
  }

  mdr->now = mdr->slave_request->now;

  // set up commit waiter (early, to clean up any freezing etc we do)
  if (!mdr->more()->slave_commit)
    mdr->more()->slave_commit = new C_MDS_SlaveRenameCommit(this, mdr, srcdn, destdn, straydn);

  // am i srcdn auth?
  if (srcdn->is_auth()) {
    if (srcdn->is_primary() && 
	!srcdn->inode->is_freezing_inode() &&
	!srcdn->inode->is_frozen_inode()) {
      // srci auth.  
      // set ambiguous auth.
      srcdn->inode->state_set(CInode::STATE_AMBIGUOUSAUTH);

      // freeze?
      // we need this to
      //  - avoid conflicting lock state changes
      //  - avoid concurrent updates to the inode
      //     (this could also be accomplished with the versionlock)
      int allowance = 1; // for the versionlock and possible linklock xlock (both are tied to mdr)
      dout(10) << " freezing srci " << *srcdn->inode << " with allowance " << allowance << dendl;
      if (!srcdn->inode->freeze_inode(allowance)) {
	srcdn->inode->add_waiter(CInode::WAIT_FROZEN, new C_MDS_RetryRequest(mdcache, mdr));
	return;
      }
    }

    // is witness list sufficient?
    set<int> srcdnrep;
    srcdn->list_replicas(srcdnrep);
    for (set<int>::iterator p = srcdnrep.begin();
	 p != srcdnrep.end();
	 ++p) {
      if (*p == mdr->slave_to_mds ||
	  mdr->slave_request->witnesses.count(*p)) continue;
      dout(10) << " witness list insufficient; providing srcdn replica list" << dendl;
      MMDSSlaveRequest *reply = new MMDSSlaveRequest(mdr->reqid, MMDSSlaveRequest::OP_RENAMEPREPACK);
      reply->witnesses.swap(srcdnrep);
      mds->send_message_mds(reply, mdr->slave_to_mds);
      delete mdr->slave_request;
      mdr->slave_request = 0;
      return;	
    }
    dout(10) << " witness list sufficient: includes all srcdn replicas" << dendl;
  }

  // journal it?
  if (srcdn->is_auth() ||
      (destdn->inode && destdn->inode->is_auth()) ||
      srcdn->inode->is_any_caps()) {
    // journal.
    mdr->ls = mdlog->get_current_segment();
    ESlaveUpdate *le = new ESlaveUpdate(mdlog, "slave_rename_prep", mdr->reqid, mdr->slave_to_mds, ESlaveUpdate::OP_PREPARE);

    // rollback case
    if (destdn->inode && destdn->inode->is_auth()) {
      assert(destdn->is_remote());
      le->rollback.add_dir_context(destdn->dir);
      le->rollback.add_dentry(destdn, true);
    }
    if (srcdn->is_auth() ||
	(srcdn->inode && srcdn->inode->is_auth())) {
      le->rollback.add_dir_context(srcdn->dir);
      le->rollback.add_dentry(srcdn, true);
    }

    // commit case
    bufferlist blah;
    _rename_prepare(mdr, &le->commit, &blah, srcdn, destdn, straydn);

    mdlog->submit_entry(le, new C_MDS_SlaveRenamePrep(this, mdr, srcdn, destdn, straydn));
  } else {
    // don't journal.
    dout(10) << "not journaling, i'm not auth for anything, and srci isn't open" << dendl;

    // prepare anyway; this may twiddle dir_auth
    EMetaBlob blob;
    bufferlist blah;
    _rename_prepare(mdr, &blob, &blah, srcdn, destdn, straydn);
    _logged_slave_rename(mdr, srcdn, destdn, straydn);
  }
}

void Server::_logged_slave_rename(MDRequest *mdr, 
				  CDentry *srcdn, CDentry *destdn, CDentry *straydn)
{
  dout(10) << "_logged_slave_rename " << *mdr << dendl;

  // prepare ack
  MMDSSlaveRequest *reply = new MMDSSlaveRequest(mdr->reqid, MMDSSlaveRequest::OP_RENAMEPREPACK);
  
  // export srci?
  if (srcdn->is_auth() && srcdn->is_primary()) {
    list<Context*> finished;
    map<int,entity_inst_t> exported_client_map;
    bufferlist inodebl;
    mdcache->migrator->encode_export_inode(srcdn->inode, inodebl, 
					   exported_client_map);
    mdcache->migrator->finish_export_inode(srcdn->inode, mdr->now, finished); 
    mds->queue_waiters(finished);   // this includes SINGLEAUTH waiters.
    ::_encode(exported_client_map, reply->inode_export);
    reply->inode_export.claim_append(inodebl);
    reply->inode_export_v = srcdn->inode->inode.version;

    // remove mdr auth pin
    mdr->auth_unpin(srcdn->inode);
    assert(!srcdn->inode->is_auth_pinned());
    
    dout(10) << " exported srci " << *srcdn->inode << dendl;
  }

  // apply
  _rename_apply(mdr, srcdn, destdn, straydn);   

  mds->send_message_mds(reply, mdr->slave_to_mds);
  
  // bump popularity
  //if (srcdn->is_auth())
    //mds->balancer->hit_dir(mdr->now, srcdn->get_dir(), META_POP_DWR);
  if (destdn->inode && destdn->inode->is_auth())
    mds->balancer->hit_inode(mdr->now, destdn->inode, META_POP_IWR);

  // done.
  delete mdr->slave_request;
  mdr->slave_request = 0;
}

void Server::_commit_slave_rename(MDRequest *mdr, int r,
				  CDentry *srcdn, CDentry *destdn, CDentry *straydn)
{
  dout(10) << "_commit_slave_rename " << *mdr << " r=" << r << dendl;

  // unfreeze+singleauth inode
  //  hmm, do i really need to delay this?
  if (srcdn->is_auth() && destdn->is_primary()) {
    dout(10) << " unfreezing exported inode " << *destdn->inode << dendl;
    list<Context*> finished;
    
    // singleauth
    assert(destdn->inode->state_test(CInode::STATE_AMBIGUOUSAUTH));
    destdn->inode->state_clear(CInode::STATE_AMBIGUOUSAUTH);
    destdn->inode->take_waiting(CInode::WAIT_SINGLEAUTH, finished);
    
    // unfreeze
    assert(destdn->inode->is_frozen_inode() ||
	   destdn->inode->is_freezing_inode());
    destdn->inode->unfreeze_inode(finished);
    
    mds->queue_waiters(finished);
  }
  

  ESlaveUpdate *le;
  if (r == 0) {
    // write a commit to the journal
    le = new ESlaveUpdate(mdlog, "slave_rename_commit", mdr->reqid, mdr->slave_to_mds, ESlaveUpdate::OP_COMMIT);

  } else {
    // abort
    le = new ESlaveUpdate(mdlog, "slave_rename_abort", mdr->reqid, mdr->slave_to_mds, ESlaveUpdate::OP_ROLLBACK);

    // -- rollback in memory --

    if (mdr->more()->was_link_merge) { 
      // link merge
      CInode *in = destdn->inode;
      in->inode.nlink++;
      if (mdr->more()->destdn_was_remote_inode) {
	destdn->dir->unlink_inode(destdn);
	srcdn->dir->link_primary_inode(srcdn, in);
	destdn->dir->link_remote_inode(destdn, in->ino(),  MODE_TO_DT(in->inode.mode));
      } else {
	srcdn->dir->link_remote_inode(srcdn, in->ino(), MODE_TO_DT(in->inode.mode));
      }
    } else {
      // normal

      // revert srcdn
      if (destdn->is_remote()) {
	srcdn->dir->link_remote_inode(srcdn, destdn->inode->ino(), MODE_TO_DT(destdn->inode->inode.mode));
	destdn->dir->unlink_inode(destdn);
      } else {
	// renamed a primary
	CInode *in = destdn->inode;
	destdn->dir->unlink_inode(destdn);
	srcdn->dir->link_primary_inode(srcdn, in);
      }
      
      // revert destdn
      if (mdr->more()->destdn_was_remote_inode) {
	destdn->dir->link_remote_inode(destdn, 
				       mdr->more()->destdn_was_remote_inode->ino(), 
				       MODE_TO_DT(mdr->more()->destdn_was_remote_inode->inode.mode));
	mdr->more()->destdn_was_remote_inode->inode.nlink++;
      } else if (straydn && straydn->inode) {
	CInode *in = straydn->inode;
	straydn->dir->unlink_inode(straydn);
	destdn->dir->link_primary_inode(destdn, in);
	straydn->dir->remove_dentry(straydn);
      }
    }

    // FIXME: reverse srci export?

    dout(-10) << "  srcdn back to " << *srcdn << dendl;
    dout(-10) << "   srci back to " << *srcdn->inode << dendl;
    dout(-10) << " destdn back to " << *destdn << dendl;
    if (destdn->inode) dout(-10) << "  desti back to " << *destdn->inode << dendl;
    
    // *** WRITE ME ***
    assert(0);

  }

  

  mdlog->submit_entry(le);
}

void Server::handle_slave_rename_prep_ack(MDRequest *mdr, MMDSSlaveRequest *ack)
{
  dout(10) << "handle_slave_rename_prep_ack " << *mdr 
	   << " witnessed by " << ack->get_source()
	   << " " << *ack << dendl;
  int from = ack->get_source().num();

  // note slave
  mdr->more()->slaves.insert(from);

  // witnessed?  or add extra witnesses?
  assert(mdr->more()->witnessed.count(from) == 0);
  if (ack->witnesses.empty()) {
    mdr->more()->witnessed.insert(from);
  } else {
    dout(10) << " extra witnesses (srcdn replicas) are " << ack->witnesses << dendl;
    mdr->more()->extra_witnesses.swap(ack->witnesses);
    mdr->more()->extra_witnesses.erase(mds->get_nodeid());  // not me!
  }

  // srci import?
  if (ack->inode_export.length()) {
    dout(10) << " got srci import" << dendl;
    mdr->more()->inode_import.claim(ack->inode_export);
    mdr->more()->inode_import_v = ack->inode_export_v;
  }

  // remove from waiting list
  assert(mdr->more()->waiting_on_slave.count(from));
  mdr->more()->waiting_on_slave.erase(from);

  if (mdr->more()->waiting_on_slave.empty())
    dispatch_client_request(mdr);  // go again!
  else 
    dout(10) << "still waiting on slaves " << mdr->more()->waiting_on_slave << dendl;
}





// ===================================
// TRUNCATE, FSYNC

class C_MDS_truncate_purged : public Context {
  MDS *mds;
  MDRequest *mdr;
  CInode *in;
  version_t pv;
  off_t size;
  utime_t ctime;
public:
  C_MDS_truncate_purged(MDS *m, MDRequest *r, CInode *i, version_t pdv, off_t sz, utime_t ct) :
    mds(m), mdr(r), in(i), 
    pv(pdv),
    size(sz), ctime(ct) { }
  void finish(int r) {
    assert(r == 0);

    // apply to cache
    in->inode.size = size;
    in->inode.ctime = ctime;
    in->inode.mtime = ctime;
    in->mark_dirty(pv, mdr->ls);

    // reply
    mds->server->reply_request(mdr, 0);
  }
};

class C_MDS_truncate_logged : public Context {
  MDS *mds;
  MDRequest *mdr;
  CInode *in;
  version_t pv;
  off_t size;
  utime_t ctime;
public:
  C_MDS_truncate_logged(MDS *m, MDRequest *r, CInode *i, version_t pdv, off_t sz, utime_t ct) :
    mds(m), mdr(r), in(i), 
    pv(pdv),
    size(sz), ctime(ct) { }
  void finish(int r) {
    assert(r == 0);

    // purge
    mds->mdcache->purge_inode(in, size, in->inode.size, mdr->ls);
    mds->mdcache->wait_for_purge(in, size, 
				 new C_MDS_truncate_purged(mds, mdr, in, pv, size, ctime));
  }
};

void Server::handle_client_truncate(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;
  CInode *cur = rdlock_path_pin_ref(mdr, true);
  if (!cur) return;

  // check permissions?  

  // xlock inode
  set<SimpleLock*> rdlocks = mdr->rdlocks;
  set<SimpleLock*> wrlocks = mdr->wrlocks;
  set<SimpleLock*> xlocks = mdr->xlocks;
  xlocks.insert(&cur->filelock);
  if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
    return;
  
  // already small enough?
  if (cur->inode.size <= req->head.args.truncate.length) {
    reply_request(mdr, 0);
    return;
  }

  // prepare
  version_t pdv = cur->pre_dirty();
  utime_t ctime = g_clock.real_now();
  Context *fin = new C_MDS_truncate_logged(mds, mdr, cur, 
					   pdv, req->head.args.truncate.length, ctime);
  
  // log + wait
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "truncate");
  le->metablob.add_client_req(mdr->reqid);
  le->metablob.add_dir_context(cur->get_parent_dir());
  le->metablob.add_inode_truncate(cur->ino(), req->head.args.truncate.length, cur->inode.size);
  inode_t *pi = le->metablob.add_dentry(cur->parent, true);
  pi->mtime = ctime;
  pi->ctime = ctime;
  pi->version = pdv;
  pi->size = req->head.args.truncate.length;
  

  mdlog->submit_entry(le, fin);
}


// ===========================
// open, openc, close

void Server::handle_client_open(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;

  int flags = req->head.args.open.flags;
  int cmode = req->get_open_file_mode();
  bool need_auth = ((cmode != FILE_MODE_R && cmode != FILE_MODE_LAZY) ||
		    (flags & O_TRUNC));
  dout(10) << "open flags = " << flags
	   << ", filemode = " << cmode
	   << ", need_auth = " << need_auth
	   << dendl;

  CInode *cur = rdlock_path_pin_ref(mdr, need_auth);
  if (!cur) return;
  
  // regular file?
  if (!cur->inode.is_file() && !cur->inode.is_dir()) {
    dout(7) << "not a file or dir " << *cur << dendl;
    reply_request(mdr, -EINVAL);                 // FIXME what error do we want?
    return;
  }
  // can only open a dir rdonly, no flags.
  if (cur->inode.is_dir() && (cmode != FILE_MODE_R || flags != O_DIRECTORY)) {
    reply_request(mdr, -EINVAL);
    return;
  }

  // hmm, check permissions or something.


  // O_TRUNC
  if (flags & O_TRUNC) {
    assert(cur->is_auth());

    // xlock file size
    set<SimpleLock*> rdlocks = mdr->rdlocks;
    set<SimpleLock*> wrlocks = mdr->wrlocks;
    set<SimpleLock*> xlocks = mdr->xlocks;
    xlocks.insert(&cur->filelock);
    if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
      return;

    if (cur->inode.size > 0) {
      handle_client_opent(mdr);
      return;
    }
  }
  
  // do it
  _do_open(mdr, cur);
}

void Server::_do_open(MDRequest *mdr, CInode *cur)
{
  MClientRequest *req = mdr->client_request;
  int cmode = req->get_open_file_mode();

  // register new cap
  Capability *cap = mds->locker->issue_new_caps(cur, cmode, mdr->session);

  // drop our locks (they may interfere with us issuing new caps)
  mds->locker->drop_locks(mdr);

  cap->set_suppress(false);  // stop suppressing messages on this cap

  dout(12) << "_do_open issued caps " << cap_string(cap->pending())
	   << " for " << req->get_source()
	   << " on " << *cur << dendl;
  
  // hit pop
  mdr->now = g_clock.now();
  if (cmode == FILE_MODE_RW ||
      cmode == FILE_MODE_W) 
    mds->balancer->hit_inode(mdr->now, cur, META_POP_IWR);
  else
    mds->balancer->hit_inode(mdr->now, cur, META_POP_IRD, 
			     mdr->client_request->get_client_inst().name.num());

  // reply
  MClientReply *reply = new MClientReply(req, 0);
  reply->set_file_caps(cap->pending());
  reply->set_file_caps_seq(cap->get_last_seq());
  //reply->set_file_data_version(fdv);
  reply_request(mdr, reply, cur);

  // journal?
  if (cur->last_open_journaled == 0) {
    queue_journal_open(cur);
    maybe_journal_opens();
  }

}

void Server::queue_journal_open(CInode *in)
{
  dout(10) << "queue_journal_open on " << *in << dendl;

  if (journal_open_queue.count(in) == 0) {
    // pin so our pointer stays valid
    in->get(CInode::PIN_BATCHOPENJOURNAL);
    
    // queue it up for a bit
    journal_open_queue.insert(in);
  }
}


void Server::journal_opens()
{
  dout(10) << "journal_opens " << journal_open_queue.size() << " inodes" << dendl;
  if (journal_open_queue.empty()) return;

  EOpen *le = 0;

  // check queued inodes
  LogSegment *ls = mdlog->get_current_segment();
  for (set<CInode*>::iterator p = journal_open_queue.begin();
       p != journal_open_queue.end();
       ++p) {
    CInode *in = *p;
    in->put(CInode::PIN_BATCHOPENJOURNAL);
    if (in->is_any_caps()) {
      if (!le) le = new EOpen(mdlog);
      le->add_inode(in);
      in->last_open_journaled = mds->mdlog->get_write_pos();
      ls->open_files.push_back(&in->xlist_open_file);
    }
  }
  journal_open_queue.clear();
  
  if (le) {
    // journal
    mdlog->submit_entry(le);
  
    // add waiters to journal entry
    for (list<Context*>::iterator p = journal_open_waiters.begin();
	 p != journal_open_waiters.end();
	 ++p) 
      mds->mdlog->wait_for_sync(*p);
    journal_open_waiters.clear();
  } else {
    // nothing worth journaling here, just kick the waiters.
    mds->queue_waiters(journal_open_waiters);
  }
}




class C_MDS_open_truncate_purged : public Context {
  MDS *mds;
  MDRequest *mdr;
  CInode *in;
  version_t pv;
  utime_t ctime;
public:
  C_MDS_open_truncate_purged(MDS *m, MDRequest *r, CInode *i, version_t pdv, utime_t ct) :
    mds(m), mdr(r), in(i), 
    pv(pdv),
    ctime(ct) { }
  void finish(int r) {
    assert(r == 0);

    // apply to cache
    in->inode.size = 0;
    in->inode.ctime = ctime;
    in->inode.mtime = ctime;
    in->mark_dirty(pv, mdr->ls);
    
    // do the open
    mds->server->_do_open(mdr, in);
  }
};

class C_MDS_open_truncate_logged : public Context {
  MDS *mds;
  MDRequest *mdr;
  CInode *in;
  version_t pv;
  utime_t ctime;
public:
  C_MDS_open_truncate_logged(MDS *m, MDRequest *r, CInode *i, version_t pdv, utime_t ct) :
    mds(m), mdr(r), in(i), 
    pv(pdv),
    ctime(ct) { }
  void finish(int r) {
    assert(r == 0);

    // hit pop
    mds->balancer->hit_inode(mdr->now, in, META_POP_IWR);   

    // purge also...
    mds->mdcache->purge_inode(in, 0, in->inode.size, mdr->ls);
    mds->mdcache->wait_for_purge(in, 0,
				 new C_MDS_open_truncate_purged(mds, mdr, in, pv, ctime));
  }
};


void Server::handle_client_opent(MDRequest *mdr)
{
  CInode *cur = mdr->ref;
  assert(cur);

  // prepare
  version_t pdv = cur->pre_dirty();
  utime_t ctime = g_clock.real_now();
  Context *fin = new C_MDS_open_truncate_logged(mds, mdr, cur, 
						pdv, ctime);
  
  // log + wait
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "open_truncate");
  le->metablob.add_client_req(mdr->reqid);
  le->metablob.add_dir_context(cur->get_parent_dir());
  le->metablob.add_inode_truncate(cur->ino(), 0, cur->inode.size);
  inode_t *pi = le->metablob.add_dentry(cur->parent, true);
  pi->mtime = ctime;
  pi->ctime = ctime;
  pi->version = pdv;
  pi->size = 0;
  
  mdlog->submit_entry(le, fin);
}



class C_MDS_openc_finish : public Context {
  MDS *mds;
  MDRequest *mdr;
  CDentry *dn;
  CInode *newi;
  version_t pv;
public:
  C_MDS_openc_finish(MDS *m, MDRequest *r, CDentry *d, CInode *ni) :
    mds(m), mdr(r), dn(d), newi(ni),
    pv(d->get_projected_version()) {}
  void finish(int r) {
    assert(r == 0);

    // link the inode
    dn->get_dir()->link_primary_inode(dn, newi);

    // dirty inode, dn, dir
    newi->mark_dirty(pv, mdr->ls);

    // downgrade xlock to rdlock
    //mds->locker->dentry_xlock_downgrade_to_rdlock(dn, mdr);

    // set/pin ref inode for open()
    mdr->ref = newi;
    mdr->pin(newi);
    
    // ok, do the open.
    mds->server->handle_client_open(mdr);
  }
};


void Server::handle_client_openc(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;

  dout(7) << "open w/ O_CREAT on " << req->get_filepath() << dendl;
  
  bool excl = (req->head.args.open.flags & O_EXCL);
  CDentry *dn = rdlock_path_xlock_dentry(mdr, !excl, false);
  if (!dn) return;

  if (!dn->is_null()) {
    // it existed.  
    if (req->head.args.open.flags & O_EXCL) {
      dout(10) << "O_EXCL, target exists, failing with -EEXIST" << dendl;
      reply_request(mdr, -EEXIST, dn->get_dir()->get_inode());
      return;
    } 
    
    // pass to regular open handler.
    handle_client_open(mdr);
    return;
  }

  // created null dn.
    
  // create inode.
  mdr->now = g_clock.real_now();
  CInode *in = prepare_new_inode(mdr, dn->dir);
  assert(in);
  
  // it's a file.
  in->inode.mode = req->head.args.open.mode;
  in->inode.mode |= S_IFREG;
  in->inode.version = dn->pre_dirty() - 1;
  
  // prepare finisher
  C_MDS_openc_finish *fin = new C_MDS_openc_finish(mds, mdr, dn, in);
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "openc");
  le->metablob.add_client_req(req->get_reqid());
  le->metablob.add_allocated_ino(in->ino(), mds->idalloc->get_version());
  le->metablob.add_dir_context(dn->dir);
  le->metablob.add_primary_dentry(dn, true, in, &in->inode);
  
  // log + wait
  mdlog->submit_entry(le, fin);
  
  /*
    FIXME. this needs to be rewritten when the write capability stuff starts
    getting journaled.  
  */
}














