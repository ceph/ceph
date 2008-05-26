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
#include <sys/xattr.h>

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
    if (session->is_stale()) {
      mds->sessionmap.set_state(session, Session::STATE_OPEN);
      mds->locker->resume_stale_caps(session);
    }
    mds->messenger->send_message(new MClientSession(CEPH_SESSION_RENEWCAPS, m->stamp), 
				 session->inst);
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
      dout(20) << " killing capability " << cap_string(cap->issued()) << " on " << *in << dendl;
      in->remove_client_cap(session->inst.name.num());
      mds->locker->try_file_eval(&in->filelock);
    }
    while (!session->leases.empty()) {
      ClientLease *r = session->leases.front();
      MDSCacheObject *p = r->parent;
      dout(20) << " killing client lease of " << *p << dendl;
      p->remove_client_lease(r, r->mask, mds->locker);
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

version_t Server::prepare_force_open_sessions(map<__u32,entity_inst_t>& cm)
{
  version_t pv = ++mds->sessionmap.projected;
  dout(10) << "prepare_force_open_sessions " << pv 
	   << " on " << cm.size() << " clients"
	   << dendl;
  for (map<__u32,entity_inst_t>::iterator p = cm.begin(); p != cm.end(); ++p) {
    Session *session = mds->sessionmap.get_or_add_session(p->second);
    if (session->is_undef() || session->is_closing())
      mds->sessionmap.set_state(session, Session::STATE_OPENING);
  }
  return pv;
}

void Server::finish_force_open_sessions(map<__u32,entity_inst_t>& cm)
{
  /*
   * FIXME: need to carefully consider the race conditions between a
   * client trying to close a session and an MDS doing an import
   * trying to force open a session...  
   */
  dout(10) << "finish_force_open_sessions on " << cm.size() << " clients,"
	   << " v " << mds->sessionmap.version << " -> " << (mds->sessionmap.version+1) << dendl;
  for (map<__u32,entity_inst_t>::iterator p = cm.begin(); p != cm.end(); ++p) {
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
	Capability *cap = in->reconnect_cap(from, p->second);
	session->touch_cap(cap);
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
    failed_reconnects++;
    client_reconnect_gather.erase(from);
    if (client_reconnect_gather.empty()) 
      reconnect_gather_finish();
  }
}

void Server::reconnect_gather_finish()
{
  dout(7) << "reconnect_gather_finish.  failed on " << failed_reconnects << " clients" << dendl;
  mds->reconnect_done();
}

void Server::reconnect_tick()
{
  utime_t reconnect_end = reconnect_start;
  reconnect_end += g_conf.mds_reconnect_timeout;
  if (g_clock.now() >= reconnect_end &&
      !client_reconnect_gather.empty()) {
    dout(10) << "reconnect timed out" << dendl;
    for (set<int>::iterator p = client_reconnect_gather.begin();
	 p != client_reconnect_gather.end();
	 p++) {
      failed_reconnects++;
      dout(1) << "reconnect gave up on "
	      << mds->sessionmap.get_inst(entity_name_t::CLIENT(*p))
	      << dendl;
    }
    client_reconnect_gather.clear();
    reconnect_gather_finish();
  }
}


/*******
 * some generic stuff for finishing off requests
 */


/*
 * send generic response (just and error code)
 */
void Server::reply_request(MDRequest *mdr, int r, CInode *tracei, CDentry *tracedn)
{
  reply_request(mdr, new MClientReply(mdr->client_request, r), tracei, tracedn);
}


/*
 * send given reply
 * include a trace to tracei
 */
void Server::reply_request(MDRequest *mdr, MClientReply *reply, CInode *tracei, CDentry *tracedn) 
{
  MClientRequest *req = mdr->client_request;
  
  dout(10) << "reply_request " << reply->get_result() 
	   << " (" << strerror(-reply->get_result())
	   << ") " << *req << dendl;

  // note result code in session map?
  if (!req->is_idempotent() && mdr->session)
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

  reply->set_mdsmap_epoch(mds->mdsmap->get_epoch());


  // infer tracei/tracedn from mdr?
  if (!tracei && !tracedn && mdr->ref) {
    tracei = mdr->ref;
    dout(20) << "inferring tracei to be " << *tracei << dendl;
    if (!mdr->trace.empty()) {
      tracedn = mdr->trace.back();
      dout(20) << "inferring tracedn to be " << *tracedn << dendl;
    }
  }

  // clean up request, drop locks, etc.
  // do this before replying, so that we can issue leases
  Session *session = mdr->session;
  entity_inst_t client_inst = req->get_client_inst();
  mdcache->request_finish(mdr);
  mdr = 0;

  // reply at all?
  if (client_inst.name.is_mds()) {
    delete reply;   // mds doesn't need a reply
    reply = 0;
  } else {
    // send reply, with trace, and possible leases
    if (tracei || tracedn)
      set_trace_dist(session, reply, tracei, tracedn);
    messenger->send_message(reply, client_inst);
  }
  
  // take a closer look at tracei, if it happens to be a remote link
  if (tracei && 
      tracei->get_parent_dn() &&
      tracei->get_parent_dn()->is_remote())
    mdcache->eval_remote(tracei->get_parent_dn());
}


/*
 * pass inode OR dentry (not both, or we may get confused)
 *
 * trace is in reverse order (i.e. root inode comes last)
 */
void Server::set_trace_dist(Session *session, MClientReply *reply, CInode *in, CDentry *dn)
{
  // inode, dentry, dir, ..., inode
  bufferlist bl;
  int whoami = mds->get_nodeid();
  int client = session->get_client();
  __u16 numi = 0, numdn = 0;

  // choose lease duration
  utime_t now = g_clock.now();
  int lmask;

  // start with dentry or inode?
  if (!in) {
    assert(dn);
    in = dn->inode;
    goto dentry;
  }

 inode:
  InodeStat::encode(bl, in);
  lmask = mds->locker->issue_client_lease(in, client, bl, now, session);
  numi++;
  dout(20) << " trace added " << lmask << " " << *in << dendl;

  if (!dn)
    dn = in->get_parent_dn();
  if (!dn) 
    goto done;

 dentry:
  ::encode(dn->get_name(), bl);
  lmask = mds->locker->issue_client_lease(dn, client, bl, now, session);
  numdn++;
  dout(20) << " trace added " << lmask << " " << *dn << dendl;
  
  // dir
  DirStat::encode(bl, dn->get_dir(), whoami);
  dout(20) << " trace added " << *dn->get_dir() << dendl;

  in = dn->get_dir()->get_inode();
  dn = 0;
  goto inode;

done:
  // put numi, numd in front
  bufferlist fbl;
  ::encode(numi, fbl);
  ::encode(numdn, fbl);
  fbl.claim_append(bl);
  reply->set_trace(fbl);
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

  // retry?
  if (req->get_retry_attempt() &&
      req->get_op() != CEPH_MDS_OP_OPEN) {
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
  case CEPH_MDS_OP_FINDINODE:
    handle_client_findinode(mdr);
    break;

    // inodes ops.
  case CEPH_MDS_OP_STAT:
  case CEPH_MDS_OP_LSTAT:
    handle_client_stat(mdr);
    break;
  case CEPH_MDS_OP_UTIME:
  case CEPH_MDS_OP_LUTIME:
    handle_client_utime(mdr);
    break;
  case CEPH_MDS_OP_CHMOD:
  case CEPH_MDS_OP_LCHMOD:
    handle_client_chmod(mdr);
    break;
  case CEPH_MDS_OP_CHOWN:
  case CEPH_MDS_OP_LCHOWN:
    handle_client_chown(mdr);
    break;
  case CEPH_MDS_OP_TRUNCATE:
  case CEPH_MDS_OP_LTRUNCATE:
    handle_client_truncate(mdr);
    break;
  case CEPH_MDS_OP_SETXATTR:
  case CEPH_MDS_OP_LSETXATTR:
    handle_client_setxattr(mdr);
    break;
  case CEPH_MDS_OP_RMXATTR:
  case CEPH_MDS_OP_LRMXATTR:
    handle_client_removexattr(mdr);
    break;
  case CEPH_MDS_OP_READDIR:
    handle_client_readdir(mdr);
    break;
  case CEPH_MDS_OP_FSYNC:
    //handle_client_fsync(req);
    break;

    // funky.
  case CEPH_MDS_OP_OPEN:
    if ((req->head.args.open.flags & O_CREAT) &&
	!(req->get_retry_attempt() &&
	  mdr->session->have_completed_request(req->get_reqid().tid)))
      handle_client_openc(mdr);
    else 
      handle_client_open(mdr);
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
  dout(10) << "prepare_new_inode " 
	   << in->inode.uid << "." << in->inode.gid << " "
	   << *in << dendl;

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
				 refpath, trace, false,
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
  dout(10) << "rdlock_path_pin_ref " << *mdr << dendl;

  // already got ref?
  if (mdr->ref) {
    if (mdr->trace.size()) {
      CDentry *last = mdr->trace[mdr->trace.size()-1];
      assert(last->get_inode() == mdr->ref);
    }
    dout(10) << "rdlock_path_pin_ref had " << *mdr->ref << dendl;
    return mdr->ref;
  }

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

  dout(10) << "rdlock_path_xlock_dentry " << *mdr << dendl;

  if (mdr->ref) {
    CDentry *last = mdr->trace[mdr->trace.size()-1];
    assert(last->get_inode() == mdr->ref);
    dout(10) << "rdlock_path_xlock_dentry had " << *last << " " << *mdr->ref << dendl;
    return last;
  }

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
  if (dn->is_null())
    xlocks.insert(&dn->lock);                 // new dn, xlock
  else
    rdlocks.insert(&dn->lock);  // existing dn, rdlock
  wrlocks.insert(&dn->dir->inode->dirlock); // also, wrlock on dir mtime

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
version_t Server::predirty_dn_diri(MDRequest *mdr, CDentry *dn, EMetaBlob *blob, int deltasize)
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
    pi->size += deltasize;
    blob->add_dir_context(diri->get_parent_dn()->get_dir());
    blob->add_primary_dentry(diri->get_parent_dn(), true, 0, pi);
  } else {
    // journal the mtime change anyway.
    inode_t *ji = blob->add_primary_dentry(diri->get_parent_dn(), true);    
    ji->ctime = ji->mtime = mdr->now;

    dout(10) << "predirty_dn_diri (non-auth) ctime/mtime " << mdr->now << " on " << *diri << dendl;
    
    //blob->add_dirtied_inode_mtime(diri->ino(), mdr->now);
    assert(mdr->ls);
    mdr->ls->dirty_dirfrag_dir.push_back(&diri->xlist_dirty_dirfrag_dir);
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
  if (mask & CEPH_LOCK_ILINK) rdlocks.insert(&ref->linklock);
  if (mask & CEPH_LOCK_IAUTH) rdlocks.insert(&ref->authlock);
  if (ref->is_file() && 
      mask & CEPH_LOCK_ICONTENT) rdlocks.insert(&ref->filelock);
  if (ref->is_dir() &&
      mask & CEPH_LOCK_ICONTENT) rdlocks.insert(&ref->dirlock);
  if (mask & CEPH_LOCK_IXATTR) rdlocks.insert(&ref->xattrlock);

  if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
    return;

  mds->balancer->hit_inode(g_clock.now(), ref, META_POP_IRD,
			   mdr->client_request->get_client_inst().name.num());

  // reply
  dout(10) << "reply to stat on " << *req << dendl;
  reply_request(mdr, 0);
}


void Server::handle_client_findinode(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;
  int r = mdcache->inopath_traverse(mdr, req->inopath);
  if (r > 0)
    return; // delayed
  dout(10) << "reply to findinode on " << *mdr->ref << dendl;
  MClientReply *reply = new MClientReply(req, r);
  reply_request(mdr, reply);
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
    mdr->apply();

    mds->balancer->hit_inode(mdr->now, in, META_POP_IWR);   

    // reply
    MClientReply *reply = new MClientReply(mdr->client_request, 0);
    reply->set_result(0);
    mds->server->reply_request(mdr, reply);
  }
};


// utime

void Server::handle_client_utime(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;
  CInode *cur = rdlock_path_pin_ref(mdr, true);
  __u32 mask;
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
  
  mask = req->head.args.utime.mask;

  if (mask & CEPH_UTIME_MTIME)
    pi->mtime = req->head.args.utime.mtime;
  if (mask & CEPH_UTIME_ATIME)
    pi->atime = req->head.args.utime.atime;

  pi->version = cur->pre_dirty();
  pi->ctime = g_clock.real_now();
  pi->time_warp_seq++;   // maybe not a timewarp, but still a serialization point.

  // log + wait
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "utime");
  le->metablob.add_client_req(req->get_reqid());
  mds->locker->predirty_nested(mdr, &le->metablob, cur, 0, true, false);
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
    (pi->mode & ~07777) | 
    (req->head.args.chmod.mode & 07777);
  pi->version = cur->pre_dirty();
  pi->ctime = g_clock.real_now();
  dout(10) << "chmod " << oct << pi->mode << " (" << req->head.args.chmod.mode << ")" << dec << *cur << dendl;

  // log + wait
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "chmod");
  le->metablob.add_client_req(req->get_reqid());
  mds->locker->predirty_nested(mdr, &le->metablob, cur, 0, true, false);
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
  if ((__s32)req->head.args.chown.uid != -1)
    pi->uid = req->head.args.chown.uid;
  if ((__s32)req->head.args.chown.gid != -1)
    pi->gid = req->head.args.chown.gid;
  pi->version = cur->pre_dirty();
  pi->ctime = g_clock.real_now();
  
  // log + wait
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "chown");
  le->metablob.add_client_req(req->get_reqid());
  mds->locker->predirty_nested(mdr, &le->metablob, cur, 0, true, false);
  le->metablob.add_primary_dentry(cur->parent, true, 0, pi);
  
  mdlog->submit_entry(le, new C_MDS_inode_update_finish(mds, mdr, cur));
}


// XATTRS

void Server::handle_client_setxattr(MDRequest *mdr)
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
  xlocks.insert(&cur->xattrlock);
  if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
    return;

  string name(req->get_path2());
  int flags = req->head.args.setxattr.flags;

  if ((flags & XATTR_CREATE) && cur->xattrs.count(name)) {
    dout(10) << "setxattr '" << name << "' XATTR_CREATE and EEXIST on " << *cur << dendl;
    reply_request(mdr, -EEXIST);
    return;
  }
  if ((flags & XATTR_REPLACE) && !cur->xattrs.count(name)) {
    dout(10) << "setxattr '" << name << "' XATTR_REPLACE and ENODATA on " << *cur << dendl;
    reply_request(mdr, -ENODATA);
    return;
  }

  int len = req->get_data().length();
  dout(10) << "setxattr '" << name << "' len " << len << " on " << *cur << dendl;

  // project update
  inode_t *pi = cur->project_inode();
  pi->version = cur->pre_dirty();
  pi->ctime = g_clock.real_now();

  cur->xattrs.erase(name);
  cur->xattrs[name] = buffer::create(len);
  if (len)
    req->get_data().copy(0, len, cur->xattrs[name].c_str());
  
  // log + wait
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "setxattr");
  le->metablob.add_client_req(req->get_reqid());
  mds->locker->predirty_nested(mdr, &le->metablob, cur, 0, true, false);
  le->metablob.add_primary_dentry(cur->parent, true, 0, pi);
  
  mdlog->submit_entry(le, new C_MDS_inode_update_finish(mds, mdr, cur));
}

void Server::handle_client_removexattr(MDRequest *mdr)
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
  xlocks.insert(&cur->xattrlock);
  if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
    return;

  string name(req->get_path2());
  if (cur->xattrs.count(name) == 0) {
    dout(10) << "removexattr '" << name << "' and ENODATA on " << *cur << dendl;
    reply_request(mdr, -ENODATA);
    return;
  }

  dout(10) << "removexattr '" << name << "' on " << *cur << dendl;

  // project update
  inode_t *pi = cur->project_inode();
  pi->version = cur->pre_dirty();
  pi->ctime = g_clock.real_now();
  cur->xattrs.erase(name);
  
  // log + wait
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "removexattr");
  le->metablob.add_client_req(req->get_reqid());
  mds->locker->predirty_nested(mdr, &le->metablob, cur, 0, true, false);
  le->metablob.add_primary_dentry(cur->parent, true, 0, pi);
  
  mdlog->submit_entry(le, new C_MDS_inode_update_finish(mds, mdr, cur));
}





// =================================================================
// DIRECTORY and NAMESPACE OPS

// READDIR

void Server::handle_client_readdir(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;
  int client = req->get_client().num();
  CInode *diri = rdlock_path_pin_ref(mdr, false);
  if (!diri) return;

  // it's a directory, right?
  if (!diri->is_dir()) {
    // not a dir
    dout(10) << "reply to " << *req << " readdir -ENOTDIR" << dendl;
    reply_request(mdr, -ENOTDIR);
    return;
  }

  // which frag?
  frag_t fg = (__u32)req->head.args.readdir.frag;

  // does the frag exist?
  if (diri->dirfragtree[fg.value()] != fg) {
    dout(10) << "frag " << fg << " doesn't appear in fragtree " << diri->dirfragtree << dendl;
    reply_request(mdr, -EAGAIN);
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

  mdr->now = g_clock.real_now();

  // build dir contents
  bufferlist dirbl, dnbl;
  DirStat::encode(dirbl, dir, mds->get_nodeid());

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
    
    // dentry
    ::encode(it->first, dnbl);
    mds->locker->issue_client_lease(dn, client, dnbl, mdr->now, mdr->session);

    // inode
    InodeStat::encode(dnbl, in);
    mds->locker->issue_client_lease(in, client, dnbl, mdr->now, mdr->session);
    numfiles++;

    // touch dn
    mdcache->lru.lru_touch(dn);
  }
  ::encode(numfiles, dirbl);
  dirbl.claim_append(dnbl);
  
  // yay, reply
  MClientReply *reply = new MClientReply(req);
  reply->set_dir_bl(dirbl);
  
  dout(10) << "reply to " << *req << " readdir " << numfiles << " files" << dendl;
  reply->set_result(0);

  // bump popularity.  NOTE: this doesn't quite capture it.
  mds->balancer->hit_dir(g_clock.now(), dir, META_POP_IRD, -1, numfiles);
  
  // reply
  reply_request(mdr, reply);
}



// ------------------------------------------------

// MKNOD

class C_MDS_mknod_finish : public Context {
  MDS *mds;
  MDRequest *mdr;
  CDentry *dn;
  CInode *newi;
public:
  C_MDS_mknod_finish(MDS *m, MDRequest *r, CDentry *d, CInode *ni) :
    mds(m), mdr(r), dn(d), newi(ni) {}
  void finish(int r) {
    assert(r == 0);

    // link the inode
    dn->get_dir()->link_primary_inode(dn, newi);
    
    // dirty inode, dn, dir
    newi->mark_dirty(newi->inode.version + 1, mdr->ls);

    // mkdir?
    if (newi->inode.is_dir()) { 
      CDir *dir = newi->get_dirfrag(frag_t());
      assert(dir);
      dir->mark_dirty(1, mdr->ls);
    }

    mdr->apply();

    // hit pop
    mds->balancer->hit_inode(mdr->now, newi, META_POP_IWR);
    //mds->balancer->hit_dir(mdr->now, dn->get_dir(), META_POP_DWR);

    // reply
    MClientReply *reply = new MClientReply(mdr->client_request, 0);
    reply->set_result(0);
    mds->server->reply_request(mdr, reply, newi, dn);
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

  newi->projected_parent = dn;
  newi->inode.rdev = req->head.args.mknod.rdev;
  newi->inode.mode = req->head.args.mknod.mode;
  if ((newi->inode.mode & S_IFMT) == 0)
    newi->inode.mode |= S_IFREG;
  newi->inode.version = dn->pre_dirty() - 1;
  newi->inode.dirstat.rfiles = 1;
  
  dout(10) << "mknod mode " << newi->inode.mode << " rdev " << newi->inode.rdev << dendl;

  // prepare finisher
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "mknod");
  le->metablob.add_client_req(req->get_reqid());
  le->metablob.add_allocated_ino(newi->ino(), mds->idalloc->get_version());

  mds->locker->predirty_nested(mdr, &le->metablob, newi, dn->dir, PREDIRTY_PRIMARY|PREDIRTY_DIR, 1);

  le->metablob.add_primary_dentry(dn, true, newi, &newi->inode);
  
  // log + wait
  mdlog->submit_entry(le, new C_MDS_mknod_finish(mds, mdr, dn, newi));
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
  newi->projected_parent = dn;
  newi->inode.mode = req->head.args.mkdir.mode;
  newi->inode.mode &= ~S_IFMT;
  newi->inode.mode |= S_IFDIR;
  newi->inode.layout = g_default_mds_dir_layout;
  newi->inode.version = dn->pre_dirty() - 1;
  newi->inode.dirstat.rsubdirs = 1;

  // ...and that new dir is empty.
  CDir *newdir = newi->get_or_open_dirfrag(mds->mdcache, frag_t());
  newdir->mark_complete();
  newdir->pre_dirty();

  //if (mds->logger) mds->logger->inc("mkdir");

  // prepare finisher
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "mkdir");
  le->metablob.add_client_req(req->get_reqid());
  le->metablob.add_allocated_ino(newi->ino(), mds->idalloc->get_version());
  mds->locker->predirty_nested(mdr, &le->metablob, newi, dn->dir, PREDIRTY_PRIMARY|PREDIRTY_DIR, 1);
  le->metablob.add_primary_dentry(dn, true, newi, &newi->inode);
  le->metablob.add_dir(newdir, true, true); // dirty AND complete
  
  // log + wait
  mdlog->submit_entry(le, new C_MDS_mknod_finish(mds, mdr, dn, newi));

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
  newi->projected_parent = dn;
  newi->inode.mode &= ~S_IFMT;
  newi->inode.mode |= S_IFLNK;
  newi->inode.mode |= 0777;     // ?
  newi->symlink = req->get_path2();
  newi->inode.size = newi->symlink.length();
  newi->inode.version = dn->pre_dirty() - 1;
  newi->inode.dirstat.rfiles = 1;

  // prepare finisher
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "symlink");
  le->metablob.add_client_req(req->get_reqid());
  le->metablob.add_allocated_ino(newi->ino(), mds->idalloc->get_version());
  mds->locker->predirty_nested(mdr, &le->metablob, newi, dn->dir, PREDIRTY_PRIMARY|PREDIRTY_DIR, 1);
  le->metablob.add_primary_dentry(dn, true, newi, &newi->inode);

  // log + wait
  mdlog->submit_entry(le, new C_MDS_mknod_finish(mds, mdr, dn, newi));
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
public:
  C_MDS_link_local_finish(MDS *m, MDRequest *r, CDentry *d, CInode *ti, 
			  version_t dnpv_, version_t tipv_) :
    mds(m), mdr(r), dn(d), targeti(ti),
    dnpv(dnpv_), tipv(tipv_) { }
  void finish(int r) {
    assert(r == 0);
    mds->server->_link_local_finish(mdr, dn, targeti, dnpv, tipv);
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
  mds->locker->predirty_nested(mdr, &le->metablob, targeti, dn->dir, PREDIRTY_DIR, 1); // new dn
  mds->locker->predirty_nested(mdr, &le->metablob, targeti, 0, PREDIRTY_PRIMARY);           // targeti
  le->metablob.add_remote_dentry(dn, true, targeti->ino(), 
				 MODE_TO_DT(targeti->inode.mode));  // new remote
  le->metablob.add_primary_dentry(targeti->parent, true, targeti, pi);  // update old primary

  mdlog->submit_entry(le, new C_MDS_link_local_finish(mds, mdr, dn, targeti, dnpv, tipv));
}

void Server::_link_local_finish(MDRequest *mdr, CDentry *dn, CInode *targeti,
				version_t dnpv, version_t tipv)
{
  dout(10) << "_link_local_finish " << *dn << " to " << *targeti << dendl;

  // link and unlock the NEW dentry
  dn->dir->link_remote_inode(dn, targeti);
  dn->mark_dirty(dnpv, mdr->ls);

  // target inode
  targeti->pop_and_dirty_projected_inode(mdr->ls);

  mdr->apply();
  
  // bump target popularity
  mds->balancer->hit_inode(mdr->now, targeti, META_POP_IWR);
  //mds->balancer->hit_dir(mdr->now, dn->get_dir(), META_POP_DWR);

  // reply
  MClientReply *reply = new MClientReply(mdr->client_request, 0);
  reply_request(mdr, reply, targeti, dn);
}


// remote

class C_MDS_link_remote_finish : public Context {
  MDS *mds;
  MDRequest *mdr;
  CDentry *dn;
  CInode *targeti;
  version_t dpv;
public:
  C_MDS_link_remote_finish(MDS *m, MDRequest *r, CDentry *d, CInode *ti) :
    mds(m), mdr(r), dn(d), targeti(ti),
    dpv(d->get_projected_version()) {}
  void finish(int r) {
    assert(r == 0);
    mds->server->_link_remote_finish(mdr, dn, targeti, dpv);
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
  mds->locker->predirty_nested(mdr, &le->metablob, targeti, dn->dir, PREDIRTY_DIR, 1);
  le->metablob.add_remote_dentry(dn, true, targeti->ino(), 
				 MODE_TO_DT(targeti->inode.mode));  // new remote

  // mark committing (needed for proper recovery)
  mdr->committing = true;

  // log + wait
  mdlog->submit_entry(le, new C_MDS_link_remote_finish(mds, mdr, dn, targeti));
}

void Server::_link_remote_finish(MDRequest *mdr, CDentry *dn, CInode *targeti,
				 version_t dpv)
{
  dout(10) << "_link_remote_finish " << *dn << " to " << *targeti << dendl;

  // link the new dentry
  dn->dir->link_remote_inode(dn, targeti);
  dn->mark_dirty(dpv, mdr->ls);

  mdr->apply();
  
  // bump target popularity
  mds->balancer->hit_inode(mdr->now, targeti, META_POP_IWR);
  //mds->balancer->hit_dir(mdr->now, dn->get_dir(), META_POP_DWR);

  // reply
  MClientReply *reply = new MClientReply(mdr->client_request, 0);
  reply_request(mdr, reply, targeti, dn);  // FIXME: imprecise ref
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

  mdr->auth_pin(targeti);

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

  inode_t *oldi = dn->inode->get_projected_inode();
  inode_t *pi = dn->inode->project_inode();

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
  mds->locker->predirty_nested(mdr, &le->commit, dn->inode, 0, PREDIRTY_PRIMARY, 0, &le->rollback);
  le->commit.add_primary_dentry(dn, true, targeti, pi);  // update old primary
  le->rollback.add_primary_dentry(dn, true, targeti, oldi);

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
  mdr->apply();

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

    // FIXME rctime etc.?
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

  CDentry *straydn = 0;
  if (dn->is_primary()) {
    straydn = mdcache->get_or_create_stray_dentry(dn->inode);
    mdr->pin(straydn);  // pin it.
    dout(10) << " straydn is " << *straydn << dendl;
    assert(straydn->is_null());
  }

  // lock
  set<SimpleLock*> rdlocks, wrlocks, xlocks;

  for (int i=0; i<(int)trace.size()-1; i++)
    rdlocks.insert(&trace[i]->lock);
  xlocks.insert(&dn->lock);
  wrlocks.insert(&dn->dir->inode->dirlock);
  xlocks.insert(&in->linklock);
  if (straydn)
    wrlocks.insert(&straydn->dir->inode->dirlock);
  
  if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
    return;

  // yay!
  mdr->done_locking = true;  // avoid wrlock racing
  if (mdr->now == utime_t())
    mdr->now = g_clock.real_now();

  // get stray dn ready?
  if (dn->is_primary()) {
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
public:
  C_MDS_unlink_local_finish(MDS *m, MDRequest *r, CDentry *d, CDentry *sd) :
    mds(m), mdr(r), dn(d), straydn(sd),
    dnpv(d->get_projected_version()) {}
  void finish(int r) {
    assert(r == 0);
    mds->server->_unlink_local_finish(mdr, dn, straydn, dnpv);
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

  if (dn->is_primary())
    dn->inode->projected_parent = straydn;

  inode_t *pi = dn->inode->project_inode();
  mdr->add_projected_inode(dn->inode);
  pi->version = dn->inode->pre_dirty();
  pi->nlink--;
  pi->ctime = mdr->now;

  if (dn->is_primary()) {
    // primary link.  add stray dentry.
    assert(straydn);
    mds->locker->predirty_nested(mdr, &le->metablob, dn->inode, dn->dir, PREDIRTY_PRIMARY|PREDIRTY_DIR, -1);
    mds->locker->predirty_nested(mdr, &le->metablob, dn->inode, straydn->dir, PREDIRTY_PRIMARY|PREDIRTY_DIR, 1);
    //le->metablob.add_dir_context(straydn->dir);
    le->metablob.add_primary_dentry(straydn, true, dn->inode, pi);
  } else {
    // remote link.  update remote inode.
    mds->locker->predirty_nested(mdr, &le->metablob, dn->inode, dn->dir, PREDIRTY_DIR, -1);
    mds->locker->predirty_nested(mdr, &le->metablob, dn->inode, 0, PREDIRTY_PRIMARY);
    le->metablob.add_primary_dentry(dn->inode->parent, true, dn->inode);
  }

  // the unlinked dentry
  dn->pre_dirty();
  le->metablob.add_null_dentry(dn, true);

  if (mdr->more()->dst_reanchor_atid)
    le->metablob.add_anchor_transaction(mdr->more()->dst_reanchor_atid);

  // log + wait
  mdlog->submit_entry(le, new C_MDS_unlink_local_finish(mds, mdr, dn, straydn));
}

void Server::_unlink_local_finish(MDRequest *mdr, 
				  CDentry *dn, CDentry *straydn,
				  version_t dnpv) 
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

  mdr->apply();
  dn->mark_dirty(dnpv, mdr->ls);  
  
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
  reply_request(mdr, reply, 0, dn);
  
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
public:
  C_MDS_unlink_remote_finish(MDS *m, MDRequest *r, CDentry *d) :
    mds(m), mdr(r), dn(d), 
    dnpv(d->get_projected_version()) { }
  void finish(int r) {
    assert(r == 0);
    mds->server->_unlink_remote_finish(mdr, dn, dnpv);
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
  mds->locker->predirty_nested(mdr, &le->metablob, dn->inode, dn->dir, PREDIRTY_DIR, -1);
  dn->pre_dirty();
  le->metablob.add_null_dentry(dn, true);

  if (mdr->more()->dst_reanchor_atid)
    le->metablob.add_anchor_transaction(mdr->more()->dst_reanchor_atid);

  // finisher
  C_MDS_unlink_remote_finish *fin = new C_MDS_unlink_remote_finish(mds, mdr, dn);
  
  // mark committing (needed for proper recovery)
  mdr->committing = true;

  // log + wait
  mdlog->submit_entry(le, fin);
}

void Server::_unlink_remote_finish(MDRequest *mdr, 
				   CDentry *dn, 
				   version_t dnpv) 
{
  dout(10) << "_unlink_remote_finish " << *dn << dendl;

  // unlink main dentry
  dn->dir->unlink_inode(dn);

  mdr->apply();
  dn->mark_dirty(dnpv, mdr->ls);  // dirty old dentry

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
  reply_request(mdr, reply, 0, dn);  // FIXME: imprecise ref

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
  if (destpath.get_ino() != srcpath.get_ino() &&
      !MDS_INO_IS_STRAY(srcpath.get_ino())) {  // <-- mds 'rename' out of stray dir is ok
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
  if (srcdn->is_primary() && srcdn->inode->is_dir())
    xlocks.insert(&srcdn->inode->dirlock);

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
  if (destdn->is_primary() && destdn->inode->is_dir())
    xlocks.insert(&destdn->inode->dirlock);

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
  reply_request(mdr, reply, destdn->get_inode(), destdn);
  
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
    indis->encode(req->stray);
    dirdis->encode(req->stray);
    dndis->encode(req->stray);
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

  // prepare
  inode_t *pi = 0, *ji = 0;    // renamed inode
  inode_t *tpi = 0, *tji = 0;  // target/overwritten inode
  
  if (linkmerge) {
    dout(10) << "will merge remote+primary links" << dendl;
    
    // destdn -> primary
    tpi = destdn->inode->project_inode();
    //mdr->add_projected_inode(destdn->inode);
    if (destdn->is_auth())
      tpi->version = mdr->more()->pvmap[destdn] = destdn->pre_dirty(destdn->inode->inode.version);
    destdn->inode->projected_parent = destdn;
    
    // do src dentry
    if (srcdn->is_auth())
      mdr->more()->pvmap[srcdn] = srcdn->pre_dirty();
  } else {
    // target?
    if (destdn->is_primary()) {
      assert(straydn);  // moving to straydn.
      // link--, and move.
      if (destdn->is_auth()) {
	tpi = destdn->inode->project_inode();
	//mdr->add_projected_inode(destdn->inode);
	tpi->version = mdr->more()->pvmap[straydn] = straydn->pre_dirty(tpi->version);
      }
      destdn->inode->projected_parent = straydn;
    } else if (destdn->is_remote()) {
      // nlink-- targeti
      if (destdn->inode->is_auth()) {
	tpi = destdn->inode->project_inode();
	//mdr->add_projected_inode(destdn->inode);
	tpi->version = mdr->more()->pvmap[destdn->inode] = destdn->inode->pre_dirty();
      }
    } else
      assert(destdn->is_null());

    // dest
    if (srcdn->is_remote()) {
      if (destdn->is_auth())
	mdr->more()->pvmap[destdn] = destdn->pre_dirty();
      if (srcdn->inode->is_auth()) {
	pi = srcdn->inode->project_inode();
	//mdr->add_projected_inode(srcdn->inode);
	pi->version = srcdn->inode->pre_dirty();
      }
    } else if (srcdn->is_primary()) {
      if (destdn->is_auth()) {
	version_t oldpv;
	if (srcdn->is_auth())
	  oldpv = srcdn->inode->get_projected_version();
	else {
	  oldpv = mdr->more()->inode_import_v;

	  /* import node */
	  bufferlist::iterator blp = mdr->more()->inode_import.begin();
	  
	  // imported caps
	  ::decode(mdr->more()->imported_client_map, blp);
	  ::encode(mdr->more()->imported_client_map, *client_map_bl);
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
	pi = srcdn->inode->project_inode();
	//mdr->add_projected_inode(srcdn->inode);
	pi->version = mdr->more()->pvmap[destdn] = destdn->pre_dirty(oldpv);
      }
      srcdn->inode->projected_parent = destdn;
    }

    // remove src
    if (srcdn->is_auth())
      mdr->more()->pvmap[srcdn] = srcdn->pre_dirty();
  }

  if (pi) {
    pi->ctime = mdr->now;
  }
  if (tpi) {
    tpi->nlink--;
    tpi->ctime = mdr->now;
  }

  // prepare nesting, mtime updates
  if (mdr->is_master()) {
    // sub off target
    if (!linkmerge && destdn->is_primary())
      mds->locker->predirty_nested(mdr, metablob, destdn->inode, destdn->dir, PREDIRTY_PRIMARY|PREDIRTY_DIR, -1);
    if (destdn->dir == srcdn->dir) {
      // same dir.  don't update nested info or adjust counts.
      mds->locker->predirty_nested(mdr, metablob, srcdn->inode, srcdn->dir,
				   false, true);
    } else {
      // different dir.  update nested accounting.
      int flags = srcdn->is_primary() ? PREDIRTY_PRIMARY:0;
      flags |= PREDIRTY_DIR;
      if (srcdn->is_auth())
	mds->locker->predirty_nested(mdr, metablob, srcdn->inode, srcdn->dir, flags, -1);
      mds->locker->predirty_nested(mdr, metablob, srcdn->inode, destdn->dir, flags, 1);
    }
  }

  // add it all to the metablob
  if (linkmerge) {
    metablob->add_primary_dentry(destdn, true, destdn->inode, tpi); 
    metablob->add_null_dentry(srcdn, true);
  } else {
    // target inode
    if (destdn->is_primary())
      tji = metablob->add_primary_dentry(straydn, true, destdn->inode, tpi);
    else if (destdn->is_remote()) {
      metablob->add_dir_context(destdn->inode->get_parent_dir());
      tji = metablob->add_primary_dentry(destdn->inode->parent, true, destdn->inode, tpi);
    }

    // dest
    if (srcdn->is_remote()) {
      metablob->add_remote_dentry(destdn, true, srcdn->get_remote_ino(), 
				  srcdn->get_remote_d_type());
      if (pi)
	metablob->add_primary_dentry(srcdn->inode->get_parent_dn(), true, srcdn->inode, pi);  // ???
    } else if (srcdn->is_primary()) {
      ji = metablob->add_primary_dentry(destdn, true, srcdn->inode, pi); 
    }
    
    // src
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

  // do inode updates in journal, even if we aren't auth (hmm, is this necessary?)
  if (ji && !pi) {
    ji->ctime = mdr->now;
  }
  if (tji && !tpi) {
    tji->nlink--;
    tji->ctime = mdr->now;
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

  if (linkmerge) {
    if (destdn->is_primary()) {
      dout(10) << "merging remote onto primary link" << dendl;

      // nlink-- in place
      if (destdn->inode->is_auth())
	destdn->inode->pop_and_dirty_projected_inode(mdr->ls);
      
      // unlink srcdn
      srcdn->dir->unlink_inode(srcdn);
      if (srcdn->is_auth())
	srcdn->mark_dirty(mdr->more()->pvmap[srcdn], mdr->ls);
    } else {
      dout(10) << "merging primary onto remote link" << dendl;

      // move inode to dest
      srcdn->dir->unlink_inode(srcdn);
      destdn->dir->unlink_inode(destdn);
      destdn->dir->link_primary_inode(destdn, oldin);
      
      // nlink--
      if (destdn->inode->is_auth())
	destdn->inode->pop_and_dirty_projected_inode(mdr->ls);
      
      // mark src dirty
      if (srcdn->is_auth())
	srcdn->mark_dirty(mdr->more()->pvmap[srcdn], mdr->ls);
    }
  } else {
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
      if (oldin->is_auth())
	oldin->pop_and_dirty_projected_inode(mdr->ls);
    }
    else if (oldin) {
      // nlink-- remote.  destdn was remote.
      if (oldin->is_auth())
	oldin->pop_and_dirty_projected_inode(mdr->ls);
    }
    
    CInode *in = srcdn->inode;
    assert(in);
    if (srcdn->is_remote()) {
      // srcdn was remote.
      srcdn->dir->unlink_inode(srcdn);
      destdn->dir->link_remote_inode(destdn, in);
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
    }

    if (destdn->inode->is_auth())
      destdn->inode->pop_and_dirty_projected_inode(mdr->ls);

    if (srcdn->is_auth())
      srcdn->mark_dirty(mdr->more()->pvmap[srcdn], mdr->ls);
  }

  // apply remaining projected inodes
  mdr->apply();

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
    map<__u32,entity_inst_t> exported_client_map;
    bufferlist inodebl;
    mdcache->migrator->encode_export_inode(srcdn->inode, inodebl, 
					   exported_client_map);
    mdcache->migrator->finish_export_inode(srcdn->inode, mdr->now, finished); 
    mds->queue_waiters(finished);   // this includes SINGLEAUTH waiters.
    ::encode(exported_client_map, reply->inode_export);
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
	destdn->dir->link_remote_inode(destdn, in);
      } else {
	srcdn->dir->link_remote_inode(srcdn, in);
      }
    } else {
      // normal

      // revert srcdn
      if (destdn->is_remote()) {
	srcdn->dir->link_remote_inode(srcdn, destdn->inode);
	destdn->dir->unlink_inode(destdn);
      } else {
	// renamed a primary
	CInode *in = destdn->inode;
	destdn->dir->unlink_inode(destdn);
	srcdn->dir->link_primary_inode(srcdn, in);
      }
      
      // revert destdn
      if (mdr->more()->destdn_was_remote_inode) {
	destdn->dir->link_remote_inode(destdn, mdr->more()->destdn_was_remote_inode);
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

    // reply
    mds->server->reply_request(mdr, 0);
  }
};

class C_MDS_truncate_logged : public Context {
  MDS *mds;
  MDRequest *mdr;
  CInode *in;
  version_t pv;
  uint64_t size;
  utime_t ctime;
public:
  C_MDS_truncate_logged(MDS *m, MDRequest *r, CInode *i, version_t pdv, uint64_t sz, utime_t ct) :
    mds(m), mdr(r), in(i), 
    pv(pdv),
    size(sz), ctime(ct) { }
  void finish(int r) {
    assert(r == 0);

    // apply to cache
    in->inode.size = size;
    in->inode.ctime = ctime;
    in->inode.mtime = ctime;
    in->pop_and_dirty_projected_inode(mdr->ls);

    // notify any clients
    mds->locker->issue_truncate(in);

    // purge
    mds->mdcache->purge_inode(in, size, in->inode.size, mdr->ls);
    mds->mdcache->wait_for_purge(in, size, 
				 new C_MDS_truncate_purged(mds, mdr, in, pv, size, ctime));
  }
};

void Server::handle_client_truncate(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;

  if (req->head.args.truncate.length > (__u64)CEPH_FILE_MAX_SIZE) {
    reply_request(mdr, -EFBIG);
    return;
  }

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
  
  // already the correct size?
  if (cur->inode.size == req->head.args.truncate.length) {
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
  le->metablob.add_inode_truncate(cur->ino(), req->head.args.truncate.length, cur->inode.size);
  inode_t *pi = cur->project_inode();
  pi->mtime = ctime;
  pi->ctime = ctime;
  pi->version = pdv;
  pi->size = le64_to_cpu(req->head.args.truncate.length);
  mds->locker->predirty_nested(mdr, &le->metablob, cur, 0, true, false);
  le->metablob.add_primary_dentry(cur->parent, true, 0, pi);
  
  mdlog->submit_entry(le, fin);
}


// ===========================
// open, openc, close

void Server::handle_client_open(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;

  int flags = req->head.args.open.flags;
  int cmode = ceph_flags_to_mode(req->head.args.open.flags);

  bool need_auth = !file_mode_is_readonly(cmode) || (flags & O_TRUNC);

  dout(7) << "open on " << req->get_filepath() << dendl;

  CInode *cur = rdlock_path_pin_ref(mdr, need_auth);
  if (!cur) return;

  // can only open a dir with mode FILE_MODE_PIN, at least for now.
  if (cur->inode.is_dir()) cmode = CEPH_FILE_MODE_PIN;

  dout(10) << "open flags = " << flags
	   << ", filemode = " << cmode
	   << ", need_auth = " << need_auth
	   << dendl;
  
  // regular file?
  if (!cur->inode.is_file() && !cur->inode.is_dir()) {
    dout(7) << "not a file or dir " << *cur << dendl;
    reply_request(mdr, -EINVAL);                 // FIXME what error do we want?
    return;
  }
  if ((req->head.args.open.flags & O_DIRECTORY) && !cur->inode.is_dir()) {
    dout(7) << "specified O_DIRECTORY on non-directory " << *cur << dendl;
    reply_request(mdr, -EINVAL);
    return;
  }
  
  // hmm, check permissions or something.

  // O_TRUNC
  if ((flags & O_TRUNC) &&
      !(req->get_retry_attempt() &&
	mdr->session->have_completed_request(req->get_reqid().tid))) {
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
  int cmode = ceph_flags_to_mode(req->head.args.open.flags);
  if (cur->inode.is_dir()) cmode = CEPH_FILE_MODE_PIN;

  // register new cap
  Capability *cap = mds->locker->issue_new_caps(cur, cmode, mdr->session);

  // drop our locks (they may interfere with us issuing new caps)
  mdcache->request_drop_locks(mdr);

  cap->set_suppress(false);  // stop suppressing messages on this cap

  dout(12) << "_do_open issued caps " << cap_string(cap->pending())
	   << " for " << req->get_source()
	   << " on " << *cur << dendl;
  
  // hit pop
  mdr->now = g_clock.now();
  if (cmode == CEPH_FILE_MODE_RDWR ||
      cmode == CEPH_FILE_MODE_WR) 
    mds->balancer->hit_inode(mdr->now, cur, META_POP_IWR);
  else
    mds->balancer->hit_inode(mdr->now, cur, META_POP_IRD, 
			     mdr->client_request->get_client_inst().name.num());

  // reply
  MClientReply *reply = new MClientReply(req, 0);
  reply->set_file_caps(cap->pending());
  reply->set_file_caps_seq(cap->get_last_seq());
  //reply->set_file_data_version(fdv);
  reply_request(mdr, reply);

  // make sure this inode gets into the journal
  if (cur->xlist_open_file.get_xlist() == 0) {
    LogSegment *ls = mds->mdlog->get_current_segment();
    EOpen *le = new EOpen(mds->mdlog);
    le->add_clean_inode(cur);
    ls->open_files.push_back(&cur->xlist_open_file);
    mds->mdlog->submit_entry(le);
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

    // apply to cache
    in->inode.size = 0;
    in->inode.ctime = ctime;
    in->inode.mtime = ctime;
    in->pop_and_dirty_projected_inode(mdr->ls);
    
    // notify any clients
    mds->locker->issue_truncate(in);

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
  le->metablob.add_inode_truncate(cur->ino(), 0, cur->inode.size);
  inode_t *pi = cur->project_inode();
  pi->mtime = ctime;
  pi->ctime = ctime;
  pi->version = pdv;
  pi->size = 0;
  mds->locker->predirty_nested(mdr, &le->metablob, cur, 0, true, false);
  le->metablob.add_primary_dentry(cur->parent, true, 0, pi);
  
  mdlog->submit_entry(le, fin);
}



class C_MDS_openc_finish : public Context {
  MDS *mds;
  MDRequest *mdr;
  CDentry *dn;
  CInode *newi;
public:
  C_MDS_openc_finish(MDS *m, MDRequest *r, CDentry *d, CInode *ni) :
    mds(m), mdr(r), dn(d), newi(ni) {}
  void finish(int r) {
    assert(r == 0);

    // link the inode
    dn->get_dir()->link_primary_inode(dn, newi);

    // dirty inode, dn, dir
    newi->mark_dirty(newi->inode.version + 1, mdr->ls);

    mdr->apply();

    // downgrade xlock to rdlock
    //mds->locker->dentry_xlock_downgrade_to_rdlock(dn, mdr);

    // set/pin ref inode for open()
    mdr->ref = newi;
    mdr->pin(newi);
    mdr->trace.push_back(dn);

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
      reply_request(mdr, -EEXIST, dn->get_inode(), dn);
      return;
    } 
    
    // pass to regular open handler.
    mdr->trace.push_back(dn);
    mdr->ref = dn->get_inode();
    handle_client_open(mdr);
    return;
  }

  // created null dn.
    
  // create inode.
  mdr->now = g_clock.real_now();
  CInode *in = prepare_new_inode(mdr, dn->dir);
  assert(in);
  
  // it's a file.
  in->projected_parent = dn;
  in->inode.mode = req->head.args.open.mode;
  in->inode.mode |= S_IFREG;
  in->inode.version = dn->pre_dirty() - 1;
  in->inode.max_size = in->get_layout_size_increment();
  in->inode.dirstat.rfiles = 1;
  
  // prepare finisher
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "openc");
  le->metablob.add_client_req(req->get_reqid());
  le->metablob.add_allocated_ino(in->ino(), mds->idalloc->get_version());
  mds->locker->predirty_nested(mdr, &le->metablob, in, dn->dir, PREDIRTY_PRIMARY|PREDIRTY_DIR, 1);
  le->metablob.add_primary_dentry(dn, true, in, &in->inode);
  
  // log + wait
  C_MDS_openc_finish *fin = new C_MDS_openc_finish(mds, mdr, dn, in);
  mdlog->submit_entry(le, fin);
  
  /*
    FIXME. this needs to be rewritten when the write capability stuff starts
    getting journaled.  
  */
}














