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
#include "InoTable.h"
#include "SnapClient.h"

#include "msg/Messenger.h"

#include "messages/MClientSession.h"
#include "messages/MClientRequest.h"
#include "messages/MClientReply.h"
#include "messages/MClientReconnect.h"
#include "messages/MClientCaps.h"
#include "messages/MClientSnap.h"

#include "messages/MMDSSlaveRequest.h"

#include "messages/MLock.h"

#include "messages/MDentryUnlink.h"

#include "events/EString.h"
#include "events/EUpdate.h"
#include "events/ESlaveUpdate.h"
#include "events/ESession.h"
#include "events/EOpen.h"
#include "events/ECommitted.h"

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

#define DOUT_SUBSYS mds
#undef dout_prefix
#define dout_prefix *_dout << dbeginl << "mds" << mds->get_nodeid() << ".server "


void Server::reopen_logger(utime_t start, bool append)
{
  static LogType mdserver_logtype(l_mdss_first, l_mdss_last);
  static bool didit = false;
  if (!didit) {
    didit = true;
    mdserver_logtype.add_inc(l_mdss_hcreq,"hcreq"); // handle client req
    mdserver_logtype.add_inc(l_mdss_hsreq, "hsreq"); // slave
    mdserver_logtype.add_inc(l_mdss_hcsess, "hcsess");    // client session
    mdserver_logtype.add_inc(l_mdss_dcreq, "dcreq"); // dispatch client req
    mdserver_logtype.add_inc(l_mdss_dsreq, "dsreq"); // slave
    mdserver_logtype.validate();
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
  deque<inodeno_t> inos;
  version_t inotablev;
public:
  C_MDS_session_finish(MDS *m, Session *se, bool s, version_t mv) :
    mds(m), session(se), open(s), cmapv(mv), inotablev(0) { }
  C_MDS_session_finish(MDS *m, Session *se, bool s, version_t mv, deque<inodeno_t>& i, version_t iv) :
    mds(m), session(se), open(s), cmapv(mv), inotablev(iv) {
    inos.swap(i);
  }
  void finish(int r) {
    assert(r == 0);
    mds->server->_session_logged(session, open, cmapv, inos, inotablev);
  }
};


void Server::handle_client_session(MClientSession *m)
{
  version_t pv, piv = 0;
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
    mdlog->flush();
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
    {
      if (!session || session->is_closing()) {
	dout(10) << "already closing|dne, dropping this req" << dendl;
	return;
      }
      if (m->seq < session->get_push_seq()) {
	dout(10) << "old push seq " << m->seq << " < " << session->get_push_seq() 
		 << ", dropping" << dendl;
	return;
      }
      if (m->seq != session->get_push_seq()) {
	dout(10) << "old push seq " << m->seq << " != " << session->get_push_seq() 
		 << ", BUGGY!" << dendl;
	assert(0);
      }
      mds->sessionmap.set_state(session, Session::STATE_CLOSING);
      pv = ++mds->sessionmap.projected;
      
      deque<inodeno_t> both = session->prealloc_inos;
      both.insert(both.end(), session->pending_prealloc_inos.begin(), 
		  session->pending_prealloc_inos.end());
      if (both.size()) {
	mds->inotable->project_release_ids(both);
	piv = mds->inotable->get_projected_version();
      } else
	piv = 0;
      
      mdlog->submit_entry(new ESession(m->get_source_inst(), false, pv, both, piv),
			  new C_MDS_session_finish(mds, session, false, pv, both, piv));
      mdlog->flush();
    }
    break;

  default:
    assert(0);
  }
}

void Server::_session_logged(Session *session, bool open, version_t pv, deque<inodeno_t>& inos, version_t piv)
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
      dout(20) << " killing capability " << ccap_string(cap->issued()) << " on " << *in << dendl;
      mds->mdcache->remove_client_cap(in, session->inst.name.num());
    }
    while (!session->leases.empty()) {
      ClientLease *r = session->leases.front();
      MDSCacheObject *p = r->parent;
      dout(20) << " killing client lease of " << *p << dendl;
      p->remove_client_lease(r, r->mask, mds->locker);
    }
    
    if (piv) {
      mds->inotable->apply_release_ids(inos);
      assert(mds->inotable->get_version() == piv);
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
    mds->sessionmap.touch_session(session);
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
    mdlog->flush();
  }
}


void Server::find_idle_sessions()
{
  dout(10) << "find_idle_sessions" << dendl;
  
  // timeout/stale
  //  (caps go stale, lease die)
  utime_t now = g_clock.now();
  utime_t cutoff = now;
  cutoff -= g_conf.mds_session_timeout;  
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
    mds->locker->remove_stale_leases(session);
    mds->messenger->send_message(new MClientSession(CEPH_SESSION_STALE, session->get_push_seq()),
				 session->inst);
  }

  // autoclose
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
    mdlog->flush();
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
    mdlog->flush();
  } else {
    
    // snaprealms
    for (vector<ceph_mds_snaprealm_reconnect>::iterator p = m->realms.begin();
	 p != m->realms.end();
	 p++) {
      CInode *in = mdcache->get_inode(inodeno_t(p->ino));
      if (in) {
	assert(in->snaprealm);
	if (in->snaprealm->have_past_parents_open()) {
	  dout(15) << "open snaprealm (w/ past parents) on " << *in << dendl;
	  mdcache->finish_snaprealm_reconnect(from, in->snaprealm, snapid_t(p->seq));
	} else {
	  dout(15) << "open snaprealm (w/o past parents) on " << *in << dendl;
	  mdcache->add_reconnected_snaprealm(from, inodeno_t(p->ino), snapid_t(p->seq));
	}
      } else {
	dout(15) << "open snaprealm (w/o inode) on " << inodeno_t(p->ino)
		 << " seq " << p->seq << dendl;
	mdcache->add_reconnected_snaprealm(from, inodeno_t(p->ino), snapid_t(p->seq));
      }
    }

    // caps
    for (map<inodeno_t, cap_reconnect_t>::iterator p = m->caps.begin();
	 p != m->caps.end();
	 ++p) {
      // make sure our last_cap_id is MAX over all issued caps
      if (p->second.capinfo.cap_id > mdcache->last_cap_id)
	mdcache->last_cap_id = p->second.capinfo.cap_id;

      CInode *in = mdcache->get_inode(p->first);
      if (in && in->is_auth()) {
	// we recovered it, and it's ours.  take note.
	dout(15) << "open cap realm " << inodeno_t(p->second.capinfo.snaprealm)
		 << " on " << *in << dendl;
	in->reconnect_cap(from, p->second.capinfo, session, &mdcache->client_rdcaps);
	mds->mdcache->add_reconnected_cap(in, from, inodeno_t(p->second.capinfo.snaprealm));
	continue;
      }
      
      filepath path(p->second.path, (__u64)p->second.capinfo.pathbase);
      if ((in && !in->is_auth()) ||
	  !mds->mdcache->path_is_mine(path)) {
	// not mine.
	dout(0) << "non-auth " << p->first << " " << path
		<< ", will pass off to authority" << dendl;
	
	// mark client caps stale.
	inode_t fake_inode;
	memset(&fake_inode, 0, sizeof(fake_inode));
	fake_inode.ino = p->first;
	MClientCaps *stale = new MClientCaps(CEPH_CAP_OP_EXPORT, p->first, 0, 0, 0);
	//stale->head.migrate_seq = 0; // FIXME ******
	mds->send_message_client(stale, m->get_source_inst());

	// add to cap export list.
	mdcache->rejoin_export_caps(p->first, from, p->second);
      } else {
	// mine.  fetch later.
	dout(0) << "missing " << p->first << " " << path
		<< " (mine), will load later" << dendl;
	mdcache->rejoin_recovered_caps(p->first, from, p->second, 
				       -1);  // "from" me.
      }
    }
  }

  // remove from gather set
  client_reconnect_gather.erase(from);
  if (client_reconnect_gather.empty())
    reconnect_gather_finish();

  delete m;
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
      Session *session = mds->sessionmap.get_session(entity_name_t::CLIENT(*p));
      dout(1) << "reconnect gave up on " << session->inst << dendl;

      /* no, we need to respect g_conf.mds_session_autoclose
      // since we are reconnecting, cheat a bit and don't project anything.
      mds->sessionmap.projected++;
      mds->sessionmap.version++;
      mdlog->submit_entry(new ESession(session->inst, false, mds->sessionmap.version));
      mds->messenger->mark_down(session->inst.addr);
      */

      failed_reconnects++;
    }
    client_reconnect_gather.clear();
    reconnect_gather_finish();
  }
}


/*******
 * some generic stuff for finishing off requests
 */

void Server::journal_and_reply(MDRequest *mdr, CInode *in, CDentry *dn, LogEvent *le, Context *fin)
{
  dout(10) << "journal_and_reply tracei " << in << " tracedn " << dn << dendl;

  // note trace items for eventual reply.
  mdr->tracei = in;
  if (in && in != mdr->ref)
    mdr->pin(in);

  mdr->tracedn = dn;
  if (dn && (mdr->trace.empty() ||
	     mdr->trace.back() != dn))
    mdr->pin(dn);

  early_reply(mdr, in, dn);

  mdr->committing = true;
  mdlog->submit_entry(le, fin,
		      mdr->did_ino_allocation());
  
  if (mdr->did_early_reply)
    mds->locker->drop_rdlocks(mdr);
  else
    mdlog->flush();
}

/*
 * send generic response (just an error code)
 */
void Server::reply_request(MDRequest *mdr, int r, CInode *tracei, CDentry *tracedn)
{
  reply_request(mdr, new MClientReply(mdr->client_request, r), tracei, tracedn);
}

void Server::early_reply(MDRequest *mdr, CInode *tracei, CDentry *tracedn)
{
  if (!g_conf.mds_early_reply)
    return;

  if (mdr->alloc_ino) {
    dout(10) << "early_reply - allocated ino, not allowed" << dendl;
    return;
  }

  MClientRequest *req = mdr->client_request;
  entity_inst_t client_inst = req->get_orig_source_inst();
  if (client_inst.name.is_mds())
    return;

  MClientReply *reply = new MClientReply(mdr->client_request, 0);
  reply->set_unsafe();

  // mark xlocks "done", indicating that we are exposing uncommitted changes
  mds->locker->set_xlocks_done(mdr);

  dout(10) << "early_reply " << reply->get_result() 
	   << " (" << strerror(-reply->get_result())
	   << ") " << *req << dendl;

  if (tracei || tracedn)
    set_trace_dist(mdr->session, reply, tracei, tracedn, mdr->ref_snapid, mdr->ref_snapdiri,
		   mdr->client_request->is_replay(),
		   mdr->client_request->get_dentry_wanted());

  messenger->send_message(reply, client_inst);

  mdr->did_early_reply = true;
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
  if (req->is_write() && mdr->session)
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

  // get tracei/tracedn from mdr?
  snapid_t snapid = mdr->ref_snapid;
  CInode *snapdiri = mdr->ref_snapdiri;
  if (!tracei)
    tracei = mdr->tracei;
  if (!tracedn)
    tracedn = mdr->tracedn;

  // give any preallocated inos to the session
  apply_allocated_inos(mdr);

  bool is_replay = mdr->client_request->is_replay();

  // clean up request, drop locks, etc.
  // do this before replying, so that we can issue leases
  Session *session = mdr->session;
  bool did_early_reply = mdr->did_early_reply;
  entity_inst_t client_inst = req->get_orig_source_inst();
  int dentry_wanted = req->get_dentry_wanted();
  mdcache->request_finish(mdr);
  mdr = 0;

  // reply at all?
  if (client_inst.name.is_mds()) {
    delete reply;   // mds doesn't need a reply
    reply = 0;
  } else {
    // send reply, with trace, and possible leases
    if (!did_early_reply &&   // don't issue leases if we sent an earlier reply already
	(tracei || tracedn)) 
      set_trace_dist(session, reply, tracei, tracedn, snapid, snapdiri, is_replay, dentry_wanted);
    messenger->send_message(reply, client_inst);
  }
  
  // take a closer look at tracei, if it happens to be a remote link
  if (tracei && 
      tracei->get_parent_dn() &&
      tracei->get_parent_dn()->get_projected_linkage()->is_remote())
    mdcache->eval_remote(tracei->get_parent_dn());
}


void Server::encode_empty_dirstat(bufferlist& bl)
{
  static DirStat empty;
  empty.encode(bl);
}

void Server::encode_infinite_lease(bufferlist& bl)
{
  LeaseStat e;
  e.seq = 0;
  e.mask = -1;
  e.duration_ms = -1;
  ::encode(e, bl);
  dout(20) << "encode_infinite_lease " << e << dendl;
}

void Server::encode_null_lease(bufferlist& bl)
{
  LeaseStat e;
  e.seq = 0;
  e.mask = 0;
  e.duration_ms = 0;
  ::encode(e, bl);
  dout(20) << "encode_null_lease " << e << dendl;
}


/*
 * pass inode OR dentry (not both, or we may get confused)
 *
 * trace is in reverse order (i.e. root inode comes last)
 */
void Server::set_trace_dist(Session *session, MClientReply *reply,
			    CInode *in, CDentry *dn,
			    snapid_t snapid, CInode *snapdiri,
			    bool is_replay, int dentry_wanted)
{
  // inode, dentry, dir, ..., inode
  bufferlist bl;
  int whoami = mds->get_nodeid();
  int client = session->get_client();
  utime_t now = g_clock.now();

  dout(20) << "set_trace_dist snapid " << snapid << dendl;

  //assert((bool)dn == (bool)dentry_wanted);  // not true for snapshot lookups

  // realm
  SnapRealm *realm = 0;
  if (in)
    realm = in->find_snaprealm();
  else
    realm = dn->get_dir()->get_inode()->find_snaprealm();
  reply->snapbl = realm->get_snap_trace();
  dout(10) << "set_trace_dist snaprealm " << *realm << " len=" << reply->snapbl.length() << dendl;

  // dir + dentry?
  if (dn) {
    reply->head.is_dentry = 1;
    CDir *dir = dn->get_dir();
    CInode *diri = dir->get_inode();

    diri->encode_inodestat(bl, session, NULL, snapid, is_replay);
    dout(20) << "set_trace_dist added diri " << *diri << dendl;

#ifdef MDS_VERIFY_FRAGSTAT
    if (dir->is_complete())
      dir->verify_fragstat();
#endif
    dir->encode_dirstat(bl, whoami);
    dout(20) << "set_trace_dist added dir  " << *dir << dendl;

    ::encode(dn->get_name(), bl);
    if (snapid == CEPH_NOSNAP)
      mds->locker->issue_client_lease(dn, client, bl, now, session);
    else
      encode_null_lease(bl);
    dout(20) << "set_trace_dist added dn   " << snapid << " " << *dn << dendl;
    /*} else if (snapdiri && snapdiri->ino() == in->ino() && snapname.length()) {
    // fake a snapname dentry
    dout(20) << "set_trace_dist added fake snap dir+dn   " << snapname << " under " << *snapdiri << dendl;
    reply->head.is_dentry = 1;
    snapdiri->encode_inodestat(bl, session, NULL, CEPH_SNAPDIR, is_replay);
    encode_empty_dirstat(bl);
    ::encode(snapname, bl);
    encode_infinite_lease(bl);
    */
  } else
    reply->head.is_dentry = 0;

  // inode
  if (in) {
    in->encode_inodestat(bl, session, NULL, snapid, is_replay);
    dout(20) << "set_trace_dist added in   " << *in << dendl;
    reply->head.is_target = 1;
  } else
    reply->head.is_target = 0;

  reply->set_trace(bl);
}




/***
 * process a client request
 */
void Server::handle_client_request(MClientRequest *req)
{
  dout(4) << "handle_client_request " << *req << dendl;

  if (logger) logger->inc(l_mdss_hcreq);

  if (!mds->is_active() &&
      !(mds->is_stopping() && req->get_orig_source().is_mds())) {
    dout(5) << " not active (or stopping+mds), discarding request." << dendl;
    delete req;
    return;
  }
  
  if (!mdcache->is_open()) {
    dout(5) << "waiting for root" << dendl;
    mdcache->wait_for_open(new C_MDS_RetryMessage(mds, req));
    return;
  }

  // active session?
  Session *session = 0;
  if (req->get_orig_source().is_client()) {
    session = mds->sessionmap.get_session(req->get_orig_source());
    if (!session) {
      dout(5) << "no session for " << req->get_orig_source() << ", dropping" << dendl;
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
      ((req->get_op() != CEPH_MDS_OP_OPEN) && 
       (req->get_op() != CEPH_MDS_OP_CREATE))) {
    assert(session);
    if (session->have_completed_request(req->get_reqid().tid)) {
      dout(5) << "already completed " << req->get_reqid() << dendl;
      mds->messenger->send_message(new MClientReply(req, 0), req->get_orig_source_inst());
      delete req;
      return;
    }
  }
  // trim completed_request list
  if (req->get_oldest_client_tid() > 0) {
    dout(15) << " oldest_client_tid=" << req->get_oldest_client_tid() << dendl;
    session->trim_completed_requests(req->get_oldest_client_tid());
  }

  // process embedded cap releases?
  if (req->get_source().is_client()) {
    int client = req->get_source().num();
    for (vector<MClientRequest::Release>::iterator p = req->releases.begin();
	 p != req->releases.end();
	 p++) {
      mds->locker->process_cap_update(client, inodeno_t((__u64)p->item.ino), p->item.cap_id, p->item.caps,
				      p->item.seq, p->item.mseq, p->dname);
    }
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

  if (logger) logger->inc(l_mdss_dcreq);

  if (mdr->ref) {
    dout(7) << "dispatch_client_request " << *req << " ref " << *mdr->ref << dendl;
  } else {
    dout(7) << "dispatch_client_request " << *req << dendl;
  }

  // we shouldn't be waiting on anyone.
  assert(mdr->more()->waiting_on_slave.empty());
  
  switch (req->get_op()) {
  case CEPH_MDS_OP_LOOKUPHASH:
    handle_client_lookup_hash(mdr);
    break;

    // inodes ops.
  case CEPH_MDS_OP_LOOKUP:
  case CEPH_MDS_OP_LOOKUPSNAP:
  case CEPH_MDS_OP_GETATTR:
    handle_client_stat(mdr);
    break;

  case CEPH_MDS_OP_SETATTR:
    handle_client_setattr(mdr);
    break;
  case CEPH_MDS_OP_SETLAYOUT:
    handle_client_setlayout(mdr);
    break;
  case CEPH_MDS_OP_SETXATTR:
    handle_client_setxattr(mdr);
    break;
  case CEPH_MDS_OP_RMXATTR:
    handle_client_removexattr(mdr);
    break;

  case CEPH_MDS_OP_READDIR:
    handle_client_readdir(mdr);
    break;

    // funky.
  case CEPH_MDS_OP_CREATE:
    if (req->get_retry_attempt() &&
	mdr->session->have_completed_request(req->get_reqid().tid))
      handle_client_open(mdr);  // already created.. just open
    else
      handle_client_openc(mdr);
    break;

  case CEPH_MDS_OP_OPEN:
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


    // snaps
  case CEPH_MDS_OP_LSSNAP:
    handle_client_lssnap(mdr);
    break;
  case CEPH_MDS_OP_MKSNAP:
    handle_client_mksnap(mdr);
    break;
  case CEPH_MDS_OP_RMSNAP:
    handle_client_rmsnap(mdr);
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

  if (logger) logger->inc(l_mdss_hsreq);

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
	lock->get_xlock(mdr, mdr->get_client());
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

    case MMDSSlaveRequest::OP_COMMITTED:
      {
	metareqid_t r = m->get_reqid();
	mds->mdcache->committed_master_slave(r, from);
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

  if (logger) logger->inc(l_mdss_dsreq);

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

  for (vector<MDSCacheObjectInfo>::iterator p = mdr->slave_request->get_authpins().begin();
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
  for (vector<MDSCacheObjectInfo>::iterator p = ack->get_authpins().begin();
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
  
  int client = mdr->get_client();

  // does it already exist?
  CDentry *dn = dir->lookup(dname);
  if (dn) {
    /*
    if (dn->lock.is_xlocked_by_other(mdr)) {
      dout(10) << "waiting on xlocked dentry " << *dn << dendl;
      dn->lock.add_waiter(SimpleLock::WAIT_RD, new C_MDS_RetryRequest(mdcache, mdr));
      return 0;
    }
    */
    if (!dn->get_linkage(client, mdr)->is_null()) {
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
CInode* Server::prepare_new_inode(MDRequest *mdr, CDir *dir, inodeno_t useino) 
{
  CInode *in = new CInode(mdcache);
  
  // assign ino
  if (mdr->session->prealloc_inos.size()) {
    mdr->used_prealloc_ino = 
      in->inode.ino = mdr->session->take_ino(useino);  // prealloc -> used
    mds->sessionmap.projected++;
    dout(10) << "prepare_new_inode used_prealloc " << mdr->used_prealloc_ino
	     << " (" << mdr->session->prealloc_inos.size() << " left)"
	     << dendl;
  } else {
    mdr->alloc_ino = 
      in->inode.ino = mds->inotable->project_alloc_id();
    dout(10) << "prepare_new_inode alloc " << mdr->alloc_ino << dendl;
  }

  if (useino && useino != in->inode.ino) {
    dout(0) << "WARNING: client specified " << useino << " and i allocated " << in->inode.ino << dendl;
    stringstream ss;
    ss << mdr->client_request->get_orig_source() << " specified ino " << useino << " but mds" << mds->whoami
       << " allocated " << in->inode.ino;
    mds->logclient.log(LOG_ERROR, ss);
    assert(0); // just for now.
  }
    
  int got = g_conf.mds_client_prealloc_inos - mdr->session->get_num_projected_prealloc_inos();
  if (got > 0) {
    mds->inotable->project_alloc_ids(mdr->prealloc_inos, got);
    assert(mdr->prealloc_inos.size());  // or else fix projected increment semantics
    mdr->session->pending_prealloc_inos.insert(mdr->session->pending_prealloc_inos.end(),
					       mdr->prealloc_inos.begin(), mdr->prealloc_inos.end());
    mds->sessionmap.projected++;
    dout(10) << "prepare_new_inode prealloc " << mdr->prealloc_inos << dendl;
  }

  in->inode.version = 1;
  in->inode.nlink = 1;   // FIXME
  in->inode.layout = g_default_file_layout;

  in->inode.truncate_size = -1ull;  // not truncated, yet!

  in->inode.uid = mdr->client_request->get_caller_uid();
  in->inode.gid = mdr->client_request->get_caller_gid();
  in->inode.ctime = in->inode.mtime = in->inode.atime = mdr->now;   // now

  mdcache->add_inode(in);  // add
  dout(10) << "prepare_new_inode " << *in << dendl;
  return in;
}

void Server::journal_allocated_inos(MDRequest *mdr, EMetaBlob *blob)
{
  dout(20) << "journal_allocated_inos sessionmapv " << mds->sessionmap.projected
	   << " inotablev " << mds->inotable->get_projected_version() << dendl;
  blob->set_ino_alloc(mdr->alloc_ino,
		      mdr->used_prealloc_ino,
		      mdr->prealloc_inos,
		      mdr->client_request->get_orig_source(),
		      mds->sessionmap.projected,
		      mds->inotable->get_projected_version());
}

void Server::apply_allocated_inos(MDRequest *mdr)
{
  Session *session = mdr->session;
  dout(10) << "apply_allocated_inos " << mdr->alloc_ino
	   << " / " << mdr->prealloc_inos
	   << " / " << mdr->used_prealloc_ino << dendl;

  if (mdr->alloc_ino) {
    mds->inotable->apply_alloc_id(mdr->alloc_ino);
  }
  if (mdr->prealloc_inos.size()) {
    for (deque<inodeno_t>::iterator p = mdr->prealloc_inos.begin();
	 p != mdr->prealloc_inos.end();
	 p++) {
      assert(session->pending_prealloc_inos.front() == *p);
      session->prealloc_inos.push_back(session->pending_prealloc_inos.front());
      session->pending_prealloc_inos.pop_front();
    }
    mds->sessionmap.version++;
    mds->inotable->apply_alloc_ids(mdr->prealloc_inos);
  }
  if (mdr->used_prealloc_ino) {
    assert(session->used_inos.front() == mdr->used_prealloc_ino);
    session->used_inos.pop_front();
    mds->sessionmap.version++;
  }
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
  int r = mdcache->path_traverse(mdr, mdr->client_request, refpath, trace, MDS_TRAVERSE_FORWARD);
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
    diri = mdcache->get_dentry_inode(trace[trace.size()-1], mdr, true);
  if (!diri) 
    return 0; // opening inode.

  // is it an auth dir?
  CDir *dir = validate_dentry_dir(mdr, diri, dname);
  if (!dir)
    return 0; // forwarded or waiting for freeze

  dout(10) << "traverse_to_auth_dir " << *dir << dendl;
  return dir;
}



CInode* Server::rdlock_path_pin_ref(MDRequest *mdr,
				    bool want_auth, bool rdlock_dft)
{
  dout(10) << "rdlock_path_pin_ref " << *mdr << dendl;

  // already got ref?
  if (mdr->ref) {
    if (mdr->trace.size()) {
      CDentry *last = mdr->trace[mdr->trace.size()-1];
      assert(last->get_projected_inode() == mdr->ref);
    }
    dout(10) << "rdlock_path_pin_ref had snap " << mdr->ref_snapid << " " << *mdr->ref << dendl;
    return mdr->ref;
  }

  MClientRequest *req = mdr->client_request;
  int client = mdr->get_client();

  // traverse
  filepath refpath = req->get_filepath();
  vector<CDentry*> trace;
  int r = mdcache->path_traverse(mdr, req, refpath, trace, MDS_TRAVERSE_FORWARD);
  if (r > 0) return false; // delayed
  if (r < 0) {  // error
    reply_request(mdr, r);
    return 0;
  }

  // open ref inode
  CInode *ref = 0;
  if (trace.empty()) {
    ref = mdcache->get_inode(refpath.get_ino());
    if (!ref) {
      ref = mdcache->get_inode(refpath.get_ino());
      if (!ref->is_multiversion()) {
	reply_request(mdr, -ESTALE);
	return 0;
      }
    }
  } else {
    CDentry *dn = trace[trace.size()-1];
    bool dnp = dn->use_projected(client, mdr);
    CDentry::linkage_t *dnl = dnp ? dn->get_projected_linkage() : dn->get_linkage();

    // if no inode (null or unattached remote), fw to dentry auth?
    if (want_auth && !dn->is_auth() &&
	(dnl->is_null() ||
	 (dnl->is_remote() && dnl->get_inode()))) {
      if (dn->is_ambiguous_auth()) {
	dout(10) << "waiting for single auth on " << *dn << dendl;
	dn->get_dir()->add_waiter(CInode::WAIT_SINGLEAUTH, new C_MDS_RetryRequest(mdcache, mdr));
      } else {
	dout(10) << "fw to auth for " << *dn << dendl;
	mdcache->request_forward(mdr, dn->authority().first);
	return 0;
      }
    }

    // open ref inode
    ref = mdcache->get_dentry_inode(dn, mdr, dnp);
    if (!ref) return 0;
  }
  dout(10) << "ref is " << *ref << dendl;

  // fw to inode auth?
  if (mdr->ref_snapid != CEPH_NOSNAP)
    want_auth = true;

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
  if (rdlock_dft)
    rdlocks.insert(&ref->dirfragtreelock);
  mds->locker->include_snap_rdlocks(rdlocks, ref);

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

  int client = mdr->get_client();

  if (mdr->ref) {
    CDentry *last = mdr->trace[mdr->trace.size()-1];
    assert(last->get_projected_inode() == mdr->ref);
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

  CInode *diri = dir->get_inode();
  if (diri->ino() < MDS_INO_SYSTEM_BASE && !diri->is_root()) {
    reply_request(mdr, -EROFS);
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
    if (dn && !dn->lock.can_read(client) && dn->lock.get_xlocked_by() != mdr) {
      dout(10) << "waiting on xlocked dentry " << *dn << dendl;
      dn->lock.add_waiter(SimpleLock::WAIT_RD, new C_MDS_RetryRequest(mdcache, mdr));
      return 0;
    }
      
    // exists?
    if (!dn || dn->get_linkage(client, mdr)->is_null()) {
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
  if (dn->get_linkage(client, mdr)->is_null())
    xlocks.insert(&dn->lock);                 // new dn, xlock
  else
    rdlocks.insert(&dn->lock);  // existing dn, rdlock
  wrlocks.insert(&dn->get_dir()->inode->filelock); // also, wrlock on dir mtime
  wrlocks.insert(&dn->get_dir()->inode->nestlock); // also, wrlock on dir mtime
  mds->locker->include_snap_rdlocks(rdlocks, dn->get_dir()->inode);

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

  /*
   * if client currently holds the EXCL cap on a field, do not rdlock
   * it; client's stat() will result in valid info if _either_ EXCL
   * cap is held or MDS rdlocks and reads the value here.
   *
   * handling this case here is easier than weakening rdlock
   * semantics... that would cause problems elsewhere.
   */
  int client = mdr->get_client();
  int issued = 0;
  Capability *cap = ref->get_client_cap(client);
  if (cap)
    issued = cap->issued();

  int mask = req->head.args.stat.mask;
  if ((mask & CEPH_CAP_LINK_RDCACHE) && (issued & CEPH_CAP_LINK_EXCL) == 0) rdlocks.insert(&ref->linklock);
  if ((mask & CEPH_CAP_AUTH_RDCACHE) && (issued & CEPH_CAP_AUTH_EXCL) == 0) rdlocks.insert(&ref->authlock);
  if ((mask & CEPH_CAP_FILE_RDCACHE) && (issued & CEPH_CAP_FILE_EXCL) == 0) rdlocks.insert(&ref->filelock);
  if ((mask & CEPH_CAP_XATTR_RDCACHE) && (issued & CEPH_CAP_XATTR_EXCL) == 0) rdlocks.insert(&ref->xattrlock);

  if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
    return;

  mds->balancer->hit_inode(g_clock.now(), ref, META_POP_IRD,
			   mdr->client_request->get_orig_source().num());

  // reply
  dout(10) << "reply to stat on " << *req << dendl;
  reply_request(mdr, 0, ref,
		req->get_op() == CEPH_MDS_OP_LOOKUP ? mdr->trace.back() : 0);
}


void Server::handle_client_lookup_hash(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;

  CInode *in = mdcache->get_inode(req->get_filepath().get_ino());
  if (!in) {
    // try the directory
    CInode *diri = mdcache->get_inode(req->get_filepath2().get_ino());
    if (!diri) {
      reply_request(mdr, -ESTALE);
      return;
    }
    unsigned hash = atoi(req->get_filepath2()[0].c_str());
    frag_t fg = diri->dirfragtree[hash];
    CDir *dir = diri->get_or_open_dirfrag(mdcache, fg);
    assert(dir);
    if (!dir->is_auth()) {
      if (dir->is_ambiguous_auth()) {
	// wait
	dout(7) << " waiting for single auth in " << *dir << dendl;
	dir->add_waiter(CDir::WAIT_SINGLEAUTH, new C_MDS_RetryRequest(mdcache, mdr));
	return;
      } 
      mdcache->request_forward(mdr, dir->authority().first);
      return;
    }
    if (!dir->is_complete()) {
      dir->fetch(0, new C_MDS_RetryRequest(mdcache, mdr));
      return;
    }
    reply_request(mdr, -ESTALE);
    return;
  }

  dout(10) << "reply to lookup_hash on " << *in << dendl;
  MClientReply *reply = new MClientReply(req, 0);
  reply_request(mdr, reply, in, in->get_parent_dn());
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
  bool smaller;
public:
  C_MDS_inode_update_finish(MDS *m, MDRequest *r, CInode *i, bool sm=false) :
    mds(m), mdr(r), in(i), smaller(sm) { }
  void finish(int r) {
    assert(r == 0);

    // apply
    in->pop_and_dirty_projected_inode(mdr->ls);
    mdr->apply();

    // notify any clients
    if (smaller && in->inode.is_truncating()) {
      mds->locker->issue_truncate(in);
      mds->mdcache->truncate_inode(in, mdr->ls);
    }

    mds->balancer->hit_inode(mdr->now, in, META_POP_IWR);   

    mds->server->reply_request(mdr, 0);
  }
};


void Server::handle_client_setattr(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;
  CInode *cur = rdlock_path_pin_ref(mdr, true);
  if (!cur) return;

  if (mdr->ref_snapid != CEPH_NOSNAP) {
    reply_request(mdr, -EINVAL);
    return;
  }
  if (cur->ino() < MDS_INO_SYSTEM_BASE && !cur->is_root()) {
    reply_request(mdr, -EPERM);
    return;
  }

  __u32 mask = req->head.args.setattr.mask;

  // xlock inode
  set<SimpleLock*> rdlocks = mdr->rdlocks;
  set<SimpleLock*> wrlocks = mdr->wrlocks;
  set<SimpleLock*> xlocks = mdr->xlocks;

  if (mask & (CEPH_SETATTR_MODE|CEPH_SETATTR_UID|CEPH_SETATTR_GID))
    xlocks.insert(&cur->authlock);
  if (mask & (CEPH_SETATTR_MTIME|CEPH_SETATTR_ATIME|CEPH_SETATTR_SIZE))
    xlocks.insert(&cur->filelock);

  mds->locker->include_snap_rdlocks(rdlocks, cur);
  if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
    return;

  // trunc from bigger -> smaller?
  inode_t *pi = cur->get_projected_inode();
  __u64 old_size = MAX(pi->size, req->head.args.setattr.old_size);
  bool smaller = req->head.args.setattr.size < old_size;
  if ((mask & CEPH_SETATTR_SIZE) && smaller && pi->is_truncating()) {
    dout(10) << " waiting for pending truncate from " << pi->truncate_from
	     << " to " << pi->truncate_size << " to complete on " << *cur << dendl;
    cur->add_waiter(CInode::WAIT_TRUNC, new C_MDS_RetryRequest(mdcache, mdr));
    mds->mdlog->flush();
    return;
  }

  // project update
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "setattr");

  pi = cur->project_inode();

  if (mask & CEPH_SETATTR_MODE)
    pi->mode = (pi->mode & ~07777) | (req->head.args.setattr.mode & 07777);
  if (mask & CEPH_SETATTR_UID)
    pi->uid = req->head.args.setattr.uid;
  if (mask & CEPH_SETATTR_GID)
    pi->gid = req->head.args.setattr.gid;

  if (mask & CEPH_SETATTR_MTIME)
    pi->mtime = req->head.args.setattr.mtime;
  if (mask & CEPH_SETATTR_ATIME)
    pi->atime = req->head.args.setattr.atime;
  if (mask & (CEPH_SETATTR_ATIME | CEPH_SETATTR_MTIME))
    pi->time_warp_seq++;   // maybe not a timewarp, but still a serialization point.
  if (mask & CEPH_SETATTR_SIZE) {
    if (smaller) {
      pi->truncate_from = old_size;
      pi->size = req->head.args.setattr.size;
      pi->truncate_size = pi->size;
      pi->truncate_seq++;
      le->metablob.add_truncate_start(cur->ino());
    } else {
      pi->size = req->head.args.setattr.size;
    }
    pi->rstat.rbytes = pi->size;
  }

  pi->version = cur->pre_dirty();
  pi->ctime = g_clock.real_now();

  // log + wait
  le->metablob.add_client_req(req->get_reqid());
  mdcache->predirty_journal_parents(mdr, &le->metablob, cur, 0, PREDIRTY_PRIMARY, false);
  mdcache->journal_dirty_inode(mdr, &le->metablob, cur);
  
  journal_and_reply(mdr, cur, 0, le, new C_MDS_inode_update_finish(mds, mdr, cur, smaller));

  // flush immediately if there are readers/writers waiting
  if (cur->get_caps_wanted() & (CEPH_CAP_FILE_RD|CEPH_CAP_FILE_WR))
    mds->mdlog->flush();
}



void Server::handle_client_setlayout(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;
  CInode *cur = rdlock_path_pin_ref(mdr, true);
  if (!cur) return;

  if (mdr->ref_snapid != CEPH_NOSNAP || cur->is_root()) {
    reply_request(mdr, -EINVAL);   // for now
    return;
  }
  if (cur->is_dir()) {
    reply_request(mdr, -EISDIR);
    return;
  }
  
  if (cur->get_projected_inode()->size ||
      cur->get_projected_inode()->truncate_seq) {
    reply_request(mdr, -ENOTEMPTY);
    return;
  }


  // write
  set<SimpleLock*> rdlocks = mdr->rdlocks;
  set<SimpleLock*> wrlocks = mdr->wrlocks;
  set<SimpleLock*> xlocks = mdr->xlocks;
  xlocks.insert(&cur->filelock);
  mds->locker->include_snap_rdlocks(rdlocks, cur);

  if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
    return;

  // project update
  inode_t *pi = cur->project_inode();
  pi->layout = req->head.args.setlayout.layout;
  pi->version = cur->pre_dirty();
  pi->ctime = g_clock.real_now();
  
  // log + wait
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "setlayout");
  le->metablob.add_client_req(req->get_reqid());
  mdcache->predirty_journal_parents(mdr, &le->metablob, cur, 0, PREDIRTY_PRIMARY, false);
  mdcache->journal_dirty_inode(mdr, &le->metablob, cur);
  
  journal_and_reply(mdr, cur, 0, le, new C_MDS_inode_update_finish(mds, mdr, cur));
}


// XATTRS

class C_MDS_inode_xattr_update_finish : public Context {
  MDS *mds;
  MDRequest *mdr;
  CInode *in;
public:

  C_MDS_inode_xattr_update_finish(MDS *m, MDRequest *r, CInode *i) :
    mds(m), mdr(r), in(i) { }
  void finish(int r) {
    assert(r == 0);

    // apply
    in->pop_and_dirty_projected_inode(mdr->ls);
    
    mdr->apply();

    mds->balancer->hit_inode(mdr->now, in, META_POP_IWR);   

    mds->server->reply_request(mdr, 0);
  }
};

void Server::handle_client_setxattr(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;
  CInode *cur = rdlock_path_pin_ref(mdr, true);
  if (!cur) return;

  if (mdr->ref_snapid != CEPH_NOSNAP || cur->is_root()) {
    reply_request(mdr, -EINVAL);   // for now
    return;
  }

  // write
  set<SimpleLock*> rdlocks = mdr->rdlocks;
  set<SimpleLock*> wrlocks = mdr->wrlocks;
  set<SimpleLock*> xlocks = mdr->xlocks;
  xlocks.insert(&cur->xattrlock);
  mds->locker->include_snap_rdlocks(rdlocks, cur);

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
  map<string,bufferptr> *px = new map<string,bufferptr>;
  inode_t *pi = cur->project_inode(px);
  pi->version = cur->pre_dirty();
  pi->ctime = g_clock.real_now();
  pi->xattr_version++;
  px->erase(name);
  (*px)[name] = buffer::create(len);
  if (len)
    req->get_data().copy(0, len, (*px)[name].c_str());

  // log + wait
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "setxattr");
  le->metablob.add_client_req(req->get_reqid());
  mdcache->predirty_journal_parents(mdr, &le->metablob, cur, 0, PREDIRTY_PRIMARY, false);
  mdcache->journal_cow_inode(mdr, &le->metablob, cur);
  le->metablob.add_primary_dentry(cur->get_projected_parent_dn(), true, cur, 0, 0, px);

  journal_and_reply(mdr, cur, 0, le, new C_MDS_inode_update_finish(mds, mdr, cur));
}

void Server::handle_client_removexattr(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;
  CInode *cur = rdlock_path_pin_ref(mdr, true);
  if (!cur) return;

  if (mdr->ref_snapid != CEPH_NOSNAP || cur->is_root()) {
    reply_request(mdr, -EINVAL);   // for now
    return;
  }

  // write
  set<SimpleLock*> rdlocks = mdr->rdlocks;
  set<SimpleLock*> wrlocks = mdr->wrlocks;
  set<SimpleLock*> xlocks = mdr->xlocks;
  xlocks.insert(&cur->xattrlock);
  mds->locker->include_snap_rdlocks(rdlocks, cur);

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
  map<string,bufferptr> *px = new map<string,bufferptr>;
  inode_t *pi = cur->project_inode(px);
  pi->version = cur->pre_dirty();
  pi->ctime = g_clock.real_now();
  pi->xattr_version++;
  px->erase(name);

  // log + wait
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "removexattr");
  le->metablob.add_client_req(req->get_reqid());
  mdcache->predirty_journal_parents(mdr, &le->metablob, cur, 0, PREDIRTY_PRIMARY, false);
  mdcache->journal_cow_inode(mdr, &le->metablob, cur);
  le->metablob.add_primary_dentry(cur->get_projected_parent_dn(), true, cur, 0, 0, px);

  journal_and_reply(mdr, cur, 0, le, new C_MDS_inode_update_finish(mds, mdr, cur));
}


// =================================================================
// DIRECTORY and NAMESPACE OPS

// READDIR

void Server::handle_client_readdir(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;
  int client = req->get_orig_source().num();
  CInode *diri = rdlock_path_pin_ref(mdr, false, true);  // rdlock dirfragtreelock!
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
  dout(10) << "handle_client_readdir on " << *dir << dendl;
  assert(dir->is_auth());

  if (!dir->is_complete()) {
    // fetch
    dout(10) << " incomplete dir contents for readdir on " << *dir << ", fetching" << dendl;
    dir->fetch(new C_MDS_RetryRequest(mdcache, mdr));
    return;
  }

#ifdef MDS_VERIFY_FRAGSTAT
  dir->verify_fragstat();
#endif

  mdr->now = g_clock.real_now();

  snapid_t snapid = mdr->ref_snapid;
  dout(10) << "snapid " << snapid << dendl;


  // purge stale snap data?
  const set<snapid_t> *snaps = 0;
  SnapRealm *realm = diri->find_snaprealm();
  if (realm->get_last_destroyed() > dir->fnode.snap_purged_thru) {
    snaps = &realm->get_snaps();
    dout(10) << " last_destroyed " << realm->get_last_destroyed() << " > " << dir->fnode.snap_purged_thru
	     << ", doing snap purge with " << *snaps << dendl;
    dir->fnode.snap_purged_thru = realm->get_last_destroyed();
    assert(snapid == CEPH_NOSNAP || snaps->count(snapid));  // just checkin'! 
  }

  // build dir contents
  bufferlist dnbl;

  CDir::map_t::iterator it = dir->begin(); 

  unsigned max = req->head.args.readdir.max_entries;
  if (!max)
    max = dir->get_num_any();  // whatever, something big.

  nstring offset_str = req->get_path2();
  const char *offset = offset_str.length() ? offset_str.c_str() : 0;

  __u32 numfiles = 0;
  while (it != dir->end() && numfiles < max) {
    CDentry *dn = it->second;
    it++;

    if (offset && strcmp(dn->get_name().c_str(), offset) <= 0)
      continue;

    bool dnp = dn->use_projected(client, mdr);
    CDentry::linkage_t *dnl = dnp ? dn->get_projected_linkage() : dn->get_linkage();

    if (dnl->is_null())
      continue;
    if (snaps && dn->last != CEPH_NOSNAP) {
      set<snapid_t>::const_iterator p = snaps->lower_bound(dn->first);
      if (p == snaps->end() || *p > dn->last) {
	dir->remove_dentry(dn);
	continue;
      }
    }
    if (dn->last < snapid || dn->first > snapid)
      continue;

    CInode *in = dnl->get_inode();

    // remote link?
    // better for the MDS to do the work, if we think the client will stat any of these files.
    if (dnl->is_remote() && !in) {
      in = mdcache->get_inode(dnl->get_remote_ino());
      if (in) {
	dn->link_remote(dnl, in);
      } else if (dn->state_test(CDentry::STATE_BADREMOTEINO)) {
	dout(10) << "skipping bad remote ino on " << *dn << dendl;
	continue;
      } else {
	mdcache->open_remote_dentry(dn, dnp, new C_MDS_RetryRequest(mdcache, mdr));

	// touch everything i _do_ have
	for (it = dir->begin(); 
	     it != dir->end(); 
	     it++) 
	  if (!it->second->get_linkage()->is_null())
	    mdcache->lru.lru_touch(it->second);
	return;
      }
    }
    assert(in);

    // dentry
    dout(12) << "including    dn " << *dn << dendl;
    ::encode(dn->name, dnbl);
    mds->locker->issue_client_lease(dn, client, dnbl, mdr->now, mdr->session);

    // inode
    dout(12) << "including inode " << *in << dendl;
    bool valid = in->encode_inodestat(dnbl, mdr->session, realm, snapid);
    assert(valid);
    numfiles++;

    // touch dn
    mdcache->lru.lru_touch(dn);
  }
  
  __u8 end = (it == dir->end());
  __u8 complete = (end && !offset);

  // final blob
  bufferlist dirbl;
  dir->encode_dirstat(dirbl, mds->get_nodeid());
  ::encode(numfiles, dirbl);
  ::encode(end, dirbl);
  ::encode(complete, dirbl);
  dirbl.claim_append(dnbl);
  
  if (snaps)
    dir->log_mark_dirty();

  // yay, reply
  MClientReply *reply = new MClientReply(req, 0);
  reply->set_dir_bl(dirbl);
  dout(10) << "reply to " << *req << " readdir num=" << numfiles << " end=" << (int)end
	   << " complete=" << (int)complete << dendl;

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
  snapid_t follows;
public:
  C_MDS_mknod_finish(MDS *m, MDRequest *r, CDentry *d, CInode *ni, snapid_t f) :
    mds(m), mdr(r), dn(d), newi(ni), follows(f) {}
  void finish(int r) {
    assert(r == 0);

    // link the inode
    dn->pop_projected_linkage();
    
    // be a bit hacky with the inode version, here.. we decrement it
    // just to keep mark_dirty() happen. (we didn't bother projecting
    // a new version of hte inode since it's just been created)
    newi->inode.version--; 
    newi->mark_dirty(newi->inode.version + 1, mdr->ls);

    // mkdir?
    if (newi->inode.is_dir()) { 
      CDir *dir = newi->get_dirfrag(frag_t());
      assert(dir);
      dir->mark_dirty(1, mdr->ls);
      dir->mark_new(mdr->ls);
    }

    mdr->apply();

    // hit pop
    mds->balancer->hit_inode(mdr->now, newi, META_POP_IWR);
    //mds->balancer->hit_dir(mdr->now, dn->get_dir(), META_POP_DWR);

    // reply
    MClientReply *reply = new MClientReply(mdr->client_request, 0);
    reply->set_result(0);
    mds->server->reply_request(mdr, reply);
  }
};


void Server::handle_client_mknod(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;
  
  CDentry *dn = rdlock_path_xlock_dentry(mdr, false, false);
  if (!dn) return;

  snapid_t follows = dn->get_dir()->inode->find_snaprealm()->get_newest_seq();
  mdr->now = g_clock.real_now();

  CInode *newi = prepare_new_inode(mdr, dn->get_dir(), inodeno_t(req->head.ino));
  assert(newi);

  dn->push_projected_linkage(newi);

  newi->inode.rdev = req->head.args.mknod.rdev;
  newi->inode.mode = req->head.args.mknod.mode;
  if ((newi->inode.mode & S_IFMT) == 0)
    newi->inode.mode |= S_IFREG;
  newi->inode.version = dn->pre_dirty();
  newi->inode.rstat.rfiles = 1;

  dn->first = newi->first = follows+1;
    
  dout(10) << "mknod mode " << newi->inode.mode << " rdev " << newi->inode.rdev << dendl;

  // prepare finisher
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "mknod");
  le->metablob.add_client_req(req->get_reqid());
  journal_allocated_inos(mdr, &le->metablob);
  
  mdcache->predirty_journal_parents(mdr, &le->metablob, newi, dn->get_dir(),
				    PREDIRTY_PRIMARY|PREDIRTY_DIR, 1);
  le->metablob.add_primary_dentry(dn, true, newi);

  journal_and_reply(mdr, newi, dn, le, new C_MDS_mknod_finish(mds, mdr, dn, newi, follows));
}



// MKDIR

void Server::handle_client_mkdir(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;
  
  CDentry *dn = rdlock_path_xlock_dentry(mdr, false, false);
  if (!dn) return;

  // new inode
  SnapRealm *realm = dn->get_dir()->inode->find_snaprealm();
  snapid_t follows = realm->get_newest_seq();
  mdr->now = g_clock.real_now();

  CInode *newi = prepare_new_inode(mdr, dn->get_dir(), inodeno_t(req->head.ino));  
  assert(newi);

  // it's a directory.
  dn->push_projected_linkage(newi);

  newi->inode.mode = req->head.args.mkdir.mode;
  newi->inode.mode &= ~S_IFMT;
  newi->inode.mode |= S_IFDIR;
  newi->inode.layout = g_default_mds_dir_layout;
  newi->inode.version = dn->pre_dirty();
  newi->inode.rstat.rsubdirs = 1;

  dn->first = newi->first = follows+1;

  // ...and that new dir is empty.
  CDir *newdir = newi->get_or_open_dirfrag(mds->mdcache, frag_t());
  newdir->mark_complete();
  newdir->pre_dirty();

  // prepare finisher
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "mkdir");
  le->metablob.add_client_req(req->get_reqid());
  journal_allocated_inos(mdr, &le->metablob);
  mdcache->predirty_journal_parents(mdr, &le->metablob, newi, dn->get_dir(), PREDIRTY_PRIMARY|PREDIRTY_DIR, 1);
  le->metablob.add_primary_dentry(dn, true, newi);
  le->metablob.add_dir(newdir, true, true, true); // dirty AND complete AND new
  
  // issue a cap on the directory
  int cmode = CEPH_FILE_MODE_RDWR;
  Capability *cap = mds->locker->issue_new_caps(newi, cmode, mdr->session, realm);
  cap->set_wanted(0);

  // put locks in excl mode
  newi->filelock.set_state(LOCK_EXCL);
  newi->authlock.set_state(LOCK_EXCL);
  cap->issue_norevoke(CEPH_CAP_AUTH_EXCL|CEPH_CAP_AUTH_RDCACHE);

  // make sure this inode gets into the journal
  le->metablob.add_opened_ino(newi->ino());
  LogSegment *ls = mds->mdlog->get_current_segment();
  ls->open_files.push_back(&newi->xlist_open_file);

  journal_and_reply(mdr, newi, dn, le, new C_MDS_mknod_finish(mds, mdr, dn, newi, follows));
}


// SYMLINK

void Server::handle_client_symlink(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;
  
  CDentry *dn = rdlock_path_xlock_dentry(mdr, false, false);
  if (!dn) return;

  mdr->now = g_clock.real_now();
  snapid_t follows = dn->get_dir()->inode->find_snaprealm()->get_newest_seq();

  CInode *newi = prepare_new_inode(mdr, dn->get_dir(), inodeno_t(req->head.ino));
  assert(newi);

  // it's a symlink
  dn->push_projected_linkage(newi);

  newi->inode.mode &= ~S_IFMT;
  newi->inode.mode |= S_IFLNK;
  newi->inode.mode |= 0777;     // ?
  newi->symlink = req->get_path2();
  newi->inode.size = newi->symlink.length();
  newi->inode.rstat.rbytes = newi->inode.size;
  newi->inode.rstat.rfiles = 1;
  newi->inode.version = dn->pre_dirty();

  dn->first = newi->first = follows+1;

  // prepare finisher
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "symlink");
  le->metablob.add_client_req(req->get_reqid());
  journal_allocated_inos(mdr, &le->metablob);
  mdcache->predirty_journal_parents(mdr, &le->metablob, newi, dn->get_dir(), PREDIRTY_PRIMARY|PREDIRTY_DIR, 1);
  le->metablob.add_primary_dentry(dn, true, newi);

  journal_and_reply(mdr, newi, dn, le, new C_MDS_mknod_finish(mds, mdr, dn, newi, follows));
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
  int r = mdcache->path_traverse(mdr, req, targetpath, targettrace, MDS_TRAVERSE_DISCOVER);
  if (r > 0) return; // wait
  if (targettrace.empty()) r = -EINVAL; 
  if (r < 0) {
    reply_request(mdr, r);
    return;
  }
  
  // identify target inode.
  //  use projected target; we'll rdlock the dentry to ensure it's correct.
  CInode *targeti = mdcache->get_dentry_inode(targettrace[targettrace.size()-1], mdr, true);
  if (!targeti)
    return;

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
  wrlocks.insert(&dn->get_dir()->inode->filelock);
  wrlocks.insert(&dn->get_dir()->inode->nestlock);
  for (int i=0; i<(int)targettrace.size(); i++)
    rdlocks.insert(&targettrace[i]->lock);
  xlocks.insert(&targeti->linklock);
  mds->locker->include_snap_rdlocks(rdlocks, targeti);
  mds->locker->include_snap_rdlocks(rdlocks, dn->get_dir()->inode);

  if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
    return;

  mdr->done_locking = true;  // avoid wrlock moving target issues.
  
  // pick mtime
  if (mdr->now == utime_t())
    mdr->now = g_clock.real_now();

  // does the target need an anchor?
  if (targeti->is_auth()) {
    if (targeti->is_anchored()) {
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
    _link_remote(mdr, true, dn, targeti);
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

  snapid_t follows = dn->get_dir()->inode->find_snaprealm()->get_newest_seq();
  dn->first = follows+1;

  // log + wait
  EUpdate *le = new EUpdate(mdlog, "link_local");
  le->metablob.add_client_req(mdr->reqid);
  mdcache->predirty_journal_parents(mdr, &le->metablob, targeti, dn->get_dir(), PREDIRTY_DIR, 1);      // new dn
  mdcache->predirty_journal_parents(mdr, &le->metablob, targeti, 0, PREDIRTY_PRIMARY);           // targeti
  le->metablob.add_remote_dentry(dn, true, targeti->ino(), targeti->d_type());  // new remote
  mdcache->journal_dirty_inode(mdr, &le->metablob, targeti);

  // do this after predirty_*, to avoid funky extra dnl arg
  dn->push_projected_linkage(targeti->ino(), targeti->d_type());

  journal_and_reply(mdr, targeti, dn, le, new C_MDS_link_local_finish(mds, mdr, dn, targeti, dnpv, tipv));
}

void Server::_link_local_finish(MDRequest *mdr, CDentry *dn, CInode *targeti,
				version_t dnpv, version_t tipv)
{
  dout(10) << "_link_local_finish " << *dn << " to " << *targeti << dendl;

  // link and unlock the NEW dentry
  dn->pop_projected_linkage();
  dn->mark_dirty(dnpv, mdr->ls);

  // target inode
  targeti->pop_and_dirty_projected_inode(mdr->ls);

  mdr->apply();
  
  // bump target popularity
  mds->balancer->hit_inode(mdr->now, targeti, META_POP_IWR);
  //mds->balancer->hit_dir(mdr->now, dn->get_dir(), META_POP_DWR);

  // reply
  MClientReply *reply = new MClientReply(mdr->client_request, 0);
  reply_request(mdr, reply);
}


// link / unlink remote

class C_MDS_link_remote_finish : public Context {
  MDS *mds;
  MDRequest *mdr;
  bool inc;
  CDentry *dn;
  CInode *targeti;
  version_t dpv;
public:
  C_MDS_link_remote_finish(MDS *m, MDRequest *r, bool i, CDentry *d, CInode *ti) :
    mds(m), mdr(r), inc(i), dn(d), targeti(ti),
    dpv(d->get_projected_version()) {}
  void finish(int r) {
    assert(r == 0);
    mds->server->_link_remote_finish(mdr, inc, dn, targeti, dpv);
  }
};

void Server::_link_remote(MDRequest *mdr, bool inc, CDentry *dn, CInode *targeti)
{
  dout(10) << "_link_remote " 
	   << (inc ? "link ":"unlink ")
	   << *dn << " to " << *targeti << dendl;

  // 1. send LinkPrepare to dest (journal nlink++ prepare)
  int linkauth = targeti->authority().first;
  if (mdr->more()->witnessed.count(linkauth) == 0) {
    dout(10) << " targeti auth must prepare nlink++/--" << dendl;

    int op;
    if (inc)
      op = MMDSSlaveRequest::OP_LINKPREP;
    else 
      op = MMDSSlaveRequest::OP_UNLINKPREP;
    MMDSSlaveRequest *req = new MMDSSlaveRequest(mdr->reqid, op);
    targeti->set_object_info(req->get_object_info());
    req->now = mdr->now;
    mds->send_message_mds(req, linkauth);

    assert(mdr->more()->waiting_on_slave.count(linkauth) == 0);
    mdr->more()->waiting_on_slave.insert(linkauth);
    return;
  }
  dout(10) << " targeti auth has prepared nlink++/--" << dendl;

  //assert(0);  // test hack: verify that remote slave can do a live rollback.

  // add to event
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, inc ? "link_remote":"unlink_remote");
  le->metablob.add_client_req(mdr->reqid);
  if (!mdr->more()->slaves.empty()) {
    dout(20) << " noting uncommitted_slaves " << mdr->more()->slaves << dendl;
    le->reqid = mdr->reqid;
    le->had_slaves = true;
    mds->mdcache->add_uncommitted_master(mdr->reqid, mdr->ls, mdr->more()->slaves);
  }

  if (inc) {
    dn->pre_dirty();
    mdcache->predirty_journal_parents(mdr, &le->metablob, targeti, dn->get_dir(), PREDIRTY_DIR, 1);
    le->metablob.add_remote_dentry(dn, true, targeti->ino(), targeti->d_type()); // new remote
  } else {
    dn->pre_dirty();
    mdcache->predirty_journal_parents(mdr, &le->metablob, targeti, dn->get_dir(), PREDIRTY_DIR, -1);
    mdcache->journal_cow_dentry(mdr, &le->metablob, dn);
    le->metablob.add_null_dentry(dn, true);
  }

  if (mdr->more()->dst_reanchor_atid)
    le->metablob.add_table_transaction(TABLE_ANCHOR, mdr->more()->dst_reanchor_atid);

  // mark committing (needed for proper recovery)
  mdr->committing = true;

  journal_and_reply(mdr, targeti, dn, le, new C_MDS_link_remote_finish(mds, mdr, inc, dn, targeti));
}

void Server::_link_remote_finish(MDRequest *mdr, bool inc,
				 CDentry *dn, CInode *targeti,
				 version_t dpv)
{
  dout(10) << "_link_remote_finish "
	   << (inc ? "link ":"unlink ")
	   << *dn << " to " << *targeti << dendl;

  if (inc) {
    // link the new dentry
    dn->pop_projected_linkage();
    dn->mark_dirty(dpv, mdr->ls);
  } else {
    // unlink main dentry
    dn->get_dir()->unlink_inode(dn);
    dn->mark_dirty(dn->get_projected_version(), mdr->ls);  // dirty old dentry

    // share unlink news with replicas
    for (map<int,int>::iterator it = dn->replicas_begin();
	 it != dn->replicas_end();
	 it++) {
      dout(7) << "_link_remote_finish sending MDentryUnlink to mds" << it->first << dendl;
      MDentryUnlink *unlink = new MDentryUnlink(dn->get_dir()->dirfrag(), dn->name);
      mds->send_message_mds(unlink, it->first);
    }
  }

  mdr->apply();
  
  // commit anchor update?
  if (mdr->more()->dst_reanchor_atid) 
    mds->anchorclient->commit(mdr->more()->dst_reanchor_atid, mdr->ls);

  // bump target popularity
  mds->balancer->hit_inode(mdr->now, targeti, META_POP_IWR);
  //mds->balancer->hit_dir(mdr->now, dn->get_dir(), META_POP_DWR);

  // reply
  MClientReply *reply = new MClientReply(mdr->client_request, 0);
  reply_request(mdr, reply);

  if (!inc)
    // removing a new dn?
    dn->get_dir()->try_remove_unlinked_dn(dn);
}


// remote linking/unlinking

class C_MDS_SlaveLinkPrep : public Context {
  Server *server;
  MDRequest *mdr;
  CInode *targeti;
public:
  C_MDS_SlaveLinkPrep(Server *s, MDRequest *r, CInode *t) :
    server(s), mdr(r), targeti(t) { }
  void finish(int r) {
    assert(r == 0);
    server->_logged_slave_link(mdr, targeti);
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
  CDentry::linkage_t *dnl = dn->get_linkage();
  assert(dnl->is_primary());

  mdr->now = mdr->slave_request->now;

  mdr->auth_pin(targeti);

  //assert(0);  // test hack: make sure master can handle a slave that fails to prepare...

  // anchor?
  if (mdr->slave_request->get_op() == MMDSSlaveRequest::OP_LINKPREP) {
    if (targeti->is_anchored()) {
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
  ESlaveUpdate *le = new ESlaveUpdate(mdlog, "slave_link_prep", mdr->reqid, mdr->slave_to_mds,
				      ESlaveUpdate::OP_PREPARE, ESlaveUpdate::LINK);

  inode_t *pi = dnl->get_inode()->project_inode();

  // update journaled target inode
  bool inc;
  if (mdr->slave_request->get_op() == MMDSSlaveRequest::OP_LINKPREP) {
    inc = true;
    pi->nlink++;
  } else {
    inc = false;
    pi->nlink--;
  }

  link_rollback rollback;
  rollback.reqid = mdr->reqid;
  rollback.ino = targeti->ino();
  rollback.old_ctime = targeti->inode.ctime;   // we hold versionlock xlock; no concorrent projections
  fnode_t *pf = targeti->get_parent_dn()->get_dir()->get_projected_fnode();
  rollback.old_dir_mtime = pf->fragstat.mtime;
  rollback.old_dir_rctime = pf->rstat.rctime;
  rollback.was_inc = inc;
  ::encode(rollback, le->rollback);
  mdr->more()->rollback_bl = le->rollback;

  pi->ctime = mdr->now;
  pi->version = targeti->pre_dirty();

  dout(10) << " projected inode " << pi << " v " << pi->version << dendl;

  // commit case
  mdcache->predirty_journal_parents(mdr, &le->commit, dnl->get_inode(), 0, PREDIRTY_SHALLOW|PREDIRTY_PRIMARY, 0);
  mdcache->journal_dirty_inode(mdr, &le->commit, targeti);

  mdlog->submit_entry(le, new C_MDS_SlaveLinkPrep(this, mdr, targeti));
  mdlog->flush();
}

class C_MDS_SlaveLinkCommit : public Context {
  Server *server;
  MDRequest *mdr;
  CInode *targeti;
public:
  C_MDS_SlaveLinkCommit(Server *s, MDRequest *r, CInode *t) :
    server(s), mdr(r), targeti(t) { }
  void finish(int r) {
    server->_commit_slave_link(mdr, r, targeti);
  }
};

void Server::_logged_slave_link(MDRequest *mdr, CInode *targeti) 
{
  dout(10) << "_logged_slave_link " << *mdr
	   << " " << *targeti << dendl;

  // update the target
  targeti->pop_and_dirty_projected_inode(mdr->ls);
  mdr->apply();

  // hit pop
  mds->balancer->hit_inode(mdr->now, targeti, META_POP_IWR);

  // ack
  MMDSSlaveRequest *reply = new MMDSSlaveRequest(mdr->reqid, MMDSSlaveRequest::OP_LINKPREPACK);
  mds->send_message_mds(reply, mdr->slave_to_mds);
  
  // set up commit waiter
  mdr->more()->slave_commit = new C_MDS_SlaveLinkCommit(this, mdr, targeti);

  // done.
  delete mdr->slave_request;
  mdr->slave_request = 0;
}


struct C_MDS_CommittedSlave : public Context {
  Server *server;
  MDRequest *mdr;
  C_MDS_CommittedSlave(Server *s, MDRequest *m) : server(s), mdr(m) {}
  void finish(int r) {
    server->_committed_slave(mdr);
  }
};

void Server::_commit_slave_link(MDRequest *mdr, int r, CInode *targeti)
{  
  dout(10) << "_commit_slave_link " << *mdr
	   << " r=" << r
	   << " " << *targeti << dendl;

  if (r == 0) {
    // drop our pins, etc.
    mdr->cleanup();

    // write a commit to the journal
    ESlaveUpdate *le = new ESlaveUpdate(mdlog, "slave_link_commit", mdr->reqid, mdr->slave_to_mds,
					ESlaveUpdate::OP_COMMIT, ESlaveUpdate::LINK);
    mdlog->submit_entry(le, new C_MDS_CommittedSlave(this, mdr));
    mdlog->flush();
  } else {
    do_link_rollback(mdr->more()->rollback_bl, mdr->slave_to_mds, mdr);
  }
}

void Server::_committed_slave(MDRequest *mdr)
{
  dout(10) << "_committed_slave " << *mdr << dendl;
  MMDSSlaveRequest *req = new MMDSSlaveRequest(mdr->reqid, MMDSSlaveRequest::OP_COMMITTED);
  mds->send_message_mds(req, mdr->slave_to_mds);
  mds->mdcache->request_finish(mdr);
}

struct C_MDS_LoggedLinkRollback : public Context {
  Server *server;
  Mutation *mut;
  MDRequest *mdr;
  C_MDS_LoggedLinkRollback(Server *s, Mutation *m, MDRequest *r) : server(s), mut(m), mdr(r) {}
  void finish(int r) {
    server->_link_rollback_finish(mut, mdr);
  }
};

void Server::do_link_rollback(bufferlist &rbl, int master, MDRequest *mdr)
{
  link_rollback rollback;
  bufferlist::iterator p = rbl.begin();
  ::decode(rollback, p);

  dout(10) << "do_link_rollback on " << rollback.reqid 
	   << (rollback.was_inc ? " inc":" dec") 
	   << " ino " << rollback.ino
	   << dendl;

  Mutation *mut = mdr;
  if (!mut) {
    assert(mds->is_resolve());
    mds->mdcache->add_rollback(rollback.reqid);  // need to finish this update before resolve finishes
    mut = new Mutation(rollback.reqid);
    mut->ls = mds->mdlog->get_current_segment();
  }


  CInode *in = mds->mdcache->get_inode(rollback.ino);
  assert(in);
  dout(10) << " target is " << *in << dendl;
  assert(!in->is_projected());  // live slave request hold versionlock xlock.
  
  inode_t *pi = in->project_inode();
  pi->version = in->pre_dirty();
  mut->add_projected_inode(in);

  // parent dir rctime
  CDir *parent = in->get_parent_dn()->get_dir();
  fnode_t *pf = parent->project_fnode();
  mut->add_projected_fnode(parent);
  pf->version = parent->pre_dirty();
  if (pf->fragstat.mtime == pi->ctime) {
    pf->fragstat.mtime = rollback.old_dir_mtime;
    if (pf->rstat.rctime == pi->ctime)
      pf->rstat.rctime = rollback.old_dir_rctime;
    mut->add_updated_lock(&parent->get_inode()->filelock);
    mut->add_updated_lock(&parent->get_inode()->nestlock);
  }

  // inode
  pi->ctime = rollback.old_ctime;
  if (rollback.was_inc)
    pi->nlink--;
  else
    pi->nlink++;

  // journal it
  ESlaveUpdate *le = new ESlaveUpdate(mdlog, "slave_link_rollback", rollback.reqid, master,
				      ESlaveUpdate::OP_ROLLBACK, ESlaveUpdate::LINK);
  le->commit.add_dir_context(parent);
  le->commit.add_dir(parent, true);
  le->commit.add_primary_dentry(in->get_parent_dn(), true, 0);
  
  mdlog->submit_entry(le, new C_MDS_LoggedLinkRollback(this, mut, mdr));
  mdlog->flush();
}

void Server::_link_rollback_finish(Mutation *mut, MDRequest *mdr)
{
  dout(10) << "_link_rollback_finish" << dendl;
  mut->apply();
  if (mdr)
    mds->mdcache->request_finish(mdr);
  else
    mds->mdcache->finish_rollback(mut->reqid);
  mut->cleanup();
  delete mut;
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
  int client = mdr->get_client();

  // traverse to path
  vector<CDentry*> trace;
  int r = mdcache->path_traverse(mdr, req, req->get_filepath(), trace, MDS_TRAVERSE_FORWARD);
  if (r > 0) return;
  if (r == 0 && trace.empty()) r = -EINVAL;   // can't unlink root
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
  if (!dn->lock.can_read(client) && dn->lock.get_xlocked_by() != mdr) {
    dout(10) << "waiting on xlocked dentry " << *dn << dendl;
    dn->lock.add_waiter(SimpleLock::WAIT_RD, new C_MDS_RetryRequest(mdcache, mdr));
    return;
  }

  CDentry::linkage_t *dnl = dn->get_linkage(client, mdr);
  if (dnl->is_null()) {
    reply_request(mdr, -ENOENT);
    return;
  }

  // dn looks ok.

  // get/open inode.
  mdr->trace.swap(trace);
  CInode *in = mdcache->get_dentry_inode(dn, mdr, true);
  if (!in) return;
  dout(7) << "dn links to " << *in << dendl;

  // rmdir vs is_dir 
  if (in->is_dir()) {
    if (rmdir) {
      // do empty directory checks
      if (_dir_is_nonempty(mdr, in))
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
  if (dnl->is_primary()) {
    straydn = mdcache->get_or_create_stray_dentry(dnl->get_inode());
    mdr->pin(straydn);  // pin it.
    dout(10) << " straydn is " << *straydn << dendl;
  }

  // lock
  set<SimpleLock*> rdlocks, wrlocks, xlocks;

  for (int i=0; i<(int)trace.size()-1; i++)
    rdlocks.insert(&trace[i]->lock);
  xlocks.insert(&dn->lock);
  wrlocks.insert(&dn->get_dir()->inode->filelock);
  wrlocks.insert(&dn->get_dir()->inode->nestlock);
  xlocks.insert(&in->linklock);
  if (straydn) {
    wrlocks.insert(&straydn->get_dir()->inode->filelock);
    wrlocks.insert(&straydn->get_dir()->inode->nestlock);
  }
  if (in->is_dir())
    rdlocks.insert(&in->filelock);   // to verify it's empty
  mds->locker->include_snap_rdlocks(rdlocks, dnl->get_inode());

  if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
    return;

  if (in->is_dir() &&
      in->get_projected_inode()->dirstat.size() != 0) {
    dout(10) << " not empty, dirstat.size() = " << in->get_projected_inode()->dirstat.size()
	     << " on " << *in << dendl;
    reply_request(mdr, -ENOTEMPTY);
    return;
  }

  // yay!
  mdr->done_locking = true;  // avoid wrlock racing
  if (mdr->now == utime_t())
    mdr->now = g_clock.real_now();

  // get stray dn ready?
  if (dnl->is_primary()) {
    if (!mdr->more()->dst_reanchor_atid &&
	dnl->get_inode()->is_anchored()) {
      dout(10) << "reanchoring to stray " << *dnl->get_inode() << dendl;
      vector<Anchor> trace;
      straydn->make_anchor_trace(trace, dnl->get_inode());
      mds->anchorclient->prepare_update(dnl->get_inode()->ino(), trace, &mdr->more()->dst_reanchor_atid, 
					new C_MDS_RetryRequest(mdcache, mdr));
      return;
    }
  }

  // ok!
  if (dnl->is_remote() && !dnl->get_inode()->is_auth()) 
    _link_remote(mdr, false, dn, dnl->get_inode());
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

  CDentry::linkage_t *dnl = dn->get_projected_linkage();

  // ok, let's do it.
  mdr->ls = mdlog->get_current_segment();

  // prepare log entry
  EUpdate *le = new EUpdate(mdlog, "unlink_local");
  le->metablob.add_client_req(mdr->reqid);

  if (straydn) {
    assert(dnl->is_primary());
    straydn->push_projected_linkage(dnl->get_inode());
  }

  // the unlinked dentry
  dn->pre_dirty();

  inode_t *pi = dnl->get_inode()->project_inode();
  mdr->add_projected_inode(dnl->get_inode()); // do this _after_ my dn->pre_dirty().. we apply that one manually.
  pi->version = dnl->get_inode()->pre_dirty();
  pi->nlink--;
  pi->ctime = mdr->now;

  if (dnl->is_primary()) {
    // primary link.  add stray dentry.
    assert(straydn);
    mdcache->predirty_journal_parents(mdr, &le->metablob, dnl->get_inode(), dn->get_dir(), PREDIRTY_PRIMARY|PREDIRTY_DIR, -1);
    mdcache->predirty_journal_parents(mdr, &le->metablob, dnl->get_inode(), straydn->get_dir(), PREDIRTY_PRIMARY|PREDIRTY_DIR, 1);

    // project snaprealm, too
    bufferlist snapbl;
    if (!dnl->get_inode()->snaprealm) {
      dnl->get_inode()->open_snaprealm(true);   // don't do a split
      dnl->get_inode()->snaprealm->project_past_parent(straydn->get_dir()->inode->find_snaprealm(), snapbl);
      dnl->get_inode()->close_snaprealm(true);  // or a matching join
    } else
      dnl->get_inode()->snaprealm->project_past_parent(straydn->get_dir()->inode->find_snaprealm(), snapbl);

    straydn->first = dnl->get_inode()->first;
    le->metablob.add_primary_dentry(straydn, true, dnl->get_inode(), 0, &snapbl);
  } else {
    // remote link.  update remote inode.
    mdcache->predirty_journal_parents(mdr, &le->metablob, dnl->get_inode(), dn->get_dir(), PREDIRTY_DIR, -1);
    mdcache->predirty_journal_parents(mdr, &le->metablob, dnl->get_inode(), 0, PREDIRTY_PRIMARY);
    mdcache->journal_dirty_inode(mdr, &le->metablob, dnl->get_inode());
  }

  mdcache->journal_cow_dentry(mdr, &le->metablob, dn);
  le->metablob.add_null_dentry(dn, true);

  if (mdr->more()->dst_reanchor_atid)
    le->metablob.add_table_transaction(TABLE_ANCHOR, mdr->more()->dst_reanchor_atid);

  dn->push_projected_linkage();

  journal_and_reply(mdr, 0, dn, le, new C_MDS_unlink_local_finish(mds, mdr, dn, straydn));
}

void Server::_unlink_local_finish(MDRequest *mdr, 
				  CDentry *dn, CDentry *straydn,
				  version_t dnpv) 
{
  dout(10) << "_unlink_local_finish " << *dn << dendl;

  // unlink main dentry
  dn->get_dir()->unlink_inode(dn);
  dn->pop_projected_linkage();

  // relink as stray?  (i.e. was primary link?)
  if (straydn) {
    dout(20) << " straydn is " << *straydn << dendl;
    CDentry::linkage_t *straydnl = straydn->pop_projected_linkage();
    
    SnapRealm *oldparent = dn->get_dir()->inode->find_snaprealm();
    
    bool isnew = false;
    if (!straydnl->get_inode()->snaprealm) {
      straydnl->get_inode()->open_snaprealm();
      straydnl->get_inode()->snaprealm->seq = oldparent->get_newest_seq();
      isnew = true;
    }
    straydnl->get_inode()->snaprealm->add_past_parent(oldparent);
    if (isnew)
      mdcache->do_realm_invalidate_and_update_notify(straydnl->get_inode(), CEPH_SNAP_OP_SPLIT);
  }

  dn->mark_dirty(dnpv, mdr->ls);  
  mdr->apply();
  
  // share unlink news with replicas
  for (map<int,int>::iterator it = dn->replicas_begin();
       it != dn->replicas_end();
       it++) {
    dout(7) << "_unlink_local_finish sending MDentryUnlink to mds" << it->first << dendl;
    MDentryUnlink *unlink = new MDentryUnlink(dn->get_dir()->dirfrag(), dn->name);
    if (straydn) {
      mdcache->replicate_inode(straydn->get_dir()->inode, it->first, unlink->straybl);
      mdcache->replicate_dir(straydn->get_dir(), it->first, unlink->straybl);
      mdcache->replicate_dentry(straydn, it->first, unlink->straybl);
    }
    mds->send_message_mds(unlink, it->first);
  }
  
  // commit anchor update?
  if (mdr->more()->dst_reanchor_atid) 
    mds->anchorclient->commit(mdr->more()->dst_reanchor_atid, mdr->ls);

  // bump pop
  //mds->balancer->hit_dir(mdr->now, dn->get_dir(), META_POP_DWR);

  // reply
  MClientReply *reply = new MClientReply(mdr->client_request, 0);
  reply_request(mdr, reply);
  
  // clean up?
  if (straydn)
    mdcache->eval_stray(straydn);

  // removing a new dn?
  dn->get_dir()->try_remove_unlinked_dn(dn);
}



/** _dir_is_not_empty
 *
 * check if a directory is non-empty (i.e. we can rmdir it), and make
 * sure it is part of the same subtree (i.e. local) so that rmdir will
 * occur locally.
 *
 * this is a fastpath check.  we can't really be sure until we rdlock
 * the filelock.
 *
 * @param in is the inode being rmdir'd.  return true if it is
 * definitely not empty.
 */
bool Server::_dir_is_nonempty(MDRequest *mdr, CInode *in)
{
  dout(10) << "dir_is_nonempty " << *in << dendl;
  assert(in->is_auth());

  list<frag_t> frags;
  in->dirfragtree.get_leaves(frags);

  for (list<frag_t>::iterator p = frags.begin();
       p != frags.end();
       ++p) {
    CDir *dir = in->get_or_open_dirfrag(mdcache, *p);
    assert(dir);

    // does the frag _look_ empty?
    if (dir->inode->get_projected_inode()->dirstat.size() > 0) {	
      dout(10) << "dir_is_nonempty still " << dir->get_num_head_items() 
	       << " cached items in frag " << *dir << dendl;
      reply_request(mdr, -ENOTEMPTY);
      return true;
    }

    // not dir auth?
    if (!dir->is_auth()) {
      dout(10) << "dir_is_nonempty non-auth dirfrag for " << *dir << dendl;
    }
  }

  return false;
}


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
 * rename master is the destdn auth.  this is because cached inodes
 * must remain connected.  thus, any replica of srci, must also
 * replicate destdn, and possibly straydn, so that srci (and
 * destdn->inode) remain connected during the rename.
 *
 * to do this, we freeze srci, then master (destdn auth) verifies that
 * all other nodes have also replciated destdn and straydn.  note that
 * destdn replicas need not also replicate srci.  this only works when 
 * destdn is master.
 */
void Server::handle_client_rename(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;
  int client = mdr->get_client();
  dout(7) << "handle_client_rename " << *req << dendl;

  filepath destpath = req->get_filepath();
  filepath srcpath = req->get_filepath2();
  if (destpath.depth() == 0 || srcpath.depth() == 0) {
    reply_request(mdr, -EINVAL);
    return;
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
  int r = mdcache->path_traverse(mdr, req, srcpath, srctrace, MDS_TRAVERSE_DISCOVER);
  if (r > 0) return;
  if (r < 0) {
    reply_request(mdr, r);
    return;
  }
  assert(!srctrace.empty());
  CDentry *srcdn = srctrace[srctrace.size()-1];
  dout(10) << " srcdn " << *srcdn << dendl;
  CInode *srci = mdcache->get_dentry_inode(srcdn, mdr, true);
  dout(10) << " srci " << *srci << dendl;
  mdr->pin(srci);

  CDentry::linkage_t *srcdnl = srcdn->get_projected_linkage();

  // -- some sanity checks --

  // src+dest traces _must_ share a common ancestor for locking to prevent orphans
  if (destpath.get_ino() != srcpath.get_ino() &&
      !MDS_INO_IS_STRAY(srcpath.get_ino())) {  // <-- mds 'rename' out of stray dir is ok!
    // do traces share a dentry?
    CDentry *common = 0;
    for (unsigned i=0; i < srctrace.size(); i++) {
      for (unsigned j=0; j < desttrace.size(); j++) {
	if (srctrace[i] == desttrace[j]) {
	  common = srctrace[i];
	  break;
	}
      }
      if (common)
	break;
    }

    if (common) {
      dout(10) << "rename src and dest traces share common dentry " << *common << dendl;
    } else {
      CInode *srcbase = srctrace[0]->get_dir()->get_inode();
      CInode *destbase = destdir->get_inode();
      if (!desttrace.empty())
	destbase = desttrace[0]->get_dir()->get_inode();

      // ok, extend srctrace toward root until it is an ancestor of desttrace.
      while (srcbase != destbase &&
	     !srcbase->is_projected_ancestor_of(destbase)) {
	srctrace.insert(srctrace.begin(),
			srcbase->get_projected_parent_dn());
	dout(10) << "rename prepending srctrace with " << *srctrace[0] << dendl;
	srcbase = srcbase->get_projected_parent_dn()->get_dir()->get_inode();
      }

      // then, extend destpath until it shares the same parent inode as srcpath.
      while (destbase != srcbase) {
	desttrace.insert(desttrace.begin(),
			 destbase->get_projected_parent_dn());
	dout(10) << "rename prepending desttrace with " << *desttrace[0] << dendl;
	destbase = destbase->get_projected_parent_dn()->get_dir()->get_inode();
      }
      dout(10) << "rename src and dest traces now share common ancestor " << *destbase << dendl;
    }
  }

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
    pdn = pdn->get_dir()->inode->parent;
  }


  // identify/create dest dentry
  CDentry *destdn = destdir->lookup(destname);
  if (destdn && !destdn->lock.can_read(client) && destdn->lock.get_xlocked_by() != mdr) {
    destdn->lock.add_waiter(SimpleLock::WAIT_RD, new C_MDS_RetryRequest(mdcache, mdr));
    return;
  }

  CDentry::linkage_t *destdnl = 0;
  if (destdn)
    destdnl = destdn->get_projected_linkage();


  // is this a stray reintegration or merge? (sanity checks!)
  if (mdr->reqid.name.is_mds() &&
      (!destdnl->is_remote() ||
       destdnl->get_remote_ino() != srci->ino())) {
    reply_request(mdr, -EINVAL);  // actually, this won't reply, but whatev.
    return;
  }

  CInode *oldin = 0;
  if (destdn && !destdnl->is_null()) {
    //dout(10) << "dest dn exists " << *destdn << dendl;
    oldin = mdcache->get_dentry_inode(destdn, mdr, true);
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
    if (oldin->is_dir() && _dir_is_nonempty(mdr, oldin))      
      return;
  }
  if (!destdn) {
    // mv /some/thing /to/some/non_existent_name
    destdn = prepare_null_dentry(mdr, destdir, destname);
    if (!destdn)
      return;
    destdnl = destdn->get_projected_linkage();
  }

  dout(10) << " destdn " << *destdn << dendl;

  bool linkmerge = (srcdnl->get_inode() == destdnl->get_inode() &&
		    (srcdnl->is_primary() || destdnl->is_primary()));
  if (linkmerge)
    dout(10) << " this is a link merge" << dendl;

  // -- create stray dentry? --
  CDentry *straydn = 0;
  if (destdnl->is_primary() && !linkmerge) {
    straydn = mdcache->get_or_create_stray_dentry(destdnl->get_inode());
    mdr->pin(straydn);
    dout(10) << "straydn is " << *straydn << dendl;
  }

  // -- locks --
  set<SimpleLock*> rdlocks, wrlocks, xlocks;

  // straydn?
  if (straydn) {
    wrlocks.insert(&straydn->get_dir()->inode->filelock);
    wrlocks.insert(&straydn->get_dir()->inode->nestlock);
  }

  // rdlock sourcedir path, xlock src dentry
  for (int i=0; i<(int)srctrace.size()-1; i++) 
    rdlocks.insert(&srctrace[i]->lock);
  xlocks.insert(&srcdn->lock);
  wrlocks.insert(&srcdn->get_dir()->inode->filelock);
  wrlocks.insert(&srcdn->get_dir()->inode->nestlock);

  // rdlock destdir path, xlock dest dentry
  for (int i=0; i<(int)desttrace.size(); i++)
    rdlocks.insert(&desttrace[i]->lock);
  xlocks.insert(&destdn->lock);
  wrlocks.insert(&destdn->get_dir()->inode->filelock);
  wrlocks.insert(&destdn->get_dir()->inode->nestlock);

  // xlock versionlock on srci if remote?
  //  this ensures it gets safely remotely auth_pinned, avoiding deadlock;
  //  strictly speaking, having the slave node freeze the inode is 
  //  otherwise sufficient for avoiding conflicts with inode locks, etc.
  if (!srcdn->is_auth() && srcdnl->is_primary())
    xlocks.insert(&srci->versionlock);

  // we need to update srci's ctime.  xlock its least contended lock to do that...
  xlocks.insert(&srci->linklock);

  // xlock oldin (for nlink--)
  if (oldin) {
    xlocks.insert(&oldin->linklock);
    if (oldin->is_dir())
      rdlocks.insert(&oldin->filelock);
  }
  mds->locker->include_snap_rdlocks(rdlocks, srcdn->get_dir()->inode);
  mds->locker->include_snap_rdlocks(rdlocks, destdn->get_dir()->inode);
  if (srcdnl->is_primary() && srci->is_dir())
    xlocks.insert(&srci->snaplock);  // FIXME: an auth bcast could be sufficient?
  else
    rdlocks.insert(&srci->snaplock);

  if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
    return;
  
  if (oldin &&
      oldin->is_dir() &&
      oldin->get_projected_inode()->dirstat.size() != 0) {
    dout(10) << " target inode dir not empty, dirstat.size() = " << oldin->get_projected_inode()->dirstat.size() 
	     << " on " << *oldin << dendl;
    reply_request(mdr, -ENOTEMPTY);
    return;
  }

  // moving between snaprealms?
  if (!srci->snaprealm &&
      srci->find_snaprealm() != destdn->get_dir()->inode->find_snaprealm()) {
    dout(10) << " renaming between snaprealms, creating snaprealm for " << *srci << dendl;
    mds->mdcache->snaprealm_create(mdr, srci);
    return;
  }

  // set done_locking flag, to avoid problems with wrlock moving auth target
  mdr->done_locking = true;

  // -- open all srcdn inode frags, if any --
  // we need these open so that auth can properly delegate from inode to dirfrags
  // after the inode is _ours_.
  if (srcdnl->is_primary() && 
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

  // test hack: bail after slave does prepare, so we can verify it's _live_ rollback.
  //if (!mdr->more()->slaves.empty() && !srci->is_dir()) assert(0); 
  //if (!mdr->more()->slaves.empty() && srci->is_dir()) assert(0); 
  
  // -- prepare anchor updates -- 
  if (!linkmerge || srcdnl->is_primary()) {
    C_Gather *anchorgather = 0;

    if (srcdnl->is_primary() &&
	(srcdnl->get_inode()->is_anchored() || 
	 (srcdnl->get_inode()->is_dir() && (srcdnl->get_inode()->inode.rstat.ranchors ||
					    srcdnl->get_inode()->nested_anchors ||
					    !mdcache->is_leaf_subtree(mdcache->get_subtree_root(srcdn->get_dir()))))) &&
	!mdr->more()->src_reanchor_atid) {
      dout(10) << "reanchoring src->dst " << *srcdnl->get_inode() << dendl;
      vector<Anchor> trace;
      destdn->make_anchor_trace(trace, srcdnl->get_inode());
      
      anchorgather = new C_Gather(new C_MDS_RetryRequest(mdcache, mdr));
      mds->anchorclient->prepare_update(srcdnl->get_inode()->ino(), trace, &mdr->more()->src_reanchor_atid, 
					anchorgather->new_sub());
    }
    if (destdnl->is_primary() &&
	destdnl->get_inode()->is_anchored() &&
	!mdr->more()->dst_reanchor_atid) {
      dout(10) << "reanchoring dst->stray " << *destdnl->get_inode() << dendl;

      assert(straydn);
      vector<Anchor> trace;
      straydn->make_anchor_trace(trace, destdnl->get_inode());
      
      if (!anchorgather)
	anchorgather = new C_Gather(new C_MDS_RetryRequest(mdcache, mdr));
      mds->anchorclient->prepare_update(destdnl->get_inode()->ino(), trace, &mdr->more()->dst_reanchor_atid, 
					anchorgather->new_sub());
    }

    if (anchorgather) 
      return;  // waiting for anchor prepares
  }

  // -- prepare journal entry --
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "rename");
  le->metablob.add_client_req(mdr->reqid);
  if (!mdr->more()->slaves.empty()) {
    dout(20) << " noting uncommitted_slaves " << mdr->more()->slaves << dendl;
    
    le->reqid = mdr->reqid;
    le->had_slaves = true;
    
    mds->mdcache->add_uncommitted_master(mdr->reqid, mdr->ls, mdr->more()->slaves);
  }
  
  _rename_prepare(mdr, &le->metablob, &le->client_map, srcdn, destdn, straydn);


  if (!srcdn->is_auth() && srcdnl->is_primary()) {
    // importing inode; also journal imported client map
    
    // ** DER FIXME **
  }

  // -- commit locally --
  C_MDS_rename_finish *fin = new C_MDS_rename_finish(mds, mdr, srcdn, destdn, straydn);

  journal_and_reply(mdr, srci, destdn, le, fin);
}


void Server::_rename_finish(MDRequest *mdr, CDentry *srcdn, CDentry *destdn, CDentry *straydn)
{
  dout(10) << "_rename_finish " << *mdr << dendl;

  // test hack: test slave commit
  //if (!mdr->more()->slaves.empty() && !destdn->get_inode()->is_dir()) assert(0); 
  //if (!mdr->more()->slaves.empty() && destdn->get_inode()->is_dir()) assert(0); 

  // apply
  _rename_apply(mdr, srcdn, destdn, straydn);

  CDentry::linkage_t *destdnl = destdn->get_linkage();
  
  // commit anchor updates?
  if (mdr->more()->src_reanchor_atid) 
    mds->anchorclient->commit(mdr->more()->src_reanchor_atid, mdr->ls);
  if (mdr->more()->dst_reanchor_atid) 
    mds->anchorclient->commit(mdr->more()->dst_reanchor_atid, mdr->ls);

  // bump popularity
  //if (srcdn->is_auth())
  //  mds->balancer->hit_dir(mdr->now, srcdn->get_dir(), META_POP_DWR);
  //  mds->balancer->hit_dir(mdr->now, destdn->get_dir(), META_POP_DWR);
  if (destdnl->is_remote() &&
      destdnl->get_inode()->is_auth())
    mds->balancer->hit_inode(mdr->now, destdnl->get_inode(), META_POP_IWR);

  // did we import srci?  if so, explicitly ack that import that, before we unlock and reply.
  

  // reply
  MClientReply *reply = new MClientReply(mdr->client_request, 0);
  reply_request(mdr, reply);
  
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
    mdcache->replicate_inode(straydn->get_dir()->inode, who, req->stray);
    mdcache->replicate_dir(straydn->get_dir(), who, req->stray);
    mdcache->replicate_dentry(straydn, who, req->stray);
  }
  
  // srcdn auth will verify our current witness list is sufficient
  req->witnesses = mdr->more()->witnessed;

  mds->send_message_mds(req, who);
  
  assert(mdr->more()->waiting_on_slave.count(who) == 0);
  mdr->more()->waiting_on_slave.insert(who);
}

version_t Server::_rename_prepare_import(MDRequest *mdr, CDentry *srcdn, bufferlist *client_map_bl)
{
  version_t oldpv = mdr->more()->inode_import_v;

  CDentry::linkage_t *srcdnl = srcdn->get_linkage();

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
  srcdnl->get_inode()->filelock.clear_dirty();  
  srcdnl->get_inode()->nestlock.clear_dirty();  

  // hack: force back to !auth and clean, temporarily
  srcdnl->get_inode()->state_clear(CInode::STATE_AUTH);
  srcdnl->get_inode()->mark_clean();

  return oldpv;
}

void Server::_rename_prepare(MDRequest *mdr,
			     EMetaBlob *metablob, bufferlist *client_map_bl,
			     CDentry *srcdn, CDentry *destdn, CDentry *straydn)
{
  dout(10) << "_rename_prepare " << *mdr << " " << *srcdn << " " << *destdn << dendl;
  if (straydn) dout(10) << " straydn " << *straydn << dendl;

  CDentry::linkage_t *srcdnl = srcdn->get_projected_linkage();
  CDentry::linkage_t *destdnl = destdn->get_projected_linkage();

  // primary+remote link merge?
  bool linkmerge = (srcdnl->get_inode() == destdnl->get_inode() &&
		    (srcdnl->is_primary() || destdnl->is_primary()));
  bool silent = srcdn->get_dir()->inode->is_stray();
  if (linkmerge)
    dout(10) << " merging remote and primary links to the same inode" << dendl;
  if (silent)
    dout(10) << " reintegrating stray; will avoid changing nlink or dir mtime" << dendl;

  // prepare
  inode_t *pi = 0, *ji = 0;    // renamed inode
  inode_t *tpi = 0, *tji = 0;  // target/overwritten inode
  
  // target inode
  if (!linkmerge) {
    if (destdnl->is_primary()) {
      assert(straydn);  // moving to straydn.
      // link--, and move.
      if (destdn->is_auth()) {
	tpi = destdnl->get_inode()->project_inode();
	tpi->version = straydn->pre_dirty(tpi->version);
      }
      straydn->push_projected_linkage(destdnl->get_inode());
    } else if (destdnl->is_remote()) {
      // nlink-- targeti
      if (destdnl->get_inode()->is_auth()) {
	tpi = destdnl->get_inode()->project_inode();
	tpi->version = destdnl->get_inode()->pre_dirty();
      }
    }
  }

  // dest
  if (srcdnl->is_remote()) {
    if (!linkmerge) {
      if (destdn->is_auth())
	mdr->more()->pvmap[destdn] = destdn->pre_dirty();
      if (srcdnl->get_inode()->is_auth()) {
	pi = srcdnl->get_inode()->project_inode();
	pi->version = srcdnl->get_inode()->pre_dirty();
      }
    } else {
      dout(10) << " will merge remote onto primary link" << dendl;
      if (destdn->is_auth()) {
	pi = destdnl->get_inode()->project_inode();
	pi->version = mdr->more()->pvmap[destdn] = destdn->pre_dirty(destdnl->get_inode()->inode.version);
      }
    }
  } else {
    if (destdn->is_auth()) {
      version_t oldpv;
      if (srcdn->is_auth())
	oldpv = srcdnl->get_inode()->get_projected_version();
      else
	oldpv = _rename_prepare_import(mdr, srcdn, client_map_bl);
      pi = srcdnl->get_inode()->project_inode();
      pi->version = mdr->more()->pvmap[destdn] = destdn->pre_dirty(oldpv);
      destdn->push_projected_linkage(srcdnl->get_inode());
    }
  }

  // src
  if (srcdn->is_auth()) {
    mdr->more()->pvmap[srcdn] = srcdn->pre_dirty();
    srcdn->push_projected_linkage();  // push null linkage
  }

  if (!silent) {
    if (pi) {
      pi->ctime = mdr->now;
      if (linkmerge)
	pi->nlink--;
    }
    if (tpi) {
      tpi->nlink--;
      tpi->ctime = mdr->now;
    }
  }

  // prepare nesting, mtime updates
  int predirty_dir = silent ? 0:PREDIRTY_DIR;
  
  // sub off target
  if (destdn->is_auth() && !destdnl->is_null())
    mdcache->predirty_journal_parents(mdr, metablob, destdnl->get_inode(), destdn->get_dir(),
				      (destdnl->is_primary() ? PREDIRTY_PRIMARY:0)|predirty_dir, -1);
  
  // move srcdn
  int predirty_primary = (srcdnl->is_primary() && srcdn->get_dir() != destdn->get_dir()) ? PREDIRTY_PRIMARY:0;
  int flags = predirty_dir | predirty_primary;
  if (srcdn->is_auth())
    mdcache->predirty_journal_parents(mdr, metablob, srcdnl->get_inode(), srcdn->get_dir(), PREDIRTY_SHALLOW|flags, -1);
  if (destdn->is_auth())
    mdcache->predirty_journal_parents(mdr, metablob, srcdnl->get_inode(), destdn->get_dir(), flags, 1);

  metablob->add_dir_context(srcdn->get_dir());
  metablob->add_dir_context(destdn->get_dir());
  if (straydn)
    metablob->add_dir_context(straydn->get_dir());

  // add it all to the metablob
  // target inode
  if (!linkmerge) {
    if (destdnl->is_primary()) {
      // project snaprealm, too
      bufferlist snapbl;
      if (!destdnl->get_inode()->snaprealm) {
	destdnl->get_inode()->open_snaprealm(true);   // don't do a split
	destdnl->get_inode()->snaprealm->project_past_parent(straydn->get_dir()->inode->find_snaprealm(), snapbl);
	destdnl->get_inode()->close_snaprealm(true);  // or a matching join
      } else
	destdnl->get_inode()->snaprealm->project_past_parent(straydn->get_dir()->inode->find_snaprealm(), snapbl);
      straydn->first = destdnl->get_inode()->first;
      tji = metablob->add_primary_dentry(straydn, true, destdnl->get_inode(), 0, &snapbl);
    } else if (destdnl->is_remote()) {
      metablob->add_dir_context(destdnl->get_inode()->get_parent_dir());
      mdcache->journal_cow_dentry(mdr, metablob, destdnl->get_inode()->parent, CEPH_NOSNAP, 0, destdnl);
      tji = metablob->add_primary_dentry(destdnl->get_inode()->parent, true, destdnl->get_inode());
    }
  }

  // dest
  if (srcdnl->is_remote()) {
    if (!linkmerge) {
      if (!destdnl->is_null())
	mdcache->journal_cow_dentry(mdr, metablob, destdn, CEPH_NOSNAP, 0, destdnl);
      else
	destdn->first = destdn->get_dir()->inode->find_snaprealm()->get_newest_seq()+1;
      metablob->add_remote_dentry(destdn, true, srcdnl->get_remote_ino(), srcdnl->get_remote_d_type());
      mdcache->journal_cow_dentry(mdr, metablob, srcdnl->get_inode()->get_parent_dn(), CEPH_NOSNAP, 0, srcdnl);
      ji = metablob->add_primary_dentry(srcdnl->get_inode()->get_parent_dn(), true, srcdnl->get_inode());
    } else {
      if (!destdnl->is_null())
	mdcache->journal_cow_dentry(mdr, metablob, destdn, CEPH_NOSNAP, 0, destdnl);
      else
	destdn->first = destdn->get_dir()->inode->find_snaprealm()->get_newest_seq()+1;
      metablob->add_primary_dentry(destdn, true, destdnl->get_inode()); 
    }
  } else if (srcdnl->is_primary()) {
    // project snap parent update?
    bufferlist snapbl;
    if (destdn->is_auth() && srcdnl->get_inode()->snaprealm)
      srcdnl->get_inode()->snaprealm->project_past_parent(destdn->get_dir()->inode->find_snaprealm(), snapbl);
    
    if (!destdnl->is_null())
      mdcache->journal_cow_dentry(mdr, metablob, destdn, CEPH_NOSNAP, 0, destdnl);
    else
      destdn->first = destdn->get_dir()->inode->find_snaprealm()->get_newest_seq()+1;
    ji = metablob->add_primary_dentry(destdn, true, srcdnl->get_inode(), 0, &snapbl); 
  }
    
  // src
  mdcache->journal_cow_dentry(mdr, metablob, srcdn, CEPH_NOSNAP, 0, srcdnl);
  metablob->add_null_dentry(srcdn, true);

  // new subtree?
  if (srcdnl->is_primary() &&
      srcdnl->get_inode()->is_dir()) {
    list<CDir*> ls;
    srcdnl->get_inode()->get_nested_dirfrags(ls);
    int auth = srcdn->authority().first;
    for (list<CDir*>::iterator p = ls.begin(); p != ls.end(); ++p) 
      mdcache->adjust_subtree_auth(*p, auth, auth);
  }

  // do inode updates in journal, even if we aren't auth (hmm, is this necessary?)
  if (!silent) {
    if (ji && !pi) {
      ji->ctime = mdr->now;
      if (linkmerge)
	ji->nlink--;
    }
    if (tji && !tpi) {
      tji->nlink--;
      tji->ctime = mdr->now;
    }
  }

  // anchor updates?
  if (mdr->more()->src_reanchor_atid)
    metablob->add_table_transaction(TABLE_ANCHOR, mdr->more()->src_reanchor_atid);
  if (mdr->more()->dst_reanchor_atid)
    metablob->add_table_transaction(TABLE_ANCHOR, mdr->more()->dst_reanchor_atid);
}


void Server::_rename_apply(MDRequest *mdr, CDentry *srcdn, CDentry *destdn, CDentry *straydn)
{
  dout(10) << "_rename_apply " << *mdr << " " << *srcdn << " " << *destdn << dendl;
  dout(10) << " pvs " << mdr->more()->pvmap << dendl;

  CDentry::linkage_t *srcdnl = srcdn->get_linkage();
  CDentry::linkage_t *destdnl = destdn->get_linkage();
  CDentry::linkage_t *straydnl = straydn ? straydn->get_linkage() : 0;

  CInode *oldin = destdnl->get_inode();
  
  // primary+remote link merge?
  bool linkmerge = (srcdnl->get_inode() == destdnl->get_inode() &&
		    (srcdnl->is_primary() || destdnl->is_primary()));

  // target inode
  if (!linkmerge && oldin) {
    if (destdnl->is_primary()) {
      assert(straydn);
      dout(10) << "straydn is " << *straydn << dendl;
      destdn->get_dir()->unlink_inode(destdn);

      if (straydn->is_auth())
	straydnl = straydn->pop_projected_linkage();
      else
	straydn->get_dir()->link_primary_inode(straydn, oldin);
      
      if (straydn->is_auth()) {
	SnapRealm *oldparent = destdn->get_dir()->inode->find_snaprealm();
	bool isnew = false;
	if (!straydnl->get_inode()->snaprealm) {
	  straydnl->get_inode()->open_snaprealm();
	  straydnl->get_inode()->snaprealm->seq = oldparent->get_newest_seq();
	  isnew = true;
	}
	straydnl->get_inode()->snaprealm->add_past_parent(oldparent);
	if (isnew)
	  mdcache->do_realm_invalidate_and_update_notify(straydnl->get_inode(), CEPH_SNAP_OP_SPLIT);
      }

    } else {
      destdn->get_dir()->unlink_inode(destdn);
    }
    // nlink-- targeti
    if (oldin->is_auth())
      oldin->pop_and_dirty_projected_inode(mdr->ls);
  }

  // dest
  CInode *in = srcdnl->get_inode();
  assert(in);
  if (srcdnl->is_remote()) {
    if (!linkmerge) {
      srcdn->get_dir()->unlink_inode(srcdn);
      if (destdn->is_auth())
	destdnl = destdn->pop_projected_linkage();
      else
	destdn->get_dir()->link_remote_inode(destdn, in);
      destdn->link_remote(destdnl, in);
      if (destdn->is_auth())
	destdn->mark_dirty(mdr->more()->pvmap[destdn], mdr->ls);
    } else {
      dout(10) << "merging remote onto primary link" << dendl;
      srcdn->get_dir()->unlink_inode(srcdn);
    }
    if (destdnl->get_inode()->is_auth())
      destdnl->get_inode()->pop_and_dirty_projected_inode(mdr->ls);
  } else {
    if (linkmerge) {
      dout(10) << "merging primary onto remote link" << dendl;
      destdn->get_dir()->unlink_inode(destdn);
    }
    srcdn->get_dir()->unlink_inode(srcdn);
    if (destdn->is_auth())
      destdnl = destdn->pop_projected_linkage();
    else
      destdn->get_dir()->link_primary_inode(destdn, in);

    // srcdn inode import?
    if (!srcdn->is_auth() && destdn->is_auth()) {
      assert(mdr->more()->inode_import.length() > 0);
      
      // finish cap imports
      finish_force_open_sessions(mdr->more()->imported_client_map);
      if (mdr->more()->cap_imports.count(destdnl->get_inode()))
	mds->mdcache->migrator->finish_import_inode_caps(destdnl->get_inode(), srcdn->authority().first, 
							 mdr->more()->cap_imports[destdnl->get_inode()]);
      
      // hack: fix auth bit
      destdnl->get_inode()->state_set(CInode::STATE_AUTH);
    }

    if (destdn->is_auth()) {
      destdnl->get_inode()->pop_and_dirty_projected_inode(mdr->ls);
    }

    // snap parent update?
    if (destdn->is_auth() && destdnl->get_inode()->snaprealm)
      destdnl->get_inode()->snaprealm->add_past_parent(srcdn->get_dir()->inode->find_snaprealm());
  }

  // src
  if (srcdn->is_auth()) {
    srcdn->mark_dirty(mdr->more()->pvmap[srcdn], mdr->ls);
    srcdnl = srcdn->pop_projected_linkage();
  }
  
  // apply remaining projected inodes (nested)
  mdr->apply();

  // update subtree map?
  if (destdnl->is_primary() && destdnl->get_inode()->is_dir()) 
    mdcache->adjust_subtree_after_rename(destdnl->get_inode(), srcdn->get_dir());

  // removing a new dn?
  if (srcdn->is_auth())
    srcdn->get_dir()->try_remove_unlinked_dn(srcdn);
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
  int r = mdcache->path_traverse(mdr, mdr->slave_request, destpath, trace, MDS_TRAVERSE_DISCOVERXLOCK);
  if (r > 0) return;
  assert(r == 0);  // we shouldn't get an error here!
      
  CDentry *destdn = trace[trace.size()-1];
  CDentry::linkage_t *destdnl = destdn->get_projected_linkage();
  dout(10) << " destdn " << *destdn << dendl;
  mdr->pin(destdn);
  
  // discover srcdn
  filepath srcpath(mdr->slave_request->srcdnpath);
  dout(10) << " src " << srcpath << dendl;
  r = mdcache->path_traverse(mdr, mdr->slave_request, srcpath, trace, MDS_TRAVERSE_DISCOVERXLOCK);
  if (r > 0) return;
  assert(r == 0);
      
  CDentry *srcdn = trace[trace.size()-1];
  CDentry::linkage_t *srcdnl = srcdn->get_projected_linkage();
  dout(10) << " srcdn " << *srcdn << dendl;
  mdr->pin(srcdn);
  assert(srcdnl->get_inode());
  mdr->pin(srcdnl->get_inode());

  // stray?
  bool linkmerge = (srcdnl->get_inode() == destdnl->get_inode() &&
		    (srcdnl->is_primary() || destdnl->is_primary()));
  CDentry *straydn = 0;
  if (destdnl->is_primary() && !linkmerge) {
    assert(mdr->slave_request->stray.length() > 0);
    straydn = mdcache->add_replica_stray(mdr->slave_request->stray, mdr->slave_to_mds);
    assert(straydn);
    mdr->pin(straydn);
  }

  mdr->now = mdr->slave_request->now;

  // set up commit waiter (early, to clean up any freezing etc we do)
  if (!mdr->more()->slave_commit)
    mdr->more()->slave_commit = new C_MDS_SlaveRenameCommit(this, mdr, srcdn, destdn, straydn);

  // am i srcdn auth?
  if (srcdn->is_auth()) {
    if (srcdnl->is_primary() && 
	!srcdnl->get_inode()->is_freezing_inode() &&
	!srcdnl->get_inode()->is_frozen_inode()) {
      // set ambiguous auth for srci
      /*
       * NOTE: we don't worry about ambiguous cache expire as we do
       * with subtree migrations because all slaves will pin
       * srcdn->get_inode() for duration of this rename.
       */
      srcdnl->get_inode()->state_set(CInode::STATE_AMBIGUOUSAUTH);

      // freeze?
      // we need this to
      //  - avoid conflicting lock state changes
      //  - avoid concurrent updates to the inode
      //     (this could also be accomplished with the versionlock)
      int allowance = 1; // for the versionlock and possible linklock xlock (both are tied to mdr)
      dout(10) << " freezing srci " << *srcdnl->get_inode() << " with allowance " << allowance << dendl;
      if (!srcdnl->get_inode()->freeze_inode(allowance)) {
	srcdnl->get_inode()->add_waiter(CInode::WAIT_FROZEN, new C_MDS_RetryRequest(mdcache, mdr));
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

  // encode everything we'd need to roll this back... basically, just the original state.
  rename_rollback rollback;
  
  rollback.reqid = mdr->reqid;
  
  rollback.orig_src.dirfrag = srcdn->get_dir()->dirfrag();
  rollback.orig_src.dirfrag_old_mtime = srcdn->get_dir()->get_projected_fnode()->fragstat.mtime;
  rollback.orig_src.dirfrag_old_rctime = srcdn->get_dir()->get_projected_fnode()->rstat.rctime;
  rollback.orig_src.dname = srcdn->name;
  if (srcdnl->is_primary())
    rollback.orig_src.ino = srcdnl->get_inode()->ino();
  else {
    assert(srcdnl->is_remote());
    rollback.orig_src.remote_ino = srcdnl->get_remote_ino();
    rollback.orig_src.remote_d_type = srcdnl->get_remote_d_type();
  }
  
  rollback.orig_dest.dirfrag = destdn->get_dir()->dirfrag();
  rollback.orig_dest.dirfrag_old_mtime = destdn->get_dir()->get_projected_fnode()->fragstat.mtime;
  rollback.orig_dest.dirfrag_old_rctime = destdn->get_dir()->get_projected_fnode()->rstat.rctime;
  rollback.orig_dest.dname = destdn->name;
  if (destdnl->is_primary())
    rollback.orig_dest.ino = destdnl->get_inode()->ino();
  else if (destdnl->is_remote()) {
    rollback.orig_dest.remote_ino = destdnl->get_remote_ino();
    rollback.orig_dest.remote_d_type = destdnl->get_remote_d_type();
  }
  
  if (straydn) {
    rollback.stray.dirfrag = straydn->get_dir()->dirfrag();
    rollback.stray.dirfrag_old_mtime = straydn->get_dir()->get_projected_fnode()->fragstat.mtime;
    rollback.stray.dirfrag_old_rctime = straydn->get_dir()->get_projected_fnode()->rstat.rctime;
    rollback.stray.dname = straydn->name;
  }
  ::encode(rollback, mdr->more()->rollback_bl);
  dout(20) << " rollback is " << mdr->more()->rollback_bl.length() << " bytes" << dendl;

  // journal it?
  if (srcdn->is_auth() ||
      (destdnl->get_inode() && destdnl->get_inode()->is_auth()) ||
      srcdnl->get_inode()->is_any_caps()) {
    // journal.
    mdr->ls = mdlog->get_current_segment();
    ESlaveUpdate *le = new ESlaveUpdate(mdlog, "slave_rename_prep", mdr->reqid, mdr->slave_to_mds,
					ESlaveUpdate::OP_PREPARE, ESlaveUpdate::RENAME);
    le->rollback = mdr->more()->rollback_bl;

    bufferlist blah;  // inode import data... obviously not used if we're the slave
    _rename_prepare(mdr, &le->commit, &blah, srcdn, destdn, straydn);

    mdlog->submit_entry(le, new C_MDS_SlaveRenamePrep(this, mdr, srcdn, destdn, straydn));
    mdlog->flush();
  } else {
    // don't journal.
    dout(10) << "not journaling: i'm not auth for anything, and srci has no caps" << dendl;

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
  
  CDentry::linkage_t *srcdnl = srcdn->get_linkage();
  CDentry::linkage_t *destdnl = destdn->get_linkage();
  //CDentry::linkage_t *straydnl = straydn ? straydn->get_linkage() : 0;

  // export srci?
  if (srcdn->is_auth() && srcdnl->is_primary()) {
    list<Context*> finished;
    map<__u32,entity_inst_t> exported_client_map;
    bufferlist inodebl;
    mdcache->migrator->encode_export_inode(srcdnl->get_inode(), inodebl, 
					   exported_client_map);
    ::encode(exported_client_map, reply->inode_export);
    reply->inode_export.claim_append(inodebl);
    reply->inode_export_v = srcdnl->get_inode()->inode.version;

    // remove mdr auth pin
    mdr->auth_unpin(srcdnl->get_inode());
    assert(!srcdnl->get_inode()->is_auth_pinned());
    
    dout(10) << " exported srci " << *srcdnl->get_inode() << dendl;
  }

  // apply
  _rename_apply(mdr, srcdn, destdn, straydn);   
  
  srcdnl = srcdn->get_linkage();
  destdnl = destdn->get_linkage();

  mds->send_message_mds(reply, mdr->slave_to_mds);
  
  // bump popularity
  //if (srcdn->is_auth())
    //mds->balancer->hit_dir(mdr->now, srcdn->get_dir(), META_POP_DWR);
  if (destdnl->get_inode() && destdnl->get_inode()->is_auth())
    mds->balancer->hit_inode(mdr->now, destdnl->get_inode(), META_POP_IWR);

  // done.
  delete mdr->slave_request;
  mdr->slave_request = 0;
}

void Server::_commit_slave_rename(MDRequest *mdr, int r,
				  CDentry *srcdn, CDentry *destdn, CDentry *straydn)
{
  dout(10) << "_commit_slave_rename " << *mdr << " r=" << r << dendl;

  CDentry::linkage_t *destdnl = destdn->get_linkage();

  ESlaveUpdate *le;
  if (r == 0) {
    // write a commit to the journal
    le = new ESlaveUpdate(mdlog, "slave_rename_commit", mdr->reqid, mdr->slave_to_mds, ESlaveUpdate::OP_COMMIT, ESlaveUpdate::RENAME);

    // unfreeze+singleauth inode
    //  hmm, do i really need to delay this?
    if (srcdn->is_auth() && destdnl->is_primary() &&
	destdnl->get_inode()->state_test(CInode::STATE_AMBIGUOUSAUTH)) {
      list<Context*> finished;

      dout(10) << " finishing inode export on " << *destdnl->get_inode() << dendl;
      mdcache->migrator->finish_export_inode(destdnl->get_inode(), mdr->now, finished); 
      mds->queue_waiters(finished);   // this includes SINGLEAUTH waiters.

      // singleauth
      assert(destdnl->get_inode()->state_test(CInode::STATE_AMBIGUOUSAUTH));
      destdnl->get_inode()->state_clear(CInode::STATE_AMBIGUOUSAUTH);
      destdnl->get_inode()->take_waiting(CInode::WAIT_SINGLEAUTH, finished);
      
      // unfreeze
      assert(destdnl->get_inode()->is_frozen_inode() ||
	     destdnl->get_inode()->is_freezing_inode());
      destdnl->get_inode()->unfreeze_inode(finished);
      
      mds->queue_waiters(finished);
    }

    // drop our pins
    mdr->cleanup();

    mdlog->submit_entry(le, new C_MDS_CommittedSlave(this, mdr));
    mdlog->flush();
  } else {
    if (srcdn->is_auth() && destdnl->is_primary() &&
	destdnl->get_inode()->state_test(CInode::STATE_AMBIGUOUSAUTH)) {
         list<Context*> finished;

      dout(10) << " reversing inode export of " << *destdnl->get_inode() << dendl;
      destdnl->get_inode()->abort_export();
    
      // singleauth
      assert(destdnl->get_inode()->state_test(CInode::STATE_AMBIGUOUSAUTH));
      destdnl->get_inode()->state_clear(CInode::STATE_AMBIGUOUSAUTH);
      destdnl->get_inode()->take_waiting(CInode::WAIT_SINGLEAUTH, finished);
      
      // unfreeze
      assert(destdnl->get_inode()->is_frozen_inode() ||
	     destdnl->get_inode()->is_freezing_inode());
      destdnl->get_inode()->unfreeze_inode(finished);
      
      mds->queue_waiters(finished);
    }

    // abort
    do_rename_rollback(mdr->more()->rollback_bl, mdr->slave_to_mds, mdr);
  }
}

void _rollback_repair_dir(Mutation *mut, CDir *dir, rename_rollback::drec &r, utime_t ctime, 
			  bool isdir, int linkunlink, bool primary, frag_info_t &dirstat, nest_info_t &rstat)
{
  fnode_t *pf;
  if (dir->is_auth()) {
    pf = dir->project_fnode();
    mut->add_projected_fnode(dir);
    pf->version = dir->pre_dirty();
  } else
    pf = dir->get_projected_fnode();

  if (isdir) {
    pf->fragstat.nsubdirs += linkunlink;
    pf->rstat.rsubdirs += linkunlink;
  } else {
    pf->fragstat.nfiles += linkunlink;
    pf->rstat.rfiles += linkunlink;
  }    
  if (primary) {
    pf->rstat.rbytes += linkunlink * rstat.rbytes;
    pf->rstat.rfiles += linkunlink * rstat.rfiles;
    pf->rstat.rsubdirs += linkunlink * rstat.rsubdirs;
    pf->rstat.ranchors += linkunlink * rstat.ranchors;
    pf->rstat.rsnaprealms += linkunlink * rstat.rsnaprealms;
  }
  if (pf->fragstat.mtime == ctime) {
    pf->fragstat.mtime = r.dirfrag_old_mtime;
    if (pf->rstat.rctime == ctime)
      pf->rstat.rctime = r.dirfrag_old_rctime;
    mut->add_updated_lock(&dir->get_inode()->filelock);
    mut->add_updated_lock(&dir->get_inode()->nestlock);
  }
}

struct C_MDS_LoggedRenameRollback : public Context {
  Server *server;
  Mutation *mut;
  MDRequest *mdr;
  CInode *in;
  CDir *olddir;
  C_MDS_LoggedRenameRollback(Server *s, Mutation *m, MDRequest *r,
			     CInode *i, CDir *d) : server(s), mut(m), mdr(r), in(i), olddir(d) {}
  void finish(int r) {
    server->_rename_rollback_finish(mut, mdr, in, olddir);
  }
};

void Server::do_rename_rollback(bufferlist &rbl, int master, MDRequest *mdr)
{
  rename_rollback rollback;
  bufferlist::iterator p = rbl.begin();
  ::decode(rollback, p);

  dout(10) << "do_rename_rollback on " << rollback.reqid << dendl;

  //Mutation *mut = mdr;
  if (!mdr) {
    assert(mds->is_resolve());
    mds->mdcache->add_rollback(rollback.reqid);  // need to finish this update before resolve finishes
  }
  Mutation *mut = new Mutation(rollback.reqid);
  mut->ls = mds->mdlog->get_current_segment();

  CDir *srcdir = mds->mdcache->get_dirfrag(rollback.orig_src.dirfrag);
  assert(srcdir);
  dout(10) << "  srcdir " << *srcdir << dendl;
  CDentry *srcdn = srcdir->lookup(rollback.orig_src.dname);
  assert(srcdn);
  dout(10) << "   srcdn " << *srcdn << dendl;
  CDentry::linkage_t *srcdnl = srcdn->get_linkage();
  assert(srcdnl->is_null());

  CInode *in;
  if (rollback.orig_src.ino)
    in = mds->mdcache->get_inode(rollback.orig_src.ino);
  else
    in = mds->mdcache->get_inode(rollback.orig_src.remote_ino);    
  assert(in);
  
  CDir *destdir = mds->mdcache->get_dirfrag(rollback.orig_dest.dirfrag);
  assert(destdir);
  dout(10) << " destdir " << *destdir << dendl;
  CDentry *destdn = destdir->lookup(rollback.orig_dest.dname);
  assert(destdn);
  CDentry::linkage_t *destdnl = destdn->get_linkage();
  dout(10) << "  destdn " << *destdn << dendl;

  CDir *straydir = 0;
  CDentry *straydn = 0;
  CDentry::linkage_t *straydnl = 0;
  if (rollback.stray.dirfrag.ino) {
    straydir = mds->mdcache->get_dirfrag(rollback.stray.dirfrag);
    assert(straydir);
    dout(10) << "straydir " << *straydir << dendl;
    straydn = straydir->lookup(rollback.stray.dname);
    assert(straydn);
    straydnl = straydn->get_linkage();
    assert(straydnl->is_primary());
    dout(10) << " straydn " << *straydn << dendl;
  }
  
  // unlink
  destdir->unlink_inode(destdn);
  if (straydn)
    straydir->unlink_inode(straydn);

  bool linkmerge = ((rollback.orig_src.ino && 
		     rollback.orig_src.ino == rollback.orig_dest.remote_ino) ||
		    (rollback.orig_dest.ino && 
		     rollback.orig_dest.ino == rollback.orig_src.remote_ino));
  
  // repair src
  if (rollback.orig_src.ino)
    srcdir->link_primary_inode(srcdn, in);
  else
    srcdir->link_remote_inode(srcdn, rollback.orig_src.remote_ino, 
			      rollback.orig_src.remote_d_type);
  inode_t *pi;
  if (in->is_auth()) {
    pi = in->project_inode();
    mut->add_projected_inode(in);
    pi->version = in->pre_dirty();
  } else
    pi = in->get_projected_inode();
  if (pi->ctime == rollback.ctime)
    pi->ctime = rollback.orig_src.old_ctime;

  _rollback_repair_dir(mut, srcdir, rollback.orig_src, rollback.ctime,
		       in->is_dir(), 1, srcdnl->is_primary(), pi->dirstat, pi->rstat);

  // repair dest
  CInode *target = 0;
  if (rollback.orig_dest.ino) {
    target = mds->mdcache->get_inode(rollback.orig_dest.ino);
    destdir->link_primary_inode(destdn, target);
    assert(linkmerge || straydn);
  } else if (rollback.orig_dest.remote_ino) {
    target = mds->mdcache->get_inode(rollback.orig_dest.remote_ino);
    destdir->link_remote_inode(destdn, rollback.orig_dest.remote_ino, 
			       rollback.orig_dest.remote_d_type);
  }
  inode_t *ti = 0;
  if (target) {
    if (target->is_auth()) {
      ti = target->project_inode();
      mut->add_projected_inode(target);
      ti->version = target->pre_dirty();
    } else 
      ti = target->get_projected_inode();
    if (ti->ctime == rollback.ctime)
      ti->ctime = rollback.orig_dest.old_ctime;
    ti->nlink++;
  }
  if (target)
    _rollback_repair_dir(mut, destdir, rollback.orig_dest, rollback.ctime,
			 target->is_dir(), 0, destdnl->is_primary(), ti->dirstat, ti->rstat);
  else {
    frag_info_t blah;
    nest_info_t blah2;
    _rollback_repair_dir(mut, destdir, rollback.orig_dest, rollback.ctime, 0, -1, 0, blah, blah2);
  }

  // repair stray
  if (straydir)
    _rollback_repair_dir(mut, straydir, rollback.stray, rollback.ctime,
			 target->is_dir(), -1, true, ti->dirstat, ti->rstat);

  dout(-10) << "  srcdn back to " << *srcdn << dendl;
  dout(-10) << "   srci back to " << *srcdnl->get_inode() << dendl;
  dout(-10) << " destdn back to " << *destdn << dendl;
  if (destdnl->get_inode()) dout(-10) << "  desti back to " << *destdnl->get_inode() << dendl;
  
  // new subtree?
  if (srcdnl->is_primary() && srcdnl->get_inode()->is_dir()) {
    list<CDir*> ls;
    srcdnl->get_inode()->get_nested_dirfrags(ls);
    int auth = srcdn->authority().first;
    for (list<CDir*>::iterator p = ls.begin(); p != ls.end(); ++p) 
      mdcache->adjust_subtree_auth(*p, auth, auth);
  }

  // journal it
  ESlaveUpdate *le = new ESlaveUpdate(mdlog, "slave_rename_rollback", rollback.reqid, master,
				      ESlaveUpdate::OP_ROLLBACK, ESlaveUpdate::RENAME);
  le->commit.add_dir_context(srcdir);
  le->commit.add_primary_dentry(srcdn, true, 0);
  le->commit.add_dir_context(destdir);
  if (destdnl->is_null())
    le->commit.add_null_dentry(destdn, true);
  else if (destdnl->is_primary())
    le->commit.add_primary_dentry(destdn, true, 0);
  else if (destdnl->is_remote())
    le->commit.add_remote_dentry(destdn, true);
  if (straydn) {
    le->commit.add_dir_context(straydir);
    le->commit.add_null_dentry(straydn, true);
  }
  
  mdlog->submit_entry(le, new C_MDS_LoggedRenameRollback(this, mut, mdr,
							 srcdnl->get_inode(), destdn->get_dir()));
  mdlog->flush();
}

void Server::_rename_rollback_finish(Mutation *mut, MDRequest *mdr, CInode *in, CDir *olddir)
{
  dout(10) << "_rename_rollback_finish" << dendl;
  mut->apply();

  // update subtree map?
  if (in->is_dir())
    mdcache->adjust_subtree_after_rename(in, olddir);

  if (mdr)
    mds->mdcache->request_finish(mdr);
  else {
    mds->mdcache->finish_rollback(mut->reqid);
  }

  mut->cleanup();
  delete mut;
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
  /*if (!cur->inode.is_file() && !cur->inode.is_dir()) {
    dout(7) << "not a file or dir " << *cur << dendl;
    reply_request(mdr, -ENXIO);                 // FIXME what error do we want?
    return;
    }*/
  if ((req->head.args.open.flags & O_DIRECTORY) && !cur->inode.is_dir()) {
    dout(7) << "specified O_DIRECTORY on non-directory " << *cur << dendl;
    reply_request(mdr, -EINVAL);
    return;
  }
  
  // snapped data is read only
  if (mdr->ref_snapid != CEPH_NOSNAP &&
      (cmode & CEPH_FILE_MODE_WR)) {
    dout(7) << "snap " << mdr->ref_snapid << " is read-only " << *cur << dendl;
    reply_request(mdr, -EPERM);
    return;
  }

  // O_TRUNC
  if ((flags & O_TRUNC) &&
      !(req->get_retry_attempt() &&
	mdr->session->have_completed_request(req->get_reqid().tid))) {
    assert(cur->is_auth());

    // xlock file size
    set<SimpleLock*> rdlocks = mdr->rdlocks;
    set<SimpleLock*> wrlocks = mdr->wrlocks;
    set<SimpleLock*> xlocks = mdr->xlocks;
    wrlocks.insert(&cur->filelock);
    if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
      return;

    inode_t *pi = cur->get_projected_inode();
    if (pi->size > 0) {
      // wait for pending truncate?
      if (pi->is_truncating()) {
	dout(10) << " waiting for pending truncate from " << pi->truncate_from
		 << " to " << pi->truncate_size << " to complete on " << *cur << dendl;
	cur->add_waiter(CInode::WAIT_TRUNC, new C_MDS_RetryRequest(mdcache, mdr));
	return;
      }

      handle_client_opent(mdr, cmode);
      return;
    }
  } 

  // sync filelock if snapped.
  //  this makes us wait for writers to flushsnaps, ensuring we get accurate metadata,
  //  and that data itself is flushed so that we can read the snapped data off disk.
  if (mdr->ref_snapid != CEPH_NOSNAP && !cur->is_dir()) {
    // first rdlock.
    set<SimpleLock*> rdlocks = mdr->rdlocks;
    set<SimpleLock*> wrlocks = mdr->wrlocks;
    set<SimpleLock*> xlocks = mdr->xlocks;
    rdlocks.insert(&cur->filelock);
    if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
      return;

    // sure sure we ended up in the SYNC state
    if (cur->filelock.is_stable() && cur->filelock.get_state() != LOCK_SYNC) {
      assert(cur->is_auth());
      mds->locker->simple_sync(&cur->filelock);
    }
    if (!cur->filelock.is_stable()) {
      dout(10) << " waiting for filelock to stabilize on " << *cur << dendl;
      cur->filelock.add_waiter(SimpleLock::WAIT_STABLE, new C_MDS_RetryRequest(mdcache, mdr));
      return;
    }
  }


  if (cur->is_file() || cur->is_dir()) {
    if (mdr->ref_snapid == CEPH_NOSNAP) {
      // register new cap
      Capability *cap = mds->locker->issue_new_caps(cur, cmode, mdr->session);
      dout(12) << "open issued caps " << ccap_string(cap->pending())
	       << " for " << req->get_orig_source()
	       << " on " << *cur << dendl;
      mdr->cap = cap;
    } else {
      int caps = ceph_caps_for_mode(cmode);
      dout(12) << "open issued IMMUTABLE SNAP caps " << ccap_string(caps)
	       << " for " << req->get_orig_source()
	       << " snapid " << mdr->ref_snapid
	       << " on " << *cur << dendl;
      mdr->snap_caps = caps;
    }
  }

  // make sure this inode gets into the journal
  if (!cur->xlist_open_file.is_on_xlist()) {
    LogSegment *ls = mds->mdlog->get_current_segment();
    EOpen *le = new EOpen(mds->mdlog);
    le->add_clean_inode(cur);
    ls->open_files.push_back(&cur->xlist_open_file);
    mds->mdlog->submit_entry(le);
  }
  
  // hit pop
  mdr->now = g_clock.now();
  if (cmode == CEPH_FILE_MODE_RDWR ||
      cmode == CEPH_FILE_MODE_WR) 
    mds->balancer->hit_inode(mdr->now, cur, META_POP_IWR);
  else
    mds->balancer->hit_inode(mdr->now, cur, META_POP_IRD, 
			     mdr->client_request->get_orig_source().num());

  CDentry *dn = 0;
  if (req->get_dentry_wanted()) {
    assert(mdr->trace.size());
    dn = mdr->trace.back();
  }
  reply_request(mdr, 0, cur, dn);
}


void Server::handle_client_opent(MDRequest *mdr, int cmode)
{
  CInode *in = mdr->ref;
  assert(in);

  dout(10) << "handle_client_opent " << *in << dendl;

  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "open_truncate");

  // prepare
  inode_t *pi = in->project_inode();
  pi->mtime = pi->ctime = g_clock.real_now();
  pi->version = in->pre_dirty();

  pi->truncate_from = pi->size;
  pi->size = 0;
  pi->rstat.rbytes = 0;
  pi->truncate_size = 0;
  pi->truncate_seq++;

  le->metablob.add_truncate_start(in->ino());
  le->metablob.add_client_req(mdr->reqid);

  mdcache->predirty_journal_parents(mdr, &le->metablob, in, 0, PREDIRTY_PRIMARY, false);
  mdcache->journal_dirty_inode(mdr, &le->metablob, in);
  
  // do the open
  SnapRealm *realm = in->find_snaprealm();
  Capability *cap = mds->locker->issue_new_caps(in, cmode, mdr->session, realm);

  in->authlock.set_state(LOCK_EXCL);
  mdr->cap = cap;

  // make sure ino gets into the journal
  le->metablob.add_opened_ino(in->ino());
  LogSegment *ls = mds->mdlog->get_current_segment();
  ls->open_files.push_back(&in->xlist_open_file);
  
  journal_and_reply(mdr, in, 0, le, new C_MDS_inode_update_finish(mds, mdr, in, true));
}



class C_MDS_openc_finish : public Context {
  MDS *mds;
  MDRequest *mdr;
  CDentry *dn;
  CInode *newi;
  snapid_t follows;
public:
  C_MDS_openc_finish(MDS *m, MDRequest *r, CDentry *d, CInode *ni, snapid_t f) :
    mds(m), mdr(r), dn(d), newi(ni), follows(f) {}
  void finish(int r) {
    assert(r == 0);

    dn->pop_projected_linkage();

    // dirty inode, dn, dir
    newi->inode.version--;   // a bit hacky, see C_MDS_mknod_finish
    newi->mark_dirty(newi->inode.version+1, mdr->ls);

    mdr->apply();

    // set/pin ref inode for open()
    mdr->ref = newi;
    mdr->ref_snapid = CEPH_NOSNAP;
    mdr->pin(newi);
    mdr->trace.push_back(dn);

    mds->server->reply_request(mdr, 0);
  }
};


void Server::handle_client_openc(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;

  dout(7) << "open w/ O_CREAT on " << req->get_filepath() << dendl;
  
  bool excl = (req->head.args.open.flags & O_EXCL);
  CDentry *dn = rdlock_path_xlock_dentry(mdr, !excl, false);
  if (!dn) return;
  CDentry::linkage_t *dnl = dn->get_projected_linkage();

  if (!dnl->is_null()) {
    // it existed.  
    if (req->head.args.open.flags & O_EXCL) {
      dout(10) << "O_EXCL, target exists, failing with -EEXIST" << dendl;
      reply_request(mdr, -EEXIST, dnl->get_inode(), dn);
      return;
    } 
    
    // pass to regular open handler.
    mdr->trace.push_back(dn);
    mdr->ref = dnl->get_inode();
    handle_client_open(mdr);
    return;
  }

  // created null dn.

  CInode *diri = dn->get_dir()->get_inode();
    
  // create inode.
  mdr->now = g_clock.real_now();

  SnapRealm *realm = diri->find_snaprealm();   // use directory's realm; inode isn't attached yet.
  snapid_t follows = realm->get_newest_seq();

  int cmode = ceph_flags_to_mode(req->head.args.open.flags);

  CInode *in = prepare_new_inode(mdr, dn->get_dir(), inodeno_t(req->head.ino));
  assert(in);
  
  // it's a file.
  dn->push_projected_linkage(in);

  in->inode.mode = req->head.args.open.mode | S_IFREG;
  in->inode.version = dn->pre_dirty();
  in->inode.max_size = (cmode & CEPH_FILE_MODE_WR) ? in->get_layout_size_increment() : 0;
  in->inode.rstat.rfiles = 1;

  dn->first = in->first = follows+1;
  
  // prepare finisher
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "openc");
  le->metablob.add_client_req(req->get_reqid());
  journal_allocated_inos(mdr, &le->metablob);
  mdcache->predirty_journal_parents(mdr, &le->metablob, in, dn->get_dir(), PREDIRTY_PRIMARY|PREDIRTY_DIR, 1);
  le->metablob.add_primary_dentry(dn, true, in);

  // do the open
  Capability *cap = mds->locker->issue_new_caps(in, cmode, mdr->session, realm);
  in->authlock.set_state(LOCK_EXCL);

  // stick cap, snapbl info in mdr
  mdr->cap = cap;

  // make sure this inode gets into the journal
  le->metablob.add_opened_ino(in->ino());
  LogSegment *ls = mds->mdlog->get_current_segment();
  ls->open_files.push_back(&in->xlist_open_file);

  C_MDS_openc_finish *fin = new C_MDS_openc_finish(mds, mdr, dn, in, follows);
  journal_and_reply(mdr, in, dn, le, fin);
}






// snaps

void Server::handle_client_lssnap(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;

  // traverse to path
  CInode *diri = mdcache->get_inode(req->get_filepath().get_ino());
  if (!diri) {
     reply_request(mdr, -ESTALE);
     return;
  }
  if (!diri->is_auth()) {
    mdcache->request_forward(mdr, diri->authority().first);
    return;
  }
  if (!diri->is_dir()) {
    reply_request(mdr, -ENOTDIR);
    return;
  }
  dout(10) << "lssnap on " << *diri << dendl;

  // lock snap
  set<SimpleLock*> rdlocks, wrlocks, xlocks;
  mds->locker->include_snap_rdlocks(rdlocks, diri);
  if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
    return;

  SnapRealm *realm = diri->find_snaprealm();
  map<snapid_t,SnapInfo*> infomap;
  realm->get_snap_info(infomap);

  snapid_t oldest = diri->get_oldest_snap();
  dout(10) << " oldest snap for this inode is " << oldest << dendl;

  utime_t now = g_clock.now();
  __u32 num = 0;
  bufferlist dnbl;
  for (map<snapid_t,SnapInfo*>::iterator p = infomap.begin();
       p != infomap.end();
       p++) {
    if (p->first < oldest)
      continue;

    dout(10) << p->first << " -> " << *p->second << dendl;

    // actual
    if (p->second->ino == diri->ino())
      ::encode(p->second->name, dnbl);
    else
      ::encode(p->second->get_long_name(), dnbl);
    encode_infinite_lease(dnbl);
    diri->encode_inodestat(dnbl, mdr->session, realm, p->first);
    num++;
  }

  bufferlist dirbl;
  encode_empty_dirstat(dirbl);
  ::encode(num, dirbl);
  __u8 t = 1;
  ::encode(t, dirbl);  // end
  ::encode(t, dirbl);  // complete
  dirbl.claim_append(dnbl);
  
  MClientReply *reply = new MClientReply(req);
  reply->set_dir_bl(dirbl);
  reply_request(mdr, reply, diri);
}


// MKSNAP

struct C_MDS_mksnap_finish : public Context {
  MDS *mds;
  MDRequest *mdr;
  CInode *diri;
  SnapInfo info;
  C_MDS_mksnap_finish(MDS *m, MDRequest *r, CInode *di, SnapInfo &i) :
    mds(m), mdr(r), diri(di), info(i) {}
  void finish(int r) {
    mds->server->_mksnap_finish(mdr, diri, info);
  }
};

void Server::handle_client_mksnap(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;

  CInode *diri = mdcache->get_inode(req->get_filepath().get_ino());
  if (!diri) {
    reply_request(mdr, -ESTALE);
    return;
  }

  if (!diri->is_auth()) {    // fw to auth?
    mdcache->request_forward(mdr, diri->authority().first);
    return;
  }

  // dir only
  if (!diri->is_dir()) {
    reply_request(mdr, -ENOTDIR);
    return;
  }
  if (diri->is_system()) {  // no snaps on root dir, at least not until we can store it
    reply_request(mdr, -EPERM);
    return;
  }

  const string &snapname = req->get_filepath().last_dentry();
  dout(10) << "mksnap " << snapname << " on " << *diri << dendl;

  // lock snap
  set<SimpleLock*> rdlocks, wrlocks, xlocks;

  mds->locker->include_snap_rdlocks(rdlocks, diri);
  rdlocks.erase(&diri->snaplock);
  xlocks.insert(&diri->snaplock);
  if (!diri->is_anchored()) {
    // we need to anchor... get these locks up front!
    CDentry *dn = diri->get_parent_dn();
    while (dn) {
      rdlocks.insert(&dn->lock);
      dn = dn->get_dir()->get_inode()->get_parent_dn();
    }
    xlocks.insert(&diri->linklock); 
  }

  if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
    return;

  // make sure name is unique
  if (diri->snaprealm &&
      diri->snaprealm->exists(snapname)) {
    reply_request(mdr, -EEXIST);
    return;
  }
  if (snapname.length() == 0 ||
      snapname[0] == '_') {
    reply_request(mdr, -EINVAL);
    return;
  }

  if (mdr->now == utime_t())
    mdr->now = g_clock.now();

  // allocate a snapid
  if (!mdr->more()->stid) {
    // prepare an stid
    mds->snapclient->prepare_create(diri->ino(), snapname, mdr->now, 
				    &mdr->more()->stid, &mdr->more()->snapidbl,
				    new C_MDS_RetryRequest(mds->mdcache, mdr));
    return;
  }

  version_t stid = mdr->more()->stid;
  snapid_t snapid;
  bufferlist::iterator p = mdr->more()->snapidbl.begin();
  ::decode(snapid, p);
  dout(10) << " stid " << stid << " snapid " << snapid << dendl;

  // journal
  SnapInfo info;
  info.ino = diri->ino();
  info.snapid = snapid;
  info.name = snapname;
  info.stamp = mdr->now;

  inode_t *pi = diri->project_inode();
  pi->ctime = info.stamp;
  pi->version = diri->pre_dirty();

  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "mksnap");
  le->metablob.add_client_req(req->get_reqid());
  le->metablob.add_table_transaction(TABLE_SNAP, stid);
  mdcache->predirty_journal_parents(mdr, &le->metablob, diri, 0, PREDIRTY_PRIMARY, false);
  mdcache->journal_cow_inode(mdr, &le->metablob, diri);
  
  // project the snaprealm.. hack!
  bufferlist snapbl;
  bool newrealm = false;
  if (!diri->snaprealm) {
    newrealm = true;
    diri->open_snaprealm(true);
    diri->snaprealm->created = snapid;
  }
  snapid_t old_seq = diri->snaprealm->seq;
  snapid_t old_lc = diri->snaprealm->last_created;
  diri->snaprealm->snaps[snapid] = info;
  diri->snaprealm->seq = snapid;
  diri->snaprealm->last_created = snapid;
  diri->encode_snap_blob(snapbl);
  diri->snaprealm->snaps.erase(snapid);
  diri->snaprealm->seq = old_seq;
  diri->snaprealm->last_created = old_lc;
  if (newrealm)
    diri->close_snaprealm(true);
  
  le->metablob.add_primary_dentry(diri->get_projected_parent_dn(), true, 0, 0, &snapbl);

  mdlog->submit_entry(le, new C_MDS_mksnap_finish(mds, mdr, diri, info));
  mdlog->flush();
}

void Server::_mksnap_finish(MDRequest *mdr, CInode *diri, SnapInfo &info)
{
  dout(10) << "_mksnap_finish " << *mdr << " " << info << dendl;

  diri->pop_and_dirty_projected_inode(mdr->ls);
  mdr->apply();

  mds->snapclient->commit(mdr->more()->stid, mdr->ls);

  // create snap
  snapid_t snapid = info.snapid;
  int op = CEPH_SNAP_OP_CREATE;
  if (!diri->snaprealm) {
    diri->open_snaprealm();
    diri->snaprealm->created = snapid;
    op = CEPH_SNAP_OP_SPLIT;
  }
  diri->snaprealm->snaps[snapid] = info;
  diri->snaprealm->seq = snapid;
  diri->snaprealm->last_created = snapid;
  dout(10) << "snaprealm now " << *diri->snaprealm << dendl;

  mdcache->do_realm_invalidate_and_update_notify(diri, op);

  // yay
  mdr->ref = diri;
  mdr->ref_snapid = snapid;
  mdr->ref_snapdiri = diri;
  MClientReply *reply = new MClientReply(mdr->client_request, 0);
  reply->snapbl = diri->snaprealm->get_snap_trace();
  reply_request(mdr, reply, diri);
}


// RMSNAP

struct C_MDS_rmsnap_finish : public Context {
  MDS *mds;
  MDRequest *mdr;
  CInode *diri;
  snapid_t snapid;
  C_MDS_rmsnap_finish(MDS *m, MDRequest *r, CInode *di, snapid_t sn) :
    mds(m), mdr(r), diri(di), snapid(sn) {}
  void finish(int r) {
    mds->server->_rmsnap_finish(mdr, diri, snapid);
  }
};

void Server::handle_client_rmsnap(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;

  CInode *diri = mdcache->get_inode(req->get_filepath().get_ino());
  if (!diri) {
    reply_request(mdr, -ESTALE);
    return;
  }
  if (!diri->is_auth()) {    // fw to auth?
    mdcache->request_forward(mdr, diri->authority().first);
    return;
  }
  if (!diri->is_dir()) {
    reply_request(mdr, -ENOTDIR);
    return;
  }
  if (diri->is_system()) {  // no snaps on root dir, at least not until we can store it
    reply_request(mdr, -EPERM);
    return;
  }

  const string &snapname = req->get_filepath().last_dentry();
  dout(10) << "rmsnap " << snapname << " on " << *diri << dendl;

  // does snap exist?
  if (snapname.length() == 0 || snapname[0] == '_') {
    reply_request(mdr, -EINVAL);   // can't prune a parent snap, currently.
    return;
  }
  if (diri->snaprealm &&
      !diri->snaprealm->exists(snapname)) {
    reply_request(mdr, -ENOENT);
    return;
  }
  snapid_t snapid = diri->snaprealm->resolve_snapname(snapname, diri->ino());
  dout(10) << " snapname " << snapname << " is " << snapid << dendl;

  set<SimpleLock*> rdlocks, wrlocks, xlocks;
  mds->locker->include_snap_rdlocks(rdlocks, diri);
  rdlocks.erase(&diri->snaplock);
  xlocks.insert(&diri->snaplock);

  if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
    return;

  // prepare
  if (!mdr->more()->stid) {
    mds->snapclient->prepare_destroy(diri->ino(), snapid,
				     &mdr->more()->stid, &mdr->more()->snapidbl,
				     new C_MDS_RetryRequest(mds->mdcache, mdr));
    return;
  }
  version_t stid = mdr->more()->stid;
  bufferlist::iterator p = mdr->more()->snapidbl.begin();
  snapid_t seq;
  ::decode(seq, p);  
  dout(10) << " stid is " << stid << ", seq is " << seq << dendl;

  // journal
  inode_t *pi = diri->project_inode();
  pi->ctime = g_clock.now();
  pi->version = diri->pre_dirty();
  
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "rmsnap");
  le->metablob.add_client_req(req->get_reqid());
  le->metablob.add_table_transaction(TABLE_SNAP, stid);
  mdcache->predirty_journal_parents(mdr, &le->metablob, diri, 0, PREDIRTY_PRIMARY, false);
  mdcache->journal_cow_inode(mdr, &le->metablob, diri);
  
  // project the snaprealm.. hack!
  bufferlist snapbl;
  snapid_t old_seq = diri->snaprealm->seq;
  snapid_t old_ld = diri->snaprealm->last_destroyed;
  SnapInfo old_info = diri->snaprealm->snaps[snapid];
  diri->snaprealm->snaps.erase(snapid);
  diri->snaprealm->seq = seq;
  diri->snaprealm->last_destroyed = seq;
  diri->encode_snap_blob(snapbl);
  diri->snaprealm->snaps[snapid] = old_info;
  diri->snaprealm->seq = old_seq;
  diri->snaprealm->last_destroyed = old_ld;
  le->metablob.add_primary_dentry(diri->get_projected_parent_dn(), true, 0, 0, &snapbl);

  mdlog->submit_entry(le, new C_MDS_rmsnap_finish(mds, mdr, diri, snapid));
  mdlog->flush();
}

void Server::_rmsnap_finish(MDRequest *mdr, CInode *diri, snapid_t snapid)
{
  dout(10) << "_rmsnap_finish " << *mdr << " " << snapid << dendl;
  snapid_t stid = mdr->more()->stid;
  bufferlist::iterator p = mdr->more()->snapidbl.begin();
  snapid_t seq;
  ::decode(seq, p);  

  diri->pop_and_dirty_projected_inode(mdr->ls);
  mdr->apply();

  mds->snapclient->commit(stid, mdr->ls);

  // remove snap
  diri->snaprealm->snaps.erase(snapid);
  diri->snaprealm->last_destroyed = seq;
  diri->snaprealm->seq = seq;
  dout(10) << "snaprealm now " << *diri->snaprealm << dendl;

  mdcache->do_realm_invalidate_and_update_notify(diri, CEPH_SNAP_OP_DESTROY);

  // yay
  mdr->ref = diri;
  MClientReply *reply = new MClientReply(mdr->client_request, 0);
  reply_request(mdr, reply);
}


