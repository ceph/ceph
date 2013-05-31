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

#include <boost/lexical_cast.hpp>
#include "include/assert.h"  // lexical_cast includes system assert.h

#include <boost/config/warning_disable.hpp>
#include <boost/spirit/include/qi.hpp>
#include <boost/fusion/include/std_pair.hpp>

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
#include "Mutation.h"

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

#include "events/EUpdate.h"
#include "events/ESlaveUpdate.h"
#include "events/ESession.h"
#include "events/EOpen.h"
#include "events/ECommitted.h"

#include "include/filepath.h"
#include "common/Timer.h"
#include "common/perf_counters.h"
#include "include/compat.h"
#include "osd/OSDMap.h"

#include <errno.h>
#include <fcntl.h>

#include <list>
#include <iostream>
using namespace std;

#include "common/config.h"

#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds." << mds->get_nodeid() << ".server "

void Server::create_logger()
{
  PerfCountersBuilder plb(g_ceph_context, "mds_server", l_mdss_first, l_mdss_last);
  plb.add_u64_counter(l_mdss_hcreq,"hcreq"); // handle client req
  plb.add_u64_counter(l_mdss_hsreq, "hsreq"); // slave
  plb.add_u64_counter(l_mdss_hcsess, "hcsess");    // client session
  plb.add_u64_counter(l_mdss_dcreq, "dcreq"); // dispatch client req
  plb.add_u64_counter(l_mdss_dsreq, "dsreq"); // slave
  logger = plb.create_perf_counters();
  g_ceph_context->get_perfcounters_collection()->add(logger);
}


/* This function DOES put the passed message before returning*/
void Server::dispatch(Message *m) 
{
  switch (m->get_type()) {
  case CEPH_MSG_CLIENT_RECONNECT:
    handle_client_reconnect(static_cast<MClientReconnect*>(m));
    return;
  }

  // active?
  if (!mds->is_active() && 
      !(mds->is_stopping() && m->get_source().is_mds())) {
    if ((mds->is_reconnect() || mds->get_want_state() == CEPH_MDS_STATE_RECONNECT) &&
	m->get_type() == CEPH_MSG_CLIENT_REQUEST &&
	(static_cast<MClientRequest*>(m))->is_replay()) {
      dout(3) << "queuing replayed op" << dendl;
      mds->enqueue_replay(new C_MDS_RetryMessage(mds, m));
      return;
    } else if (mds->is_clientreplay() &&
	       // session open requests need to be handled during replay,
	       // close requests need to be delayed
	       ((m->get_type() == CEPH_MSG_CLIENT_SESSION &&
		 (static_cast<MClientSession*>(m))->get_op() != CEPH_SESSION_REQUEST_CLOSE) ||
		(m->get_type() == CEPH_MSG_CLIENT_REQUEST &&
		 (static_cast<MClientRequest*>(m))->is_replay()))) {
      // replaying!
    } else if (m->get_type() == MSG_MDS_SLAVE_REQUEST) {
      // handle_slave_request() will wait if necessary
    } else {
      dout(3) << "not active yet, waiting" << dendl;
      mds->wait_for_active(new C_MDS_RetryMessage(mds, m));
      return;
    }
  }

  switch (m->get_type()) {
  case CEPH_MSG_CLIENT_SESSION:
    handle_client_session(static_cast<MClientSession*>(m));
    return;
  case CEPH_MSG_CLIENT_REQUEST:
    handle_client_request(static_cast<MClientRequest*>(m));
    return;
  case MSG_MDS_SLAVE_REQUEST:
    handle_slave_request(static_cast<MMDSSlaveRequest*>(m));
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
  uint64_t state_seq;
  bool open;
  version_t cmapv;
  interval_set<inodeno_t> inos;
  version_t inotablev;
public:
  C_MDS_session_finish(MDS *m, Session *se, uint64_t sseq, bool s, version_t mv) :
    mds(m), session(se), state_seq(sseq), open(s), cmapv(mv), inotablev(0) { }
  C_MDS_session_finish(MDS *m, Session *se, uint64_t sseq, bool s, version_t mv, interval_set<inodeno_t>& i, version_t iv) :
    mds(m), session(se), state_seq(sseq), open(s), cmapv(mv), inos(i), inotablev(iv) { }
  void finish(int r) {
    assert(r == 0);
    mds->server->_session_logged(session, state_seq, open, cmapv, inos, inotablev);
  }
};

Session *Server::get_session(Message *m)
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

/* This function DOES put the passed message before returning*/
void Server::handle_client_session(MClientSession *m)
{
  version_t pv;
  Session *session = get_session(m);

  dout(3) << "handle_client_session " << *m << " from " << m->get_source() << dendl;
  assert(m->get_source().is_client()); // should _not_ come from an mds!

  if (!session) {
    dout(0) << " ignoring sessionless msg " << *m << dendl;
    m->put();
    return;
  }

  uint64_t sseq = 0;
  switch (m->get_op()) {
  case CEPH_SESSION_REQUEST_OPEN:
    if (session->is_opening() ||
	session->is_open() ||
	session->is_stale() ||
	session->is_killing()) {
      dout(10) << "currently open|opening|stale|killing, dropping this req" << dendl;
      m->put();
      return;
    }
    assert(session->is_closed() ||
	   session->is_closing());
    sseq = mds->sessionmap.set_state(session, Session::STATE_OPENING);
    mds->sessionmap.touch_session(session);
    pv = ++mds->sessionmap.projected;
    mdlog->start_submit_entry(new ESession(m->get_source_inst(), true, pv),
			      new C_MDS_session_finish(mds, session, sseq, true, pv));
    mdlog->flush();
    break;

  case CEPH_SESSION_REQUEST_RENEWCAPS:
    if (session->is_open() ||
	session->is_stale()) {
      mds->sessionmap.touch_session(session);
      if (session->is_stale()) {
	mds->sessionmap.set_state(session, Session::STATE_OPEN);
	mds->locker->resume_stale_caps(session);
	mds->sessionmap.touch_session(session);
      }
      mds->messenger->send_message(new MClientSession(CEPH_SESSION_RENEWCAPS, m->get_seq()), 
				   m->get_connection());
    } else {
      dout(10) << "ignoring renewcaps on non open|stale session (" << session->get_state_name() << ")" << dendl;
    }
    break;
    
  case CEPH_SESSION_REQUEST_CLOSE:
    {
      if (session->is_closed() || 
	  session->is_closing() ||
	  session->is_killing()) {
	dout(10) << "already closed|closing|killing, dropping this req" << dendl;
	m->put();
	return;
      }
      if (session->is_importing()) {
	dout(10) << "ignoring close req on importing session" << dendl;
	m->put();
	return;
      }
      assert(session->is_open() || 
	     session->is_stale() || 
	     session->is_opening());
      if (m->get_seq() < session->get_push_seq()) {
	dout(10) << "old push seq " << m->get_seq() << " < " << session->get_push_seq() 
		 << ", dropping" << dendl;
	m->put();
	return;
      }
      if (m->get_seq() != session->get_push_seq()) {
	dout(0) << "old push seq " << m->get_seq() << " != " << session->get_push_seq() 
		<< ", BUGGY!" << dendl;
	assert(0);
      }
      journal_close_session(session, Session::STATE_CLOSING);
    }
    break;

  default:
    assert(0);
  }
  m->put();
}

void Server::_session_logged(Session *session, uint64_t state_seq, bool open, version_t pv,
			     interval_set<inodeno_t>& inos, version_t piv)
{
  dout(10) << "_session_logged " << session->info.inst << " state_seq " << state_seq << " " << (open ? "open":"close")
	   << " " << pv << dendl;

  if (piv) {
    mds->inotable->apply_release_ids(inos);
    assert(mds->inotable->get_version() == piv);
  }

  // apply
  if (session->get_state_seq() != state_seq) {
    dout(10) << " journaled state_seq " << state_seq << " != current " << session->get_state_seq()
	     << ", noop" << dendl;
    // close must have been canceled (by an import?), or any number of other things..
  } else if (open) {
    assert(session->is_opening());
    mds->sessionmap.set_state(session, Session::STATE_OPEN);
    mds->sessionmap.touch_session(session);
    mds->messenger->send_message(new MClientSession(CEPH_SESSION_OPEN), session->connection);
  } else if (session->is_closing() ||
	     session->is_killing()) {
    // kill any lingering capabilities, leases, requests
    while (!session->caps.empty()) {
      Capability *cap = session->caps.front();
      CInode *in = cap->get_inode();
      dout(20) << " killing capability " << ccap_string(cap->issued()) << " on " << *in << dendl;
      mds->locker->remove_client_cap(in, session->info.inst.name.num());
    }
    while (!session->leases.empty()) {
      ClientLease *r = session->leases.front();
      CDentry *dn = static_cast<CDentry*>(r->parent);
      dout(20) << " killing client lease of " << *dn << dendl;
      dn->remove_client_lease(r, mds->locker);
    }
    
    if (session->is_closing()) {
      // mark con disposable.  if there is a fault, we will get a
      // reset and clean it up.  if the client hasn't received the
      // CLOSE message yet, they will reconnect and get an
      // ms_handle_remote_reset() and realize they had in fact closed.
      // do this *before* sending the message to avoid a possible
      // race.
      mds->messenger->mark_disposable(session->connection);

      // reset session
      mds->send_message_client(new MClientSession(CEPH_SESSION_CLOSE), session);
      mds->sessionmap.set_state(session, Session::STATE_CLOSED);
      session->clear();
    } else if (session->is_killing()) {
      // destroy session, close connection
      mds->messenger->mark_down(session->connection);
      mds->sessionmap.remove_session(session);
    } else {
      assert(0);
    }
  } else {
    assert(0);
  }
  mds->sessionmap.version++;  // noop
}

version_t Server::prepare_force_open_sessions(map<client_t,entity_inst_t>& cm,
					      map<client_t,uint64_t>& sseqmap)
{
  version_t pv = ++mds->sessionmap.projected;
  dout(10) << "prepare_force_open_sessions " << pv 
	   << " on " << cm.size() << " clients"
	   << dendl;
  for (map<client_t,entity_inst_t>::iterator p = cm.begin(); p != cm.end(); ++p) {
    Session *session = mds->sessionmap.get_or_add_session(p->second);
    if (session->is_closed() || 
	session->is_closing() ||
	session->is_killing())
      sseqmap[p->first] = mds->sessionmap.set_state(session, Session::STATE_OPENING);
    else
      assert(session->is_open() ||
	     session->is_opening() ||
	     session->is_stale());
    session->inc_importing();
    mds->sessionmap.touch_session(session);
  }
  return pv;
}

void Server::finish_force_open_sessions(map<client_t,entity_inst_t>& cm,
					map<client_t,uint64_t>& sseqmap)
{
  /*
   * FIXME: need to carefully consider the race conditions between a
   * client trying to close a session and an MDS doing an import
   * trying to force open a session...  
   */
  dout(10) << "finish_force_open_sessions on " << cm.size() << " clients,"
	   << " v " << mds->sessionmap.version << " -> " << (mds->sessionmap.version+1) << dendl;
  for (map<client_t,entity_inst_t>::iterator p = cm.begin(); p != cm.end(); ++p) {
    Session *session = mds->sessionmap.get_session(p->second.name);
    assert(session);
    
    if (sseqmap.count(p->first)) {
      uint64_t sseq = sseqmap[p->first];
      if (session->get_state_seq() != sseq) {
	dout(10) << "force_open_sessions skipping changed " << session->info.inst << dendl;
      } else {
	dout(10) << "force_open_sessions opened " << session->info.inst << dendl;
	mds->sessionmap.set_state(session, Session::STATE_OPEN);
	mds->sessionmap.touch_session(session);
	Message *m = new MClientSession(CEPH_SESSION_OPEN);
	if (session->connection)
	  messenger->send_message(m, session->connection);
	else
	  session->preopen_out_queue.push_back(m);
      }
    } else {
      dout(10) << "force_open_sessions skipping already-open " << session->info.inst << dendl;
      assert(session->is_open() || session->is_stale());
    }
    session->dec_importing();
  }
  mds->sessionmap.version++;
}

struct C_MDS_TerminatedSessions : public Context {
  Server *server;
  C_MDS_TerminatedSessions(Server *s) : server(s) {}
  void finish(int r) {
    server->terminating_sessions = false;
  }
};

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
    if (session->is_closing() ||
	session->is_killing() ||
	session->is_closed())
      continue;
    journal_close_session(session, Session::STATE_CLOSING);
  }

  mdlog->wait_for_safe(new C_MDS_TerminatedSessions(this));
}


void Server::find_idle_sessions()
{
  dout(10) << "find_idle_sessions.  laggy until " << mds->laggy_until << dendl;
  
  // timeout/stale
  //  (caps go stale, lease die)
  utime_t now = ceph_clock_now(g_ceph_context);
  utime_t cutoff = now;
  cutoff -= g_conf->mds_session_timeout;  
  while (1) {
    Session *session = mds->sessionmap.get_oldest_session(Session::STATE_OPEN);
    if (!session) break;
    dout(20) << "laggiest active session is " << session->info.inst << dendl;
    if (session->last_cap_renew >= cutoff) {
      dout(20) << "laggiest active session is " << session->info.inst << " and sufficiently new (" 
	       << session->last_cap_renew << ")" << dendl;
      break;
    }

    dout(10) << "new stale session " << session->info.inst << " last " << session->last_cap_renew << dendl;
    mds->sessionmap.set_state(session, Session::STATE_STALE);
    mds->locker->revoke_stale_caps(session);
    mds->locker->remove_stale_leases(session);
    mds->send_message_client(new MClientSession(CEPH_SESSION_STALE, session->get_push_seq()), session);
  }

  // autoclose
  cutoff = now;
  cutoff -= g_conf->mds_session_autoclose;

  // don't kick clients if we've been laggy
  if (mds->laggy_until > cutoff) {
    dout(10) << " laggy_until " << mds->laggy_until << " > cutoff " << cutoff
	     << ", not kicking any clients to be safe" << dendl;
    return;
  }

  while (1) {
    Session *session = mds->sessionmap.get_oldest_session(Session::STATE_STALE);
    if (!session)
      break;
    if (session->is_importing()) {
      dout(10) << "stopping at importing session " << session->info.inst << dendl;
      break;
    }
    assert(session->is_stale());
    if (session->last_cap_renew >= cutoff) {
      dout(20) << "oldest stale session is " << session->info.inst << " and sufficiently new (" 
	       << session->last_cap_renew << ")" << dendl;
      break;
    }
    
    utime_t age = now;
    age -= session->last_cap_renew;
    mds->clog.info() << "closing stale session " << session->info.inst
	<< " after " << age << "\n";
    dout(10) << "autoclosing stale session " << session->info.inst << " last " << session->last_cap_renew << dendl;
    kill_session(session);
  }
}

void Server::kill_session(Session *session)
{
  if ((session->is_opening() ||
       session->is_open() ||
       session->is_stale()) &&
      !session->is_importing()) {
    dout(10) << "kill_session " << session << dendl;
    journal_close_session(session, Session::STATE_KILLING);
  } else {
    dout(10) << "kill_session importing or already closing/killing " << session << dendl;
    assert(session->is_closing() || 
	   session->is_closed() || 
	   session->is_killing() ||
	   session->is_importing());
  }
}

void Server::journal_close_session(Session *session, int state)
{
  uint64_t sseq = mds->sessionmap.set_state(session, state);
  version_t pv = ++mds->sessionmap.projected;
  version_t piv = 0;

  // release alloc and pending-alloc inos for this session
  // and wipe out session state, in case the session close aborts for some reason
  interval_set<inodeno_t> both;
  both.swap(session->info.prealloc_inos);
  both.insert(session->pending_prealloc_inos);
  session->pending_prealloc_inos.clear();
  if (both.size()) {
    mds->inotable->project_release_ids(both);
    piv = mds->inotable->get_projected_version();
  } else
    piv = 0;

  mdlog->start_submit_entry(new ESession(session->info.inst, false, pv, both, piv),
			    new C_MDS_session_finish(mds, session, sseq, false, pv, both, piv));
  mdlog->flush();

  // clean up requests, too
  elist<MDRequest*>::iterator p = session->requests.begin(member_offset(MDRequest,
									item_session_request));
  while (!p.end()) {
    MDRequest *mdr = *p;
    ++p;
    mdcache->request_kill(mdr);
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

  // clients will get the mdsmap and discover we're reconnecting via the monitor.
  
  reconnect_start = ceph_clock_now(g_ceph_context);
  dout(1) << "reconnect_clients -- " << client_reconnect_gather.size() << " sessions" << dendl;
  mds->sessionmap.dump();
}

/* This function DOES put the passed message before returning*/
void Server::handle_client_reconnect(MClientReconnect *m)
{
  dout(7) << "handle_client_reconnect " << m->get_source() << dendl;
  int from = m->get_source().num();
  Session *session = get_session(m);
  assert(session);

  if (!mds->is_reconnect() && mds->get_want_state() == CEPH_MDS_STATE_RECONNECT) {
    dout(10) << " we're almost in reconnect state (mdsmap delivery race?); waiting" << dendl;
    mds->wait_for_reconnect(new C_MDS_RetryMessage(mds, m));
    return;
  }

  utime_t delay = ceph_clock_now(g_ceph_context);
  delay -= reconnect_start;
  dout(10) << " reconnect_start " << reconnect_start << " delay " << delay << dendl;

  if (!mds->is_reconnect()) {
    // XXX maybe in the future we can do better than this?
    dout(1) << " no longer in reconnect state, ignoring reconnect, sending close" << dendl;
    mds->clog.info() << "denied reconnect attempt (mds is "
       << ceph_mds_state_name(mds->get_state())
       << ") from " << m->get_source_inst()
       << " after " << delay << " (allowed interval " << g_conf->mds_reconnect_timeout << ")\n";
    mds->messenger->send_message(new MClientSession(CEPH_SESSION_CLOSE), m->get_connection());
    m->put();
    return;
  }

  // notify client of success with an OPEN
  mds->messenger->send_message(new MClientSession(CEPH_SESSION_OPEN), m->get_connection());

  if (session->is_closed()) {
    dout(10) << " session is closed, will make best effort to reconnect " 
	     << m->get_source_inst() << dendl;
    mds->sessionmap.set_state(session, Session::STATE_OPENING);
    version_t pv = ++mds->sessionmap.projected;
    uint64_t sseq = session->get_state_seq();
    mdlog->start_submit_entry(new ESession(session->info.inst, true, pv),
			      new C_MDS_session_finish(mds, session, sseq, true, pv));
    mdlog->flush();
    mds->clog.debug() << "reconnect by new " << session->info.inst
	<< " after " << delay << "\n";
  } else {
    mds->clog.debug() << "reconnect by " << session->info.inst
	<< " after " << delay << "\n";
  }
  
  // snaprealms
  for (vector<ceph_mds_snaprealm_reconnect>::iterator p = m->realms.begin();
       p != m->realms.end();
       ++p) {
    CInode *in = mdcache->get_inode(inodeno_t(p->ino));
    if (in && in->state_test(CInode::STATE_PURGING))
      continue;
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
    if (in && in->state_test(CInode::STATE_PURGING))
      continue;
    if (in && in->is_auth()) {
      // we recovered it, and it's ours.  take note.
      dout(15) << "open cap realm " << inodeno_t(p->second.capinfo.snaprealm)
	       << " on " << *in << dendl;
      in->reconnect_cap(from, p->second.capinfo, session);
      mds->mdcache->add_reconnected_cap(in, from, inodeno_t(p->second.capinfo.snaprealm));
      recover_filelocks(in, p->second.flockbl, m->get_orig_source().num());
      continue;
    }
      
    if (in && !in->is_auth()) {
      // not mine.
      dout(10) << "non-auth " << *in << ", will pass off to authority" << dendl;
      // add to cap export list.
      mdcache->rejoin_export_caps(p->first, from, p->second.capinfo,
				  in->authority().first);
    } else {
      // don't know if the inode is mine
      dout(10) << "missing ino " << p->first << ", will load later" << dendl;
      mdcache->rejoin_recovered_caps(p->first, from, p->second, -1);
    }
  }

  // remove from gather set
  client_reconnect_gather.erase(from);
  if (client_reconnect_gather.empty())
    reconnect_gather_finish();

  m->put();
}



void Server::reconnect_gather_finish()
{
  dout(7) << "reconnect_gather_finish.  failed on " << failed_reconnects << " clients" << dendl;
  mds->reconnect_done();
}

void Server::reconnect_tick()
{
  utime_t reconnect_end = reconnect_start;
  reconnect_end += g_conf->mds_reconnect_timeout;
  if (ceph_clock_now(g_ceph_context) >= reconnect_end &&
      !client_reconnect_gather.empty()) {
    dout(10) << "reconnect timed out" << dendl;
    for (set<client_t>::iterator p = client_reconnect_gather.begin();
	 p != client_reconnect_gather.end();
	 ++p) {
      Session *session = mds->sessionmap.get_session(entity_name_t::CLIENT(p->v));
      dout(1) << "reconnect gave up on " << session->info.inst << dendl;
      failed_reconnects++;
    }
    client_reconnect_gather.clear();
    reconnect_gather_finish();
  }
}

void Server::recover_filelocks(CInode *in, bufferlist locks, int64_t client)
{
  if (!locks.length()) return;
  int numlocks;
  ceph_filelock lock;
  bufferlist::iterator p = locks.begin();
  ::decode(numlocks, p);
  for (int i = 0; i < numlocks; ++i) {
    ::decode(lock, p);
    lock.client = client;
    in->fcntl_locks.held_locks.insert(pair<uint64_t, ceph_filelock>
				      (lock.start, lock));
    ++in->fcntl_locks.client_held_lock_counts[client];
  }
  ::decode(numlocks, p);
  for (int i = 0; i < numlocks; ++i) {
    ::decode(lock, p);
    lock.client = client;
    in->flock_locks.held_locks.insert(pair<uint64_t, ceph_filelock>
				      (lock.start, lock));
    ++in->flock_locks.client_held_lock_counts[client];
  }
}

void Server::recall_client_state(float ratio)
{
  int max_caps_per_client = (int)(g_conf->mds_cache_size * .8);
  int min_caps_per_client = 100;

  dout(10) << "recall_client_state " << ratio
	   << ", caps per client " << min_caps_per_client << "-" << max_caps_per_client
	   << dendl;

  set<Session*> sessions;
  mds->sessionmap.get_client_session_set(sessions);
  for (set<Session*>::const_iterator p = sessions.begin();
       p != sessions.end();
       ++p) {
    Session *session = *p;
    if (!session->is_open() ||
	!session->info.inst.name.is_client())
      continue;

    dout(10) << " session " << session->info.inst
	     << " caps " << session->caps.size()
	     << ", leases " << session->leases.size()
	     << dendl;

    if (session->caps.size() > min_caps_per_client) {	
      int newlim = (int)(session->caps.size() * ratio);
      if (newlim > max_caps_per_client)
	newlim = max_caps_per_client;
      MClientSession *m = new MClientSession(CEPH_SESSION_RECALL_STATE);
      m->head.max_caps = newlim;
      mds->send_message_client(m, session);
    }
  }
 
}


/*******
 * some generic stuff for finishing off requests
 */
/* This function takes responsibility for the passed mdr*/
void Server::journal_and_reply(MDRequest *mdr, CInode *in, CDentry *dn, LogEvent *le, Context *fin)
{
  dout(10) << "journal_and_reply tracei " << in << " tracedn " << dn << dendl;

  // note trace items for eventual reply.
  mdr->tracei = in;
  if (in)
    mdr->pin(in);

  mdr->tracedn = dn;
  if (dn)
    mdr->pin(dn);

  early_reply(mdr, in, dn);
  
  mdr->committing = true;
  mdlog->submit_entry(le, fin);
  
  if (mdr->client_request && mdr->client_request->is_replay()) {
    if (mds->queue_one_replay()) {
      dout(10) << " queued next replay op" << dendl;
    } else {
      dout(10) << " journaled last replay op, flushing" << dendl;
      mdlog->flush();
    }
  } else if (mdr->did_early_reply)
    mds->locker->drop_rdlocks(mdr);
  else
    mdlog->flush();
}

/*
 * send generic response (just an error code), clean up mdr
 */
void Server::reply_request(MDRequest *mdr, int r, CInode *tracei, CDentry *tracedn)
{
  reply_request(mdr, new MClientReply(mdr->client_request, r), tracei, tracedn);
}

void Server::early_reply(MDRequest *mdr, CInode *tracei, CDentry *tracedn)
{
  if (!g_conf->mds_early_reply)
    return;

  if (mdr->are_slaves()) {
    dout(10) << "early_reply - there are slaves, not allowed." << dendl;
    mds->mdlog->flush();
    return; 
  }

  if (mdr->alloc_ino) {
    dout(10) << "early_reply - allocated ino, not allowed" << dendl;
    return;
  }

  MClientRequest *req = mdr->client_request;
  entity_inst_t client_inst = req->get_source_inst();
  if (client_inst.name.is_mds())
    return;

  if (req->is_replay()) {
    dout(10) << " no early reply on replay op" << dendl;
    mds->mdlog->flush();
    return;
  }


  MClientReply *reply = new MClientReply(mdr->client_request, 0);
  reply->set_unsafe();

  // mark xlocks "done", indicating that we are exposing uncommitted changes.
  //
  //_rename_finish() does not send dentry link/unlink message to replicas.
  // so do not set xlocks on dentries "done", the xlocks prevent dentries
  // that have projected linkages from getting new replica.
  mds->locker->set_xlocks_done(mdr, mdr->client_request->get_op() == CEPH_MDS_OP_RENAME);

  char buf[80];
  dout(10) << "early_reply " << reply->get_result() 
	   << " (" << strerror_r(-reply->get_result(), buf, sizeof(buf))
	   << ") " << *req << dendl;

  if (tracei || tracedn) {
    if (tracei)
      mdr->cap_releases.erase(tracei->vino());
    if (tracedn)
      mdr->cap_releases.erase(tracedn->get_dir()->get_inode()->vino());

    set_trace_dist(mdr->session, reply, tracei, tracedn, mdr->snapid,
		   mdr->client_request->get_dentry_wanted(),
		   mdr);
  }

  reply->set_extra_bl(mdr->reply_extra_bl);
  messenger->send_message(reply, req->get_connection());

  mdr->did_early_reply = true;

  mds->logger->inc(l_mds_reply);
  utime_t lat = ceph_clock_now(g_ceph_context) - mdr->client_request->get_recv_stamp();
  mds->logger->tinc(l_mds_replyl, lat);
  dout(20) << "lat " << lat << dendl;
}

/*
 * send given reply
 * include a trace to tracei
 * Clean up mdr
 */
void Server::reply_request(MDRequest *mdr, MClientReply *reply, CInode *tracei, CDentry *tracedn) 
{
  assert(mdr);
  MClientRequest *req = mdr->client_request;
  
  char buf[80];
  dout(10) << "reply_request " << reply->get_result() 
	   << " (" << strerror_r(-reply->get_result(), buf, sizeof(buf))
	   << ") " << *req << dendl;

  // note successful request in session map?
  if (req->may_write() && mdr->session && reply->get_result() == 0)
    mdr->session->add_completed_request(mdr->reqid.tid, mdr->alloc_ino);

  // give any preallocated inos to the session
  apply_allocated_inos(mdr);

  // get tracei/tracedn from mdr?
  snapid_t snapid = mdr->snapid;
  if (!tracei)
    tracei = mdr->tracei;
  if (!tracedn)
    tracedn = mdr->tracedn;

  bool is_replay = mdr->client_request->is_replay();
  bool did_early_reply = mdr->did_early_reply;
  Session *session = mdr->session;
  entity_inst_t client_inst = req->get_source_inst();
  int dentry_wanted = req->get_dentry_wanted();

  if (!did_early_reply && !is_replay) {

    mds->logger->inc(l_mds_reply);
    utime_t lat = ceph_clock_now(g_ceph_context) - mdr->client_request->get_recv_stamp();
    mds->logger->tinc(l_mds_replyl, lat);
    dout(20) << "lat " << lat << dendl;
    
    if (tracei)
      mdr->cap_releases.erase(tracei->vino());
    if (tracedn)
      mdr->cap_releases.erase(tracedn->get_dir()->get_inode()->vino());
  }

  // note client connection to direct my reply
  Connection *client_con = req->get_connection();
  client_con->get();

  // drop non-rdlocks before replying, so that we can issue leases
  mdcache->request_drop_non_rdlocks(mdr);

  // reply at all?
  if (client_inst.name.is_mds()) {
    reply->put();   // mds doesn't need a reply
    reply = 0;
  } else {
    // send reply.
    if (!did_early_reply &&   // don't issue leases if we sent an earlier reply already
	(tracei || tracedn)) {
      if (is_replay) {
	if (tracei)
	  mdcache->try_reconnect_cap(tracei, session);
      } else {
	// include metadata in reply
	set_trace_dist(session, reply, tracei, tracedn,
	               snapid, dentry_wanted,
		       mdr);
      }
    }

    reply->set_mdsmap_epoch(mds->mdsmap->get_epoch());
    messenger->send_message(reply, client_con);
  }
  client_con->put();
  
  // clean up request
  mdcache->request_finish(mdr);
  mdr = 0;
  req = 0;

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
			    snapid_t snapid,
			    int dentry_wanted,
			    MDRequest *mdr)
{
  // skip doing this for debugging purposes?
  if (g_conf->mds_inject_traceless_reply_probability &&
      mdr->ls && !mdr->o_trunc &&
      (rand() % 10000 < g_conf->mds_inject_traceless_reply_probability * 10000.0)) {
    dout(5) << "deliberately skipping trace for " << *reply << dendl;
    return;
  }

  // inode, dentry, dir, ..., inode
  bufferlist bl;
  int whoami = mds->get_nodeid();
  client_t client = session->get_client();
  utime_t now = ceph_clock_now(g_ceph_context);

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

    diri->encode_inodestat(bl, session, NULL, snapid);
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
  } else
    reply->head.is_dentry = 0;

  // inode
  if (in) {
    in->encode_inodestat(bl, session, NULL, snapid);
    dout(20) << "set_trace_dist added in   " << *in << dendl;
    reply->head.is_target = 1;
  } else
    reply->head.is_target = 0;

  reply->set_trace(bl);
}




/***
 * process a client request
 * This function DOES put the passed message before returning
 */
void Server::handle_client_request(MClientRequest *req)
{
  dout(4) << "handle_client_request " << *req << dendl;

  if (logger) logger->inc(l_mdss_hcreq);

  if (!mdcache->is_open()) {
    dout(5) << "waiting for root" << dendl;
    mdcache->wait_for_open(new C_MDS_RetryMessage(mds, req));
    return;
  }

  // active session?
  Session *session = 0;
  if (req->get_source().is_client()) {
    session = get_session(req);
    if (!session) {
      dout(5) << "no session for " << req->get_source() << ", dropping" << dendl;
      req->put();
      return;
    }
    if (session->is_closed() ||
	session->is_closing() ||
	session->is_killing()) {
      dout(5) << "session closed|closing|killing, dropping" << dendl;
      req->put();
      return;
    }
  }

  // old mdsmap?
  if (req->get_mdsmap_epoch() < mds->mdsmap->get_epoch()) {
    // send it?  hrm, this isn't ideal; they may get a lot of copies if
    // they have a high request rate.
  }

  // completed request?
  if (req->is_replay() ||
      (req->get_retry_attempt() &&
       req->get_op() != CEPH_MDS_OP_OPEN && 
       req->get_op() != CEPH_MDS_OP_CREATE)) {
    assert(session);
    inodeno_t created;
    if (session->have_completed_request(req->get_reqid().tid, &created)) {
      dout(5) << "already completed " << req->get_reqid() << dendl;
      MClientReply *reply = new MClientReply(req, 0);
      if (created != inodeno_t()) {
	bufferlist extra;
	::encode(created, extra);
	reply->set_extra_bl(extra);
      }
      mds->messenger->send_message(reply, req->get_connection());

      if (req->is_replay())
	mds->queue_one_replay();

      req->put();
      return;
    }
  }

  // trim completed_request list
  if (req->get_oldest_client_tid() > 0) {
    dout(15) << " oldest_client_tid=" << req->get_oldest_client_tid() << dendl;
    assert(session);
    session->trim_completed_requests(req->get_oldest_client_tid());
  }

  // request_start may drop the request, get a reference for cap release
  if (!req->releases.empty() && req->get_source().is_client() && !req->is_replay())
    req->get();

  // register + dispatch
  MDRequest *mdr = mdcache->request_start(req);
  if (mdr) {
    if (session) {
      mdr->session = session;
      session->requests.push_back(&mdr->item_session_request);
    }
  }

  // process embedded cap releases?
  //  (only if NOT replay!)
  if (!req->releases.empty() && req->get_source().is_client() && !req->is_replay()) {
    client_t client = req->get_source().num();
    for (vector<MClientRequest::Release>::iterator p = req->releases.begin();
	 p != req->releases.end();
	 ++p)
      mds->locker->process_request_cap_release(mdr, client, p->item, p->dname);
    req->put();
  }

  if (mdr)
    dispatch_client_request(mdr);
  return;
}

/* This function takes responsibility for the passed mdr*/
void Server::dispatch_client_request(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;

  if (logger) logger->inc(l_mdss_dcreq);

  dout(7) << "dispatch_client_request " << *req << dendl;

  // we shouldn't be waiting on anyone.
  assert(mdr->more()->waiting_on_slave.empty());
  
  switch (req->get_op()) {
  case CEPH_MDS_OP_LOOKUPHASH:
    handle_client_lookup_hash(mdr);
    break;

  case CEPH_MDS_OP_LOOKUPINO:
    handle_client_lookup_ino(mdr);
    break;

    // inodes ops.
  case CEPH_MDS_OP_LOOKUP:
  case CEPH_MDS_OP_LOOKUPSNAP:
    handle_client_getattr(mdr, true);
    break;

  case CEPH_MDS_OP_GETATTR:
    handle_client_getattr(mdr, false);
    break;

  case CEPH_MDS_OP_LOOKUPPARENT:
    handle_client_lookup_parent(mdr);
    break;

  case CEPH_MDS_OP_SETATTR:
    handle_client_setattr(mdr);
    break;
  case CEPH_MDS_OP_SETLAYOUT:
    handle_client_setlayout(mdr);
    break;
  case CEPH_MDS_OP_SETDIRLAYOUT:
    handle_client_setdirlayout(mdr);
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

  case CEPH_MDS_OP_SETFILELOCK:
    handle_client_file_setlock(mdr);
    break;

  case CEPH_MDS_OP_GETFILELOCK:
    handle_client_file_readlock(mdr);
    break;

    // funky.
  case CEPH_MDS_OP_CREATE:
    if (req->get_retry_attempt() &&
	mdr->session->have_completed_request(req->get_reqid().tid, NULL))
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
    reply_request(mdr, -EOPNOTSUPP);
  }
}


// ---------------------------------------
// SLAVE REQUESTS

/* This function DOES put the passed message before returning*/
void Server::handle_slave_request(MMDSSlaveRequest *m)
{
  dout(4) << "handle_slave_request " << m->get_reqid() << " from " << m->get_source() << dendl;
  int from = m->get_source().num();

  if (logger) logger->inc(l_mdss_hsreq);

  // reply?
  if (m->is_reply())
    return handle_slave_request_reply(m);

  CDentry *straydn = NULL;
  if (m->stray.length() > 0) {
    straydn = mdcache->add_replica_stray(m->stray, from);
    assert(straydn);
    m->stray.clear();
  }

  // am i a new slave?
  MDRequest *mdr = NULL;
  if (mdcache->have_request(m->get_reqid())) {
    // existing?
    mdr = mdcache->request_get(m->get_reqid());

    // is my request newer?
    if (mdr->attempt > m->get_attempt()) {
      dout(10) << "local request " << *mdr << " attempt " << mdr->attempt << " > " << m->get_attempt()
	       << ", dropping " << *m << dendl;
      m->put();
      return;
    }


    if (mdr->attempt < m->get_attempt()) {
      // mine is old, close it out
      dout(10) << "local request " << *mdr << " attempt " << mdr->attempt << " < " << m->get_attempt()
	       << ", closing out" << dendl;
      mdcache->request_finish(mdr);
      mdr = NULL;
    } else if (mdr->slave_to_mds != from) {
      dout(10) << "local request " << *mdr << " not slave to mds." << from << dendl;
      m->put();
      return;
    }
  }
  if (!mdr) {
    // new?
    if (m->get_op() == MMDSSlaveRequest::OP_FINISH) {
      dout(10) << "missing slave request for " << m->get_reqid() 
	       << " OP_FINISH, must have lost race with a forward" << dendl;
      m->put();
      return;
    }
    mdr = mdcache->request_start_slave(m->get_reqid(), m->get_attempt(), from);
  }
  assert(mdr->slave_request == 0);     // only one at a time, please!  

  if (straydn) {
    mdr->pin(straydn);
    mdr->straydn = straydn;
  }

  if (!mds->is_clientreplay() && !mds->is_active() && !mds->is_stopping()) {
    dout(3) << "not clientreplay|active yet, waiting" << dendl;
    mds->wait_for_replay(new C_MDS_RetryMessage(mds, m));
    return;
  } else if (mds->is_clientreplay() && !mds->mdsmap->is_clientreplay(from) &&
	     mdr->locks.empty()) {
    dout(3) << "not active yet, waiting" << dendl;
    mds->wait_for_active(new C_MDS_RetryMessage(mds, m));
    return;
  }

  mdr->slave_request = m;
  
  dispatch_slave_request(mdr);
}

/* This function DOES put the passed message before returning*/
void Server::handle_slave_request_reply(MMDSSlaveRequest *m)
{
  int from = m->get_source().num();
  
  if (!mds->is_clientreplay() && !mds->is_active() && !mds->is_stopping()) {
    dout(3) << "not clientreplay|active yet, waiting" << dendl;
    mds->wait_for_replay(new C_MDS_RetryMessage(mds, m));
    return;
  }

  if (m->get_op() == MMDSSlaveRequest::OP_COMMITTED) {
    metareqid_t r = m->get_reqid();
    mds->mdcache->committed_master_slave(r, from);
    m->put();
    return;
  }

  MDRequest *mdr = mdcache->request_get(m->get_reqid());
  if (!mdr) {
    dout(10) << "handle_slave_request_reply ignoring reply from unknown reqid " << m->get_reqid() << dendl;
    m->put();
    return;
  }
  if (m->get_attempt() != mdr->attempt) {
    dout(10) << "handle_slave_request_reply " << *mdr << " ignoring reply from other attempt "
	     << m->get_attempt() << dendl;
    m->put();
    return;
  }

  switch (m->get_op()) {
  case MMDSSlaveRequest::OP_XLOCKACK:
    {
      // identify lock, master request
      SimpleLock *lock = mds->locker->get_lock(m->get_lock_type(),
					       m->get_object_info());
      mdr->more()->slaves.insert(from);
      dout(10) << "got remote xlock on " << *lock << " on " << *lock->get_parent() << dendl;
      mdr->xlocks.insert(lock);
      mdr->locks.insert(lock);
      mdr->finish_locking(lock);
      lock->get_xlock(mdr, mdr->get_client());

      assert(mdr->more()->waiting_on_slave.count(from));
      mdr->more()->waiting_on_slave.erase(from);
      assert(mdr->more()->waiting_on_slave.empty());
      dispatch_client_request(mdr);
    }
    break;
    
  case MMDSSlaveRequest::OP_WRLOCKACK:
    {
      // identify lock, master request
      SimpleLock *lock = mds->locker->get_lock(m->get_lock_type(),
					       m->get_object_info());
      mdr->more()->slaves.insert(from);
      dout(10) << "got remote wrlock on " << *lock << " on " << *lock->get_parent() << dendl;
      mdr->remote_wrlocks[lock] = from;
      mdr->locks.insert(lock);
      mdr->finish_locking(lock);

      assert(mdr->more()->waiting_on_slave.count(from));
      mdr->more()->waiting_on_slave.erase(from);
      assert(mdr->more()->waiting_on_slave.empty());
      dispatch_client_request(mdr);
    }
    break;

  case MMDSSlaveRequest::OP_AUTHPINACK:
    handle_slave_auth_pin_ack(mdr, m);
    break;

  case MMDSSlaveRequest::OP_LINKPREPACK:
    handle_slave_link_prep_ack(mdr, m);
    break;

  case MMDSSlaveRequest::OP_RMDIRPREPACK:
    handle_slave_rmdir_prep_ack(mdr, m);
    break;

  case MMDSSlaveRequest::OP_RENAMEPREPACK:
    handle_slave_rename_prep_ack(mdr, m);
    break;

  default:
    assert(0);
  }
  
  // done with reply.
  m->put();
}

/* This function DOES put the mdr->slave_request before returning*/
void Server::dispatch_slave_request(MDRequest *mdr)
{
  dout(7) << "dispatch_slave_request " << *mdr << " " << *mdr->slave_request << dendl;

  if (mdr->aborted) {
    dout(7) << " abort flag set, finishing" << dendl;
    mdcache->request_finish(mdr);
    return;
  }

  if (logger) logger->inc(l_mdss_dsreq);

  int op = mdr->slave_request->get_op();
  switch (op) {
  case MMDSSlaveRequest::OP_XLOCK:
  case MMDSSlaveRequest::OP_WRLOCK:
    {
      // identify object
      SimpleLock *lock = mds->locker->get_lock(mdr->slave_request->get_lock_type(),
					       mdr->slave_request->get_object_info());

      if (!lock) {
	dout(10) << "don't have object, dropping" << dendl;
	assert(0); // can this happen, if we auth pinned properly.
      }
      if (op == MMDSSlaveRequest::OP_XLOCK && !lock->get_parent()->is_auth()) {
	dout(10) << "not auth for remote xlock attempt, dropping on " 
		 << *lock << " on " << *lock->get_parent() << dendl;
      } else {
	// use acquire_locks so that we get auth_pinning.
	set<SimpleLock*> rdlocks;
	set<SimpleLock*> wrlocks = mdr->wrlocks;
	set<SimpleLock*> xlocks = mdr->xlocks;

	int replycode = 0;
	switch (op) {
	case MMDSSlaveRequest::OP_XLOCK:
	  xlocks.insert(lock);
	  replycode = MMDSSlaveRequest::OP_XLOCKACK;
	  break;
	case MMDSSlaveRequest::OP_WRLOCK:
	  wrlocks.insert(lock);
	  replycode = MMDSSlaveRequest::OP_WRLOCKACK;
	  break;
	}
	
	if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
	  return;
	
	// ack
	MMDSSlaveRequest *r = new MMDSSlaveRequest(mdr->reqid, mdr->attempt, replycode);
	r->set_lock_type(lock->get_type());
	lock->get_parent()->set_object_info(r->get_object_info());
	mds->send_message(r, mdr->slave_request->get_connection());
      }

      // done.
      mdr->slave_request->put();
      mdr->slave_request = 0;
    }
    break;

  case MMDSSlaveRequest::OP_UNXLOCK:
  case MMDSSlaveRequest::OP_UNWRLOCK:
    {  
      SimpleLock *lock = mds->locker->get_lock(mdr->slave_request->get_lock_type(),
					       mdr->slave_request->get_object_info());
      assert(lock);
      bool need_issue = false;
      switch (op) {
      case MMDSSlaveRequest::OP_UNXLOCK:
	mds->locker->xlock_finish(lock, mdr, &need_issue);
	break;
      case MMDSSlaveRequest::OP_UNWRLOCK:
	mds->locker->wrlock_finish(lock, mdr, &need_issue);
	break;
      }
      if (need_issue)
	mds->locker->issue_caps(static_cast<CInode*>(lock->get_parent()));

      // done.  no ack necessary.
      mdr->slave_request->put();
      mdr->slave_request = 0;
    }
    break;

  case MMDSSlaveRequest::OP_DROPLOCKS:
    mds->locker->drop_locks(mdr);
    mdr->slave_request->put();
    mdr->slave_request = 0;
    break;

  case MMDSSlaveRequest::OP_AUTHPIN:
    handle_slave_auth_pin(mdr);
    break;

  case MMDSSlaveRequest::OP_LINKPREP:
  case MMDSSlaveRequest::OP_UNLINKPREP:
    handle_slave_link_prep(mdr);
    break;

  case MMDSSlaveRequest::OP_RMDIRPREP:
    handle_slave_rmdir_prep(mdr);
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

/* This function DOES put the mdr->slave_request before returning*/
void Server::handle_slave_auth_pin(MDRequest *mdr)
{
  dout(10) << "handle_slave_auth_pin " << *mdr << dendl;

  // build list of objects
  list<MDSCacheObject*> objects;
  CInode *auth_pin_freeze = NULL;
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
    if (*p == mdr->slave_request->get_authpin_freeze())
      auth_pin_freeze = static_cast<CInode*>(object);
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
      if (!mdr->can_auth_pin(*p)) {
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
    /* handle_slave_rename_prep() call freeze_inode() to wait for all other operations
     * on the source inode to complete. This happens after all locks for the rename
     * operation are acquired. But to acquire locks, we need auth pin locks' parent
     * objects first. So there is an ABBA deadlock if someone auth pins the source inode
     * after locks are acquired and before Server::handle_slave_rename_prep() is called.
     * The solution is freeze the inode and prevent other MDRequests from getting new
     * auth pins.
     */
    if (auth_pin_freeze) {
      dout(10) << " freezing auth pin on " << *auth_pin_freeze << dendl;
      if (!mdr->freeze_auth_pin(auth_pin_freeze)) {
	auth_pin_freeze->add_waiter(CInode::WAIT_FROZEN, new C_MDS_RetryRequest(mdcache, mdr));
	mds->mdlog->flush();
	return;
      }
    }
    for (list<MDSCacheObject*>::iterator p = objects.begin();
	 p != objects.end();
	 ++p) {
      dout(10) << "auth_pinning " << **p << dendl;
      mdr->auth_pin(*p);
    }
  }

  // ack!
  MMDSSlaveRequest *reply = new MMDSSlaveRequest(mdr->reqid, mdr->attempt, MMDSSlaveRequest::OP_AUTHPINACK);
  
  // return list of my auth_pins (if any)
  for (set<MDSCacheObject*>::iterator p = mdr->auth_pins.begin();
       p != mdr->auth_pins.end();
       ++p) {
    MDSCacheObjectInfo info;
    (*p)->set_object_info(info);
    reply->get_authpins().push_back(info);
  }

  if (auth_pin_freeze)
    auth_pin_freeze->set_object_info(reply->get_authpin_freeze());

  mds->send_message_mds(reply, mdr->slave_to_mds);
  
  // clean up this request
  mdr->slave_request->put();
  mdr->slave_request = 0;
  return;
}

/* This function DOES NOT put the passed ack before returning*/
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
    if (*p == ack->get_authpin_freeze())
      mdr->set_remote_frozen_auth_pin(static_cast<CInode *>(object));
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
    return NULL;
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
    return NULL;
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
  
  client_t client = mdr->get_client();

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
  if (!dir->is_complete() && (!dir->has_bloom() || dir->is_in_bloom(dname))) {
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

CDentry* Server::prepare_stray_dentry(MDRequest *mdr, CInode *in)
{
  CDentry *straydn = mdr->straydn;
  if (straydn) {
    string name;
    in->name_stray_dentry(name);
    if (straydn->get_name() == name)
      return straydn;

    assert(!mdr->done_locking);
    mdr->unpin(straydn);
  }

  straydn = mdcache->get_or_create_stray_dentry(in);
  mdr->straydn = straydn;
  mdr->pin(straydn);
  return straydn;
}

/** prepare_new_inode
 *
 * create a new inode.  set c/m/atime.  hit dir pop.
 */
CInode* Server::prepare_new_inode(MDRequest *mdr, CDir *dir, inodeno_t useino, unsigned mode,
				  ceph_file_layout *layout) 
{
  CInode *in = new CInode(mdcache);
  
  // assign ino
  if (mdr->session->info.prealloc_inos.size()) {
    mdr->used_prealloc_ino = 
      in->inode.ino = mdr->session->take_ino(useino);  // prealloc -> used
    mds->sessionmap.projected++;
    dout(10) << "prepare_new_inode used_prealloc " << mdr->used_prealloc_ino
	     << " (" << mdr->session->info.prealloc_inos
	     << ", " << mdr->session->info.prealloc_inos.size() << " left)"
	     << dendl;
  } else {
    mdr->alloc_ino = 
      in->inode.ino = mds->inotable->project_alloc_id();
    dout(10) << "prepare_new_inode alloc " << mdr->alloc_ino << dendl;
  }

  if (useino && useino != in->inode.ino) {
    dout(0) << "WARNING: client specified " << useino << " and i allocated " << in->inode.ino << dendl;
    mds->clog.error() << mdr->client_request->get_source()
       << " specified ino " << useino
       << " but mds." << mds->whoami << " allocated " << in->inode.ino << "\n";
    //assert(0); // just for now.
  }
    
  int got = g_conf->mds_client_prealloc_inos - mdr->session->get_num_projected_prealloc_inos();
  if (got > 0) {
    mds->inotable->project_alloc_ids(mdr->prealloc_inos, got);
    assert(mdr->prealloc_inos.size());  // or else fix projected increment semantics
    mdr->session->pending_prealloc_inos.insert(mdr->prealloc_inos);
    mds->sessionmap.projected++;
    dout(10) << "prepare_new_inode prealloc " << mdr->prealloc_inos << dendl;
  }

  in->inode.version = 1;
  in->inode.nlink = 1;   // FIXME

  in->inode.mode = mode;

  memset(&in->inode.dir_layout, 0, sizeof(in->inode.dir_layout));
  if (in->inode.is_dir())
    in->inode.dir_layout.dl_dir_hash = g_conf->mds_default_dir_hash;

  if (layout)
    in->inode.layout = *layout;
  else if (in->inode.is_dir())
    memset(&in->inode.layout, 0, sizeof(in->inode.layout));
  else
    in->inode.layout = mds->mdcache->default_file_layout;

  in->inode.truncate_size = -1ull;  // not truncated, yet!
  in->inode.truncate_seq = 1; /* starting with 1, 0 is kept for no-truncation logic */

  CInode *diri = dir->get_inode();

  dout(10) << oct << " dir mode 0" << diri->inode.mode << " new mode 0" << mode << dec << dendl;

  if (diri->inode.mode & S_ISGID) {
    dout(10) << " dir is sticky" << dendl;
    in->inode.gid = diri->inode.gid;
    if (S_ISDIR(mode)) {
      dout(10) << " new dir also sticky" << dendl;      
      in->inode.mode |= S_ISGID;
    }
  } else 
    in->inode.gid = mdr->client_request->get_caller_gid();

  in->inode.uid = mdr->client_request->get_caller_uid();

  in->inode.ctime = in->inode.mtime = in->inode.atime = mdr->now;   // now

  MClientRequest *req = mdr->client_request;
  if (req->get_data().length()) {
    bufferlist::iterator p = req->get_data().begin();

    // xattrs on new inode?
    map<string,bufferptr> xattrs;
    ::decode(xattrs, p);
    for (map<string,bufferptr>::iterator p = xattrs.begin(); p != xattrs.end(); ++p) {
      dout(10) << "prepare_new_inode setting xattr " << p->first << dendl;
      in->xattrs[p->first] = p->second;
    }
  }

  mdcache->add_inode(in);  // add
  dout(10) << "prepare_new_inode " << *in << dendl;
  return in;
}

void Server::journal_allocated_inos(MDRequest *mdr, EMetaBlob *blob)
{
  dout(20) << "journal_allocated_inos sessionmapv " << mds->sessionmap.projected
	   << " inotablev " << mds->inotable->get_projected_version()
	   << dendl;
  blob->set_ino_alloc(mdr->alloc_ino,
		      mdr->used_prealloc_ino,
		      mdr->prealloc_inos,
		      mdr->client_request->get_source(),
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
    assert(session);
    session->pending_prealloc_inos.subtract(mdr->prealloc_inos);
    session->info.prealloc_inos.insert(mdr->prealloc_inos);
    mds->sessionmap.version++;
    mds->inotable->apply_alloc_ids(mdr->prealloc_inos);
  }
  if (mdr->used_prealloc_ino) {
    assert(session);
    session->info.used_inos.erase(mdr->used_prealloc_ino);
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
  CInode *diri;
  int r = mdcache->path_traverse(mdr, NULL, NULL, refpath, &trace, &diri, MDS_TRAVERSE_FORWARD);
  if (r > 0) return 0; // delayed
  if (r < 0) {
    reply_request(mdr, r);
    return 0;
  }

  // is it an auth dir?
  CDir *dir = validate_dentry_dir(mdr, diri, dname);
  if (!dir)
    return 0; // forwarded or waiting for freeze

  dout(10) << "traverse_to_auth_dir " << *dir << dendl;
  return dir;
}

class C_MDS_TryFindInode : public Context {
  Server *server;
  MDRequest *mdr;
public:
  C_MDS_TryFindInode(Server *s, MDRequest *r) : server(s), mdr(r) {
    mdr->get();
  }
  virtual void finish(int r) {
    if (r == -ESTALE) // :( find_ino_peers failed
      server->reply_request(mdr, r);
    else
      server->dispatch_client_request(mdr);
    mdr->put();
  }
};

/* If this returns null, the request has been handled
 * as appropriate: forwarded on, or the client's been replied to */
CInode* Server::rdlock_path_pin_ref(MDRequest *mdr, int n,
				    set<SimpleLock*> &rdlocks,
				    bool want_auth,
				    bool no_want_auth, /* for readdir, who doesn't want auth _even_if_ it's
							  a snapped dir */
				    ceph_file_layout **layout,
				    bool no_lookup)    // true if we cannot return a null dentry lease
{
  MClientRequest *req = mdr->client_request;
  const filepath& refpath = n ? req->get_filepath2() : req->get_filepath();
  dout(10) << "rdlock_path_pin_ref " << *mdr << " " << refpath << dendl;

  if (mdr->done_locking)
    return mdr->in[n];

  // traverse
  int r = mdcache->path_traverse(mdr, NULL, NULL, refpath, &mdr->dn[n], &mdr->in[n], MDS_TRAVERSE_FORWARD);
  if (r > 0)
    return NULL; // delayed
  if (r < 0) {  // error
    if (r == -ENOENT && n == 0 && mdr->dn[n].size()) {
      reply_request(mdr, r, NULL, no_lookup ? NULL : mdr->dn[n][mdr->dn[n].size()-1]);
    } else if (r == -ESTALE) {
      dout(10) << "FAIL on ESTALE but attempting recovery" << dendl;
      Context *c = new C_MDS_TryFindInode(this, mdr);
      mdcache->find_ino_peers(refpath.get_ino(), c);
    } else {
      dout(10) << "FAIL on error " << r << dendl;
      reply_request(mdr, r);
    }
    return 0;
  }
  CInode *ref = mdr->in[n];
  dout(10) << "ref is " << *ref << dendl;

  // fw to inode auth?
  if (mdr->snapid != CEPH_NOSNAP && !no_want_auth)
    want_auth = true;

  if (want_auth) {
    if (ref->is_ambiguous_auth()) {
      dout(10) << "waiting for single auth on " << *ref << dendl;
      ref->add_waiter(CInode::WAIT_SINGLEAUTH, new C_MDS_RetryRequest(mdcache, mdr));
      return 0;
    }
    if (!ref->is_auth()) {
      dout(10) << "fw to auth for " << *ref << dendl;
      mdcache->request_forward(mdr, ref->authority().first);
      return 0;
    }

    // auth_pin?
    //   do NOT proceed if freezing, as cap release may defer in that case, and
    //   we could deadlock when we try to lock @ref.
    // if we're already auth_pinned, continue; the release has already been processed.
    if (ref->is_frozen() || ref->is_frozen_auth_pin() ||
	(ref->is_freezing() && !mdr->is_auth_pinned(ref))) {
      dout(7) << "waiting for !frozen/authpinnable on " << *ref << dendl;
      ref->add_waiter(CInode::WAIT_UNFREEZE, new C_MDS_RetryRequest(mdcache, mdr));
      /* If we have any auth pins, this will deadlock.
       * But the only way to get here if we've already got auth pins
       * is because we're on an inode with snapshots that got updated
       * between dispatches of this request. So we're going to drop
       * our locks and our auth pins and reacquire them later.
       *
       * This is safe since we're only in this function when working on
       * a single MDS request; otherwise we'd be in
       * rdlock_path_xlock_dentry.
       */
      mds->locker->drop_locks(mdr, NULL);
      mdr->drop_local_auth_pins();
      return 0;
    }

    mdr->auth_pin(ref);
  }

  for (int i=0; i<(int)mdr->dn[n].size(); i++) 
    rdlocks.insert(&mdr->dn[n][i]->lock);
  if (layout)
    mds->locker->include_snap_rdlocks_wlayout(rdlocks, ref, layout);
  else
    mds->locker->include_snap_rdlocks(rdlocks, ref);

  // set and pin ref
  mdr->pin(ref);
  return ref;
}


/** rdlock_path_xlock_dentry
 * traverse path to the directory that could/would contain dentry.
 * make sure i am auth for that dentry, forward as necessary.
 * create null dentry in place (or use existing if okexist).
 * get rdlocks on traversed dentries, xlock on new dentry.
 */
CDentry* Server::rdlock_path_xlock_dentry(MDRequest *mdr, int n,
					  set<SimpleLock*>& rdlocks, set<SimpleLock*>& wrlocks, set<SimpleLock*>& xlocks,
					  bool okexist, bool mustexist, bool alwaysxlock,
					  ceph_file_layout **layout)
{
  MClientRequest *req = mdr->client_request;
  const filepath& refpath = n ? req->get_filepath2() : req->get_filepath();

  dout(10) << "rdlock_path_xlock_dentry " << *mdr << " " << refpath << dendl;

  client_t client = mdr->get_client();

  if (mdr->done_locking)
    return mdr->dn[n].back();

  CDir *dir = traverse_to_auth_dir(mdr, mdr->dn[n], refpath);
  if (!dir) return 0;
  dout(10) << "rdlock_path_xlock_dentry dir " << *dir << dendl;

  // make sure we can auth_pin (or have already authpinned) dir
  if (dir->is_frozen()) {
    dout(7) << "waiting for !frozen/authpinnable on " << *dir << dendl;
    dir->add_waiter(CInode::WAIT_UNFREEZE, new C_MDS_RetryRequest(mdcache, mdr));
    return 0;
  }

  CInode *diri = dir->get_inode();
  if (!mdr->reqid.name.is_mds()) {
    if (diri->is_system() && !diri->is_root()) {
      reply_request(mdr, -EROFS);
      return 0;
    }
    if (!diri->is_base() && diri->get_projected_parent_dir()->inode->is_stray()) {
      reply_request(mdr, -ENOENT);
      return 0;
    }
  }

  // make a null dentry?
  const string &dname = refpath.last_dentry();
  CDentry *dn;
  if (mustexist) {
    dn = dir->lookup(dname);

    // make sure dir is complete
    if (!dn && !dir->is_complete() &&
        (!dir->has_bloom() || dir->is_in_bloom(dname))) {
      dout(7) << " incomplete dir contents for " << *dir << ", fetching" << dendl;
      dir->fetch(new C_MDS_RetryRequest(mdcache, mdr));
      return 0;
    }

    // readable?
    if (dn && !dn->lock.can_read(client) && dn->lock.get_xlock_by() != mdr) {
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

  mdr->dn[n].push_back(dn);
  mdr->in[n] = dn->get_projected_linkage()->get_inode();

  // -- lock --
  // NOTE: rename takes the same set of locks for srcdn
  for (int i=0; i<(int)mdr->dn[n].size(); i++) 
    rdlocks.insert(&mdr->dn[n][i]->lock);
  if (alwaysxlock || dn->get_linkage(client, mdr)->is_null())
    xlocks.insert(&dn->lock);                 // new dn, xlock
  else
    rdlocks.insert(&dn->lock);  // existing dn, rdlock
  wrlocks.insert(&dn->get_dir()->inode->filelock); // also, wrlock on dir mtime
  wrlocks.insert(&dn->get_dir()->inode->nestlock); // also, wrlock on dir mtime
  if (layout)
    mds->locker->include_snap_rdlocks_wlayout(rdlocks, dn->get_dir()->inode, layout);
  else
    mds->locker->include_snap_rdlocks(rdlocks, dn->get_dir()->inode);

  return dn;
}





/**
 * try_open_auth_dirfrag -- open dirfrag, or forward to dirfrag auth
 *
 * @param diri base inode
 * @param fg the exact frag we want
 * @param mdr request
 * @returns the pointer, or NULL if it had to be delayed (but mdr is taken care of)
 */
CDir* Server::try_open_auth_dirfrag(CInode *diri, frag_t fg, MDRequest *mdr)
{
  CDir *dir = diri->get_dirfrag(fg);

  // not open and inode not mine?
  if (!dir && !diri->is_auth()) {
    int inauth = diri->authority().first;
    dout(7) << "try_open_auth_dirfrag: not open, not inode auth, fw to mds." << inauth << dendl;
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
	    << ", fw to mds." << auth << dendl;
    mdcache->request_forward(mdr, auth);
    return 0;
  }

  return dir;
}


// ===============================================================================
// STAT

void Server::handle_client_getattr(MDRequest *mdr, bool is_lookup)
{
  MClientRequest *req = mdr->client_request;
  set<SimpleLock*> rdlocks, wrlocks, xlocks;

  if (req->get_filepath().depth() == 0 && is_lookup) {
    // refpath can't be empty for lookup but it can for
    // getattr (we do getattr with empty refpath for mount of '/')
    reply_request(mdr, -EINVAL);
    return;
  }

  CInode *ref = rdlock_path_pin_ref(mdr, 0, rdlocks, false, false, NULL, !is_lookup);
  if (!ref) return;

  /*
   * if client currently holds the EXCL cap on a field, do not rdlock
   * it; client's stat() will result in valid info if _either_ EXCL
   * cap is held or MDS rdlocks and reads the value here.
   *
   * handling this case here is easier than weakening rdlock
   * semantics... that would cause problems elsewhere.
   */
  client_t client = mdr->get_client();
  int issued = 0;
  Capability *cap = ref->get_client_cap(client);
  if (cap && (mdr->snapid == CEPH_NOSNAP ||
	      mdr->snapid <= cap->client_follows))
    issued = cap->issued();

  int mask = req->head.args.getattr.mask;
  if ((mask & CEPH_CAP_LINK_SHARED) && (issued & CEPH_CAP_LINK_EXCL) == 0) rdlocks.insert(&ref->linklock);
  if ((mask & CEPH_CAP_AUTH_SHARED) && (issued & CEPH_CAP_AUTH_EXCL) == 0) rdlocks.insert(&ref->authlock);
  if ((mask & CEPH_CAP_FILE_SHARED) && (issued & CEPH_CAP_FILE_EXCL) == 0) rdlocks.insert(&ref->filelock);
  if ((mask & CEPH_CAP_XATTR_SHARED) && (issued & CEPH_CAP_XATTR_EXCL) == 0) rdlocks.insert(&ref->xattrlock);

  if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
    return;

  mds->balancer->hit_inode(ceph_clock_now(g_ceph_context), ref, META_POP_IRD,
			   mdr->client_request->get_source().num());

  // reply
  dout(10) << "reply to stat on " << *req << dendl;
  reply_request(mdr, 0, ref,
		is_lookup ? mdr->dn[0].back() : 0);
}

/* This function will clean up the passed mdr*/
void Server::handle_client_lookup_parent(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;

  CInode *in = mdcache->get_inode(req->get_filepath().get_ino());
  if (!in) {
    reply_request(mdr, -ESTALE);
    return;
  }
  if (in->is_base()) {
    reply_request(mdr, -EINVAL);
    return;
  }

  CDentry *dn = in->get_projected_parent_dn();

  set<SimpleLock*> rdlocks, wrlocks, xlocks;
  rdlocks.insert(&dn->lock);
  if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
    return;

  reply_request(mdr, 0, in, dn);  // reply
}

struct C_MDS_LookupHash2 : public Context {
  Server *server;
  MDRequest *mdr;
  C_MDS_LookupHash2(Server *s, MDRequest *r) : server(s), mdr(r) {}
  void finish(int r) {
    server->_lookup_hash_2(mdr, r);
  }
};

/* This function DOES clean up the mdr before returning*/
/*
 * filepath:  ino
 * filepath2: dirino/<hash as base-10 %d>
 *
 * This dirino+hash is optional.
 */
void Server::handle_client_lookup_hash(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;

  inodeno_t ino = req->get_filepath().get_ino();
  inodeno_t dirino = req->get_filepath2().get_ino();

  CInode *in = 0;

  if (ino) {
    in = mdcache->get_inode(ino);
    if (in && in->state_test(CInode::STATE_PURGING)) {
      reply_request(mdr, -ESTALE);
      return;
    }
    if (!in && !dirino) {
      dout(10) << " no dirino, looking up ino " << ino << " directly" << dendl;
      _lookup_ino(mdr);
      return;
    }
  }
  if (!in) {
    // try the directory
    CInode *diri = mdcache->get_inode(dirino);
    if (!diri) {
      mdcache->find_ino_peers(dirino,
			      new C_MDS_LookupHash2(this, mdr), -1);
      return;
    }
    if (diri->state_test(CInode::STATE_PURGING)) {
      reply_request(mdr, -ESTALE);
      return;
    }
    dout(10) << " have diri " << *diri << dendl;
    unsigned hash = atoi(req->get_filepath2()[0].c_str());
    frag_t fg = diri->dirfragtree[hash];
    dout(10) << " fg is " << fg << dendl;
    CDir *dir = diri->get_dirfrag(fg);
    if (!dir) {
      if (!diri->is_auth()) {
	if (diri->is_ambiguous_auth()) {
	  // wait
	  dout(7) << " waiting for single auth in " << *diri << dendl;
	  diri->add_waiter(CInode::WAIT_SINGLEAUTH, new C_MDS_RetryRequest(mdcache, mdr));
	  return;
	} 
	mdcache->request_forward(mdr, diri->authority().first);
	return;
      }
      dir = diri->get_or_open_dirfrag(mdcache, fg);
    }
    assert(dir);
    dout(10) << " have dir " << *dir << dendl;
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
      dir->fetch(new C_MDS_RetryRequest(mdcache, mdr));
      return;
    }
    reply_request(mdr, -ESTALE);
    return;
  }

  dout(10) << "reply to lookup_hash on " << *in << dendl;
  MClientReply *reply = new MClientReply(req, 0);
  reply_request(mdr, reply, in, in->get_parent_dn());
}

struct C_MDS_LookupHash3 : public Context {
  Server *server;
  MDRequest *mdr;
  C_MDS_LookupHash3(Server *s, MDRequest *r) : server(s), mdr(r) {}
  void finish(int r) {
    server->_lookup_hash_3(mdr, r);
  }
};

void Server::_lookup_hash_2(MDRequest *mdr, int r)
{
  inodeno_t dirino = mdr->client_request->get_filepath2().get_ino();
  dout(10) << "_lookup_hash_2 " << mdr << " checked peers for dirino " << dirino << " and got r=" << r << dendl;
  if (r == 0) {
    dispatch_client_request(mdr);
    return;
  }

  // okay fine, try the dir object then!
  mdcache->find_ino_dir(dirino, new C_MDS_LookupHash3(this, mdr));
}

void Server::_lookup_hash_3(MDRequest *mdr, int r)
{
  inodeno_t dirino = mdr->client_request->get_filepath2().get_ino();
  dout(10) << "_lookup_hash_3 " << mdr << " checked dir object for dirino " << dirino
	   << " and got r=" << r << dendl;
  if (r == 0) {
    dispatch_client_request(mdr);
    return;
  }
  dout(10) << "_lookup_hash_3 " << mdr << " trying the ino itself" << dendl;
  _lookup_ino(mdr);
}

/***************/

struct C_MDS_LookupIno2 : public Context {
  Server *server;
  MDRequest *mdr;
  C_MDS_LookupIno2(Server *s, MDRequest *r) : server(s), mdr(r) {}
  void finish(int r) {
    server->_lookup_ino_2(mdr, r);
  }
};

/* This function DOES clean up the mdr before returning*/
/*
 * filepath:  ino
 */
void Server::handle_client_lookup_ino(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;

  inodeno_t ino = req->get_filepath().get_ino();
  CInode *in = mdcache->get_inode(ino);
  if (in && in->state_test(CInode::STATE_PURGING)) {
    reply_request(mdr, -ESTALE);
    return;
  }
  if (!in) {
    _lookup_ino(mdr);
    return;
  }

  dout(10) << "reply to lookup_ino " << *in << dendl;
  MClientReply *reply = new MClientReply(req, 0);
  reply_request(mdr, reply, in, in->get_parent_dn());
}

void Server::_lookup_ino(MDRequest *mdr)
{
  inodeno_t ino = mdr->client_request->get_filepath().get_ino();
  dout(10) << "_lookup_ino " << mdr << " checking peers for ino " << ino << dendl;
  mdcache->find_ino_peers(ino,
			  new C_MDS_LookupIno2(this, mdr), -1);
}

struct C_MDS_LookupIno3 : public Context {
  Server *server;
  MDRequest *mdr;
  C_MDS_LookupIno3(Server *s, MDRequest *r) : server(s), mdr(r) {}
  void finish(int r) {
    server->_lookup_ino_3(mdr, r);
  }
};

void Server::_lookup_ino_2(MDRequest *mdr, int r)
{
  inodeno_t ino = mdr->client_request->get_filepath().get_ino();
  dout(10) << "_lookup_ino_2 " << mdr << " checked peers for ino " << ino
	   << " and got r=" << r << dendl;
  if (r == 0) {
    dispatch_client_request(mdr);
    return;
  }

  // okay fine, maybe it's a directory though...
  mdcache->find_ino_dir(ino, new C_MDS_LookupIno3(this, mdr));
}

void Server::_lookup_ino_3(MDRequest *mdr, int r)
{
  inodeno_t ino = mdr->client_request->get_filepath().get_ino();
  dout(10) << "_lookup_ino_3 " << mdr << " checked dir obj for ino " << ino
	   << " and got r=" << r << dendl;
  if (r == 0) {
    dispatch_client_request(mdr);
    return;
  }

  // give up
  if (r == -ENOENT || r == -ENODATA)
    r = -ESTALE;
  reply_request(mdr, r);
}


/* This function takes responsibility for the passed mdr*/
void Server::handle_client_open(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;

  int flags = req->head.args.open.flags;
  int cmode = ceph_flags_to_mode(req->head.args.open.flags);

  bool need_auth = !file_mode_is_readonly(cmode) || (flags & O_TRUNC);

  dout(7) << "open on " << req->get_filepath() << dendl;

  if (cmode < 0) {
    reply_request(mdr, -EINVAL);
    return;
  }
  
  set<SimpleLock*> rdlocks, wrlocks, xlocks;
  CInode *cur = rdlock_path_pin_ref(mdr, 0, rdlocks, need_auth);
  if (!cur)
    return;

  if (cur->is_frozen() || cur->state_test(CInode::STATE_EXPORTINGCAPS)) {
    assert(!need_auth);
    mdr->done_locking = false;
    CInode *cur = rdlock_path_pin_ref(mdr, 0, rdlocks, true);
    if (!cur)
      return;
  }

  if (mdr->snapid != CEPH_NOSNAP && mdr->client_request->may_write()) {
    reply_request(mdr, -EROFS);
    return;
  }

  // can only open a dir with mode FILE_MODE_PIN, at least for now.
  if (cur->inode.is_dir())
    cmode = CEPH_FILE_MODE_PIN;

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
  if (mdr->snapid != CEPH_NOSNAP &&
      (cmode & CEPH_FILE_MODE_WR)) {
    dout(7) << "snap " << mdr->snapid << " is read-only " << *cur << dendl;
    reply_request(mdr, -EPERM);
    return;
  }

  // O_TRUNC
  if ((flags & O_TRUNC) &&
      !(req->get_retry_attempt() &&
	mdr->session->have_completed_request(req->get_reqid().tid, NULL))) {
    assert(cur->is_auth());

    wrlocks.insert(&cur->filelock);
    if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
      return;

    // wait for pending truncate?
    inode_t *pi = cur->get_projected_inode();
    if (pi->is_truncating()) {
      dout(10) << " waiting for pending truncate from " << pi->truncate_from
	       << " to " << pi->truncate_size << " to complete on " << *cur << dendl;
      cur->add_waiter(CInode::WAIT_TRUNC, new C_MDS_RetryRequest(mdcache, mdr));
      return;
    }
    
    do_open_truncate(mdr, cmode);
    return;
  }

  // sync filelock if snapped.
  //  this makes us wait for writers to flushsnaps, ensuring we get accurate metadata,
  //  and that data itself is flushed so that we can read the snapped data off disk.
  if (mdr->snapid != CEPH_NOSNAP && !cur->is_dir()) {
    rdlocks.insert(&cur->filelock);
    if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
      return;
  }

  if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
    return;

  if (cur->is_file() || cur->is_dir()) {
    if (mdr->snapid == CEPH_NOSNAP) {
      // register new cap
      Capability *cap = mds->locker->issue_new_caps(cur, cmode, mdr->session, 0, req->is_replay());
      if (cap)
	dout(12) << "open issued caps " << ccap_string(cap->pending())
		 << " for " << req->get_source()
		 << " on " << *cur << dendl;
    } else {
      int caps = ceph_caps_for_mode(cmode);
      dout(12) << "open issued IMMUTABLE SNAP caps " << ccap_string(caps)
	       << " for " << req->get_source()
	       << " snapid " << mdr->snapid
	       << " on " << *cur << dendl;
      mdr->snap_caps = caps;
    }
  }

  // increase max_size?
  if (cmode & CEPH_FILE_MODE_WR)
    mds->locker->check_inode_max_size(cur);

  // make sure this inode gets into the journal
  if (cur->is_auth() && cur->last == CEPH_NOSNAP &&
      !cur->item_open_file.is_on_list()) {
    LogSegment *ls = mds->mdlog->get_current_segment();
    EOpen *le = new EOpen(mds->mdlog);
    mdlog->start_entry(le);
    le->add_clean_inode(cur);
    ls->open_files.push_back(&cur->item_open_file);
    mds->mdlog->submit_entry(le);
  }
  
  // hit pop
  mdr->now = ceph_clock_now(g_ceph_context);
  if (cmode == CEPH_FILE_MODE_RDWR ||
      cmode == CEPH_FILE_MODE_WR) 
    mds->balancer->hit_inode(mdr->now, cur, META_POP_IWR);
  else
    mds->balancer->hit_inode(mdr->now, cur, META_POP_IRD, 
			     mdr->client_request->get_source().num());

  CDentry *dn = 0;
  if (req->get_dentry_wanted()) {
    assert(mdr->dn[0].size());
    dn = mdr->dn[0].back();
  }

  reply_request(mdr, 0, cur, dn);
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
    newi->_mark_dirty_parent(mdr->ls);

    mdr->apply();

    mds->locker->share_inode_max_size(newi);

    mds->mdcache->send_dentry_link(dn);

    mds->balancer->hit_inode(mdr->now, newi, META_POP_IWR);

    MClientReply *reply = new MClientReply(mdr->client_request, 0);
    reply->set_extra_bl(mdr->reply_extra_bl);
    mds->server->reply_request(mdr, reply);

    assert(g_conf->mds_kill_openc_at != 1);
  }
};

/* This function takes responsibility for the passed mdr*/
void Server::handle_client_openc(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;
  client_t client = mdr->get_client();

  dout(7) << "open w/ O_CREAT on " << req->get_filepath() << dendl;

  int cmode = ceph_flags_to_mode(req->head.args.open.flags);
  if (cmode < 0) {
    reply_request(mdr, -EINVAL);
    return;
  }

  if (!(req->head.args.open.flags & O_EXCL)) {
    int r = mdcache->path_traverse(mdr, NULL, NULL, req->get_filepath(),
				   &mdr->dn[0], NULL, MDS_TRAVERSE_FORWARD);
    if (r > 0) return;
    if (r == 0) {
      // it existed.
      handle_client_open(mdr);
      return;
    }
    if (r < 0 && r != -ENOENT) {
      if (r == -ESTALE) {
	dout(10) << "FAIL on ESTALE but attempting recovery" << dendl;
	Context *c = new C_MDS_TryFindInode(this, mdr);
	mdcache->find_ino_peers(req->get_filepath().get_ino(), c);
      } else {
	dout(10) << "FAIL on error " << r << dendl;
	reply_request(mdr, r);
      }
      return;
    }
    // r == -ENOENT
  }

  bool excl = (req->head.args.open.flags & O_EXCL);
  set<SimpleLock*> rdlocks, wrlocks, xlocks;
  ceph_file_layout *dir_layout = NULL;
  CDentry *dn = rdlock_path_xlock_dentry(mdr, 0, rdlocks, wrlocks, xlocks,
                                         !excl, false, false, &dir_layout);
  if (!dn) return;
  if (mdr->snapid != CEPH_NOSNAP) {
    reply_request(mdr, -EROFS);
    return;
  }
  // set layout
  ceph_file_layout layout;
  if (dir_layout)
    layout = *dir_layout;
  else
    layout = mds->mdcache->default_file_layout;

  // fill in any special params from client
  if (req->head.args.open.stripe_unit)
    layout.fl_stripe_unit = req->head.args.open.stripe_unit;
  if (req->head.args.open.stripe_count)
    layout.fl_stripe_count = req->head.args.open.stripe_count;
  if (req->head.args.open.object_size)
    layout.fl_object_size = req->head.args.open.object_size;
  if (req->get_connection()->has_feature(CEPH_FEATURE_CREATEPOOLID) &&
      (__s32)req->head.args.open.pool >= 0) {
    layout.fl_pg_pool = req->head.args.open.pool;

    // make sure we have as new a map as the client
    if (req->get_mdsmap_epoch() > mds->mdsmap->get_epoch()) {
      mds->wait_for_mdsmap(req->get_mdsmap_epoch(), new C_MDS_RetryRequest(mdcache, mdr));
      return;
    }
  }

  if (!ceph_file_layout_is_valid(&layout)) {
    dout(10) << " invalid initial file layout" << dendl;
    reply_request(mdr, -EINVAL);
    return;
  }
  if (!mds->mdsmap->is_data_pool(layout.fl_pg_pool)) {
    dout(10) << " invalid data pool " << layout.fl_pg_pool << dendl;
    reply_request(mdr, -EINVAL);
    return;
  }

  CInode *diri = dn->get_dir()->get_inode();
  rdlocks.insert(&diri->authlock);
  if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
    return;

  CDentry::linkage_t *dnl = dn->get_projected_linkage();

  if (!dnl->is_null()) {
    // it existed.  
    assert(req->head.args.open.flags & O_EXCL);
    dout(10) << "O_EXCL, target exists, failing with -EEXIST" << dendl;
    reply_request(mdr, -EEXIST, dnl->get_inode(), dn);
    return;
  }

  // created null dn.
    
  // create inode.
  mdr->now = ceph_clock_now(g_ceph_context);

  SnapRealm *realm = diri->find_snaprealm();   // use directory's realm; inode isn't attached yet.
  snapid_t follows = realm->get_newest_seq();

  CInode *in = prepare_new_inode(mdr, dn->get_dir(), inodeno_t(req->head.ino),
				 req->head.args.open.mode | S_IFREG, &layout);
  assert(in);

  // it's a file.
  dn->push_projected_linkage(in);

  in->inode.version = dn->pre_dirty();
  in->inode.update_backtrace();
  if (cmode & CEPH_FILE_MODE_WR) {
    in->inode.client_ranges[client].range.first = 0;
    in->inode.client_ranges[client].range.last = in->inode.get_layout_size_increment();
    in->inode.client_ranges[client].follows = follows;
  }
  in->inode.rstat.rfiles = 1;

  if (follows >= dn->first)
    dn->first = follows+1;
  in->first = dn->first;
  
  // prepare finisher
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "openc");
  mdlog->start_entry(le);
  le->metablob.add_client_req(req->get_reqid(), req->get_oldest_client_tid());
  journal_allocated_inos(mdr, &le->metablob);
  mdcache->predirty_journal_parents(mdr, &le->metablob, in, dn->get_dir(), PREDIRTY_PRIMARY|PREDIRTY_DIR, 1);
  le->metablob.add_primary_dentry(dn, in, true, true);

  // do the open
  mds->locker->issue_new_caps(in, cmode, mdr->session, realm, req->is_replay());
  in->authlock.set_state(LOCK_EXCL);
  in->xattrlock.set_state(LOCK_EXCL);

  // make sure this inode gets into the journal
  le->metablob.add_opened_ino(in->ino());
  LogSegment *ls = mds->mdlog->get_current_segment();
  ls->open_files.push_back(&in->item_open_file);

  C_MDS_openc_finish *fin = new C_MDS_openc_finish(mds, mdr, dn, in, follows);

  if (mdr->client_request->get_connection()->has_feature(CEPH_FEATURE_REPLY_CREATE_INODE)) {
    dout(10) << "adding ino to reply to indicate inode was created" << dendl;
    // add the file created flag onto the reply if create_flags features is supported
    ::encode(in->inode.ino, mdr->reply_extra_bl);
  }

  journal_and_reply(mdr, in, dn, le, fin);
}



void Server::handle_client_readdir(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;
  client_t client = req->get_source().num();
  set<SimpleLock*> rdlocks, wrlocks, xlocks;
  CInode *diri = rdlock_path_pin_ref(mdr, 0, rdlocks, false, true);
  if (!diri) return;

  // it's a directory, right?
  if (!diri->is_dir()) {
    // not a dir
    dout(10) << "reply to " << *req << " readdir -ENOTDIR" << dendl;
    reply_request(mdr, -ENOTDIR);
    return;
  }

  rdlocks.insert(&diri->filelock);
  rdlocks.insert(&diri->dirfragtreelock);

  if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
    return;

  // which frag?
  frag_t fg = (__u32)req->head.args.readdir.frag;
  dout(10) << " frag " << fg << dendl;

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
    if (dir->is_frozen()) {
      dout(7) << "dir is frozen " << *dir << dendl;
      dir->add_waiter(CDir::WAIT_UNFREEZE, new C_MDS_RetryRequest(mdcache, mdr));
      return;
    }
    // fetch
    dout(10) << " incomplete dir contents for readdir on " << *dir << ", fetching" << dendl;
    dir->fetch(new C_MDS_RetryRequest(mdcache, mdr), true);
    return;
  }

#ifdef MDS_VERIFY_FRAGSTAT
  dir->verify_fragstat();
#endif

  mdr->now = ceph_clock_now(g_ceph_context);

  snapid_t snapid = mdr->snapid;

  string offset_str = req->get_path2();
  const char *offset = offset_str.length() ? offset_str.c_str() : 0;

  dout(10) << "snapid " << snapid << " offset '" << offset_str << "'" << dendl;


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
  unsigned max_bytes = req->head.args.readdir.max_bytes;
  if (!max_bytes)
    max_bytes = 512 << 10;  // 512 KB?

  // start final blob
  bufferlist dirbl;
  dir->encode_dirstat(dirbl, mds->get_nodeid());

  // count bytes available.
  //  this isn't perfect, but we should capture the main variable/unbounded size items!
  int front_bytes = dirbl.length() + sizeof(__u32) + sizeof(__u8)*2;
  int bytes_left = max_bytes - front_bytes;
  bytes_left -= realm->get_snap_trace().length();

  __u32 numfiles = 0;
  while (it != dir->end() && numfiles < max) {
    CDentry *dn = it->second;
    ++it;

    if (dn->state_test(CDentry::STATE_PURGING))
      continue;

    bool dnp = dn->use_projected(client, mdr);
    CDentry::linkage_t *dnl = dnp ? dn->get_projected_linkage() : dn->get_linkage();

    if (dnl->is_null())
      continue;
    if (snaps && dn->last != CEPH_NOSNAP)
      if (dir->try_trim_snap_dentry(dn, *snaps))
	continue;
    if (dn->last < snapid || dn->first > snapid) {
      dout(20) << "skipping non-overlapping snap " << *dn << dendl;
      continue;
    }

    if (offset && strcmp(dn->get_name().c_str(), offset) <= 0)
      continue;

    CInode *in = dnl->get_inode();

    if (in && in->ino() == CEPH_INO_CEPH)
      continue;

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
	// touch everything i _do_ have
	for (CDir::map_t::iterator p = dir->begin(); p != dir->end(); ++p)
	  if (!p->second->get_linkage()->is_null())
	    mdcache->lru.lru_touch(p->second);

	// already issued caps and leases, reply immediately.
	if (dnbl.length() > 0) {
	  mdcache->open_remote_dentry(dn, dnp, new C_NoopContext);
	  dout(10) << " open remote dentry after caps were issued, stopping at "
		   << dnbl.length() << " < " << bytes_left << dendl;
	  break;
	}

	mds->locker->drop_locks(mdr);
	mdr->drop_local_auth_pins();
	mdcache->open_remote_dentry(dn, dnp, new C_MDS_RetryRequest(mdcache, mdr));
	return;
      }
    }
    assert(in);

    if ((int)(dnbl.length() + dn->name.length() + sizeof(__u32) + sizeof(LeaseStat)) > bytes_left) {
      dout(10) << " ran out of room, stopping at " << dnbl.length() << " < " << bytes_left << dendl;
      break;
    }
    
    unsigned start_len = dnbl.length();

    // dentry
    dout(12) << "including    dn " << *dn << dendl;
    ::encode(dn->name, dnbl);
    mds->locker->issue_client_lease(dn, client, dnbl, mdr->now, mdr->session);

    // inode
    dout(12) << "including inode " << *in << dendl;
    int r = in->encode_inodestat(dnbl, mdr->session, realm, snapid, bytes_left - (int)dnbl.length());
    if (r < 0) {
      // chop off dn->name, lease
      dout(10) << " ran out of room, stopping at " << start_len << " < " << bytes_left << dendl;
      bufferlist keep;
      keep.substr_of(dnbl, 0, start_len);
      dnbl.swap(keep);
      break;
    }
    assert(r >= 0);
    numfiles++;

    // touch dn
    mdcache->lru.lru_touch(dn);
  }
  
  __u8 end = (it == dir->end());
  __u8 complete = (end && !offset);  // FIXME: what purpose does this serve
  
  // finish final blob
  ::encode(numfiles, dirbl);
  ::encode(end, dirbl);
  ::encode(complete, dirbl);
  dirbl.claim_append(dnbl);
  
  if (snaps)
    dir->log_mark_dirty();

  // yay, reply
  dout(10) << "reply to " << *req << " readdir num=" << numfiles
	   << " bytes=" << dirbl.length()
	   << " end=" << (int)end
	   << " complete=" << (int)complete
	   << dendl;
  MClientReply *reply = new MClientReply(req, 0);
  reply->set_extra_bl(dirbl);
  dout(10) << "reply to " << *req << " readdir num=" << numfiles << " end=" << (int)end
	   << " complete=" << (int)complete << dendl;

  // bump popularity.  NOTE: this doesn't quite capture it.
  mds->balancer->hit_dir(ceph_clock_now(g_ceph_context), dir, META_POP_IRD, -1, numfiles);
  
  // reply
  reply_request(mdr, reply, diri);
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
  bool truncating_smaller, changed_ranges;
public:
  C_MDS_inode_update_finish(MDS *m, MDRequest *r, CInode *i,
			    bool sm=false, bool cr=false) :
    mds(m), mdr(r), in(i), truncating_smaller(sm), changed_ranges(cr) { }
  void finish(int r) {
    assert(r == 0);

    // apply
    in->pop_and_dirty_projected_inode(mdr->ls);
    mdr->apply();

    // notify any clients
    if (truncating_smaller && in->inode.is_truncating()) {
      mds->locker->issue_truncate(in);
      mds->mdcache->truncate_inode(in, mdr->ls);
    }

    mds->balancer->hit_inode(mdr->now, in, META_POP_IWR);   

    mds->server->reply_request(mdr, 0);

    if (changed_ranges)
      mds->locker->share_inode_max_size(in);
  }
};

void Server::handle_client_file_setlock(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;
  set<SimpleLock*> rdlocks, wrlocks, xlocks;

  // get the inode to operate on, and set up any locks needed for that
  CInode *cur = rdlock_path_pin_ref(mdr, 0, rdlocks, true);
  if (!cur)
    return;

  xlocks.insert(&cur->flocklock);
  /* acquire_locks will return true if it gets the locks. If it fails,
     it will redeliver this request at a later date, so drop the request.
   */
  if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks)) {
    dout(0) << "handle_client_file_setlock could not get locks!" << dendl;
    return;
  }

  // copy the lock change into a ceph_filelock so we can store/apply it
  ceph_filelock set_lock;
  set_lock.start = req->head.args.filelock_change.start;
  set_lock.length = req->head.args.filelock_change.length;
  set_lock.client = req->get_orig_source().num();
  set_lock.pid = req->head.args.filelock_change.pid;
  set_lock.pid_namespace = req->head.args.filelock_change.pid_namespace;
  set_lock.type = req->head.args.filelock_change.type;
  bool will_wait = req->head.args.filelock_change.wait;

  dout(0) << "handle_client_file_setlock: " << set_lock << dendl;

  ceph_lock_state_t *lock_state = NULL;

  // get the appropriate lock state
  switch (req->head.args.filelock_change.rule) {
  case CEPH_LOCK_FLOCK:
    lock_state = &cur->flock_locks;
    break;

  case CEPH_LOCK_FCNTL:
    lock_state = &cur->fcntl_locks;
    break;

  default:
    dout(0) << "got unknown lock type " << set_lock.type
	    << ", dropping request!" << dendl;
    return;
  }

  dout(10) << " state prior to lock change: " << *lock_state << dendl;;
  if (CEPH_LOCK_UNLOCK == set_lock.type) {
    list<ceph_filelock> activated_locks;
    list<Context*> waiters;
    if (lock_state->is_waiting(set_lock)) {
      dout(10) << " unlock removing waiting lock " << set_lock << dendl;
      lock_state->remove_waiting(set_lock);
    } else {
      dout(10) << " unlock attempt on " << set_lock << dendl;
      lock_state->remove_lock(set_lock, activated_locks);
      cur->take_waiting(CInode::WAIT_FLOCK, waiters);
    }
    reply_request(mdr, 0);
    /* For now we're ignoring the activated locks because their responses
     * will be sent when the lock comes up again in rotation by the MDS.
     * It's a cheap hack, but it's easy to code. */
    mds->queue_waiters(waiters);
  } else {
    dout(10) << " lock attempt on " << set_lock << dendl;
    if (mdr->more()->flock_was_waiting &&
	!lock_state->is_waiting(set_lock)) {
      dout(10) << " was waiting for lock but not anymore, must have been canceled " << set_lock << dendl;
      reply_request(mdr, -EINTR);
    } else if (!lock_state->add_lock(set_lock, will_wait, mdr->more()->flock_was_waiting)) {
      dout(10) << " it failed on this attempt" << dendl;
      // couldn't set lock right now
      if (!will_wait) {
	reply_request(mdr, -EWOULDBLOCK);
      } else {
	dout(10) << " added to waiting list" << dendl;
	assert(lock_state->is_waiting(set_lock));
	mdr->more()->flock_was_waiting = true;
	mds->locker->drop_locks(mdr);
	mdr->drop_local_auth_pins();
	cur->add_waiter(CInode::WAIT_FLOCK, new C_MDS_RetryRequest(mdcache, mdr));
      }
    } else
      reply_request(mdr, 0);
  }
  dout(10) << " state after lock change: " << *lock_state << dendl;
}

void Server::handle_client_file_readlock(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;
  set<SimpleLock*> rdlocks, wrlocks, xlocks;

  // get the inode to operate on, and set up any locks needed for that
  CInode *cur = rdlock_path_pin_ref(mdr, 0, rdlocks, true);
  if (!cur)
    return;

  /* acquire_locks will return true if it gets the locks. If it fails,
     it will redeliver this request at a later date, so drop the request.
  */
  rdlocks.insert(&cur->flocklock);
  if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks)) {
    dout(0) << "handle_client_file_readlock could not get locks!" << dendl;
    return;
  }
  
  // copy the lock change into a ceph_filelock so we can store/apply it
  ceph_filelock checking_lock;
  checking_lock.start = req->head.args.filelock_change.start;
  checking_lock.length = req->head.args.filelock_change.length;
  checking_lock.client = req->get_orig_source().num();
  checking_lock.pid = req->head.args.filelock_change.pid;
  checking_lock.type = req->head.args.filelock_change.type;

  // get the appropriate lock state
  ceph_lock_state_t *lock_state = NULL;
  switch (req->head.args.filelock_change.rule) {
  case CEPH_LOCK_FLOCK:
    lock_state = &cur->flock_locks;
    break;

  case CEPH_LOCK_FCNTL:
    lock_state = &cur->fcntl_locks;
    break;

  default:
    dout(0) << "got unknown lock type " << checking_lock.type
	    << ", dropping request!" << dendl;
    return;
  }
  lock_state->look_for_lock(checking_lock);

  bufferlist lock_bl;
  ::encode(checking_lock, lock_bl);

  MClientReply *reply = new MClientReply(req);
  reply->set_extra_bl(lock_bl);
  reply_request(mdr, reply);
}

void Server::handle_client_setattr(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;
  set<SimpleLock*> rdlocks, wrlocks, xlocks;
  CInode *cur = rdlock_path_pin_ref(mdr, 0, rdlocks, true);
  if (!cur) return;

  if (mdr->snapid != CEPH_NOSNAP) {
    reply_request(mdr, -EROFS);
    return;
  }
  if (cur->ino() < MDS_INO_SYSTEM_BASE && !cur->is_base()) {
    reply_request(mdr, -EPERM);
    return;
  }

  __u32 mask = req->head.args.setattr.mask;

  // xlock inode
  if (mask & (CEPH_SETATTR_MODE|CEPH_SETATTR_UID|CEPH_SETATTR_GID))
    xlocks.insert(&cur->authlock);
  if (mask & (CEPH_SETATTR_MTIME|CEPH_SETATTR_ATIME|CEPH_SETATTR_SIZE))
    xlocks.insert(&cur->filelock);
  if (mask & CEPH_SETATTR_CTIME)
    wrlocks.insert(&cur->versionlock);

  if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
    return;

  // trunc from bigger -> smaller?
  inode_t *pi = cur->get_projected_inode();

  uint64_t old_size = MAX(pi->size, req->head.args.setattr.old_size);
  bool truncating_smaller = false;
  if (mask & CEPH_SETATTR_SIZE) {
    truncating_smaller = req->head.args.setattr.size < old_size;
    if (truncating_smaller && pi->is_truncating()) {
      dout(10) << " waiting for pending truncate from " << pi->truncate_from
	       << " to " << pi->truncate_size << " to complete on " << *cur << dendl;
      cur->add_waiter(CInode::WAIT_TRUNC, new C_MDS_RetryRequest(mdcache, mdr));
      mds->mdlog->flush();
      return;
    }
  }

  bool changed_ranges = false;

  // project update
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "setattr");
  mdlog->start_entry(le);

  pi = cur->project_inode();

  utime_t now = ceph_clock_now(g_ceph_context);

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
    if (truncating_smaller) {
      pi->truncate(old_size, req->head.args.setattr.size);
      le->metablob.add_truncate_start(cur->ino());
    } else {
      pi->size = req->head.args.setattr.size;
      pi->rstat.rbytes = pi->size;
    }
    pi->mtime = now;

    // adjust client's max_size?
    map<client_t,client_writeable_range_t> new_ranges;
    mds->locker->calc_new_client_ranges(cur, pi->size, new_ranges);
    if (pi->client_ranges != new_ranges) {
      dout(10) << " client_ranges " << pi->client_ranges << " -> " << new_ranges << dendl;
      pi->client_ranges = new_ranges;
      changed_ranges = true;
    }
  }

  pi->version = cur->pre_dirty();
  pi->ctime = now;

  // log + wait
  le->metablob.add_client_req(req->get_reqid(), req->get_oldest_client_tid());
  mdcache->predirty_journal_parents(mdr, &le->metablob, cur, 0, PREDIRTY_PRIMARY, false);
  mdcache->journal_dirty_inode(mdr, &le->metablob, cur);
  
  journal_and_reply(mdr, cur, 0, le, new C_MDS_inode_update_finish(mds, mdr, cur,
								   truncating_smaller, changed_ranges));

  // flush immediately if there are readers/writers waiting
  if (cur->get_caps_wanted() & (CEPH_CAP_FILE_RD|CEPH_CAP_FILE_WR))
    mds->mdlog->flush();
}

/* Takes responsibility for mdr */
void Server::do_open_truncate(MDRequest *mdr, int cmode)
{
  CInode *in = mdr->in[0];
  client_t client = mdr->get_client();
  assert(in);

  dout(10) << "do_open_truncate " << *in << dendl;

  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "open_truncate");
  mdlog->start_entry(le);

  // prepare
  inode_t *pi = in->project_inode();
  pi->mtime = pi->ctime = ceph_clock_now(g_ceph_context);
  pi->version = in->pre_dirty();

  uint64_t old_size = MAX(pi->size, mdr->client_request->head.args.open.old_size);
  if (old_size > 0) {
    pi->truncate(old_size, 0);
    le->metablob.add_truncate_start(in->ino());
  }

  bool changed_ranges = false;
  if (cmode & CEPH_FILE_MODE_WR) {
    pi->client_ranges[client].range.first = 0;
    pi->client_ranges[client].range.last = pi->get_layout_size_increment();
    pi->client_ranges[client].follows = in->find_snaprealm()->get_newest_seq();
    changed_ranges = true;
  }
  
  le->metablob.add_client_req(mdr->reqid, mdr->client_request->get_oldest_client_tid());

  mdcache->predirty_journal_parents(mdr, &le->metablob, in, 0, PREDIRTY_PRIMARY, false);
  mdcache->journal_dirty_inode(mdr, &le->metablob, in);
  
  // do the open
  SnapRealm *realm = in->find_snaprealm();
  mds->locker->issue_new_caps(in, cmode, mdr->session, realm, mdr->client_request->is_replay());

  // make sure ino gets into the journal
  le->metablob.add_opened_ino(in->ino());
  LogSegment *ls = mds->mdlog->get_current_segment();
  ls->open_files.push_back(&in->item_open_file);
  
  mdr->o_trunc = true;

  journal_and_reply(mdr, in, 0, le, new C_MDS_inode_update_finish(mds, mdr, in, old_size > 0,
								  changed_ranges));
}


/* This function cleans up the passed mdr */
void Server::handle_client_setlayout(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;
  set<SimpleLock*> rdlocks, wrlocks, xlocks;
  CInode *cur = rdlock_path_pin_ref(mdr, 0, rdlocks, true);
  if (!cur) return;

  if (mdr->snapid != CEPH_NOSNAP) {
    reply_request(mdr, -EROFS);
    return;
  }
  if (cur->is_dir()) {
    reply_request(mdr, -EISDIR);
    return;
  }
  
  if (cur->get_projected_inode()->size ||
      cur->get_projected_inode()->truncate_seq > 1) {
    reply_request(mdr, -ENOTEMPTY);
    return;
  }

  // validate layout
  ceph_file_layout layout = cur->get_projected_inode()->layout;
  // save existing layout for later
  int64_t old_pool = layout.fl_pg_pool;

  if (req->head.args.setlayout.layout.fl_object_size > 0)
    layout.fl_object_size = req->head.args.setlayout.layout.fl_object_size;
  if (req->head.args.setlayout.layout.fl_stripe_unit > 0)
    layout.fl_stripe_unit = req->head.args.setlayout.layout.fl_stripe_unit;
  if (req->head.args.setlayout.layout.fl_stripe_count > 0)
    layout.fl_stripe_count=req->head.args.setlayout.layout.fl_stripe_count;
  if (req->head.args.setlayout.layout.fl_cas_hash > 0)
    layout.fl_cas_hash = req->head.args.setlayout.layout.fl_cas_hash;
  if (req->head.args.setlayout.layout.fl_object_stripe_unit > 0)
    layout.fl_object_stripe_unit = req->head.args.setlayout.layout.fl_object_stripe_unit;
  if (req->head.args.setlayout.layout.fl_pg_pool > 0) {
    layout.fl_pg_pool = req->head.args.setlayout.layout.fl_pg_pool;

    // make sure we have as new a map as the client
    if (req->get_mdsmap_epoch() > mds->mdsmap->get_epoch()) {
      mds->wait_for_mdsmap(req->get_mdsmap_epoch(), new C_MDS_RetryRequest(mdcache, mdr));
      return;
    }
  }
  if (!ceph_file_layout_is_valid(&layout)) {
    dout(10) << "bad layout" << dendl;
    reply_request(mdr, -EINVAL);
    return;
  }
  if (!mds->mdsmap->is_data_pool(layout.fl_pg_pool)) {
    dout(10) << " invalid data pool " << layout.fl_pg_pool << dendl;
    reply_request(mdr, -EINVAL);
    return;
  }

  xlocks.insert(&cur->filelock);
  if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
    return;

  // project update
  inode_t *pi = cur->project_inode();
  pi->layout = layout;
  // add the old pool to the inode
  pi->add_old_pool(old_pool);
  pi->version = cur->pre_dirty();
  pi->ctime = ceph_clock_now(g_ceph_context);
  
  // log + wait
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "setlayout");
  mdlog->start_entry(le);
  le->metablob.add_client_req(req->get_reqid(), req->get_oldest_client_tid());
  mdcache->predirty_journal_parents(mdr, &le->metablob, cur, 0, PREDIRTY_PRIMARY, false);
  mdcache->journal_dirty_inode(mdr, &le->metablob, cur);
  
  journal_and_reply(mdr, cur, 0, le, new C_MDS_inode_update_finish(mds, mdr, cur));
}

void Server::handle_client_setdirlayout(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;
  set<SimpleLock*> rdlocks, wrlocks, xlocks;
  ceph_file_layout *dir_layout = NULL;
  CInode *cur = rdlock_path_pin_ref(mdr, 0, rdlocks, true, false, &dir_layout);
  if (!cur) return;

  if (mdr->snapid != CEPH_NOSNAP) {
    reply_request(mdr, -EROFS);
    return;
  }

  if (!cur->is_dir()) {
    reply_request(mdr, -ENOTDIR);
    return;
  }

  xlocks.insert(&cur->policylock);
  if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
    return;

  // validate layout
  inode_t *pi = cur->get_projected_inode();
  ceph_file_layout layout;
  if (pi->has_layout())
    layout = pi->layout;
  else if (dir_layout)
    layout = *dir_layout;
  else
    layout = mds->mdcache->default_file_layout;

  if (req->head.args.setlayout.layout.fl_object_size > 0)
    layout.fl_object_size = req->head.args.setlayout.layout.fl_object_size;
  if (req->head.args.setlayout.layout.fl_stripe_unit > 0)
    layout.fl_stripe_unit = req->head.args.setlayout.layout.fl_stripe_unit;
  if (req->head.args.setlayout.layout.fl_stripe_count > 0)
    layout.fl_stripe_count=req->head.args.setlayout.layout.fl_stripe_count;
  if (req->head.args.setlayout.layout.fl_cas_hash > 0)
    layout.fl_cas_hash = req->head.args.setlayout.layout.fl_cas_hash;
  if (req->head.args.setlayout.layout.fl_object_stripe_unit > 0)
    layout.fl_object_stripe_unit = req->head.args.setlayout.layout.fl_object_stripe_unit;
  if (req->head.args.setlayout.layout.fl_pg_pool > 0) {
    layout.fl_pg_pool = req->head.args.setlayout.layout.fl_pg_pool;
    // make sure we have as new a map as the client
    if (req->get_mdsmap_epoch() > mds->mdsmap->get_epoch()) {
      mds->wait_for_mdsmap(req->get_mdsmap_epoch(), new C_MDS_RetryRequest(mdcache, mdr));
      return;
    }  
  }
  if (!ceph_file_layout_is_valid(&layout)) {
    dout(10) << "bad layout" << dendl;
    reply_request(mdr, -EINVAL);
    return;
  }
  if (!mds->mdsmap->is_data_pool(layout.fl_pg_pool)) {
    dout(10) << " invalid data pool " << layout.fl_pg_pool << dendl;
    reply_request(mdr, -EINVAL);
    return;
  }

  pi = cur->project_inode();
  pi->layout = layout;
  pi->version = cur->pre_dirty();

  // log + wait
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "setlayout");
  mdlog->start_entry(le);
  le->metablob.add_client_req(req->get_reqid(), req->get_oldest_client_tid());
  mdcache->predirty_journal_parents(mdr, &le->metablob, cur, 0, PREDIRTY_PRIMARY, false);
  mdcache->journal_dirty_inode(mdr, &le->metablob, cur);

  journal_and_reply(mdr, cur, 0, le, new C_MDS_inode_update_finish(mds, mdr, cur));
}




// XATTRS

// parse a map of keys/values.
namespace qi = boost::spirit::qi;

template <typename Iterator>
struct keys_and_values
  : qi::grammar<Iterator, std::map<string, string>()>
{
    keys_and_values()
      : keys_and_values::base_type(query)
    {
      query =  pair >> *(qi::lit(' ') >> pair);
      pair  =  key >> '=' >> value;
      key   =  qi::char_("a-zA-Z_") >> *qi::char_("a-zA-Z_0-9");
      value = +qi::char_("a-zA-Z_0-9");
    }
    qi::rule<Iterator, std::map<string, string>()> query;
    qi::rule<Iterator, std::pair<string, string>()> pair;
    qi::rule<Iterator, string()> key, value;
};

int Server::parse_layout_vxattr(string name, string value, ceph_file_layout *layout)
{
  dout(20) << "parse_layout_vxattr name " << name << " value '" << value << "'" << dendl;
  try {
    if (name == "layout") {
      string::iterator begin = value.begin();
      string::iterator end = value.end();
      keys_and_values<string::iterator> p;    // create instance of parser
      std::map<string, string> m;             // map to receive results
      if (!qi::parse(begin, end, p, m)) {     // returns true if successful
	return -EINVAL;
      }
      string left(begin, end);
      dout(10) << " parsed " << m << " left '" << left << "'" << dendl;
      if (begin != end)
	return -EINVAL;
      for (map<string,string>::iterator q = m.begin(); q != m.end(); ++q) {
	int r = parse_layout_vxattr(string("layout.") + q->first, q->second, layout);
	if (r < 0)
	  return r;
      }
    } else if (name == "layout.object_size") {
      layout->fl_object_size = boost::lexical_cast<unsigned>(value);
    } else if (name == "layout.stripe_unit") {
      layout->fl_stripe_unit = boost::lexical_cast<unsigned>(value);
    } else if (name == "layout.stripe_count") {
      layout->fl_stripe_count = boost::lexical_cast<unsigned>(value);
    } else if (name == "layout.pool") {
      try {
	layout->fl_pg_pool = boost::lexical_cast<unsigned>(value);
      } catch (boost::bad_lexical_cast const&) {
	int64_t pool = mds->osdmap->lookup_pg_pool_name(value);
	if (pool < 0) {
	  dout(10) << " unknown pool " << value << dendl;
	  return -ENOENT;
	}
	layout->fl_pg_pool = pool;
      }
    } else {
      dout(10) << " unknown layout vxattr " << name << dendl;
      return -EINVAL;
    }
  } catch (boost::bad_lexical_cast const&) {
    dout(10) << "bad vxattr value, unable to parse int for " << name << dendl;
    return -EINVAL;
  }

  if (!ceph_file_layout_is_valid(layout)) {
    dout(10) << "bad layout" << dendl;
    return -EINVAL;
  }
  if (!mds->mdsmap->is_data_pool(layout->fl_pg_pool)) {
    dout(10) << " invalid data pool " << layout->fl_pg_pool << dendl;
    return -EINVAL;
  }
  return 0;
}

void Server::handle_set_vxattr(MDRequest *mdr, CInode *cur,
			       ceph_file_layout *dir_layout,
			       set<SimpleLock*> rdlocks,
			       set<SimpleLock*> wrlocks,
			       set<SimpleLock*> xlocks)
{
  MClientRequest *req = mdr->client_request;
  string name(req->get_path2());
  bufferlist bl = req->get_data();
  string value (bl.c_str(), bl.length());
  dout(10) << "handle_set_vxattr " << name << " val " << value.length() << " bytes on " << *cur << dendl;

  // layout?
  if (name.find("ceph.file.layout") == 0 ||
      name.find("ceph.dir.layout") == 0) {
    inode_t *pi;
    string rest;
    int64_t old_pool = -1;
    if (name.find("ceph.dir.layout") == 0) {
      if (!cur->is_dir()) {
	reply_request(mdr, -EINVAL);
	return;
      }

      ceph_file_layout layout;
      if (cur->get_projected_inode()->has_layout())
	layout = cur->get_projected_inode()->layout;
      else if (dir_layout)
	layout = *dir_layout;
      else
	layout = mds->mdcache->default_file_layout;

      rest = name.substr(name.find("layout"));
      int r = parse_layout_vxattr(rest, value, &layout);
      if (r < 0) {
	if (r == -ENOENT) {
	  if (!mdr->waited_for_osdmap) {
	    // send request to get latest map, but don't wait if
	    // we don't get anything newer than what we have
	    mdr->waited_for_osdmap = true;
	    mds->request_osdmap(
		  new C_MDS_RetryRequest(mdcache, mdr));
	    return;
	  }
	  r = -EINVAL;
	}
	reply_request(mdr, r);
	return;
      }

      xlocks.insert(&cur->policylock);
      if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
	return;

      pi = cur->project_inode();
      cur->get_projected_inode()->layout = layout;
    } else {
      if (!cur->is_file()) {
	reply_request(mdr, -EINVAL);
	return;
      }
      ceph_file_layout layout = cur->get_projected_inode()->layout;
      rest = name.substr(name.find("layout"));
      int r = parse_layout_vxattr(rest, value, &layout);
      if (r < 0) {
	if (r == -ENOENT) {
	  if (!mdr->waited_for_osdmap) {
	    // send request to get latest map, but don't wait if
	    // we don't get anything newer than what we have
	    mdr->waited_for_osdmap = true;
	    mds->request_osdmap(
		  new C_MDS_RetryRequest(mdcache, mdr));
	    return;
	  }
	  r = -EINVAL;
	}
	reply_request(mdr, r);
	return;
      }

      xlocks.insert(&cur->filelock);
      if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
	return;

      pi = cur->project_inode();
      old_pool = pi->layout.fl_pg_pool;
      pi->add_old_pool(old_pool);
      pi->layout = layout;
      pi->ctime = ceph_clock_now(g_ceph_context);
    }

    pi->version = cur->pre_dirty();
    if (cur->is_file())
      pi->update_backtrace();

    // log + wait
    mdr->ls = mdlog->get_current_segment();
    EUpdate *le = new EUpdate(mdlog, "set vxattr layout");
    mdlog->start_entry(le);
    le->metablob.add_client_req(req->get_reqid(), req->get_oldest_client_tid());
    mdcache->predirty_journal_parents(mdr, &le->metablob, cur, 0, PREDIRTY_PRIMARY, false);
    mdcache->journal_dirty_inode(mdr, &le->metablob, cur);

    journal_and_reply(mdr, cur, 0, le, new C_MDS_inode_update_finish(mds, mdr, cur));
    return;
  }

  dout(10) << " unknown vxattr " << name << dendl;
  reply_request(mdr, -EINVAL);
}

void Server::handle_remove_vxattr(MDRequest *mdr, CInode *cur,
				  set<SimpleLock*> rdlocks,
				  set<SimpleLock*> wrlocks,
				  set<SimpleLock*> xlocks)
{
  MClientRequest *req = mdr->client_request;
  string name(req->get_path2());
  if (name == "ceph.dir.layout") {
    if (!cur->is_dir()) {
      reply_request(mdr, -ENODATA);
      return;
    }
    if (cur->is_root()) {
      dout(10) << "can't remove layout policy on the root directory" << dendl;
      reply_request(mdr, -EINVAL);
      return;
    }

    if (!cur->get_projected_inode()->has_layout()) {
      reply_request(mdr, -ENODATA);
      return;
    }

    xlocks.insert(&cur->policylock);
    if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
      return;

    cur->project_inode();
    cur->get_projected_inode()->clear_layout();
    cur->get_projected_inode()->version = cur->pre_dirty();

    // log + wait
    mdr->ls = mdlog->get_current_segment();
    EUpdate *le = new EUpdate(mdlog, "remove dir layout vxattr");
    mdlog->start_entry(le);
    le->metablob.add_client_req(req->get_reqid(), req->get_oldest_client_tid());
    mdcache->predirty_journal_parents(mdr, &le->metablob, cur, 0, PREDIRTY_PRIMARY, false);
    mdcache->journal_dirty_inode(mdr, &le->metablob, cur);

    journal_and_reply(mdr, cur, 0, le, new C_MDS_inode_update_finish(mds, mdr, cur));
    return;
  }

  reply_request(mdr, -ENODATA);
}

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
  string name(req->get_path2());
  set<SimpleLock*> rdlocks, wrlocks, xlocks;
  CInode *cur;

  ceph_file_layout *dir_layout = NULL;
  if (name.find("ceph.dir.layout") == 0)
    cur = rdlock_path_pin_ref(mdr, 0, rdlocks, true, false, &dir_layout);
  else
    cur = rdlock_path_pin_ref(mdr, 0, rdlocks, true);
  if (!cur)
    return;

  if (mdr->snapid != CEPH_NOSNAP) {
    reply_request(mdr, -EROFS);
    return;
  }

  int flags = req->head.args.setxattr.flags;

  // magic ceph.* namespace?
  if (name.find("ceph.") == 0) {
    handle_set_vxattr(mdr, cur, dir_layout, rdlocks, wrlocks, xlocks);
    return;
  }

  xlocks.insert(&cur->xattrlock);
  if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
    return;

  if ((flags & CEPH_XATTR_CREATE) && cur->xattrs.count(name)) {
    dout(10) << "setxattr '" << name << "' XATTR_CREATE and EEXIST on " << *cur << dendl;
    reply_request(mdr, -EEXIST);
    return;
  }
  if ((flags & CEPH_XATTR_REPLACE) && !cur->xattrs.count(name)) {
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
  pi->ctime = ceph_clock_now(g_ceph_context);
  pi->xattr_version++;
  px->erase(name);
  (*px)[name] = buffer::create(len);
  if (len)
    req->get_data().copy(0, len, (*px)[name].c_str());

  // log + wait
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "setxattr");
  mdlog->start_entry(le);
  le->metablob.add_client_req(req->get_reqid(), req->get_oldest_client_tid());
  mdcache->predirty_journal_parents(mdr, &le->metablob, cur, 0, PREDIRTY_PRIMARY, false);
  mdcache->journal_dirty_inode(mdr, &le->metablob, cur);

  journal_and_reply(mdr, cur, 0, le, new C_MDS_inode_update_finish(mds, mdr, cur));
}

void Server::handle_client_removexattr(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;
  string name(req->get_path2());
  set<SimpleLock*> rdlocks, wrlocks, xlocks;
  ceph_file_layout *dir_layout = NULL;
  CInode *cur;
  if (name == "ceph.dir.layout")
    cur = rdlock_path_pin_ref(mdr, 0, rdlocks, true, false, &dir_layout);
  else
    cur = rdlock_path_pin_ref(mdr, 0, rdlocks, true);
  if (!cur)
    return;

  if (mdr->snapid != CEPH_NOSNAP) {
    reply_request(mdr, -EROFS);
    return;
  }

  if (name.find("ceph.") == 0) {
    handle_remove_vxattr(mdr, cur, rdlocks, wrlocks, xlocks);
    return;
  }

  xlocks.insert(&cur->xattrlock);
  if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
    return;

  map<string, bufferptr> *pxattrs = cur->get_projected_xattrs();
  if (pxattrs->count(name) == 0) {
    dout(10) << "removexattr '" << name << "' and ENODATA on " << *cur << dendl;
    reply_request(mdr, -ENODATA);
    return;
  }

  dout(10) << "removexattr '" << name << "' on " << *cur << dendl;

  // project update
  map<string,bufferptr> *px = new map<string,bufferptr>;
  inode_t *pi = cur->project_inode(px);
  pi->version = cur->pre_dirty();
  pi->ctime = ceph_clock_now(g_ceph_context);
  pi->xattr_version++;
  px->erase(name);

  // log + wait
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "removexattr");
  mdlog->start_entry(le);
  le->metablob.add_client_req(req->get_reqid(), req->get_oldest_client_tid());
  mdcache->predirty_journal_parents(mdr, &le->metablob, cur, 0, PREDIRTY_PRIMARY, false);
  mdcache->journal_dirty_inode(mdr, &le->metablob, cur);

  journal_and_reply(mdr, cur, 0, le, new C_MDS_inode_update_finish(mds, mdr, cur));
}


// =================================================================
// DIRECTORY and NAMESPACE OPS


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
    newi->_mark_dirty_parent(mdr->ls);

    // mkdir?
    if (newi->inode.is_dir()) { 
      CDir *dir = newi->get_dirfrag(frag_t());
      assert(dir);
      dir->mark_dirty(1, mdr->ls);
      dir->mark_new(mdr->ls);
    }

    mdr->apply();

    mds->mdcache->send_dentry_link(dn);

    if (newi->inode.is_file())
      mds->locker->share_inode_max_size(newi);

    // hit pop
    mds->balancer->hit_inode(mdr->now, newi, META_POP_IWR);

    // reply
    MClientReply *reply = new MClientReply(mdr->client_request, 0);
    reply->set_result(0);
    mds->server->reply_request(mdr, reply);
  }
};


void Server::handle_client_mknod(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;
  client_t client = mdr->get_client();
  set<SimpleLock*> rdlocks, wrlocks, xlocks;
  ceph_file_layout *dir_layout = NULL;
  CDentry *dn = rdlock_path_xlock_dentry(mdr, 0, rdlocks, wrlocks, xlocks, false, false, false,
					 &dir_layout);
  if (!dn) return;
  if (mdr->snapid != CEPH_NOSNAP) {
    reply_request(mdr, -EROFS);
    return;
  }
  CInode *diri = dn->get_dir()->get_inode();
  rdlocks.insert(&diri->authlock);
  if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
    return;

  // set layout
  ceph_file_layout layout;
  if (dir_layout)
    layout = *dir_layout;
  else
    layout = mds->mdcache->default_file_layout;

  if (!ceph_file_layout_is_valid(&layout)) {
    dout(10) << " invalid initial file layout" << dendl;
    reply_request(mdr, -EINVAL);
    return;
  }

  SnapRealm *realm = dn->get_dir()->inode->find_snaprealm();
  snapid_t follows = realm->get_newest_seq();
  mdr->now = ceph_clock_now(g_ceph_context);

  CInode *newi = prepare_new_inode(mdr, dn->get_dir(), inodeno_t(req->head.ino),
				   req->head.args.mknod.mode, &layout);
  assert(newi);

  dn->push_projected_linkage(newi);

  newi->inode.rdev = req->head.args.mknod.rdev;
  if ((newi->inode.mode & S_IFMT) == 0)
    newi->inode.mode |= S_IFREG;
  newi->inode.version = dn->pre_dirty();
  newi->inode.rstat.rfiles = 1;
  newi->inode.update_backtrace();

  // if the client created a _regular_ file via MKNOD, it's highly likely they'll
  // want to write to it (e.g., if they are reexporting NFS)
  if (S_ISREG(newi->inode.mode)) {
    dout(15) << " setting a client_range too, since this is a regular file" << dendl;
    newi->inode.client_ranges[client].range.first = 0;
    newi->inode.client_ranges[client].range.last = newi->inode.get_layout_size_increment();
    newi->inode.client_ranges[client].follows = follows;

    // issue a cap on the file
    int cmode = CEPH_FILE_MODE_RDWR;
    Capability *cap = mds->locker->issue_new_caps(newi, cmode, mdr->session, realm, req->is_replay());
    if (cap) {
      cap->set_wanted(0);

      // put locks in excl mode
      newi->filelock.set_state(LOCK_EXCL);
      newi->authlock.set_state(LOCK_EXCL);
      newi->xattrlock.set_state(LOCK_EXCL);
      cap->issue_norevoke(CEPH_CAP_AUTH_EXCL|CEPH_CAP_AUTH_SHARED|
			  CEPH_CAP_XATTR_EXCL|CEPH_CAP_XATTR_SHARED|
			  CEPH_CAP_ANY_FILE_WR);
    }
  }

  if (follows >= dn->first)
    dn->first = follows + 1;
  newi->first = dn->first;
    
  dout(10) << "mknod mode " << newi->inode.mode << " rdev " << newi->inode.rdev << dendl;

  // prepare finisher
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "mknod");
  mdlog->start_entry(le);
  le->metablob.add_client_req(req->get_reqid(), req->get_oldest_client_tid());
  journal_allocated_inos(mdr, &le->metablob);
  
  mdcache->predirty_journal_parents(mdr, &le->metablob, newi, dn->get_dir(),
				    PREDIRTY_PRIMARY|PREDIRTY_DIR, 1);
  le->metablob.add_primary_dentry(dn, newi, true, true);

  journal_and_reply(mdr, newi, dn, le, new C_MDS_mknod_finish(mds, mdr, dn, newi, follows));
}



// MKDIR
/* This function takes responsibility for the passed mdr*/
void Server::handle_client_mkdir(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;
  set<SimpleLock*> rdlocks, wrlocks, xlocks;
  CDentry *dn = rdlock_path_xlock_dentry(mdr, 0, rdlocks, wrlocks, xlocks, false, false, false);
  if (!dn) return;
  if (mdr->snapid != CEPH_NOSNAP) {
    reply_request(mdr, -EROFS);
    return;
  }
  CInode *diri = dn->get_dir()->get_inode();
  rdlocks.insert(&diri->authlock);
  if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
    return;

  // new inode
  SnapRealm *realm = dn->get_dir()->inode->find_snaprealm();
  snapid_t follows = realm->get_newest_seq();
  mdr->now = ceph_clock_now(g_ceph_context);

  unsigned mode = req->head.args.mkdir.mode;
  mode &= ~S_IFMT;
  mode |= S_IFDIR;
  CInode *newi = prepare_new_inode(mdr, dn->get_dir(), inodeno_t(req->head.ino), mode);  
  assert(newi);

  // it's a directory.
  dn->push_projected_linkage(newi);

  newi->inode.version = dn->pre_dirty();
  newi->inode.rstat.rsubdirs = 1;
  newi->inode.update_backtrace();

  dout(12) << " follows " << follows << dendl;
  if (follows >= dn->first)
    dn->first = follows + 1;
  newi->first = dn->first;

  // ...and that new dir is empty.
  CDir *newdir = newi->get_or_open_dirfrag(mds->mdcache, frag_t());
  newdir->mark_complete();
  newdir->pre_dirty();

  // prepare finisher
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "mkdir");
  mdlog->start_entry(le);
  le->metablob.add_client_req(req->get_reqid(), req->get_oldest_client_tid());
  journal_allocated_inos(mdr, &le->metablob);
  mdcache->predirty_journal_parents(mdr, &le->metablob, newi, dn->get_dir(), PREDIRTY_PRIMARY|PREDIRTY_DIR, 1);
  le->metablob.add_primary_dentry(dn, newi, true, true);
  le->metablob.add_new_dir(newdir); // dirty AND complete AND new
  
  // issue a cap on the directory
  int cmode = CEPH_FILE_MODE_RDWR;
  Capability *cap = mds->locker->issue_new_caps(newi, cmode, mdr->session, realm, req->is_replay());
  if (cap) {
    cap->set_wanted(0);

    // put locks in excl mode
    newi->filelock.set_state(LOCK_EXCL);
    newi->authlock.set_state(LOCK_EXCL);
    newi->xattrlock.set_state(LOCK_EXCL);
    cap->issue_norevoke(CEPH_CAP_AUTH_EXCL|CEPH_CAP_AUTH_SHARED|
			CEPH_CAP_XATTR_EXCL|CEPH_CAP_XATTR_SHARED);
  }

  // make sure this inode gets into the journal
  le->metablob.add_opened_ino(newi->ino());
  LogSegment *ls = mds->mdlog->get_current_segment();
  ls->open_files.push_back(&newi->item_open_file);

  journal_and_reply(mdr, newi, dn, le, new C_MDS_mknod_finish(mds, mdr, dn, newi, follows));
}


// SYMLINK

void Server::handle_client_symlink(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;
  set<SimpleLock*> rdlocks, wrlocks, xlocks;
  CDentry *dn = rdlock_path_xlock_dentry(mdr, 0, rdlocks, wrlocks, xlocks, false, false, false);
  if (!dn) return;
  if (mdr->snapid != CEPH_NOSNAP) {
    reply_request(mdr, -EROFS);
    return;
  }
  CInode *diri = dn->get_dir()->get_inode();
  rdlocks.insert(&diri->authlock);
  if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
    return;

  mdr->now = ceph_clock_now(g_ceph_context);
  snapid_t follows = dn->get_dir()->inode->find_snaprealm()->get_newest_seq();

  unsigned mode = S_IFLNK | 0777;
  CInode *newi = prepare_new_inode(mdr, dn->get_dir(), inodeno_t(req->head.ino), mode);
  assert(newi);

  // it's a symlink
  dn->push_projected_linkage(newi);

  newi->symlink = req->get_path2();
  newi->inode.size = newi->symlink.length();
  newi->inode.rstat.rbytes = newi->inode.size;
  newi->inode.rstat.rfiles = 1;
  newi->inode.version = dn->pre_dirty();
  newi->inode.update_backtrace();

  if (follows >= dn->first)
    dn->first = follows + 1;
  newi->first = dn->first;

  // prepare finisher
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "symlink");
  mdlog->start_entry(le);
  le->metablob.add_client_req(req->get_reqid(), req->get_oldest_client_tid());
  journal_allocated_inos(mdr, &le->metablob);
  mdcache->predirty_journal_parents(mdr, &le->metablob, newi, dn->get_dir(), PREDIRTY_PRIMARY|PREDIRTY_DIR, 1);
  le->metablob.add_primary_dentry(dn, newi, true, true);

  journal_and_reply(mdr, newi, dn, le, new C_MDS_mknod_finish(mds, mdr, dn, newi, follows));
}





// LINK

void Server::handle_client_link(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;

  dout(7) << "handle_client_link " << req->get_filepath()
	  << " to " << req->get_filepath2()
	  << dendl;

  set<SimpleLock*> rdlocks, wrlocks, xlocks;

  CDentry *dn = rdlock_path_xlock_dentry(mdr, 0, rdlocks, wrlocks, xlocks, false, false, false);
  if (!dn) return;
  CInode *targeti = rdlock_path_pin_ref(mdr, 1, rdlocks, false);
  if (!targeti) return;
  if (mdr->snapid != CEPH_NOSNAP) {
    reply_request(mdr, -EROFS);
    return;
  }

  CDir *dir = dn->get_dir();
  dout(7) << "handle_client_link link " << dn->get_name() << " in " << *dir << dendl;
  dout(7) << "target is " << *targeti << dendl;
  if (targeti->is_dir()) {
    dout(7) << "target is a dir, failing..." << dendl;
    reply_request(mdr, -EINVAL);
    return;
  }
  
  xlocks.insert(&targeti->linklock);

  // take any locks needed for anchor creation/verification
  // NOTE: we do this on the master even if the anchor/link update may happen
  // on the slave.  That means we may have out of date anchor state on our
  // end.  That's fine:  either, we xlock when we don't need to (slow but
  // not a problem), or we rdlock when we need to xlock, but then discover we
  // need to xlock and on our next pass through we adjust the locks (this works
  // as long as the linklock rdlock isn't the very last lock we take).
  mds->mdcache->anchor_create_prep_locks(mdr, targeti, rdlocks, xlocks);

  if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
    return;

  // pick mtime
  if (mdr->now == utime_t())
    mdr->now = ceph_clock_now(g_ceph_context);

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
  assert(g_conf->mds_kill_link_at != 1);

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
  if (follows >= dn->first)
    dn->first = follows;

  // log + wait
  EUpdate *le = new EUpdate(mdlog, "link_local");
  mdlog->start_entry(le);
  le->metablob.add_client_req(mdr->reqid, mdr->client_request->get_oldest_client_tid());
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
  
  mds->mdcache->send_dentry_link(dn);

  // bump target popularity
  mds->balancer->hit_inode(mdr->now, targeti, META_POP_IWR);
  mds->balancer->hit_dir(mdr->now, dn->get_dir(), META_POP_IWR);

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
    if (!mds->mdsmap->is_clientreplay_or_active_or_stopping(linkauth)) {
      dout(10) << " targeti auth mds." << linkauth << " is not active" << dendl;
      if (mdr->more()->waiting_on_slave.empty())
	mds->wait_for_active_peer(linkauth, new C_MDS_RetryRequest(mdcache, mdr));
      return;
    }

    dout(10) << " targeti auth must prepare nlink++/--" << dendl;
    int op;
    if (inc)
      op = MMDSSlaveRequest::OP_LINKPREP;
    else 
      op = MMDSSlaveRequest::OP_UNLINKPREP;
    MMDSSlaveRequest *req = new MMDSSlaveRequest(mdr->reqid, mdr->attempt, op);
    targeti->set_object_info(req->get_object_info());
    req->now = mdr->now;
    mds->send_message_mds(req, linkauth);

    assert(mdr->more()->waiting_on_slave.count(linkauth) == 0);
    mdr->more()->waiting_on_slave.insert(linkauth);
    return;
  }
  dout(10) << " targeti auth has prepared nlink++/--" << dendl;

  assert(g_conf->mds_kill_link_at != 2);

  // add to event
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, inc ? "link_remote":"unlink_remote");
  mdlog->start_entry(le);
  le->metablob.add_client_req(mdr->reqid, mdr->client_request->get_oldest_client_tid());
  if (!mdr->more()->witnessed.empty()) {
    dout(20) << " noting uncommitted_slaves " << mdr->more()->witnessed << dendl;
    le->reqid = mdr->reqid;
    le->had_slaves = true;
    mds->mdcache->add_uncommitted_master(mdr->reqid, mdr->ls, mdr->more()->witnessed);
  }

  if (inc) {
    dn->pre_dirty();
    mdcache->predirty_journal_parents(mdr, &le->metablob, targeti, dn->get_dir(), PREDIRTY_DIR, 1);
    le->metablob.add_remote_dentry(dn, true, targeti->ino(), targeti->d_type()); // new remote
    dn->push_projected_linkage(targeti->ino(), targeti->d_type());
  } else {
    dn->pre_dirty();
    mdcache->predirty_journal_parents(mdr, &le->metablob, targeti, dn->get_dir(), PREDIRTY_DIR, -1);
    mdcache->journal_cow_dentry(mdr, &le->metablob, dn);
    le->metablob.add_null_dentry(dn, true);
  }

  if (mdr->more()->dst_reanchor_atid)
    le->metablob.add_table_transaction(TABLE_ANCHOR, mdr->more()->dst_reanchor_atid);

  journal_and_reply(mdr, targeti, dn, le, new C_MDS_link_remote_finish(mds, mdr, inc, dn, targeti));
}

void Server::_link_remote_finish(MDRequest *mdr, bool inc,
				 CDentry *dn, CInode *targeti,
				 version_t dpv)
{
  dout(10) << "_link_remote_finish "
	   << (inc ? "link ":"unlink ")
	   << *dn << " to " << *targeti << dendl;

  assert(g_conf->mds_kill_link_at != 3);

  if (!mdr->more()->witnessed.empty())
    mdcache->logged_master_update(mdr->reqid);

  if (inc) {
    // link the new dentry
    dn->pop_projected_linkage();
    dn->mark_dirty(dpv, mdr->ls);
  } else {
    // unlink main dentry
    dn->get_dir()->unlink_inode(dn);
    dn->mark_dirty(dn->get_projected_version(), mdr->ls);  // dirty old dentry
  }

  mdr->apply();

  if (inc)
    mds->mdcache->send_dentry_link(dn);
  else
    mds->mdcache->send_dentry_unlink(dn, NULL, NULL);
  
  // commit anchor update?
  if (mdr->more()->dst_reanchor_atid) 
    mds->anchorclient->commit(mdr->more()->dst_reanchor_atid, mdr->ls);

  // bump target popularity
  mds->balancer->hit_inode(mdr->now, targeti, META_POP_IWR);
  mds->balancer->hit_dir(mdr->now, dn->get_dir(), META_POP_IWR);

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

/* This function DOES put the mdr->slave_request before returning*/
void Server::handle_slave_link_prep(MDRequest *mdr)
{
  dout(10) << "handle_slave_link_prep " << *mdr 
	   << " on " << mdr->slave_request->get_object_info() 
	   << dendl;

  assert(g_conf->mds_kill_link_at != 4);

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
    
    // NOTE: the master took any locks needed for anchor creation/verification.

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

  assert(g_conf->mds_kill_link_at != 5);

  // journal it
  mdr->ls = mdlog->get_current_segment();
  ESlaveUpdate *le = new ESlaveUpdate(mdlog, "slave_link_prep", mdr->reqid, mdr->slave_to_mds,
				      ESlaveUpdate::OP_PREPARE, ESlaveUpdate::LINK);
  mdlog->start_entry(le);

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

  assert(g_conf->mds_kill_link_at != 6);

  // update the target
  targeti->pop_and_dirty_projected_inode(mdr->ls);
  mdr->apply();

  // hit pop
  mds->balancer->hit_inode(mdr->now, targeti, META_POP_IWR);

  // ack
  MMDSSlaveRequest *reply = new MMDSSlaveRequest(mdr->reqid, mdr->attempt,
						 MMDSSlaveRequest::OP_LINKPREPACK);
  mds->send_message_mds(reply, mdr->slave_to_mds);
  
  // set up commit waiter
  mdr->more()->slave_commit = new C_MDS_SlaveLinkCommit(this, mdr, targeti);

  // done.
  mdr->slave_request->put();
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

  assert(g_conf->mds_kill_link_at != 7);

  if (r == 0) {
    // drop our pins, etc.
    mdr->cleanup();

    // write a commit to the journal
    ESlaveUpdate *le = new ESlaveUpdate(mdlog, "slave_link_commit", mdr->reqid, mdr->slave_to_mds,
					ESlaveUpdate::OP_COMMIT, ESlaveUpdate::LINK);
    mdlog->start_submit_entry(le, new C_MDS_CommittedSlave(this, mdr));
    mdlog->flush();
  } else {
    do_link_rollback(mdr->more()->rollback_bl, mdr->slave_to_mds, mdr);
  }
}

void Server::_committed_slave(MDRequest *mdr)
{
  dout(10) << "_committed_slave " << *mdr << dendl;

  assert(g_conf->mds_kill_link_at != 8);

  MMDSSlaveRequest *req = new MMDSSlaveRequest(mdr->reqid, mdr->attempt, 
					       MMDSSlaveRequest::OP_COMMITTED);
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

  assert(g_conf->mds_kill_link_at != 9);

  mds->mdcache->add_rollback(rollback.reqid, master); // need to finish this update before resolve finishes
  assert(mdr || mds->is_resolve());

  Mutation *mut = new Mutation(rollback.reqid);
  mut->ls = mds->mdlog->get_current_segment();

  CInode *in = mds->mdcache->get_inode(rollback.ino);
  assert(in);
  dout(10) << " target is " << *in << dendl;
  assert(!in->is_projected());  // live slave request hold versionlock xlock.
  
  inode_t *pi = in->project_inode();
  pi->version = in->pre_dirty();
  mut->add_projected_inode(in);

  // parent dir rctime
  CDir *parent = in->get_projected_parent_dn()->get_dir();
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
  mdlog->start_entry(le);
  le->commit.add_dir_context(parent);
  le->commit.add_dir(parent, true);
  le->commit.add_primary_dentry(in->get_projected_parent_dn(), 0, true);
  
  mdlog->submit_entry(le, new C_MDS_LoggedLinkRollback(this, mut, mdr));
  mdlog->flush();
}

void Server::_link_rollback_finish(Mutation *mut, MDRequest *mdr)
{
  dout(10) << "_link_rollback_finish" << dendl;

  assert(g_conf->mds_kill_link_at != 10);

  mut->apply();
  if (mdr)
    mds->mdcache->request_finish(mdr);

  mds->mdcache->finish_rollback(mut->reqid);

  mut->cleanup();
  delete mut;
}


/* This function DOES NOT put the passed message before returning*/
void Server::handle_slave_link_prep_ack(MDRequest *mdr, MMDSSlaveRequest *m)
{
  dout(10) << "handle_slave_link_prep_ack " << *mdr 
	   << " " << *m << dendl;
  int from = m->get_source().num();

  assert(g_conf->mds_kill_link_at != 11);

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
  client_t client = mdr->get_client();

  // rmdir or unlink?
  bool rmdir = false;
  if (req->get_op() == CEPH_MDS_OP_RMDIR) rmdir = true;

  if (req->get_filepath().depth() == 0) {
    reply_request(mdr, -EINVAL);
    return;
  }    

  // traverse to path
  vector<CDentry*> trace;
  CInode *in;
  int r = mdcache->path_traverse(mdr, NULL, NULL, req->get_filepath(), &trace, &in, MDS_TRAVERSE_FORWARD);
  if (r > 0) return;
  if (r < 0) {
    reply_request(mdr, r);
    return;
  }
  if (mdr->snapid != CEPH_NOSNAP) {
    reply_request(mdr, -EROFS);
    return;
  }

  CDentry *dn = trace[trace.size()-1];
  assert(dn);
  if (!dn->is_auth()) {
    mdcache->request_forward(mdr, dn->authority().first);
    return;
  }

  CDentry::linkage_t *dnl = dn->get_linkage(client, mdr);
  assert(!dnl->is_null());

  if (rmdir) {
    dout(7) << "handle_client_rmdir on " << *dn << dendl;
  } else {
    dout(7) << "handle_client_unlink on " << *dn << dendl;
  }
  dout(7) << "dn links to " << *in << dendl;

  // rmdir vs is_dir 
  if (in->is_dir()) {
    if (rmdir) {
      // do empty directory checks
      if (_dir_is_nonempty_unlocked(mdr, in)) {
	reply_request(mdr, -ENOTEMPTY);
	return;
      }
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

  // -- create stray dentry? --
  CDentry *straydn = NULL;
  if (dnl->is_primary()) {
    straydn = prepare_stray_dentry(mdr, dnl->get_inode());
    dout(10) << " straydn is " << *straydn << dendl;
  } else if (mdr->straydn) {
    mdr->unpin(mdr->straydn);
    mdr->straydn = NULL;
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
    xlocks.insert(&straydn->lock);
  }
  if (in->is_dir())
    rdlocks.insert(&in->filelock);   // to verify it's empty
  mds->locker->include_snap_rdlocks(rdlocks, dnl->get_inode());

  // if we unlink a snapped multiversion inode and are creating a
  // remote link to it, it must be anchored.  this mirrors the logic
  // in MDCache::journal_cow_dentry().
  bool need_snap_dentry = 
    dnl->is_primary() &&
    in->is_multiversion() &&
    in->find_snaprealm()->get_newest_seq() + 1 > dn->first;
  if (need_snap_dentry) {
    dout(10) << " i need to be anchored because i am multiversion and will get a remote cow dentry" << dendl;
    mds->mdcache->anchor_create_prep_locks(mdr, in, rdlocks, xlocks);
  }

  if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
    return;

  if (in->is_dir() &&
      _dir_is_nonempty(mdr, in)) {
    reply_request(mdr, -ENOTEMPTY);
    return;
  }

  // yay!
  if (mdr->now == utime_t())
    mdr->now = ceph_clock_now(g_ceph_context);

  // NOTE: this is non-optimal.  we create an anchor at the old
  // location, and then change it.  we can do better, but it's more
  // complicated.  this is fine for now.
  if (need_snap_dentry && !in->is_anchored()) {
    mdcache->anchor_create(mdr, in, new C_MDS_RetryRequest(mdcache, mdr));
    return;
  }

  // get stray dn ready?
  if (dnl->is_primary()) {
    if (!mdr->more()->dst_reanchor_atid && in->is_anchored()) {
      dout(10) << "reanchoring to stray " << *dnl->get_inode() << dendl;
      vector<Anchor> trace;
      straydn->make_anchor_trace(trace, dnl->get_inode());
      mds->anchorclient->prepare_update(dnl->get_inode()->ino(), trace, &mdr->more()->dst_reanchor_atid, 
					new C_MDS_RetryRequest(mdcache, mdr));
      return;
    }
  }

  if (in->is_dir() && in->has_subtree_root_dirfrag()) {
    // subtree root auths need to be witnesses
    set<int> witnesses;
    list<CDir*> ls;
    in->get_subtree_dirfrags(ls);
    for (list<CDir*>::iterator p = ls.begin(); p != ls.end(); ++p) {
      CDir *dir = *p;
      int auth = dir->authority().first;
      witnesses.insert(auth);
      dout(10) << " need mds." << auth << " to witness for dirfrag " << *dir << dendl;      
    } 
    dout(10) << " witnesses " << witnesses << ", have " << mdr->more()->witnessed << dendl;

    for (set<int>::iterator p = witnesses.begin();
	 p != witnesses.end();
	 ++p) {
      if (mdr->more()->witnessed.count(*p)) {
	dout(10) << " already witnessed by mds." << *p << dendl;
      } else if (mdr->more()->waiting_on_slave.count(*p)) {
	dout(10) << " already waiting on witness mds." << *p << dendl;      
      } else {
	if (!_rmdir_prepare_witness(mdr, *p, dn, straydn))
	  return;
      }
    }
    if (!mdr->more()->waiting_on_slave.empty())
      return;  // we're waiting for a witness.
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
  CInode *in = dnl->get_inode();

  SnapRealm *realm = in->find_snaprealm();
  snapid_t follows = realm->get_newest_seq();

  // ok, let's do it.
  mdr->ls = mdlog->get_current_segment();

  // prepare log entry
  EUpdate *le = new EUpdate(mdlog, "unlink_local");
  mdlog->start_entry(le);
  le->metablob.add_client_req(mdr->reqid, mdr->client_request->get_oldest_client_tid());
  if (!mdr->more()->witnessed.empty()) {
    dout(20) << " noting uncommitted_slaves " << mdr->more()->witnessed << dendl;
    le->reqid = mdr->reqid;
    le->had_slaves = true;
    mds->mdcache->add_uncommitted_master(mdr->reqid, mdr->ls, mdr->more()->witnessed);
  }

  if (straydn) {
    assert(dnl->is_primary());
    straydn->push_projected_linkage(in);
    straydn->first = follows + 1;
  }

  // the unlinked dentry
  dn->pre_dirty();

  inode_t *pi = in->project_inode();
  mdr->add_projected_inode(in); // do this _after_ my dn->pre_dirty().. we apply that one manually.
  pi->version = in->pre_dirty();
  pi->nlink--;
  pi->ctime = mdr->now;

  if (dnl->is_primary()) {
    // primary link.  add stray dentry.
    assert(straydn);
    mdcache->predirty_journal_parents(mdr, &le->metablob, in, dn->get_dir(), PREDIRTY_PRIMARY|PREDIRTY_DIR, -1);
    mdcache->predirty_journal_parents(mdr, &le->metablob, in, straydn->get_dir(), PREDIRTY_PRIMARY|PREDIRTY_DIR, 1);

    // project snaprealm, too
    if (in->snaprealm || follows + 1 > dn->first)
      in->project_past_snaprealm_parent(straydn->get_dir()->inode->find_snaprealm());

    pi->update_backtrace();
    le->metablob.add_primary_dentry(straydn, in, true, true);
  } else {
    // remote link.  update remote inode.
    mdcache->predirty_journal_parents(mdr, &le->metablob, in, dn->get_dir(), PREDIRTY_DIR, -1);
    mdcache->predirty_journal_parents(mdr, &le->metablob, in, 0, PREDIRTY_PRIMARY);
    mdcache->journal_dirty_inode(mdr, &le->metablob, in);
  }

  mdcache->journal_cow_dentry(mdr, &le->metablob, dn);
  le->metablob.add_null_dentry(dn, true);

  if (in->is_dir()) {
    dout(10) << " noting renamed (unlinked) dir ino " << in->ino() << " in metablob" << dendl;
    le->metablob.renamed_dirino = in->ino();
  }

  if (mdr->more()->dst_reanchor_atid)
    le->metablob.add_table_transaction(TABLE_ANCHOR, mdr->more()->dst_reanchor_atid);

  dn->push_projected_linkage();

  if (in->is_dir()) {
    assert(straydn);
    mds->mdcache->project_subtree_rename(in, dn->get_dir(), straydn->get_dir());
  }

  journal_and_reply(mdr, 0, dn, le, new C_MDS_unlink_local_finish(mds, mdr, dn, straydn));
}

void Server::_unlink_local_finish(MDRequest *mdr, 
				  CDentry *dn, CDentry *straydn,
				  version_t dnpv) 
{
  dout(10) << "_unlink_local_finish " << *dn << dendl;

  if (!mdr->more()->witnessed.empty())
    mdcache->logged_master_update(mdr->reqid);

  // unlink main dentry
  dn->get_dir()->unlink_inode(dn);
  dn->pop_projected_linkage();

  // relink as stray?  (i.e. was primary link?)
  CDentry::linkage_t *straydnl = 0;

  bool snap_is_new = false;
  if (straydn) {
    dout(20) << " straydn is " << *straydn << dendl;
    straydnl = straydn->pop_projected_linkage();
    
    snap_is_new = straydnl->get_inode()->snaprealm ? true : false;
    mdcache->touch_dentry_bottom(straydn);
  }

  dn->mark_dirty(dnpv, mdr->ls);
  mdr->apply();

  if (snap_is_new) //only new if straydnl exists
    mdcache->do_realm_invalidate_and_update_notify(straydnl->get_inode(), CEPH_SNAP_OP_SPLIT, true);
  
  mds->mdcache->send_dentry_unlink(dn, straydn, mdr);
  
  // update subtree map?
  if (straydn && straydnl->get_inode()->is_dir()) 
    mdcache->adjust_subtree_after_rename(straydnl->get_inode(), dn->get_dir(), true);

  // commit anchor update?
  if (mdr->more()->dst_reanchor_atid) 
    mds->anchorclient->commit(mdr->more()->dst_reanchor_atid, mdr->ls);

  // bump pop
  mds->balancer->hit_dir(mdr->now, dn->get_dir(), META_POP_IWR);

  // reply
  MClientReply *reply = new MClientReply(mdr->client_request, 0);
  reply_request(mdr, reply);
  
  // clean up?
  if (straydn)
    mdcache->eval_stray(straydn);

  // removing a new dn?
  dn->get_dir()->try_remove_unlinked_dn(dn);
}

bool Server::_rmdir_prepare_witness(MDRequest *mdr, int who, CDentry *dn, CDentry *straydn)
{
  if (!mds->mdsmap->is_clientreplay_or_active_or_stopping(who)) {
    dout(10) << "_rmdir_prepare_witness mds." << who << " is not active" << dendl;
    if (mdr->more()->waiting_on_slave.empty())
      mds->wait_for_active_peer(who, new C_MDS_RetryRequest(mdcache, mdr));
    return false;
  }
  
  dout(10) << "_rmdir_prepare_witness mds." << who << dendl;
  MMDSSlaveRequest *req = new MMDSSlaveRequest(mdr->reqid, mdr->attempt,
					       MMDSSlaveRequest::OP_RMDIRPREP);
  dn->make_path(req->srcdnpath);
  straydn->make_path(req->destdnpath);
  req->now = mdr->now;
  
  mdcache->replicate_stray(straydn, who, req->stray);
  
  mds->send_message_mds(req, who);
  
  assert(mdr->more()->waiting_on_slave.count(who) == 0);
  mdr->more()->waiting_on_slave.insert(who);
  return true;
}

struct C_MDS_SlaveRmdirPrep : public Context {
  Server *server;
  MDRequest *mdr;
  CDentry *dn, *straydn;
  C_MDS_SlaveRmdirPrep(Server *s, MDRequest *r, CDentry *d, CDentry *st)
    : server(s), mdr(r), dn(d), straydn(st) {}
  void finish(int r) {
    server->_logged_slave_rmdir(mdr, dn, straydn);
  }
};

void Server::handle_slave_rmdir_prep(MDRequest *mdr)
{
  dout(10) << "handle_slave_rmdir_prep " << *mdr 
	   << " " << mdr->slave_request->srcdnpath 
	   << " to " << mdr->slave_request->destdnpath
	   << dendl;

  vector<CDentry*> trace;
  filepath srcpath(mdr->slave_request->srcdnpath);
  dout(10) << " src " << srcpath << dendl;
  CInode *in;
  int r = mdcache->path_traverse(mdr, NULL, NULL, srcpath, &trace, &in, MDS_TRAVERSE_DISCOVERXLOCK);
  assert(r == 0);
  CDentry *dn = trace[trace.size()-1];
  dout(10) << " dn " << *dn << dendl;
  mdr->pin(dn);

  assert(mdr->straydn);
  CDentry *straydn = mdr->straydn;
  dout(10) << " straydn " << *straydn << dendl;
  
  mdr->now = mdr->slave_request->now;

  rmdir_rollback rollback;
  rollback.reqid = mdr->reqid;
  rollback.src_dir = dn->get_dir()->dirfrag();
  rollback.src_dname = dn->name;
  rollback.dest_dir = straydn->get_dir()->dirfrag();
  rollback.dest_dname = straydn->name;
  ::encode(rollback, mdr->more()->rollback_bl);
  dout(20) << " rollback is " << mdr->more()->rollback_bl.length() << " bytes" << dendl;

  straydn->push_projected_linkage(in);
  dn->push_projected_linkage();

  ESlaveUpdate *le =  new ESlaveUpdate(mdlog, "slave_rmdir", mdr->reqid, mdr->slave_to_mds,
				       ESlaveUpdate::OP_PREPARE, ESlaveUpdate::RMDIR);
  mdlog->start_entry(le);
  le->rollback = mdr->more()->rollback_bl;

  le->commit.add_dir_context(straydn->get_dir());
  le->commit.add_primary_dentry(straydn, in, true);
  // slave: no need to journal original dentry

  dout(10) << " noting renamed (unlinked) dir ino " << in->ino() << " in metablob" << dendl;
  le->commit.renamed_dirino = in->ino();

  mds->mdcache->project_subtree_rename(in, dn->get_dir(), straydn->get_dir());

  mdlog->submit_entry(le, new C_MDS_SlaveRmdirPrep(this, mdr, dn, straydn));
  mdlog->flush();
}

struct C_MDS_SlaveRmdirCommit : public Context {
  Server *server;
  MDRequest *mdr;
  C_MDS_SlaveRmdirCommit(Server *s, MDRequest *r)
    : server(s), mdr(r) { }
  void finish(int r) {
    server->_commit_slave_rmdir(mdr, r);
  }
};

void Server::_logged_slave_rmdir(MDRequest *mdr, CDentry *dn, CDentry *straydn)
{
  dout(10) << "_logged_slave_rmdir " << *mdr << " on " << *dn << dendl;

  // update our cache now, so we are consistent with what is in the journal
  // when we journal a subtree map
  CInode *in = dn->get_linkage()->get_inode();
  dn->get_dir()->unlink_inode(dn);
  straydn->pop_projected_linkage();
  dn->pop_projected_linkage();
  mdcache->adjust_subtree_after_rename(in, dn->get_dir(), true);

  MMDSSlaveRequest *reply = new MMDSSlaveRequest(mdr->reqid, mdr->attempt,
						 MMDSSlaveRequest::OP_RMDIRPREPACK);
  mds->send_message_mds(reply, mdr->slave_to_mds);

  // set up commit waiter
  mdr->more()->slave_commit = new C_MDS_SlaveRmdirCommit(this, mdr);

  // done.
  mdr->slave_request->put();
  mdr->slave_request = 0;
  mdr->straydn = 0;
}

void Server::handle_slave_rmdir_prep_ack(MDRequest *mdr, MMDSSlaveRequest *ack)
{
  dout(10) << "handle_slave_rmdir_prep_ack " << *mdr 
	   << " " << *ack << dendl;

  int from = ack->get_source().num();

  mdr->more()->slaves.insert(from);
  mdr->more()->witnessed.insert(from);

  // remove from waiting list
  assert(mdr->more()->waiting_on_slave.count(from));
  mdr->more()->waiting_on_slave.erase(from);

  if (mdr->more()->waiting_on_slave.empty())
    dispatch_client_request(mdr);  // go again!
  else 
    dout(10) << "still waiting on slaves " << mdr->more()->waiting_on_slave << dendl;
}

void Server::_commit_slave_rmdir(MDRequest *mdr, int r)
{
  dout(10) << "_commit_slave_rmdir " << *mdr << " r=" << r << dendl;
  
  if (r == 0) {
    // write a commit to the journal
    ESlaveUpdate *le = new ESlaveUpdate(mdlog, "slave_rmdir_commit", mdr->reqid, mdr->slave_to_mds,
					ESlaveUpdate::OP_COMMIT, ESlaveUpdate::RMDIR);
    mdlog->start_entry(le);
    mdr->cleanup();

    mdlog->submit_entry(le, new C_MDS_CommittedSlave(this, mdr));
    mdlog->flush();
  } else {
    // abort
    do_rmdir_rollback(mdr->more()->rollback_bl, mdr->slave_to_mds, mdr);
  }
}

struct C_MDS_LoggedRmdirRollback : public Context {
  Server *server;
  MDRequest *mdr;
  metareqid_t reqid;
  CDentry *dn;
  CDentry *straydn;
  C_MDS_LoggedRmdirRollback(Server *s, MDRequest *m, metareqid_t mr, CDentry *d, CDentry *st)
    : server(s), mdr(m), reqid(mr), dn(d), straydn(st) {}
  void finish(int r) {
    server->_rmdir_rollback_finish(mdr, reqid, dn, straydn);
  }
};

void Server::do_rmdir_rollback(bufferlist &rbl, int master, MDRequest *mdr)
{
  // unlink the other rollback methods, the rmdir rollback is only
  // needed to record the subtree changes in the journal for inode
  // replicas who are auth for empty dirfrags.  no actual changes to
  // the file system are taking place here, so there is no Mutation.

  rmdir_rollback rollback;
  bufferlist::iterator p = rbl.begin();
  ::decode(rollback, p);
  
  dout(10) << "do_rmdir_rollback on " << rollback.reqid << dendl;
  mds->mdcache->add_rollback(rollback.reqid, master); // need to finish this update before resolve finishes
  assert(mdr || mds->is_resolve());

  CDir *dir = mds->mdcache->get_dirfrag(rollback.src_dir);
  assert(dir);
  CDentry *dn = dir->lookup(rollback.src_dname);
  assert(dn);
  dout(10) << " dn " << *dn << dendl;
  dir = mds->mdcache->get_dirfrag(rollback.dest_dir);
  assert(dir);
  CDentry *straydn = dir->lookup(rollback.dest_dname);
  assert(straydn);
  dout(10) << " straydn " << *dn << dendl;
  CInode *in = straydn->get_linkage()->get_inode();

  dn->push_projected_linkage(in);
  straydn->push_projected_linkage();

  ESlaveUpdate *le = new ESlaveUpdate(mdlog, "slave_rmdir_rollback", rollback.reqid, master,
				      ESlaveUpdate::OP_ROLLBACK, ESlaveUpdate::RMDIR);
  mdlog->start_entry(le);
  
  le->commit.add_dir_context(dn->get_dir());
  le->commit.add_primary_dentry(dn, in, true);
  // slave: no need to journal straydn
  
  dout(10) << " noting renamed (unlinked) dir ino " << in->ino() << " in metablob" << dendl;
  le->commit.renamed_dirino = in->ino();

  mdcache->project_subtree_rename(in, straydn->get_dir(), dn->get_dir());

  mdlog->submit_entry(le, new C_MDS_LoggedRmdirRollback(this, mdr, rollback.reqid, dn, straydn));
  mdlog->flush();
}

void Server::_rmdir_rollback_finish(MDRequest *mdr, metareqid_t reqid, CDentry *dn, CDentry *straydn)
{
  dout(10) << "_rmdir_rollback_finish " << reqid << dendl;

  straydn->get_dir()->unlink_inode(straydn);
  dn->pop_projected_linkage();
  straydn->pop_projected_linkage();

  CInode *in = dn->get_linkage()->get_inode();
  mdcache->adjust_subtree_after_rename(in, straydn->get_dir(), true);
  if (mds->is_resolve()) {
    CDir *root = mdcache->get_subtree_root(straydn->get_dir());
    mdcache->try_trim_non_auth_subtree(root);
  }

  if (mdr)
    mds->mdcache->request_finish(mdr);

  mds->mdcache->finish_rollback(reqid);
}


/** _dir_is_nonempty[_unlocked]
 *
 * check if a directory is non-empty (i.e. we can rmdir it).
 *
 * the unlocked varient this is a fastpath check.  we can't really be
 * sure until we rdlock the filelock.
 */
bool Server::_dir_is_nonempty_unlocked(MDRequest *mdr, CInode *in)
{
  dout(10) << "dir_is_nonempty_unlocked " << *in << dendl;
  assert(in->is_auth());

  if (in->snaprealm && in->snaprealm->srnode.snaps.size())
    return true; // in a snapshot!

  list<frag_t> frags;
  in->dirfragtree.get_leaves(frags);

  for (list<frag_t>::iterator p = frags.begin();
       p != frags.end();
       ++p) {
    CDir *dir = in->get_or_open_dirfrag(mdcache, *p);
    assert(dir);

    // is the frag obviously non-empty?
    if (dir->is_auth()) {
      if (dir->get_projected_fnode()->fragstat.size()) {
	dout(10) << "dir_is_nonempty_unlocked dirstat has " 
		 << dir->get_projected_fnode()->fragstat.size() << " items " << *dir << dendl;
	return true;
      }
    }
  }

  return false;
}

bool Server::_dir_is_nonempty(MDRequest *mdr, CInode *in)
{
  dout(10) << "dir_is_nonempty " << *in << dendl;
  assert(in->is_auth());

  if (in->snaprealm && in->snaprealm->srnode.snaps.size())
    return true; // in a snapshot!

  if (in->get_projected_inode()->dirstat.size() > 0) {	
    dout(10) << "dir_is_nonempty projected dir size still "
	     << in->get_projected_inode()->dirstat.size()
	     << " on " << *in
	     << dendl;
    return true;
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
 *
 * This function takes responsibility for the passed mdr.
 */
void Server::handle_client_rename(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;
  dout(7) << "handle_client_rename " << *req << dendl;

  filepath destpath = req->get_filepath();
  filepath srcpath = req->get_filepath2();
  if (destpath.depth() == 0 || srcpath.depth() == 0) {
    reply_request(mdr, -EINVAL);
    return;
  }
  const string &destname = destpath.last_dentry();

  vector<CDentry*>& srctrace = mdr->dn[1];
  vector<CDentry*>& desttrace = mdr->dn[0];

  set<SimpleLock*> rdlocks, wrlocks, xlocks;

  CDentry *destdn = rdlock_path_xlock_dentry(mdr, 0, rdlocks, wrlocks, xlocks, true, false, true);
  if (!destdn) return;
  dout(10) << " destdn " << *destdn << dendl;
  if (mdr->snapid != CEPH_NOSNAP) {
    reply_request(mdr, -EROFS);
    return;
  }
  CDentry::linkage_t *destdnl = destdn->get_projected_linkage();
  CDir *destdir = destdn->get_dir();
  assert(destdir->is_auth());

  int r = mdcache->path_traverse(mdr, NULL, NULL, srcpath, &srctrace, NULL, MDS_TRAVERSE_DISCOVER);
  if (r > 0)
    return; // delayed
  if (r < 0) {
    if (r == -ESTALE) {
      dout(10) << "FAIL on ESTALE but attempting recovery" << dendl;
      Context *c = new C_MDS_TryFindInode(this, mdr);
      mdcache->find_ino_peers(srcpath.get_ino(), c);
    } else {
      dout(10) << "FAIL on error " << r << dendl;
      reply_request(mdr, r);
    }
    return;

  }
  assert(!srctrace.empty());
  CDentry *srcdn = srctrace[srctrace.size()-1];
  dout(10) << " srcdn " << *srcdn << dendl;
  if (srcdn->last != CEPH_NOSNAP) {
    reply_request(mdr, -EROFS);
    return;
  }
  CDentry::linkage_t *srcdnl = srcdn->get_projected_linkage();
  CInode *srci = srcdnl->get_inode();
  dout(10) << " srci " << *srci << dendl;

  CInode *oldin = 0;
  if (!destdnl->is_null()) {
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
    if (oldin->is_dir() && _dir_is_nonempty_unlocked(mdr, oldin)) {
      reply_request(mdr, -ENOTEMPTY);
      return;
    }
    if (srci == oldin && !srcdn->get_dir()->inode->is_stray()) {
      reply_request(mdr, 0);  // no-op.  POSIX makes no sense.
      return;
    }
  }

  // -- some sanity checks --

  // src+dest traces _must_ share a common ancestor for locking to prevent orphans
  if (destpath.get_ino() != srcpath.get_ino() &&
      !(req->get_source().is_mds() &&
	MDS_INO_IS_MDSDIR(srcpath.get_ino()))) {  // <-- mds 'rename' out of stray dir is ok!
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
	rdlocks.insert(&desttrace[0]->lock);
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

  // is this a stray migration, reintegration or merge? (sanity checks!)
  if (mdr->reqid.name.is_mds() &&
      !(MDS_INO_IS_MDSDIR(srcpath.get_ino()) &&
	MDS_INO_IS_STRAY(destpath.get_ino())) &&
      !(destdnl->is_remote() &&
	destdnl->get_remote_ino() == srci->ino())) {
    reply_request(mdr, -EINVAL);  // actually, this won't reply, but whatev.
    return;
  }

  bool linkmerge = (srcdnl->get_inode() == destdnl->get_inode() &&
		    (srcdnl->is_primary() || destdnl->is_primary()));
  if (linkmerge)
    dout(10) << " this is a link merge" << dendl;

  // -- create stray dentry? --
  CDentry *straydn = NULL;
  if (destdnl->is_primary() && !linkmerge) {
    straydn = prepare_stray_dentry(mdr, destdnl->get_inode());
    dout(10) << " straydn is " << *straydn << dendl;
  } else if (mdr->straydn) {
    mdr->unpin(mdr->straydn);
    mdr->straydn = NULL;
  }

  // -- prepare witness list --
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


  // -- locks --
  map<SimpleLock*, int> remote_wrlocks;

  // srctrace items.  this mirrors locks taken in rdlock_path_xlock_dentry
  for (int i=0; i<(int)srctrace.size(); i++) 
    rdlocks.insert(&srctrace[i]->lock);
  xlocks.insert(&srcdn->lock);
  int srcdirauth = srcdn->get_dir()->authority().first;
  if (srcdirauth != mds->whoami) {
    dout(10) << " will remote_wrlock srcdir scatterlocks on mds." << srcdirauth << dendl;
    remote_wrlocks[&srcdn->get_dir()->inode->filelock] = srcdirauth;
    remote_wrlocks[&srcdn->get_dir()->inode->nestlock] = srcdirauth;
  } else {
    wrlocks.insert(&srcdn->get_dir()->inode->filelock);
    wrlocks.insert(&srcdn->get_dir()->inode->nestlock);
  }
  mds->locker->include_snap_rdlocks(rdlocks, srcdn->get_dir()->inode);

  // straydn?
  if (straydn) {
    wrlocks.insert(&straydn->get_dir()->inode->filelock);
    wrlocks.insert(&straydn->get_dir()->inode->nestlock);
    xlocks.insert(&straydn->lock);
  }

  // xlock versionlock on dentries if there are witnesses.
  //  replicas can't see projected dentry linkages, and will get
  //  confused if we try to pipeline things.
  if (!witnesses.empty()) {
    // take xlock on all projected ancestor dentries for srcdn and destdn.
    // this ensures the srcdn and destdn can be traversed to by the witnesses.
    for (int i= 0; i<(int)srctrace.size(); i++) {
      if (srctrace[i]->is_auth() && srctrace[i]->is_projected())
	  xlocks.insert(&srctrace[i]->versionlock);
    }
    for (int i=0; i<(int)desttrace.size(); i++) {
      if (desttrace[i]->is_auth() && desttrace[i]->is_projected())
	  xlocks.insert(&desttrace[i]->versionlock);
    }
    // xlock srci and oldin's primary dentries, so witnesses can call
    // open_remote_ino() with 'want_locked=true' when the srcdn or destdn
    // is traversed.
    if (srcdnl->is_remote())
      xlocks.insert(&srci->get_parent_dn()->lock);
    if (destdnl->is_remote())
      xlocks.insert(&oldin->get_parent_dn()->lock);
  }

  // we need to update srci's ctime.  xlock its least contended lock to do that...
  xlocks.insert(&srci->linklock);

  // xlock oldin (for nlink--)
  if (oldin) {
    xlocks.insert(&oldin->linklock);
    if (oldin->is_dir())
      rdlocks.insert(&oldin->filelock);
  }
  if (srcdnl->is_primary() && srci->is_dir())  
    // FIXME: this should happen whenever we are renamning between
    // realms, regardless of the file type
    // FIXME: If/when this changes, make sure to update the
    // "allowance" in handle_slave_rename_prep
    xlocks.insert(&srci->snaplock);  // FIXME: an auth bcast could be sufficient?
  else
    rdlocks.insert(&srci->snaplock);

  // take any locks needed for anchor creation/verification
  mds->mdcache->anchor_create_prep_locks(mdr, srci, rdlocks, xlocks);

  CInode *auth_pin_freeze = !srcdn->is_auth() && srcdnl->is_primary() ? srci : NULL;
  if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks,
				  &remote_wrlocks, auth_pin_freeze))
    return;

  if (oldin &&
      oldin->is_dir() &&
      _dir_is_nonempty(mdr, oldin)) {
    reply_request(mdr, -ENOTEMPTY);
    return;
  }

  // moving between snaprealms?
  if (srcdnl->is_primary() && srci->is_multiversion() && !srci->snaprealm) {
    SnapRealm *srcrealm = srci->find_snaprealm();
    SnapRealm *destrealm = destdn->get_dir()->inode->find_snaprealm();
    if (srcrealm != destrealm &&
	(srcrealm->get_newest_seq() + 1 > srcdn->first ||
	 destrealm->get_newest_seq() + 1 > srcdn->first)) {
      dout(10) << " renaming between snaprealms, creating snaprealm for " << *srci << dendl;
      mds->mdcache->snaprealm_create(mdr, srci);
      return;
    }
  }

  assert(g_conf->mds_kill_rename_at != 1);

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
    mdr->now = ceph_clock_now(g_ceph_context);

  // -- prepare anchor updates --
  if (!linkmerge || srcdnl->is_primary()) {
    C_GatherBuilder anchorgather(g_ceph_context);

    if (srcdnl->is_primary() &&
      (srcdnl->get_inode()->is_anchored() ||
       (srcdnl->get_inode()->is_dir() && (srcdnl->get_inode()->inode.rstat.ranchors ||
                                          srcdnl->get_inode()->nested_anchors ||
                                          !mdcache->is_leaf_subtree(mdcache->get_projected_subtree_root(srcdn->get_dir()))))) &&
      !mdr->more()->src_reanchor_atid) {
      dout(10) << "reanchoring src->dst " << *srcdnl->get_inode() << dendl;
      vector<Anchor> trace;
      destdn->make_anchor_trace(trace, srcdnl->get_inode());
      mds->anchorclient->prepare_update(srcdnl->get_inode()->ino(),
					trace, &mdr->more()->src_reanchor_atid,
					anchorgather.new_sub());
    }
    if (destdnl->is_primary() &&
	destdnl->get_inode()->is_anchored() &&
	!mdr->more()->dst_reanchor_atid) {
      dout(10) << "reanchoring dst->stray " << *destdnl->get_inode() << dendl;

      assert(straydn);
      vector<Anchor> trace;
      straydn->make_anchor_trace(trace, destdnl->get_inode());

      mds->anchorclient->prepare_update(destdnl->get_inode()->ino(), trace,
		  &mdr->more()->dst_reanchor_atid, anchorgather.new_sub());
    }

    if (anchorgather.has_subs())  {
      anchorgather.set_finisher(new C_MDS_RetryRequest(mdcache, mdr));
      anchorgather.activate();
      return;  // waiting for anchor prepares
    }

    assert(g_conf->mds_kill_rename_at != 2);
  }

  // -- prepare witnesses --

  // do srcdn auth last
  int last = -1;
  if (!srcdn->is_auth()) {
    last = srcdn->authority().first;
    mdr->more()->srcdn_auth_mds = last;
    // ask auth of srci to mark srci as ambiguous auth if more than two MDS
    // are involved in the rename operation.
    if (srcdnl->is_primary() && !mdr->more()->is_ambiguous_auth) {
      dout(10) << " preparing ambiguous auth for srci" << dendl;
      mdr->set_ambiguous_auth(srci);
      _rename_prepare_witness(mdr, last, witnesses, srcdn, destdn, straydn);
      return;
    }
  }
  
  for (set<int>::iterator p = witnesses.begin();
       p != witnesses.end();
       ++p) {
    if (*p == last) continue;  // do it last!
    if (mdr->more()->witnessed.count(*p)) {
      dout(10) << " already witnessed by mds." << *p << dendl;
    } else if (mdr->more()->waiting_on_slave.count(*p)) {
      dout(10) << " already waiting on witness mds." << *p << dendl;      
    } else {
      if (!_rename_prepare_witness(mdr, *p, witnesses, srcdn, destdn, straydn))
	return;
    }
  }
  if (!mdr->more()->waiting_on_slave.empty())
    return;  // we're waiting for a witness.

  if (last >= 0 && mdr->more()->witnessed.count(last) == 0) {
    dout(10) << " preparing last witness (srcdn auth)" << dendl;
    assert(mdr->more()->waiting_on_slave.count(last) == 0);
    _rename_prepare_witness(mdr, last, witnesses, srcdn, destdn, straydn);
    return;
  }

  // test hack: bail after slave does prepare, so we can verify it's _live_ rollback.
  if (!mdr->more()->slaves.empty() && !srci->is_dir())
    assert(g_conf->mds_kill_rename_at != 3);
  if (!mdr->more()->slaves.empty() && srci->is_dir())
    assert(g_conf->mds_kill_rename_at != 4);

  // -- prepare journal entry --
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "rename");
  mdlog->start_entry(le);
  le->metablob.add_client_req(mdr->reqid, mdr->client_request->get_oldest_client_tid());
  if (!mdr->more()->witnessed.empty()) {
    dout(20) << " noting uncommitted_slaves " << mdr->more()->witnessed << dendl;
    
    le->reqid = mdr->reqid;
    le->had_slaves = true;
    
    mds->mdcache->add_uncommitted_master(mdr->reqid, mdr->ls, mdr->more()->witnessed);
    // no need to send frozen auth pin to recovring auth MDS of srci
    mdr->more()->is_remote_frozen_authpin = false;
  }
  
  _rename_prepare(mdr, &le->metablob, &le->client_map, srcdn, destdn, straydn);
  if (le->client_map.length())
    le->cmapv = mds->sessionmap.projected;

  // -- commit locally --
  C_MDS_rename_finish *fin = new C_MDS_rename_finish(mds, mdr, srcdn, destdn, straydn);

  journal_and_reply(mdr, srci, destdn, le, fin);
}


void Server::_rename_finish(MDRequest *mdr, CDentry *srcdn, CDentry *destdn, CDentry *straydn)
{
  dout(10) << "_rename_finish " << *mdr << dendl;

  if (!mdr->more()->witnessed.empty())
    mdcache->logged_master_update(mdr->reqid);

  // apply
  _rename_apply(mdr, srcdn, destdn, straydn);

  CDentry::linkage_t *destdnl = destdn->get_linkage();
  // test hack: test slave commit
  if (!mdr->more()->slaves.empty() && !destdnl->get_inode()->is_dir())
    assert(g_conf->mds_kill_rename_at != 5);
  if (!mdr->more()->slaves.empty() && destdnl->get_inode()->is_dir())
    assert(g_conf->mds_kill_rename_at != 6);
  
  // commit anchor updates?
  if (mdr->more()->src_reanchor_atid) 
    mds->anchorclient->commit(mdr->more()->src_reanchor_atid, mdr->ls);
  if (mdr->more()->dst_reanchor_atid) 
    mds->anchorclient->commit(mdr->more()->dst_reanchor_atid, mdr->ls);

  // bump popularity
  mds->balancer->hit_dir(mdr->now, srcdn->get_dir(), META_POP_IWR);
  if (destdnl->is_remote() &&
      destdnl->get_inode()->is_auth())
    mds->balancer->hit_inode(mdr->now, destdnl->get_inode(), META_POP_IWR);

  // did we import srci?  if so, explicitly ack that import that, before we unlock and reply.

  assert(g_conf->mds_kill_rename_at != 7);

  // reply
  MClientReply *reply = new MClientReply(mdr->client_request, 0);
  reply_request(mdr, reply);
  
  // clean up?
  if (straydn) 
    mdcache->eval_stray(straydn);
}



// helpers

bool Server::_rename_prepare_witness(MDRequest *mdr, int who, set<int> &witnesse,
				     CDentry *srcdn, CDentry *destdn, CDentry *straydn)
{
  if (!mds->mdsmap->is_clientreplay_or_active_or_stopping(who)) {
    dout(10) << "_rename_prepare_witness mds." << who << " is not active" << dendl;
    if (mdr->more()->waiting_on_slave.empty())
      mds->wait_for_active_peer(who, new C_MDS_RetryRequest(mdcache, mdr));
    return false;
  }

  dout(10) << "_rename_prepare_witness mds." << who << dendl;
  MMDSSlaveRequest *req = new MMDSSlaveRequest(mdr->reqid, mdr->attempt,
					       MMDSSlaveRequest::OP_RENAMEPREP);
  srcdn->make_path(req->srcdnpath);
  destdn->make_path(req->destdnpath);
  req->now = mdr->now;
  
  if (straydn)
    mdcache->replicate_stray(straydn, who, req->stray);
  
  // srcdn auth will verify our current witness list is sufficient
  req->witnesses = witnesse;

  mds->send_message_mds(req, who);
  
  assert(mdr->more()->waiting_on_slave.count(who) == 0);
  mdr->more()->waiting_on_slave.insert(who);
  return true;
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
  prepare_force_open_sessions(mdr->more()->imported_client_map, mdr->more()->sseq_map);

  list<ScatterLock*> updated_scatterlocks;  // we clear_updated explicitly below
  mdcache->migrator->decode_import_inode(srcdn, blp, 
					 srcdn->authority().first,
					 mdr->ls, 0,
					 mdr->more()->cap_imports, updated_scatterlocks);
  srcdnl->get_inode()->filelock.remove_dirty();  
  srcdnl->get_inode()->nestlock.remove_dirty();  

  // hack: force back to !auth and clean, temporarily
  srcdnl->get_inode()->state_clear(CInode::STATE_AUTH);
  srcdnl->get_inode()->mark_clean();

  return oldpv;
}

bool Server::_need_force_journal(CInode *diri, bool empty)
{
  list<CDir*> ls;
  diri->get_dirfrags(ls);

  bool force_journal = false;
  if (empty) {
    for (list<CDir*>::iterator p = ls.begin(); p != ls.end(); ++p) {
      if ((*p)->is_subtree_root() && (*p)->get_dir_auth().first == mds->whoami) {
	dout(10) << " frag " << (*p)->get_frag() << " is auth subtree dirfrag, will force journal" << dendl;
	force_journal = true;
	break;
      } else
	dout(20) << " frag " << (*p)->get_frag() << " is not auth subtree dirfrag" << dendl;
    }
  } else {
    // see if any children of our frags are auth subtrees.
    list<CDir*> subtrees;
    mds->mdcache->list_subtrees(subtrees);
    dout(10) << " subtrees " << subtrees << " frags " << ls << dendl;
    for (list<CDir*>::iterator p = ls.begin(); p != ls.end(); ++p) {
      CDir *dir = *p;
      for (list<CDir*>::iterator q = subtrees.begin(); q != subtrees.end(); ++q) {
	if (dir->contains(*q)) {
	  if ((*q)->get_dir_auth().first == mds->whoami) {
	    dout(10) << " frag " << (*p)->get_frag() << " contains (maybe) auth subtree, will force journal "
		     << **q << dendl;
	    force_journal = true;
	    break;
	  } else
	    dout(20) << " frag " << (*p)->get_frag() << " contains but isn't auth for " << **q << dendl;
	} else
	  dout(20) << " frag " << (*p)->get_frag() << " does not contain " << **q << dendl;
      }
      if (force_journal)
	break;
    }
  }
  return force_journal;
}

void Server::_rename_prepare(MDRequest *mdr,
			     EMetaBlob *metablob, bufferlist *client_map_bl,
			     CDentry *srcdn, CDentry *destdn, CDentry *straydn)
{
  dout(10) << "_rename_prepare " << *mdr << " " << *srcdn << " " << *destdn << dendl;
  if (straydn)
    dout(10) << " straydn " << *straydn << dendl;

  CDentry::linkage_t *srcdnl = srcdn->get_projected_linkage();
  CDentry::linkage_t *destdnl = destdn->get_projected_linkage();
  CInode *srci = srcdnl->get_inode();
  CInode *oldin = destdnl->get_inode();

  // primary+remote link merge?
  bool linkmerge = (srci == destdnl->get_inode() &&
		    (srcdnl->is_primary() || destdnl->is_primary()));
  bool silent = srcdn->get_dir()->inode->is_stray();

  bool force_journal_dest = false;
  if (srci->is_dir() && !destdn->is_auth()) {
    if (srci->is_auth()) {
      // if we are auth for srci and exporting it, force journal because journal replay needs
      // the source inode to create auth subtrees.
      dout(10) << " we are exporting srci, will force journal destdn" << dendl;
      force_journal_dest = true;
    } else
      force_journal_dest = _need_force_journal(srci, false);
  }

  bool force_journal_stray = false;
  if (oldin && oldin->is_dir() && !straydn->is_auth())
    force_journal_stray = _need_force_journal(oldin, true);

  if (linkmerge)
    dout(10) << " merging remote and primary links to the same inode" << dendl;
  if (silent)
    dout(10) << " reintegrating stray; will avoid changing nlink or dir mtime" << dendl;
  if (force_journal_dest)
    dout(10) << " forcing journal destdn because we (will) have auth subtrees nested beneath it" << dendl;
  if (force_journal_stray)
    dout(10) << " forcing journal straydn because we (will) have auth subtrees nested beneath it" << dendl;

  if (srci->is_dir() && (destdn->is_auth() || force_journal_dest)) {
    dout(10) << " noting renamed dir ino " << srci->ino() << " in metablob" << dendl;
    metablob->renamed_dirino = srci->ino();
  } else if (oldin && oldin->is_dir() && force_journal_stray) {
    dout(10) << " noting rename target dir " << oldin->ino() << " in metablob" << dendl;
    metablob->renamed_dirino = oldin->ino();
  }

  // prepare
  inode_t *pi = 0;    // renamed inode
  inode_t *tpi = 0;  // target/overwritten inode
  
  // target inode
  if (!linkmerge) {
    if (destdnl->is_primary()) {
      assert(straydn);  // moving to straydn.
      // link--, and move.
      if (destdn->is_auth()) {
	tpi = oldin->project_inode(); //project_snaprealm
	tpi->version = straydn->pre_dirty(tpi->version);
	tpi->update_backtrace();
      }
      straydn->push_projected_linkage(oldin);
    } else if (destdnl->is_remote()) {
      // nlink-- targeti
      if (oldin->is_auth()) {
	tpi = oldin->project_inode();
	tpi->version = oldin->pre_dirty();
      }
    }
  }

  // dest
  if (srcdnl->is_remote()) {
    if (!linkmerge) {
      // destdn
      if (destdn->is_auth())
	mdr->more()->pvmap[destdn] = destdn->pre_dirty();
      destdn->push_projected_linkage(srcdnl->get_remote_ino(), srcdnl->get_remote_d_type());
      // srci
      if (srci->is_auth()) {
	pi = srci->project_inode();
	pi->version = srci->pre_dirty();
      }
    } else {
      dout(10) << " will merge remote onto primary link" << dendl;
      if (destdn->is_auth()) {
	pi = oldin->project_inode();
	pi->version = mdr->more()->pvmap[destdn] = destdn->pre_dirty(oldin->inode.version);
      }
    }
  } else { // primary
    if (destdn->is_auth()) {
      version_t oldpv;
      if (srcdn->is_auth())
	oldpv = srci->get_projected_version();
      else {
	oldpv = _rename_prepare_import(mdr, srcdn, client_map_bl);

	// note which dirfrags have child subtrees in the journal
	// event, so that we can open those (as bounds) during replay.
	if (srci->is_dir()) {
	  list<CDir*> ls;
	  srci->get_dirfrags(ls);
	  for (list<CDir*>::iterator p = ls.begin(); p != ls.end(); ++p) {
	    CDir *dir = *p;
	    if (!dir->is_auth())
	      metablob->renamed_dir_frags.push_back(dir->get_frag());
	  }
	  dout(10) << " noting renamed dir open frags " << metablob->renamed_dir_frags << dendl;
	}
      }
      pi = srci->project_inode(); // project snaprealm if srcdnl->is_primary
                                                 // & srcdnl->snaprealm
      pi->version = mdr->more()->pvmap[destdn] = destdn->pre_dirty(oldpv);
      pi->update_backtrace();
    }
    destdn->push_projected_linkage(srci);
  }

  // src
  if (srcdn->is_auth())
    mdr->more()->pvmap[srcdn] = srcdn->pre_dirty();
  srcdn->push_projected_linkage();  // push null linkage

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
  
  // guarantee stray dir is processed first during journal replay. unlink the old inode,
  // then link the source inode to destdn
  if (destdnl->is_primary()) {
    assert(straydn);
    if (straydn->is_auth()) {
      metablob->add_dir_context(straydn->get_dir());
      metablob->add_dir(straydn->get_dir(), true);
    }
  }

  // sub off target
  if (destdn->is_auth() && !destdnl->is_null()) {
    mdcache->predirty_journal_parents(mdr, metablob, oldin, destdn->get_dir(),
				      (destdnl->is_primary() ? PREDIRTY_PRIMARY:0)|predirty_dir, -1);
    if (destdnl->is_primary())
      mdcache->predirty_journal_parents(mdr, metablob, oldin, straydn->get_dir(),
					PREDIRTY_PRIMARY|PREDIRTY_DIR, 1);
  }
  
  // move srcdn
  int predirty_primary = (srcdnl->is_primary() && srcdn->get_dir() != destdn->get_dir()) ? PREDIRTY_PRIMARY:0;
  int flags = predirty_dir | predirty_primary;
  if (srcdn->is_auth())
    mdcache->predirty_journal_parents(mdr, metablob, srci, srcdn->get_dir(), PREDIRTY_SHALLOW|flags, -1);
  if (destdn->is_auth())
    mdcache->predirty_journal_parents(mdr, metablob, srci, destdn->get_dir(), flags, 1);

  SnapRealm *src_realm = srci->find_snaprealm();
  SnapRealm *dest_realm = destdn->get_dir()->inode->find_snaprealm();
  snapid_t next_dest_snap = dest_realm->get_newest_seq() + 1;

  // add it all to the metablob
  // target inode
  if (!linkmerge) {
    if (destdnl->is_primary()) {
      if (destdn->is_auth()) {
	// project snaprealm, too
	if (oldin->snaprealm || src_realm->get_newest_seq() + 1 > srcdn->first)
	  oldin->project_past_snaprealm_parent(straydn->get_dir()->inode->find_snaprealm());
	straydn->first = MAX(oldin->first, next_dest_snap);
	metablob->add_primary_dentry(straydn, oldin, true, true);
      } else if (force_journal_stray) {
	dout(10) << " forced journaling straydn " << *straydn << dendl;
	metablob->add_dir_context(straydn->get_dir());
	metablob->add_primary_dentry(straydn, oldin, true);
      }
    } else if (destdnl->is_remote()) {
      if (oldin->is_auth()) {
	// auth for targeti
	metablob->add_dir_context(oldin->get_projected_parent_dir());
	mdcache->journal_cow_dentry(mdr, metablob, oldin->get_projected_parent_dn(),
				    CEPH_NOSNAP, 0, destdnl);
	metablob->add_primary_dentry(oldin->get_projected_parent_dn(), oldin, true);
      }
    }
  }

  // dest
  if (srcdnl->is_remote()) {
    if (!linkmerge) {
      if (destdn->is_auth() && !destdnl->is_null())
	mdcache->journal_cow_dentry(mdr, metablob, destdn, CEPH_NOSNAP, 0, destdnl);
      else
	destdn->first = MAX(destdn->first, next_dest_snap);

      if (destdn->is_auth())
        metablob->add_remote_dentry(destdn, true, srcdnl->get_remote_ino(), srcdnl->get_remote_d_type());
      if (srci->get_projected_parent_dn()->is_auth()) { // it's remote
	metablob->add_dir_context(srci->get_projected_parent_dir());
        mdcache->journal_cow_dentry(mdr, metablob, srci->get_projected_parent_dn(), CEPH_NOSNAP, 0, srcdnl);
	metablob->add_primary_dentry(srci->get_projected_parent_dn(), srci, true);
      }
    } else {
      if (destdn->is_auth() && !destdnl->is_null())
	mdcache->journal_cow_dentry(mdr, metablob, destdn, CEPH_NOSNAP, 0, destdnl);
      else
	destdn->first = MAX(destdn->first, next_dest_snap);

      if (destdn->is_auth())
        metablob->add_primary_dentry(destdn, destdnl->get_inode(), true, true);
    }
  } else if (srcdnl->is_primary()) {
    // project snap parent update?
    if (destdn->is_auth() &&
        (srci->snaprealm || src_realm->get_newest_seq() + 1 > srcdn->first))
      srci->project_past_snaprealm_parent(dest_realm);
    
    if (destdn->is_auth() && !destdnl->is_null())
      mdcache->journal_cow_dentry(mdr, metablob, destdn, CEPH_NOSNAP, 0, destdnl);
    else
      destdn->first = MAX(destdn->first, next_dest_snap);

    if (destdn->is_auth())
      metablob->add_primary_dentry(destdn, srci, true, true);
    else if (force_journal_dest) {
      dout(10) << " forced journaling destdn " << *destdn << dendl;
      metablob->add_dir_context(destdn->get_dir());
      metablob->add_primary_dentry(destdn, srci, true);
      if (srcdn->is_auth() && srci->is_dir()) {
	// journal new subtrees root dirfrags
	list<CDir*> ls;
	srci->get_dirfrags(ls);
	for (list<CDir*>::iterator p = ls.begin(); p != ls.end(); ++p) {
	  CDir *dir = *p;
	  if (dir->is_auth())
	    metablob->add_dir(dir, true);
	}
      }
    }
  }
    
  // src
  if (srcdn->is_auth()) {
    dout(10) << " journaling srcdn " << *srcdn << dendl;
    mdcache->journal_cow_dentry(mdr, metablob, srcdn, CEPH_NOSNAP, 0, srcdnl);
    // also journal the inode in case we need do slave rename rollback. It is Ok to add
    // both primary and NULL dentries. Because during journal replay, null dentry is
    // processed after primary dentry.
    if (srcdnl->is_primary() && !srci->is_dir() && !destdn->is_auth())
      metablob->add_primary_dentry(srcdn, srci, true);
    metablob->add_null_dentry(srcdn, true);
  } else
    dout(10) << " NOT journaling srcdn " << *srcdn << dendl;

  // make renamed inode first track the dn
  if (srcdnl->is_primary() && destdn->is_auth())
    srci->first = destdn->first;  

  // anchor updates?
  if (mdr->more()->src_reanchor_atid)
    metablob->add_table_transaction(TABLE_ANCHOR, mdr->more()->src_reanchor_atid);
  if (mdr->more()->dst_reanchor_atid)
    metablob->add_table_transaction(TABLE_ANCHOR, mdr->more()->dst_reanchor_atid);

  if (oldin && oldin->is_dir())
    mdcache->project_subtree_rename(oldin, destdn->get_dir(), straydn->get_dir());
  if (srci->is_dir())
    mdcache->project_subtree_rename(srci, srcdn->get_dir(), destdn->get_dir());

}


void Server::_rename_apply(MDRequest *mdr, CDentry *srcdn, CDentry *destdn, CDentry *straydn)
{
  dout(10) << "_rename_apply " << *mdr << " " << *srcdn << " " << *destdn << dendl;
  dout(10) << " pvs " << mdr->more()->pvmap << dendl;

  CDentry::linkage_t *srcdnl = srcdn->get_linkage();
  CDentry::linkage_t *destdnl = destdn->get_linkage();

  CInode *oldin = destdnl->get_inode();
  
  bool imported_inode = false;

  // primary+remote link merge?
  bool linkmerge = (srcdnl->get_inode() == destdnl->get_inode() &&
		    (srcdnl->is_primary() || destdnl->is_primary()));

  // target inode
  if (!linkmerge) {
    if (destdnl->is_primary()) {
      assert(straydn);
      dout(10) << "straydn is " << *straydn << dendl;
      destdn->get_dir()->unlink_inode(destdn);

      straydn->pop_projected_linkage();
      mdcache->touch_dentry_bottom(straydn);  // drop dn as quickly as possible.

      // nlink-- targeti
      if (destdn->is_auth()) {
	bool hadrealm = (oldin->snaprealm ? true : false);
	oldin->pop_and_dirty_projected_inode(mdr->ls);
	if (oldin->snaprealm && !hadrealm)
	  mdcache->do_realm_invalidate_and_update_notify(oldin, CEPH_SNAP_OP_SPLIT);
      } else {
	// FIXME this snaprealm is not filled out correctly
	//oldin->open_snaprealm();  might be sufficient..	
      }
    } else if (destdnl->is_remote()) {
      destdn->get_dir()->unlink_inode(destdn);
      if (oldin->is_auth())
  	oldin->pop_and_dirty_projected_inode(mdr->ls);
    }
  }

  // unlink src before we relink it at dest
  CInode *in = srcdnl->get_inode();
  assert(in);

  bool srcdn_was_remote = srcdnl->is_remote();
  srcdn->get_dir()->unlink_inode(srcdn);

  // dest
  if (srcdn_was_remote) {
    if (!linkmerge) {
      // destdn
      destdnl = destdn->pop_projected_linkage();

      destdn->link_remote(destdnl, in);
      if (destdn->is_auth())
	destdn->mark_dirty(mdr->more()->pvmap[destdn], mdr->ls);
      // in
      if (in->is_auth())
	in->pop_and_dirty_projected_inode(mdr->ls);
    } else {
      dout(10) << "merging remote onto primary link" << dendl;
      oldin->pop_and_dirty_projected_inode(mdr->ls);
    }
  } else { // primary
    if (linkmerge) {
      dout(10) << "merging primary onto remote link" << dendl;
      destdn->get_dir()->unlink_inode(destdn);
    }
    destdnl = destdn->pop_projected_linkage();

    // srcdn inode import?
    if (!srcdn->is_auth() && destdn->is_auth()) {
      assert(mdr->more()->inode_import.length() > 0);
      
      // finish cap imports
      finish_force_open_sessions(mdr->more()->imported_client_map, mdr->more()->sseq_map);
      if (mdr->more()->cap_imports.count(destdnl->get_inode())) {
	mds->mdcache->migrator->finish_import_inode_caps(destdnl->get_inode(), srcdn->authority().first, 
							 mdr->more()->cap_imports[destdnl->get_inode()]);
      }
      /* hack: add an auth pin for each xlock we hold. These were
       * remote xlocks previously but now they're local and
       * we're going to try and unpin when we xlock_finish. */
      for (set<SimpleLock *>::iterator i = mdr->xlocks.begin();
	  i !=  mdr->xlocks.end();
	  ++i)
	if ((*i)->get_parent() == destdnl->get_inode() &&
	    !(*i)->is_locallock())
	  mds->locker->xlock_import(*i, mdr);
      
      // hack: fix auth bit
      in->state_set(CInode::STATE_AUTH);
      imported_inode = true;

      mdr->clear_ambiguous_auth();
    }

    if (destdn->is_auth()) {
      in->pop_and_dirty_projected_inode(mdr->ls);

    } else {
      // FIXME: fix up snaprealm!
    }
  }

  // src
  if (srcdn->is_auth())
    srcdn->mark_dirty(mdr->more()->pvmap[srcdn], mdr->ls);
  srcdn->pop_projected_linkage();
  
  // apply remaining projected inodes (nested)
  mdr->apply();

  // update subtree map?
  if (destdnl->is_primary() && in->is_dir()) 
    mdcache->adjust_subtree_after_rename(in, srcdn->get_dir(), true, imported_inode);

  if (straydn && oldin->is_dir())
    mdcache->adjust_subtree_after_rename(oldin, destdn->get_dir(), true);

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

/* This function DOES put the mdr->slave_request before returning*/
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
  int r = mdcache->path_traverse(mdr, NULL, NULL, destpath, &trace, NULL, MDS_TRAVERSE_DISCOVERXLOCK);
  if (r > 0) return;
  assert(r == 0);  // we shouldn't get an error here!
      
  CDentry *destdn = trace[trace.size()-1];
  CDentry::linkage_t *destdnl = destdn->get_projected_linkage();
  dout(10) << " destdn " << *destdn << dendl;
  mdr->pin(destdn);
  
  // discover srcdn
  filepath srcpath(mdr->slave_request->srcdnpath);
  dout(10) << " src " << srcpath << dendl;
  CInode *srci;
  r = mdcache->path_traverse(mdr, NULL, NULL, srcpath, &trace, &srci, MDS_TRAVERSE_DISCOVERXLOCK);
  if (r > 0) return;
  assert(r == 0);
      
  CDentry *srcdn = trace[trace.size()-1];
  CDentry::linkage_t *srcdnl = srcdn->get_projected_linkage();
  dout(10) << " srcdn " << *srcdn << dendl;
  mdr->pin(srcdn);
  mdr->pin(srci);

  // stray?
  bool linkmerge = (srcdnl->get_inode() == destdnl->get_inode() &&
		    (srcdnl->is_primary() || destdnl->is_primary()));
  CDentry *straydn = mdr->straydn;
  if (destdnl->is_primary() && !linkmerge)
    assert(straydn);

  mdr->now = mdr->slave_request->now;
  mdr->more()->srcdn_auth_mds = srcdn->authority().first;

  // set up commit waiter (early, to clean up any freezing etc we do)
  if (!mdr->more()->slave_commit)
    mdr->more()->slave_commit = new C_MDS_SlaveRenameCommit(this, mdr, srcdn, destdn, straydn);

  // am i srcdn auth?
  if (srcdn->is_auth()) {
    bool reply_witness = false;
    if (srcdnl->is_primary() && !srcdnl->get_inode()->state_test(CInode::STATE_AMBIGUOUSAUTH)) {
      // freeze?
      // we need this to
      //  - avoid conflicting lock state changes
      //  - avoid concurrent updates to the inode
      //     (this could also be accomplished with the versionlock)
      int allowance = 2; // 1 for the mdr auth_pin, 1 for the link lock
      allowance += srcdnl->get_inode()->is_dir(); // for the snap lock
      dout(10) << " freezing srci " << *srcdnl->get_inode() << " with allowance " << allowance << dendl;
      bool frozen_inode = srcdnl->get_inode()->freeze_inode(allowance);

      // unfreeze auth pin after freezing the inode to avoid queueing waiters
      if (srcdnl->get_inode()->is_frozen_auth_pin())
	mdr->unfreeze_auth_pin();

      if (!frozen_inode) {
	srcdnl->get_inode()->add_waiter(CInode::WAIT_FROZEN, new C_MDS_RetryRequest(mdcache, mdr));
	return;
      }

      /*
       * set ambiguous auth for srci
       * NOTE: we don't worry about ambiguous cache expire as we do
       * with subtree migrations because all slaves will pin
       * srcdn->get_inode() for duration of this rename.
       */
      mdr->set_ambiguous_auth(srcdnl->get_inode());

      // just mark the source inode as ambiguous auth if more than two MDS are involved.
      // the master will send another OP_RENAMEPREP slave request later.
      if (mdr->slave_request->witnesses.size() > 1) {
	dout(10) << " set srci ambiguous auth; providing srcdn replica list" << dendl;
	reply_witness = true;
      }
    }

    // is witness list sufficient?
    set<int> srcdnrep;
    srcdn->list_replicas(srcdnrep);
    for (set<int>::iterator p = srcdnrep.begin(); p != srcdnrep.end(); ++p) {
      if (*p == mdr->slave_to_mds ||
	  mdr->slave_request->witnesses.count(*p)) continue;
      dout(10) << " witness list insufficient; providing srcdn replica list" << dendl;
      reply_witness = true;
      break;
    }

    if (reply_witness) {
      assert(srcdnrep.size());
      MMDSSlaveRequest *reply = new MMDSSlaveRequest(mdr->reqid, mdr->attempt,
						     MMDSSlaveRequest::OP_RENAMEPREPACK);
      reply->witnesses.swap(srcdnrep);
      mds->send_message_mds(reply, mdr->slave_to_mds);
      mdr->slave_request->put();
      mdr->slave_request = 0;
      return;	
    }
    dout(10) << " witness list sufficient: includes all srcdn replicas" << dendl;
  } else if (srcdnl->is_primary() && srcdn->authority() != destdn->authority()) {
    // set ambiguous auth for srci on witnesses
    mdr->set_ambiguous_auth(srcdnl->get_inode());
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

  // journal.
  mdr->ls = mdlog->get_current_segment();
  ESlaveUpdate *le = new ESlaveUpdate(mdlog, "slave_rename_prep", mdr->reqid, mdr->slave_to_mds,
				      ESlaveUpdate::OP_PREPARE, ESlaveUpdate::RENAME);
  mdlog->start_entry(le);
  le->rollback = mdr->more()->rollback_bl;
  
  bufferlist blah;  // inode import data... obviously not used if we're the slave
  _rename_prepare(mdr, &le->commit, &blah, srcdn, destdn, straydn);
  
  mdlog->submit_entry(le, new C_MDS_SlaveRenamePrep(this, mdr, srcdn, destdn, straydn));
  mdlog->flush();
}

void Server::_logged_slave_rename(MDRequest *mdr, 
				  CDentry *srcdn, CDentry *destdn, CDentry *straydn)
{
  dout(10) << "_logged_slave_rename " << *mdr << dendl;

  // prepare ack
  MMDSSlaveRequest *reply = new MMDSSlaveRequest(mdr->reqid, mdr->attempt,
						 MMDSSlaveRequest::OP_RENAMEPREPACK);
  
  CDentry::linkage_t *srcdnl = srcdn->get_linkage();
  CDentry::linkage_t *destdnl = destdn->get_linkage();
  //CDentry::linkage_t *straydnl = straydn ? straydn->get_linkage() : 0;

  // export srci?
  if (srcdn->is_auth() && srcdnl->is_primary()) {
    // set export bounds for CInode::encode_export()
    list<CDir*> bounds;
    if (srcdnl->get_inode()->is_dir()) {
      srcdnl->get_inode()->get_dirfrags(bounds);
      for (list<CDir*>::iterator p = bounds.begin(); p != bounds.end(); ++p)
	(*p)->state_set(CDir::STATE_EXPORTBOUND);
    }

    map<client_t,entity_inst_t> exported_client_map;
    bufferlist inodebl;
    mdcache->migrator->encode_export_inode(srcdnl->get_inode(), inodebl, 
					   exported_client_map);

    for (list<CDir*>::iterator p = bounds.begin(); p != bounds.end(); ++p)
      (*p)->state_clear(CDir::STATE_EXPORTBOUND);

    ::encode(exported_client_map, reply->inode_export);
    reply->inode_export.claim_append(inodebl);
    reply->inode_export_v = srcdnl->get_inode()->inode.version;

    // remove mdr auth pin
    mdr->auth_unpin(srcdnl->get_inode());
    mdr->more()->is_inode_exporter = true;

    if (srcdnl->get_inode()->is_dirty())
      srcdnl->get_inode()->mark_clean();

    dout(10) << " exported srci " << *srcdnl->get_inode() << dendl;
  }

  // apply
  _rename_apply(mdr, srcdn, destdn, straydn);   
  
  destdnl = destdn->get_linkage();

  mds->send_message_mds(reply, mdr->slave_to_mds);
  
  // bump popularity
  mds->balancer->hit_dir(mdr->now, srcdn->get_dir(), META_POP_IWR);
  if (destdnl->get_inode() && destdnl->get_inode()->is_auth())
    mds->balancer->hit_inode(mdr->now, destdnl->get_inode(), META_POP_IWR);

  // done.
  mdr->slave_request->put();
  mdr->slave_request = 0;
  mdr->straydn = 0;
}

void Server::_commit_slave_rename(MDRequest *mdr, int r,
				  CDentry *srcdn, CDentry *destdn, CDentry *straydn)
{
  dout(10) << "_commit_slave_rename " << *mdr << " r=" << r << dendl;

  CDentry::linkage_t *destdnl = destdn->get_linkage();

  list<Context*> finished;
  if (r == 0) {
    // write a commit to the journal
    ESlaveUpdate *le = new ESlaveUpdate(mdlog, "slave_rename_commit", mdr->reqid, 
					mdr->slave_to_mds, ESlaveUpdate::OP_COMMIT, 
					ESlaveUpdate::RENAME);
    mdlog->start_entry(le);

    // unfreeze+singleauth inode
    //  hmm, do i really need to delay this?
    if (mdr->more()->is_inode_exporter) {

      CInode *in = destdnl->get_inode();

      // drop our pins
      // we exported, clear out any xlocks that we moved to another MDS
      set<SimpleLock*>::iterator i = mdr->xlocks.begin();
      while (i != mdr->xlocks.end()) {
        SimpleLock *lock = *i++;

	// we only care about xlocks on the exported inode
	if (lock->get_parent() == in &&
	    !lock->is_locallock())
	  mds->locker->xlock_export(lock, mdr);
      }

      dout(10) << " finishing inode export on " << *destdnl->get_inode() << dendl;
      mdcache->migrator->finish_export_inode(destdnl->get_inode(), mdr->now, finished);
      mds->queue_waiters(finished);   // this includes SINGLEAUTH waiters.

      // unfreeze
      assert(destdnl->get_inode()->is_frozen_inode());
      destdnl->get_inode()->unfreeze_inode(finished);
    }

    // singleauth
    if (mdr->more()->is_ambiguous_auth) {
      mdr->more()->rename_inode->clear_ambiguous_auth(finished);
      mdr->more()->is_ambiguous_auth = false;
    }

    mds->queue_waiters(finished);
    mdr->cleanup();

    mdlog->submit_entry(le, new C_MDS_CommittedSlave(this, mdr));
    mdlog->flush();
  } else {
    if (srcdn->is_auth() && destdnl->is_primary()) {
      dout(10) << " reversing inode export of " << *destdnl->get_inode() << dendl;
      destdnl->get_inode()->abort_export();
    }

    // abort
    //  rollback_bl may be empty if we froze the inode but had to provide an expanded
    // witness list from the master, and they failed before we tried prep again.
    if (mdr->more()->rollback_bl.length()) {
      if (mdcache->is_ambiguous_slave_update(mdr->reqid, mdr->slave_to_mds)) {
	mdcache->remove_ambiguous_slave_update(mdr->reqid, mdr->slave_to_mds);
	// rollback but preserve the slave request
	do_rename_rollback(mdr->more()->rollback_bl, mdr->slave_to_mds, mdr, false);
      } else
	do_rename_rollback(mdr->more()->rollback_bl, mdr->slave_to_mds, mdr, true);
    } else {
      dout(10) << " rollback_bl empty, not rollback back rename (master failed after getting extra witnesses?)" << dendl;
      // singleauth
      if (mdr->more()->is_ambiguous_auth) {
	if (srcdn->is_auth())
	  mdr->more()->rename_inode->unfreeze_inode(finished);

	mdr->more()->rename_inode->clear_ambiguous_auth(finished);
	mdr->more()->is_ambiguous_auth = false;
      }
      mds->queue_waiters(finished);
      mds->mdcache->request_finish(mdr);
    }
  }
}

void _rollback_repair_dir(Mutation *mut, CDir *dir, rename_rollback::drec &r, utime_t ctime, 
			  bool isdir, int linkunlink, nest_info_t &rstat)
{
  fnode_t *pf;
  pf = dir->project_fnode();
  mut->add_projected_fnode(dir);
  pf->version = dir->pre_dirty();

  if (isdir) {
    pf->fragstat.nsubdirs += linkunlink;
    pf->rstat.rsubdirs += linkunlink;
  } else {
    pf->fragstat.nfiles += linkunlink;
    pf->rstat.rfiles += linkunlink;
  }    
  if (r.ino) {
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
  }
  mut->add_updated_lock(&dir->get_inode()->filelock);
  mut->add_updated_lock(&dir->get_inode()->nestlock);
}

struct C_MDS_LoggedRenameRollback : public Context {
  Server *server;
  Mutation *mut;
  MDRequest *mdr;
  CDentry *srcdn;
  version_t srcdnpv;
  CDentry *destdn;
  CDentry *straydn;
  bool finish_mdr;
  C_MDS_LoggedRenameRollback(Server *s, Mutation *m, MDRequest *r,
			     CDentry *sd, version_t pv, CDentry *dd,
			    CDentry *st, bool f) :
    server(s), mut(m), mdr(r), srcdn(sd), srcdnpv(pv), destdn(dd),
    straydn(st), finish_mdr(f) {}
  void finish(int r) {
    server->_rename_rollback_finish(mut, mdr, srcdn, srcdnpv,
				    destdn, straydn, finish_mdr);
  }
};

void Server::do_rename_rollback(bufferlist &rbl, int master, MDRequest *mdr,
				bool finish_mdr)
{
  rename_rollback rollback;
  bufferlist::iterator p = rbl.begin();
  ::decode(rollback, p);

  dout(10) << "do_rename_rollback on " << rollback.reqid << dendl;
  // need to finish this update before sending resolve to claim the subtree
  mds->mdcache->add_rollback(rollback.reqid, master);

  Mutation *mut = new Mutation(rollback.reqid);
  mut->ls = mds->mdlog->get_current_segment();

  CDentry *srcdn = NULL;
  CDir *srcdir = mds->mdcache->get_dirfrag(rollback.orig_src.dirfrag);
  if (srcdir) {
    dout(10) << "  srcdir " << *srcdir << dendl;
    srcdn = srcdir->lookup(rollback.orig_src.dname);
    if (srcdn) {
      dout(10) << "   srcdn " << *srcdn << dendl;
      assert(srcdn->get_linkage()->is_null());
    } else
      dout(10) << "   srcdn not found" << dendl;
  } else
    dout(10) << "  srcdir not found" << dendl;

  CDentry *destdn = NULL;
  CDir *destdir = mds->mdcache->get_dirfrag(rollback.orig_dest.dirfrag);
  if (destdir) {
    dout(10) << " destdir " << *destdir << dendl;
    destdn = destdir->lookup(rollback.orig_dest.dname);
    if (destdn)
      dout(10) << "  destdn " << *destdn << dendl;
    else
      dout(10) << "  destdn not found" << dendl;
  } else
    dout(10) << " destdir not found" << dendl;

  CInode *in = NULL;
  if (rollback.orig_src.ino) {
    in = mds->mdcache->get_inode(rollback.orig_src.ino);
    if (in && in->is_dir())
      assert(srcdn && destdn);
  } else
    in = mds->mdcache->get_inode(rollback.orig_src.remote_ino);

  CDir *straydir = NULL;
  CDentry *straydn = NULL;
  if (rollback.stray.dirfrag.ino) {
    straydir = mds->mdcache->get_dirfrag(rollback.stray.dirfrag);
    if (straydir) {
      dout(10) << "straydir " << *straydir << dendl;
      straydn = straydir->lookup(rollback.stray.dname);
      if (straydn) {
	dout(10) << " straydn " << *straydn << dendl;
	assert(straydn->get_linkage()->is_primary());
      } else
	dout(10) << " straydn not found" << dendl;
    } else
      dout(10) << "straydir not found" << dendl;
  }

  CInode *target = NULL;
  if (rollback.orig_dest.ino) {
    target = mds->mdcache->get_inode(rollback.orig_dest.ino);
    if (target)
      assert(destdn && straydn);
  } else if (rollback.orig_dest.remote_ino)
    target = mds->mdcache->get_inode(rollback.orig_dest.remote_ino);

  // can't use is_auth() in the resolve stage
  int whoami = mds->get_nodeid();
  // slave
  assert(!destdn || destdn->authority().first != whoami);
  assert(!straydn || straydn->authority().first != whoami);

  bool force_journal_src = false;
  bool force_journal_dest = false;
  if (in && in->is_dir() && srcdn->authority().first != whoami)
    force_journal_src = _need_force_journal(in, false);
  if (in && target && target->is_dir())
    force_journal_dest = _need_force_journal(in, true);
  
  version_t srcdnpv = 0;
  // repair src
  if (srcdn) {
    if (srcdn->authority().first == whoami)
      srcdnpv = srcdn->pre_dirty();
    if (rollback.orig_src.ino) {
      assert(in);
      srcdn->push_projected_linkage(in);
    } else
      srcdn->push_projected_linkage(rollback.orig_src.remote_ino,
				    rollback.orig_src.remote_d_type);
  }

  inode_t *pi = 0;
  if (in) {
    if (in->authority().first == whoami) {
      pi = in->project_inode();
      mut->add_projected_inode(in);
      pi->version = in->pre_dirty();
    } else
      pi = in->get_projected_inode();
    if (pi->ctime == rollback.ctime)
      pi->ctime = rollback.orig_src.old_ctime;
  }

  if (srcdn && srcdn->authority().first == whoami) {
    nest_info_t blah;
    _rollback_repair_dir(mut, srcdir, rollback.orig_src, rollback.ctime,
			 in ? in->is_dir() : false, 1, pi ? pi->rstat : blah);
  }

  // repair dest
  if (destdn) {
    if (rollback.orig_dest.ino && target) {
      destdn->push_projected_linkage(target);
    } else if (rollback.orig_dest.remote_ino) {
      destdn->push_projected_linkage(rollback.orig_dest.remote_ino,
				     rollback.orig_dest.remote_d_type);
    } else {
      // the dentry will be trimmed soon, it's ok to have wrong linkage
      if (rollback.orig_dest.ino)
	assert(mds->is_resolve());
      destdn->push_projected_linkage();
    }
  }

  if (straydn)
    straydn->push_projected_linkage();

  if (target) {
    inode_t *ti = NULL;
    if (target->authority().first == whoami) {
      ti = target->project_inode();
      mut->add_projected_inode(target);
      ti->version = target->pre_dirty();
    } else 
      ti = target->get_projected_inode();
    if (ti->ctime == rollback.ctime)
      ti->ctime = rollback.orig_dest.old_ctime;
    ti->nlink++;
  }

  if (srcdn)
    dout(0) << " srcdn back to " << *srcdn << dendl;
  if (in)
    dout(0) << "  srci back to " << *in << dendl;
  if (destdn)
    dout(0) << " destdn back to " << *destdn << dendl;
  if (target)
    dout(0) << "  desti back to " << *target << dendl;
  
  // journal it
  ESlaveUpdate *le = new ESlaveUpdate(mdlog, "slave_rename_rollback", rollback.reqid, master,
				      ESlaveUpdate::OP_ROLLBACK, ESlaveUpdate::RENAME);
  mdlog->start_entry(le);

  if (srcdn && (srcdn->authority().first == whoami || force_journal_src)) {
    le->commit.add_dir_context(srcdir);
    if (rollback.orig_src.ino)
      le->commit.add_primary_dentry(srcdn, 0, true);
    else
      le->commit.add_remote_dentry(srcdn, true);
  }

  if (force_journal_dest) {
    assert(rollback.orig_dest.ino);
    le->commit.add_dir_context(destdir);
    le->commit.add_primary_dentry(destdn, 0, true);
  }

  // slave: no need to journal straydn

  if (target && target->authority().first == whoami) {
    assert(rollback.orig_dest.remote_ino);
    le->commit.add_dir_context(target->get_projected_parent_dir());
    le->commit.add_primary_dentry(target->get_projected_parent_dn(), target, true);
  }

  if (force_journal_dest) {
    dout(10) << " noting rename target ino " << target->ino() << " in metablob" << dendl;
    le->commit.renamed_dirino = target->ino();
  } else if (force_journal_src || (in && in->is_dir() && srcdn->authority().first == whoami)) {
    dout(10) << " noting renamed dir ino " << in->ino() << " in metablob" << dendl;
    le->commit.renamed_dirino = in->ino();
  }
  
  if (target && target->is_dir()) {
    assert(destdn);
    mdcache->project_subtree_rename(in, straydir, destdir);
  }

  if (in && in->is_dir()) {
    assert(srcdn);
    mdcache->project_subtree_rename(in, destdir, srcdir);
  }

  mdlog->submit_entry(le, new C_MDS_LoggedRenameRollback(this, mut, mdr, srcdn, srcdnpv,
							 destdn, straydn, finish_mdr));
  mdlog->flush();
}

void Server::_rename_rollback_finish(Mutation *mut, MDRequest *mdr, CDentry *srcdn,
				     version_t srcdnpv, CDentry *destdn,
				     CDentry *straydn, bool finish_mdr)
{
  dout(10) << "_rename_rollback_finish " << mut->reqid << dendl;

  if (straydn) {
    straydn->get_dir()->unlink_inode(straydn);
    straydn->pop_projected_linkage();
  }
  if (destdn) {
    destdn->get_dir()->unlink_inode(destdn);
    destdn->pop_projected_linkage();
  }
  if (srcdn) {
    srcdn->pop_projected_linkage();
    if (srcdn->authority().first == mds->get_nodeid())
      srcdn->mark_dirty(srcdnpv, mut->ls);
  }

  mut->apply();

  if (srcdn) {
    CInode *in = srcdn->get_linkage()->get_inode();
    // update subtree map?
    if (in && in->is_dir()) {
      assert(destdn);
      mdcache->adjust_subtree_after_rename(in, destdn->get_dir(), true);
    }
  }

  if (destdn) {
    CInode *oldin = destdn->get_linkage()->get_inode();
    // update subtree map?
    if (oldin && oldin->is_dir()) {
      assert(straydn);
      mdcache->adjust_subtree_after_rename(oldin, straydn->get_dir(), true);
    }
  }

  if (mds->is_resolve()) {
    CDir *root = NULL;
    if (straydn)
      root = mdcache->get_subtree_root(straydn->get_dir());
    else if (destdn)
      root = mdcache->get_subtree_root(destdn->get_dir());
    if (root)
      mdcache->try_trim_non_auth_subtree(root);
  }

  if (mdr) {
    list<Context*> finished;
    if (mdr->more()->is_ambiguous_auth) {
      if (srcdn->is_auth())
	mdr->more()->rename_inode->unfreeze_inode(finished);

      mdr->more()->rename_inode->clear_ambiguous_auth(finished);
      mdr->more()->is_ambiguous_auth = false;
    }
    mds->queue_waiters(finished);
    if (finish_mdr)
      mds->mdcache->request_finish(mdr);
  }

  mds->mdcache->finish_rollback(mut->reqid);

  mut->cleanup();
  delete mut;
}

/* This function DOES put the passed message before returning*/
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




// snaps
/* This function takes responsibility for the passed mdr*/
void Server::handle_client_lssnap(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;

  // traverse to path
  CInode *diri = mdcache->get_inode(req->get_filepath().get_ino());
  if (!diri || diri->state_test(CInode::STATE_PURGING)) {
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
  realm->get_snap_info(infomap, diri->get_oldest_snap());

  __u32 num = 0;
  bufferlist dnbl;
  for (map<snapid_t,SnapInfo*>::iterator p = infomap.begin();
       p != infomap.end();
       ++p) {
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
  reply->set_extra_bl(dirbl);
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

/* This function takes responsibility for the passed mdr*/
void Server::handle_client_mksnap(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;
  CInode *diri = mdcache->get_inode(req->get_filepath().get_ino());
  if (!diri || diri->state_test(CInode::STATE_PURGING)) {
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
  if (diri->is_system() && !diri->is_root()) {
    // no snaps in system dirs (root is ok)
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

  // we need to anchor... get these locks up front!
  mds->mdcache->anchor_create_prep_locks(mdr, diri, rdlocks, xlocks);

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
    mdr->now = ceph_clock_now(g_ceph_context);

  // anchor
  if (!diri->is_anchored()) {
    mdcache->anchor_create(mdr, diri, new C_MDS_RetryRequest(mdcache, mdr));
    return;
  }

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

  // project the snaprealm
  sr_t *newsnap = diri->project_snaprealm(snapid);
  newsnap->snaps[snapid] = info;
  newsnap->seq = snapid;
  newsnap->last_created = snapid;

  // journal the inode changes
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "mksnap");
  mdlog->start_entry(le);

  le->metablob.add_client_req(req->get_reqid(), req->get_oldest_client_tid());
  le->metablob.add_table_transaction(TABLE_SNAP, stid);
  mdcache->predirty_journal_parents(mdr, &le->metablob, diri, 0, PREDIRTY_PRIMARY, false);
  mdcache->journal_dirty_inode(mdr, &le->metablob, diri);

  // journal the snaprealm changes
  mdlog->submit_entry(le, new C_MDS_mksnap_finish(mds, mdr, diri, info));
  mdlog->flush();
}

void Server::_mksnap_finish(MDRequest *mdr, CInode *diri, SnapInfo &info)
{
  dout(10) << "_mksnap_finish " << *mdr << " " << info << dendl;

  int op = (diri->snaprealm? CEPH_SNAP_OP_CREATE : CEPH_SNAP_OP_SPLIT);

  diri->pop_and_dirty_projected_inode(mdr->ls);
  mdr->apply();

  mds->snapclient->commit(mdr->more()->stid, mdr->ls);

  // create snap
  dout(10) << "snaprealm now " << *diri->snaprealm << dendl;

  mdcache->do_realm_invalidate_and_update_notify(diri, op);

  // yay
  mdr->in[0] = diri;
  mdr->snapid = info.snapid;
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

/* This function takes responsibility for the passed mdr*/
void Server::handle_client_rmsnap(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;

  CInode *diri = mdcache->get_inode(req->get_filepath().get_ino());
  if (!diri || diri->state_test(CInode::STATE_PURGING)) {
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

  const string &snapname = req->get_filepath().last_dentry();
  dout(10) << "rmsnap " << snapname << " on " << *diri << dendl;

  // does snap exist?
  if (snapname.length() == 0 || snapname[0] == '_') {
    reply_request(mdr, -EINVAL);   // can't prune a parent snap, currently.
    return;
  }
  if (!diri->snaprealm || !diri->snaprealm->exists(snapname)) {
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
  pi->ctime = ceph_clock_now(g_ceph_context);
  pi->version = diri->pre_dirty();
  
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "rmsnap");
  mdlog->start_entry(le);
  
  // project the snaprealm
  sr_t *newnode = diri->project_snaprealm();
  newnode->snaps.erase(snapid);
  newnode->seq = seq;
  newnode->last_destroyed = seq;

  le->metablob.add_client_req(req->get_reqid(), req->get_oldest_client_tid());
  le->metablob.add_table_transaction(TABLE_SNAP, stid);
  mdcache->predirty_journal_parents(mdr, &le->metablob, diri, 0, PREDIRTY_PRIMARY, false);
  mdcache->journal_dirty_inode(mdr, &le->metablob, diri);

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

  dout(10) << "snaprealm now " << *diri->snaprealm << dendl;

  mdcache->do_realm_invalidate_and_update_notify(diri, CEPH_SNAP_OP_DESTROY);

  // yay
  mdr->in[0] = diri;
  MClientReply *reply = new MClientReply(mdr->client_request, 0);
  reply_request(mdr, reply);
}


