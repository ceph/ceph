#include "CInode.h"
#include "CDir.h"
#include "CDentry.h"

#include "MDSRank.h"
#include "MDCache.h"
#include "Server.h"
#include "Locker.h"
#include "Mutation.h"

#include "MDLog.h"
#include "SessionMap.h"
#include "InoTable.h"
#include "LogEvent.h"
#include "snap.h"

#include "messages/MClientSession.h"
#include "messages/MClientRequest.h"
#include "messages/MClientReply.h"
#include "messages/MClientReconnect.h"

#include "events/EUpdate.h"
#include "events/ESession.h"

#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds." << mds->get_nodeid() << ".server "

class ServerContext : public MDSLogContextBase {
protected:
  Server *server;
  MDSRank *get_mds() { return server->mds; }
public:
  explicit ServerContext(Server *s) : server(s) {
    assert(server != NULL);
  }
};

Server::Server(MDSRank *_mds) :
  mds(_mds), mdcache(_mds->mdcache), locker(_mds->locker),
  reconnect_done(NULL)
{
}

void Server::dispatch(Message *m)
{
  if (m->get_type() == CEPH_MSG_CLIENT_RECONNECT) {
    handle_client_reconnect(static_cast<MClientReconnect*>(m));
    return;
  }

  // active?
  if (!mds->is_active()) {
    if (m->get_type() == CEPH_MSG_CLIENT_REQUEST &&
	(mds->is_reconnect() || mds->get_want_state() == CEPH_MDS_STATE_RECONNECT)) {
      MClientRequest *req = static_cast<MClientRequest*>(m);
      Session *session = mds->get_session(req);

      session->mutex_lock();
      if (session->is_closed()) {
	dout(5) << "session is closed, dropping " << req->get_reqid() << dendl;
	session->mutex_unlock();
	req->put();
	return;
      }

      bool queue_replay = false;
      if (req->is_replay()) {
	dout(3) << "queuing replayed op" << dendl;
	queue_replay = true;
      } else if (req->get_retry_attempt()) {
	// process completed request in clientreplay stage. The completed request
	// might have created new file/directorie. This guarantees MDS sends a reply
	// to client before other request modifies the new file/directorie.
	if (session->have_completed_request(req->get_reqid().tid, NULL)) {
	  dout(3) << "queuing completed op" << dendl;
	  queue_replay = true;
	}
	// this request was created before the cap reconnect message, drop any embedded
	// cap releases.
	req->releases.clear();
      }
      session->mutex_unlock();

      if (queue_replay) {
	mds->queue_replay_request(req);
	return;
      }
    }

    bool wait_for_active = true;
    if (mds->is_clientreplay()) {
      // session open requests need to be handled during replay,
      // close requests need to be delayed
      if ((m->get_type() == CEPH_MSG_CLIENT_SESSION &&
          (static_cast<MClientSession*>(m))->get_op() != CEPH_SESSION_REQUEST_CLOSE)) {
        wait_for_active = false;
      } else if (m->get_type() == CEPH_MSG_CLIENT_REQUEST) {
        MClientRequest *req = static_cast<MClientRequest*>(m);
        if (req->is_replay()) {
          wait_for_active = false;
        } else if (req->get_retry_attempt()) {
          Session *session = mds->get_session(req);
	  session->mutex_lock();
          if (session->have_completed_request(req->get_reqid().tid, NULL))
            wait_for_active = false;
	  session->mutex_unlock();
        }
      }
    }

    if (wait_for_active) {
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
    default:
      derr << "server unknown message " << m->get_type() << dendl;
      assert(0 == "server unknown message");
  }
}


class C_MDS_session_finish : public ServerContext {
  SessionRef session;
  uint64_t state_seq;
  bool open;
  version_t cmapv;
  interval_set<inodeno_t> inos;
  version_t inotablev;
  Context *fin;
public:
  C_MDS_session_finish(Server *srv, Session *se, uint64_t sseq, bool s, version_t mv, Context *fin_ = NULL) :
    ServerContext(srv), session(se), state_seq(sseq), open(s), cmapv(mv), inotablev(0), fin(fin_) { }
  C_MDS_session_finish(Server *srv, Session *se, uint64_t sseq, bool s, version_t mv,
		       interval_set<inodeno_t>& i, version_t iv, Context *fin_ = NULL) :
    ServerContext(srv), session(se), state_seq(sseq), open(s), cmapv(mv), inos(i), inotablev(iv), fin(fin_) { }
  void finish(int r) {
    assert(r == 0);
    server->__session_logged(session.get(), state_seq, open, cmapv, inos, inotablev);
    if (fin) {
      fin->complete(r);
    }
  }
};

void Server::__session_logged(Session *session, uint64_t state_seq, bool open, version_t pv,
			      interval_set<inodeno_t>& inos, version_t piv)
{
  dout(10) << "_session_logged " << session->get_source() << " state_seq " << state_seq << " " << (open ? "open":"close")
    << " " << pv << dendl;

  session->mutex_lock();
  mds->sessionmap->mutex_lock();

  mds->sessionmap->mark_dirty(session);

  if (piv) {
    mds->inotable->mutex_lock();
    mds->inotable->apply_release_ids(inos);
    assert(mds->inotable->get_version() == piv);
    mds->inotable->mutex_unlock();
  }

  // apply
  if (session->get_state_seq() != state_seq) {
    dout(10) << " journaled state_seq " << state_seq << " != current " << session->get_state_seq()
      << ", noop" << dendl;
    // close must have been canceled (by an import?), or any number of other things..
    mds->sessionmap->mutex_unlock();
  } else if (open) {
    assert(session->is_opening());
    mds->sessionmap->set_state(session, Session::STATE_OPEN);
    mds->sessionmap->touch_session(session);
    assert(session->connection != NULL);
    session->connection->send_message(new MClientSession(CEPH_SESSION_OPEN));
    if (mdcache->is_readonly())
      session->connection->send_message(new MClientSession(CEPH_SESSION_FORCE_RO));
    mds->sessionmap->mutex_unlock();
  } else if (session->is_closing() ||
	     session->is_killing()) {

    assert(session->requests.empty());
    assert(session->caps.empty());
    assert(session->leases.empty());

    if (session->is_closing()) {
      // mark con disposable.  if there is a fault, we will get a
      // reset and clean it up.  if the client hasn't received the
      // CLOSE message yet, they will reconnect and get an
      // ms_handle_remote_reset() and realize they had in fact closed.
      // do this *before* sending the message to avoid a possible
      // race.
      if (session->connection != NULL) {
	// Conditional because terminate_sessions will indiscrimately
	// put sessions in CLOSING whether they ever had a conn or not.
	session->connection->mark_disposable();
      }

      // reset session
      mds->send_message_client(new MClientSession(CEPH_SESSION_CLOSE), session);
    } else if (session->is_killing()) {
      // destroy session, close connection
      if (session->connection != NULL) {
	session->connection->mark_down();
      }
    } else {
      assert(0);
    }
    mds->sessionmap->set_state(session, Session::STATE_CLOSED);
    session->clear();
    mds->sessionmap->remove_session(session);
    mds->sessionmap->mutex_unlock();
  } else {
    mds->sessionmap->mutex_unlock();
    assert(0);
  }
  session->mutex_unlock();
}

void Server::journal_close_session(Session *session, int state, Context *on_safe)
{
  session->mutex_assert_locked_by_me();
  client_t client = session->get_client();

  mds->sessionmap->mutex_lock();
  uint64_t sseq = mds->sessionmap->set_state(session, state);
  mds->sessionmap->mutex_unlock();

  mdcache->lock_client_reconnect();
  if (client_reconnect_gather.count(client)) {
    client_reconnect_gather.erase(client);
    if (client_reconnect_gather.empty()) {
      reconnect_gather_finish();
    }
  }
  mdcache->unlock_client_reconnect();

  // clean up requests, too
  if (!session->requests.empty()) {
    auto p = session->requests.begin(member_offset(MDRequestImpl, item_session_request));
    vector<metareqid_t> reqids;
    while (!p.end()) {
      reqids.push_back((*p)->reqid);
      ++p;
    }
    session->mutex_unlock();

    for (auto& reqid : reqids) {
      MDRequestRef mdr = mdcache->request_get(reqid);
      if (mdr)
	mdcache->request_kill(mdr);
    }

    session->mutex_lock();
  }

  while (!session->caps.empty()) {
    Capability *cap = session->caps.front();
    CInodeRef in = cap->get_inode();
    session->mutex_unlock();

    in->mutex_lock();
    cap = in->get_client_cap(client);
    if (cap) {
      dout(20) << " killing capability " << ccap_string(cap->issued()) << " on " << *in << dendl;
      locker->remove_client_cap(in.get(), session);
    }
    in->mutex_unlock();
    in.reset();

    session->mutex_lock();
  }

  while (!session->leases.empty()) {
    DentryLease *l = session->leases.front();
    CDentryRef dn = l->get_dentry();
    session->mutex_unlock();

    dn->mutex_lock();
    l = dn->get_client_lease(client);
    if (l) {
      dout(20) << " killing lease " << l << " on " << *dn << dendl;
      locker->remove_client_lease(dn.get(), session);
    }
    dn->mutex_unlock();
    dn.reset();

    session->mutex_lock();
  }

  assert(session->get_state_seq() == sseq);

  mds->sessionmap->mutex_lock();
  version_t pv = mds->sessionmap->mark_projected(session);
  version_t piv = 0;

  // release alloc and pending-alloc inos for this session
  // and wipe out session state, in case the session close aborts for some reason
  interval_set<inodeno_t> both;
  both.swap(session->info.prealloc_inos);
  both.insert(session->pending_prealloc_inos);
  session->pending_prealloc_inos.clear();
  if (both.size()) {
    mds->inotable->mutex_lock();
    mds->inotable->project_release_ids(both);
    piv = mds->inotable->get_projected_version();
    mds->inotable->mutex_unlock();
  } else
    piv = 0;

  mds->mdlog->start_submit_entry(new ESession(session->info.inst, false, pv, both, piv),
				 new C_MDS_session_finish(this, session, sseq, false, pv, both, piv, on_safe),
				 true);
  mds->sessionmap->mutex_unlock();

//  finish_flush_session(session, session->get_push_seq());
}

void Server::handle_client_session(MClientSession *m)
{
  Session *session = mds->get_session(m);
  dout(3) << "handle_client_session " << *m << " from " << m->get_source() << dendl;

  session->mutex_lock();
  switch (m->get_op()) {
  case CEPH_SESSION_REQUEST_OPEN:
    {
      if (session->is_opening() ||
          session->is_open() ||
          session->is_stale() ||
          session->is_killing()) {
        dout(10) << "currently open|opening|stale|killing, dropping this req" << dendl;
        goto out_unlock;
      }

      if (session->is_closing()) {
	// FIXME: wait until close done;
      }

      mds->sessionmap->mutex_lock();
      if (session->is_closed())
        mds->sessionmap->add_session(session);

      version_t pv = mds->sessionmap->mark_projected(session);
      uint64_t sseq = mds->sessionmap->set_state(session, Session::STATE_OPENING);
      mds->sessionmap->touch_session(session);
      mds->mdlog->start_submit_entry(new ESession(m->get_source_inst(), true, pv, m->client_meta),
				     new C_MDS_session_finish(this, session, sseq, true, pv),
				     true);
      mds->sessionmap->mutex_unlock();
    }
    break;
  case CEPH_SESSION_REQUEST_CLOSE:
    {
      if (session->is_closed() ||
          session->is_closing() ||
          session->is_killing()) {
        dout(10) << "already closed|closing|killing, dropping this req" << dendl;
        goto out_unlock;	  
      }
      /*
      if (session->is_importing()) {
        dout(10) << "ignoring close req on importing session" << dendl;
        goto out_unlock;
      }
      */
      assert(session->is_open() || session->is_stale() || session->is_opening());
      if (m->get_seq() < session->get_push_seq()) {
        dout(10) << "old push seq " << m->get_seq() << " < " << session->get_push_seq()
		 << ", dropping" << dendl;
        goto out_unlock;	  
      }
      // We are getting a seq that is higher than expected.
      // Handle the same as any other seqn error.
      //
      if (m->get_seq() != session->get_push_seq()) {
        dout(0) << "old push seq " << m->get_seq() << " != " << session->get_push_seq()
		<< ", BUGGY!" << dendl;
        mds->clog->warn() << "incorrect push seq " << m->get_seq() << " != "
			  << session->get_push_seq() << ", dropping"
			  << " from client : " << session->get_human_name();
        goto out_unlock;	  
      }
      journal_close_session(session, Session::STATE_CLOSING, NULL);
    }
    break;
  case CEPH_SESSION_REQUEST_RENEWCAPS:
    if (session->is_open() ||
	session->is_stale()) {
      mds->sessionmap->mutex_lock();
      uint64_t sseq = 0;
      if (session->is_stale()) {
	sseq = mds->sessionmap->set_state(session, Session::STATE_OPEN);
      }
      mds->sessionmap->touch_session(session);
      m->get_connection()->send_message(new MClientSession(CEPH_SESSION_RENEWCAPS, m->get_seq()));
      mds->sessionmap->mutex_unlock();

      if (sseq > 0)
	mds->locker->resume_stale_caps(session, sseq);
    } else {
      dout(10) << "ignoring renewcaps on non open|stale session (" << session->get_state_name() << ")" << dendl;
    }
    m->get_connection()->send_message(new MClientSession(CEPH_SESSION_RENEWCAPS, m->get_seq()));
    break;
  case CEPH_SESSION_FLUSHMSG_ACK:
    break;
  default:
    derr << "server unknown session op " << m->get_op() << dendl;
    assert(0 == "server unknown session op");
  }
out_unlock:
  session->mutex_unlock();
  m->put();
}

void Server::kill_session(Session *session, Context *on_safe)
{
  session->mutex_assert_locked_by_me();
  if ((session->is_opening() ||
       session->is_open() ||
       session->is_stale()) &&
      !session->is_importing()) {
    dout(10) << "kill_session " << session << dendl;
    journal_close_session(session, Session::STATE_KILLING, on_safe);
  } else {
    dout(10) << "kill_session importing or already closing/killing " << session << dendl;
    assert(session->is_closing() ||
	   session->is_closed() ||
	   session->is_killing() ||
	   session->is_importing());
    if (on_safe) {
      on_safe->complete(0);
    }
  }
}

void Server::find_idle_sessions()
{
  dout(10) << "find_idle_sessions.  laggy until " << mds->get_laggy_until() << dendl;

  // timeout/stale
  //  (caps go stale, lease die)
  utime_t now = ceph_clock_now(g_ceph_context);
  utime_t cutoff = now;
  cutoff -= g_conf->mds_session_timeout;

  mds->sessionmap->mutex_lock();
  while (1) {
    SessionRef session = mds->sessionmap->get_oldest_session(Session::STATE_OPEN);
    if (!session)
      break;

    if (!session->mutex_trylock()) {
      mds->sessionmap->mutex_unlock();
      session->mutex_lock();
      mds->sessionmap->mutex_lock();

      if (!session->is_open()) {
	session->mutex_unlock();
	continue;
      }
    }

    dout(20) << "laggiest active session is " << session->info.inst << dendl;
    if (session->last_cap_renew >= cutoff) {
      dout(20) << "laggiest active session is " << session->info.inst << " and sufficiently new ("
	       << session->last_cap_renew << ")" << dendl;
      session->mutex_unlock();
      break;
    }

    dout(10) << "new stale session " << session->info.inst << " last " << session->last_cap_renew << dendl;
    uint64_t sseq = mds->sessionmap->set_state(session.get(), Session::STATE_STALE);
    mds->send_message_client(new MClientSession(CEPH_SESSION_STALE, session->get_push_seq()), session.get());

    mds->sessionmap->mutex_unlock();

    ceph_seq_t lseq = session->lease_seq;

    mds->locker->revoke_stale_caps(session.get(), sseq);
    mds->locker->remove_stale_leases(session.get(), lseq);

    // finish_flush_session(session, session->get_push_seq());
    session->mutex_unlock();
    mds->sessionmap->mutex_lock();
  }
  mds->sessionmap->mutex_unlock();

  // autoclose
  cutoff = now;
  cutoff -= g_conf->mds_session_autoclose;

  // don't kick clients if we've been laggy
  if (mds->get_laggy_until() > cutoff) {
    dout(10) << " laggy_until " << mds->get_laggy_until() << " > cutoff " << cutoff
	     << ", not kicking any clients to be safe" << dendl;
    return;
  }

  mds->sessionmap->mutex_lock();
  while (1) {
    SessionRef session = mds->sessionmap->get_oldest_session(Session::STATE_STALE);
    if (!session)
      break;

    if (!session->mutex_trylock()) {
      mds->sessionmap->mutex_unlock();
      session->mutex_lock();
      mds->sessionmap->mutex_lock();

      if (!session->is_stale()) {
	session->mutex_unlock();
	continue;
      }
    }

    /*
    if (session->is_importing()) {
      dout(10) << "stopping at importing session " << session->info.inst << dendl;
      break;
    }
    */
    assert(session->is_stale());
    if (session->last_cap_renew >= cutoff) {
      dout(20) << "oldest stale session is " << session->info.inst << " and sufficiently new ("
	       << session->last_cap_renew << ")" << dendl;
      session->mutex_unlock();
      break;
    }
    mds->sessionmap->mutex_unlock();

    utime_t age = now;
    age -= session->last_cap_renew;
    mds->clog->info() << "closing stale session " << session->info.inst
		      << " after " << age << "\n";
    dout(10) << "autoclosing stale session " << session->info.inst << " last " << session->last_cap_renew << dendl;
    kill_session(session.get(), NULL);

    session->mutex_unlock();
    mds->sessionmap->mutex_lock();
  }
  mds->sessionmap->mutex_unlock();
}

void Server::trim_client_leases()
{
  dout(10) << "trim_client_leases" << dendl;

  list<Session*> ls;
  mds->sessionmap->mutex_lock();
  mds->sessionmap->get_sessions(Session::STATE_OPEN, ls);
  for (auto session : ls)
    session->get();
  mds->sessionmap->mutex_unlock();

  int count = 0;
  for (auto session : ls) {
    utime_t now = ceph_clock_now(g_ceph_context);

    session->mutex_lock();
    client_t client = session->get_client();

    while (!session->leases.empty()) {
      if (!session->is_open())
	break;

      DentryLease *l = session->leases.front();
      if (l->ttl > now)
	break;

      CDentryRef dn = l->get_dentry();
      session->mutex_unlock();

      dn->mutex_lock();
      l = dn->get_client_lease(client);
      if (l) {
	dout(12) << " expiring client." << client << " lease of " << *dn << dendl;
	locker->remove_client_lease(dn.get(), session);
	count++;
      }
      dn->mutex_unlock();
      dn.reset();

      session->mutex_lock();
    }
    session->mutex_unlock();

    session->put();
  }

  dout(10) << "trim_client_leases trimmed " << count << " leases " << dendl;
}

void Server::handle_client_request(MClientRequest *req)
{
  dout(4) << "handle_client_request " << *req << dendl;

  Session *session = 0;
  if (req->get_source().is_client()) {
    session = mds->get_session(req);
    if (!session) {
      dout(5) << "no session for " << req->get_source() << ", dropping" << dendl;
      req->put();
      return;
    }

    session->mutex_lock();
    if (session->is_closed() ||
	session->is_closing() ||
	session->is_killing()) {
      dout(5) << "session closed|closing|killing, dropping" << dendl;
      session->mutex_unlock();
      req->put();
      return;
    }
  }

  bool completed = false;
  if (req->is_replay() || req->get_retry_attempt()) {
    assert(session);
    inodeno_t created;
    if (session->have_completed_request(req->get_reqid().tid, &created)) {
      completed = true;
      // Don't send traceless reply if the completed request has created
      // new inode. Treat the request as lookup request instead.
      if (req->is_replay() ||
	  (created == inodeno_t() &&
	   req->get_op() != CEPH_MDS_OP_OPEN &&
	   req->get_op() != CEPH_MDS_OP_CREATE)) {
	session->mutex_unlock();

	dout(5) << "already completed " << req->get_reqid() << dendl;
	MClientReply *reply = new MClientReply(req, 0);
	if (created != inodeno_t()) {
	  bufferlist extra;
	  ::encode(created, extra);
	  reply->set_extra_bl(extra);
	}
	req->get_connection()->send_message(reply);
	req->put();
	
	mds->replay_next_request();
	return;
      }
      if (req->get_op() != CEPH_MDS_OP_OPEN &&
	  req->get_op() != CEPH_MDS_OP_CREATE) {
	dout(10) << " completed request which created new inode " << created
	  << ", convert it to lookup request" << dendl;
	req->head.op = req->get_dentry_wanted() ? CEPH_MDS_OP_LOOKUP : CEPH_MDS_OP_GETATTR;
	req->head.args.getattr.mask = CEPH_STAT_CAP_INODE_ALL;
      }
    }
    if (mds->is_clientreplay()) {
      assert(req->is_replay() || completed);
    } else {
      assert(!req->is_replay() && !completed);
    }
  }

  MDRequestRef mdr = mdcache->request_start(req);

  if (session) {
    if (mdr) {
      session->requests.push_back(&mdr->item_session_request);
      mdr->session = session;
    }
    mdr->completed = completed;
    mdr->replaying = mds->is_clientreplay();
    session->mutex_unlock();
  }

  if (req->get_oldest_client_tid() > 0) {
    dout(15) << " oldest_client_tid=" << req->get_oldest_client_tid() << dendl;
    assert(session);
    session->trim_completed_requests(req->get_oldest_client_tid());
    //FIXME warn if client does not trim completed request
  }

  if (!mdr)
    return;

  if (!req->releases.empty() && req->get_source().is_client() && !req->is_replay()) {
    for (vector<MClientRequest::Release>::iterator p = req->releases.begin();
	p != req->releases.end();
	++p)
      locker->process_request_cap_release(mdr, p->item, p->dname);
    req->releases.clear();
  }

  mds->req_wq.queue(mdr);
  //dispatch_client_request(mdr);
  return;
}

void Server::dispatch_client_request(const MDRequestRef& mdr)
{
  MClientRequest *req = mdr->client_request;
  dout(7) << "dispatch_client_request " << *req << dendl;

  switch (req->get_op()) {
    case CEPH_MDS_OP_LOOKUP:
      handle_client_getattr(mdr, true);
      break;
    case CEPH_MDS_OP_GETATTR:
      handle_client_getattr(mdr, false);
      break;
    case CEPH_MDS_OP_SETATTR:
      handle_client_setattr(mdr);
      break;
    case CEPH_MDS_OP_CREATE:
      if (mdr->completed)
	handle_client_open(mdr);  // already created.. just open
      else
	handle_client_openc(mdr);
      break;
    case CEPH_MDS_OP_OPEN:
      handle_client_open(mdr);
      break;
    case CEPH_MDS_OP_MKNOD:
      handle_client_mknod(mdr);
      break;
    case CEPH_MDS_OP_SYMLINK:
      handle_client_symlink(mdr);
      break;
    case CEPH_MDS_OP_MKDIR:
      handle_client_mkdir(mdr);
      break;
    case CEPH_MDS_OP_READDIR:
      handle_client_readdir(mdr);
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
    default:
      dout(1) << " unknown client op " << req->get_op() << dendl;
      respond_to_request(mdr, -EOPNOTSUPP);
  }
}

void Server::encode_empty_dirstat(bufferlist& bl)
{
  static DirStat empty;
  empty.encode(bl);
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

void Server::set_trace_dist(Session *session, MClientReply *reply,
			    CInode *in, CDentry *dn, const MDRequestRef& mdr)
{
  utime_t now = ceph_clock_now(g_ceph_context);
  bufferlist bl;

  {
    SnapRealmInfo info(CEPH_INO_ROOT, 0, 1, 1);
    ::encode(info, reply->snapbl);
  }

  // dir + dentry?
  if (dn) {
    reply->head.is_dentry = 1;
    CDir *dir = dn->get_dir();
    CInode *diri = dir->get_inode();

    diri->encode_inodestat(bl, session);
    //    dout(20) << "set_trace_dist added diri " << *diri << dendl;

    encode_empty_dirstat(bl);
    //    dout(20) << "set_trace_dist added dir  " << *dir << dendl;

    dn->mutex_lock();
    ::encode(dn->name, bl);
    locker->issue_client_lease(dn, bl, now, session);
    dn->mutex_unlock();
    //    dout(20) << "set_trace_dist added dn   " << snapid << " " << *dn << dendl;
  } else
    reply->head.is_dentry = 0;

  if (in) {
    in->encode_inodestat(bl, session, mdr->getattr_mask);
//    dout(20) << "set_trace_dist added in  " << *in << dendl;
    reply->head.is_target = 1;
  } else
    reply->head.is_target = 0;

  reply->set_trace(bl);
}

void Server::respond_to_request(const MDRequestRef& mdr, int r)
{
  bool queue_safe_reply = false;
  if (mdr->killed) {
    //
  } else if (mdr->client_request) {
    if (!reply_client_request(mdr, r))
      queue_safe_reply = true;
  } else {
    assert(0); 
  }

  mdr->apply();
  locker->drop_locks(mdr);

  if (mdr->client_request && mdr->is_committing() && !mdr->killed) {
    assert(mdr->session);
    inodeno_t created = mdr->alloc_ino ? mdr->alloc_ino : mdr->used_prealloc_ino;
    mdr->session->add_completed_request(mdr->reqid.tid, created);
    if (mdr->ls) {
      mdcache->lock_log_segments();
      mdr->ls->touched_sessions.insert(mdr->session->get_source());
      mdcache->unlock_log_segments();
    }
  }

  bool replay_next = mdr->completed && mdr->replaying;
  if (queue_safe_reply)
    mds->req_wq.queue_front(mdr);
  else
    mdcache->request_finish(mdr);

  if (replay_next)
    mds->replay_next_request();
}

void Server::early_reply(const MDRequestRef& mdr)
{
  if (!g_conf->mds_early_reply)
    return;

  if (mdr->alloc_ino) {
    dout(10) << "early_reply - allocated ino, not allowed" << dendl;
    return;
  }

  MClientRequest *req = mdr->client_request;
  if (req->is_replay()) {
    dout(10) << " no early reply on replay op" << dendl;
    mds->mdlog->flush();
    return;
  }

  locker->set_xlocks_done(mdr, req->get_op() == CEPH_MDS_OP_RENAME);

  MClientReply *reply = new MClientReply(req, 0);
  reply->set_unsafe();

  if (mdr->tracei >= 0 || mdr->tracedn >= 0) {
    CInode *in = NULL;
    if (mdr->tracei >= 0) {
      in = mdr->in[mdr->tracei].get();
      assert(mdr->is_object_locked(in));
    }
    CDentry *dn = NULL;
    if (mdr->tracedn >= 0) {
      dn = mdr->dn[mdr->tracedn].back().get();
      assert(mdr->is_object_locked(dn->get_dir_inode()));
    }

    set_trace_dist(mdr->session, reply, in, dn, mdr);
  }

  reply->set_extra_bl(mdr->reply_extra_bl);
  reply->set_mdsmap_epoch(mds->mdsmap->get_epoch());
  req->get_connection()->send_message(reply);

  mdr->did_early_reply = true;
}

void Server::send_safe_reply(const MDRequestRef& mdr)
{
  assert(mdr->did_early_reply);
  MClientRequest *req = mdr->client_request;
  MClientReply *reply = new MClientReply(req, 0);
  req->get_connection()->send_message(reply);
}

bool Server::reply_client_request(const MDRequestRef& mdr, int r)
{
  MClientRequest *req = mdr->client_request;
  MClientReply *reply = NULL;

  if (req->is_replay()) {
    reply = new MClientReply(mdr->client_request, r);

    if (mdr->tracei >= 0) {
	CInodeRef& in = mdr->in[mdr->tracei];
	assert(mdr->is_object_locked(in.get()));
	mdcache->try_reconnect_cap(in.get(), mdr->session);
    }
  } else if (!mdr->did_early_reply) {
    reply = new MClientReply(mdr->client_request, r);

    if (mdr->tracei >= 0 || mdr->tracedn >= 0) {
      set<CObject*> objs;

      CInode *in = NULL;
      if (mdr->tracei >= 0) {
	in = mdr->in[mdr->tracei].get();
	assert(mdr->is_object_locked(in));
	objs.insert(in);
      }
      CDentry *dn = NULL;
      if (mdr->tracedn >= 0) {
	dn = mdr->dn[mdr->tracedn].back().get();
	assert(mdr->is_object_locked(dn->get_dir_inode()));
	objs.insert(dn);
	objs.insert(dn->get_dir_inode());
      }

      // drop non-rdlocks before replying, so that we can issue leases
      locker->drop_non_rdlocks(mdr, objs);

      set_trace_dist(mdr->session, reply, in, dn, mdr);
    }

    reply->set_extra_bl(mdr->reply_extra_bl);
  } else {
    assert(r == 0);
  }

  if (!reply)
    return false;

  reply->set_mdsmap_epoch(mds->mdsmap->get_epoch());
  req->get_connection()->send_message(reply);
  return true;
}

int Server::rdlock_path_pin_ref(const MDRequestRef& mdr, int n,
				set<SimpleLock*> &rdlocks,
				bool is_lookup)
{
  const filepath& refpath = n ? mdr->get_filepath2() : mdr->get_filepath();
  dout(10) << "rdlock_path_pin_ref " << *mdr << " " << refpath << dendl;

  int r = mdcache->path_traverse(mdr, refpath, &mdr->dn[n], &mdr->in[n]);
  if (r > 0)
    return -EAGAIN;
  if (r < 0) {
    /*
     * FIXME: 
    if (r == -ENOENT && n == 0 && mdr->dn[n].size()) {
      if (is_lookup)
	mdr->tracedn = n;
    }
    */
    respond_to_request(mdr, r);
    return r;
  }

  for (auto& p : mdr->dn[n])
    rdlocks.insert(&p->lock);

  return 0;
}

int Server::rdlock_path_xlock_dentry(const MDRequestRef& mdr, int n,
				     set<SimpleLock*>& rdlocks,
				     set<SimpleLock*>& wrlocks,
				     set<SimpleLock*>& xlocks,
				     bool okexist, bool mustexist)
{
  const filepath& refpath = n ? mdr->get_filepath2() : mdr->get_filepath();
  dout(10) << "rdlock_path_xlock_dentry " << *mdr << " " << refpath << dendl;

  int err;
  if (refpath.depth() == 0) {
    err = -EINVAL;
    respond_to_request(mdr, err);
    return err;
  }

  filepath dirpath = refpath;
  string dname = dirpath.last_dentry();
  dirpath.pop_dentry();

  CInodeRef diri;
  err = mdcache->path_traverse(mdr, dirpath, &mdr->dn[n], &diri);
  if (err > 0)
    return -EAGAIN;
  if (err < 0) {
    respond_to_request(mdr, err);
    return err;
  }

  if (!diri->is_dir()) {
    err = -ENOTDIR;
    respond_to_request(mdr, err);
    return err; 
  }

  mdr->lock_object(diri.get());

  frag_t fg = diri->pick_dirfrag(dname);
  CDirRef dir = diri->get_or_open_dirfrag(fg);
  assert(dir);

  CDentryRef dn = dir->lookup(dname);
  if (!dn && !dir->is_complete() &&
      !(dir->has_bloom() && !dir->is_in_bloom(dname))) {
    dir->fetch(new C_MDS_RetryRequest(mds, mdr));
    mdr->unlock_object(diri.get());
    return -EAGAIN;
  }

  const CDentry::linkage_t* dnl = dn ? dn->get_linkage(mdr->get_client(), mdr) : NULL;
  CInodeRef in;
  if (mustexist) {
    if (!dn || dnl->is_null()) {
      err = -ENOENT;
      respond_to_request(mdr, err);
      return err;
    }
  } else {
    if (dn) {
      if (!okexist && !dnl->is_null()) {
	err = -EEXIST;
	respond_to_request(mdr, err);
	return err;
      }
    } else {
      dn = dir->add_null_dentry(dname);
    }
  }
  if (dnl && !dnl->is_null()) {
    in = dnl->get_inode();
    if (dnl->is_remote() && !in) {
      in = mdcache->get_inode(dnl->get_remote_ino());
      if (in) {
	dn->link_remote(dnl, in);
      } else if (dn->is_bad_remote_ino(dnl)) {
	respond_to_request(mdr, -EIO);
      } else {
	mdcache->open_remote_dentry(dn.get(), dnl->get_remote_ino(), dnl->get_remote_d_type(),
				    new C_MDS_RetryRequest(mds, mdr));
	mdr->unlock_object(diri.get());
	locker->drop_locks(mdr);
	return -EAGAIN;
      }
    }
    assert(in);
  }

  for (auto& p : mdr->dn[n])
    rdlocks.insert(&p->lock);

  wrlocks.insert(&diri->filelock); // also, wrlock on dir mtime
  wrlocks.insert(&diri->nestlock); // also, wrlock on dir mtime

  xlocks.insert(&dn->lock);

  mdr->unlock_object(diri.get());

  mdr->dn[n].push_back(dn);
  mdr->in[n] = in;
  return 0;
}

void Server::journal_and_reply(const MDRequestRef& mdr, int tracei, int tracedn,
			       LogEvent *le, MDSLogContextBase *fin, bool flush)
{
  // start journal
  mds->mdlog->start_entry(le);
  mdr->ls = mds->mdlog->get_current_segment();

  // drop inodetable/sessionmap lock ASAP
  journal_allocated_inos(mdr, le);

  mds->mdlog->submit_entry(le, fin, flush);

  mdr->tracei = tracei;
  mdr->tracedn = tracedn;

  early_reply(mdr);

  mdr->unlock_all_objects();
  if (mdr->did_early_reply) {
    locker->drop_rdlocks(mdr);
  } else if (mdr->replaying) {
    if (mds->replay_next_request()) {
      dout(10) << " replay next client request" << dendl;
    } else {
      dout(10) << " journaled last replay op, flushing" << dendl;
      mds->mdlog->flush();
    }
  } else {
    mds->mdlog->flush();
  }

  mdr->start_committing();
}

CInodeRef Server::prepare_new_inode(const MDRequestRef& mdr, CDentryRef& dn,
				    inodeno_t useino, unsigned mode, file_layout_t *layout)
{
  CInodeRef in = new CInode(mdcache);

  inode_t *pi = in->__get_inode();
  if (useino)
    pi->ino = useino;

  pi->version = 1;
  pi->xattr_version = 1;
  pi->nlink = 1;
  pi->mode = mode;

  memset(&pi->dir_layout, 0, sizeof(pi->dir_layout));
  if (pi->is_dir()) {
    pi->dir_layout.dl_dir_hash = g_conf->mds_default_dir_hash;
  } else if (layout) {
    pi->layout = *layout;
  } else {
    pi->layout = mdcache->get_default_file_layout();
  }

  pi->truncate_size = -1ull;  // not truncated, yet!
  pi->truncate_seq = 1; /* starting with 1, 0 is kept for no-truncation logic */

  CInode *diri = dn->get_dir_inode();

  dout(10) << oct << " dir mode 0" << diri->get_inode()->mode << " new mode 0" << mode << dec << dendl;

  MClientRequest *req = mdr->client_request;
  pi->uid = req->get_caller_uid();

  if (diri->get_inode()->mode & S_ISGID) {
    dout(10) << " dir is sticky" << dendl;
    pi->gid = diri->get_inode()->gid;
    if (S_ISDIR(mode)) {
      dout(10) << " new dir also sticky" << dendl;
      pi->mode |= S_ISGID;
    }
  } else {
    pi->gid = req->get_caller_gid();
  }

  pi->ctime = pi->mtime = pi->atime = mdr->get_op_stamp();

  if (req->get_data().length()) {
    bufferlist::iterator p = req->get_data().begin();
    // xattrs on new inode?
    map<string,bufferptr> xattrs;
    try {
      ::decode(xattrs, p);
    } catch (buffer::error& e) {
    }
    for (map<string,bufferptr>::iterator p = xattrs.begin(); p != xattrs.end(); ++p) {
      dout(10) << "prepare_new_inode setting xattr " << p->first << dendl;
    }
    in->__get_xattrs()->swap(xattrs);
  }

  in->mutex_lock();
  return in;
}

void Server::project_alloc_inos(const MDRequestRef& mdr, CInode *in, inodeno_t useino)
{
  // assign ino
  inodeno_t ino;
  mds->sessionmap->mutex_lock(); // FIXME: lock session instead

  if (mdr->session->info.prealloc_inos.size()) {
    mdr->used_prealloc_ino = ino = mdr->session->take_ino(useino);  // prealloc -> used
    mds->sessionmap->mark_projected(mdr->session);

    dout(10) << "project_alloc_inos used_prealloc " << mdr->used_prealloc_ino
	     << " (" << mdr->session->info.prealloc_inos
	     << ", " << mdr->session->info.prealloc_inos.size() << " left)" << dendl;

    if (useino && useino != ino) {
      dout(0) << "WARNING: client specified " << useino << " and i allocated " << ino << dendl;
      mds->clog->error() << mdr->session->get_source() << " specified ino " << useino
			 << " but mds." << mds->get_nodeid() << " allocated " << ino << "\n";
      assert(0); // just for now.
    }
  } else {
    mds->inotable->mutex_lock();
    mdr->alloc_ino = ino = mds->inotable->project_alloc_id();
    dout(10) << "project_alloc_inos alloc " << mdr->alloc_ino << dendl;
  }

  int got = g_conf->mds_client_prealloc_inos - mdr->session->get_num_projected_prealloc_inos();
  if (got > g_conf->mds_client_prealloc_inos / 2) {
    if (!mdr->alloc_ino)
      mds->inotable->mutex_lock();
    mds->inotable->project_alloc_ids(mdr->prealloc_inos, got);
    assert(mdr->prealloc_inos.size());  // or else fix projected increment semantics
    mdr->session->pending_prealloc_inos.insert(mdr->prealloc_inos);
    mds->sessionmap->mark_projected(mdr->session);
    dout(10) << "project_alloc_inos prealloc " << mdr->prealloc_inos << dendl;
  }

  if (useino)
    assert(in->ino() == useino);
  else
    in->__get_inode()->ino = ino;

  mdcache->add_inode(in);  // add
}

void Server::journal_allocated_inos(const MDRequestRef& mdr, LogEvent *le)
{
  if (!mdr->alloc_ino  && !mdr->used_prealloc_ino)
    return;

  version_t inotable_pv = 0;
  if (mdr->alloc_ino || !mdr->prealloc_inos.empty())
    inotable_pv = mds->inotable->get_projected_version();

  dout(20) << "journal_allocated_inos sessionmapv "
	   << mds->sessionmap->get_projected()
	   << " inotablev " << inotable_pv << dendl;


  le->get_metablob()->set_ino_alloc(mdr->alloc_ino,
				    mdr->used_prealloc_ino,
				    mdr->prealloc_inos,
				    mdr->session->get_source(),
				    mds->sessionmap->get_projected(),
				    inotable_pv);

  if (inotable_pv)
    mds->inotable->mutex_unlock();
  mds->sessionmap->mutex_unlock();
}

void Server::apply_allocated_inos(const MDRequestRef& mdr)
{
  if (!mdr->alloc_ino && !mdr->used_prealloc_ino)
    return;

  Session *session = mdr->session;
  assert(session);
  dout(10) << "apply_allocated_inos " << mdr->alloc_ino
	   << " / " << mdr->prealloc_inos
	   << " / " << mdr->used_prealloc_ino << dendl;

  mds->sessionmap->mutex_lock();
  assert(!session->is_closed());

  if (mdr->alloc_ino || !mdr->prealloc_inos.empty())
    mds->inotable->mutex_lock();

  if (mdr->alloc_ino) {
    mds->inotable->apply_alloc_id(mdr->alloc_ino);
  }
  if (mdr->prealloc_inos.size()) {
    if (!mdr->killed) {
      session->pending_prealloc_inos.subtract(mdr->prealloc_inos);
      session->info.prealloc_inos.insert(mdr->prealloc_inos);
    }
    mds->sessionmap->mark_dirty(session);
    mds->inotable->apply_alloc_ids(mdr->prealloc_inos);
  }
  if (mdr->used_prealloc_ino) {
    session->info.used_inos.erase(mdr->used_prealloc_ino);
    mds->sessionmap->mark_dirty(session);
  }

  if (mdr->alloc_ino || !mdr->prealloc_inos.empty())
    mds->inotable->mutex_unlock();
  mds->sessionmap->mutex_unlock();
}

void Server::handle_client_getattr(const MDRequestRef& mdr, bool is_lookup)
{
  set<SimpleLock*> rdlocks, wrlocks, xlocks;
  int r = rdlock_path_pin_ref(mdr, 0, rdlocks, is_lookup);
  if (r < 0)
    return;

  CInodeRef& in = mdr->in[0];
  mdr->lock_object(in.get());

  Capability *cap = in->get_client_cap(mdr->get_client());
  int issued = cap ? cap->issued() : 0;

  MClientRequest *req = mdr->client_request;
  int mask = req->head.args.getattr.mask;
  if ((mask & CEPH_CAP_LINK_SHARED) && (issued & CEPH_CAP_LINK_EXCL) == 0) rdlocks.insert(&in->linklock);
  if ((mask & CEPH_CAP_AUTH_SHARED) && (issued & CEPH_CAP_AUTH_EXCL) == 0) rdlocks.insert(&in->authlock);
  if ((mask & CEPH_CAP_FILE_SHARED) && (issued & CEPH_CAP_FILE_EXCL) == 0) rdlocks.insert(&in->filelock);
  if ((mask & CEPH_CAP_XATTR_SHARED) && (issued & CEPH_CAP_XATTR_EXCL) == 0) rdlocks.insert(&in->xattrlock);

  mdr->unlock_object(in.get());

  r = locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks);
  if (r <= 0) {
    if (r == -EAGAIN)
      dispatch_client_request(mdr); // revalidate path
    return;
  }

  if (is_lookup) {
    CDentryRef &dn = mdr->dn[0].back();
    mdr->lock_object(dn->get_dir_inode());
    mdr->tracedn = 0;
  }

  mdr->lock_object(in.get());
  mdr->tracei = 0;
  mdr->getattr_mask = mask;
  respond_to_request(mdr, 0);
}

void Server::__inode_update_finish(const MDRequestRef& mdr, bool truncate_smaller)
{
  mdr->wait_committing();
  Mutex::Locker l(mdr->dispatch_mutex);

  CInodeRef& in = mdr->in[0]; 
  mdcache->lock_objects_for_update(mdr, in.get(), true);
  mdr->early_apply();

  if (truncate_smaller && in->get_inode()->is_truncating()) {
    locker->issue_truncate(in.get());
    mdcache->truncate_inode(in, mdr->ls);
  }

  respond_to_request(mdr, 0);
}

class C_MDS_inode_update_finish : public ServerContext {
  MDRequestRef mdr;
  bool truncate_smaller;
public:
  C_MDS_inode_update_finish(Server *srv, const MDRequestRef& r, bool ts) :
    ServerContext(srv), mdr(r), truncate_smaller(ts) { }
  void finish(int r) {
    assert(r == 0);
    server->__inode_update_finish(mdr, truncate_smaller);
  }
};

void Server::handle_client_setattr(const MDRequestRef& mdr)
{
  set<SimpleLock*> rdlocks, wrlocks, xlocks;
  int r = rdlock_path_pin_ref(mdr, 0, rdlocks, false);
  if (r < 0)
    return;

  CInodeRef& in = mdr->in[0]; 

  MClientRequest *req = mdr->client_request;
  __u32 mask = req->head.args.setattr.mask;

  // xlock inode
  if (mask & (CEPH_SETATTR_MODE|CEPH_SETATTR_UID|CEPH_SETATTR_GID))
    xlocks.insert(&in->authlock);
  if (mask & (CEPH_SETATTR_MTIME|CEPH_SETATTR_ATIME|CEPH_SETATTR_SIZE))
    xlocks.insert(&in->filelock);
  if (mask & CEPH_SETATTR_CTIME)
    wrlocks.insert(&in->versionlock);

  r = locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks);
  if (r <= 0) {
    if (r == -EAGAIN)
      dispatch_client_request(mdr); // revalidate path
    return;
  }

  if (mask & CEPH_SETATTR_SIZE) {
    in->mutex_lock();
    const inode_t *pi = in->get_projected_inode();
    uint64_t old_size = MAX(pi->size, req->head.args.setattr.old_size);
    if (req->head.args.setattr.size < old_size && pi->is_truncating()) {
      in->add_waiter(CInode::WAIT_TRUNC, new C_MDS_RetryRequest(mds, mdr));
      in->mutex_unlock();
      locker->drop_locks(mdr);
      return;
    }
    in->mutex_unlock();
  }

  EUpdate *le = new EUpdate(NULL, "setattr");
  le->metablob.add_client_req(req->get_reqid(), req->get_oldest_client_tid());

  mdcache->lock_objects_for_update(mdr, in.get(), false);

  mdr->add_projected_inode(in.get(), true);
  inode_t *pi = in->project_inode();
  pi->version = in->pre_dirty();
  pi->ctime = mdr->get_op_stamp();

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

  bool truncate_smaller = false;
  if (mask & CEPH_SETATTR_SIZE) {
    uint64_t old_size = MAX(pi->size, req->head.args.setattr.old_size);
    if (req->head.args.setattr.size < old_size) {
      assert(!pi->is_truncating());
      pi->truncate(old_size, req->head.args.setattr.size);
      truncate_smaller = true;
      le->metablob.add_truncate_start(in->ino());
    } else {
      pi->size = req->head.args.setattr.size;
      pi->rstat.rbytes = pi->size;
    }

    pi->mtime = mdr->get_op_stamp();

    // adjust client's max_size?
    bool max_increased = false;
    map<client_t,client_writeable_range_t> new_ranges;
    locker->calc_new_client_ranges(in.get(), pi->size, &new_ranges, &max_increased);
    if (max_increased || pi->client_ranges != new_ranges) {
      dout(10) << " client_ranges " << pi->client_ranges << " -> " << new_ranges << dendl;
      pi->client_ranges = new_ranges;
//      ranges_changed = true;
    }
  }

  mdcache->predirty_journal_parents(mdr, &le->metablob, in.get(), NULL, PREDIRTY_PRIMARY);
  mdcache->journal_dirty_inode(mdr, &le->metablob, in.get());
  // journal inode;

  CDentryRef null_dn; 
  journal_and_reply(mdr, 0, -1, le, new C_MDS_inode_update_finish(this, mdr, truncate_smaller));
}

void Server::__mknod_finish(const MDRequestRef& mdr)
{
  mdr->wait_committing();
  Mutex::Locker l(mdr->dispatch_mutex);

  apply_allocated_inos(mdr);

  CInodeRef& in = mdr->in[0];
  CDentryRef& dn = mdr->dn[0].back();
  CInode *diri = dn->get_dir_inode();

  mdr->lock_object(diri);
  mdr->lock_object(in.get());

  dn->pop_projected_linkage();

  // be a bit hacky with the inode version, here.. we decrement it
  // just to keep mark_dirty() happen. (we didn't bother projecting
  // a new version of hte inode since it's just been created)
  in->__get_inode()->version--;
  in->mark_dirty(in->get_inode()->version + 1, mdr->ls);
  in->_mark_dirty_parent(mdr->ls, true);

  if (in->is_dir()) {
    CDirRef dir = in->get_dirfrag(frag_t());
    dir->__get_fnode()->version--;
    dir->mark_dirty(dir->get_fnode()->version + 1, mdr->ls);
  }

  // apply
  mdr->early_apply();
  respond_to_request(mdr, 0);
}

class C_MDS_mknod_finish : public ServerContext {
  MDRequestRef mdr;
public:
  C_MDS_mknod_finish(Server *srv, const MDRequestRef& r) :
    ServerContext(srv), mdr(r) { }
  void finish(int r) {
    assert(r == 0);
    server->__mknod_finish(mdr);
  }
};

void Server::handle_client_mknod(const MDRequestRef& mdr)
{
  set<SimpleLock*> rdlocks, wrlocks, xlocks;
  int r = rdlock_path_xlock_dentry(mdr, 0, rdlocks, wrlocks, xlocks, false, false);
  if (r < 0)
    return;

  CDentryRef& dn = mdr->dn[0].back();
  CInode *diri = dn->get_dir_inode();

  rdlocks.insert(&diri->authlock);
  r = locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks);
  if (r <= 0) {
    if (r == -EAGAIN)
      dispatch_client_request(mdr); // revalidate path
    return;
  }

  MClientRequest *req = mdr->client_request;
  EUpdate *le = new EUpdate(NULL, "mknod");
  le->metablob.add_client_req(req->get_reqid(), req->get_oldest_client_tid());

  mdr->lock_object(diri);
  assert(dn->get_projected_linkage()->is_null());

  unsigned mode = req->head.args.mknod.mode;
  if ((mode & S_IFMT) == 0)
    mode |= S_IFREG;

  CInodeRef newi = prepare_new_inode(mdr, dn, inodeno_t(req->head.ino), mode);
  assert(newi);
  mdr->add_locked_object(newi.get());

  inode_t *pi = newi->__get_inode();
  pi->version = dn->pre_dirty();
  pi->update_backtrace();
  pi->rdev = req->head.args.mknod.rdev;
  pi->rstat.rfiles = 1;

  // if the client created a _regular_ file via MKNOD, it's highly likely they'll
  // want to write to it (e.g., if they are reexporting NFS)
  if (pi->is_file()) {
    dout(15) << " setting a client_range too, since this is a regular file" << dendl;
    client_t client = mdr->get_client();
    pi->client_ranges[client].range.first = 0;
    pi->client_ranges[client].range.last = pi->get_layout_size_increment();
    pi->client_ranges[client].follows = 1;

    // issue a cap on the file
    int cmode = CEPH_FILE_MODE_RDWR;
    Capability *cap = mds->locker->issue_new_caps(newi.get(), cmode, mdr->session, req->is_replay());
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

  dn->push_projected_linkage(newi.get());

  mdcache->predirty_journal_parents(mdr, &le->metablob, newi.get(), dn->get_dir(), PREDIRTY_PRIMARY, 1);
  
  project_alloc_inos(mdr, newi.get(), inodeno_t(req->head.ino));

  le->metablob.add_primary_dentry(dn.get(), newi.get(), true, true, true);

  mdr->in[0] = newi;
  journal_and_reply(mdr, 0, 0, le, new C_MDS_mknod_finish(this, mdr));
}

void Server::handle_client_symlink(const MDRequestRef& mdr)
{
  set<SimpleLock*> rdlocks, wrlocks, xlocks;
  int r = rdlock_path_xlock_dentry(mdr, 0, rdlocks, wrlocks, xlocks, false, false);
  if (r < 0)
    return;

  CDentryRef& dn = mdr->dn[0].back();
  CInode *diri = dn->get_dir_inode();

  rdlocks.insert(&diri->authlock);
  r = locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks);
  if (r <= 0) {
    if (r == -EAGAIN)
      dispatch_client_request(mdr); // revalidate path
    return;
  }

  MClientRequest *req = mdr->client_request;
  EUpdate *le = new EUpdate(NULL, "symlink");
  le->metablob.add_client_req(req->get_reqid(), req->get_oldest_client_tid());

  mdr->lock_object(diri);
  assert(dn->get_projected_linkage()->is_null());

  unsigned mode = S_IFLNK | 0777; 

  CInodeRef newi = prepare_new_inode(mdr, dn, inodeno_t(req->head.ino), mode);
  assert(newi);
  mdr->add_locked_object(newi.get());

  inode_t *pi = newi->__get_inode();
  pi->version = dn->pre_dirty();
  pi->update_backtrace();

  newi->__set_symlink(req->get_path2());
  pi->size = newi->get_symlink().length();
  pi->rstat.rbytes = pi->size;
  pi->rstat.rfiles = 1;

  dn->push_projected_linkage(newi.get());

  mdcache->predirty_journal_parents(mdr, &le->metablob, newi.get(), dn->get_dir(), PREDIRTY_PRIMARY, 1);

  project_alloc_inos(mdr, newi.get(), inodeno_t(req->head.ino));

  le->metablob.add_primary_dentry(dn.get(), newi.get(), true, true);

  mdr->in[0] = newi;
  journal_and_reply(mdr, 0, 0, le, new C_MDS_mknod_finish(this, mdr));
};

void Server::handle_client_mkdir(const MDRequestRef& mdr)
{
  set<SimpleLock*> rdlocks, wrlocks, xlocks;
  int r = rdlock_path_xlock_dentry(mdr, 0, rdlocks, wrlocks, xlocks, false, false);
  if (r < 0)
    return;

  CDentryRef& dn = mdr->dn[0].back();
  CInode *diri = dn->get_dir_inode();

  rdlocks.insert(&diri->authlock);
  r = locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks);
  if (r <= 0) {
    if (r == -EAGAIN)
      dispatch_client_request(mdr); // revalidate path
    return;
  }

  MClientRequest *req = mdr->client_request;
  EUpdate *le = new EUpdate(NULL, "mkdir");
  le->metablob.add_client_req(req->get_reqid(), req->get_oldest_client_tid());

  mdr->lock_object(diri);
  assert(dn->get_projected_linkage()->is_null());

  unsigned mode = req->head.args.mkdir.mode;
  mode &= ~S_IFMT;
  mode |= S_IFDIR;

  CInodeRef newi = prepare_new_inode(mdr, dn, inodeno_t(req->head.ino), mode);
  assert(newi);
  mdr->add_locked_object(newi.get());

  inode_t *pi = newi->__get_inode();
  pi->version = dn->pre_dirty();
  pi->update_backtrace();
  pi->rstat.rsubdirs = 1;

  // issue a cap on the directory
  int cmode = CEPH_FILE_MODE_RDWR;
  Capability *cap = locker->issue_new_caps(newi.get(), cmode, mdr->session, req->is_replay());
  if (cap) {
    cap->set_wanted(0);

    // put locks in excl mode
    newi->filelock.set_state(LOCK_EXCL);
    newi->authlock.set_state(LOCK_EXCL);
    newi->xattrlock.set_state(LOCK_EXCL);
    cap->issue_norevoke(CEPH_CAP_AUTH_EXCL|CEPH_CAP_AUTH_SHARED|
			CEPH_CAP_XATTR_EXCL|CEPH_CAP_XATTR_SHARED);
  }

  CDirRef newdir = newi->get_or_open_dirfrag(frag_t());
  newdir->__get_fnode()->version = newdir->pre_dirty();
  newdir->mark_complete();

  dn->push_projected_linkage(newi.get());

  mdcache->predirty_journal_parents(mdr, &le->metablob, newi.get(), dn->get_dir(), PREDIRTY_PRIMARY, 1);

  project_alloc_inos(mdr, newi.get(), inodeno_t(req->head.ino));

  le->metablob.add_primary_dentry(dn.get(), newi.get(), true, true);
  le->metablob.add_new_dir(newdir.get());

  mdr->in[0] = newi;
  journal_and_reply(mdr, 0, 0, le, new C_MDS_mknod_finish(this, mdr));
}

void Server::handle_client_readdir(const MDRequestRef& mdr)
{
  set<SimpleLock*> rdlocks, wrlocks, xlocks;
  int r = rdlock_path_pin_ref(mdr, 0, rdlocks, false);
  if (r < 0)
    return;

  CInodeRef& diri = mdr->in[0];
  if (!diri->is_dir()) {
    respond_to_request(mdr, -ENOTDIR);
    return;
  }

  rdlocks.insert(&diri->filelock);
  rdlocks.insert(&diri->dirfragtreelock);

  r = locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks);
  if (r <= 0) {
    if (r == -EAGAIN)
      dispatch_client_request(mdr); // revalidate path
    return;
  }

  MClientRequest *req = mdr->client_request;
  string offset_str = req->get_path2();
  unsigned req_flags = (__u32)req->head.args.readdir.flags;

  unsigned max = req->head.args.readdir.max_entries;
  if (!max)
    max = (unsigned)-1;
  unsigned max_bytes = req->head.args.readdir.max_bytes;
  if (!max_bytes)
    max_bytes = 512 << 10;  // 512 KB?


  mdr->lock_object(diri.get());

  CDirRef dir = diri->get_or_open_dirfrag(frag_t());

  if (!dir->is_complete()) {
    dir->fetch(new C_MDS_RetryRequest(mds, mdr));
    mdr->unlock_object(diri.get());
    //locker->drop_locks(mdr);
    return;
  }

  bufferlist dirbl;
  encode_empty_dirstat(dirbl);

  int front_bytes = dirbl.length() + sizeof(__u32) + sizeof(__u8)*2;
  int bytes_left = max_bytes - front_bytes;

  vector<pair<CInodeRef, int64_t> >suppressed_caps;

  // build dir contents
  bufferlist dnbl;
  __u32 num_entries = 0;
  auto it = dir->begin();
  for (; num_entries < max && it != dir->end(); ++it) {
    CDentry *dn = it->second;

    const CDentry::linkage_t *dnl = dn->get_linkage(mdr->get_client(), mdr);
    if (dnl->is_null())
      continue;

    if (!offset_str.empty()) {
      dentry_key_t offset_key(CEPH_NOSNAP, offset_str.c_str(),
			      diri->hash_dentry_name(offset_str));
      if (!(offset_key < dn->get_key()))
        continue;
    }

    mdcache->touch_dentry(dn);

    bool stop = false;
    if (num_entries > 0 &&
	(int)(dnbl.length() + dn->name.length() + sizeof(__u32) + sizeof(LeaseStat)) > bytes_left)
      stop = true;

    CInodeRef in = dnl->get_inode();
    // remote link?
    if (dnl->is_remote() && !in) {
      in = mdcache->get_inode(dnl->get_remote_ino());
      if (in) {
	dn->link_remote(dnl, in);
      } else if (dn->is_bad_remote_ino(dnl)) {
	// skip bad remote linkage
	continue;
      } else {
	if (num_entries > 0) {
	  mdcache->open_remote_dentry(dn, dnl->get_remote_ino(), dnl->get_remote_d_type());
	} else {
	  mdcache->open_remote_dentry(dn, dnl->get_remote_ino(), dnl->get_remote_d_type(),
				      new C_MDS_RetryRequest(mds, mdr));
	  mdr->unlock_object(diri.get());
	  //locker->drop_locks(mdr);
	}
	stop = true;
      }
    }

    if (stop) {
      // touch everything I have. It's likely they will be used later
      for (auto p = dir->rbegin(); p != dir->rend(); ++p) {
	CDentry *odn = p->second;
	if (!odn->get_projected_linkage()->is_null())
	  mdcache->touch_dentry(odn);
	if (odn == dn)
	  break;
      }
      if (num_entries == 0)
	return;
      break;
    }

    assert(in);
    unsigned start_len = dnbl.length();

    // dentry
    ::encode(dn->name, dnbl);
    encode_null_lease(dnbl);

    in->mutex_lock();
    int64_t r = in->encode_inodestat(dnbl, mdr->session, 0, bytes_left - (int)dnbl.length());
    in->mutex_unlock();
    if (r < 0) {
      // chop off dn->name, lease
      bufferlist keep;
      keep.substr_of(dnbl, 0, start_len);
      dnbl.swap(keep);
      break;
    }
    if (r > 0)
      suppressed_caps.push_back(make_pair(in, r));

    assert(r >= 0);
    num_entries++;
  }

  bool complete = false;
  __u16 flags = 0;
  if (it == dir->end()) {
    flags = CEPH_READDIR_FRAG_END;
    complete = offset_str.empty(); // FIXME: what purpose does this serve
    if (complete)
      flags |= CEPH_READDIR_FRAG_COMPLETE;
  }

  // client only understand END and COMPLETE flags ?
  if (req_flags & CEPH_READDIR_REPLY_BITFLAGS) {
    flags |= CEPH_READDIR_HASH_ORDER;
  }

  // finish final blob
  ::encode(num_entries, dirbl);
  ::encode(flags, dirbl);
  dirbl.claim_append(dnbl);

  mdr->reply_extra_bl = dirbl;
  mdr->tracei = 0;

  respond_to_request(mdr, 0);

  if (!suppressed_caps.empty()) {
    client_t client = mdr->get_client();
    for (auto& p : suppressed_caps) {
      CInodeRef &in = p.first;
      CObject::Locker l(in.get());
      Capability *cap = in->get_client_cap(client);
      if (cap && cap->get_cap_id() == (uint64_t)p.second)
	cap->dec_suppress();
    }
  }
}

CDentryRef Server::prepare_stray_dentry(const MDRequestRef& mdr, CInode *in)
{
  return mdcache->get_or_create_stray_dentry(in);
}

void Server::__unlink_finish(const MDRequestRef& mdr, version_t dnpv)
{
  mdr->wait_committing();
  Mutex::Locker l(mdr->dispatch_mutex);

  CInodeRef& in = mdr->in[0];
  CDentryRef& dn = mdr->dn[0].back();
  CDentryRef& straydn = mdr->straydn;

  mdcache->lock_parents_for_linkunlink(mdr, in.get(), dn.get(), true);

  if (straydn) {
    mdr->lock_object(straydn->get_dir_inode());
  }

  mdr->lock_object(in.get());

  dn->get_dir()->unlink_inode(dn.get());
  dn->pop_projected_linkage();
  
  if (straydn) {
    straydn->pop_projected_linkage();
  }

  dn->mark_dirty(dnpv, mdr->ls);
  mdr->early_apply();

  if (in->get_inode()->nlink == 0) {
    in->state_set(CInode::STATE_ORPHAN);
    if (in->has_dirfrags()) {
      list<CDir*> ls;
      in->get_dirfrags(ls);
      for (auto p = ls.begin(); p != ls.end(); ++p)
	(*p)->touch_dentries_bottom();
    }
  }

  respond_to_request(mdr, 0);
}

bool Server::directory_is_nonempty(CInodeRef& diri)
{
  diri->mutex_assert_locked_by_me();
  dout(10) << "directory_is_nonempty" << *diri << dendl;
  assert(diri->filelock.can_read(-1));

  frag_info_t dirstat;
  version_t dirstat_version = diri->get_projected_inode()->dirstat.version;

  list<CDir*> ls;
  diri->get_dirfrags(ls);
  for (auto p = ls.begin(); p != ls.end(); ++p) {
    CDir* dir = *p;
    const fnode_t *pf = dir->get_projected_fnode();
    if (pf->fragstat.size()) {
      dout(10) << "directory_is_nonempty has " << pf->fragstat.size()
	       << " items " << *dir << dendl;
      return true;
    }

    if (pf->accounted_fragstat.version == dirstat_version)
      dirstat.add(pf->accounted_fragstat);
    else
      dirstat.add(pf->fragstat);
  }
  return dirstat.size() != diri->get_projected_inode()->dirstat.size();
}

class C_MDS_unlink_finish : public ServerContext {
  MDRequestRef mdr;
  version_t dnpv;
public:
  C_MDS_unlink_finish(Server *srv, const MDRequestRef& r, version_t pv) :
    ServerContext(srv), mdr(r), dnpv(pv) {}
  void finish(int r) {
    assert(r==0);
    server->__unlink_finish(mdr, dnpv);
  }
};

void Server::handle_client_unlink(const MDRequestRef& mdr)
{
  set<SimpleLock*> rdlocks, wrlocks, xlocks;
  int r = rdlock_path_xlock_dentry(mdr, 0, rdlocks, wrlocks, xlocks, true, true);
  if (r < 0)
    return;

  MClientRequest *req = mdr->client_request;
  // rmdir or unlink?
  bool rmdir = req->get_op() == CEPH_MDS_OP_RMDIR;

  CDentryRef& dn = mdr->dn[0].back();
  CInodeRef& in = mdr->in[0];

  mdr->lock_object(in.get());
  if (in->is_dir()) {               
    if (!rmdir) {                   
      respond_to_request(mdr, -EISDIR);
      return;                       
    }                               
  } else {                          
    if (rmdir) {                    
      respond_to_request(mdr, -ENOTDIR);
      return;                       
    }                               
  } 
  
  bool need_stray = (dn.get() == in->get_projected_parent_dn());
  mdr->unlock_object(in.get());

  CDentryRef straydn;
  if (need_stray) {
    straydn = prepare_stray_dentry(mdr, in.get());
    wrlocks.insert(&straydn->get_dir_inode()->filelock);
    wrlocks.insert(&straydn->get_dir_inode()->nestlock);
    xlocks.insert(&straydn->lock);
  }

  xlocks.insert(&in->linklock);
  if (rmdir)
    rdlocks.insert(&in->filelock);   // to verify it's empty
  
  r = locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks);
  if (r <= 0) {
    if (r == -EAGAIN)
      dispatch_client_request(mdr); // revalidate path
    return;
  }

  EUpdate *le = new EUpdate(NULL, "unlink");
  le->metablob.add_client_req(req->get_reqid(), req->get_oldest_client_tid());

  mdcache->lock_parents_for_linkunlink(mdr, in.get(), dn.get(), false);

  const CDentry::linkage_t *dnl = dn->get_projected_linkage();

  if (straydn)
    assert(dnl->is_primary() && dnl->get_inode() == in.get());
  else
    assert((dnl->is_remote() && dnl->get_remote_ino() == in->ino()));

  if (straydn)
    mdr->lock_object(straydn->get_dir_inode());

  mdr->lock_object(in.get());

  if (rmdir && directory_is_nonempty(in)) {
    respond_to_request(mdr, -ENOTEMPTY);
    return;                       
  }

  if (straydn) {
    straydn->push_projected_linkage(in.get());
  }

  version_t dnpv = dn->pre_dirty();

  mdr->add_projected_inode(in.get(), true);
  inode_t *pi = in->project_inode();
  pi->version = in->pre_dirty();
  pi->ctime = mdr->get_op_stamp();
  pi->nlink--;

  dn->push_projected_linkage();

  if (dnl->is_primary()) {
    pi->update_backtrace();
    mdcache->predirty_journal_parents(mdr, &le->metablob, in.get(), straydn->get_dir(), PREDIRTY_PRIMARY|PREDIRTY_DIR, 1);
    mdcache->predirty_journal_parents(mdr, &le->metablob, in.get(), dn->get_dir(), PREDIRTY_PRIMARY|PREDIRTY_DIR, -1);
    le->metablob.add_primary_dentry(straydn.get(), in.get(), true, true);
  } else {
    mdcache->predirty_journal_parents(mdr, &le->metablob, in.get(), NULL, PREDIRTY_PRIMARY);
    mdcache->predirty_journal_parents(mdr, &le->metablob, in.get(), dn->get_dir(), PREDIRTY_DIR, -1);
    mdcache->journal_dirty_inode(mdr, &le->metablob, in.get());
  }
  le->metablob.add_null_dentry(dn.get(), true);

  mdr->straydn = straydn;
  journal_and_reply(mdr, -1, 0, le, new C_MDS_unlink_finish(this, mdr, dnpv));
}

void Server::__link_finish(const MDRequestRef& mdr, version_t dnpv)
{ 
  mdr->wait_committing();
  Mutex::Locker l(mdr->dispatch_mutex);

  CInodeRef& in = mdr->in[1];
  CDentryRef& dn = mdr->dn[0].back();

  mdcache->lock_parents_for_linkunlink(mdr, in.get(), dn.get(), true);

  mdr->lock_object(in.get());

  dn->pop_projected_linkage();

  dn->mark_dirty(dnpv, mdr->ls);
  mdr->early_apply();

  mds->server->respond_to_request(mdr, 0);
}

class C_MDS_link_finish : public ServerContext {
  MDRequestRef mdr;
  version_t dnpv;
  public:
  C_MDS_link_finish(Server *srv, const MDRequestRef& r, version_t pv)
    : ServerContext(srv), mdr(r), dnpv(pv) {}
  void finish(int r) {
    assert(r==0);
    server->__link_finish(mdr, dnpv);
  }
};

void Server::handle_client_link(const MDRequestRef& mdr)
{
  int r;
  set<SimpleLock*> rdlocks, wrlocks, xlocks;
  r = rdlock_path_xlock_dentry(mdr, 0, rdlocks, wrlocks, xlocks, false, false);
  if (r < 0)
    return;

  r = rdlock_path_pin_ref(mdr, 1, rdlocks, false);
  if (r < 0)
    return;

  CDentryRef& dn = mdr->dn[0].back();
  CInodeRef& in = mdr->in[1];

  if (in->is_dir()) {
    respond_to_request(mdr, -EINVAL);
    return;
  }

  xlocks.insert(&in->linklock);

  r = locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks);
  if (r <= 0) {
    if (r == -EAGAIN)
      dispatch_client_request(mdr); // revalidate path
    return;
  }

  MClientRequest *req = mdr->client_request;
  EUpdate *le = new EUpdate(NULL, "link");
  le->metablob.add_client_req(req->get_reqid(), req->get_oldest_client_tid());

  mdcache->lock_parents_for_linkunlink(mdr, in.get(), dn.get(), false);
  assert(dn->get_projected_linkage()->is_null());

  mdr->lock_object(in.get());

  dn->push_projected_linkage(in->ino(), in->d_type());

  version_t dnpv = dn->pre_dirty();

  mdr->add_projected_inode(in.get(), true);
  inode_t *pi = in->project_inode();
  pi->version = in->pre_dirty();
  pi->ctime = mdr->get_op_stamp();
  pi->nlink++;

  mdcache->predirty_journal_parents(mdr, &le->metablob, in.get(), dn->get_dir(), PREDIRTY_DIR, 1);
  mdcache->predirty_journal_parents(mdr, &le->metablob, in.get(), 0, PREDIRTY_PRIMARY);
  mdcache->journal_dirty_inode(mdr, &le->metablob, in.get());
  le->metablob.add_remote_dentry(dn.get(), true, in->ino(), in->d_type());  // new remote

  journal_and_reply(mdr, 1, 0, le, new C_MDS_link_finish(this, mdr, dnpv));
}

void Server::__rename_finish(const MDRequestRef& mdr, version_t srcdn_pv, version_t destdn_pv)
{
  mdr->wait_committing();
  Mutex::Locker l(mdr->dispatch_mutex);

  CDentryRef& srcdn = mdr->dn[1].back();
  CInodeRef& srci = mdr->in[1];
  CDentryRef& destdn = mdr->dn[0].back();
  CInodeRef& oldin = mdr->in[0];
  CDentryRef& straydn = mdr->straydn;

  mdcache->lock_parents_for_rename(mdr, srci.get(), oldin.get(),
				   srcdn.get(), destdn.get(), true);
  if (straydn) {
    mdr->lock_object(straydn->get_dir_inode());
  }

  if (oldin && oldin != srci) {
    if (srci->is_lt(oldin.get())) {
      srci->mutex_lock();
      oldin->mutex_lock();
    } else {
      oldin->mutex_lock();
      srci->mutex_lock();
    }
    mdr->add_locked_object(srci.get());
    mdr->add_locked_object(oldin.get());
  } else {
    mdr->lock_object(srci.get());
  }

  bool linkmerge = (srci == oldin) && srcdn->get_dir_inode()->is_stray();

  if (!linkmerge) {
    const CDentry::linkage_t *dest_dnl = destdn->get_linkage();
    if (dest_dnl->is_primary()) {
      assert(straydn);

      destdn->get_dir()->unlink_inode(destdn.get());

      straydn->pop_projected_linkage();
    } else if (dest_dnl->is_remote()) {
      destdn->get_dir()->unlink_inode(destdn.get());
    }
  }

  bool srcdn_was_remote = srcdn->get_linkage()->is_remote();
  srcdn->get_dir()->unlink_inode(srcdn.get());

  if (srcdn_was_remote) {
    if (!linkmerge) {
      destdn->pop_projected_linkage();
    }
  } else {
    if (linkmerge) {
      destdn->get_dir()->unlink_inode(destdn.get());
    }
    destdn->pop_projected_linkage();
  }

  srcdn->pop_projected_linkage();

  srcdn->mark_dirty(srcdn_pv, mdr->ls);
  if (!linkmerge && srcdn_was_remote)
    destdn->mark_dirty(destdn_pv, mdr->ls);

  if (mdr->hold_rename_dir_mutex) {
    mdcache->unlock_rename_dir_mutex();
    mdr->hold_rename_dir_mutex = false;
  }

  mdr->early_apply();

  if (oldin && oldin->get_inode()->nlink == 0) {
    oldin->state_set(CInode::STATE_ORPHAN);
    if (oldin->has_dirfrags()) {
      list<CDir*> ls;
      oldin->get_dirfrags(ls);
      for (auto p = ls.begin(); p != ls.end(); ++p)
	(*p)->touch_dentries_bottom();
    }
  }

  respond_to_request(mdr, 0);
}

class C_MDS_rename_finish : public ServerContext {
  MDRequestRef mdr;
  version_t srcdn_pv;
  version_t destdn_pv;
  public:
  C_MDS_rename_finish(Server *srv, const MDRequestRef& r, version_t spv, version_t dpv)
    : ServerContext(srv), mdr(r), srcdn_pv(spv), destdn_pv(dpv) {}
  void finish(int r) {
    assert(r==0);
    server->__rename_finish(mdr, srcdn_pv, destdn_pv);
  }
};

void Server::handle_client_rename(const MDRequestRef& mdr)
{
  int r;
  set<SimpleLock*> rdlocks, wrlocks, xlocks;
  r = rdlock_path_xlock_dentry(mdr, 0, rdlocks, wrlocks, xlocks, true, false);
  if (r < 0)
    return;

  r = rdlock_path_xlock_dentry(mdr, 1, rdlocks, wrlocks, xlocks, true, true);
  if (r < 0)
    return;

  CDentryRef& srcdn = mdr->dn[1].back();
  CInodeRef& srci = mdr->in[1];
  CDentryRef& destdn = mdr->dn[0].back();
  CInodeRef& oldin = mdr->in[0];

  bool need_stray = false;
  if (oldin) {
    mdr->lock_object(oldin.get());
    if (oldin->is_dir() && !srci->is_dir()) {
      respond_to_request(mdr, -EISDIR);
      return;
    }
    if (!oldin->is_dir() && srci->is_dir()) {
      respond_to_request(mdr, -ENOTDIR);
      return;
    }
    if (srci == oldin) {
      if (!srcdn->get_dir_inode()->is_stray()) {
	respond_to_request(mdr, 0);  // no-op.  POSIX makes no sense.
	return;
      }
    }
    need_stray = (srci != oldin && destdn.get() == oldin->get_projected_parent_dn());
    mdr->unlock_object(oldin.get());
  }

  CDentryRef straydn;
  if (need_stray) {
    straydn = prepare_stray_dentry(mdr, oldin.get());
    wrlocks.insert(&straydn->get_dir_inode()->filelock);
    wrlocks.insert(&straydn->get_dir_inode()->nestlock);
    xlocks.insert(&straydn->lock);
  }

  // we need to update srci's ctime.  xlock its least contended lock to do that...
  xlocks.insert(&srci->linklock);
  // xlock oldin (for nlink--)
  if (oldin) {
    xlocks.insert(&oldin->linklock);
    if (oldin->is_dir())
      rdlocks.insert(&oldin->filelock);
  }

  r = locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks);
  if (r <= 0) {
    if (r == -EAGAIN)
      dispatch_client_request(mdr); // revalidate path
    return;
  }

  r = mdcache->lock_parents_for_rename(mdr, srci.get(), oldin.get(),
		  		       srcdn.get(), destdn.get(), false);
  if (r < 0) {
    respond_to_request(mdr, r);
    return;
  }

  MClientRequest *req = mdr->client_request;
  EUpdate *le = new EUpdate(NULL, "rename");
  le->metablob.add_client_req(req->get_reqid(), req->get_oldest_client_tid());

  const CDentry::linkage_t *src_dnl = srcdn->get_projected_linkage();
  const CDentry::linkage_t *dest_dnl = destdn->get_projected_linkage();
  
  assert((src_dnl->is_primary() && src_dnl->get_inode() == srci.get()) ||
	 (src_dnl->is_remote() && src_dnl->get_remote_ino() == srci->ino()));
  assert((!oldin && dest_dnl->is_null()) ||
	 (need_stray && dest_dnl->is_primary() && dest_dnl->get_inode() == oldin.get()) ||
	 (!need_stray && dest_dnl->is_remote() && dest_dnl->get_remote_ino() == oldin->ino()));

  bool linkmerge = srci == oldin;
  if (linkmerge) {
    assert(srcdn->get_dir_inode()->is_stray());
    assert(src_dnl->is_primary());
  }

  if (straydn)
    mdr->lock_object(straydn->get_dir_inode());

  if (oldin && oldin != srci) {
    if (srci->is_lt(oldin.get())) {
      srci->mutex_lock();
      oldin->mutex_lock();
    } else {
      oldin->mutex_lock();
      srci->mutex_lock();
    }
    mdr->add_locked_object(srci.get());
    mdr->add_locked_object(oldin.get());
  } else {
    mdr->lock_object(srci.get());
  }

  if (oldin && oldin->is_dir() && directory_is_nonempty(oldin)) {
    if (mdr->hold_rename_dir_mutex) {
      mdcache->unlock_rename_dir_mutex();
      mdr->hold_rename_dir_mutex = false;
    }
    respond_to_request(mdr, -ENOTEMPTY);
  }

  inode_t *pi;
  inode_t *tpi = NULL;

  version_t srcdn_pv = srcdn->pre_dirty();
  version_t destdn_pv = 0;
  if (!linkmerge && src_dnl->is_remote())
    destdn_pv = destdn->pre_dirty();

  if (!linkmerge && !dest_dnl->is_null()) {
    mdr->add_projected_inode(oldin.get(), true);
    tpi = oldin->project_inode();
    if (dest_dnl->is_primary()) {
      straydn->push_projected_linkage(oldin.get());
      tpi->version = straydn->pre_dirty(tpi->version);
      tpi->update_backtrace();
    } else {
      tpi->version = oldin->pre_dirty();
    }
  }

  mdr->add_projected_inode(srci.get(), true);
  if (src_dnl->is_remote()) {
    pi = srci->project_inode();
    pi->version = srci->pre_dirty();
    if (!linkmerge) {
      destdn->push_projected_linkage(src_dnl->get_remote_ino(), src_dnl->get_remote_d_type());
    }
  } else {
    destdn->push_projected_linkage(srci.get());
    pi = srci->project_inode();
    pi->version = destdn->pre_dirty(pi->version);
    pi->update_backtrace();
  }

  if (!linkmerge) {
    pi->ctime = mdr->get_op_stamp();
    if (tpi) {
      tpi->ctime = mdr->get_op_stamp();
      tpi->nlink--;
    }
  }

  srcdn->push_projected_linkage();

  // unlock rename_dir_mutex after projecting linkages
  if (mdr->hold_rename_dir_mutex) {
    mdcache->unlock_rename_dir_mutex();
    mdr->hold_rename_dir_mutex = false;
  }

  if (straydn) {
    mdcache->predirty_journal_parents(mdr, &le->metablob, oldin.get(), straydn->get_dir(),
				      PREDIRTY_PRIMARY|PREDIRTY_DIR, 1);
  }
  int predirty_dir = linkmerge ? 0 : PREDIRTY_DIR;
  int predirty_primary;
  if (!dest_dnl->is_null()) {
    predirty_primary = dest_dnl->is_primary() ? PREDIRTY_PRIMARY : 0;
    mdcache->predirty_journal_parents(mdr, &le->metablob, oldin.get(), destdn->get_dir(),
				      predirty_dir|predirty_primary , -1);
  }

  predirty_primary = (src_dnl->is_primary() && srcdn->get_dir() != destdn->get_dir()) ? PREDIRTY_PRIMARY : 0;
  mdcache->predirty_journal_parents(mdr, &le->metablob, srci.get(), destdn->get_dir(),
				    predirty_dir|predirty_primary, 1);
  mdcache->predirty_journal_parents(mdr, &le->metablob, srci.get(), srcdn->get_dir(),
				    predirty_dir|predirty_primary, -1);

  if (!linkmerge) {
    if (dest_dnl->is_remote()) {
      mdcache->predirty_journal_parents(mdr, &le->metablob, oldin.get(), NULL, PREDIRTY_PRIMARY);
    }
    if (src_dnl->is_remote()) {
      mdcache->predirty_journal_parents(mdr, &le->metablob, srci.get(), NULL, PREDIRTY_PRIMARY);
    }
  }

  if (straydn)
    le->metablob.add_primary_dentry(straydn.get(), oldin.get(), true, true);

  if (src_dnl->is_remote()) {
    if (!linkmerge) {
      le->metablob.add_remote_dentry(destdn.get(), true, src_dnl->get_remote_ino(), src_dnl->get_remote_d_type());
    } else {
      le->metablob.add_primary_dentry(destdn.get(), srci.get(), true);
    }
  } else {
    le->metablob.add_primary_dentry(destdn.get(), srci.get(), true, true);
  }

  le->metablob.add_null_dentry(srcdn.get(), true);

  if (!linkmerge) {
    if (dest_dnl->is_remote()) {
      le->metablob.add_primary_dentry(oldin->get_projected_parent_dn(), oldin.get(), true);
    }
    if (src_dnl->is_remote()) {
      le->metablob.add_primary_dentry(srci->get_projected_parent_dn(), srci.get(), true);
    }
  }

  mdr->straydn = straydn;
  journal_and_reply(mdr, 1, 0, le, new C_MDS_rename_finish(this, mdr, srcdn_pv, destdn_pv));
}

void Server::do_open_truncate(const MDRequestRef& mdr, int cmode)
{
  MClientRequest *req = mdr->client_request;

  CInodeRef& in = mdr->in[0];

  int tracedn = -1;
  if (req->get_dentry_wanted()) {
    assert(!mdr->dn[0].empty());
    CDentryRef &dn = mdr->dn[0].back();
    mdcache->lock_parents_for_linkunlink(mdr, in.get(), dn.get(), false);
    mdr->lock_object(in.get());
    tracedn = 0;
  } else {
    mdcache->lock_objects_for_update(mdr, in.get(), false);
  }

  EUpdate *le = new EUpdate(NULL, "open_truncate");
  le->metablob.add_client_req(req->get_reqid(), req->get_oldest_client_tid());

  locker->issue_new_caps(in.get(), cmode, mdr->session, req->is_replay());

  mdr->add_projected_inode(in.get(), true);
  inode_t *pi = in->project_inode();
  pi->version = in->pre_dirty();
  pi->ctime = mdr->get_op_stamp();

  assert(!pi->is_truncating());
  uint64_t old_size = MAX(pi->size, req->head.args.setattr.old_size);
  if (old_size > 0) {
    pi->truncate(old_size, 0);
    le->metablob.add_truncate_start(in->ino());
  }

  // adjust client's max_size?
  bool max_increased = false;
  map<client_t,client_writeable_range_t> new_ranges;
  locker->calc_new_client_ranges(in.get(), pi->size, &new_ranges, &max_increased);
  if (max_increased || pi->client_ranges != new_ranges) {
    dout(10) << " client_ranges " << pi->client_ranges << " -> " << new_ranges << dendl;
    pi->client_ranges = new_ranges;
  }


  mdcache->predirty_journal_parents(mdr, &le->metablob, in.get(), NULL, PREDIRTY_PRIMARY);
  // journal inode;
  mdcache->journal_dirty_inode(mdr, &le->metablob, in.get());
  le->metablob.add_opened_ino(in->ino());

  journal_and_reply(mdr, 0, tracedn, le,
		    new C_MDS_inode_update_finish(this, mdr, old_size > 0),
		    true);
}

void Server::handle_client_open(const MDRequestRef& mdr)
{
  MClientRequest *req = mdr->client_request;

  int flags = req->head.args.open.flags;
  int cmode = ceph_flags_to_mode(flags);

  if (cmode < 0) {
    respond_to_request(mdr, -EINVAL);
    return;
  }

  set<SimpleLock*> rdlocks, wrlocks, xlocks;
  int r = rdlock_path_pin_ref(mdr, 0, rdlocks, req->get_dentry_wanted());
  if (r < 0)
    return;

  CInodeRef& in = mdr->in[0];

  mdr->lock_object(in.get());

  if (!in->is_file()) {
    // can only open non-regular inode with mode FILE_MODE_PIN, at least for now.
    cmode = CEPH_FILE_MODE_PIN;
    // the inode is symlink and client wants to follow it, ignore the O_TRUNC flag.
    if (in->is_symlink() && !(flags & O_NOFOLLOW))
      flags &= ~O_TRUNC;
  }

  if ((flags & O_DIRECTORY) && !in->is_dir() && !in->is_symlink()) {
    dout(7) << "specified O_DIRECTORY on non-directory " << *in << dendl;
    respond_to_request(mdr, -EINVAL);
    return;
  }

  if ((flags & O_TRUNC) && !in->is_file()) {
    dout(7) << "specified O_TRUNC on !(file|symlink) " << *in << dendl;
    // we should return -EISDIR for directory, return -EINVAL for other non-regular
    respond_to_request(mdr, in->is_dir() ? -EISDIR : -EINVAL);
    return;
  }

  if ((flags & O_TRUNC) && !mdr->completed)  {
    xlocks.insert(&in->filelock);
  }

  mdr->unlock_object(in.get());

  r = locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks);
  if (r <= 0) {
    if (r == -EAGAIN)
      dispatch_client_request(mdr); // revalidate path
    return;
  }

  if ((flags & O_TRUNC) && !mdr->completed)  {
    in->mutex_lock();
    if (in->get_projected_inode()->is_truncating()) {
      in->add_waiter(CInode::WAIT_TRUNC, new C_MDS_RetryRequest(mds, mdr));
      in->mutex_unlock();
      locker->drop_locks(mdr);
    } else {
      in->mutex_unlock();
      do_open_truncate(mdr, cmode);
    }
    return;
  }

  if (req->get_dentry_wanted()) {
    assert(!mdr->dn[0].empty());
    CDentryRef &dn = mdr->dn[0].back();
    mdr->lock_object(dn->get_dir_inode());
    mdr->tracedn = 0;
  }

  mdr->lock_object(in.get());

  if (in->is_file() || in->is_dir()) {
    // register new cap
    Capability *cap = mds->locker->issue_new_caps(in.get(), cmode, mdr->session, req->is_replay());
    dout(12) << "open issued caps " << ccap_string(cap->pending())
	     << " for " << req->get_source() << " on " << *in << dendl;
  }

  // increase max_size?
  if (cmode & CEPH_FILE_MODE_WR) {
    bool parent_locked = false;
    if (!in->is_base()) {
      CDentry *dn = in->get_projected_parent_dn();
      parent_locked = mdr->is_object_locked(dn->get_dir_inode());
    } 
    locker->check_inode_max_size(in.get(), parent_locked);
  }

  mdr->tracei = 0;
  respond_to_request(mdr, 0);
}

void Server::handle_client_openc(const MDRequestRef& mdr)
{
  MClientRequest *req = mdr->client_request;
  int cmode = ceph_flags_to_mode(req->head.args.open.flags);
  if (cmode < 0) {
    respond_to_request(mdr, -EINVAL);
    return;
  }

  bool excl = (req->head.args.open.flags & O_EXCL);

  set<SimpleLock*> rdlocks, wrlocks, xlocks;
  int r = rdlock_path_xlock_dentry(mdr, 0, rdlocks, wrlocks, xlocks, !excl, false);
  if (r < 0)
    return;

  if (mdr->in[0]) {
    assert(!excl);
    handle_client_open(mdr);
    return;
  }

  CDentryRef& dn = mdr->dn[0].back();
  CInode *diri = dn->get_dir_inode();

  rdlocks.insert(&diri->authlock);
  r = locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks);
  if (r <= 0) {
    if (r == -EAGAIN)
      dispatch_client_request(mdr); // revalidate path
    return;
  }

  EUpdate *le = new EUpdate(NULL, "openc");
  le->metablob.add_client_req(req->get_reqid(), req->get_oldest_client_tid());

  mdr->lock_object(diri);
  assert(dn->get_projected_linkage()->is_null());

  unsigned mode = req->head.args.open.mode | S_IFREG;
  CInodeRef newi = prepare_new_inode(mdr, dn, inodeno_t(req->head.ino), mode);
  assert(newi);
  mdr->add_locked_object(newi.get());

  inode_t *pi = newi->__get_inode();
  pi->version = dn->pre_dirty();
  pi->update_backtrace();
  pi->rstat.rfiles = 1;

  if (cmode & CEPH_FILE_MODE_WR) {
    client_t client = mdr->get_client();
    pi->client_ranges[client].range.first = 0;
    pi->client_ranges[client].range.last = pi->get_layout_size_increment();
    pi->client_ranges[client].follows = 1;
  }

  // do the open
  locker->issue_new_caps(newi.get(), cmode, mdr->session, req->is_replay());
  newi->authlock.set_state(LOCK_EXCL);
  newi->xattrlock.set_state(LOCK_EXCL);

  dn->push_projected_linkage(newi.get());

  mdcache->predirty_journal_parents(mdr, &le->metablob, newi.get(), dn->get_dir(), PREDIRTY_PRIMARY, 1);

  project_alloc_inos(mdr, newi.get(), inodeno_t(req->head.ino));

  le->metablob.add_opened_ino(newi->ino());
  le->metablob.add_primary_dentry(dn.get(), newi.get(), true, true, true);

  mdr->in[0] = newi;
  journal_and_reply(mdr, 0, 0, le, new C_MDS_mknod_finish(this, mdr));
}


/* This function DOES put the passed message before returning*/
void Server::handle_client_reconnect(MClientReconnect *m)
{

  dout(7) << "handle_client_reconnect " << m->get_source() << dendl;
  client_t from = m->get_source().num();
  Session *session = mds->get_session(m);
  assert(session);

  if (!mds->is_reconnect() && mds->get_want_state() == CEPH_MDS_STATE_RECONNECT) {
    dout(10) << " we're almost in reconnect state (mdsmap delivery race?); waiting" << dendl;
    mds->wait_for_reconnect(new C_MDS_RetryMessage(mds, m));
    return;
  }

  utime_t delay = ceph_clock_now(g_ceph_context);
  delay -= reconnect_start;
  dout(10) << " reconnect_start " << reconnect_start << " delay " << delay << dendl;

  bool deny = false;
  if (!mds->is_reconnect()) {
    // XXX maybe in the future we can do better than this?
    dout(1) << " no longer in reconnect state, ignoring reconnect, sending close" << dendl;
    mds->clog->info() << "denied reconnect attempt (mds is "
		      << ceph_mds_state_name(mds->get_state()) << ") from "
		      << m->get_source_inst() << " after " << delay << " (allowed interval "
		      << g_conf->mds_reconnect_timeout << ")\n";
    deny = true;
  } else {
    session->mutex_lock();
    if (session->is_closed()) {
      dout(1) << " session is closed, ignoring reconnect, sending close" << dendl;
      mds->clog->info() << "denied reconnect attempt (mds is "
			<< ceph_mds_state_name(mds->get_state())
			<< ") from " << m->get_source_inst() << " (session is closed)\n";
      deny = true;
    }
    session->mutex_unlock();
  }

  if (deny) {
    m->get_connection()->send_message(new MClientSession(CEPH_SESSION_CLOSE));
    m->put();
    return;
  }
  
  m->get_connection()->send_message(new MClientSession(CEPH_SESSION_OPEN));
  mds->clog->debug() << "reconnect by " << session->info.inst << " after " << delay << "\n";

  // snaprealms
  for (auto p = m->realms.begin(); p != m->realms.end(); ++p) {
  }

  // caps
  for (auto p = m->caps.begin(); p != m->caps.end(); ++p) {
    mdcache->note_client_cap_id(p->second.capinfo.cap_id);

    CInodeRef in = mdcache->get_inode(p->first);

//    if (in && in->state_test(CInode::STATE_PURGING))
//      continue;

    if (in) {
      // we recovered it, and it's ours.  take note.
      dout(15) << "open cap realm " << inodeno_t(p->second.capinfo.snaprealm)
               << " on " << *in << dendl;
      in->mutex_lock();
      in->reconnect_cap(p->second, session);
      in->mutex_unlock();
      mdcache->add_reconnected_cap(from, p->first, p->second);
//      recover_filelocks(in, p->second.flockbl, m->get_orig_source().num());
    } else {
      // don't know if the inode is mine
      dout(10) << "missing ino " << p->first << ", will load later" << dendl;
      p->second.path.clear(); // we don't need path
      mdcache->add_cap_reconnect(p->first, from, p->second);
    }
  }

  // remove from gather set
  mdcache->lock_client_reconnect();
  client_reconnect_gather.erase(from);
  if (client_reconnect_gather.empty())
    reconnect_gather_finish();
  mdcache->unlock_client_reconnect();

  m->put();
}

void Server::reconnect_clients(MDSContextBase *c)
{
  set<client_t> clients;
  mds->sessionmap->mutex_lock();
  mds->sessionmap->get_client_set(clients);
  mds->sessionmap->mutex_unlock();

  mdcache->lock_client_reconnect();
  client_reconnect_gather.swap(clients);
  reconnect_done = c;

  if (client_reconnect_gather.empty()) {
    dout(7) << "reconnect_clients -- no sessions, doing nothing." << dendl;
    reconnect_gather_finish();
  } else {

    // clients will get the mdsmap and discover we're reconnecting via the monitor.
    reconnect_start = ceph_clock_now(g_ceph_context);
    dout(1) << "reconnect_clients -- " << client_reconnect_gather.size() << " sessions" << dendl;
    //  mds->sessionmap.dump();
  }

  mdcache->unlock_client_reconnect();
}

void Server::reconnect_gather_finish(int failed)
{
  dout(7) << "reconnect_gather_finish.  failed on " << failed << " clients" << dendl;
  assert(reconnect_done);
  mds->queue_context(reconnect_done);
  reconnect_done = NULL;
}


// FIXME:: this can race with in-processing reconnect
void Server::reconnect_tick()
{
  mdcache->lock_client_reconnect();
  if (!reconnect_done) {
    mdcache->unlock_client_reconnect();
    return;
  }

  utime_t reconnect_end = reconnect_start;
  reconnect_end += g_conf->mds_reconnect_timeout;

  if (ceph_clock_now(g_ceph_context) >= reconnect_end &&
      !client_reconnect_gather.empty()) {

    dout(10) << "reconnect timed out" << dendl;
    int failed_reconnects = 0;
    while (!client_reconnect_gather.empty()) {
      client_t client = *client_reconnect_gather.begin();
      mdcache->unlock_client_reconnect();

      mds->sessionmap->mutex_lock();
      SessionRef session = mds->sessionmap->get_session(entity_name_t::CLIENT(client.v));
      mds->sessionmap->mutex_unlock();

      if (session) {
	session->mutex_lock();
	dout(1) << "reconnect gave up on " << session->get_source() << dendl;
	kill_session(session.get(), NULL);
	failed_reconnects++;
	session->mutex_unlock();
      }

      mdcache->lock_client_reconnect();
    }

    if (reconnect_done)
      reconnect_gather_finish(failed_reconnects);
  }
  mdcache->unlock_client_reconnect();
}
