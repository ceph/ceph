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

#include "MDSTableServer.h"
#include "MDSRank.h"
#include "MDLog.h"
#include "msg/Messenger.h"

#include "events/ETableServer.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds." << rank << ".tableserver(" << get_mdstable_name(table) << ") "

void MDSTableServer::handle_request(const MMDSTableRequest::const_ref &req)
{
  ceph_assert(req->op >= 0);
  switch (req->op) {
  case TABLESERVER_OP_QUERY: return handle_query(req);
  case TABLESERVER_OP_PREPARE: return handle_prepare(req);
  case TABLESERVER_OP_COMMIT: return handle_commit(req);
  case TABLESERVER_OP_ROLLBACK: return handle_rollback(req);
  case TABLESERVER_OP_NOTIFY_ACK: return handle_notify_ack(req);
  default: ceph_abort_msg("unrecognized mds_table_server request op");
  }
}

class C_Prepare : public MDSLogContextBase {
  MDSTableServer *server;
  MMDSTableRequest::const_ref req;
  version_t tid;
  MDSRank *get_mds() override { return server->mds; }
public:

  C_Prepare(MDSTableServer *s, const MMDSTableRequest::const_ref r, version_t v) : server(s), req(r), tid(v) {}
  void finish(int r) override {
    server->_prepare_logged(req, tid);
  }
};

// prepare
void MDSTableServer::handle_prepare(const MMDSTableRequest::const_ref &req)
{
  dout(7) << "handle_prepare " << *req << dendl;
  mds_rank_t from = mds_rank_t(req->get_source().num());

  ceph_assert(g_conf()->mds_kill_mdstable_at != 1);

  projected_version++;

  ETableServer *le = new ETableServer(table, TABLESERVER_OP_PREPARE, req->reqid, from,
				      projected_version, projected_version);
  mds->mdlog->start_entry(le);
  le->mutation = req->bl;
  mds->mdlog->submit_entry(le, new C_Prepare(this, req, projected_version));
  mds->mdlog->flush();
}

void MDSTableServer::_prepare_logged(const MMDSTableRequest::const_ref &req, version_t tid)
{
  dout(7) << "_create_logged " << *req << " tid " << tid << dendl;
  mds_rank_t from = mds_rank_t(req->get_source().num());

  ceph_assert(g_conf()->mds_kill_mdstable_at != 2);

  _note_prepare(from, req->reqid);
  bufferlist out;
  _prepare(req->bl, req->reqid, from, out);
  ceph_assert(version == tid);

  auto reply = MMDSTableRequest::create(table, TABLESERVER_OP_AGREE, req->reqid, tid);
  reply->bl = std::move(out);

  if (_notify_prep(tid)) {
    auto& p = pending_notifies[tid];
    p.notify_ack_gather = active_clients;
    p.mds = from;
    p.reply = reply;
  } else {
    mds->send_message_mds(reply, from);
  }
}

void MDSTableServer::handle_notify_ack(const MMDSTableRequest::const_ref &m)
{
  dout(7) << __func__ << " " << *m << dendl;
  mds_rank_t from = mds_rank_t(m->get_source().num());
  version_t tid = m->get_tid();

  auto p = pending_notifies.find(tid);
  if (p != pending_notifies.end()) {
    if (p->second.notify_ack_gather.erase(from)) {
      if (p->second.notify_ack_gather.empty()) {
	if (p->second.onfinish)
	  p->second.onfinish->complete(0);
	else
	  mds->send_message_mds(p->second.reply, p->second.mds);
	pending_notifies.erase(p);
      }
    } else {
      dout(0) << "got unexpected notify ack for tid " <<  tid << " from mds." << from << dendl;
    }
  } else {
  }
}

class C_Commit : public MDSLogContextBase {
  MDSTableServer *server;
  MMDSTableRequest::const_ref req;
  MDSRank *get_mds() override { return server->mds; }
public:
  C_Commit(MDSTableServer *s, const MMDSTableRequest::const_ref &r) : server(s), req(r) {}
  void finish(int r) override {
    server->_commit_logged(req);
  }
};

// commit
void MDSTableServer::handle_commit(const MMDSTableRequest::const_ref &req)
{
  dout(7) << "handle_commit " << *req << dendl;

  version_t tid = req->get_tid();

  if (pending_for_mds.count(tid)) {

    if (committing_tids.count(tid)) {
      dout(0) << "got commit for tid " << tid << ", already committing, waiting." << dendl;
      return;
    }

    ceph_assert(g_conf()->mds_kill_mdstable_at != 5);

    projected_version++;
    committing_tids.insert(tid);

    mds->mdlog->start_submit_entry(new ETableServer(table, TABLESERVER_OP_COMMIT, 0, MDS_RANK_NONE, 
						    tid, projected_version),
				   new C_Commit(this, req));
  }
  else if (tid <= version) {
    dout(0) << "got commit for tid " << tid << " <= " << version
	    << ", already committed, sending ack." << dendl;
    auto reply = MMDSTableRequest::create(table, TABLESERVER_OP_ACK, req->reqid, tid);
    mds->send_message(reply, req->get_connection());
  } 
  else {
    // wtf.
    dout(0) << "got commit for tid " << tid << " > " << version << dendl;
    ceph_assert(tid <= version);
  }
}

void MDSTableServer::_commit_logged(const MMDSTableRequest::const_ref &req)
{
  dout(7) << "_commit_logged, sending ACK" << dendl;

  ceph_assert(g_conf()->mds_kill_mdstable_at != 6);
  version_t tid = req->get_tid();

  pending_for_mds.erase(tid);
  committing_tids.erase(tid);

  _commit(tid, req);
  _note_commit(tid);

  auto reply = MMDSTableRequest::create(table, TABLESERVER_OP_ACK, req->reqid, req->get_tid());
  mds->send_message_mds(reply, mds_rank_t(req->get_source().num()));
}

class C_Rollback : public MDSLogContextBase {
  MDSTableServer *server;
  MMDSTableRequest::const_ref req;
  MDSRank *get_mds() override { return server->mds; }
public:
  C_Rollback(MDSTableServer *s, const MMDSTableRequest::const_ref &r) : server(s), req(r) {}
  void finish(int r) override {
    server->_rollback_logged(req);
  }
};

// ROLLBACK
void MDSTableServer::handle_rollback(const MMDSTableRequest::const_ref &req)
{
  dout(7) << "handle_rollback " << *req << dendl;

  ceph_assert(g_conf()->mds_kill_mdstable_at != 8);
  version_t tid = req->get_tid();
  ceph_assert(pending_for_mds.count(tid));
  ceph_assert(!committing_tids.count(tid));

  projected_version++;
  committing_tids.insert(tid);

  mds->mdlog->start_submit_entry(new ETableServer(table, TABLESERVER_OP_ROLLBACK, 0, MDS_RANK_NONE,
						  tid, projected_version),
				 new C_Rollback(this, req));
}

void MDSTableServer::_rollback_logged(const MMDSTableRequest::const_ref &req)
{
  dout(7) << "_rollback_logged " << *req << dendl;

  version_t tid = req->get_tid();

  pending_for_mds.erase(tid);
  committing_tids.erase(tid);

  _rollback(tid);
  _note_rollback(tid);
}



// SERVER UPDATE
class C_ServerUpdate : public MDSLogContextBase {
  MDSTableServer *server;
  bufferlist bl;
  MDSRank *get_mds() override { return server->mds; }
public:
  C_ServerUpdate(MDSTableServer *s, bufferlist &b)  : server(s), bl(b) {}
  void finish(int r) override {
    server->_server_update_logged(bl);
  }
};

void MDSTableServer::do_server_update(bufferlist& bl)
{
  dout(10) << "do_server_update len " << bl.length() << dendl;

  projected_version++;

  ETableServer *le = new ETableServer(table, TABLESERVER_OP_SERVER_UPDATE, 0, MDS_RANK_NONE, 0, projected_version);
  mds->mdlog->start_entry(le);
  le->mutation = bl;
  mds->mdlog->submit_entry(le, new C_ServerUpdate(this, bl));
}

void MDSTableServer::_server_update_logged(bufferlist& bl)
{
  dout(10) << "_server_update_logged len " << bl.length() << dendl;
  _server_update(bl);
  _note_server_update(bl);
}

// recovery

class C_ServerRecovery : public MDSContext {
  MDSTableServer *server;
  MDSRank *get_mds() override { return server->mds; }
public:
  C_ServerRecovery(MDSTableServer *s)  : server(s) {}
  void finish(int r) override {
    server->_do_server_recovery();
  }
};

void MDSTableServer::_do_server_recovery()
{
  dout(7) << __func__ << " " << active_clients <<  dendl;
  map<mds_rank_t, uint64_t> next_reqids;

  for (auto p : pending_for_mds) {
    mds_rank_t who = p.second.mds;
    if (!active_clients.count(who))
      continue;

    if (p.second.reqid >= next_reqids[who])
      next_reqids[who] = p.second.reqid + 1;

    version_t tid = p.second.tid;
    auto reply = MMDSTableRequest::create(table, TABLESERVER_OP_AGREE, p.second.reqid, tid);
    _get_reply_buffer(tid, &reply->bl);
    mds->send_message_mds(reply, who);
  }

  for (auto p : active_clients) {
    auto reply = MMDSTableRequest::create(table, TABLESERVER_OP_SERVER_READY, next_reqids[p]);
    mds->send_message_mds(reply, p);
  }
  recovered = true;
}

void MDSTableServer::finish_recovery(set<mds_rank_t>& active)
{
  dout(7) << __func__ << dendl;

  active_clients = active;

  // don't know if survivor mds have received all 'notify prep' messages.
  // so we need to send 'notify prep' again.
  if (!pending_for_mds.empty() && _notify_prep(version)) {
    auto& q = pending_notifies[version];
    q.notify_ack_gather = active_clients;
    q.mds = MDS_RANK_NONE;
    q.onfinish = new C_ServerRecovery(this);
  } else {
    _do_server_recovery();
  }
}

void MDSTableServer::handle_mds_recovery(mds_rank_t who)
{
  dout(7) << "handle_mds_recovery mds." << who << dendl;

  active_clients.insert(who);
  if (!recovered) {
    dout(7) << " still not recovered, delaying" << dendl;
    return;
  }

  uint64_t next_reqid = 0;
  // resend agrees for recovered mds
  for (auto p = pending_for_mds.begin(); p != pending_for_mds.end(); ++p) {
    if (p->second.mds != who)
      continue;
    ceph_assert(!pending_notifies.count(p->second.tid));

    if (p->second.reqid >= next_reqid)
      next_reqid = p->second.reqid + 1;

    auto reply = MMDSTableRequest::create(table, TABLESERVER_OP_AGREE, p->second.reqid, p->second.tid);
    _get_reply_buffer(p->second.tid, &reply->bl);
    mds->send_message_mds(reply, who);
  }

  auto reply = MMDSTableRequest::create(table, TABLESERVER_OP_SERVER_READY, next_reqid);
  mds->send_message_mds(reply, who);
}

void MDSTableServer::handle_mds_failure_or_stop(mds_rank_t who)
{
  dout(7) << __func__ << " mds." << who << dendl;

  active_clients.erase(who);

  list<MMDSTableRequest::ref> rollback;
  for (auto p = pending_notifies.begin(); p != pending_notifies.end(); ) {
    auto q = p++;
    if (q->second.mds == who) {
      // haven't sent reply yet.
      rollback.push_back(q->second.reply);
      pending_notifies.erase(q);
    } else if (q->second.notify_ack_gather.erase(who)) {
      // the failed mds will reload snaptable when it recovers.
      // so we can remove it from the gather set.
      if (q->second.notify_ack_gather.empty()) {
	if (q->second.onfinish)
	  q->second.onfinish->complete(0);
	else
	  mds->send_message_mds(q->second.reply, q->second.mds);
	pending_notifies.erase(q);
      }
    }
  }

  for (auto &req : rollback) {
    req->op = TABLESERVER_OP_ROLLBACK;
    handle_rollback(req);
  }
}
