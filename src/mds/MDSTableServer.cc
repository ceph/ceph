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

#include "messages/MMDSTableRequest.h"
#include "events/ETableServer.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds." << rank << ".tableserver(" << get_mdstable_name(table) << ") "

/* This function DOES put the passed message before returning */
void MDSTableServer::handle_request(MMDSTableRequest *req)
{
  assert(req->op >= 0);
  switch (req->op) {
  case TABLESERVER_OP_QUERY: return handle_query(req);
  case TABLESERVER_OP_PREPARE: return handle_prepare(req);
  case TABLESERVER_OP_COMMIT: return handle_commit(req);
  case TABLESERVER_OP_ROLLBACK: return handle_rollback(req);
  default: assert(0 == "unrecognized mds_table_server request op");
  }
}

class C_Prepare : public MDSLogContextBase {
  MDSTableServer *server;
  MMDSTableRequest *req;
  version_t tid;
  MDSRank *get_mds() override { return server->mds; }
public:

  C_Prepare(MDSTableServer *s, MMDSTableRequest *r, version_t v) : server(s), req(r), tid(v) {}
  void finish(int r) override {
    server->_prepare_logged(req, tid);
  }
};

// prepare
/* This function DOES put the passed message before returning */
void MDSTableServer::handle_prepare(MMDSTableRequest *req)
{
  dout(7) << "handle_prepare " << *req << dendl;
  mds_rank_t from = mds_rank_t(req->get_source().num());

  assert(g_conf->mds_kill_mdstable_at != 1);

  projected_version++;

  ETableServer *le = new ETableServer(table, TABLESERVER_OP_PREPARE, req->reqid, from,
				      projected_version, projected_version);
  mds->mdlog->start_entry(le);
  le->mutation = req->bl;
  mds->mdlog->submit_entry(le, new C_Prepare(this, req, projected_version));
  mds->mdlog->flush();
}

void MDSTableServer::_prepare_logged(MMDSTableRequest *req, version_t tid)
{
  dout(7) << "_create_logged " << *req << " tid " << tid << dendl;
  mds_rank_t from = mds_rank_t(req->get_source().num());

  assert(g_conf->mds_kill_mdstable_at != 2);

  _note_prepare(from, req->reqid);
  _prepare(req->bl, req->reqid, from);
  assert(version == tid);

  MMDSTableRequest *reply = new MMDSTableRequest(table, TABLESERVER_OP_AGREE, req->reqid, tid);
  reply->bl = req->bl;
  mds->send_message_mds(reply, from);
  req->put();
}

class C_Commit : public MDSLogContextBase {
  MDSTableServer *server;
  MMDSTableRequest *req;
  MDSRank *get_mds() override { return server->mds; }
public:
  C_Commit(MDSTableServer *s, MMDSTableRequest *r) : server(s), req(r) {}
  void finish(int r) override {
    server->_commit_logged(req);
  }
};

// commit
/* This function DOES put the passed message before returning */
void MDSTableServer::handle_commit(MMDSTableRequest *req)
{
  dout(7) << "handle_commit " << *req << dendl;

  version_t tid = req->get_tid();

  if (pending_for_mds.count(tid)) {

    if (committing_tids.count(tid)) {
      dout(0) << "got commit for tid " << tid << ", already committing, waiting." << dendl;
      req->put();
      return;
    }

    assert(g_conf->mds_kill_mdstable_at != 5);

    projected_version++;
    committing_tids.insert(tid);

    mds->mdlog->start_submit_entry(new ETableServer(table, TABLESERVER_OP_COMMIT, 0, MDS_RANK_NONE, 
						    tid, projected_version),
				   new C_Commit(this, req));
  }
  else if (tid <= version) {
    dout(0) << "got commit for tid " << tid << " <= " << version
	    << ", already committed, sending ack." << dendl;
    MMDSTableRequest *reply = new MMDSTableRequest(table, TABLESERVER_OP_ACK, req->reqid, tid);
    mds->send_message(reply, req->get_connection());
    req->put();
  } 
  else {
    // wtf.
    dout(0) << "got commit for tid " << tid << " > " << version << dendl;
    assert(tid <= version);
  }
}

/* This function DOES put the passed message before returning */
void MDSTableServer::_commit_logged(MMDSTableRequest *req)
{
  dout(7) << "_commit_logged, sending ACK" << dendl;

  assert(g_conf->mds_kill_mdstable_at != 6);
  version_t tid = req->get_tid();

  pending_for_mds.erase(tid);
  committing_tids.erase(tid);

  _commit(tid, req);
  _note_commit(tid);

  MMDSTableRequest *reply = new MMDSTableRequest(table, TABLESERVER_OP_ACK, req->reqid, req->get_tid());
  mds->send_message_mds(reply, mds_rank_t(req->get_source().num()));
  req->put();
}

class C_Rollback : public MDSLogContextBase {
  MDSTableServer *server;
  MMDSTableRequest *req;
  MDSRank *get_mds() override { return server->mds; }
public:
  C_Rollback(MDSTableServer *s, MMDSTableRequest *r) : server(s), req(r) {}
  void finish(int r) override {
    server->_rollback_logged(req);
  }
};

// ROLLBACK
void MDSTableServer::handle_rollback(MMDSTableRequest *req)
{
  dout(7) << "handle_rollback " << *req << dendl;

  version_t tid = req->get_tid();
  assert(pending_for_mds.count(tid));
  assert(!committing_tids.count(tid));

  projected_version++;
  committing_tids.insert(tid);

  mds->mdlog->start_submit_entry(new ETableServer(table, TABLESERVER_OP_ROLLBACK, 0, MDS_RANK_NONE,
						  tid, projected_version),
				 new C_Rollback(this, req));
}

/* This function DOES put the passed message before returning */
void MDSTableServer::_rollback_logged(MMDSTableRequest *req)
{
  dout(7) << "_rollback_logged " << *req << dendl;

  assert(g_conf->mds_kill_mdstable_at != 7);
  version_t tid = req->get_tid();

  pending_for_mds.erase(tid);
  committing_tids.erase(tid);

  _rollback(tid);
  _note_rollback(tid);

  req->put();
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

void MDSTableServer::finish_recovery(set<mds_rank_t>& active)
{
  dout(7) << "finish_recovery" << dendl;
  for (set<mds_rank_t>::iterator p = active.begin(); p != active.end(); ++p)
    handle_mds_recovery(*p);  // resend agrees for everyone.
}

void MDSTableServer::handle_mds_recovery(mds_rank_t who)
{
  dout(7) << "handle_mds_recovery mds." << who << dendl;

  uint64_t next_reqid = 0;
  // resend agrees for recovered mds
  for (map<version_t,mds_table_pending_t>::iterator p = pending_for_mds.begin();
       p != pending_for_mds.end();
       ++p) {
    if (p->second.mds != who)
      continue;
    if (p->second.reqid >= next_reqid)
      next_reqid = p->second.reqid + 1;
    MMDSTableRequest *reply = new MMDSTableRequest(table, TABLESERVER_OP_AGREE, p->second.reqid, p->second.tid);
    _get_reply_buffer(p->second.tid, &reply->bl);
    mds->send_message_mds(reply, who);
  }

  MMDSTableRequest *reply = new MMDSTableRequest(table, TABLESERVER_OP_SERVER_READY, next_reqid);
  mds->send_message_mds(reply, who);
}
