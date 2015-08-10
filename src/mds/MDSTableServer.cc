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
  default: assert(0);
  }
}

class C_Prepare : public MDSInternalContext {
  MDSTableServer *server;
  MMDSTableRequest *req;
  version_t tid;
public:

  C_Prepare(MDSTableServer *s, MMDSTableRequest *r, version_t v) : MDSInternalContext(s->mds), server(s), req(r), tid(v) {}
  void finish(int r) {
    server->_prepare_logged(req, tid);
  }
};

// prepare
/* This function DOES put the passed message before returning */
void MDSTableServer::handle_prepare(MMDSTableRequest *req)
{
  dout(7) << "handle_prepare " << *req << dendl;
  mds_rank_t from = mds_rank_t(req->get_source().num());
  bufferlist bl = req->bl;

  _prepare(req->bl, req->reqid, from);
  _note_prepare(from, req->reqid);

  assert(g_conf->mds_kill_mdstable_at != 1);

  ETableServer *le = new ETableServer(table, TABLESERVER_OP_PREPARE, req->reqid, from, version, version);
  mds->mdlog->start_entry(le);
  le->mutation = bl;  // original request, NOT modified return value coming out of _prepare!
  mds->mdlog->submit_entry(le, new C_Prepare(this, req, version));
  mds->mdlog->flush();
}

void MDSTableServer::_prepare_logged(MMDSTableRequest *req, version_t tid)
{
  dout(7) << "_create_logged " << *req << " tid " << tid << dendl;

  assert(g_conf->mds_kill_mdstable_at != 2);

  MMDSTableRequest *reply = new MMDSTableRequest(table, TABLESERVER_OP_AGREE, req->reqid, tid);
  reply->bl = req->bl;
  mds->send_message_mds(reply, mds_rank_t(req->get_source().num()));
  req->put();
}

class C_Commit : public MDSInternalContext {
  MDSTableServer *server;
  MMDSTableRequest *req;
public:
  C_Commit(MDSTableServer *s, MMDSTableRequest *r) : MDSInternalContext(s->mds), server(s), req(r) {}
  void finish(int r) {
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

    assert(g_conf->mds_kill_mdstable_at != 5);

    if (!_commit(tid, req))
      return;

    _note_commit(tid);
    mds->mdlog->start_submit_entry(new ETableServer(table, TABLESERVER_OP_COMMIT, 0, MDS_RANK_NONE, 
						    tid, version),
				   new C_Commit(this, req));
  }
  else if (tid <= version) {
    dout(0) << "got commit for tid " << tid << " <= " << version 
	    << ", already committed, sending ack." 
	    << dendl;
    _commit_logged(req);
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

  MMDSTableRequest *reply = new MMDSTableRequest(table, TABLESERVER_OP_ACK, req->reqid, req->get_tid());
  mds->send_message_mds(reply, mds_rank_t(req->get_source().num()));
  req->put();
}

// ROLLBACK
/* This function DOES put the passed message before returning */
void MDSTableServer::handle_rollback(MMDSTableRequest *req)
{
  dout(7) << "handle_rollback " << *req << dendl;

  version_t tid = req->get_tid();
  assert(pending_for_mds.count(tid));
  _rollback(tid);
  _note_rollback(tid);
  mds->mdlog->start_submit_entry(new ETableServer(table, TABLESERVER_OP_ROLLBACK, 0, MDS_RANK_NONE, 
						  tid, version));
  req->put();
}



// SERVER UPDATE

void MDSTableServer::do_server_update(bufferlist& bl)
{
  dout(10) << "do_server_update len " << bl.length() << dendl;
  _server_update(bl);
  ETableServer *le = new ETableServer(table, TABLESERVER_OP_SERVER_UPDATE, 0, MDS_RANK_NONE, 0, version);
  mds->mdlog->start_entry(le);
  le->mutation = bl;
  mds->mdlog->submit_entry(le);
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
    mds->send_message_mds(reply, who);
  }

  MMDSTableRequest *reply = new MMDSTableRequest(table, TABLESERVER_OP_SERVER_READY, next_reqid);
  mds->send_message_mds(reply, who);
}
