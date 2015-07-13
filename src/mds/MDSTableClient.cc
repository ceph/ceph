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

#include <iostream>

#include "MDSMap.h"

#include "MDSContext.h"
#include "msg/Messenger.h"

#include "MDSRank.h"
#include "MDLog.h"
#include "LogSegment.h"

#include "MDSTableClient.h"
#include "events/ETableClient.h"

#include "messages/MMDSTableRequest.h"

#include "common/config.h"

#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds." << mds->get_nodeid() << ".tableclient(" << get_mdstable_name(table) << ") "


class C_LoggedAck : public MDSInternalContext {
  MDSTableClient *tc;
  version_t tid;
public:
  C_LoggedAck(MDSTableClient *a, version_t t) : MDSInternalContext(a->mds), tc(a), tid(t) {}
  void finish(int r) {
    tc->_logged_ack(tid);
  }
};


void MDSTableClient::handle_request(class MMDSTableRequest *m)
{
  dout(10) << "handle_request " << *m << dendl;
  assert(m->table == table);

  version_t tid = m->get_tid();
  uint64_t reqid = m->reqid;

  switch (m->op) {
  case TABLESERVER_OP_QUERY_REPLY:
    handle_query_result(m);
    break;
    
  case TABLESERVER_OP_AGREE:
    if (pending_prepare.count(reqid)) {
      dout(10) << "got agree on " << reqid << " atid " << tid << dendl;

      assert(g_conf->mds_kill_mdstable_at != 3);

      MDSInternalContextBase *onfinish = pending_prepare[reqid].onfinish;
      *pending_prepare[reqid].ptid = tid;
      if (pending_prepare[reqid].pbl)
	*pending_prepare[reqid].pbl = m->bl;
      pending_prepare.erase(reqid);
      prepared_update[tid] = reqid;
      if (onfinish) {
        onfinish->complete(0);
      }
    }
    else if (prepared_update.count(tid)) {
      dout(10) << "got duplicated agree on " << reqid << " atid " << tid << dendl;
      assert(prepared_update[tid] == reqid);
      assert(!server_ready);
    }
    else if (pending_commit.count(tid)) {
      dout(10) << "stray agree on " << reqid << " tid " << tid
	       << ", already committing, will resend COMMIT" << dendl;
      assert(!server_ready);
      // will re-send commit when receiving the server ready message
    }
    else {
      dout(10) << "stray agree on " << reqid << " tid " << tid
	       << ", sending ROLLBACK" << dendl;
      assert(!server_ready);
      MMDSTableRequest *req = new MMDSTableRequest(table, TABLESERVER_OP_ROLLBACK, 0, tid);
      mds->send_message_mds(req, mds->get_mds_map()->get_tableserver());
    }
    break;

  case TABLESERVER_OP_ACK:
    if (pending_commit.count(tid) &&
	pending_commit[tid]->pending_commit_tids[table].count(tid)) {
      dout(10) << "got ack on tid " << tid << ", logging" << dendl;
      
      assert(g_conf->mds_kill_mdstable_at != 7);
      
      // remove from committing list
      pending_commit[tid]->pending_commit_tids[table].erase(tid);
      pending_commit.erase(tid);

      // log ACK.
      mds->mdlog->start_submit_entry(new ETableClient(table, TABLESERVER_OP_ACK, tid),
				     new C_LoggedAck(this, tid));
    } else {
      dout(10) << "got stray ack on tid " << tid << ", ignoring" << dendl;
    }
    break;

  case TABLESERVER_OP_SERVER_READY:
    assert(!server_ready);
    server_ready = true;

    if (last_reqid == ~0ULL)
      last_reqid = reqid;

    resend_queries();
    resend_prepares();
    resend_commits();
    break;

  default:
    assert(0);
  }

  m->put();
}


void MDSTableClient::_logged_ack(version_t tid)
{
  dout(10) << "_logged_ack " << tid << dendl;

  assert(g_conf->mds_kill_mdstable_at != 8);

  // kick any waiters (LogSegment trim)
  if (ack_waiters.count(tid)) {
    dout(15) << "kicking ack waiters on tid " << tid << dendl;
    mds->queue_waiters(ack_waiters[tid]);
    ack_waiters.erase(tid);
  }
}

void MDSTableClient::_prepare(bufferlist& mutation, version_t *ptid, bufferlist *pbl,
			      MDSInternalContextBase *onfinish)
{
  if (last_reqid == ~0ULL) {
    dout(10) << "tableserver is not ready yet, waiting for request id" << dendl;
    waiting_for_reqid.push_back(_pending_prepare(onfinish, ptid, pbl, mutation));
    return;
  }

  uint64_t reqid = ++last_reqid;
  dout(10) << "_prepare " << reqid << dendl;

  pending_prepare[reqid].mutation = mutation;
  pending_prepare[reqid].ptid = ptid;
  pending_prepare[reqid].pbl = pbl;
  pending_prepare[reqid].onfinish = onfinish;

  if (server_ready) {
    // send message
    MMDSTableRequest *req = new MMDSTableRequest(table, TABLESERVER_OP_PREPARE, reqid);
    req->bl = mutation;
    mds->send_message_mds(req, mds->get_mds_map()->get_tableserver());
  } else
    dout(10) << "tableserver is not ready yet, deferring request" << dendl;
}

void MDSTableClient::commit(version_t tid, LogSegment *ls)
{
  dout(10) << "commit " << tid << dendl;

  assert(prepared_update.count(tid));
  prepared_update.erase(tid);

  assert(pending_commit.count(tid) == 0);
  pending_commit[tid] = ls;
  ls->pending_commit_tids[table].insert(tid);

  assert(g_conf->mds_kill_mdstable_at != 4);

  if (server_ready) {
    // send message
    MMDSTableRequest *req = new MMDSTableRequest(table, TABLESERVER_OP_COMMIT, 0, tid);
    mds->send_message_mds(req, mds->get_mds_map()->get_tableserver());
  } else
    dout(10) << "tableserver is not ready yet, deferring request" << dendl;
}



// recovery

void MDSTableClient::got_journaled_agree(version_t tid, LogSegment *ls)
{
  dout(10) << "got_journaled_agree " << tid << dendl;
  ls->pending_commit_tids[table].insert(tid);
  pending_commit[tid] = ls;
}

void MDSTableClient::got_journaled_ack(version_t tid)
{
  dout(10) << "got_journaled_ack " << tid << dendl;
  if (pending_commit.count(tid)) {
    pending_commit[tid]->pending_commit_tids[table].erase(tid);
    pending_commit.erase(tid);
  }
}

void MDSTableClient::resend_commits()
{
  for (map<version_t,LogSegment*>::iterator p = pending_commit.begin();
       p != pending_commit.end();
       ++p) {
    dout(10) << "resending commit on " << p->first << dendl;
    MMDSTableRequest *req = new MMDSTableRequest(table, TABLESERVER_OP_COMMIT, 0, p->first);
    mds->send_message_mds(req, mds->get_mds_map()->get_tableserver());
  }
}

void MDSTableClient::resend_prepares()
{
  while (!waiting_for_reqid.empty()) {
    pending_prepare[++last_reqid] = waiting_for_reqid.front();
    waiting_for_reqid.pop_front();
  }

  for (map<uint64_t, _pending_prepare>::iterator p = pending_prepare.begin();
       p != pending_prepare.end();
       ++p) {
    dout(10) << "resending prepare on " << p->first << dendl;
    MMDSTableRequest *req = new MMDSTableRequest(table, TABLESERVER_OP_PREPARE, p->first);
    req->bl = p->second.mutation;
    mds->send_message_mds(req, mds->get_mds_map()->get_tableserver());
  }
}

void MDSTableClient::handle_mds_failure(mds_rank_t who)
{
  if (who != mds->get_mds_map()->get_tableserver())
    return; // do nothing.

  dout(7) << "tableserver mds." << who << " fails" << dendl;
  server_ready = false;
}
