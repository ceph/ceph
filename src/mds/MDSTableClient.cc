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
using std::cout;
using std::cerr;

#include "MDSMap.h"

#include "include/Context.h"
#include "msg/Messenger.h"

#include "MDS.h"
#include "MDLog.h"
#include "LogSegment.h"

#include "MDSTableClient.h"
#include "events/ETableClient.h"

#include "messages/MMDSTableRequest.h"

#include "config.h"

#define DOUT_SUBSYS mds
#undef dout_prefix
#define dout_prefix *_dout << dbeginl << "mds" << mds->get_nodeid() << ".tableclient(" << get_mdstable_name(table) << ") "


void MDSTableClient::handle_request(class MMDSTableRequest *m)
{
  dout(10) << "handle_request " << *m << dendl;
  assert(m->table == table);

  version_t tid = m->tid;
  __u64 reqid = m->reqid;

  switch (m->op) {
  case TABLESERVER_OP_QUERY_REPLY:
    handle_query_result(m);
    break;
    
  case TABLESERVER_OP_AGREE:
    if (pending_prepare.count(reqid)) {
      dout(10) << "got agree on " << reqid << " atid " << tid << dendl;
      Context *onfinish = pending_prepare[reqid].onfinish;
      *pending_prepare[reqid].ptid = tid;
      if (pending_prepare[reqid].pbl)
	*pending_prepare[reqid].pbl = m->bl;
      pending_prepare.erase(reqid);
      if (onfinish) {
        onfinish->finish(0);
        delete onfinish;
      }
    } 
    else if (pending_commit.count(tid)) {
      dout(10) << "stray agree on " << reqid
	       << " tid " << tid
	       << ", already committing, resending COMMIT"
	       << dendl;      
      MMDSTableRequest *req = new MMDSTableRequest(table, TABLESERVER_OP_COMMIT, 0, tid);
      mds->send_message_mds(req, mds->mdsmap->get_tableserver());
    }
    else {
      dout(10) << "stray agree on " << reqid
	       << " tid " << tid
	       << ", sending ROLLBACK"
	       << dendl;      
      MMDSTableRequest *req = new MMDSTableRequest(table, TABLESERVER_OP_ROLLBACK, 0, tid);
      mds->send_message_mds(req, mds->mdsmap->get_tableserver());
    }
    break;

  case TABLESERVER_OP_ACK:
    dout(10) << "got ack on tid " << tid << ", logging" << dendl;

    // remove from committing list
    assert(pending_commit.count(tid));
    assert(pending_commit[tid]->pending_commit_tids[table].count(tid));
    
    // log ACK.
    mds->mdlog->submit_entry(new ETableClient(table, TABLESERVER_OP_ACK, tid),
			     new C_LoggedAck(this, tid));
    break;

  default:
    assert(0);
  }

  delete m;
}


void MDSTableClient::_logged_ack(version_t tid)
{
  dout(10) << "_logged_ack " << tid << dendl;

  assert(pending_commit.count(tid));
  assert(pending_commit[tid]->pending_commit_tids[table].count(tid));
  
  pending_commit[tid]->pending_commit_tids[table].erase(tid);
  pending_commit.erase(tid);
  
  // kick any waiters (LogSegment trim)
  if (ack_waiters.count(tid)) {
    dout(15) << "kicking ack waiters on tid " << tid << dendl;
    mds->queue_waiters(ack_waiters[tid]);
    ack_waiters.erase(tid);
  }
}

void MDSTableClient::_prepare(bufferlist& mutation, version_t *ptid, bufferlist *pbl,
			      Context *onfinish)
{
  __u64 reqid = ++last_reqid;
  dout(10) << "_prepare " << reqid << dendl;

  // send message
  MMDSTableRequest *req = new MMDSTableRequest(table, TABLESERVER_OP_PREPARE, reqid);
  req->bl = mutation;

  pending_prepare[reqid].mutation = mutation;
  pending_prepare[reqid].ptid = ptid;
  pending_prepare[reqid].pbl = pbl;
  pending_prepare[reqid].onfinish = onfinish;

  mds->send_message_mds(req, mds->mdsmap->get_tableserver());
}

void MDSTableClient::commit(version_t tid, LogSegment *ls)
{
  dout(10) << "commit " << tid << dendl;

  assert(pending_commit.count(tid) == 0);
  pending_commit[tid] = ls;
  ls->pending_commit_tids[table].insert(tid);

  // send message
  MMDSTableRequest *req = new MMDSTableRequest(table, TABLESERVER_OP_COMMIT, 0, tid);
  mds->send_message_mds(req, mds->mdsmap->get_tableserver());
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
  if (pending_commit.count(tid))
    pending_commit[tid]->pending_commit_tids[table].erase(tid);
  pending_commit.erase(tid);
}

void MDSTableClient::finish_recovery()
{
  dout(7) << "finish_recovery" << dendl;
  resend_commits();
}

void MDSTableClient::resend_commits()
{
  for (map<version_t,LogSegment*>::iterator p = pending_commit.begin();
       p != pending_commit.end();
       ++p) {
    dout(10) << "resending commit on " << p->first << dendl;
    MMDSTableRequest *req = new MMDSTableRequest(table, TABLESERVER_OP_COMMIT, 0, p->first);
    mds->send_message_mds(req, mds->mdsmap->get_tableserver());
  }
}

void MDSTableClient::handle_mds_recovery(int who)
{
  dout(7) << "handle_mds_recovery mds" << who << dendl;

  if (who != mds->mdsmap->get_tableserver()) 
    return; // do nothing.

  resend_queries();
  
  // prepares.
  for (map<__u64, _pending_prepare>::iterator p = pending_prepare.begin();
       p != pending_prepare.end();
       p++) {
    dout(10) << "resending " << p->first << dendl;
    MMDSTableRequest *req = new MMDSTableRequest(table, TABLESERVER_OP_PREPARE, p->first);
    req->bl = p->second.mutation;
    mds->send_message_mds(req, mds->mdsmap->get_tableserver());
  } 

  resend_commits();
}
