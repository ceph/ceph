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

#ifndef CEPH_MDSTABLECLIENT_H
#define CEPH_MDSTABLECLIENT_H

#include "include/types.h"
#include "MDSContext.h"
#include "mds_table_types.h"

class MDSRank;
class LogSegment;
class MMDSTableRequest;

class MDSTableClient {
protected:
  MDSRank *mds;
  int table;

  uint64_t last_reqid;

  bool server_ready;

  // prepares
  struct _pending_prepare {
    MDSInternalContextBase *onfinish;
    version_t *ptid;
    bufferlist *pbl; 
    bufferlist mutation;

    _pending_prepare() : onfinish(0), ptid(0), pbl(0) {}
    _pending_prepare(MDSInternalContextBase *c, version_t *pt, bufferlist *pb, bufferlist& m) :
      onfinish(c), ptid(pt), pbl(pb), mutation(m) {}
  };

  map<uint64_t, _pending_prepare> pending_prepare;
  map<version_t, uint64_t> prepared_update;
  list<_pending_prepare> waiting_for_reqid;

  // pending commits
  map<version_t, LogSegment*> pending_commit;
  map<version_t, list<MDSInternalContextBase*> > ack_waiters;

  void handle_reply(class MMDSTableQuery *m);  
  void _logged_ack(version_t tid);
  friend class C_LoggedAck;

public:
  MDSTableClient(MDSRank *m, int tab) :
    mds(m), table(tab), last_reqid(~0ULL), server_ready(false) {}
  virtual ~MDSTableClient() {}

  void handle_request(MMDSTableRequest *m);

  void _prepare(bufferlist& mutation, version_t *ptid, bufferlist *pbl, MDSInternalContextBase *onfinish);
  void commit(version_t tid, LogSegment *ls);

  void resend_commits();
  void resend_prepares();

  // for recovery (by me)
  void got_journaled_agree(version_t tid, LogSegment *ls);
  void got_journaled_ack(version_t tid);

  bool has_committed(version_t tid) {
    return pending_commit.count(tid) == 0;
  }
  void wait_for_ack(version_t tid, MDSInternalContextBase *c) {
    ack_waiters[tid].push_back(c);
  }

  void handle_mds_failure(mds_rank_t mds);

  // child must implement
  virtual void resend_queries() = 0;
  virtual void handle_query_result(MMDSTableRequest *m) = 0;

  // and friendly front-end for _prepare.

};

#endif
