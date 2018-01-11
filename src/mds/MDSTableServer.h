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

#ifndef CEPH_MDSTABLESERVER_H
#define CEPH_MDSTABLESERVER_H

#include "MDSTable.h"

class MMDSTableRequest;

class MDSTableServer : public MDSTable {
protected:
  int table;
private:
  map<version_t,mds_table_pending_t> pending_for_mds;  // ** child should encode this! **
  set<version_t> committing_tids;

  void handle_prepare(MMDSTableRequest *m);
  void _prepare_logged(MMDSTableRequest *m, version_t tid);
  friend class C_Prepare;

  void handle_commit(MMDSTableRequest *m);
  void _commit_logged(MMDSTableRequest *m);
  friend class C_Commit;

  void handle_rollback(MMDSTableRequest *m);
  void _rollback_logged(MMDSTableRequest *m);
  friend class C_Rollback;

  void _server_update_logged(bufferlist& bl);
  friend class C_ServerUpdate;

public:
  virtual void handle_query(MMDSTableRequest *m) = 0;
  virtual void _prepare(bufferlist &bl, uint64_t reqid, mds_rank_t bymds) = 0;
  virtual void _get_reply_buffer(version_t tid, bufferlist *pbl) const = 0;
  virtual void _commit(version_t tid, MMDSTableRequest *req=NULL) = 0;
  virtual void _rollback(version_t tid) = 0;
  virtual void _server_update(bufferlist& bl) { ceph_abort(); }

  void _note_prepare(mds_rank_t mds, uint64_t reqid, bool replay=false) {
    version++;
    if (replay)
      projected_version = version;
    pending_for_mds[version].mds = mds;
    pending_for_mds[version].reqid = reqid;
    pending_for_mds[version].tid = version;
  }
  void _note_commit(uint64_t tid, bool replay=false) {
    version++;
    if (replay)
      projected_version = version;
    pending_for_mds.erase(tid);
  }
  void _note_rollback(uint64_t tid, bool replay=false) {
    version++;
    if (replay)
      projected_version = version;
    pending_for_mds.erase(tid);
  }
  void _note_server_update(bufferlist& bl, bool replay=false) {
    version++;
    if (replay)
      projected_version = version;
  }

  MDSTableServer(MDSRank *m, int tab) : MDSTable(m, get_mdstable_name(tab), false), table(tab) {}
  ~MDSTableServer() override {}

  void handle_request(MMDSTableRequest *m);
  void do_server_update(bufferlist& bl);

  virtual void encode_server_state(bufferlist& bl) const = 0;
  virtual void decode_server_state(bufferlist::iterator& bl) = 0;

  void encode_state(bufferlist& bl) const override {
    encode_server_state(bl);
    encode(pending_for_mds, bl);
  }
  void decode_state(bufferlist::iterator& bl) override {
    decode_server_state(bl);
    decode(pending_for_mds, bl);
  }

  // recovery
  void finish_recovery(set<mds_rank_t>& active);
  void handle_mds_recovery(mds_rank_t who);
};

#endif
