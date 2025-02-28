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
#include "mdstypes.h" // for mds_table_pending_t

#include "messages/MMDSTableRequest.h"

class MDSContext;

class MDSTableServer : public MDSTable {
public:
  friend class C_ServerRecovery;

  MDSTableServer(MDSRank *m, int tab) :
    MDSTable(m, get_mdstable_name(tab), false), table(tab) {}
  ~MDSTableServer() override {}

  virtual void handle_query(const cref_t<MMDSTableRequest> &m) = 0;
  virtual void _prepare(const bufferlist &bl, uint64_t reqid, mds_rank_t bymds, bufferlist& out) = 0;
  virtual void _get_reply_buffer(version_t tid, bufferlist *pbl) const = 0;
  virtual void _commit(version_t tid, cref_t<MMDSTableRequest> req) = 0;
  virtual void _rollback(version_t tid) = 0;
  virtual void _server_update(bufferlist& bl) { ceph_abort(); }
  virtual bool _notify_prep(version_t tid) { return false; };

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

  void reset_state() override {
    pending_for_mds.clear();
    ++version;
  }

  void handle_request(const cref_t<MMDSTableRequest> &m);
  void do_server_update(bufferlist& bl);

  virtual void encode_server_state(bufferlist& bl) const = 0;
  virtual void decode_server_state(bufferlist::const_iterator& bl) = 0;

  void encode_state(bufferlist& bl) const override {
    encode_server_state(bl);
    encode(pending_for_mds, bl);
  }
  void decode_state(bufferlist::const_iterator& bl) override {
    decode_server_state(bl);
    decode(pending_for_mds, bl);
  }

  // recovery
  void finish_recovery(std::set<mds_rank_t>& active);
  void _do_server_recovery();

  void handle_mds_recovery(mds_rank_t who);
  void handle_mds_failure_or_stop(mds_rank_t who);
protected:
  int table;
  bool recovered = false;
  std::set<mds_rank_t> active_clients;
private:
  struct notify_info_t {
    notify_info_t() {}
    std::set<mds_rank_t> notify_ack_gather;
    mds_rank_t mds;
    ref_t<MMDSTableRequest> reply = NULL;
    MDSContext *onfinish = nullptr;
  };

  friend class C_Prepare;
  friend class C_Commit;
  friend class C_Rollback;
  friend class C_ServerUpdate;

  void handle_prepare(const cref_t<MMDSTableRequest> &m);
  void _prepare_logged(const cref_t<MMDSTableRequest> &m, version_t tid);

  void handle_commit(const cref_t<MMDSTableRequest> &m);
  void _commit_logged(const cref_t<MMDSTableRequest> &m);

  void handle_rollback(const cref_t<MMDSTableRequest> &m);
  void _rollback_logged(const cref_t<MMDSTableRequest> &m);

  void _server_update_logged(bufferlist& bl);

  void handle_notify_ack(const cref_t<MMDSTableRequest> &m);

  std::map<version_t,mds_table_pending_t> pending_for_mds;  // ** child should encode this! **
  std::set<version_t> committing_tids;

  std::map<version_t, notify_info_t> pending_notifies;
};
#endif
