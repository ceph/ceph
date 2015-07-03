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

#ifndef CEPH_SNAPSERVER_H
#define CEPH_SNAPSERVER_H

#include "MDSTableServer.h"
#include "snap.h"

class MDSRank;
class MonClient;

class SnapServer : public MDSTableServer {
protected:
  MonClient *mon_client;
  snapid_t last_snap;
  map<snapid_t, SnapInfo> snaps;
  map<int, set<snapid_t> > need_to_purge;
  
  map<version_t, SnapInfo> pending_update;
  map<version_t, pair<snapid_t,snapid_t> > pending_destroy; // (removed_snap, seq)
  set<version_t>           pending_noop;

  version_t last_checked_osdmap;

public:
  SnapServer(MDSRank *m, MonClient *monc)
    : MDSTableServer(m, TABLE_SNAP), mon_client(monc), last_checked_osdmap(0)
  {}
    
  void reset_state();
  void encode_server_state(bufferlist& bl) const {
    ENCODE_START(3, 3, bl);
    ::encode(last_snap, bl);
    ::encode(snaps, bl);
    ::encode(need_to_purge, bl);
    ::encode(pending_update, bl);
    ::encode(pending_destroy, bl);
    ::encode(pending_noop, bl);
    ENCODE_FINISH(bl);
  }
  void decode_server_state(bufferlist::iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(3, 3, 3, bl);
    ::decode(last_snap, bl);
    ::decode(snaps, bl);
    ::decode(need_to_purge, bl);
    ::decode(pending_update, bl);
    if (struct_v >= 2)
      ::decode(pending_destroy, bl);
    else {
      map<version_t, snapid_t> t;
      ::decode(t, bl);
      for (map<version_t, snapid_t>::iterator p = t.begin(); p != t.end(); ++p)
	pending_destroy[p->first].first = p->second; 
    } 
    ::decode(pending_noop, bl);
    DECODE_FINISH(bl);
  }

  // To permit enc/decoding in isolation in dencoder
  SnapServer() : MDSTableServer(NULL, TABLE_SNAP), last_checked_osdmap(0) {}
  void encode(bufferlist& bl) const {
    encode_server_state(bl);
  }
  void decode(bufferlist::iterator& bl) {
    decode_server_state(bl);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<SnapServer*>& ls);

  // server bits
  void _prepare(bufferlist &bl, uint64_t reqid, mds_rank_t bymds);
  bool _is_prepared(version_t tid);
  bool _commit(version_t tid, MMDSTableRequest *req=NULL);
  void _rollback(version_t tid);
  void _server_update(bufferlist& bl);
  void handle_query(MMDSTableRequest *m);

  void check_osd_map(bool force);
};

#endif
