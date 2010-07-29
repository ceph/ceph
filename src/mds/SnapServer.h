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

class MDS;

class SnapServer : public MDSTableServer {
public:
  
protected:
  snapid_t last_snap;
  map<snapid_t, SnapInfo> snaps;
  map<int, set<snapid_t> > need_to_purge;
  
  map<version_t, SnapInfo> pending_create;
  map<version_t, pair<snapid_t,snapid_t> > pending_destroy; // (removed_snap, seq)
  set<version_t>           pending_noop;

  version_t last_checked_osdmap;

public:
  SnapServer(MDS *m) : MDSTableServer(m, TABLE_SNAP),
		       last_checked_osdmap(0) { }
    
  void reset_state();
  void encode_server_state(bufferlist& bl) {
    __u8 v = 2;
    ::encode(v, bl);
    ::encode(last_snap, bl);
    ::encode(snaps, bl);
    ::encode(need_to_purge, bl);
    ::encode(pending_create, bl);
    ::encode(pending_destroy, bl);
    ::encode(pending_noop, bl);
  }
  void decode_server_state(bufferlist::iterator& bl) {
    __u8 v;
    ::decode(v, bl);
    ::decode(last_snap, bl);
    ::decode(snaps, bl);
    ::decode(need_to_purge, bl);
    ::decode(pending_create, bl);
    if (v >= 2)
      ::decode(pending_destroy, bl);
    else {
      map<version_t, snapid_t> t;
      ::decode(t, bl);
      for (map<version_t, snapid_t>::iterator p = t.begin(); p != t.end(); p++)
	pending_destroy[p->first].first = p->second; 
    } 
    ::decode(pending_noop, bl);
  }

  // server bits
  void _prepare(bufferlist &bl, uint64_t reqid, int bymds);
  bool _is_prepared(version_t tid);
  void _commit(version_t tid);
  void _rollback(version_t tid);
  void _server_update(bufferlist& bl);
  void handle_query(MMDSTableRequest *m);

  void check_osd_map(bool force);
};

#endif
