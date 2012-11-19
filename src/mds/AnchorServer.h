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

#ifndef CEPH_ANCHORSERVER_H
#define CEPH_ANCHORSERVER_H

#include "MDSTableServer.h"
#include "Anchor.h"

class AnchorServer : public MDSTableServer {
 public:
  AnchorServer(MDS *mds) :
    MDSTableServer(mds, TABLE_ANCHOR) {}

  // table bits
  map<inodeno_t, Anchor>  anchor_map;

  // uncommitted operations
  map<version_t, inodeno_t> pending_create;
  map<version_t, inodeno_t> pending_destroy;
  map<version_t, pair<inodeno_t, vector<Anchor> > > pending_update;

  void reset_state();
  void encode_server_state(bufferlist& bl) {
    __u8 v = 1;
    ::encode(v, bl);
    ::encode(anchor_map, bl);
    ::encode(pending_create, bl);
    ::encode(pending_destroy, bl);
    ::encode(pending_update, bl);
  }
  void decode_server_state(bufferlist::iterator& p) {
    __u8 v;
    ::decode(v, p);
    ::decode(anchor_map, p);
    ::decode(pending_create, p);
    ::decode(pending_destroy, p);
    ::decode(pending_update, p);
  }

  bool add(inodeno_t ino, inodeno_t dirino, __u32 dn_hash, bool replace);
  void inc(inodeno_t ino, int ref=1);
  void dec(inodeno_t ino, int ref=1);

  void dump();

  // server bits
  void _prepare(bufferlist &bl, uint64_t reqid, int bymds);
  void _commit(version_t tid);
  void _rollback(version_t tid);
  void handle_query(MMDSTableRequest *m);
};


#endif
