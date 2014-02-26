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
  map<inodeno_t, list<pair<version_t, Context*> > > pending_ops;

  void reset_state();
  void encode_server_state(bufferlist& bl) const {
    ENCODE_START(2, 2, bl);
    ::encode(anchor_map, bl);
    ::encode(pending_create, bl);
    ::encode(pending_destroy, bl);
    ::encode(pending_update, bl);
    ENCODE_FINISH(bl);
  }
  void decode_server_state(bufferlist::iterator& p) {
    DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, p);
    ::decode(anchor_map, p);
    ::decode(pending_create, p);
    ::decode(pending_destroy, p);
    ::decode(pending_update, p);
    DECODE_FINISH(p);

    map<version_t, inodeno_t> sort;
    sort.insert(pending_create.begin(), pending_create.end());
    sort.insert(pending_destroy.begin(), pending_destroy.end());
    for (map<version_t, pair<inodeno_t, vector<Anchor> > >::iterator p = pending_update.begin();
	 p != pending_update.end(); ++p)
      sort[p->first] = p->second.first;
    for (map<version_t, inodeno_t>::iterator p = sort.begin(); p != sort.end(); ++p)
      pending_ops[p->second].push_back(pair<version_t, Context*>(p->first, NULL));
  }

  bool add(inodeno_t ino, inodeno_t dirino, __u32 dn_hash, bool replace);
  void inc(inodeno_t ino, int ref=1);
  void dec(inodeno_t ino, int ref=1);
  bool check_pending(version_t tid, MMDSTableRequest *req, list<Context *>& finished);

  void dump();
  void dump(Formatter *f) const;
  static void generate_test_instances(list<AnchorServer*>& ls);
  // for the dencoder
  AnchorServer() : MDSTableServer(NULL, TABLE_ANCHOR) {}
  void encode(bufferlist& bl) const {
    encode_server_state(bl);
  }
  void decode(bufferlist::iterator& bl) {
    decode_server_state(bl);
  }

  // server bits
  void _prepare(bufferlist &bl, uint64_t reqid, int bymds);
  bool _commit(version_t tid, MMDSTableRequest *req=NULL);
  void _rollback(version_t tid);
  void handle_query(MMDSTableRequest *m);
};


#endif
