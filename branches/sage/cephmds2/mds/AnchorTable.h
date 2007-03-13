// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
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


#ifndef __ANCHORTABLE_H
#define __ANCHORTABLE_H

#include "Anchor.h"
#include "include/Context.h"

#include <ext/hash_map>
using namespace __gnu_cxx;

class MDS;
class MAnchor;

class AnchorTable {
  MDS *mds;

  // keep the entire table in memory.
  hash_map<inodeno_t, Anchor>  anchor_map;

  // uncommitted operations
  set<inodeno_t> pending_create;
  set<inodeno_t> pending_destroy;
  map<inodeno_t, vector<Anchor> > pending_update;

  version_t version;  // this includes anchor_map AND pending_* state.

  // load/save state
  bool opening, opened;

  // waiters
  list<Context*> waiting_for_open;

protected:

  // basic updates
  bool add(inodeno_t ino, dirfrag_t dirfrag);
  void inc(inodeno_t ino);
  void dec(inodeno_t ino);

  // mid-level
  void create_prepare(inodeno_t ino, vector<Anchor>& trace);
  void create_commit(inodeno_t ino);
  void destroy_prepare(inodeno_t ino);
  void destroy_commit(inodeno_t ino);
  void update_prepare(inodeno_t ino, vector<Anchor>& trace);
  void update_commit(inodeno_t ino);
  friend class EAnchor;  // used for journal replay.

  // high level interface
  void handle_lookup(MAnchor *req);

  void handle_create_prepare(MAnchor *req);
  void _create_prepare_logged(MAnchor *req);
  void handle_create_commit(MAnchor *req);
  friend class C_AT_CreatePrepare;

  void handle_destroy_prepare(MAnchor *req);
  void _destroy_prepare_logged(MAnchor *req);
  void handle_destroy_commit(MAnchor *req);
  friend class C_AT_DestroyPrepare;

  void handle_update_prepare(MAnchor *req);
  void _update_prepare_logged(MAnchor *req);
  void handle_update_commit(MAnchor *req);
  friend class C_AT_UpdatePrepare;

  // messages
  void handle_anchor_request(MAnchor *m);  

public:
  AnchorTable(MDS *m) :
    mds(m),
    version(0), 
    opening(false), opened(false) { }

  void dispatch(class Message *m);

  version_t get_version() { return version; }

  void create_fresh() {   // reset on mkfs() to empty, loaded table.
    version = 0;
    opened = true;
    opening = false;
    anchor_map.clear();
    pending_create.clear();
    pending_destroy.clear();
    pending_update.clear();
  }

  // load/save entire table for now!
  void save(Context *onfinish);
  void load(Context *onfinish);
  void _loaded(bufferlist& bl);


};

#endif
