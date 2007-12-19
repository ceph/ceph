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
  map<version_t, int> pending_reqmds;
  map<version_t, inodeno_t> pending_create;
  map<version_t, inodeno_t> pending_destroy;
  map<version_t, pair<inodeno_t, vector<Anchor> > > pending_update;

  version_t version;  // this includes anchor_map AND pending_* state.
  version_t committing_version;
  version_t committed_version;

  // load/save state
  bool opening, opened;

  // waiters
  list<Context*> waiting_for_open;
  map<version_t, list<Context*> > waiting_for_save;

protected:

  // basic updates
  bool add(inodeno_t ino, dirfrag_t dirfrag);
  void inc(inodeno_t ino);
  void dec(inodeno_t ino);

  // mid-level
  void create_prepare(inodeno_t ino, vector<Anchor>& trace, int reqmds);
  void destroy_prepare(inodeno_t ino, int reqmds);
  void update_prepare(inodeno_t ino, vector<Anchor>& trace, int reqmds);
  void commit(version_t atid);
  void rollback(version_t atid);
  friend class EAnchor;  // used for journal replay.

  // high level interface
  void handle_lookup(MAnchor *req);

  void handle_create_prepare(MAnchor *req);
  void _create_prepare_logged(MAnchor *req, version_t atid);
  friend class C_AT_CreatePrepare;

  void handle_destroy_prepare(MAnchor *req);
  void _destroy_prepare_logged(MAnchor *req, version_t atid);
  friend class C_AT_DestroyPrepare;

  void handle_update_prepare(MAnchor *req);
  void _update_prepare_logged(MAnchor *req, version_t atid);
  friend class C_AT_UpdatePrepare;

  void handle_commit(MAnchor *req);
  void _commit_logged(MAnchor *req);
  friend class C_AT_Commit;

  void handle_rollback(MAnchor *req);

  // messages
  void handle_anchor_request(MAnchor *m);  

  void dump();

public:
  AnchorTable(MDS *m) :
    mds(m),
    version(0), committing_version(0), committed_version(0), 
    opening(false), opened(false) { }

  void dispatch(class Message *m);

  version_t get_version() { return version; }
  version_t get_committed_version() { return committed_version; }

  void create_fresh() {   
    // reset (i.e. on mkfs) to empty, but unsaved table.
    version = 1;
    opened = true;
    opening = false;
    anchor_map.clear();
    pending_create.clear();
    pending_destroy.clear();
    pending_update.clear();
  }

  // load/save entire table for now!
  void save(Context *onfinish);
  void _saved(version_t v);
  void load(Context *onfinish);
  void _loaded(bufferlist& bl);

  // recovery
  void handle_mds_recovery(int who);
  void finish_recovery();
  void resend_agree(version_t v, int who);

};

#endif
