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

#ifndef CEPH_MDSTABLE_H
#define CEPH_MDSTABLE_H

#include "mdstypes.h"
#include "mds_table_types.h"
#include "include/buffer_fwd.h"
#include "common/Mutex.h"

class MDSRank;
class Context;
class MDSContextBase;

class MDSTable {
  Mutex mutex;
public:
  void mutex_lock() { mutex.Lock(); }
  void mutex_unlock() { mutex.Unlock(); }
  MDSRank* const mds;
protected:
  const char *table_name;
  bool per_mds;
  mds_rank_t rank;

  object_t get_object_name();
  
  static const int STATE_UNDEF   = 0;
  static const int STATE_OPENING = 1;
  static const int STATE_ACTIVE  = 2;
  //static const int STATE_COMMITTING = 3;
  int state;
  
  version_t version, committing_version, committed_version, projected_version;
  
  map<version_t, list<MDSContextBase*> > waitfor_save;
  
public:
  MDSTable(MDSRank *m, const char *n, bool is_per_mds) :
    mutex("MDSTable::mutex"),
    mds(m), table_name(n), per_mds(is_per_mds), rank(MDS_RANK_NONE),
    state(STATE_UNDEF),
    version(0), committing_version(0), committed_version(0), projected_version(0) {}
  virtual ~MDSTable() {}

  void set_rank(mds_rank_t r)
  {
    rank = r;
  }

  version_t get_version() { return version; }
  version_t get_committed_version() { return committed_version; }
  version_t get_committing_version() { return committing_version; }
  version_t get_projected_version() { return projected_version; }
  
  void force_replay_version(version_t v) {
    version = projected_version = v;
  }

  //version_t project_version() { return ++projected_version; }
  //version_t inc_version() { return ++version; }

  // load/save from disk (hack)
  bool is_undef() { return state == STATE_UNDEF; }
  bool is_active() { return state == STATE_ACTIVE; }
  bool is_opening() { return state == STATE_OPENING; }

  void reset();
  void save(MDSContextBase *onfinish=0, version_t need=0);
  void save_2(int r, version_t v);

  void shutdown() {
    if (is_active()) save(0);
  }

  void load(MDSContextBase *onfinish);
  void load_2(int, bufferlist&, Context *onfinish);

  // child must overload these
  virtual void reset_state() = 0;
  virtual void decode_state(bufferlist::iterator& p) = 0;
  virtual void encode_state(bufferlist& bl) const = 0;
};

#endif
