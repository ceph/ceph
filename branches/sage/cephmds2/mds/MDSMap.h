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


#ifndef __MDSMAP_H
#define __MDSMAP_H

#include "common/Clock.h"
#include "msg/Message.h"

#include "include/types.h"

#include <set>
#include <map>
#include <string>
using namespace std;

class MDSMap {
 public:
  static const int STATE_DNE =      0;    // down, never existed.
  static const int STATE_OUT =      1;    // down, once existed, but no imports.
  static const int STATE_FAILED =   2;    // down, holds (er, held) metadata; needs to be recovered.

  static const int STATE_STANDBY =  3;    // up, but inactive; waiting for someone to fail.
  static const int STATE_CREATING = 4;    // up, creating MDS instance (initializing journal, etc.)
  static const int STATE_STARTING = 5;    // up, scanning journal, recoverying any shared state
  static const int STATE_ACTIVE =   6;    // up, active
  static const int STATE_STOPPING = 7;    // up, exporting metadata (-> standby or out)
  static const int STATE_STOPPED  = 8;    // up, finished stopping.  like standby, but not avail to takeover.
  
  static const char *get_state_name(int s) {
    switch (s) {
      // down
    case STATE_DNE:      return "down:dne";
    case STATE_OUT:      return "down:out";
    case STATE_FAILED:   return "down:failed";
      // up
    case STATE_STANDBY:  return "up:standby";
    case STATE_CREATING: return "up:creating";
    case STATE_STARTING: return "up:starting";
    case STATE_ACTIVE:   return "up:active";
    case STATE_STOPPING: return "up:stopping";
    case STATE_STOPPED:  return "up:stopped";
    default: assert(0);
    }
  }

 protected:
  epoch_t epoch;
  utime_t ctime;

  int anchortable;   // which MDS has anchortable (fixme someday)
  int root;          // which MDS has root directory

  set<int>               mds_set;    // set of MDSs
  map<int,int>           mds_state;  // MDS state
  map<int,version_t>     mds_state_seq;
  map<int,entity_inst_t> mds_inst;   // up instances

  friend class MDSMonitor;

 public:
  MDSMap() : epoch(0), anchortable(0), root(0) {}

  epoch_t get_epoch() const { return epoch; }
  void inc_epoch() { epoch++; }

  const utime_t& get_ctime() const { return ctime; }

  int get_anchortable() const { return anchortable; }
  int get_root() const { return root; }

  int get_num_mds() const { return mds_set.size(); }
  int get_num_up_mds() {
    int n = 0;
    for (set<int>::const_iterator p = mds_set.begin();
	 p != mds_set.end();
	 p++)
      if (is_up(*p)) ++n;
    return n;
  }

  const set<int>& get_mds_set() const { return mds_set; }

  bool is_down(int m) { return is_dne(m) || is_out(m) || is_failed(m); }
  bool is_up(int m) { return !is_down(m); }

  bool is_dne(int m)      { return mds_state.count(m) == 0 || mds_state[m] == STATE_DNE; }
  bool is_out(int m)      { return mds_state.count(m) && mds_state[m] == STATE_OUT; }
  bool is_failed(int m)   { return mds_state.count(m) && mds_state[m] == STATE_FAILED; }

  bool is_standby(int m)  { return mds_state.count(m) && mds_state[m] == STATE_STANDBY; }
  bool is_creating(int m) { return mds_state.count(m) && mds_state[m] == STATE_CREATING; }
  bool is_starting(int m) { return mds_state.count(m) && mds_state[m] == STATE_STARTING; }
  bool is_active(int m)   { return mds_state.count(m) && mds_state[m] == STATE_ACTIVE; }
  bool is_stopping(int m) { return mds_state.count(m) && mds_state[m] == STATE_STOPPING; }
  bool is_stopped(int m)  { return mds_state.count(m) && mds_state[m] == STATE_STOPPED; }

  int get_state(int m) {
    if (mds_state.count(m)) return mds_state[m];
    return STATE_OUT;
  }

  const entity_inst_t& get_inst(int m) {
    assert(mds_inst.count(m));
    return mds_inst[m];
  }
  bool get_inst(int m, entity_inst_t& inst) { 
    if (mds_inst.count(m)) {
      inst = mds_inst[m];
      return true;
    } 
    return false;
  }
  
  int get_inst_rank(const entity_inst_t& inst) {
    for (map<int,entity_inst_t>::iterator p = mds_inst.begin();
	 p != mds_inst.end();
	 ++p) {
      if (p->second == inst) return p->first;
    }
    return -1;
  }

  void remove_mds(int m) {
    mds_set.erase(m);
    mds_inst.erase(m);
    mds_state.erase(m);
  }


  // serialize, unserialize
  void encode(bufferlist& blist) {
    blist.append((char*)&epoch, sizeof(epoch));
    blist.append((char*)&ctime, sizeof(ctime));
    blist.append((char*)&anchortable, sizeof(anchortable));
    blist.append((char*)&root, sizeof(root));
    
    _encode(mds_set, blist);
    _encode(mds_state, blist);
    _encode(mds_inst, blist);
  }
  
  void decode(bufferlist& blist) {
    int off = 0;
    blist.copy(off, sizeof(epoch), (char*)&epoch);
    off += sizeof(epoch);
    blist.copy(off, sizeof(ctime), (char*)&ctime);
    off += sizeof(ctime);
    blist.copy(off, sizeof(anchortable), (char*)&anchortable);
    off += sizeof(anchortable);
    blist.copy(off, sizeof(root), (char*)&root);
    off += sizeof(root);
    
    _decode(mds_set, blist, off);
    _decode(mds_state, blist, off);
    _decode(mds_inst, blist, off);
  }


  /*** mapping functions ***/

  int hash_dentry( inodeno_t dirino, const string& dn );  
};

#endif
