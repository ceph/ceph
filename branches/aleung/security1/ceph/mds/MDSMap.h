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

#include "crypto/CryptoLib.h"
using namespace CryptoLib;

#include <set>
#include <map>
#include <string>
using namespace std;

class MDSMap {
 public:
  // mds states
  static const int STATE_DNE =      0;    // down, never existed.
  static const int STATE_OUT =      1;    // down, once existed, but no imports, empty log.
  static const int STATE_FAILED =   2;    // down, holds (er, held) metadata; needs to be recovered.

  static const int STATE_STANDBY =  3;    // up, but inactive.  waiting for assignment by monitor.
  static const int STATE_CREATING = 4;    // up, creating MDS instance (new journal, idalloc..)
  static const int STATE_STARTING = 5;    // up, starting prior out MDS instance.
  static const int STATE_REPLAY   = 6;    // up, scanning journal, recoverying any shared state
  static const int STATE_RESOLVE  = 7;    // up, disambiguating partial distributed operations (import/export, ...rename?)
  static const int STATE_REJOIN   = 8;    // up, replayed journal, rejoining distributed cache
  static const int STATE_ACTIVE =   9;    // up, active
  static const int STATE_STOPPING = 10;    // up, exporting metadata (-> standby or out)
  static const int STATE_STOPPED  = 11;   // up, finished stopping.  like standby, but not avail to takeover.
  
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
    case STATE_REPLAY:   return "up:replay";
    case STATE_RESOLVE:  return "up:resolve";
    case STATE_REJOIN:   return "up:rejoin";
    case STATE_ACTIVE:   return "up:active";
    case STATE_STOPPING: return "up:stopping";
    case STATE_STOPPED:  return "up:stopped";
    default: assert(0);
    }
    return 0;
  }

 protected:
  epoch_t epoch;
  utime_t ctime;

  int anchortable;   // which MDS has anchortable (fixme someday)
  int root;          // which MDS has root directory

  set<int>               mds_created;   // which mds ids have initialized journals and id tables.
  map<int,int>           mds_state;     // MDS state
  map<int,version_t>     mds_state_seq;
  map<int,entity_inst_t> mds_inst;      // up instances
  map<int,int>           mds_inc;       // incarnation count (monotonically increases)

  friend class MDSMonitor;

 public:
  MDSMap() : epoch(0), anchortable(0), root(0) {}

  epoch_t get_epoch() const { return epoch; }
  void inc_epoch() { epoch++; }

  const utime_t& get_ctime() const { return ctime; }

  int get_anchortable() const { return anchortable; }
  int get_root() const { return root; }

  // counts
  int get_num_mds() const { return mds_state.size(); }
  int get_num_mds(int state) {
    int n = 0;
    for (map<int,int>::const_iterator p = mds_state.begin();
	 p != mds_state.end();
	 p++)
      if (p->second == state) ++n;
    return n;
  }
  int get_num_up_mds() {
    int n = 0;
    for (map<int,int>::const_iterator p = mds_state.begin();
	 p != mds_state.end();
	 p++)
      if (is_up(p->first)) ++n;
    return n;
  }
  int get_num_up_or_failed_mds() {
    int n = 0;
    for (map<int,int>::const_iterator p = mds_state.begin();
	 p != mds_state.end();
	 p++)
      if (is_up(p->first) || is_failed(p->first)) 
	++n;
    return n;
  }

  // sets
  void get_mds_set(set<int>& s) {
    s.clear();
    for (map<int,int>::const_iterator p = mds_state.begin();
	 p != mds_state.end();
	 p++)
      s.insert(p->first);
  }
  void get_up_mds_set(set<int>& s) {
    s.clear();
    for (map<int,int>::const_iterator p = mds_state.begin();
	 p != mds_state.end();
	 p++)
      if (is_up(p->first)) 
	s.insert(p->first);
  }
  void get_mds_set(set<int>& s, int state) {
    s.clear();
    for (map<int,int>::const_iterator p = mds_state.begin();
	 p != mds_state.end();
	 p++)
      if (p->second == state)
	s.insert(p->first);
  }
  void get_active_mds_set(set<int>& s) {
    get_mds_set(s, MDSMap::STATE_ACTIVE);
  }
  void get_failed_mds_set(set<int>& s) {
    get_mds_set(s, MDSMap::STATE_FAILED);
  }
  void get_recovery_mds_set(set<int>& s) {
    s.clear();
    for (map<int,int>::const_iterator p = mds_state.begin();
	 p != mds_state.end();
	 p++)
      if (is_failed(p->first) || 
	  is_replay(p->first) || is_resolve(p->first) || is_rejoin(p->first) ||
	  is_active(p->first) || is_stopping(p->first))
	s.insert(p->first);
  }


  // mds states
  bool is_down(int m) { return is_dne(m) || is_out(m) || is_failed(m); }
  bool is_up(int m) { return !is_down(m); }

  bool is_dne(int m)      { return mds_state.count(m) == 0 || mds_state[m] == STATE_DNE; }
  bool is_out(int m)      { return mds_state.count(m) && mds_state[m] == STATE_OUT; }
  bool is_failed(int m)    { return mds_state.count(m) && mds_state[m] == STATE_FAILED; }

  bool is_standby(int m)  { return mds_state.count(m) && mds_state[m] == STATE_STANDBY; }
  bool is_creating(int m) { return mds_state.count(m) && mds_state[m] == STATE_CREATING; }
  bool is_starting(int m) { return mds_state.count(m) && mds_state[m] == STATE_STARTING; }
  bool is_replay(int m)    { return mds_state.count(m) && mds_state[m] == STATE_REPLAY; }
  bool is_resolve(int m)   { return mds_state.count(m) && mds_state[m] == STATE_RESOLVE; }
  bool is_rejoin(int m)    { return mds_state.count(m) && mds_state[m] == STATE_REJOIN; }
  bool is_active(int m)   { return mds_state.count(m) && mds_state[m] == STATE_ACTIVE; }
  bool is_stopping(int m) { return mds_state.count(m) && mds_state[m] == STATE_STOPPING; }
  bool is_stopped(int m)  { return mds_state.count(m) && mds_state[m] == STATE_STOPPED; }

  bool has_created(int m) { return mds_created.count(m); }

  // cluster states
  bool is_degraded() {   // degraded = some recovery in process.  fixes active membership and recovery_set.
    return get_num_mds(STATE_REPLAY) + 
      get_num_mds(STATE_RESOLVE) + 
      get_num_mds(STATE_REJOIN) + 
      get_num_mds(STATE_FAILED);
  }
  /*bool is_resolving() {  // nodes are resolving distributed ops
    return get_num_mds(STATE_RESOLVE);
    }*/
  bool is_rejoining() {  
    // nodes are rejoining cache state
    return get_num_mds(STATE_REJOIN) > 0 &&
      get_num_mds(STATE_RESOLVE) == 0 &&
      get_num_mds(STATE_REPLAY) == 0 &&
      get_num_mds(STATE_FAILED) == 0;
  }


  int get_state(int m) {
    if (mds_state.count(m)) return mds_state[m];
    return STATE_OUT;
  }

  // inst
  bool have_inst(int m) {
    return mds_inst.count(m);
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
  
  int get_inst_rank(const entity_addr_t& addr) {
    for (map<int,entity_inst_t>::iterator p = mds_inst.begin();
	 p != mds_inst.end();
	 ++p) {
      if (p->second.addr == addr) return p->first;
    }
    /*else
      for (map<int,entity_inst_t>::iterator p = mds_inst.begin();
	   p != mds_inst.end();
	   ++p) {
	if (memcmp(&p->second.addr,&inst.addr, sizeof(inst.addr)) == 0) return p->first;
      }
    */

    return -1;
  }

  int get_inc(int m) {
    assert(mds_inc.count(m));
    return mds_inc[m];
  }


  void remove_mds(int m) {
    mds_inst.erase(m);
    mds_state.erase(m);
    mds_state_seq.erase(m);
  }


  // serialize, unserialize
  void encode(bufferlist& blist) {
    blist.append((char*)&epoch, sizeof(epoch));
    blist.append((char*)&ctime, sizeof(ctime));
    blist.append((char*)&anchortable, sizeof(anchortable));
    blist.append((char*)&root, sizeof(root));
    
    ::_encode(mds_state, blist);
    ::_encode(mds_state_seq, blist);
    ::_encode(mds_inst, blist);
    ::_encode(mds_inc, blist);
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
    
    ::_decode(mds_state, blist, off);
    ::_decode(mds_state_seq, blist, off);
    ::_decode(mds_inst, blist, off);
    ::_decode(mds_inc, blist, off);
  }


  /*** mapping functions ***/

  int hash_dentry( inodeno_t dirino, const string& dn );  
};

#endif
