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


#ifndef __MDSMAP_H
#define __MDSMAP_H

#include "include/types.h"
#include "common/Clock.h"
#include "msg/Message.h"

#include <set>
#include <map>
#include <string>
using namespace std;

#include "config.h"

/*

 boot  --> standby, creating, or starting.


 dne  ---->   creating  ----->   active*
 ^ ^___________/                /  ^ ^
 |                             /  /  |
 destroying                   /  /   |
   ^                         /  /    |
   |                        /  /     |
 stopped <---- stopping* <-/  /      |
      \                      /       |
        ----- starting* ----/        |
                                     |
 failed                              |
    \                                |
     \--> replay*  --> reconnect* --> rejoin*

     * = can fail

*/


class MDSMap {
 public:
  // mds states
  static const int STATE_DNE =        CEPH_MDS_STATE_DNE;  // down, never existed.
  static const int STATE_DESTROYING = CEPH_MDS_STATE_DESTROYING;  // down, existing, semi-destroyed.
  static const int STATE_STOPPED =    CEPH_MDS_STATE_STOPPED;  // down, once existed, but no subtrees. empty log.
  static const int STATE_FAILED =     CEPH_MDS_STATE_FAILED;  // down, active subtrees; needs to be recovered.

  static const int STATE_BOOT     =   CEPH_MDS_STATE_BOOT;  // up, boot announcement.  destiny unknown.
  static const int STATE_STANDBY  =   CEPH_MDS_STATE_STANDBY;  // up, idle.  waiting for assignment by monitor.

  static const int STATE_CREATING  =  CEPH_MDS_STATE_CREATING;  // up, creating MDS instance (new journal, idalloc..).
  static const int STATE_STARTING  =  CEPH_MDS_STATE_STARTING;  // up, starting prior stopped MDS instance.

  static const int STATE_REPLAY    =  CEPH_MDS_STATE_REPLAY;  // up, starting prior failed instance. scanning journal.
  static const int STATE_RESOLVE   =  CEPH_MDS_STATE_RESOLVE;  // up, disambiguating distributed operations (import, rename, etc.)
  static const int STATE_RECONNECT =  CEPH_MDS_STATE_RECONNECT;  // up, reconnect to clients
  static const int STATE_REJOIN    =  CEPH_MDS_STATE_REJOIN; // up, replayed journal, rejoining distributed cache
  static const int STATE_ACTIVE =     CEPH_MDS_STATE_ACTIVE; // up, active
  static const int STATE_STOPPING  =  CEPH_MDS_STATE_STOPPING; // up, exporting metadata (-> standby or out)
  
  static const char *get_state_name(int s) {
    switch (s) {
      // down and out
    case STATE_DNE:        return "down:dne";
    case STATE_DESTROYING: return "down:destroying";
    case STATE_STOPPED:    return "down:stopped";
      // down and in
    case STATE_FAILED:     return "down:failed";
      // up and out
    case STATE_BOOT:       return "up:boot";
    case STATE_STANDBY:    return "up:standby";
    case STATE_CREATING:   return "up:creating";
    case STATE_STARTING:   return "up:starting";
      // up and in
    case STATE_REPLAY:     return "up:replay";
    case STATE_RESOLVE:    return "up:resolve";
    case STATE_RECONNECT:  return "up:reconnect";
    case STATE_REJOIN:     return "up:rejoin";
    case STATE_ACTIVE:     return "up:active";
    case STATE_STOPPING:   return "up:stopping";
    default: assert(0);
    }
    return 0;
  }

 protected:
  epoch_t epoch;
  epoch_t client_epoch;       // incremented only when change is significant to client.
  epoch_t last_failure;   // epoch of last failure.  for inclocks
  utime_t created;

  int32_t max_mds;
  int32_t tableserver;   // which MDS has anchortable, snaptable
  int32_t root;          // which MDS has root directory

  __u32 session_timeout;
  __u32 session_autoclose;

  map<int32_t,int32_t>       mds_state;     // MDS state
  map<int32_t,version_t>     mds_state_seq;
  map<int32_t,entity_inst_t> mds_inst;      // up instances
  map<int32_t,int32_t>       mds_inc;       // incarnation count (monotonically increases)

  map<entity_addr_t,int32_t> standby;    // -1 == any
  map<int32_t, set<entity_addr_t> > standby_for;
  set<entity_addr_t> standby_any;

  friend class MDSMonitor;

 public:
  MDSMap() : epoch(0), client_epoch(0), last_failure(0), tableserver(0), root(0) {
    // hack.. this doesn't really belong here
    session_timeout = (int)g_conf.mds_session_timeout;
    session_autoclose = (int)g_conf.mds_session_autoclose;
  }

  utime_t get_session_timeout() {
    return utime_t(session_timeout,0);
  }
  
  epoch_t get_epoch() const { return epoch; }
  void inc_epoch() { epoch++; }

  const utime_t& get_created() const { return created; }
  void set_created(utime_t ct) { created = ct; }

  epoch_t get_last_failure() const { return last_failure; }

  int get_max_mds() const { return max_mds; }
  void set_max_mds(int m) { max_mds = m; }

  int get_tableserver() const { return tableserver; }
  int get_root() const { return root; }

  // counts
  int get_num_mds() {
    return get_num_in_mds();
  }
  int get_num_mds(int state) {
    int n = 0;
    for (map<int32_t,int32_t>::const_iterator p = mds_state.begin();
	 p != mds_state.end();
	 p++)
      if (p->second == state) ++n;
    return n;
  }

  int get_num_in_mds() { 
    int n = 0;
    for (map<int32_t,int32_t>::const_iterator p = mds_state.begin();
	 p != mds_state.end();
	 p++)
      if (p->second > 0) ++n;
    return n;
  }

  // sets
  void get_mds_set(set<int>& s) {
    for (map<int32_t,int32_t>::const_iterator p = mds_state.begin();
	 p != mds_state.end();
	 p++)
      s.insert(p->first);
  }
  void get_mds_set(set<int>& s, int state) {
    for (map<int32_t,int32_t>::const_iterator p = mds_state.begin();
	 p != mds_state.end();
	 p++)
      if (p->second == state)
	s.insert(p->first);
  } 
  void get_up_mds_set(set<int>& s) {
    for (map<int32_t,int32_t>::const_iterator p = mds_state.begin();
	 p != mds_state.end();
	 p++)
      if (is_up(p->first)) s.insert(p->first);
  }
  void get_in_mds_set(set<int>& s) {
    for (map<int32_t,int32_t>::const_iterator p = mds_state.begin();
	 p != mds_state.end();
	 p++)
      if (is_in(p->first)) s.insert(p->first);
  }
  void get_active_mds_set(set<int>& s) {
    get_mds_set(s, MDSMap::STATE_ACTIVE);
  }
  void get_failed_mds_set(set<int>& s) {
    get_mds_set(s, MDSMap::STATE_FAILED);
  }
  void get_recovery_mds_set(set<int>& s) {
    for (map<int32_t,int32_t>::const_iterator p = mds_state.begin();
	 p != mds_state.end();
	 p++)
      if (is_failed(p->first) || 
	  (p->second >= STATE_REPLAY && p->second <= STATE_STOPPING))
	s.insert(p->first);
  }

  int get_random_in_mds() {
    vector<int> v;
    for (map<int32_t,int32_t>::const_iterator p = mds_state.begin();
	 p != mds_state.end();
	 p++)
      if (p->second > 0) v.push_back(p->first);
    if (v.empty())
      return -1;
    else 
      return v[rand() % v.size()];
  }

  int get_num_standby_any() {
    return standby_any.size();
  }
  int get_num_standby_for(int m) {
    if (standby_for.count(m))
      return standby_for[m].size();
    return 0;
  }


  // mds states
  bool is_down(int m) { return is_dne(m) || is_stopped(m) || is_failed(m); }
  bool is_up(int m) { return !is_down(m); }
  bool is_in(int m) { return mds_state.count(m) && mds_state[m] > 0; }
  bool is_out(int m) { return !mds_state.count(m) || mds_state[m] <= 0; }

  bool is_dne(int m)      { return mds_state.count(m) == 0 || mds_state[m] == STATE_DNE; }
  bool is_failed(int m)    { return mds_state.count(m) && mds_state[m] == STATE_FAILED; }

  bool is_boot(int m)  { return mds_state.count(m) && mds_state[m] == STATE_BOOT; }
  bool is_creating(int m) { return mds_state.count(m) && mds_state[m] == STATE_CREATING; }
  bool is_starting(int m) { return mds_state.count(m) && mds_state[m] == STATE_STARTING; }
  bool is_replay(int m)    { return mds_state.count(m) && mds_state[m] == STATE_REPLAY; }
  bool is_resolve(int m)   { return mds_state.count(m) && mds_state[m] == STATE_RESOLVE; }
  bool is_reconnect(int m) { return mds_state.count(m) && mds_state[m] == STATE_RECONNECT; }
  bool is_rejoin(int m)    { return mds_state.count(m) && mds_state[m] == STATE_REJOIN; }
  bool is_active(int m)   { return mds_state.count(m) && mds_state[m] == STATE_ACTIVE; }
  bool is_stopping(int m) { return mds_state.count(m) && mds_state[m] == STATE_STOPPING; }
  bool is_active_or_stopping(int m)   { return is_active(m) || is_stopping(m); }
  bool is_stopped(int m)  { return mds_state.count(m) && mds_state[m] == STATE_STOPPED; }

  bool is_standby(entity_addr_t a)  { return standby.count(a); }

  // cluster states
  bool is_full() {
    return get_num_in_mds() >= max_mds;
  }
  bool is_degraded() {   // degraded = some recovery in process.  fixes active membership and recovery_set.
    return 
      get_num_mds(STATE_REPLAY) + 
      get_num_mds(STATE_RESOLVE) + 
      get_num_mds(STATE_RECONNECT) + 
      get_num_mds(STATE_REJOIN) + 
      get_num_mds(STATE_FAILED);
  }
  bool is_rejoining() {  
    // nodes are rejoining cache state
    return 
      get_num_mds(STATE_REJOIN) > 0 &&
      get_num_mds(STATE_REPLAY) == 0 &&
      get_num_mds(STATE_RECONNECT) == 0 &&
      get_num_mds(STATE_RESOLVE) == 0 &&
      get_num_mds(STATE_FAILED) == 0;
  }
  bool is_stopped() {
    return
      get_num_in_mds() == 0 &&
      get_num_mds(STATE_CREATING) == 0 &&
      get_num_mds(STATE_STARTING) == 0 &&
      get_num_mds(STATE_STANDBY) == 0;
  }

  bool would_be_overfull_with(int mds) {
    int in = 1;  // mds!
    for (map<int32_t,int32_t>::const_iterator p = mds_state.begin();
	 p != mds_state.end();
	 p++) {
      if (p->first == mds) continue;
      if (p->second > 0 ||
	  p->second == STATE_STARTING ||
	  p->second == STATE_CREATING) 
	in++;
    }
    return (in > max_mds);
  }

  int get_state(int m) {
    if (mds_state.count(m)) 
      return mds_state[m];
    else
      return STATE_DNE;
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
  
  int get_addr_rank(const entity_addr_t& addr) {
    for (map<int32_t,entity_inst_t>::iterator p = mds_inst.begin();
	 p != mds_inst.end();
	 ++p) {
      if (p->second.addr == addr) return p->first;
    }
    if (standby.count(addr))
      return -2;
    return -1;
  }

  int get_inc(int m) {
    if (mds_inc.count(m))
      return mds_inc[m];
    return 0;
  }


  void remove_mds(int m) {
    mds_inst.erase(m);
    mds_state.erase(m);
    mds_state_seq.erase(m);
  }


  // serialize, unserialize
  void encode(bufferlist& bl) {
    ::encode(epoch, bl);
    ::encode(client_epoch, bl);
    ::encode(last_failure, bl);
    ::encode(created, bl);
    ::encode(tableserver, bl);
    ::encode(root, bl);
    ::encode(session_timeout, bl);
    ::encode(session_autoclose, bl);
    ::encode(max_mds, bl);
    ::encode(mds_state, bl);
    ::encode(mds_state_seq, bl);
    ::encode(mds_inst, bl);
    ::encode(mds_inc, bl);
    ::encode(standby, bl);
    ::encode(standby_for, bl);
    ::encode(standby_any, bl);
  }
  
  void decode(bufferlist& bl) {
    bufferlist::iterator p = bl.begin();
    ::decode(epoch, p);
    ::decode(client_epoch, p);
    ::decode(last_failure, p);
    ::decode(created, p);
    ::decode(tableserver, p);
    ::decode(root, p);
    ::decode(session_timeout, p);
    ::decode(session_autoclose, p);
    ::decode(max_mds, p);
    ::decode(mds_state, p);
    ::decode(mds_state_seq, p);
    ::decode(mds_inst, p);
    ::decode(mds_inc, p);
    ::decode(standby, p);
    ::decode(standby_for, p);
    ::decode(standby_any, p);
  }


  void print(ostream& out);
  void print_summary(ostream& out);
};

#endif
