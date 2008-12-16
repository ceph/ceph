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
  /*
  static const int STATE_DNE =        CEPH_MDS_STATE_DNE;  // down, never existed.
  static const int STATE_DESTROYING = CEPH_MDS_STATE_DESTROYING;  // down, existing, semi-destroyed.
  static const int STATE_FAILED =     CEPH_MDS_STATE_FAILED;  // down, active subtrees; needs to be recovered.
  */
  static const int STATE_STOPPED =    CEPH_MDS_STATE_STOPPED;  // down, once existed, but no subtrees. empty log.
  static const int STATE_BOOT     =   CEPH_MDS_STATE_BOOT;  // up, boot announcement.  destiny unknown.

  static const int STATE_STANDBY  =   CEPH_MDS_STATE_STANDBY;  // up, idle.  waiting for assignment by monitor.
  static const int STATE_STANDBY_REPLAY = CEPH_MDS_STATE_STANDBY_REPLAY;  // up, replaying active node; ready to take over.

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
    case STATE_STOPPED:    return "down:stopped";
      /*
    case STATE_DNE:        return "dne";
    case STATE_DESTROYING: return "down:destroying";
      // down and in
    case STATE_FAILED:     return "down:failed";
      */
      // up and out
    case STATE_BOOT:       return "up:boot";
    case STATE_STANDBY:    return "up:standby";
    case STATE_STANDBY_REPLAY:    return "up:standby-replay";
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

  struct mds_info_t {
    int32_t mds;
    int32_t inc;
    int32_t state;
    version_t state_seq;
    entity_addr_t addr;
    utime_t laggy_since;

    mds_info_t() : mds(-1), inc(0), state(STATE_STANDBY), state_seq(0) { }

    bool laggy() const { return !(laggy_since == utime_t()); }
    void clear_laggy() { laggy_since = utime_t(); }

    entity_inst_t get_inst() const { return entity_inst_t(entity_name_t::MDS(mds), addr); }

    void encode(bufferlist& bl) const {
      ::encode(mds, bl);
      ::encode(inc, bl);
      ::encode(state, bl);
      ::encode(state_seq, bl);
      ::encode(addr, bl);
      ::encode(laggy_since, bl);
    }
    void decode(bufferlist::iterator& bl) {
      ::decode(mds, bl);
      ::decode(inc, bl);
      ::decode(state, bl);
      ::decode(state_seq, bl);
      ::decode(addr, bl);
      ::decode(laggy_since, bl);
    }
  };
  WRITE_CLASS_ENCODER(mds_info_t)


 protected:
  // base map
  epoch_t epoch;
  epoch_t client_epoch;  // incremented only when change is significant to client.
  epoch_t last_failure;  // epoch of last failure.  for inclocks
  utime_t created;

  int32_t tableserver;   // which MDS has anchortable, snaptable
  int32_t root;          // which MDS has root directory

  __u32 session_timeout;
  __u32 session_autoclose;
  
  /*
   * in: the set of logical mds #'s that define the cluster.  this is the set
   *     of mds's the metadata may be distributed over.
   * up: map from logical mds #'s to the addrs filling those roles.
   * failed: subset of @in that are failed.
   * stopped: set of nodes that have been initialized, but are not active.
   *
   *    @up + @failed = @in.  @in * @stopped = {}.
   */

  uint32_t max_mds;
  set<int32_t> in;              // currently defined cluster
  map<int32_t,int32_t> inc;     // most recent incarnation.
  set<int32_t> failed, stopped; // which roles are failed or stopped
  map<int32_t,entity_addr_t> up;  // who is in those roles
  map<entity_addr_t,mds_info_t> mds_info;

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
  unsigned get_num_mds() {
    return in.size();
  }
  int get_num_mds(int state) {
    int n = 0;
    for (map<entity_addr_t,mds_info_t>::const_iterator p = mds_info.begin();
	 p != mds_info.end();
	 p++)
      if (p->second.state == state) ++n;
    return n;
  }
  int get_num_failed() { return failed.size(); }


  // sets
  void get_mds_set(set<int>& s) {
    s = in;
  }
  void get_up_mds_set(set<int>& s) {
    for (map<int32_t,entity_addr_t>::const_iterator p = up.begin();
	 p != up.end();
	 p++)
      s.insert(p->first);
  }
  void get_active_mds_set(set<int>& s) {
    get_mds_set(s, MDSMap::STATE_ACTIVE);
  }
  void get_failed_mds_set(set<int>& s) {
    s = failed;
  }
  int get_failed() {
    if (failed.size()) return *failed.begin();
    return -1;
  }
  void get_stopped_mds_set(set<int>& s) {
    s = stopped;
  }
  void get_recovery_mds_set(set<int>& s) {
    s = failed;
    for (map<entity_addr_t,mds_info_t>::const_iterator p = mds_info.begin();
	 p != mds_info.end();
	 p++)
      if (p->second.state >= STATE_REPLAY && p->second.state <= STATE_STOPPING)
	s.insert(p->second.mds);
  }
  void get_mds_set(set<int>& s, int state) {
    for (map<entity_addr_t,mds_info_t>::const_iterator p = mds_info.begin();
	 p != mds_info.end();
	 p++)
      if (p->second.state == state)
	s.insert(p->second.mds);
  } 

  int get_random_up_mds() {
    if (up.empty())
      return -1;
    map<int32_t,entity_addr_t>::iterator p = up.begin();
    for (int n = rand() % up.size(); n; n--) p++;
    return p->first;
  }

  bool find_standby_for(int mds, entity_addr_t &a) {
    for (map<entity_addr_t,mds_info_t>::const_iterator p = mds_info.begin();
	 p != mds_info.end();
	 p++) {
      if (p->second.mds == mds &&
	  p->second.state == MDSMap::STATE_STANDBY &&
	  !p->second.laggy()) {
	a = p->second.addr;
	return true;
      }
    }
    for (map<entity_addr_t,mds_info_t>::const_iterator p = mds_info.begin();
	 p != mds_info.end();
	 p++) {
      if (p->second.mds == -1 &&
	  p->second.state == MDSMap::STATE_STANDBY &&
	  !p->second.laggy()) {
	a = p->second.addr;
	return true;
      }
    }
    return false;
  }

  // mds states
  bool is_down(int m) { return up.count(m) == 0; }
  bool is_up(int m) { return up.count(m); }
  bool is_in(int m) { return up.count(m) || failed.count(m); }
  bool is_out(int m) { return !is_in(m); }

  bool is_failed(int m)    { return failed.count(m); }
  bool is_stopped(int m)    { return stopped.count(m); }

  bool is_dne(int m)      { return in.count(m) == 0; }
  bool is_dne(entity_addr_t a) { return mds_info.count(a) == 0; }

  int get_state(int m) { return up.count(m) ? mds_info[up[m]].state : 0; }
  int get_state(entity_addr_t a) { return mds_info.count(a) ? mds_info[a].state : 0; }
  mds_info_t& get_info(entity_addr_t a) { assert(mds_info.count(a)); return mds_info[a]; }

  bool is_boot(int m)  { return get_state(m) == STATE_BOOT; }
  bool is_creating(int m) { return get_state(m) == STATE_CREATING; }
  bool is_starting(int m) { return get_state(m) == STATE_STARTING; }
  bool is_replay(int m)    { return get_state(m) == STATE_REPLAY; }
  bool is_resolve(int m)   { return get_state(m) == STATE_RESOLVE; }
  bool is_reconnect(int m) { return get_state(m) == STATE_RECONNECT; }
  bool is_rejoin(int m)    { return get_state(m) == STATE_REJOIN; }
  bool is_active(int m)   { return get_state(m) == STATE_ACTIVE; }
  bool is_stopping(int m) { return get_state(m) == STATE_STOPPING; }
  bool is_active_or_stopping(int m)   { return is_active(m) || is_stopping(m); }

  bool is_laggy(entity_addr_t a) { return mds_info.count(a) && mds_info[a].laggy(); }


  // cluster states
  bool is_full() {
    return in.size() >= max_mds;
  }
  bool is_degraded() {   // degraded = some recovery in process.  fixes active membership and recovery_set.
    return 
      get_num_mds(STATE_REPLAY) + 
      get_num_mds(STATE_RESOLVE) + 
      get_num_mds(STATE_RECONNECT) + 
      get_num_mds(STATE_REJOIN) + 
      failed.size();
  }
  bool is_rejoining() {  
    // nodes are rejoining cache state
    return 
      get_num_mds(STATE_REJOIN) > 0 &&
      get_num_mds(STATE_REPLAY) == 0 &&
      get_num_mds(STATE_RECONNECT) == 0 &&
      get_num_mds(STATE_RESOLVE) == 0 &&
      failed.empty();
  }
  bool is_stopped() {
    return up.size() == 0;
  }

  // inst
  bool have_inst(int m) {
    return up.count(m);
  }
  const entity_inst_t get_inst(int m) {
    assert(up.count(m));
    return mds_info[up[m]].get_inst();
  }
  bool get_inst(int m, entity_inst_t& inst) { 
    if (up.count(m)) {
      inst = get_inst(m);
      return true;
    } 
    return false;
  }
  
  int get_rank(const entity_addr_t& addr) {
    if (mds_info.count(addr))
      return mds_info[addr].mds;
    return -1;
  }

  int get_inc(int m) {
    if (up.count(m)) 
      return mds_info[up[m]].inc;
    return 0;
  }



  void encode(bufferlist& bl) const {
    ::encode(epoch, bl);
    ::encode(client_epoch, bl);
    ::encode(last_failure, bl);
    ::encode(root, bl);
    ::encode(session_timeout, bl);
    ::encode(session_autoclose, bl);
    ::encode(max_mds, bl);
    ::encode(mds_info, bl);

    ::encode(created, bl);
    ::encode(tableserver, bl);
    ::encode(in, bl);
    ::encode(inc, bl);
    ::encode(up, bl);
    ::encode(failed, bl);
    ::encode(stopped, bl);
  }
  void decode(bufferlist::iterator& p) {
    ::decode(epoch, p);
    ::decode(client_epoch, p);
    ::decode(last_failure, p);
    ::decode(root, p);
    ::decode(session_timeout, p);
    ::decode(session_autoclose, p);
    ::decode(max_mds, p);
    ::decode(mds_info, p);

    ::decode(created, p);
    ::decode(tableserver, p);
    ::decode(in, p);
    ::decode(inc, p);
    ::decode(up, p);
    ::decode(failed, p);
    ::decode(stopped, p);
  }
  void decode(bufferlist& bl) {
    bufferlist::iterator p = bl.begin();
    decode(p);
  }


  void print(ostream& out);
  void print_summary(ostream& out);
};
WRITE_CLASS_ENCODER(MDSMap::mds_info_t)
WRITE_CLASS_ENCODER(MDSMap)

inline ostream& operator<<(ostream& out, MDSMap& m) {
  m.print_summary(out);
  return out;
}

#endif
