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


#ifndef CEPH_MDSMAP_H
#define CEPH_MDSMAP_H

#include <errno.h>

#include "include/types.h"
#include "common/Clock.h"
#include "msg/Message.h"

#include <set>
#include <map>
#include <string>
using namespace std;

#include "common/config.h"

#include "include/CompatSet.h"
#include "include/ceph_features.h"
#include "common/Formatter.h"

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

class CephContext;

extern CompatSet get_mdsmap_compat_set();
extern CompatSet get_mdsmap_compat_set_base(); // pre v0.20

#define MDS_FEATURE_INCOMPAT_BASE CompatSet::Feature(1, "base v0.20")
#define MDS_FEATURE_INCOMPAT_CLIENTRANGES CompatSet::Feature(2, "client writeable ranges")
#define MDS_FEATURE_INCOMPAT_FILELAYOUT CompatSet::Feature(3, "default file layouts on dirs")
#define MDS_FEATURE_INCOMPAT_DIRINODE CompatSet::Feature(4, "dir inode in separate object")
#define MDS_FEATURE_INCOMPAT_ENCODING CompatSet::Feature(5, "mds uses versioned encoding")

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
  static const int STATE_ONESHOT_REPLAY = CEPH_MDS_STATE_REPLAYONCE; //up, replaying active node journal to verify it, then shutting down

  static const int STATE_CREATING  =  CEPH_MDS_STATE_CREATING;  // up, creating MDS instance (new journal, idalloc..).
  static const int STATE_STARTING  =  CEPH_MDS_STATE_STARTING;  // up, starting prior stopped MDS instance.

  static const int STATE_REPLAY    =  CEPH_MDS_STATE_REPLAY;  // up, starting prior failed instance. scanning journal.
  static const int STATE_RESOLVE   =  CEPH_MDS_STATE_RESOLVE;  // up, disambiguating distributed operations (import, rename, etc.)
  static const int STATE_RECONNECT =  CEPH_MDS_STATE_RECONNECT;  // up, reconnect to clients
  static const int STATE_REJOIN    =  CEPH_MDS_STATE_REJOIN; // up, replayed journal, rejoining distributed cache
  static const int STATE_CLIENTREPLAY = CEPH_MDS_STATE_CLIENTREPLAY; // up, active
  static const int STATE_ACTIVE =     CEPH_MDS_STATE_ACTIVE; // up, active
  static const int STATE_STOPPING  =  CEPH_MDS_STATE_STOPPING; // up, exporting metadata (-> standby or out)

  // indicate startup standby preferences for MDS
  // of course, if they have a specific rank to follow, they just set that!
  static const int MDS_NO_STANDBY_PREF = -1; // doesn't have instructions to do anything
  static const int MDS_STANDBY_ANY = -2; // is instructed to be standby-replay, may
                                     // or may not have specific name to follow
  static const int MDS_STANDBY_NAME = -3; // standby for a named MDS
  static const int MDS_MATCHED_ACTIVE = -4; // has a matched standby, which if up
                                            // it should follow, but otherwise should
                                            // be assigned a rank

  struct mds_info_t {
    uint64_t global_id;
    string name;
    int32_t rank;
    int32_t inc;
    int32_t state;
    version_t state_seq;
    entity_addr_t addr;
    utime_t laggy_since;
    int32_t standby_for_rank;
    string standby_for_name;
    set<int32_t> export_targets;

    mds_info_t() : global_id(0), rank(-1), inc(0), state(STATE_STANDBY), state_seq(0),
		   standby_for_rank(MDS_NO_STANDBY_PREF) { }

    bool laggy() const { return !(laggy_since == utime_t()); }
    void clear_laggy() { laggy_since = utime_t(); }

    entity_inst_t get_inst() const { return entity_inst_t(entity_name_t::MDS(rank), addr); }

    void encode(bufferlist& bl, uint64_t features) const {
      if ((features & CEPH_FEATURE_MDSENC) == 0 ) encode_unversioned(bl);
      else encode_versioned(bl, features);
    }
    void decode(bufferlist::iterator& p);
    void dump(Formatter *f) const;
    static void generate_test_instances(list<mds_info_t*>& ls);
  private:
    void encode_versioned(bufferlist& bl, uint64_t features) const;
    void encode_unversioned(bufferlist& bl) const;
  };


protected:
  // base map
  epoch_t epoch;
  uint32_t flags;        // flags
  epoch_t last_failure;  // mds epoch of last failure
  epoch_t last_failure_osd_epoch; // osd epoch of last failure; any mds entering replay needs
                                  // at least this osdmap to ensure the blacklist propagates.
  utime_t created, modified;

  int32_t tableserver;   // which MDS has anchortable, snaptable
  int32_t root;          // which MDS has root directory

  __u32 session_timeout;
  __u32 session_autoclose;
  uint64_t max_file_size;

  set<int64_t> data_pools;  // file data pools available to clients (via an ioctl).  first is the default.
  int64_t cas_pool;            // where CAS objects go
  int64_t metadata_pool;       // where fs metadata objects go
  
  /*
   * in: the set of logical mds #'s that define the cluster.  this is the set
   *     of mds's the metadata may be distributed over.
   * up: map from logical mds #'s to the addrs filling those roles.
   * failed: subset of @in that are failed.
   * stopped: set of nodes that have been initialized, but are not active.
   *
   *    @up + @failed = @in.  @in * @stopped = {}.
   */

  uint32_t max_mds; /* The maximum number of active MDSes. Also, the maximum rank. */

  set<int32_t> in;              // currently defined cluster
  map<int32_t,int32_t> inc;     // most recent incarnation.
  set<int32_t> failed, stopped; // which roles are failed or stopped
  map<int32_t,uint64_t> up;        // who is in those roles
  map<uint64_t,mds_info_t> mds_info;

public:
  CompatSet compat;

  friend class MDSMonitor;

public:
  MDSMap() 
    : epoch(0), flags(0), last_failure(0), last_failure_osd_epoch(0), tableserver(0), root(0),
      session_timeout(0),
      session_autoclose(0),
      max_file_size(0),
      cas_pool(-1),
      metadata_pool(0),
      max_mds(0)
  { }

  utime_t get_session_timeout() {
    return utime_t(session_timeout,0);
  }
  uint64_t get_max_filesize() { return max_file_size; }
  
  int get_flags() const { return flags; }
  int test_flag(int f) const { return flags & f; }
  void set_flag(int f) { flags |= f; }
  void clear_flag(int f) { flags &= ~f; }

  epoch_t get_epoch() const { return epoch; }
  void inc_epoch() { epoch++; }

  const utime_t& get_created() const { return created; }
  void set_created(utime_t ct) { modified = created = ct; }
  const utime_t& get_modified() const { return modified; }
  void set_modified(utime_t mt) { modified = mt; }

  epoch_t get_last_failure() const { return last_failure; }
  epoch_t get_last_failure_osd_epoch() const { return last_failure_osd_epoch; }

  unsigned get_max_mds() const { return max_mds; }
  void set_max_mds(int m) { max_mds = m; }

  int get_tableserver() const { return tableserver; }
  int get_root() const { return root; }

  const set<int64_t> &get_data_pools() const { return data_pools; }
  int64_t get_first_data_pool() const { return *data_pools.begin(); }
  int64_t get_cas_pool() const { return cas_pool; }
  int64_t get_metadata_pool() const { return metadata_pool; }
  bool is_data_pool(int64_t poolid) const {
    return data_pools.count(poolid);
  }

  const map<uint64_t,mds_info_t>& get_mds_info() { return mds_info; }
  const mds_info_t& get_mds_info_gid(uint64_t gid) {
    assert(mds_info.count(gid));
    return mds_info[gid];
  }
  const mds_info_t& get_mds_info(int m) {
    assert(up.count(m) && mds_info.count(up[m]));
    return mds_info[up[m]];
  }
  uint64_t find_mds_gid_by_name(const string& s) {
    for (map<uint64_t,mds_info_t>::const_iterator p = mds_info.begin();
	 p != mds_info.end();
	 ++p) {
      if (p->second.name == s) {
	return p->first;
      }
    }
    return 0;
  }

  // counts
  unsigned get_num_in_mds() {
    return in.size();
  }
  unsigned get_num_up_mds() {
    return up.size();
  }
  int get_num_failed_mds() {
    return failed.size();
  }
  unsigned get_num_mds(int state) const {
    unsigned n = 0;
    for (map<uint64_t,mds_info_t>::const_iterator p = mds_info.begin();
	 p != mds_info.end();
	 ++p)
      if (p->second.state == state) ++n;
    return n;
  }

  // data pools
  void add_data_pool(int64_t poolid) {
    data_pools.insert(poolid);
  }
  int remove_data_pool(int64_t poolid) {
    set<int64_t>::iterator p = data_pools.find(poolid);
    if (p == data_pools.end())
      return -ENOENT;
    data_pools.erase(p);
    return 0;
  }

  // sets
  void get_mds_set(set<int>& s) {
    s = in;
  }
  void get_up_mds_set(set<int>& s) {
    for (map<int32_t,uint64_t>::const_iterator p = up.begin();
	 p != up.end();
	 ++p)
      s.insert(p->first);
  }
  void get_active_mds_set(set<int>& s) {
    get_mds_set(s, MDSMap::STATE_ACTIVE);
  }
  void get_failed_mds_set(set<int>& s) {
    s = failed;
  }
  int get_failed() {
    if (!failed.empty()) return *failed.begin();
    return -1;
  }
  void get_stopped_mds_set(set<int>& s) {
    s = stopped;
  }
  void get_recovery_mds_set(set<int>& s) {
    s = failed;
    for (map<uint64_t,mds_info_t>::const_iterator p = mds_info.begin();
	 p != mds_info.end();
	 ++p)
      if (p->second.state >= STATE_REPLAY && p->second.state <= STATE_STOPPING)
	s.insert(p->second.rank);
  }
  void get_clientreplay_or_active_or_stopping_mds_set(set<int>& s) {
    for (map<uint64_t,mds_info_t>::const_iterator p = mds_info.begin();
	 p != mds_info.end();
	 ++p)
      if (p->second.state >= STATE_CLIENTREPLAY && p->second.state <= STATE_STOPPING)
	s.insert(p->second.rank);
  }
  void get_mds_set(set<int>& s, int state) {
    for (map<uint64_t,mds_info_t>::const_iterator p = mds_info.begin();
	 p != mds_info.end();
	 ++p)
      if (p->second.state == state)
	s.insert(p->second.rank);
  } 

  int get_random_up_mds() {
    if (up.empty())
      return -1;
    map<int32_t,uint64_t>::iterator p = up.begin();
    for (int n = rand() % up.size(); n; n--)
      ++p;
    return p->first;
  }

  const mds_info_t* find_by_name(const string& name) const {
    for (map<uint64_t,mds_info_t>::const_iterator p = mds_info.begin();
	 p != mds_info.end();
	 ++p) {
      if (p->second.name == name)
	return &p->second;
    }
    return NULL;
  }

  uint64_t find_standby_for(int mds, string& name) {
    map<uint64_t, mds_info_t>::const_iterator generic_standby
      = mds_info.end();
    for (map<uint64_t,mds_info_t>::const_iterator p = mds_info.begin();
	 p != mds_info.end();
	 ++p) {
      if ((p->second.state != MDSMap::STATE_STANDBY && p->second.state != MDSMap::STATE_STANDBY_REPLAY) ||
	  p->second.laggy() ||
	  p->second.rank >= 0)
	continue;
      if (p->second.standby_for_rank == mds || (name.length() && p->second.standby_for_name == name))
	return p->first;
      if (p->second.standby_for_rank < 0 && p->second.standby_for_name.length() == 0)
	generic_standby = p;
    }
    if (generic_standby != mds_info.end())
      return generic_standby->first;
    return 0;
  }
  uint64_t find_unused_for(int mds, string& name) {
    for (map<uint64_t,mds_info_t>::const_iterator p = mds_info.begin();
	 p != mds_info.end();
	 ++p) {
      if (p->second.state != MDSMap::STATE_STANDBY ||
	  p->second.laggy() ||
	  p->second.rank >= 0)
	continue;
      if ((p->second.standby_for_rank == MDS_NO_STANDBY_PREF ||
	   p->second.standby_for_rank == MDS_MATCHED_ACTIVE ||
	   (p->second.standby_for_rank == MDS_STANDBY_ANY && g_conf->mon_force_standby_active))) {
	return p->first;
      }
    }
    return 0;
  }
  uint64_t find_replacement_for(int mds, string& name) {
    uint64_t standby = find_standby_for(mds, name);
    if (standby)
      return standby;
    else
      return find_unused_for(mds, name);
  }

  void get_health(list<pair<health_status_t,string> >& summary,
		  list<pair<health_status_t,string> > *detail) const;

  // mds states
  bool is_down(int m) const { return up.count(m) == 0; }
  bool is_up(int m) const { return up.count(m); }
  bool is_in(int m) const { return up.count(m) || failed.count(m); }
  bool is_out(int m) const { return !is_in(m); }

  bool is_failed(int m) const   { return failed.count(m); }
  bool is_stopped(int m) const    { return stopped.count(m); }

  bool is_dne(int m) const      { return in.count(m) == 0; }
  bool is_dne_gid(uint64_t gid) const     { return mds_info.count(gid) == 0; }

  int get_state(int m) const {
    map<int32_t,uint64_t>::const_iterator u = up.find(m);
    if (u == up.end())
      return 0;
    return get_state_gid(u->second);
  }
  int get_state_gid(uint64_t gid) const {
    map<uint64_t,mds_info_t>::const_iterator i = mds_info.find(gid);
    if (i == mds_info.end())
      return 0;
    return i->second.state;
  }

  mds_info_t& get_info(int m) { assert(up.count(m)); return mds_info[up[m]]; }
  mds_info_t& get_info_gid(uint64_t gid) { assert(mds_info.count(gid)); return mds_info[gid]; }

  bool is_boot(int m) const { return get_state(m) == STATE_BOOT; }
  bool is_creating(int m) const { return get_state(m) == STATE_CREATING; }
  bool is_starting(int m) const { return get_state(m) == STATE_STARTING; }
  bool is_replay(int m) const   { return get_state(m) == STATE_REPLAY; }
  bool is_resolve(int m) const  { return get_state(m) == STATE_RESOLVE; }
  bool is_reconnect(int m) const { return get_state(m) == STATE_RECONNECT; }
  bool is_rejoin(int m) const   { return get_state(m) == STATE_REJOIN; }
  bool is_clientreplay(int m) const { return get_state(m) == STATE_CLIENTREPLAY; }
  bool is_active(int m) const  { return get_state(m) == STATE_ACTIVE; }
  bool is_stopping(int m) const { return get_state(m) == STATE_STOPPING; }
  bool is_active_or_stopping(int m) const {
    return is_active(m) || is_stopping(m);
  }
  bool is_clientreplay_or_active_or_stopping(int m) const {
    return is_clientreplay(m) || is_active(m) || is_stopping(m);
  }

  bool is_followable(int m) const {
    return (is_resolve(m) ||
	    is_replay(m) ||
	    is_rejoin(m) ||
	    is_clientreplay(m) ||
	    is_active(m) ||
	    is_stopping(m));
  }

  bool is_laggy_gid(uint64_t gid) const {
    if (!mds_info.count(gid))
      return false;
    map<uint64_t,mds_info_t>::const_iterator p = mds_info.find(gid);
    return p->second.laggy();
  }


  // cluster states
  bool is_full() const {
    return in.size() >= max_mds;
  }
  bool is_degraded() const {   // degraded = some recovery in process.  fixes active membership and recovery_set.
    return 
      get_num_mds(STATE_REPLAY) + 
      get_num_mds(STATE_RESOLVE) + 
      get_num_mds(STATE_RECONNECT) + 
      get_num_mds(STATE_REJOIN) + 
      failed.size();
  }
  bool is_any_failed() {
    return failed.size();
  }
  bool is_resolving() {
    return
      get_num_mds(STATE_RESOLVE) > 0 &&
      get_num_mds(STATE_REPLAY) == 0 &&
      failed.empty();
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
    return up.empty();
  }

  // inst
  bool have_inst(int m) {
    return up.count(m);
  }
  const entity_inst_t get_inst(int m) {
    assert(up.count(m));
    return mds_info[up[m]].get_inst();
  }
  const entity_addr_t get_addr(int m) {
    assert(up.count(m));
    return mds_info[up[m]].addr;
  }
  bool get_inst(int m, entity_inst_t& inst) { 
    if (up.count(m)) {
      inst = get_inst(m);
      return true;
    } 
    return false;
  }
  
  int get_rank_gid(uint64_t gid) {
    if (mds_info.count(gid))
      return mds_info[gid].rank;
    return -1;
  }

  int get_inc(int m) {
    if (up.count(m)) 
      return mds_info[up[m]].inc;
    return 0;
  }
  int get_inc_gid(uint64_t gid) {
    if (mds_info.count(gid))
      return mds_info[gid].inc;
    return -1;
  }
  void encode(bufferlist& bl, uint64_t features) const;
  void decode(bufferlist::iterator& p);
  void decode(bufferlist& bl) {
    bufferlist::iterator p = bl.begin();
    decode(p);
  }


  void print(ostream& out);
  void print_summary(ostream& out);

  void dump(Formatter *f) const;
  static void generate_test_instances(list<MDSMap*>& ls);
};
WRITE_CLASS_ENCODER_FEATURES(MDSMap::mds_info_t)
WRITE_CLASS_ENCODER_FEATURES(MDSMap)

inline ostream& operator<<(ostream& out, MDSMap& m) {
  m.print_summary(out);
  return out;
}

#endif
