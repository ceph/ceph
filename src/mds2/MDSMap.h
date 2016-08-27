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

#include "common/config.h"

#include "include/CompatSet.h"
#include "include/ceph_features.h"
#include "common/Formatter.h"
#include "mdstypes.h"

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

extern CompatSet get_mdsmap_compat_set_all();
extern CompatSet get_mdsmap_compat_set_default();
extern CompatSet get_mdsmap_compat_set_base(); // pre v0.20

#define MDS_FEATURE_INCOMPAT_BASE CompatSet::Feature(1, "base v0.20")
#define MDS_FEATURE_INCOMPAT_CLIENTRANGES CompatSet::Feature(2, "client writeable ranges")
#define MDS_FEATURE_INCOMPAT_FILELAYOUT CompatSet::Feature(3, "default file layouts on dirs")
#define MDS_FEATURE_INCOMPAT_DIRINODE CompatSet::Feature(4, "dir inode in separate object")
#define MDS_FEATURE_INCOMPAT_ENCODING CompatSet::Feature(5, "mds uses versioned encoding")
#define MDS_FEATURE_INCOMPAT_OMAPDIRFRAG CompatSet::Feature(6, "dirfrag is stored in omap")
#define MDS_FEATURE_INCOMPAT_INLINE CompatSet::Feature(7, "mds uses inline data")
#define MDS_FEATURE_INCOMPAT_NOANCHOR CompatSet::Feature(8, "no anchor table")
#define MDS_FEATURE_INCOMPAT_FILE_LAYOUT_V2 CompatSet::Feature(8, "file layout v2")

#define MDS_FS_NAME_DEFAULT "cephfs"

class MDSMap {
public:
  /* These states are the union of the set of possible states of an MDS daemon,
   * and the set of possible states of an MDS rank */
  typedef enum {
    // States of an MDS daemon not currently holding a rank
    // ====================================================
    STATE_NULL     =   CEPH_MDS_STATE_NULL,                                  // null value for fns returning this type.
    STATE_BOOT     =   CEPH_MDS_STATE_BOOT,                // up, boot announcement.  destiny unknown.
    STATE_STANDBY  =   CEPH_MDS_STATE_STANDBY,             // up, idle.  waiting for assignment by monitor.
    STATE_STANDBY_REPLAY = CEPH_MDS_STATE_STANDBY_REPLAY,  // up, replaying active node, ready to take over.

    // States of an MDS rank, and of any MDS daemon holding that rank
    // ==============================================================
    STATE_STOPPED  =   CEPH_MDS_STATE_STOPPED,        // down, once existed, but no subtrees. empty log.  may not be held by a daemon.

    STATE_CREATING  =  CEPH_MDS_STATE_CREATING,       // up, creating MDS instance (new journal, idalloc..).
    STATE_STARTING  =  CEPH_MDS_STATE_STARTING,       // up, starting prior stopped MDS instance.

    STATE_REPLAY    =  CEPH_MDS_STATE_REPLAY,         // up, starting prior failed instance. scanning journal.
    STATE_RESOLVE   =  CEPH_MDS_STATE_RESOLVE,        // up, disambiguating distributed operations (import, rename, etc.)
    STATE_RECONNECT =  CEPH_MDS_STATE_RECONNECT,      // up, reconnect to clients
    STATE_REJOIN    =  CEPH_MDS_STATE_REJOIN,         // up, replayed journal, rejoining distributed cache
    STATE_CLIENTREPLAY = CEPH_MDS_STATE_CLIENTREPLAY, // up, active
    STATE_ACTIVE =     CEPH_MDS_STATE_ACTIVE,         // up, active
    STATE_STOPPING  =  CEPH_MDS_STATE_STOPPING,       // up, exporting metadata (-> standby or out)
    STATE_DNE       =  CEPH_MDS_STATE_DNE,             // down, rank does not exist

    // State which a daemon may send to MDSMonitor in its beacon
    // to indicate that offline repair is required.  Daemon must stop
    // immediately after indicating this state.
    STATE_DAMAGED   = CEPH_MDS_STATE_DAMAGED

    /*
     * In addition to explicit states, an MDS rank implicitly in state:
     *  - STOPPED if it is not currently associated with an MDS daemon gid but it
     *    is in MDSMap::stopped
     *  - FAILED if it is not currently associated with an MDS daemon gid but it
     *    is in MDSMap::failed
     *  - DNE if it is not currently associated with an MDS daemon gid and it is
     *    missing from both MDSMap::failed and MDSMap::stopped
     */
  } DaemonState;

  struct mds_info_t {
    mds_gid_t global_id;
    std::string name;
    mds_rank_t rank;
    int32_t inc;
    MDSMap::DaemonState state;
    version_t state_seq;
    entity_addr_t addr;
    utime_t laggy_since;
    mds_rank_t standby_for_rank;
    std::string standby_for_name;
    fs_cluster_id_t standby_for_fscid;
    bool standby_replay;
    std::set<mds_rank_t> export_targets;
    uint64_t mds_features;

    mds_info_t() : global_id(MDS_GID_NONE), rank(MDS_RANK_NONE), inc(0),
                   state(STATE_STANDBY), state_seq(0),
                   standby_for_rank(MDS_RANK_NONE),
                   standby_for_fscid(FS_CLUSTER_ID_NONE),
                   standby_replay(false)
    { }

    bool laggy() const { return !(laggy_since == utime_t()); }
    void clear_laggy() { laggy_since = utime_t(); }

    entity_inst_t get_inst() const { return entity_inst_t(entity_name_t::MDS(rank), addr); }

    void encode(bufferlist& bl, uint64_t features) const {
      if ((features & CEPH_FEATURE_MDSENC) == 0 ) encode_unversioned(bl);
      else encode_versioned(bl, features);
    }
    void decode(bufferlist::iterator& p);
    void dump(Formatter *f) const;
    void print_summary(ostream &out) const;
    static void generate_test_instances(list<mds_info_t*>& ls);
  private:
    void encode_versioned(bufferlist& bl, uint64_t features) const;
    void encode_unversioned(bufferlist& bl) const;
  };


protected:
  // base map
  epoch_t epoch;
  bool enabled;
  std::string fs_name;
  uint32_t flags;        // flags
  epoch_t last_failure;  // mds epoch of last failure
  epoch_t last_failure_osd_epoch; // osd epoch of last failure; any mds entering replay needs
                                  // at least this osdmap to ensure the blacklist propagates.
  utime_t created, modified;

  mds_rank_t tableserver;   // which MDS has snaptable
  mds_rank_t root;          // which MDS has root directory

  __u32 session_timeout;
  __u32 session_autoclose;
  uint64_t max_file_size;

  std::set<int64_t> data_pools;  // file data pools available to clients (via an ioctl).  first is the default.
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

  mds_rank_t max_mds; /* The maximum number of active MDSes. Also, the maximum rank. */

  std::set<mds_rank_t> in;              // currently defined cluster

  // which ranks are failed, stopped, damaged (i.e. not held by a daemon)
  std::set<mds_rank_t> failed, stopped, damaged;
  std::map<mds_rank_t, mds_gid_t> up;        // who is in those roles
  std::map<mds_gid_t, mds_info_t> mds_info;

  uint8_t ever_allowed_features; //< bitmap of features the cluster has allowed
  uint8_t explicitly_allowed_features; //< bitmap of features explicitly enabled 

  bool inline_data_enabled;

  uint64_t cached_up_features;

public:
  CompatSet compat;

  friend class MDSMonitor;
  friend class Filesystem;
  friend class FSMap;

public:
  MDSMap() 
    : epoch(0), enabled(false), fs_name(MDS_FS_NAME_DEFAULT),
      flags(0), last_failure(0),
      last_failure_osd_epoch(0),
      tableserver(0), root(0),
      session_timeout(0),
      session_autoclose(0),
      max_file_size(0),
      cas_pool(-1),
      metadata_pool(-1),
      max_mds(0),
      ever_allowed_features(0),
      explicitly_allowed_features(0),
      inline_data_enabled(false),
      cached_up_features(0)
  { }

  bool get_inline_data_enabled() { return inline_data_enabled; }
  void set_inline_data_enabled(bool enabled) { inline_data_enabled = enabled; }

  utime_t get_session_timeout() {
    return utime_t(session_timeout,0);
  }
  uint64_t get_max_filesize() { return max_file_size; }
  void set_max_filesize(uint64_t m) { max_file_size = m; }
  
  int get_flags() const { return flags; }
  bool test_flag(int f) const { return flags & f; }
  void set_flag(int f) { flags |= f; }
  void clear_flag(int f) { flags &= ~f; }

  const std::string &get_fs_name() const {return fs_name;}

  void set_snaps_allowed() {
    set_flag(CEPH_MDSMAP_ALLOW_SNAPS);
    ever_allowed_features |= CEPH_MDSMAP_ALLOW_SNAPS;
    explicitly_allowed_features |= CEPH_MDSMAP_ALLOW_SNAPS;
  }
  void clear_snaps_allowed() { clear_flag(CEPH_MDSMAP_ALLOW_SNAPS); }
  bool allows_snaps() const { return test_flag(CEPH_MDSMAP_ALLOW_SNAPS); }

  void set_multimds_allowed() {
    set_flag(CEPH_MDSMAP_ALLOW_MULTIMDS);
    ever_allowed_features |= CEPH_MDSMAP_ALLOW_MULTIMDS;
    explicitly_allowed_features |= CEPH_MDSMAP_ALLOW_MULTIMDS;
  }
  void clear_multimds_allowed() { clear_flag(CEPH_MDSMAP_ALLOW_MULTIMDS); }
  bool allows_multimds() const { return test_flag(CEPH_MDSMAP_ALLOW_MULTIMDS); }

  void set_dirfrags_allowed() {
    set_flag(CEPH_MDSMAP_ALLOW_DIRFRAGS);
    ever_allowed_features |= CEPH_MDSMAP_ALLOW_DIRFRAGS;
    explicitly_allowed_features |= CEPH_MDSMAP_ALLOW_DIRFRAGS;
  }
  void clear_dirfrags_allowed() { clear_flag(CEPH_MDSMAP_ALLOW_DIRFRAGS); }
  bool allows_dirfrags() const { return test_flag(CEPH_MDSMAP_ALLOW_DIRFRAGS); }

  epoch_t get_epoch() const { return epoch; }
  void inc_epoch() { epoch++; }

  bool get_enabled() const { return enabled; }

  const utime_t& get_created() const { return created; }
  void set_created(utime_t ct) { modified = created = ct; }
  const utime_t& get_modified() const { return modified; }
  void set_modified(utime_t mt) { modified = mt; }

  epoch_t get_last_failure() const { return last_failure; }
  epoch_t get_last_failure_osd_epoch() const { return last_failure_osd_epoch; }

  mds_rank_t get_max_mds() const { return max_mds; }
  void set_max_mds(mds_rank_t m) { max_mds = m; }

  mds_rank_t get_tableserver() const { return tableserver; }
  mds_rank_t get_root() const { return root; }

  const std::set<int64_t> &get_data_pools() const { return data_pools; }
  int64_t get_first_data_pool() const { return *data_pools.begin(); }
  int64_t get_metadata_pool() const { return metadata_pool; }
  bool is_data_pool(int64_t poolid) const {
    return data_pools.count(poolid);
  }

  bool pool_in_use(int64_t poolid) const {
    return get_enabled() && (is_data_pool(poolid) || metadata_pool == poolid);
  }

  const std::map<mds_gid_t,mds_info_t>& get_mds_info() const { return mds_info; }
  const mds_info_t& get_mds_info_gid(mds_gid_t gid) const {
    return mds_info.at(gid);
  }
  const mds_info_t& get_mds_info(mds_rank_t m) const {
    assert(up.count(m) && mds_info.count(up.at(m)));
    return mds_info.at(up.at(m));
  }
  mds_gid_t find_mds_gid_by_name(const std::string& s) const {
    for (std::map<mds_gid_t,mds_info_t>::const_iterator p = mds_info.begin();
	 p != mds_info.end();
	 ++p) {
      if (p->second.name == s) {
	return p->first;
      }
    }
    return MDS_GID_NONE;
  }

  // counts
  unsigned get_num_in_mds() const {
    return in.size();
  }
  unsigned get_num_up_mds() const {
    return up.size();
  }
  int get_num_failed_mds() const {
    return failed.size();
  }
  unsigned get_num_mds(int state) const {
    unsigned n = 0;
    for (std::map<mds_gid_t,mds_info_t>::const_iterator p = mds_info.begin();
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
    std::set<int64_t>::iterator p = data_pools.find(poolid);
    if (p == data_pools.end())
      return -ENOENT;
    data_pools.erase(p);
    return 0;
  }

  // sets
  void get_mds_set(std::set<mds_rank_t>& s) const {
    s = in;
  }
  void get_up_mds_set(std::set<mds_rank_t>& s) const {
    for (std::map<mds_rank_t, mds_gid_t>::const_iterator p = up.begin();
	 p != up.end();
	 ++p)
      s.insert(p->first);
  }
  void get_active_mds_set(std::set<mds_rank_t>& s) const {
    get_mds_set(s, MDSMap::STATE_ACTIVE);
  }
  void get_failed_mds_set(std::set<mds_rank_t>& s) const {
    s = failed;
  }

  // features
  uint64_t get_up_features() {
    if (!cached_up_features) {
      bool first = true;
      for (std::map<mds_rank_t, mds_gid_t>::const_iterator p = up.begin();
	   p != up.end();
	   ++p) {
	std::map<mds_gid_t, mds_info_t>::const_iterator q =
	  mds_info.find(p->second);
	assert(q != mds_info.end());
	if (first) {
	  cached_up_features = q->second.mds_features;
	  first = false;
	} else {
	  cached_up_features &= q->second.mds_features;
	}
      }
    }
    return cached_up_features;
  }

  /**
   * Get MDS ranks which are in but not up.
   */
  void get_down_mds_set(std::set<mds_rank_t> *s)
  {
    assert(s != NULL);
    s->insert(failed.begin(), failed.end());
    s->insert(damaged.begin(), damaged.end());
  }

  int get_failed() {
    if (!failed.empty()) return *failed.begin();
    return -1;
  }
  void get_stopped_mds_set(std::set<mds_rank_t>& s) {
    s = stopped;
  }
  void get_recovery_mds_set(std::set<mds_rank_t>& s) {
    s = failed;
    for (std::map<mds_gid_t, mds_info_t>::const_iterator p = mds_info.begin();
	 p != mds_info.end();
	 ++p)
      if (p->second.state >= STATE_REPLAY && p->second.state <= STATE_STOPPING)
	s.insert(p->second.rank);
  }
  void get_clientreplay_or_active_or_stopping_mds_set(std::set<mds_rank_t>& s) {
    for (std::map<mds_gid_t, mds_info_t>::const_iterator p = mds_info.begin();
	 p != mds_info.end();
	 ++p)
      if (p->second.state >= STATE_CLIENTREPLAY && p->second.state <= STATE_STOPPING)
	s.insert(p->second.rank);
  }
  void get_mds_set(std::set<mds_rank_t>& s, DaemonState state) const {
    for (std::map<mds_gid_t, mds_info_t>::const_iterator p = mds_info.begin();
	 p != mds_info.end();
	 ++p)
      if (p->second.state == state)
	s.insert(p->second.rank);
  } 

  void get_health(list<pair<health_status_t,std::string> >& summary,
		  list<pair<health_status_t,std::string> > *detail) const;

  typedef enum
  {
    AVAILABLE = 0,
    TRANSIENT_UNAVAILABLE = 1,
    STUCK_UNAVAILABLE = 2

  } availability_t;

  /**
   * Return indication of whether cluster is available.  This is a
   * heuristic for clients to see if they should bother waiting to talk to
   * MDSs, or whether they should error out at startup/mount.
   *
   * A TRANSIENT_UNAVAILABLE result indicates that the cluster is in a
   * transition state like replaying, or is potentially about the fail over.
   * Clients should wait for an updated map before making a final decision
   * about whether the filesystem is mountable.
   *
   * A STUCK_UNAVAILABLE result indicates that we can't see a way that
   * the cluster is about to recover on its own, so it'll probably require
   * administrator intervention: clients should probaly not bother trying
   * to mount.
   */
  availability_t is_cluster_available() const;

  // mds states
  bool is_down(mds_rank_t m) const { return up.count(m) == 0; }
  bool is_up(mds_rank_t m) const { return up.count(m); }
  bool is_in(mds_rank_t m) const { return up.count(m) || failed.count(m); }
  bool is_out(mds_rank_t m) const { return !is_in(m); }

  bool is_failed(mds_rank_t m) const   { return failed.count(m); }
  bool is_stopped(mds_rank_t m) const    { return stopped.count(m); }

  bool is_dne(mds_rank_t m) const      { return in.count(m) == 0; }
  bool is_dne_gid(mds_gid_t gid) const     { return mds_info.count(gid) == 0; }

  /**
   * Get MDS rank state if the rank is up, else STATE_NULL
   */
  DaemonState get_state(mds_rank_t m) const {
    std::map<mds_rank_t, mds_gid_t>::const_iterator u = up.find(m);
    if (u == up.end())
      return STATE_NULL;
    return get_state_gid(u->second);
  }

  /**
   * Get MDS daemon status by GID
   */
  DaemonState get_state_gid(mds_gid_t gid) const {
    std::map<mds_gid_t,mds_info_t>::const_iterator i = mds_info.find(gid);
    if (i == mds_info.end())
      return STATE_NULL;
    return i->second.state;
  }

  const mds_info_t& get_info(const mds_rank_t m) const {
    return mds_info.at(up.at(m));
  }
  const mds_info_t& get_info_gid(const mds_gid_t gid) const {
    return mds_info.at(gid);
  }

  bool is_boot(mds_rank_t m) const { return get_state(m) == STATE_BOOT; }
  bool is_creating(mds_rank_t m) const { return get_state(m) == STATE_CREATING; }
  bool is_starting(mds_rank_t m) const { return get_state(m) == STATE_STARTING; }
  bool is_replay(mds_rank_t m) const   { return get_state(m) == STATE_REPLAY; }
  bool is_resolve(mds_rank_t m) const  { return get_state(m) == STATE_RESOLVE; }
  bool is_reconnect(mds_rank_t m) const { return get_state(m) == STATE_RECONNECT; }
  bool is_rejoin(mds_rank_t m) const   { return get_state(m) == STATE_REJOIN; }
  bool is_clientreplay(mds_rank_t m) const { return get_state(m) == STATE_CLIENTREPLAY; }
  bool is_active(mds_rank_t m) const  { return get_state(m) == STATE_ACTIVE; }
  bool is_stopping(mds_rank_t m) const { return get_state(m) == STATE_STOPPING; }
  bool is_active_or_stopping(mds_rank_t m) const {
    return is_active(m) || is_stopping(m);
  }
  bool is_clientreplay_or_active_or_stopping(mds_rank_t m) const {
    return is_clientreplay(m) || is_active(m) || is_stopping(m);
  }

  bool is_followable(mds_rank_t m) const {
    return (is_resolve(m) ||
	    is_replay(m) ||
	    is_rejoin(m) ||
	    is_clientreplay(m) ||
	    is_active(m) ||
	    is_stopping(m));
  }

  bool is_laggy_gid(mds_gid_t gid) const {
    if (!mds_info.count(gid))
      return false;
    std::map<mds_gid_t,mds_info_t>::const_iterator p = mds_info.find(gid);
    return p->second.laggy();
  }

  // degraded = some recovery in process.  fixes active membership and
  // recovery_set.
  bool is_degraded() const {
    if (!failed.empty() || !damaged.empty())
      return true;
    for (std::map<mds_gid_t,mds_info_t>::const_iterator p = mds_info.begin();
	 p != mds_info.end();
	 ++p)
      if (p->second.state >= STATE_REPLAY && p->second.state <= STATE_CLIENTREPLAY)
	return true;
    return false;
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

  /**
   * Get whether a rank is 'up', i.e. has
   * an MDS daemon's entity_inst_t associated
   * with it.
   */
  bool have_inst(mds_rank_t m) {
    return up.count(m);
  }

  /**
   * Get the MDS daemon entity_inst_t for a rank
   * known to be up.
   */
  const entity_inst_t get_inst(mds_rank_t m) {
    assert(up.count(m));
    return mds_info[up[m]].get_inst();
  }
  const entity_addr_t get_addr(mds_rank_t m) {
    assert(up.count(m));
    return mds_info[up[m]].addr;
  }

  /**
   * Get the MDS daemon entity_inst_t for a rank,
   * if it is up.
   * 
   * @return true if the rank was up and the inst
   *         was populated, else false.
   */
  bool get_inst(mds_rank_t m, entity_inst_t& inst) {
    if (up.count(m)) {
      inst = get_inst(m);
      return true;
    } 
    return false;
  }
  
  mds_rank_t get_rank_gid(mds_gid_t gid) const {
    if (mds_info.count(gid)) {
      return mds_info.at(gid).rank;
    } else {
      return MDS_RANK_NONE;
    }
  }

  int get_inc_gid(mds_gid_t gid) {
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


  void print(ostream& out) const;
  void print_summary(Formatter *f, ostream *out) const;

  void dump(Formatter *f) const;
  static void generate_test_instances(list<MDSMap*>& ls);

  static bool state_transition_valid(DaemonState prev, DaemonState next);
};
WRITE_CLASS_ENCODER_FEATURES(MDSMap::mds_info_t)
WRITE_CLASS_ENCODER_FEATURES(MDSMap)

inline ostream& operator<<(ostream &out, const MDSMap &m) {
  m.print_summary(NULL, &out);
  return out;
}

#endif
