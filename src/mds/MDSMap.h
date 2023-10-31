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

#include <algorithm>
#include <map>
#include <set>
#include <string>
#include <string_view>

#include <errno.h>

#include "include/types.h"
#include "include/ceph_features.h"
#include "include/health.h"
#include "include/CompatSet.h"
#include "include/common_fwd.h"

#include "common/Clock.h"
#include "common/Formatter.h"
#include "common/ceph_releases.h"
#include "common/config.h"

#include "mds/mdstypes.h"
#include "mds/cephfs_features.h"

static inline const auto MDS_FEATURE_INCOMPAT_BASE = CompatSet::Feature(1, "base v0.20");
static inline const auto MDS_FEATURE_INCOMPAT_CLIENTRANGES = CompatSet::Feature(2, "client writeable ranges");
static inline const auto MDS_FEATURE_INCOMPAT_FILELAYOUT = CompatSet::Feature(3, "default file layouts on dirs");
static inline const auto MDS_FEATURE_INCOMPAT_DIRINODE = CompatSet::Feature(4, "dir inode in separate object");
static inline const auto MDS_FEATURE_INCOMPAT_ENCODING = CompatSet::Feature(5, "mds uses versioned encoding");
static inline const auto MDS_FEATURE_INCOMPAT_OMAPDIRFRAG = CompatSet::Feature(6, "dirfrag is stored in omap");
static inline const auto MDS_FEATURE_INCOMPAT_INLINE = CompatSet::Feature(7, "mds uses inline data");
static inline const auto MDS_FEATURE_INCOMPAT_NOANCHOR = CompatSet::Feature(8, "no anchor table");
static inline const auto MDS_FEATURE_INCOMPAT_FILE_LAYOUT_V2 = CompatSet::Feature(9, "file layout v2");
static inline const auto MDS_FEATURE_INCOMPAT_SNAPREALM_V2 = CompatSet::Feature(10, "snaprealm v2");
static inline const auto MDS_FEATURE_INCOMPAT_QUIESCE_SUBVOLUMES = CompatSet::Feature(12, "quiesce subvolumes");

#define MDS_FS_NAME_DEFAULT "cephfs"

/*
 * Maximum size of xattrs the MDS can handle per inode by default.  This
 * includes the attribute name and 4+4 bytes for the key/value sizes.
 */
#define MDS_MAX_XATTR_SIZE (1<<16) /* 64K */

class health_check_map_t;

class MDSMap {
public:
  /* These states are the union of the set of possible states of an MDS daemon,
   * and the set of possible states of an MDS rank. See
   * doc/cephfs/mds-states.rst for state descriptions and a visual state diagram, and
   * doc/cephfs/mds-state-diagram.dot to update the diagram.
   */
  typedef enum {
    // States of an MDS daemon not currently holding a rank
    // ====================================================
    STATE_NULL     =   CEPH_MDS_STATE_NULL,                                  // null value for fns returning this type.
    STATE_BOOT     =   CEPH_MDS_STATE_BOOT,                // up, boot announcement.  destiny unknown.
    STATE_STANDBY  =   CEPH_MDS_STATE_STANDBY,             // up, idle.  waiting for assignment by monitor.

    // States of an MDS rank, and of any MDS daemon holding that rank
    // ==============================================================
    STATE_STANDBY_REPLAY = CEPH_MDS_STATE_STANDBY_REPLAY,  // up, replaying active node, ready to take over and not serving clients. Note: Up to two MDS hold the rank being replayed.
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

  typedef enum
  {
    AVAILABLE = 0,
    TRANSIENT_UNAVAILABLE = 1,
    STUCK_UNAVAILABLE = 2

  } availability_t;

  struct mds_info_t {
    enum mds_flags : uint64_t {
      FROZEN = 1 << 0,
    };

    mds_info_t() = default;

    bool laggy() const { return !(laggy_since == utime_t()); }
    void clear_laggy() { laggy_since = utime_t(); }

    bool is_degraded() const {
      return STATE_REPLAY <= state && state <= STATE_CLIENTREPLAY;
    }

    void freeze() { flags |= mds_flags::FROZEN; }
    void unfreeze() { flags &= ~mds_flags::FROZEN; }
    bool is_frozen() const { return flags&mds_flags::FROZEN; }

    const entity_addrvec_t& get_addrs() const {
      return addrs;
    }

    void encode(ceph::buffer::list& bl, uint64_t features) const {
      if ((features & CEPH_FEATURE_MDSENC) == 0 ) encode_unversioned(bl);
      else encode_versioned(bl, features);
    }
    void decode(ceph::buffer::list::const_iterator& p);
    void dump(ceph::Formatter *f) const;
    void dump(std::ostream&) const;

    // The long form name for use in cluster log messages`
    std::string human_name() const;

    static void generate_test_instances(std::list<mds_info_t*>& ls);

    mds_gid_t global_id = MDS_GID_NONE;
    std::string name;
    mds_rank_t rank = MDS_RANK_NONE;
    int32_t inc = 0;
    MDSMap::DaemonState state = STATE_STANDBY;
    version_t state_seq = 0;
    entity_addrvec_t addrs;
    utime_t laggy_since;
    std::set<mds_rank_t> export_targets;
    fs_cluster_id_t join_fscid = FS_CLUSTER_ID_NONE;
    uint64_t mds_features = 0;
    uint64_t flags = 0;
    CompatSet compat;
  private:
    void encode_versioned(ceph::buffer::list& bl, uint64_t features) const;
    void encode_unversioned(ceph::buffer::list& bl) const;
  };

  friend class MDSMonitor;
  friend class Filesystem;
  friend class FSMap;

  static CompatSet get_compat_set_all();
  static CompatSet get_compat_set_default();
  static CompatSet get_compat_set_base(); // pre v0.20
  static CompatSet get_compat_set_v16_2_4(); // pre-v16.2.5 CompatSet in MDS beacon

  static MDSMap create_null_mdsmap() {
    MDSMap null_map;
    /* Use the largest epoch so it's always bigger than whatever the MDS has. */
    null_map.epoch = std::numeric_limits<decltype(epoch)>::max();
    return null_map;
  }

  bool get_inline_data_enabled() const { return inline_data_enabled; }
  void set_inline_data_enabled(bool enabled) { inline_data_enabled = enabled; }

  utime_t get_session_timeout() const {
    return utime_t(session_timeout,0);
  }
  void set_session_timeout(uint32_t t) {
    session_timeout = t;
  }

  utime_t get_session_autoclose() const {
    return utime_t(session_autoclose, 0);
  }
  void set_session_autoclose(uint32_t t) {
    session_autoclose = t;
  }

  uint64_t get_max_filesize() const { return max_file_size; }
  void set_max_filesize(uint64_t m) { max_file_size = m; }

  uint64_t get_max_xattr_size() const { return max_xattr_size; }
  void set_max_xattr_size(uint64_t m) { max_xattr_size = m; }

  void set_min_compat_client(ceph_release_t version);

  void add_required_client_feature(size_t bit) {
    required_client_features.insert(bit);
  }
  void remove_required_client_feature(size_t bit) {
    required_client_features.erase(bit);
  }
  const auto& get_required_client_features() const {
    return required_client_features;
  }
  
  int get_flags() const { return flags; }
  bool test_flag(int f) const { return flags & f; }
  void set_flag(int f) { flags |= f; }
  void clear_flag(int f) { flags &= ~f; }

  std::string_view get_fs_name() const {return fs_name;}
  void set_fs_name(std::string new_fs_name) { fs_name = std::move(new_fs_name); }

  void set_snaps_allowed() {
    set_flag(CEPH_MDSMAP_ALLOW_SNAPS);
    ever_allowed_features |= CEPH_MDSMAP_ALLOW_SNAPS;
    explicitly_allowed_features |= CEPH_MDSMAP_ALLOW_SNAPS;
  }
  void clear_snaps_allowed() { clear_flag(CEPH_MDSMAP_ALLOW_SNAPS); }
  bool allows_snaps() const { return test_flag(CEPH_MDSMAP_ALLOW_SNAPS); }
  bool was_snaps_ever_allowed() const { return ever_allowed_features & CEPH_MDSMAP_ALLOW_SNAPS; }

  void set_standby_replay_allowed() {
    set_flag(CEPH_MDSMAP_ALLOW_STANDBY_REPLAY);
    ever_allowed_features |= CEPH_MDSMAP_ALLOW_STANDBY_REPLAY;
    explicitly_allowed_features |= CEPH_MDSMAP_ALLOW_STANDBY_REPLAY;
  }
  void clear_standby_replay_allowed() { clear_flag(CEPH_MDSMAP_ALLOW_STANDBY_REPLAY); }
  bool allows_standby_replay() const { return test_flag(CEPH_MDSMAP_ALLOW_STANDBY_REPLAY); }
  bool was_standby_replay_ever_allowed() const { return ever_allowed_features & CEPH_MDSMAP_ALLOW_STANDBY_REPLAY; }

  void set_multimds_snaps_allowed() {
    set_flag(CEPH_MDSMAP_ALLOW_MULTIMDS_SNAPS);
    ever_allowed_features |= CEPH_MDSMAP_ALLOW_MULTIMDS_SNAPS;
    explicitly_allowed_features |= CEPH_MDSMAP_ALLOW_MULTIMDS_SNAPS;
  }
  void clear_multimds_snaps_allowed() { clear_flag(CEPH_MDSMAP_ALLOW_MULTIMDS_SNAPS); }
  bool allows_multimds_snaps() const { return test_flag(CEPH_MDSMAP_ALLOW_MULTIMDS_SNAPS); }
  bool joinable() const { return !test_flag(CEPH_MDSMAP_NOT_JOINABLE); }

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
  void set_old_max_mds() { old_max_mds = max_mds; }
  mds_rank_t get_old_max_mds() const { return old_max_mds; }

  mds_rank_t get_standby_count_wanted(mds_rank_t standby_daemon_count) const {
    ceph_assert(standby_daemon_count >= 0);
    std::set<mds_rank_t> s;
    get_standby_replay_mds_set(s);
    mds_rank_t standbys_avail = (mds_rank_t)s.size()+standby_daemon_count;
    mds_rank_t wanted = std::max(0, standby_count_wanted);
    return wanted > standbys_avail ? wanted - standbys_avail : 0;
  }
  void set_standby_count_wanted(mds_rank_t n) { standby_count_wanted = n; }
  bool check_health(mds_rank_t standby_daemon_count);

  const std::string get_balancer() const { return balancer; }
  void set_balancer(std::string val) { balancer.assign(val); }

  const std::bitset<MAX_MDS>& get_bal_rank_mask_bitset() const;
  void set_bal_rank_mask(std::string val);
  unsigned get_num_mdss_in_rank_mask_bitset() const { return num_mdss_in_rank_mask_bitset; }
  void update_num_mdss_in_rank_mask_bitset();
  int hex2bin(std::string hex_string, std::string &bin_string, unsigned int max_bits, std::ostream& ss) const;

  typedef enum
  {
    BAL_RANK_MASK_TYPE_ANY = 0,
    BAL_RANK_MASK_TYPE_ALL = 1,
    BAL_RANK_MASK_TYPE_NONE = 2,
  } bal_rank_mask_type_t;

  const bool check_special_bal_rank_mask(std::string val, bal_rank_mask_type_t type) const;

  mds_rank_t get_tableserver() const { return tableserver; }
  mds_rank_t get_root() const { return root; }

  const std::vector<int64_t> &get_data_pools() const { return data_pools; }
  int64_t get_first_data_pool() const { return *data_pools.begin(); }
  int64_t get_metadata_pool() const { return metadata_pool; }
  bool is_data_pool(int64_t poolid) const {
    auto p = std::find(data_pools.begin(), data_pools.end(), poolid);
    if (p == data_pools.end())
      return false;
    return true;
  }

  bool pool_in_use(int64_t poolid) const {
    return get_enabled() && (is_data_pool(poolid) || metadata_pool == poolid);
  }

  const auto& get_mds_info() const { return mds_info; }
  const auto& get_mds_info_gid(mds_gid_t gid) const {
    return mds_info.at(gid);
  }
  const mds_info_t& get_mds_info(mds_rank_t m) const {
    ceph_assert(up.count(m) && mds_info.count(up.at(m)));
    return mds_info.at(up.at(m));
  }
  mds_gid_t find_mds_gid_by_name(std::string_view s) const;

  // counts
  unsigned get_num_in_mds() const {
    return in.size();
  }
  unsigned get_num_up_mds() const {
    return up.size();
  }
  mds_rank_t get_last_in_mds() const {
    auto p = in.rbegin();
    return p == in.rend() ? MDS_RANK_NONE : *p;
  }
  int get_num_failed_mds() const {
    return failed.size();
  }
  unsigned get_num_standby_replay_mds() const {
    unsigned num = 0;
    for (auto& i : mds_info) {
      if (i.second.state == MDSMap::STATE_STANDBY_REPLAY) {
	++num;
      }
    }
    return num;
  }
  unsigned get_num_mds(int state) const;
  // data pools
  void add_data_pool(int64_t poolid) {
    data_pools.push_back(poolid);
  }
  int remove_data_pool(int64_t poolid) {
    std::vector<int64_t>::iterator p = std::find(data_pools.begin(), data_pools.end(), poolid);
    if (p == data_pools.end())
      return -CEPHFS_ENOENT;
    data_pools.erase(p);
    return 0;
  }

  // sets
  void get_mds_set(std::set<mds_rank_t>& s) const {
    s = in;
  }
  void get_up_mds_set(std::set<mds_rank_t>& s) const;
  void get_active_mds_set(std::set<mds_rank_t>& s) const {
    get_mds_set(s, MDSMap::STATE_ACTIVE);
  }
  void get_standby_replay_mds_set(std::set<mds_rank_t>& s) const {
    get_mds_set(s, MDSMap::STATE_STANDBY_REPLAY);
  }
  void get_failed_mds_set(std::set<mds_rank_t>& s) const {
    s = failed;
  }
  void get_damaged_mds_set(std::set<mds_rank_t>& s) const {
    s = damaged;
  }

  // features
  uint64_t get_up_features();

  /**
   * Get MDS ranks which are in but not up.
   */
  void get_down_mds_set(std::set<mds_rank_t> *s) const
  {
    ceph_assert(s != NULL);
    s->insert(failed.begin(), failed.end());
    s->insert(damaged.begin(), damaged.end());
  }

  int get_failed() const {
    if (!failed.empty()) return *failed.begin();
    return -1;
  }
  void get_stopped_mds_set(std::set<mds_rank_t>& s) const {
    s = stopped;
  }
  void get_recovery_mds_set(std::set<mds_rank_t>& s) const;

  void get_mds_set_lower_bound(std::set<mds_rank_t>& s, DaemonState first) const;
  void get_mds_set(std::set<mds_rank_t>& s, DaemonState state) const;

  void get_health(std::list<std::pair<health_status_t,std::string> >& summary,
		  std::list<std::pair<health_status_t,std::string> > *detail) const;

  void get_health_checks(health_check_map_t *checks) const;

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
   * administrator intervention: clients should probably not bother trying
   * to mount.
   */
  availability_t is_cluster_available() const;

  /**
   * Return whether this MDSMap is suitable for resizing based on the state
   * of the ranks.
   */
  bool is_resizeable() const {
    return !is_degraded() &&
        get_num_mds(CEPH_MDS_STATE_CREATING) == 0 &&
        get_num_mds(CEPH_MDS_STATE_STARTING) == 0 &&
        get_num_mds(CEPH_MDS_STATE_STOPPING) == 0;
  }

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
   * Get MDS daemon status by GID
   */
  auto get_state_gid(mds_gid_t gid) const {
    auto it = mds_info.find(gid);
    if (it == mds_info.end())
      return STATE_NULL;
    return it->second.state;
  }

  /**
   * Get MDS rank state if the rank is up, else STATE_NULL
   */
  auto get_state(mds_rank_t m) const {
    auto it = up.find(m);
    if (it == up.end())
      return STATE_NULL;
    return get_state_gid(it->second);
  }

  auto get_gid(mds_rank_t r) const {
    return up.at(r);
  }
  const auto& get_info(mds_rank_t m) const {
    return mds_info.at(up.at(m));
  }
  const auto& get_info_gid(mds_gid_t gid) const {
    return mds_info.at(gid);
  }

  bool is_boot(mds_rank_t m) const { return get_state(m) == STATE_BOOT; }
  bool is_bootstrapping(mds_rank_t m) const {
    return is_creating(m) || is_starting(m) || is_replay(m);
  }
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

  mds_gid_t get_standby_replay(mds_rank_t r) const;
  bool has_standby_replay(mds_rank_t r) const {
    return get_standby_replay(r) != MDS_GID_NONE;
  }

  bool is_followable(mds_rank_t r) const {
    if (auto it1 = up.find(r); it1 != up.end()) {
      if (auto it2 = mds_info.find(it1->second); it2 != mds_info.end()) {
        auto& info = it2->second;
        if (!info.is_degraded() && !has_standby_replay(r)) {
          return true;
        }
      }
    }
    return false;
  }

  bool is_laggy_gid(mds_gid_t gid) const {
    auto it = mds_info.find(gid);
    return it == mds_info.end() ? false : it->second.laggy();
  }

  // degraded = some recovery in process.  fixes active membership and
  // recovery_set.
  bool is_degraded() const;
  bool is_any_failed() const {
    return !failed.empty();
  }
  bool is_any_damaged() const {
    return !damaged.empty();
  }
  bool is_resolving() const {
    return
      get_num_mds(STATE_RESOLVE) > 0 &&
      get_num_mds(STATE_REPLAY) == 0 &&
      failed.empty() && damaged.empty();
  }
  bool is_rejoining() const {
    // nodes are rejoining cache state
    return 
      get_num_mds(STATE_REJOIN) > 0 &&
      get_num_mds(STATE_REPLAY) == 0 &&
      get_num_mds(STATE_RECONNECT) == 0 &&
      get_num_mds(STATE_RESOLVE) == 0 &&
      failed.empty() && damaged.empty();
  }
  bool is_stopped() const {
    return up.empty();
  }

  /**
   * Get whether a rank is 'up', i.e. has
   * an MDS daemon's entity_inst_t associated
   * with it.
   */
  bool have_inst(mds_rank_t m) const {
    return up.count(m);
  }

  /**
   * Get the MDS daemon entity_inst_t for a rank
   * known to be up.
   */
  entity_addrvec_t get_addrs(mds_rank_t m) const {
    return mds_info.at(up.at(m)).get_addrs();
  }

  mds_rank_t get_rank_gid(mds_gid_t gid) const {
    if (mds_info.count(gid)) {
      return mds_info.at(gid).rank;
    } else {
      return MDS_RANK_NONE;
    }
  }

  /**
   * Get MDS rank incarnation if the rank is up, else -1
   */
  mds_gid_t get_incarnation(mds_rank_t m) const {
    auto it = up.find(m);
    if (it == up.end())
      return MDS_GID_NONE;
    return (mds_gid_t)get_inc_gid(it->second);
  }

  int get_inc_gid(mds_gid_t gid) const {
    auto mds_info_entry = mds_info.find(gid);
    if (mds_info_entry != mds_info.end())
      return mds_info_entry->second.inc;
    return -1;
  }
  void encode(ceph::buffer::list& bl, uint64_t features) const;
  void decode(ceph::buffer::list::const_iterator& p);
  void decode(const ceph::buffer::list& bl) {
    auto p = bl.cbegin();
    decode(p);
  }
  void sanitize(const std::function<bool(int64_t pool)>& pool_exists);

  void print(std::ostream& out) const;
  void print_summary(ceph::Formatter *f, std::ostream *out) const;
  void print_flags(std::ostream& out) const;

  void dump(ceph::Formatter *f) const;
  void dump_flags_state(Formatter *f) const;
  static void generate_test_instances(std::list<MDSMap*>& ls);

  static bool state_transition_valid(DaemonState prev, DaemonState next);

  CompatSet compat;
protected:
  // base map
  epoch_t epoch = 0;
  bool enabled = false;
  std::string fs_name = MDS_FS_NAME_DEFAULT;
  uint32_t flags = CEPH_MDSMAP_DEFAULTS; // flags
  epoch_t last_failure = 0;  // mds epoch of last failure
  epoch_t last_failure_osd_epoch = 0; // osd epoch of last failure; any mds entering replay needs
                                  // at least this osdmap to ensure the blocklist propagates.
  utime_t created;
  utime_t modified;

  mds_rank_t tableserver = 0;   // which MDS has snaptable
  mds_rank_t root = 0;          // which MDS has root directory

  __u32 session_timeout = 60;
  __u32 session_autoclose = 300;
  uint64_t max_file_size = 1ULL<<40; /* 1TB */

  uint64_t max_xattr_size = MDS_MAX_XATTR_SIZE;

  feature_bitset_t required_client_features;

  std::vector<int64_t> data_pools;  // file data pools available to clients (via an ioctl).  first is the default.
  int64_t cas_pool = -1;            // where CAS objects go
  int64_t metadata_pool = -1;       // where fs metadata objects go

  /*
   * in: the set of logical mds #'s that define the cluster.  this is the set
   *     of mds's the metadata may be distributed over.
   * up: map from logical mds #'s to the addrs filling those roles.
   * failed: subset of @in that are failed.
   * stopped: set of nodes that have been initialized, but are not active.
   *
   *    @up + @failed = @in.  @in * @stopped = {}.
   */

  mds_rank_t max_mds = 1; /* The maximum number of active MDSes. Also, the maximum rank. */
  mds_rank_t old_max_mds = 0; /* Value to restore when MDS cluster is marked up */
  mds_rank_t standby_count_wanted = -1;
  std::string balancer;    /* The name/version of the mantle balancer (i.e. the rados obj name) */

  std::string bal_rank_mask = "-1";
  std::bitset<MAX_MDS> bal_rank_mask_bitset;
  uint32_t num_mdss_in_rank_mask_bitset;

  std::set<mds_rank_t> in;              // currently defined cluster

  // which ranks are failed, stopped, damaged (i.e. not held by a daemon)
  std::set<mds_rank_t> failed, stopped, damaged;
  std::map<mds_rank_t, mds_gid_t> up;        // who is in those roles
  std::map<mds_gid_t, mds_info_t> mds_info;

  uint8_t ever_allowed_features = 0; //< bitmap of features the cluster has allowed
  uint8_t explicitly_allowed_features = 0; //< bitmap of features explicitly enabled

  bool inline_data_enabled = false;

  uint64_t cached_up_features = 0;
private:
  inline static const std::map<int, std::string> flag_display = {
    {CEPH_MDSMAP_NOT_JOINABLE, "joinable"}, //inverse for user display
    {CEPH_MDSMAP_ALLOW_SNAPS, "allow_snaps"},
    {CEPH_MDSMAP_ALLOW_MULTIMDS_SNAPS, "allow_multimds_snaps"},
    {CEPH_MDSMAP_ALLOW_STANDBY_REPLAY, "allow_standby_replay"},
    {CEPH_MDSMAP_REFUSE_CLIENT_SESSION, "refuse_client_session"},
    {CEPH_MDSMAP_REFUSE_STANDBY_FOR_ANOTHER_FS, "refuse_standby_for_another_fs"}
  };
};
WRITE_CLASS_ENCODER_FEATURES(MDSMap::mds_info_t)
WRITE_CLASS_ENCODER_FEATURES(MDSMap)

inline std::ostream& operator<<(std::ostream &out, const MDSMap &m) {
  m.print_summary(NULL, &out);
  return out;
}

inline std::ostream& operator<<(std::ostream& o, const MDSMap::mds_info_t& info) {
  info.dump(o);
  return o;
}
#endif
