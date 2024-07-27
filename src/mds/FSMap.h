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


#ifndef CEPH_FSMAP_H
#define CEPH_FSMAP_H

#include <map>
#include <memory>
#include <set>
#include <string>
#include <string_view>
#include <type_traits>

#include <errno.h>

#include "include/types.h"
#include "common/ceph_time.h"
#include "common/Clock.h"
#include "mds/MDSMap.h"

#include "include/CompatSet.h"
#include "include/ceph_features.h"
#include "include/common_fwd.h"
#include "common/Formatter.h"
#include "mds/mdstypes.h"

#if __cplusplus <= 201703L
template<class Key, class T, class Compare, class Alloc, class Pred>
typename std::map<Key, T, Compare, Alloc>::size_type
erase_if(std::map<Key, T, Compare, Alloc>& c, Pred pred) {
  auto old_size = c.size();
  for (auto i = c.begin(), last = c.end(); i != last; ) {
    if (pred(*i)) {
      i = c.erase(i);
    } else {
      ++i;
    }
  }
  return old_size - c.size();
}
#endif

class health_check_map_t;

struct ClusterInfo {
  ClusterInfo() = default;
  ClusterInfo(std::string_view client_name, std::string_view cluster_name,
              std::string_view fs_name)
    : client_name(client_name),
      cluster_name(cluster_name),
      fs_name(fs_name) {
  }

  std::string client_name;
  std::string cluster_name;
  std::string fs_name;

  bool operator==(const ClusterInfo &cluster_info) const {
    return client_name == cluster_info.client_name &&
           cluster_name == cluster_info.cluster_name &&
           fs_name == cluster_info.fs_name;
  }

  void dump(ceph::Formatter *f) const;
  void print(std::ostream& out) const;

  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &iter);
};

inline std::ostream& operator<<(std::ostream& out, const ClusterInfo &cluster_info) {
  out << "{client_name=" << cluster_info.client_name << ", cluster_name="
      << cluster_info.cluster_name << ", fs_name=" << cluster_info.fs_name << "}";
  return out;
}

struct Peer {
  Peer() = default;
  Peer(std::string_view uuid)
    : uuid(uuid) {
  }
  Peer(std::string_view uuid,
       const ClusterInfo &remote)
    : uuid(uuid),
      remote(remote) {
  }

  std::string uuid;
  ClusterInfo remote;

  bool operator==(const Peer &rhs) const {
    return uuid == rhs.uuid;
  }

  bool operator<(const Peer &rhs) const {
    return uuid < rhs.uuid;
  }

  void dump(ceph::Formatter *f) const;
  void print(std::ostream& out) const;

  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &iter);
};

typedef std::set<Peer> Peers;
inline std::ostream& operator<<(std::ostream& out, const Peer &peer) {
  out << "{uuid=" << peer.uuid << ", remote_cluster=" << peer.remote << "}";
  return out;
}

struct MirrorInfo {
  MirrorInfo() = default;

  bool is_mirrored() const {
    return mirrored;
  }
  void enable_mirroring() {
    mirrored = true;
  }
  void disable_mirroring() {
    peers.clear();
    mirrored = false;
  }

  // uuid variant check
  bool has_peer(std::string_view uuid) const {
    return peers.find(Peer(uuid)) != peers.end();
  }
  // client_name/cluster_name/fs_name variant check
  bool has_peer(std::string_view client_name,
                std::string_view cluster_name,
                std::string_view fs_name) const {
    ClusterInfo cluster_info(client_name, cluster_name, fs_name);
    for (auto &peer : peers) {
      if (peer.remote == cluster_info) {
        return true;
      }
    }
    return false;
  }
  bool has_peers() const {
    return !peers.empty();
  }

  void peer_add(std::string_view uuid,
                std::string_view client_name,
                std::string_view cluster_name,
                std::string_view fs_name) {
    peers.emplace(Peer(uuid, ClusterInfo(client_name, cluster_name, fs_name)));
  }
  void peer_remove(std::string_view uuid) {
    peers.erase(uuid);
  }

  bool mirrored = false;
  Peers peers;

  void dump(ceph::Formatter *f) const;
  void print(std::ostream& out) const;

  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &iter);
};

inline std::ostream& operator<<(std::ostream& out, const MirrorInfo &mirror_info) {
  out << "{peers=" << mirror_info.peers << "}";
  return out;
}

WRITE_CLASS_ENCODER(ClusterInfo)
WRITE_CLASS_ENCODER(Peer)
WRITE_CLASS_ENCODER(MirrorInfo)

/**
 * The MDSMap and any additional fields describing a particular
 * filesystem (a unique fs_cluster_id_t).
 */
class Filesystem
{
public:
  Filesystem() = default;

  void encode(ceph::buffer::list& bl, uint64_t features) const;
  void decode(ceph::buffer::list::const_iterator& p);

  void dump(ceph::Formatter *f) const;
  void print(std::ostream& out) const;

  bool is_upgradeable() const {
    bool asr = mds_map.allows_standby_replay();
    auto in_mds = mds_map.get_num_in_mds();
    auto up_mds = mds_map.get_num_up_mds();
    return
              /* fs was "down" */
              (in_mds == 0)
              /* max_mds was set to 1; asr must be disabled */
           || (!asr && in_mds == 1)
              /* max_mds any value and all MDS were failed; asr must be disabled */
           || (!asr && up_mds == 0);
  }

  /**
   * Return true if a daemon is already assigned as
   * STANDBY_REPLAY for the gid `who`
   */
  bool has_standby_replay(mds_gid_t who) const
  {
    return get_standby_replay(who) != MDS_GID_NONE;
  }
  mds_gid_t get_standby_replay(mds_gid_t who) const;
  bool is_standby_replay(mds_gid_t who) const
  {
    auto p = mds_map.mds_info.find(who);
    if (p != mds_map.mds_info.end() &&
	p->second.state == MDSMap::STATE_STANDBY_REPLAY) {
      return true;
    }
    return false;
  }

  const auto& get_mds_map() const
  {
    return mds_map;
  }
  auto& get_mds_map()
  {
    return mds_map;
  }

  const auto& get_mirror_info() const
  {
    return mirror_info;
  }
  auto& get_mirror_info()
  {
    return mirror_info;
  }

  auto get_fscid() const
  {
    return fscid;
  }

private:
  void set_fscid(fs_cluster_id_t new_fscid) {
    fscid = new_fscid;
  }

  friend class FSMap;

  fs_cluster_id_t fscid = FS_CLUSTER_ID_NONE;
  MDSMap mds_map;
  MirrorInfo mirror_info;
};
WRITE_CLASS_ENCODER_FEATURES(Filesystem)

class FSMap {
public:
  using real_clock = ceph::real_clock;
  using mds_info_t = MDSMap::mds_info_t;
  using fsmap = typename std::map<fs_cluster_id_t, Filesystem>;
  using const_iterator = typename fsmap::const_iterator;
  using iterator = typename fsmap::iterator;

  static const version_t STRUCT_VERSION = 8;
  static const version_t STRUCT_VERSION_TRIM_TO = 7;

  FSMap() : default_compat(MDSMap::get_compat_set_default()) {}

  FSMap(const FSMap &rhs)
    :
      epoch(rhs.epoch),
      btime(rhs.btime),
      next_filesystem_id(rhs.next_filesystem_id),
      legacy_client_fscid(rhs.legacy_client_fscid),
      default_compat(rhs.default_compat),
      enable_multiple(rhs.enable_multiple),
      ever_enabled_multiple(rhs.ever_enabled_multiple),
      mds_roles(rhs.mds_roles),
      standby_daemons(rhs.standby_daemons),
      standby_epochs(rhs.standby_epochs),
      struct_version(rhs.struct_version)
  {
    filesystems.clear();
    filesystems = rhs.filesystems;
  }

  FSMap &operator=(const FSMap &rhs);

  const_iterator begin() const {
    return filesystems.begin();
  }
  const_iterator end() const {
    return filesystems.end();
  }

  const CompatSet& get_default_compat() const {return default_compat;}
  CompatSet& get_default_compat() {return default_compat;}

  void filter(const std::vector<std::string>& allowed)
  {
    if (allowed.empty()) {
      return;
    }

    erase_if(filesystems, [&](const auto& f) {
      return std::find(allowed.begin(), allowed.end(), f.second.mds_map.get_fs_name()) == allowed.end();
    });

    erase_if(mds_roles, [&](const auto& r) {
      return std::find(allowed.begin(), allowed.end(), fs_name_from_gid(r.first)) == allowed.end();
    });
  }

  void set_enable_multiple(const bool v)
  {
    enable_multiple = v;
    if (true == v) {
      ever_enabled_multiple = true;
    }
  }

  bool get_enable_multiple() const
  {
    return enable_multiple;
  }

  void set_legacy_client_fscid(fs_cluster_id_t fscid)
  {
    ceph_assert(fscid == FS_CLUSTER_ID_NONE || filesystems.count(fscid));
    legacy_client_fscid = fscid;
  }

  fs_cluster_id_t get_legacy_client_fscid() const
  {
    return legacy_client_fscid;
  }

  size_t get_num_standby() const {
    return standby_daemons.size();
  }

  bool is_any_degraded() const;

  /**
   * Get state of all daemons (for all filesystems, including all standbys)
   */
  std::map<mds_gid_t, mds_info_t> get_mds_info() const;

  const mds_info_t* get_available_standby(const Filesystem& fs) const;

  /**
   * Resolve daemon name to GID
   */
  mds_gid_t find_mds_gid_by_name(std::string_view s) const;

  /**
   * Resolve daemon name to status
   */
  const mds_info_t* find_by_name(std::string_view name) const;

  /**
   * Does a daemon exist with this GID?
   */
  bool gid_exists(mds_gid_t gid,
		  const std::vector<std::string>& in = {}) const
  {
    try {
      std::string_view m = fs_name_from_gid(gid);
      return in.empty() || std::find(in.begin(), in.end(), m) != in.end();
    } catch (const std::out_of_range&) {
      return false;
    }
  }

  /**
   * Does a daemon with this GID exist, *and* have an MDS rank assigned?
   */
  bool gid_has_rank(mds_gid_t gid) const
  {
    return gid_exists(gid) && mds_roles.at(gid) != FS_CLUSTER_ID_NONE;
  }

  /**
   * Which filesystem owns this GID?
   */
  fs_cluster_id_t fscid_from_gid(mds_gid_t gid) const {
    if (!gid_exists(gid)) {
      return FS_CLUSTER_ID_NONE;
    }
    return mds_roles.at(gid);
  }

  /**
   * Insert a new MDS daemon, as a standby
   */
  void insert(const MDSMap::mds_info_t &new_info);

  /**
   * Assign an MDS cluster standby replay rank to a standby daemon
   */
  void assign_standby_replay(
      const mds_gid_t standby_gid,
      const fs_cluster_id_t leader_ns,
      const mds_rank_t leader_rank);

  /**
   * Assign an MDS cluster rank to a standby daemon
   */
  void promote(
      mds_gid_t standby_gid,
      fs_cluster_id_t fscid,
      mds_rank_t assigned_rank);

  /**
   * A daemon reports that it is STATE_STOPPED: remove it,
   * and the rank it held.
   *
   * @returns a list of any additional GIDs that were removed from the map
   * as a side effect (like standby replays)
   */
  std::vector<mds_gid_t> stop(mds_gid_t who);

  /**
   * The rank held by 'who', if any, is to be relinquished, and
   * the state for the daemon GID is to be forgotten.
   */
  void erase(mds_gid_t who, epoch_t blocklist_epoch);

  /**
   * Update to indicate that the rank held by 'who' is damaged
   */
  void damaged(mds_gid_t who, epoch_t blocklist_epoch);

  /**
   * Update to indicate that the rank `rank` is to be removed
   * from the damaged list of the filesystem `fscid`
   */
  bool undamaged(const fs_cluster_id_t fscid, const mds_rank_t rank);

  /**
   * Initialize a Filesystem and assign a fscid.  Update legacy_client_fscid
   * to point to the new filesystem if it's the only one.
   *
   * Caller must already have validated all arguments vs. the existing
   * FSMap and OSDMap contents.
   */
  Filesystem create_filesystem(
      std::string_view name, int64_t metadata_pool, int64_t data_pool,
      uint64_t features, bool recover);

  /**
   * Commit the created filesystem to the FSMap.
   *
   */
  const Filesystem& commit_filesystem(fs_cluster_id_t fscid, Filesystem fs);

  /**
   * Remove the filesystem (it must exist).  Caller should already
   * have failed out any MDSs that were assigned to the filesystem.
   */
  void erase_filesystem(fs_cluster_id_t fscid);

  /**
   * Reset all the state information (not configuration information)
   * in a particular filesystem.  Caller must have verified that
   * the filesystem already exists.
   */
  void reset_filesystem(fs_cluster_id_t fscid);

  /**
   * Mutator helper for Filesystem objects: expose a non-const
   * Filesystem pointer to `fn` and update epochs appropriately.
   */
  template<typename T>
  void modify_filesystem(fs_cluster_id_t fscid, T&& fn)
  {
    auto& fs = filesystems.at(fscid);
    bool did_update = true;

    if constexpr (std::is_convertible_v<std::invoke_result_t<T, Filesystem&>, bool>) {
      did_update = fn(fs);
    } else {
      fn(fs);
    }
    
    if (did_update) {
      fs.mds_map.epoch = epoch;
      fs.mds_map.modified = ceph_clock_now();
    }
  }

  /* This is method is written for the option of "ceph fs swap" commmand
   * that intiates swap of FSCIDs.
   */
  void swap_fscids(fs_cluster_id_t fscid1, fs_cluster_id_t fscid2);

  /**
   * Apply a mutation to the mds_info_t structure for a particular
   * daemon (identified by GID), and make appropriate updates to epochs.
   */
  template<typename T>
  void modify_daemon(mds_gid_t who, T&& fn)
  {
    const auto& fscid = mds_roles.at(who);
    if (fscid == FS_CLUSTER_ID_NONE) {
      auto& info = standby_daemons.at(who);
      fn(info);
      ceph_assert(info.state == MDSMap::STATE_STANDBY);
      standby_epochs[who] = epoch;
    } else {
      auto& fs = filesystems.at(fscid);
      auto& info = fs.mds_map.mds_info.at(who);
      fn(info);
      fs.mds_map.epoch = epoch;
      fs.mds_map.modified = ceph_clock_now();
    }
  }

  /**
   * Given that gid exists in a filesystem or as a standby, return
   * a reference to its info.
   */
  const mds_info_t& get_info_gid(mds_gid_t gid) const
  {
    auto fscid = mds_roles.at(gid);
    if (fscid == FS_CLUSTER_ID_NONE) {
      return standby_daemons.at(gid);
    } else {
      return filesystems.at(fscid).mds_map.mds_info.at(gid);
    }
  }

  std::string_view fs_name_from_gid(mds_gid_t gid) const
  {
    auto fscid = mds_roles.at(gid);
    if (fscid == FS_CLUSTER_ID_NONE or !filesystem_exists(fscid)) {
      return std::string_view();
    } else {
      return filesystems.at(fscid).mds_map.get_fs_name();
    }
  }

  bool is_standby_replay(mds_gid_t who) const
  {
    return filesystems.at(mds_roles.at(who)).is_standby_replay(who);
  }

  mds_gid_t get_standby_replay(mds_gid_t who) const
  {
    return filesystems.at(mds_roles.at(who)).get_standby_replay(who);
  }

  const Filesystem* get_legacy_filesystem() const
  {
    if (legacy_client_fscid == FS_CLUSTER_ID_NONE) {
      return nullptr;
    } else {
      return &filesystems.at(legacy_client_fscid);
    }
  }

  /**
   * A daemon has informed us of its offload targets
   */
  void update_export_targets(mds_gid_t who, const std::set<mds_rank_t> &targets)
  {
    auto fscid = mds_roles.at(who);
    modify_filesystem(fscid, [who, &targets](auto&& fs) {
      fs.mds_map.mds_info.at(who).export_targets = targets;
    });
  }

  epoch_t get_epoch() const { return epoch; }
  void inc_epoch() { epoch++; }

  void set_btime() {
    btime = real_clock::now();
  }
  auto get_btime() const {
    return btime;
  }

  version_t get_struct_version() const { return struct_version; }
  bool is_struct_old() const {
    return struct_version < STRUCT_VERSION_TRIM_TO;
  }

  size_t filesystem_count() const {
    return filesystems.size();
  }
  bool filesystem_exists(fs_cluster_id_t fscid) const {
    return filesystems.count(fscid) > 0;
  }
  const Filesystem& get_filesystem(fs_cluster_id_t fscid) const {
    return filesystems.at(fscid);
  }
  const Filesystem& get_filesystem(void) const {
    return filesystems.at(filesystems.begin()->second.fscid);
  }
  Filesystem const* get_filesystem(std::string_view name) const;
  const Filesystem& get_filesystem(mds_gid_t gid) const {
    return filesystems.at(mds_roles.at(gid));
  }

  int parse_filesystem(std::string_view ns_str, Filesystem const** result) const;

  int parse_role(
      std::string_view role_str,
      mds_role_t *role,
      std::ostream &ss,
      const std::vector<std::string> &filter) const;

  int parse_role(
      std::string_view role_str,
      mds_role_t *role,
      std::ostream &ss) const;

  /**
   * Return true if this pool is in use by any of the filesystems
   */
  bool pool_in_use(int64_t poolid) const;

  const mds_info_t* find_replacement_for(mds_role_t role) const;

  void get_health(std::list<std::pair<health_status_t,std::string> >& summary,
		  std::list<std::pair<health_status_t,std::string> > *detail) const;

  void get_health_checks(health_check_map_t *checks) const;

  bool check_health(void);

  const auto& get_mds_roles() const {
    return mds_roles;
  }

  const auto& get_standby_daemons() const {
    return standby_daemons;
  }

  const auto& get_standby_epochs() const {
    return standby_epochs;
  }

  /**
   * Assert that the FSMap, Filesystem, MDSMap, mds_info_t relations are
   * all self-consistent.
   */
  void sanity(bool pending=false) const;

  void encode(ceph::buffer::list& bl, uint64_t features) const;
  void decode(ceph::buffer::list::const_iterator& p);
  void decode(ceph::buffer::list& bl) {
    auto p = bl.cbegin();
    decode(p);
  }
  void sanitize(const std::function<bool(int64_t pool)>& pool_exists);

  void print(std::ostream& out) const;
  void print_summary(ceph::Formatter *f, std::ostream *out) const;
  void print_daemon_summary(std::ostream& out) const;
  void print_fs_summary(std::ostream& out) const;

  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<FSMap*>& ls);

protected:
  iterator begin() {
    return filesystems.begin();
  }
  iterator end() {
    return filesystems.end();
  }

  epoch_t epoch = 0;
  ceph::real_time btime = real_clock::zero();

  uint64_t next_filesystem_id = FS_CLUSTER_ID_ANONYMOUS + 1;
  fs_cluster_id_t legacy_client_fscid = FS_CLUSTER_ID_NONE;
  CompatSet default_compat;
  bool enable_multiple = true;
  bool ever_enabled_multiple = true; // < the cluster had multiple FS enabled once

  fsmap filesystems;

  // Remember which Filesystem an MDS daemon's info is stored in
  // (or in standby_daemons for FS_CLUSTER_ID_NONE)
  std::map<mds_gid_t, fs_cluster_id_t> mds_roles;

  // For MDS daemons not yet assigned to a Filesystem
  std::map<mds_gid_t, mds_info_t> standby_daemons;
  std::map<mds_gid_t, epoch_t> standby_epochs;

private:
  epoch_t struct_version = 0;
};
WRITE_CLASS_ENCODER_FEATURES(FSMap)

inline std::ostream& operator<<(std::ostream& out, const FSMap& m) {
  m.print_summary(NULL, &out);
  return out;
}

#endif
