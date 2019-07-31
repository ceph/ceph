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

#include <errno.h>

#include "include/types.h"
#include "common/Clock.h"
#include "mds/MDSMap.h"

#include "include/CompatSet.h"
#include "include/ceph_features.h"
#include "common/Formatter.h"
#include "mds/mdstypes.h"

class CephContext;
class health_check_map_t;

#define MDS_FS_NAME_DEFAULT "cephfs"

/**
 * The MDSMap and any additional fields describing a particular
 * filesystem (a unique fs_cluster_id_t).
 */
class Filesystem
{
public:
  using ref = std::shared_ptr<Filesystem>;
  using const_ref = std::shared_ptr<Filesystem const>;

  template<typename... Args>
  static ref create(Args&&... args)
  {
    return std::make_shared<Filesystem>(std::forward<Args>(args)...);
  }

  void encode(bufferlist& bl, uint64_t features) const;
  void decode(bufferlist::const_iterator& p);

  void dump(Formatter *f) const;
  void print(std::ostream& out) const;

  /**
   * Return true if a daemon is already assigned as
   * STANDBY_REPLAY for the gid `who`
   */
  bool has_standby_replay(mds_gid_t who) const
  {
    return get_standby_replay(who) != MDS_GID_NONE;
  }
  mds_gid_t get_standby_replay(mds_gid_t who) const
  {
    for (const auto &i : mds_map.mds_info) {
      const auto &info = i.second;
      if (info.state == MDSMap::STATE_STANDBY_REPLAY
          && info.rank == mds_map.mds_info.at(who).rank) {
        return info.global_id;
      }
    }
    return MDS_GID_NONE;
  }
  bool is_standby_replay(mds_gid_t who) const
  {
    auto p = mds_map.mds_info.find(who);
    if (p != mds_map.mds_info.end() &&
	p->second.state == MDSMap::STATE_STANDBY_REPLAY) {
      return true;
    }
    return false;
  }

  fs_cluster_id_t fscid = FS_CLUSTER_ID_NONE;
  MDSMap mds_map;
};
WRITE_CLASS_ENCODER_FEATURES(Filesystem)

class FSMap {
protected:
  epoch_t epoch = 0;
  uint64_t next_filesystem_id = FS_CLUSTER_ID_ANONYMOUS + 1;
  fs_cluster_id_t legacy_client_fscid = FS_CLUSTER_ID_NONE;
  CompatSet compat;
  bool enable_multiple = false;
  bool ever_enabled_multiple = false; // < the cluster had multiple MDSes enabled once

  std::map<fs_cluster_id_t, Filesystem::ref> filesystems;

  // Remember which Filesystem an MDS daemon's info is stored in
  // (or in standby_daemons for FS_CLUSTER_ID_NONE)
  std::map<mds_gid_t, fs_cluster_id_t> mds_roles;

  // For MDS daemons not yet assigned to a Filesystem
  std::map<mds_gid_t, MDSMap::mds_info_t> standby_daemons;
  std::map<mds_gid_t, epoch_t> standby_epochs;

public:

  friend class MDSMonitor;
  friend class PaxosFSMap;

  FSMap() : compat(MDSMap::get_compat_set_default()) {}

  FSMap(const FSMap &rhs)
    :
      epoch(rhs.epoch),
      next_filesystem_id(rhs.next_filesystem_id),
      legacy_client_fscid(rhs.legacy_client_fscid),
      compat(rhs.compat),
      enable_multiple(rhs.enable_multiple),
      ever_enabled_multiple(rhs.ever_enabled_multiple),
      mds_roles(rhs.mds_roles),
      standby_daemons(rhs.standby_daemons),
      standby_epochs(rhs.standby_epochs)
  {
    filesystems.clear();
    for (const auto &i : rhs.filesystems) {
      const auto &fs = i.second;
      filesystems[fs->fscid] = std::make_shared<Filesystem>(*fs);
    }
  }

  FSMap &operator=(const FSMap &rhs)
  {
    epoch = rhs.epoch;
    next_filesystem_id = rhs.next_filesystem_id;
    legacy_client_fscid = rhs.legacy_client_fscid;
    compat = rhs.compat;
    enable_multiple = rhs.enable_multiple;
    mds_roles = rhs.mds_roles;
    standby_daemons = rhs.standby_daemons;
    standby_epochs = rhs.standby_epochs;

    filesystems.clear();
    for (const auto &i : rhs.filesystems) {
      const auto &fs = i.second;
      filesystems[fs->fscid] = std::make_shared<Filesystem>(*fs);
    }

    return *this;
  }

  const CompatSet &get_compat() const {return compat;}

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

  bool is_any_degraded() const {
    for (auto& i : filesystems) {
      if (i.second->mds_map.is_degraded()) {
	return true;
      }
    }
    return false;
  }

  /**
   * Get state of all daemons (for all filesystems, including all standbys)
   */
  std::map<mds_gid_t, MDSMap::mds_info_t> get_mds_info() const
  {
    std::map<mds_gid_t, MDSMap::mds_info_t> result;
    for (const auto &i : standby_daemons) {
      result[i.first] = i.second;
    }

    for (const auto &i : filesystems) {
      const auto &fs_info = i.second->mds_map.get_mds_info();
      for (const auto &j : fs_info) {
        result[j.first] = j.second;
      }
    }

    return result;
  }

  mds_gid_t get_available_standby() const;

  /**
   * Resolve daemon name to GID
   */
  mds_gid_t find_mds_gid_by_name(std::string_view s) const
  {
    const auto info = get_mds_info();
    for (const auto &p : info) {
      if (p.second.name == s) {
	return p.first;
      }
    }
    return MDS_GID_NONE;
  }

  /**
   * Resolve daemon name to status
   */
  const MDSMap::mds_info_t* find_by_name(std::string_view name) const
  {
    std::map<mds_gid_t, MDSMap::mds_info_t> result;
    for (const auto &i : standby_daemons) {
      if (i.second.name == name) {
        return &(i.second);
      }
    }

    for (const auto &i : filesystems) {
      const auto &fs_info = i.second->mds_map.get_mds_info();
      for (const auto &j : fs_info) {
        if (j.second.name == name) {
          return &(j.second);
        }
      }
    }

    return nullptr;
  }

  /**
   * Does a daemon exist with this GID?
   */
  bool gid_exists(mds_gid_t gid) const
  {
    return mds_roles.count(gid) > 0;
  }

  /**
   * Does a daemon with this GID exist, *and* have an MDS rank assigned?
   */
  bool gid_has_rank(mds_gid_t gid) const
  {
    return gid_exists(gid) && mds_roles.at(gid) != FS_CLUSTER_ID_NONE;
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
      Filesystem& filesystem,
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
  void erase(mds_gid_t who, epoch_t blacklist_epoch);

  /**
   * Update to indicate that the rank held by 'who' is damaged
   */
  void damaged(mds_gid_t who, epoch_t blacklist_epoch);

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
  Filesystem::ref create_filesystem(
      std::string_view name, int64_t metadata_pool,
      int64_t data_pool, uint64_t features);

  /**
   * Remove the filesystem (it must exist).  Caller should already
   * have failed out any MDSs that were assigned to the filesystem.
   */
  void erase_filesystem(fs_cluster_id_t fscid)
  {
    filesystems.erase(fscid);
  }

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
    fn(fs);
    fs->mds_map.epoch = epoch;
  }

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
      auto& info = fs->mds_map.mds_info.at(who);
      fn(info);
      fs->mds_map.epoch = epoch;
    }
  }

  /**
   * Given that gid exists in a filesystem or as a standby, return
   * a reference to its info.
   */
  const MDSMap::mds_info_t& get_info_gid(mds_gid_t gid) const
  {
    auto fscid = mds_roles.at(gid);
    if (fscid == FS_CLUSTER_ID_NONE) {
      return standby_daemons.at(gid);
    } else {
      return filesystems.at(fscid)->mds_map.mds_info.at(gid);
    }
  }

  bool is_standby_replay(mds_gid_t who) const
  {
    return filesystems.at(mds_roles.at(who))->is_standby_replay(who);
  }

  mds_gid_t get_standby_replay(mds_gid_t who) const
  {
    return filesystems.at(mds_roles.at(who))->get_standby_replay(who);
  }

  /**
   * A daemon has told us it's compat, and it's too new
   * for the one we had previously.  Impose the new one
   * on all filesystems.
   */
  void update_compat(const CompatSet &c)
  {
    // We could do something more complicated here to enable
    // different filesystems to be served by different MDS versions,
    // but this is a lot simpler because it doesn't require us to
    // track the compat versions for standby daemons.
    compat = c;
    for (const auto &i : filesystems) {
      MDSMap &mds_map = i.second->mds_map;
      mds_map.compat = c;
      mds_map.epoch = epoch;
    }
  }

  Filesystem::const_ref get_legacy_filesystem()
  {
    if (legacy_client_fscid == FS_CLUSTER_ID_NONE) {
      return nullptr;
    } else {
      return filesystems.at(legacy_client_fscid);
    }
  }

  /**
   * A daemon has informed us of its offload targets
   */
  void update_export_targets(mds_gid_t who, const std::set<mds_rank_t> &targets)
  {
    auto fscid = mds_roles.at(who);
    modify_filesystem(fscid, [who, &targets](auto&& fs) {
      fs->mds_map.mds_info.at(who).export_targets = targets;
    });
  }

  epoch_t get_epoch() const { return epoch; }
  void inc_epoch() { epoch++; }

  size_t filesystem_count() const {return filesystems.size();}
  bool filesystem_exists(fs_cluster_id_t fscid) const {return filesystems.count(fscid) > 0;}
  Filesystem::const_ref get_filesystem(fs_cluster_id_t fscid) const {return std::const_pointer_cast<const Filesystem>(filesystems.at(fscid));}
  Filesystem::ref get_filesystem(fs_cluster_id_t fscid) {return filesystems.at(fscid);}
  Filesystem::const_ref get_filesystem(void) const {return std::const_pointer_cast<const Filesystem>(filesystems.begin()->second);}
  Filesystem::const_ref get_filesystem(std::string_view name) const
  {
    for (const auto& p : filesystems) {
      if (p.second->mds_map.fs_name == name) {
        return p.second;
      }
    }
    return nullptr;
  }
  std::vector<Filesystem::const_ref> get_filesystems(void) const
  {
    std::vector<Filesystem::const_ref> ret;
    for (const auto& p : filesystems) {
      ret.push_back(p.second);
    }
    return ret;
  }

  int parse_filesystem(
      std::string_view ns_str,
      Filesystem::const_ref *result
      ) const;

  int parse_role(
      std::string_view role_str,
      mds_role_t *role,
      std::ostream &ss) const;

  /**
   * Return true if this pool is in use by any of the filesystems
   */
  bool pool_in_use(int64_t poolid) const {
    for (auto const &i : filesystems) {
      if (i.second->mds_map.is_data_pool(poolid)
          || i.second->mds_map.metadata_pool == poolid) {
        return true;
      }
    }
    return false;
  }

  mds_gid_t find_replacement_for(mds_role_t mds, std::string_view name) const;

  void get_health(list<pair<health_status_t,std::string> >& summary,
		  list<pair<health_status_t,std::string> > *detail) const;

  void get_health_checks(health_check_map_t *checks) const;

  bool check_health(void);

  /**
   * Assert that the FSMap, Filesystem, MDSMap, mds_info_t relations are
   * all self-consistent.
   */
  void sanity() const;

  void encode(bufferlist& bl, uint64_t features) const;
  void decode(bufferlist::const_iterator& p);
  void decode(bufferlist& bl) {
    auto p = bl.cbegin();
    decode(p);
  }
  void sanitize(const std::function<bool(int64_t pool)>& pool_exists);

  void print(ostream& out) const;
  void print_summary(Formatter *f, ostream *out) const;

  void dump(Formatter *f) const;
  static void generate_test_instances(std::list<FSMap*>& ls);
};
WRITE_CLASS_ENCODER_FEATURES(FSMap)

inline ostream& operator<<(ostream& out, const FSMap& m) {
  m.print_summary(NULL, &out);
  return out;
}

#endif
