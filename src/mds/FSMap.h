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

#include <errno.h>

#include "include/types.h"
#include "common/Clock.h"
#include "msg/Message.h"
#include "mds/MDSMap.h"

#include <set>
#include <map>
#include <string>

#include "common/config.h"

#include "include/CompatSet.h"
#include "include/ceph_features.h"
#include "common/Formatter.h"
#include "mds/mdstypes.h"

class CephContext;

#define MDS_FEATURE_INCOMPAT_BASE CompatSet::Feature(1, "base v0.20")
#define MDS_FEATURE_INCOMPAT_CLIENTRANGES CompatSet::Feature(2, "client writeable ranges")
#define MDS_FEATURE_INCOMPAT_FILELAYOUT CompatSet::Feature(3, "default file layouts on dirs")
#define MDS_FEATURE_INCOMPAT_DIRINODE CompatSet::Feature(4, "dir inode in separate object")
#define MDS_FEATURE_INCOMPAT_ENCODING CompatSet::Feature(5, "mds uses versioned encoding")
#define MDS_FEATURE_INCOMPAT_OMAPDIRFRAG CompatSet::Feature(6, "dirfrag is stored in omap")
#define MDS_FEATURE_INCOMPAT_INLINE CompatSet::Feature(7, "mds uses inline data")
#define MDS_FEATURE_INCOMPAT_NOANCHOR CompatSet::Feature(8, "no anchor table")

#define MDS_FS_NAME_DEFAULT "cephfs"

/**
 * The MDSMap and any additional fields describing a particular
 * filesystem (a unique fs_cluster_id_t).
 */
class Filesystem
{
  public:
  fs_cluster_id_t fscid;
  MDSMap mds_map;

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& p);

  Filesystem()
    :
      fscid(FS_CLUSTER_ID_NONE)
  {
  }

  void dump(Formatter *f) const;
  void print(std::ostream& out) const;

  /**
   * Return true if a daemon is already assigned as
   * STANDBY_REPLAY for the gid `who`
   */
  bool has_standby_replay(mds_gid_t who) const
  {
    for (const auto &i : mds_map.mds_info) {
      const auto &info = i.second;
      if (info.state == MDSMap::STATE_STANDBY_REPLAY
          && info.rank == mds_map.mds_info.at(who).rank) {
        return true;
      }
    }

    return false;
  }
};
WRITE_CLASS_ENCODER(Filesystem)

class FSMap {
protected:
  epoch_t epoch;
  uint64_t next_filesystem_id;
  fs_cluster_id_t legacy_client_fscid;
  CompatSet compat;
  bool enable_multiple;

  std::map<fs_cluster_id_t, std::shared_ptr<Filesystem> > filesystems;

  // Remember which Filesystem an MDS daemon's info is stored in
  // (or in standby_daemons for FS_CLUSTER_ID_NONE)
  std::map<mds_gid_t, fs_cluster_id_t> mds_roles;

  // For MDS daemons not yet assigned to a Filesystem
  std::map<mds_gid_t, MDSMap::mds_info_t> standby_daemons;
  std::map<mds_gid_t, epoch_t> standby_epochs;

public:

  friend class MDSMonitor;

  FSMap() 
    : epoch(0),
      next_filesystem_id(FS_CLUSTER_ID_ANONYMOUS + 1),
      legacy_client_fscid(FS_CLUSTER_ID_NONE),
      compat(get_mdsmap_compat_set_default()),
      enable_multiple(false)
  { }

  FSMap(const FSMap &rhs)
    :
      epoch(rhs.epoch),
      next_filesystem_id(rhs.next_filesystem_id),
      legacy_client_fscid(rhs.legacy_client_fscid),
      compat(rhs.compat),
      enable_multiple(rhs.enable_multiple),
      mds_roles(rhs.mds_roles),
      standby_daemons(rhs.standby_daemons),
      standby_epochs(rhs.standby_epochs)
  {
    for (auto &i : rhs.filesystems) {
      auto fs = i.second;
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

    for (auto &i : rhs.filesystems) {
      auto fs = i.second;
      filesystems[fs->fscid] = std::make_shared<Filesystem>(*fs);
    }

    return *this;
  }

  const CompatSet &get_compat() const {return compat;}

  void set_enable_multiple(const bool v)
  {
    enable_multiple = v;
  }

  bool get_enable_multiple() const
  {
    return enable_multiple;
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
      auto fs_info = i.second->mds_map.get_mds_info();
      for (auto j : fs_info) {
        result[j.first] = j.second;
      }
    }

    return result;
  }

  /**
   * Resolve daemon name to GID
   */
  mds_gid_t find_mds_gid_by_name(const std::string& s) const
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
  const MDSMap::mds_info_t* find_by_name(const std::string& name) const
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
      std::shared_ptr<Filesystem> filesystem,
      mds_rank_t assigned_rank);

  /**
   * A daemon reports that it is STATE_STOPPED: remove it,
   * and the rank it held.
   */
  void stop(mds_gid_t who);

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
   * Mutator helper for Filesystem objects: expose a non-const
   * Filesystem pointer to `fn` and update epochs appropriately.
   */
  void modify_filesystem(
      const fs_cluster_id_t fscid,
      std::function<void(std::shared_ptr<Filesystem> )> fn)
  {
    auto fs = filesystems.at(fscid);
    fn(fs);
    fs->mds_map.epoch = epoch;
  }

  /**
   * Apply a mutation to the mds_info_t structure for a particular
   * daemon (identified by GID), and make appropriate updates to epochs.
   */
  void modify_daemon(
      mds_gid_t who,
      std::function<void(MDSMap::mds_info_t *info)> fn)
  {
    if (mds_roles.at(who) == FS_CLUSTER_ID_NONE) {
      fn(&standby_daemons.at(who));
      standby_epochs[who] = epoch;
    } else {
      auto fs = filesystems[mds_roles.at(who)];
      auto &info = fs->mds_map.mds_info.at(who);
      fn(&info);

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
    for (auto i : filesystems) {
      MDSMap &mds_map = i.second->mds_map;
      mds_map.compat = c;
      mds_map.epoch = epoch;
    }
  }

  std::shared_ptr<const Filesystem> get_legacy_filesystem()
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
  void update_export_targets(mds_gid_t who, const std::set<mds_rank_t> targets)
  {
    auto fscid = mds_roles.at(who);
    modify_filesystem(fscid, [who, &targets](std::shared_ptr<Filesystem> fs) {
      fs->mds_map.mds_info.at(who).export_targets = targets;
    });
  }

  const std::map<fs_cluster_id_t, std::shared_ptr<Filesystem> > &get_filesystems() const
  {
    return filesystems;
  }
  bool any_filesystems() const {return !filesystems.empty(); }
  bool filesystem_exists(fs_cluster_id_t fscid) const
    {return filesystems.count(fscid) > 0;}

  epoch_t get_epoch() const { return epoch; }
  void inc_epoch() { epoch++; }

  std::shared_ptr<const Filesystem> get_filesystem(fs_cluster_id_t fscid) const
  {
    return std::const_pointer_cast<const Filesystem>(filesystems.at(fscid));
  }

  int parse_filesystem(
      std::string const &ns_str,
      std::shared_ptr<const Filesystem> *result
      ) const;

  int parse_role(
      const std::string &role_str,
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

  mds_gid_t find_standby_for(mds_role_t mds, const std::string& name) const;

  mds_gid_t find_unused(bool force_standby_active) const;

  mds_gid_t find_replacement_for(mds_role_t mds, const std::string& name,
                                 bool force_standby_active) const;

  void get_health(list<pair<health_status_t,std::string> >& summary,
		  list<pair<health_status_t,std::string> > *detail) const;

  std::shared_ptr<const Filesystem> get_filesystem(const std::string &name) const
  {
    for (auto &i : filesystems) {
      if (i.second->mds_map.fs_name == name) {
        return i.second;
      }
    }

    return nullptr;
  }

  /**
   * Assert that the FSMap, Filesystem, MDSMap, mds_info_t relations are
   * all self-consistent.
   */
  void sanity() const;

  void encode(bufferlist& bl, uint64_t features) const;
  void decode(bufferlist::iterator& p);
  void decode(bufferlist& bl) {
    bufferlist::iterator p = bl.begin();
    decode(p);
  }

  void print(ostream& out) const;
  void print_summary(Formatter *f, ostream *out);

  void dump(Formatter *f) const;
  static void generate_test_instances(list<FSMap*>& ls);
};
WRITE_CLASS_ENCODER_FEATURES(FSMap)

inline ostream& operator<<(ostream& out, FSMap& m) {
  m.print_summary(NULL, &out);
  return out;
}

#endif
