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

#ifndef CEPH_MDS_SNAPREALM_H
#define CEPH_MDS_SNAPREALM_H

#include <string_view>

#include "mdstypes.h"
#include "snap.h"
#include "include/xlist.h"
#include "include/elist.h"
#include "common/snap_types.h"
#include "MDSContext.h"

struct SnapRealm {
public:
  SnapRealm(MDCache *c, CInode *in);

  bool exists(std::string_view name) const {
    for (auto p = srnode.snaps.begin(); p != srnode.snaps.end(); ++p) {
      if (p->second.name == name)
	return true;
    }
    return false;
  }

  void prune_past_parent_snaps();
  bool has_past_parent_snaps() const {
    return !srnode.past_parent_snaps.empty();
  }

  void build_snap_set() const;
  void get_snap_info(std::map<snapid_t, const SnapInfo*>& infomap, snapid_t first=0, snapid_t last=CEPH_NOSNAP);

  const ceph::buffer::list& get_snap_trace() const;
  const ceph::buffer::list& get_snap_trace_new() const;
  void build_snap_trace() const;

  std::string_view get_snapname(snapid_t snapid, inodeno_t atino);
  snapid_t resolve_snapname(std::string_view name, inodeno_t atino, snapid_t first=0, snapid_t last=CEPH_NOSNAP);

  const std::set<snapid_t>& get_snaps() const;
  const SnapContext& get_snap_context() const;
  void invalidate_cached_snaps() {
    cached_seq = 0;
  }
  snapid_t get_last_created() {
    check_cache();
    return cached_last_created;
  }
  snapid_t get_last_destroyed() {
    check_cache();
    return cached_last_destroyed;
  }
  snapid_t get_newest_snap() {
    check_cache();
    if (cached_snaps.empty())
      return 0;
    else
      return *cached_snaps.rbegin();
  }
  snapid_t get_newest_seq() {
    check_cache();
    return cached_seq;
  }

  snapid_t get_snap_following(snapid_t follows) {
    check_cache();
    const std::set<snapid_t>& s = get_snaps();
    auto p = s.upper_bound(follows);
    if (p != s.end())
      return *p;
    return CEPH_NOSNAP;
  }

  bool has_snaps_in_range(snapid_t first, snapid_t last) {
    check_cache();
    const auto& s = get_snaps();
    auto p = s.lower_bound(first);
    return (p != s.end() && *p <= last);
  }

  inodeno_t get_subvolume_ino() {
    check_cache();
    return cached_subvolume_ino;
  }

  void adjust_parent();

  void split_at(SnapRealm *child);
  void merge_to(SnapRealm *newparent);

  void add_cap(client_t client, Capability *cap) {
    auto client_caps_entry = client_caps.find(client);
    if (client_caps_entry == client_caps.end())
      client_caps_entry = client_caps.emplace(client,
					      new xlist<Capability*>).first;
    client_caps_entry->second->push_back(&cap->item_snaprealm_caps);
  }
  void remove_cap(client_t client, Capability *cap) {
    cap->item_snaprealm_caps.remove_myself();
    auto found = client_caps.find(client);
    if (found != client_caps.end() && found->second->empty()) {
      delete found->second;
      client_caps.erase(found);
    }
  }

  // realm state
  sr_t srnode;

  // in-memory state
  MDCache *mdcache;
  CInode *inode;

  SnapRealm *parent = nullptr;
  std::set<SnapRealm*> open_children;    // active children that are currently open

  elist<CInode*> inodes_with_caps;             // for efficient realm splits
  std::map<client_t, xlist<Capability*>* > client_caps;   // to identify clients who need snap notifications

protected:
  void check_cache() const;

private:
  bool global;

  // cache
  mutable snapid_t cached_seq;           // max seq over self and all past+present parents.
  mutable snapid_t cached_last_created;  // max last_created over all past+present parents
  mutable snapid_t cached_last_destroyed;
  mutable std::set<snapid_t> cached_snaps;
  mutable SnapContext cached_snap_context;
  mutable ceph::buffer::list cached_snap_trace;
  mutable ceph::buffer::list cached_snap_trace_new;
  mutable inodeno_t cached_subvolume_ino = 0;
};

std::ostream& operator<<(std::ostream& out, const SnapRealm &realm);
#endif
