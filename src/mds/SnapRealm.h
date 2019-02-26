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
protected:
  // cache
  mutable snapid_t cached_seq;           // max seq over self and all past+present parents.
  mutable snapid_t cached_last_created;  // max last_created over all past+present parents
  mutable snapid_t cached_last_destroyed;
  mutable set<snapid_t> cached_snaps;
  mutable SnapContext cached_snap_context;
  mutable bufferlist cached_snap_trace;

  void check_cache() const;

public:
  // realm state
  sr_t srnode;

  // in-memory state
  MDCache *mdcache;
  CInode *inode;

  mutable bool open = false;                        // set to true once all past_parents are opened
  bool past_parents_dirty = false;
  bool global;

  SnapRealm *parent;
  set<SnapRealm*> open_children;    // active children that are currently open
  set<SnapRealm*> open_past_children;  // past children who has pinned me
  map<inodeno_t, pair<SnapRealm*, set<snapid_t> > > open_past_parents;  // these are explicitly pinned.
  unsigned num_open_past_parents;

  elist<CInode*> inodes_with_caps;             // for efficient realm splits
  map<client_t, xlist<Capability*>* > client_caps;   // to identify clients who need snap notifications

  SnapRealm(MDCache *c, CInode *in);

  bool exists(std::string_view name) const {
    for (map<snapid_t,SnapInfo>::const_iterator p = srnode.snaps.begin();
	 p != srnode.snaps.end();
	 ++p) {
      if (p->second.name == name)
	return true;
    }
    return false;
  }

  bool _open_parents(MDSContext *retryorfinish, snapid_t first=1, snapid_t last=CEPH_NOSNAP);
  bool open_parents(MDSContext *retryorfinish);
  void _remove_missing_parent(snapid_t snapid, inodeno_t parent, int err);
  bool have_past_parents_open(snapid_t first=1, snapid_t last=CEPH_NOSNAP) const;
  void add_open_past_parent(SnapRealm *parent, snapid_t last);
  void remove_open_past_parent(inodeno_t ino, snapid_t last);
  void close_parents();

  void prune_past_parents();
  bool has_past_parents() const {
    return !srnode.past_parent_snaps.empty() ||
	   !srnode.past_parents.empty();
  }

  void build_snap_set() const;
  void get_snap_info(map<snapid_t, const SnapInfo*>& infomap, snapid_t first=0, snapid_t last=CEPH_NOSNAP);

  const bufferlist& get_snap_trace() const;
  void build_snap_trace() const;

  std::string_view get_snapname(snapid_t snapid, inodeno_t atino);
  snapid_t resolve_snapname(std::string_view name, inodeno_t atino, snapid_t first=0, snapid_t last=CEPH_NOSNAP);

  const set<snapid_t>& get_snaps() const;
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
    const set<snapid_t>& s = get_snaps();
    set<snapid_t>::const_iterator p = s.upper_bound(follows);
    if (p != s.end())
      return *p;
    return CEPH_NOSNAP;
  }

  bool has_snaps_in_range(snapid_t first, snapid_t last) {
    check_cache();
    const set<snapid_t>& s = get_snaps();
    set<snapid_t>::const_iterator p = s.lower_bound(first);
    return (p != s.end() && *p <= last);
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
    if (client_caps[client]->empty()) {
      delete client_caps[client];
      client_caps.erase(client);
    }
  }
};

ostream& operator<<(ostream& out, const SnapRealm &realm);

#endif
