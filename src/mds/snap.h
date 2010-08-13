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

#ifndef CEPH_MDS_SNAP_H
#define CEPH_MDS_SNAP_H

#include "mdstypes.h"
#include "include/xlist.h"
#include "include/elist.h"

/*
 * generic snap descriptor.
 */
struct SnapInfo {
  snapid_t snapid;
  inodeno_t ino;
  utime_t stamp;
  string name, long_name;
  
  void encode(bufferlist& bl) const {
    __u8 struct_v = 1;
    ::encode(struct_v, bl);
    ::encode(snapid, bl);
    ::encode(ino, bl);
    ::encode(stamp, bl);
    ::encode(name, bl);
  }
  void decode(bufferlist::iterator& bl) {
    __u8 struct_v;
    ::decode(struct_v, bl);
    ::decode(snapid, bl);
    ::decode(ino, bl);
    ::decode(stamp, bl);
    ::decode(name, bl);
  }
  const string& get_long_name();
};
WRITE_CLASS_ENCODER(SnapInfo)

inline ostream& operator<<(ostream& out, const SnapInfo &sn) {
  return out << "snap(" << sn.snapid
	     << " " << sn.ino
	     << " '" << sn.name
	     << "' " << sn.stamp << ")";
}



/*
 * SnapRealm - a subtree that shares the same set of snapshots.
 */
struct SnapRealm;
struct CapabilityGroup;
class CInode;
class MDCache;
class MDRequest;



#include "Capability.h"

struct snaplink_t {
  inodeno_t ino;
  snapid_t first;
  void encode(bufferlist& bl) const {
    __u8 struct_v = 1;
    ::encode(struct_v, bl);
    ::encode(ino, bl);
    ::encode(first, bl);
  }
  void decode(bufferlist::iterator& bl) {
    __u8 struct_v;
    ::decode(struct_v, bl);
    ::decode(ino, bl);
    ::decode(first, bl);
  }
};
WRITE_CLASS_ENCODER(snaplink_t)

inline ostream& operator<<(ostream& out, const snaplink_t &l)
{
  return out << l.ino << "@" << l.first;
}

// carry data about a specific version of a SnapRealm
struct sr_t {
  snapid_t seq;                     // basically, a version/seq # for changes to _this_ realm.
  snapid_t created;                 // when this realm was created.
  snapid_t last_created;            // last snap created in _this_ realm.
  snapid_t last_destroyed;          // seq for last removal
  snapid_t current_parent_since;
  map<snapid_t, SnapInfo> snaps;
  map<snapid_t, snaplink_t> past_parents;  // key is "last" (or NOSNAP)

  sr_t() :
    seq(0), created(0),
    last_created(0), last_destroyed(0),
    current_parent_since(1)
  {}


  void encode(bufferlist& bl) const {
    __u8 struct_v = 1;
    ::encode(struct_v, bl);
    ::encode(seq, bl);
    ::encode(created, bl);
    ::encode(last_created, bl);
    ::encode(last_destroyed, bl);
    ::encode(current_parent_since, bl);
    ::encode(snaps, bl);
    ::encode(past_parents, bl);
  }
  void decode(bufferlist::iterator& p) {
    __u8 struct_v;
    ::decode(struct_v, p);
    ::decode(seq, p);
    ::decode(created, p);
    ::decode(last_created, p);
    ::decode(last_destroyed, p);
    ::decode(current_parent_since, p);
    ::decode(snaps, p);
    ::decode(past_parents, p);
  }
};
WRITE_CLASS_ENCODER(sr_t);

struct SnapRealm {
  // realm state

  sr_t srnode;
  void encode(bufferlist& bl) const {
    __u8 struct_v = 2;
    ::encode(struct_v, bl);
    ::encode(srnode, bl);
  }
  void decode(bufferlist::iterator& p) {
    __u8 struct_v;
    ::decode(struct_v, p);
    if (struct_v >= 2)
      ::decode(srnode, p);
    else {
      ::decode(srnode.seq, p);
      ::decode(srnode.created, p);
      ::decode(srnode.last_created, p);
      ::decode(srnode.last_destroyed, p);
      ::decode(srnode.current_parent_since, p);
      ::decode(srnode.snaps, p);
      ::decode(srnode.past_parents, p);
    }
  }

  // in-memory state
  MDCache *mdcache;
  CInode *inode;

  bool open;                        // set to true once all past_parents are opened
  SnapRealm *parent;
  set<SnapRealm*> open_children;    // active children that are currently open
  map<inodeno_t,SnapRealm*> open_past_parents;  // these are explicitly pinned.

  // cache
  snapid_t cached_seq;           // max seq over self and all past+present parents.
  snapid_t cached_last_created;  // max last_created over all past+present parents
  snapid_t cached_last_destroyed;
  set<snapid_t> cached_snaps;
  SnapContext cached_snap_context;

  bufferlist cached_snap_trace;

  elist<CInode*> inodes_with_caps;             // for efficient realm splits
  map<client_t, xlist<Capability*> > client_caps;   // to identify clients who need snap notifications

  SnapRealm(MDCache *c, CInode *in) : 
    srnode(),
    mdcache(c), inode(in),
    open(false), parent(0),
    inodes_with_caps(0) 
  { }

  bool exists(const string &name) {
    for (map<snapid_t,SnapInfo>::iterator p = srnode.snaps.begin();
	 p != srnode.snaps.end();
	 p++)
      if (p->second.name == name)
	return true;
    return false;
  }

  bool _open_parents(Context *retryorfinish, snapid_t first=1, snapid_t last=CEPH_NOSNAP);
  bool open_parents(Context *retryorfinish) {
    if (!_open_parents(retryorfinish))
      return false;
    delete retryorfinish;
    return true;
  }
  bool have_past_parents_open(snapid_t first=1, snapid_t last=CEPH_NOSNAP);
  void add_open_past_parent(SnapRealm *parent);
  void close_parents();

  void add_past_parent(SnapRealm *oldparent);
  void prune_past_parents();
  bool has_past_parents() { return !srnode.past_parents.empty(); }

  void build_snap_set(set<snapid_t>& s, 
		      snapid_t& max_seq, snapid_t& max_last_created, snapid_t& max_last_destroyed,
		      snapid_t first, snapid_t last);
  void get_snap_info(map<snapid_t,SnapInfo*>& infomap, snapid_t first=0, snapid_t last=CEPH_NOSNAP);

  const bufferlist& get_snap_trace();
  void build_snap_trace(bufferlist& snapbl);

  const string& get_snapname(snapid_t snapid, inodeno_t atino);
  snapid_t resolve_snapname(const string &name, inodeno_t atino, snapid_t first=0, snapid_t last=CEPH_NOSNAP);

  void check_cache();
  const set<snapid_t>& get_snaps();
  const SnapContext& get_snap_context();
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
    set<snapid_t> s = get_snaps();
    set<snapid_t>::iterator p = s.upper_bound(follows);
    if (p != s.end())
      return *p;
    return CEPH_NOSNAP;
  }

  void adjust_parent();

  void split_at(SnapRealm *child);
  void join(SnapRealm *child);

  void add_cap(client_t client, Capability *cap) {
    client_caps[client].push_back(&cap->item_snaprealm_caps);
  }
  void remove_cap(client_t client, Capability *cap) {
    cap->item_snaprealm_caps.remove_myself();
    if (client_caps[client].empty())
      client_caps.erase(client);
  }

};
WRITE_CLASS_ENCODER(SnapRealm)

ostream& operator<<(ostream& out, const SnapRealm &realm);





#endif
