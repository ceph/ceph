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

#ifndef __CEPH_MDS_SNAP_H
#define __CEPH_MDS_SNAP_H

#include "mdstypes.h"
#include "include/xlist.h"

/*
 * generic snap descriptor.
 */
struct SnapInfo {
  snapid_t snapid;
  inodeno_t dirino;
  utime_t stamp;
  string name;
  
  void encode(bufferlist& bl) const {
    ::encode(snapid, bl);
    ::encode(dirino, bl);
    ::encode(stamp, bl);
    ::encode(name, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(snapid, bl);
    ::decode(dirino, bl);
    ::decode(stamp, bl);
    ::decode(name, bl);
  }
};
WRITE_CLASS_ENCODER(SnapInfo)

inline ostream& operator<<(ostream& out, const SnapInfo &sn) {
  return out << "snap(" << sn.snapid
	     << " " << sn.dirino
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

struct snaplink_t {
  inodeno_t dirino;
  snapid_t first;
};

struct SnapRealm {
  // realm state
  inodeno_t dirino;
  map<snapid_t, SnapInfo> snaps;
  multimap<snapid_t, snaplink_t> parents, children;  // key is "last" (or NOSNAP)

  // in-memory state
  MDCache *mdcache;
  CInode *inode;

  // caches?
  //set<snapid_t> cached_snaps;
  //set<SnapRealm*> cached_active_children;    // active children that are currently open

  xlist<CInode*> inodes_with_caps;               // for efficient realm splits
  map<int, CapabilityGroup*> client_cap_groups;  // to identify clients who need snap notifications

  SnapRealm(inodeno_t i, MDCache *c, CInode *in) : dirino(i), mdcache(c), inode(in) {}

  bool open_parents(MDRequest *mdr);
  void get_snap_list(set<snapid_t>& s);
};



/*
 * CapabilityGroup - group per-realm, per-client caps for efficient
 * client snap notifications.
 */
struct Capability;

struct CapabilityGroup {
  int client;
  xlist<Capability*> caps;
  SnapRealm *realm;
};


#endif
