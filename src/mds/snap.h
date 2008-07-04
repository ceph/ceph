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


/*
 * CapabilityGroup - group per-realm, per-client caps for efficient
 * client snap notifications.
 */
#include "Capability.h"

struct snaplink_t {
  inodeno_t dirino;
  snapid_t first;
  void encode(bufferlist& bl) const {
    ::encode(dirino, bl);
    ::encode(first, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(dirino, bl);
    ::decode(first, bl);
  }
};
WRITE_CLASS_ENCODER(snaplink_t)

struct SnapRealm {
  // realm state
  map<snapid_t, SnapInfo> snaps;
  multimap<snapid_t, snaplink_t> parents, children;  // key is "last" (or NOSNAP)

  void encode(bufferlist& bl) const {
    ::encode(snaps, bl);
    ::encode(parents, bl);
    ::encode(children, bl);
  }
  void decode(bufferlist::iterator& p) {
    ::decode(snaps, p);
    ::decode(parents, p);
    ::decode(children, p);
  }

  // in-memory state
  MDCache *mdcache;
  CInode *inode;

  snapid_t snap_highwater;  // largest snap this realm has exposed to clients (implicitly or explicitly)

  // caches?
  //set<snapid_t> cached_snaps;
  //set<SnapRealm*> cached_active_children;    // active children that are currently open

  xlist<CInode*> inodes_with_caps;             // for efficient realm splits
  map<int, xlist<Capability*> > client_caps;   // to identify clients who need snap notifications

  SnapRealm(MDCache *c, CInode *in) : mdcache(c), inode(in), snap_highwater(0) {}

  bool open_parents(MDRequest *mdr);
  void get_snap_set(set<snapid_t>& s, snapid_t first=0, snapid_t last=CEPH_NOSNAP);
  void get_snap_vector(vector<snapid_t>& s);

  void split_at(SnapRealm *child);

  void add_cap(int client, Capability *cap) {
    client_caps[client].push_back(&cap->snaprealm_caps_item);
  }
  void remove_cap(int client, Capability *cap) {
    cap->snaprealm_caps_item.remove_myself();
    if (client_caps[client].empty())
      client_caps.erase(client);
  }
};
WRITE_CLASS_ENCODER(SnapRealm)

inline ostream& operator<<(ostream& out, const SnapRealm &realm) {
  out << "snaprealm(" << realm.snaps;
  if (realm.parents.size()) {
    out << " parents=(";
    for (multimap<snapid_t, snaplink_t>::const_iterator p = realm.parents.begin(); 
	 p != realm.parents.end(); 
	 p++) {
      if (p != realm.parents.begin()) out << ",";
      out << p->second.first << "-";
      if (p->first == CEPH_NOSNAP)
	out << "head";
      else
	out << p->first;
      out << "=" << p->second.dirino;
    }
    out << ")";
  }
  if (realm.children.size()) {
    out << " children=(";
    for (multimap<snapid_t, snaplink_t>::const_iterator p = realm.parents.begin(); 
	 p != realm.parents.end(); 
	 p++) {
      if (p != realm.parents.begin()) out << ",";
      out << p->second.first << "-";
      if (p->first == CEPH_NOSNAP)
	out << "head";
      else
	out << p->first;
      out << "=" << p->second.dirino;
    }
    out << ")";
  }
  out << ")";
  return out;
}


#endif
