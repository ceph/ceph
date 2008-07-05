// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004- Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#include "snap.h"
#include "MDCache.h"
#include "MDS.h"

#include "messages/MClientSnap.h"

/*
 * SnapRealm
 */

#define dout(x) if (x < g_conf.debug_mds) *_dout << dbeginl << g_clock.now() \
						 << " mds" << mdcache->mds->get_nodeid() \
						 << ".snaprealm(" << inode->ino() << ") "

bool SnapRealm::open_parents(MDRequest *mdr)
{
  dout(10) << "open_parents" << dendl;
  for (multimap<snapid_t, snaplink_t>::iterator p = parents.begin();
       p != parents.end();
       p++) {
    CInode *parent = mdcache->get_inode(p->second.dirino);
    if (parent)
      continue;
    mdcache->open_remote_ino(p->second.dirino, mdr, 
			     new C_MDS_RetryRequest(mdcache, mdr));
    return false;
  }
  return true;
}

/*
 * get list of snaps for this realm.  we must include parents' snaps
 * for the intervals during which they were our parent.
 */
void SnapRealm::get_snap_set(set<snapid_t> &s, snapid_t first, snapid_t last)
{
  dout(10) << "get_snap_set [" << first << "," << last << "] on " << *this << dendl;

  // include my snaps within interval [first,last]
  for (map<snapid_t, SnapInfo>::iterator p = snaps.lower_bound(first); // first element >= first
       p != snaps.end() && p->first <= last;
       p++)
    s.insert(p->first);

  // include snaps for parents during intervals that intersect [first,last]
  for (multimap<snapid_t, snaplink_t>::iterator p = parents.lower_bound(first);
       p != parents.end() && p->first >= first && p->second.first <= last;
       p++) {
    CInode *parent = mdcache->get_inode(p->second.dirino);
    assert(parent);  // call open_parents first!
    assert(parent->snaprealm);

    parent->snaprealm->get_snap_set(s, 
				    MAX(first, p->second.first),
				    MIN(last, p->first));				    
  }
  
  if (!s.empty()) {
    snapid_t t = *s.rbegin();
    if (highwater < t)
      highwater = t;
  }
}

/*
 * build vector in reverse sorted order
 */
void SnapRealm::get_snap_vector(vector<snapid_t> &v)
{
  set<snapid_t> s;
  get_snap_set(s);
  v.resize(s.size());
  int i = 0;
  for (set<snapid_t>::reverse_iterator p = s.rbegin(); p != s.rend(); p++)
    v[i++] = *p;

  dout(10) << "get_snap_vector " << v << " (highwater " << highwater << ")" << dendl;
}


void SnapRealm::split_at(SnapRealm *child)
{
  dout(10) << "split_at " << *child << " on " << *child->inode << dendl;

  xlist<CInode*>::iterator p = inodes_with_caps.begin();
  while (!p.end()) {
    CInode *in = *p;
    ++p;

    // does inode fall within the child realm?
    CInode *t = in;
    bool under_child = false;
    while (t) {
      t = t->get_parent_dn()->get_dir()->get_inode();
      if (t == child->inode) {
	under_child = true;
	break;
      }
      if (t == in)
	break;
    }
    if (!under_child) {
      dout(20) << " keeping " << *in << dendl;
      continue;
    }
    
    dout(20) << " child gets " << *in << dendl;
    in->move_to_containing_realm(child);
  }

}
