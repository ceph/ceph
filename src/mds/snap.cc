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

#define dout(x) if (x <= g_conf.debug_mds) *_dout << dbeginl << g_clock.now() \
						  << " mds" << mdcache->mds->get_nodeid() \
						  << ".cache.snaprealm(" << inode->ino() \
						  << " seq " << seq << " " << this << ") "

bool SnapRealm::open_parents(MDRequest *mdr)
{
  dout(10) << "open_parents" << dendl;

  // make sure my current parents' parents are open...
  if (parent) {
    dout(10) << " parent is " << *parent
	     << " on " << *parent->inode << dendl;
    if (!parent->open_parents(mdr))
      return false;
  }

  // and my past parents too!
  for (map<snapid_t, snaplink_t>::iterator p = past_parents.begin();
       p != past_parents.end();
       p++) {    
    CInode *parent = mdcache->get_inode(p->second.ino);
    if (parent)
      continue;
    mdcache->open_remote_ino(p->second.ino, mdr, 
			     new C_MDS_RetryRequest(mdcache, mdr));
    return false;
  }
  return true;
}

/*
 * get list of snaps for this realm.  we must include parents' snaps
 * for the intervals during which they were our parent.
 */
void SnapRealm::build_snap_set(set<snapid_t> &s, snapid_t first, snapid_t last)
{
  dout(10) << "build_snap_set [" << first << "," << last << "] on " << *this << dendl;

  // include my snaps within interval [first,last]
  for (map<snapid_t, SnapInfo>::iterator p = snaps.lower_bound(first); // first element >= first
       p != snaps.end() && p->first <= last;
       p++)
    s.insert(p->first);

  // include snaps for parents during intervals that intersect [first,last]
  for (map<snapid_t, snaplink_t>::iterator p = past_parents.lower_bound(first);
       p != past_parents.end() && p->first >= first && p->second.first <= last;
       p++) {
    CInode *oldparent = mdcache->get_inode(p->second.ino);
    assert(oldparent);  // call open_parents first!
    assert(oldparent->snaprealm);
    
    snapid_t thru = MIN(last, p->first);
    oldparent->snaprealm->build_snap_set(s, 
					 MAX(first, p->second.first),
					 thru);
  }
  if (current_parent_since <= last && parent)
    parent->build_snap_set(s, current_parent_since, last);
}

/*
 * build vector in reverse sorted order
 */
const set<snapid_t>& SnapRealm::get_snaps()
{
  if (cached_snaps.empty() || cached_snaps_seq < seq) {
    cached_snaps.clear();
    cached_snap_vec.clear();
    build_snap_set(cached_snaps, 0, CEPH_NOSNAP);

    dout(10) << "get_snaps " << cached_snaps
	     << " (seq " << seq << ")" 
	     << dendl;
  } else {
    dout(10) << "get_snaps " << cached_snaps
	     << " (seq " << seq << ")" 
	     << " (cached)"
	     << dendl;
  }
  return cached_snaps;
}

const vector<snapid_t>& SnapRealm::get_snap_vector()
{
  get_snaps();

  if (cached_snap_vec.empty()) {
    cached_snap_vec.resize(cached_snaps.size());
    unsigned i = 0;
    for (set<snapid_t>::reverse_iterator p = cached_snaps.rbegin();
	 p != cached_snaps.rend();
	 p++)
      cached_snap_vec[i++] = *p;
  }

  return cached_snap_vec;
}

void SnapRealm::get_snap_info(map<snapid_t,SnapInfo*>& infomap, snapid_t first, snapid_t last)
{
  dout(10) << "get_snap_info snaps " << get_snaps() << dendl;

  // include my snaps within interval [first,last]
  for (map<snapid_t, SnapInfo>::iterator p = snaps.lower_bound(first); // first element >= first
       p != snaps.end() && p->first <= last;
       p++)
    infomap[p->first] = &p->second;

  // include snaps for parents during intervals that intersect [first,last]
  for (map<snapid_t, snaplink_t>::iterator p = past_parents.lower_bound(first);
       p != past_parents.end() && p->first >= first && p->second.first <= last;
       p++) {
    CInode *oldparent = mdcache->get_inode(p->second.ino);
    assert(oldparent);  // call open_parents first!
    assert(oldparent->snaprealm);
    
    snapid_t thru = MIN(last, p->first);
    oldparent->snaprealm->get_snap_info(infomap,
					MAX(first, p->second.first),
					thru);
  }
  if (current_parent_since <= last && parent)
    parent->get_snap_info(infomap, current_parent_since, last);
}

const string& SnapInfo::get_long_name()
{
  if (long_name.length() == 0) {
    char nm[80];
    sprintf(nm, "_%s_%llu", name.c_str(), (unsigned long long)ino);
    long_name = nm;
  }
  return long_name;
}

const string& SnapRealm::get_snapname(snapid_t snapid, inodeno_t atino)
{
  if (snaps.count(snapid)) {
    if (atino == inode->ino())
      return snaps[snapid].name;
    else
      return snaps[snapid].get_long_name();
  }

  map<snapid_t,snaplink_t>::iterator p = past_parents.lower_bound(snapid);
  if (p != past_parents.end() && p->second.first <= snapid) {
    CInode *oldparent = mdcache->get_inode(p->second.ino);
    assert(oldparent);  // call open_parents first!
    assert(oldparent->snaprealm);    
    return oldparent->snaprealm->get_snapname(snapid, atino);
  }

  assert(current_parent_since <= snapid);
  assert(parent);
  return parent->get_snapname(snapid, atino);
}

snapid_t SnapRealm::resolve_snapname(const string& n, inodeno_t atino, snapid_t first, snapid_t last)
{
  // first try me
  dout(10) << "resolve_snapname '" << n << "' in [" << first << "," << last << "]" << dendl;

  //snapid_t num;
  //if (n[0] == '~') num = atoll(n.c_str()+1);

  bool actual = (atino == inode->ino());
  string pname;
  inodeno_t pino;
  if (!actual) {
    if (!n.length() ||
	n[0] != '_') return 0;
    int next_ = n.find('_', 1);
    if (next_ < 0) return 0;
    pname = n.substr(1, next_ - 1);
    pino = atoll(n.c_str() + next_ + 1);
    dout(10) << " " << n << " parses to name '" << pname << "' dirino " << pino << dendl;
  }

  for (map<snapid_t, SnapInfo>::iterator p = snaps.lower_bound(first); // first element >= first
       p != snaps.end() && p->first <= last;
       p++) {
    dout(15) << " ? " << p->second << dendl;
    //if (num && p->second.snapid == num)
    //return p->first;
    if (actual && p->second.name == n)
	return p->first;
    if (!actual && p->second.name == pname && p->second.ino == pino)
      return p->first;
  }

    // include snaps for parents during intervals that intersect [first,last]
  for (map<snapid_t, snaplink_t>::iterator p = past_parents.lower_bound(first);
       p != past_parents.end() && p->first >= first && p->second.first <= last;
       p++) {
    CInode *oldparent = mdcache->get_inode(p->second.ino);
    assert(oldparent);  // call open_parents first!
    assert(oldparent->snaprealm);
    
    snapid_t thru = MIN(last, p->first);
    snapid_t r = oldparent->snaprealm->resolve_snapname(n, atino,
							MAX(first, p->second.first),
							thru);
    if (r)
      return r;
  }
  if (current_parent_since <= last && parent)
    return parent->resolve_snapname(n, atino, current_parent_since, last);
  return 0;
}



void SnapRealm::split_at(SnapRealm *child)
{
  dout(10) << "split_at " << *child 
	   << " on " << *child->inode << dendl;

  // split open_children
  dout(10) << " open_children are " << open_children << dendl;
  for (set<SnapRealm*>::iterator p = open_children.begin();
       p != open_children.end(); ) {
    SnapRealm *realm = *p;
    if (realm != child &&
	child->inode->is_ancestor_of(realm->inode)) {
      dout(20) << " child gets child realm " << *realm << " on " << *realm->inode << dendl;
      realm->parent = child;
      child->open_children.insert(realm);
      open_children.erase(p++);
    } else {
      dout(20) << "    keeping child realm " << *realm << " on " << *realm->inode << dendl;
      p++;
    }
  }

  // split inodes_with_caps
  xlist<CInode*>::iterator p = inodes_with_caps.begin();
  while (!p.end()) {
    CInode *in = *p;
    ++p;

    // does inode fall within the child realm?
    CInode *t = in;
    bool under_child = false;
    while (t->get_parent_dn()) {
      t = t->get_parent_dn()->get_dir()->get_inode();
      if (t == child->inode) {
	under_child = true;
	break;
      }
      if (t == in)
	break;
    }
    if (under_child) {
      dout(20) << " child gets " << *in << dendl;
      in->move_to_containing_realm(child);
    } else {
      dout(20) << "    keeping " << *in << dendl;
    }
  }

}




void SnapRealm::build_snap_trace(bufferlist& snapbl)
{
  SnapRealmInfo info;
  info.ino = inode->ino();
  info.seq = seq;

  if (parent) {
    info.parent = parent->inode->ino();
    if (!past_parents.empty()) {
      snapid_t last = past_parents.rbegin()->first;
      set<snapid_t> past;
      build_snap_set(past, 0, last);
      info.prior_parent_snaps.reserve(past.size());
      for (set<snapid_t>::reverse_iterator p = past.rbegin(); p != past.rend(); p++)
	info.prior_parent_snaps.push_back(*p);
      info.parent_since = last+1;
      dout(10) << "build_snap_trace prior_parent_snaps from [1," << last << "] "
	       << info.prior_parent_snaps << dendl;
    } else
      info.parent_since = 1;
  } else 
    info.parent = 0;

  info.my_snaps.reserve(snaps.size());
  for (map<snapid_t,SnapInfo>::reverse_iterator p = snaps.rbegin();
       p != snaps.rend();
       p++)
    info.my_snaps.push_back(p->first);
  dout(10) << "build_snap_trace my_snaps " << info.my_snaps << dendl;

  ::encode(info, snapbl);

  if (parent)
    parent->build_snap_trace(snapbl);
}
