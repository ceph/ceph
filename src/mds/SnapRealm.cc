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

#include "SnapRealm.h"
#include "MDCache.h"
#include "MDSRank.h"

#include "messages/MClientSnap.h"


/*
 * SnapRealm
 */

#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix _prefix(_dout, mdcache->mds->get_nodeid(), inode, srnode.seq, this)
static ostream& _prefix(std::ostream *_dout, int whoami, CInode *inode,
			uint64_t seq, SnapRealm *realm) {
  return *_dout << " mds." << whoami
		<< ".cache.snaprealm(" << inode->ino()
		<< " seq " << seq << " " << realm << ") ";
}

ostream& operator<<(ostream& out, const SnapRealm& realm) 
{
  out << "snaprealm(" << realm.inode->ino()
      << " seq " << realm.srnode.seq
      << " lc " << realm.srnode.last_created
      << " cr " << realm.srnode.created;
  if (realm.srnode.created != realm.srnode.current_parent_since)
    out << " cps " << realm.srnode.current_parent_since;
  out << " snaps=" << realm.srnode.snaps;
  if (realm.srnode.past_parents.size()) {
    out << " past_parents=(";
    for (map<snapid_t, snaplink_t>::const_iterator p = realm.srnode.past_parents.begin(); 
	 p != realm.srnode.past_parents.end(); 
	 ++p) {
      if (p != realm.srnode.past_parents.begin()) out << ",";
      out << p->second.first << "-" << p->first
	  << "=" << p->second.ino;
    }
    out << ")";
  }
  out << " " << &realm << ")";
  return out;
}


void SnapRealm::add_open_past_parent(SnapRealm *parent, snapid_t last)
{
  auto p = open_past_parents.find(parent->inode->ino());
  if (p != open_past_parents.end()) {
    assert(p->second.second.count(last) == 0);
    p->second.second.insert(last);
  } else {
    open_past_parents[parent->inode->ino()].first = parent;
    open_past_parents[parent->inode->ino()].second.insert(last);
    parent->open_past_children.insert(this);
    parent->inode->get(CInode::PIN_PASTSNAPPARENT);
  }
  ++num_open_past_parents;
}

void SnapRealm::remove_open_past_parent(inodeno_t ino, snapid_t last)
{
  auto p = open_past_parents.find(ino);
  assert(p != open_past_parents.end());
  auto q = p->second.second.find(last);
  assert(q != p->second.second.end());
  p->second.second.erase(q);
  --num_open_past_parents;
  if (p->second.second.empty()) {
    SnapRealm *parent = p->second.first;
    open_past_parents.erase(p);
    parent->open_past_children.erase(this);
    parent->inode->put(CInode::PIN_PASTSNAPPARENT);
  }
}

struct C_SR_RetryOpenParents : public MDSInternalContextBase {
  SnapRealm *sr;
  snapid_t first, last, parent_last;
  inodeno_t parent;
  MDSInternalContextBase* fin;
  C_SR_RetryOpenParents(SnapRealm *s, snapid_t f, snapid_t l, snapid_t pl,
			inodeno_t p, MDSInternalContextBase *c) :
    sr(s), first(f), last(l), parent_last(pl),  parent(p), fin(c) {
    sr->inode->get(CInode::PIN_OPENINGSNAPPARENTS);
  }
  MDSRank *get_mds() { return sr->mdcache->mds; }
  void finish(int r) {
    if (r < 0)
      sr->_remove_missing_parent(parent_last, parent, r);
    if (sr->_open_parents(fin, first, last))
      fin->complete(0);
    sr->inode->put(CInode::PIN_OPENINGSNAPPARENTS);
  }
};

void SnapRealm::_remove_missing_parent(snapid_t snapid, inodeno_t parent, int err)
{
  map<snapid_t, snaplink_t>::iterator p = srnode.past_parents.find(snapid);
  if (p != srnode.past_parents.end()) {
    dout(10) << __func__ << " " << parent << " [" << p->second.first << ","
	     << p->first << "]  errno " << err << dendl;
    srnode.past_parents.erase(p);
  } else {
    dout(10) << __func__ << " " << parent << " not found" << dendl;
  }
}

bool SnapRealm::_open_parents(MDSInternalContextBase *finish, snapid_t first, snapid_t last)
{
  dout(10) << "open_parents [" << first << "," << last << "]" << dendl;
  if (open) 
    return true;

  // make sure my current parents' parents are open...
  if (parent) {
    dout(10) << " current parent [" << srnode.current_parent_since << ",head] is " << *parent
	     << " on " << *parent->inode << dendl;
    if (last >= srnode.current_parent_since &&
	!parent->_open_parents(finish, MAX(first, srnode.current_parent_since), last))
      return false;
  }

  // and my past parents too!
  assert(srnode.past_parents.size() >= num_open_past_parents);
  if (srnode.past_parents.size() > num_open_past_parents) {
    for (map<snapid_t, snaplink_t>::iterator p = srnode.past_parents.begin();
	 p != srnode.past_parents.end(); ) {
      dout(10) << " past_parent [" << p->second.first << "," << p->first << "] is "
	       << p->second.ino << dendl;
      CInode *parent = mdcache->get_inode(p->second.ino);
      if (!parent) {
	C_SR_RetryOpenParents *fin = new C_SR_RetryOpenParents(this, first, last, p->first,
							       p->second.ino, finish);
	mdcache->open_ino(p->second.ino, mdcache->mds->mdsmap->get_metadata_pool(), fin);
	return false;
      }
      if (parent->state_test(CInode::STATE_PURGING)) {
	dout(10) << " skip purging past_parent " << *parent << dendl;
	srnode.past_parents.erase(p++);
	continue;
      }
      assert(parent->snaprealm);  // hmm!
      if (!parent->snaprealm->_open_parents(finish, p->second.first, p->first))
	return false;
      auto q = open_past_parents.find(p->second.ino);
      if (q == open_past_parents.end() ||
	  q->second.second.count(p->first) == 0) {
	add_open_past_parent(parent->snaprealm, p->first);
      }
      ++p;
    }
  }

  open = true;
  return true;
}

bool SnapRealm::have_past_parents_open(snapid_t first, snapid_t last)
{
  dout(10) << "have_past_parents_open [" << first << "," << last << "]" << dendl;
  if (open)
    return true;

  for (map<snapid_t, snaplink_t>::iterator p = srnode.past_parents.lower_bound(first);
       p != srnode.past_parents.end();
       ++p) {
    if (p->second.first > last)
      break;
    dout(10) << " past parent [" << p->second.first << "," << p->first << "] was "
	     << p->second.ino << dendl;
    if (open_past_parents.count(p->second.ino) == 0) {
      dout(10) << " past parent " << p->second.ino << " is not open" << dendl;
      return false;
    }
    SnapRealm *parent_realm = open_past_parents[p->second.ino].first;
    if (!parent_realm->have_past_parents_open(MAX(first, p->second.first),
					      MIN(last, p->first)))
      return false;
  }

  open = true;
  return true;
}

void SnapRealm::close_parents()
{
  for (auto p = open_past_parents.begin(); p != open_past_parents.end(); ++p) {
    num_open_past_parents -= p->second.second.size();
    p->second.first->inode->put(CInode::PIN_PASTSNAPPARENT);
    p->second.first->open_past_children.erase(this);
  }
  open_past_parents.clear();
}


/*
 * get list of snaps for this realm.  we must include parents' snaps
 * for the intervals during which they were our parent.
 */
void SnapRealm::build_snap_set(set<snapid_t> &s,
			       snapid_t& max_seq, snapid_t& max_last_created, snapid_t& max_last_destroyed,
			       snapid_t first, snapid_t last)
{
  dout(10) << "build_snap_set [" << first << "," << last << "] on " << *this << dendl;

  if (srnode.seq > max_seq)
    max_seq = srnode.seq;
  if (srnode.last_created > max_last_created)
    max_last_created = srnode.last_created;
  if (srnode.last_destroyed > max_last_destroyed)
    max_last_destroyed = srnode.last_destroyed;

  // include my snaps within interval [first,last]
  for (map<snapid_t, SnapInfo>::iterator p = srnode.snaps.lower_bound(first); // first element >= first
       p != srnode.snaps.end() && p->first <= last;
       ++p)
    s.insert(p->first);

  // include snaps for parents during intervals that intersect [first,last]
  for (map<snapid_t, snaplink_t>::iterator p = srnode.past_parents.lower_bound(first);
       p != srnode.past_parents.end() && p->first >= first && p->second.first <= last;
       ++p) {
    CInode *oldparent = mdcache->get_inode(p->second.ino);
    assert(oldparent);  // call open_parents first!
    assert(oldparent->snaprealm);
    oldparent->snaprealm->build_snap_set(s, max_seq, max_last_created, max_last_destroyed,
					 MAX(first, p->second.first),
					 MIN(last, p->first));
  }
  if (srnode.current_parent_since <= last && parent)
    parent->build_snap_set(s, max_seq, max_last_created, max_last_destroyed,
			   MAX(first, srnode.current_parent_since), last);
}


void SnapRealm::check_cache()
{
  assert(open);
  if (cached_seq >= srnode.seq)
    return;

  cached_snaps.clear();
  cached_snap_context.clear();

  cached_last_created = srnode.last_created;
  cached_last_destroyed = srnode.last_destroyed;
  cached_seq = srnode.seq;
  build_snap_set(cached_snaps, cached_seq, cached_last_created, cached_last_destroyed,
		 0, CEPH_NOSNAP);

  cached_snap_trace.clear();
  build_snap_trace(cached_snap_trace);
  
  dout(10) << "check_cache rebuilt " << cached_snaps
	   << " seq " << srnode.seq
	   << " cached_seq " << cached_seq
	   << " cached_last_created " << cached_last_created
	   << " cached_last_destroyed " << cached_last_destroyed
	   << ")" << dendl;
}

const set<snapid_t>& SnapRealm::get_snaps()
{
  check_cache();
  dout(10) << "get_snaps " << cached_snaps
	   << " (seq " << srnode.seq << " cached_seq " << cached_seq << ")"
	   << dendl;
  return cached_snaps;
}

/*
 * build vector in reverse sorted order
 */
const SnapContext& SnapRealm::get_snap_context()
{
  check_cache();

  if (!cached_snap_context.seq) {
    cached_snap_context.seq = cached_seq;
    cached_snap_context.snaps.resize(cached_snaps.size());
    unsigned i = 0;
    for (set<snapid_t>::reverse_iterator p = cached_snaps.rbegin();
	 p != cached_snaps.rend();
	 ++p)
      cached_snap_context.snaps[i++] = *p;
  }

  return cached_snap_context;
}

void SnapRealm::get_snap_info(map<snapid_t,SnapInfo*>& infomap, snapid_t first, snapid_t last)
{
  const set<snapid_t>& snaps = get_snaps();
  dout(10) << "get_snap_info snaps " << snaps << dendl;

  // include my snaps within interval [first,last]
  for (map<snapid_t, SnapInfo>::iterator p = srnode.snaps.lower_bound(first); // first element >= first
       p != srnode.snaps.end() && p->first <= last;
       ++p)
    infomap[p->first] = &p->second;

  // include snaps for parents during intervals that intersect [first,last]
  for (map<snapid_t, snaplink_t>::iterator p = srnode.past_parents.lower_bound(first);
       p != srnode.past_parents.end() && p->first >= first && p->second.first <= last;
       ++p) {
    CInode *oldparent = mdcache->get_inode(p->second.ino);
    assert(oldparent);  // call open_parents first!
    assert(oldparent->snaprealm);
    oldparent->snaprealm->get_snap_info(infomap,
					MAX(first, p->second.first),
					MIN(last, p->first));
  }
  if (srnode.current_parent_since <= last && parent)
    parent->get_snap_info(infomap, MAX(first, srnode.current_parent_since), last);
}

const string& SnapRealm::get_snapname(snapid_t snapid, inodeno_t atino)
{
  if (srnode.snaps.count(snapid)) {
    if (atino == inode->ino())
      return srnode.snaps[snapid].name;
    else
      return srnode.snaps[snapid].get_long_name();
  }

  map<snapid_t,snaplink_t>::iterator p = srnode.past_parents.lower_bound(snapid);
  if (p != srnode.past_parents.end() && p->second.first <= snapid) {
    CInode *oldparent = mdcache->get_inode(p->second.ino);
    assert(oldparent);  // call open_parents first!
    assert(oldparent->snaprealm);    
    return oldparent->snaprealm->get_snapname(snapid, atino);
  }

  assert(srnode.current_parent_since <= snapid);
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

  for (map<snapid_t, SnapInfo>::iterator p = srnode.snaps.lower_bound(first); // first element >= first
       p != srnode.snaps.end() && p->first <= last;
       ++p) {
    dout(15) << " ? " << p->second << dendl;
    //if (num && p->second.snapid == num)
    //return p->first;
    if (actual && p->second.name == n)
	return p->first;
    if (!actual && p->second.name == pname && p->second.ino == pino)
      return p->first;
  }

    // include snaps for parents during intervals that intersect [first,last]
  for (map<snapid_t, snaplink_t>::iterator p = srnode.past_parents.lower_bound(first);
       p != srnode.past_parents.end() && p->first >= first && p->second.first <= last;
       ++p) {
    CInode *oldparent = mdcache->get_inode(p->second.ino);
    assert(oldparent);  // call open_parents first!
    assert(oldparent->snaprealm);
    snapid_t r = oldparent->snaprealm->resolve_snapname(n, atino,
							MAX(first, p->second.first),
							MIN(last, p->first));
    if (r)
      return r;
  }
  if (parent && srnode.current_parent_since <= last)
    return parent->resolve_snapname(n, atino, MAX(first, srnode.current_parent_since), last);
  return 0;
}


void SnapRealm::adjust_parent()
{
  SnapRealm *newparent = inode->get_parent_dn()->get_dir()->get_inode()->find_snaprealm();
  if (newparent != parent) {
    dout(10) << "adjust_parent " << parent << " -> " << newparent << dendl;
    if (parent)
      parent->open_children.erase(this);
    parent = newparent;
    if (parent)
      parent->open_children.insert(this);
    
    invalidate_cached_snaps();
  }
}

void SnapRealm::split_at(SnapRealm *child)
{
  dout(10) << "split_at " << *child 
	   << " on " << *child->inode << dendl;

  if (inode->is_mdsdir() || !child->inode->is_dir()) {
    // it's not a dir.
    if (child->inode->containing_realm) {
      //  - no open children.
      //  - only need to move this child's inode's caps.
      child->inode->move_to_realm(child);
    } else {
      // no caps, nothing to move/split.
      dout(20) << " split no-op, no caps to move on file " << *child->inode << dendl;
      assert(!child->inode->is_any_caps());
    }
    return;
  }

  // it's a dir.

  // split open_children
  dout(10) << " open_children are " << open_children << dendl;
  for (set<SnapRealm*>::iterator p = open_children.begin();
       p != open_children.end(); ) {
    SnapRealm *realm = *p;
    if (realm != child &&
	child->inode->is_projected_ancestor_of(realm->inode)) {
      dout(20) << " child gets child realm " << *realm << " on " << *realm->inode << dendl;
      realm->parent = child;
      child->open_children.insert(realm);
      open_children.erase(p++);
    } else {
      dout(20) << "    keeping child realm " << *realm << " on " << *realm->inode << dendl;
      ++p;
    }
  }

  // split inodes_with_caps
  elist<CInode*>::iterator p = inodes_with_caps.begin(member_offset(CInode, item_caps));
  while (!p.end()) {
    CInode *in = *p;
    ++p;

    // does inode fall within the child realm?
    bool under_child = false;

    if (in == child->inode) {
      under_child = true;
    } else {
      CInode *t = in;
      while (t->get_parent_dn()) {
	t = t->get_parent_dn()->get_dir()->get_inode();
	if (t == child->inode) {
	  under_child = true;
	  break;
	}
	if (t == in)
	  break;
      }
    }
    if (under_child) {
      dout(20) << " child gets " << *in << dendl;
      in->move_to_realm(child);
    } else {
      dout(20) << "    keeping " << *in << dendl;
    }
  }

}

const bufferlist& SnapRealm::get_snap_trace()
{
  check_cache();
  return cached_snap_trace;
}

void SnapRealm::build_snap_trace(bufferlist& snapbl)
{
  SnapRealmInfo info(inode->ino(), srnode.created, srnode.seq, srnode.current_parent_since);

  if (parent) {
    info.h.parent = parent->inode->ino();
    if (!srnode.past_parents.empty()) {
      snapid_t last = srnode.past_parents.rbegin()->first;
      set<snapid_t> past;
      snapid_t max_seq, max_last_created, max_last_destroyed;
      build_snap_set(past, max_seq, max_last_created, max_last_destroyed, 0, last);
      info.prior_parent_snaps.reserve(past.size());
      for (set<snapid_t>::reverse_iterator p = past.rbegin(); p != past.rend(); ++p)
	info.prior_parent_snaps.push_back(*p);
      dout(10) << "build_snap_trace prior_parent_snaps from [1," << last << "] "
	       << info.prior_parent_snaps << dendl;
    }
  } else 
    info.h.parent = 0;

  info.my_snaps.reserve(srnode.snaps.size());
  for (map<snapid_t,SnapInfo>::reverse_iterator p = srnode.snaps.rbegin();
       p != srnode.snaps.rend();
       ++p)
    info.my_snaps.push_back(p->first);
  dout(10) << "build_snap_trace my_snaps " << info.my_snaps << dendl;

  ::encode(info, snapbl);

  if (parent)
    parent->build_snap_trace(snapbl);
}



void SnapRealm::prune_past_parents()
{
  dout(10) << "prune_past_parents" << dendl;
  check_cache();
  assert(open);

  map<snapid_t, snaplink_t>::iterator p = srnode.past_parents.begin();
  while (p != srnode.past_parents.end()) {
    set<snapid_t>::iterator q = cached_snaps.lower_bound(p->second.first);
    if (q == cached_snaps.end() ||
	*q > p->first) {
      dout(10) << "prune_past_parents pruning [" << p->second.first << "," << p->first 
	       << "] " << p->second.ino << dendl;
      remove_open_past_parent(p->second.ino, p->first);
      srnode.past_parents.erase(p++);
    } else {
      dout(10) << "prune_past_parents keeping [" << p->second.first << "," << p->first 
	       << "] " << p->second.ino << dendl;
      ++p;
    }
  }
}

