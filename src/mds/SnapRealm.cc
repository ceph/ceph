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
#include "SnapClient.h"

#include <string_view>


/*
 * SnapRealm
 */

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix _prefix(_dout, mdcache->mds->get_nodeid(), inode, srnode.seq, this)
static ostream& _prefix(std::ostream *_dout, int whoami, const CInode *inode,
			uint64_t seq, const SnapRealm *realm) {
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
  out << " past_parent_snaps=" << realm.srnode.past_parent_snaps;

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

  if (realm.srnode.is_parent_global())
    out << " global ";
  out << " " << &realm << ")";
  return out;
}

SnapRealm::SnapRealm(MDCache *c, CInode *in) :
    mdcache(c), inode(in), parent(nullptr),
    num_open_past_parents(0), inodes_with_caps(0)
{
  global = (inode->ino() == MDS_INO_GLOBAL_SNAPREALM);
}

void SnapRealm::add_open_past_parent(SnapRealm *parent, snapid_t last)
{
  auto p = open_past_parents.find(parent->inode->ino());
  if (p != open_past_parents.end()) {
    ceph_assert(p->second.second.count(last) == 0);
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
  ceph_assert(p != open_past_parents.end());
  auto q = p->second.second.find(last);
  ceph_assert(q != p->second.second.end());
  p->second.second.erase(q);
  --num_open_past_parents;
  if (p->second.second.empty()) {
    SnapRealm *parent = p->second.first;
    open_past_parents.erase(p);
    parent->open_past_children.erase(this);
    parent->inode->put(CInode::PIN_PASTSNAPPARENT);
  }
}

struct C_SR_RetryOpenParents : public MDSContext {
  SnapRealm *sr;
  snapid_t first, last, parent_last;
  inodeno_t parent;
  MDSContext* fin;
  C_SR_RetryOpenParents(SnapRealm *s, snapid_t f, snapid_t l, snapid_t pl,
			inodeno_t p, MDSContext *c) :
    sr(s), first(f), last(l), parent_last(pl),  parent(p), fin(c) {
    sr->inode->get(CInode::PIN_OPENINGSNAPPARENTS);
  }
  MDSRank *get_mds() override { return sr->mdcache->mds; }
  void finish(int r) override {
    if (r < 0)
      sr->_remove_missing_parent(parent_last, parent, r);
    if (sr->_open_parents(fin, first, last)) {
      if (fin)
	fin->complete(0);
    }
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
    past_parents_dirty = true;
  } else {
    dout(10) << __func__ << " " << parent << " not found" << dendl;
  }
}

bool SnapRealm::_open_parents(MDSContext *finish, snapid_t first, snapid_t last)
{
  dout(10) << "open_parents [" << first << "," << last << "]" << dendl;
  if (open) 
    return true;

  // make sure my current parents' parents are open...
  if (parent) {
    dout(10) << " current parent [" << srnode.current_parent_since << ",head] is " << *parent
	     << " on " << *parent->inode << dendl;
    if (last >= srnode.current_parent_since &&
	!parent->_open_parents(finish, std::max(first, srnode.current_parent_since), last))
      return false;
  }

  if (!srnode.past_parent_snaps.empty())
    ceph_assert(mdcache->mds->snapclient->get_cached_version() > 0);

  if (!srnode.past_parents.empty() &&
      mdcache->mds->allows_multimds_snaps()) {
    dout(10) << " skip non-empty past_parents since multimds_snaps is allowed" << dendl;
    open = true;
    return true;
  }

  // and my past parents too!
  ceph_assert(srnode.past_parents.size() >= num_open_past_parents);
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
	past_parents_dirty = true;
	continue;
      }
      ceph_assert(parent->snaprealm);  // hmm!
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

bool SnapRealm::open_parents(MDSContext *retryorfinish) {
  if (!_open_parents(retryorfinish))
    return false;
  delete retryorfinish;
  return true;
}

bool SnapRealm::have_past_parents_open(snapid_t first, snapid_t last) const
{
  dout(10) << "have_past_parents_open [" << first << "," << last << "]" << dendl;
  if (open)
    return true;

  if (!srnode.past_parent_snaps.empty())
    ceph_assert(mdcache->mds->snapclient->get_cached_version() > 0);

  if (!srnode.past_parents.empty() &&
      mdcache->mds->allows_multimds_snaps()) {
    dout(10) << " skip non-empty past_parents since multimds_snaps is allowed" << dendl;
    open = true;
    return true;
  }

  for (auto p = srnode.past_parents.lower_bound(first);
       p != srnode.past_parents.end();
       ++p) {
    if (p->second.first > last)
      break;
    dout(10) << " past parent [" << p->second.first << "," << p->first << "] was "
	     << p->second.ino << dendl;
    auto q = open_past_parents.find(p->second.ino);
    if (q == open_past_parents.end()) {
      dout(10) << " past parent " << p->second.ino << " is not open" << dendl;
      return false;
    }
    SnapRealm *parent_realm = q->second.first;
    if (!parent_realm->have_past_parents_open(std::max(first, p->second.first),
					      std::min(last, p->first)))
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
void SnapRealm::build_snap_set() const
{
  dout(10) << "build_snap_set on " << *this << dendl;

  cached_snaps.clear();

  if (global) {
    mdcache->mds->snapclient->get_snaps(cached_snaps);
    return;
  }

  // include my snaps
  for (const auto& p : srnode.snaps)
    cached_snaps.insert(p.first);

  if (!srnode.past_parent_snaps.empty()) {
    set<snapid_t> snaps = mdcache->mds->snapclient->filter(srnode.past_parent_snaps);
    if (!snaps.empty()) {
      snapid_t last = *snaps.rbegin();
      cached_seq = std::max(cached_seq, last);
      cached_last_created = std::max(cached_last_created, last);
    }
    cached_snaps.insert(snaps.begin(), snaps.end());
  } else {
    // include snaps for parents
    for (const auto& p : srnode.past_parents) {
      const CInode *oldparent = mdcache->get_inode(p.second.ino);
      ceph_assert(oldparent);  // call open_parents first!
      ceph_assert(oldparent->snaprealm);

      const set<snapid_t>& snaps = oldparent->snaprealm->get_snaps();
      snapid_t last = 0;
      for (auto q = snaps.lower_bound(p.second.first);
	  q != snaps.end() && *q <= p.first;
	  q++) {
	cached_snaps.insert(*q);
	last = *q;
      }
      cached_seq = std::max(cached_seq, last);
      cached_last_created = std::max(cached_last_created, last);
    }
  }

  snapid_t parent_seq = parent ? parent->get_newest_seq() : snapid_t(0);
  if (parent_seq >= srnode.current_parent_since) {
    auto& snaps = parent->get_snaps();
    auto p = snaps.lower_bound(srnode.current_parent_since);
    cached_snaps.insert(p, snaps.end());
    cached_seq = std::max(cached_seq, parent_seq);
    cached_last_created = std::max(cached_last_created, parent->get_last_created());
  }
}

void SnapRealm::check_cache() const
{
  ceph_assert(have_past_parents_open());
  snapid_t seq;
  snapid_t last_created;
  snapid_t last_destroyed = mdcache->mds->snapclient->get_last_destroyed();
  if (global || srnode.is_parent_global()) {
    last_created = mdcache->mds->snapclient->get_last_created();
    seq = std::max(last_created, last_destroyed);
  } else {
    last_created = srnode.last_created;
    seq = srnode.seq;
  }
  if (cached_seq >= seq &&
      cached_last_destroyed == last_destroyed)
    return;

  cached_snap_context.clear();

  cached_seq = seq;
  cached_last_created = last_created;
  cached_last_destroyed = last_destroyed;
  build_snap_set();

  build_snap_trace();
  
  dout(10) << "check_cache rebuilt " << cached_snaps
	   << " seq " << seq
	   << " cached_seq " << cached_seq
	   << " cached_last_created " << cached_last_created
	   << " cached_last_destroyed " << cached_last_destroyed
	   << ")" << dendl;
}

const set<snapid_t>& SnapRealm::get_snaps() const
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
const SnapContext& SnapRealm::get_snap_context() const
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

void SnapRealm::get_snap_info(map<snapid_t, const SnapInfo*>& infomap, snapid_t first, snapid_t last)
{
  const set<snapid_t>& snaps = get_snaps();
  dout(10) << "get_snap_info snaps " << snaps << dendl;

  // include my snaps within interval [first,last]
  for (auto p = srnode.snaps.lower_bound(first); // first element >= first
       p != srnode.snaps.end() && p->first <= last;
       ++p)
    infomap[p->first] = &p->second;

  if (!srnode.past_parent_snaps.empty()) {
    set<snapid_t> snaps;
    for (auto p = srnode.past_parent_snaps.lower_bound(first); // first element >= first
	p != srnode.past_parent_snaps.end() && *p <= last;
	++p) {
      snaps.insert(*p);
    }

    map<snapid_t, const SnapInfo*> _infomap;
    mdcache->mds->snapclient->get_snap_infos(_infomap, snaps);
    infomap.insert(_infomap.begin(), _infomap.end());
  } else {
    // include snaps for parents during intervals that intersect [first,last]
    for (map<snapid_t, snaplink_t>::iterator p = srnode.past_parents.lower_bound(first);
	p != srnode.past_parents.end() && p->first >= first && p->second.first <= last;
	++p) {
      CInode *oldparent = mdcache->get_inode(p->second.ino);
      ceph_assert(oldparent);  // call open_parents first!
      ceph_assert(oldparent->snaprealm);
      oldparent->snaprealm->get_snap_info(infomap,
					  std::max(first, p->second.first),
					  std::min(last, p->first));
    }
  }

  if (srnode.current_parent_since <= last && parent)
    parent->get_snap_info(infomap, std::max(first, srnode.current_parent_since), last);
}

std::string_view SnapRealm::get_snapname(snapid_t snapid, inodeno_t atino)
{
  auto srnode_snaps_entry = srnode.snaps.find(snapid);
  if (srnode_snaps_entry != srnode.snaps.end()) {
    if (atino == inode->ino())
      return srnode_snaps_entry->second.name;
    else
      return srnode_snaps_entry->second.get_long_name();
  }

  if (!srnode.past_parent_snaps.empty()) {
    if (srnode.past_parent_snaps.count(snapid)) {
      const SnapInfo *sinfo = mdcache->mds->snapclient->get_snap_info(snapid);
      if (sinfo) {
	if (atino == sinfo->ino)
	  return sinfo->name;
	else
	  return sinfo->get_long_name();
      }
    }
  } else {
    map<snapid_t,snaplink_t>::iterator p = srnode.past_parents.lower_bound(snapid);
    if (p != srnode.past_parents.end() && p->second.first <= snapid) {
      CInode *oldparent = mdcache->get_inode(p->second.ino);
      ceph_assert(oldparent);  // call open_parents first!
      ceph_assert(oldparent->snaprealm);
      return oldparent->snaprealm->get_snapname(snapid, atino);
    }
  }

  ceph_assert(srnode.current_parent_since <= snapid);
  ceph_assert(parent);
  return parent->get_snapname(snapid, atino);
}

snapid_t SnapRealm::resolve_snapname(std::string_view n, inodeno_t atino, snapid_t first, snapid_t last)
{
  // first try me
  dout(10) << "resolve_snapname '" << n << "' in [" << first << "," << last << "]" << dendl;

  //snapid_t num;
  //if (n[0] == '~') num = atoll(n.c_str()+1);

  bool actual = (atino == inode->ino());
  string pname;
  inodeno_t pino;
  if (n.length() && n[0] == '_') {
    int next_ = n.find('_', 1);
    if (next_ > 1) {
      pname = n.substr(1, next_ - 1);
      pino = atoll(n.data() + next_ + 1);
      dout(10) << " " << n << " parses to name '" << pname << "' dirino " << pino << dendl;
    }
  }

  for (auto p = srnode.snaps.lower_bound(first); // first element >= first
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

  if (!srnode.past_parent_snaps.empty()) {
    set<snapid_t> snaps;
    for (auto p = srnode.past_parent_snaps.lower_bound(first); // first element >= first
	 p != srnode.past_parent_snaps.end() && *p <= last;
	 ++p)
      snaps.insert(*p);

    map<snapid_t, const SnapInfo*> _infomap;
    mdcache->mds->snapclient->get_snap_infos(_infomap, snaps);

    for (auto& it : _infomap) {
      dout(15) << " ? " << *it.second << dendl;
      actual = (it.second->ino == atino);
      if (actual && it.second->name == n)
	return it.first;
      if (!actual && it.second->name == pname && it.second->ino == pino)
	return it.first;
    }
  } else {
    // include snaps for parents during intervals that intersect [first,last]
    for (map<snapid_t, snaplink_t>::iterator p = srnode.past_parents.lower_bound(first);
	 p != srnode.past_parents.end() && p->first >= first && p->second.first <= last;
	 ++p) {
      CInode *oldparent = mdcache->get_inode(p->second.ino);
      ceph_assert(oldparent);  // call open_parents first!
      ceph_assert(oldparent->snaprealm);
      snapid_t r = oldparent->snaprealm->resolve_snapname(n, atino,
							  std::max(first, p->second.first),
							  std::min(last, p->first));
      if (r)
	return r;
    }
  }

  if (parent && srnode.current_parent_since <= last)
    return parent->resolve_snapname(n, atino, std::max(first, srnode.current_parent_since), last);
  return 0;
}


void SnapRealm::adjust_parent()
{
  SnapRealm *newparent;
  if (srnode.is_parent_global()) {
    newparent = mdcache->get_global_snaprealm();
  } else {
    CDentry *pdn = inode->get_parent_dn();
    newparent = pdn ? pdn->get_dir()->get_inode()->find_snaprealm() : NULL;
  }
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
      ceph_assert(!child->inode->is_any_caps());
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
	child->inode->is_ancestor_of(realm->inode)) {
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
  for (elist<CInode*>::iterator p = inodes_with_caps.begin(member_offset(CInode, item_caps));
       !p.end(); ) {
    CInode *in = *p;
    ++p;
    // does inode fall within the child realm?
    if (child->inode->is_ancestor_of(in)) {
      dout(20) << " child gets " << *in << dendl;
      in->move_to_realm(child);
    } else {
      dout(20) << "    keeping " << *in << dendl;
    }
  }
}

void SnapRealm::merge_to(SnapRealm *newparent)
{
  if (!newparent)
    newparent = parent;
  dout(10) << "merge to " << *newparent << " on " << *newparent->inode << dendl;

  ceph_assert(open_past_children.empty());

  dout(10) << " open_children are " << open_children << dendl;
  for (auto realm : open_children) {
    dout(20) << " child realm " << *realm << " on " << *realm->inode << dendl;
    newparent->open_children.insert(realm);
    realm->parent = newparent;
  }
  open_children.clear();

  elist<CInode*>::iterator p = inodes_with_caps.begin(member_offset(CInode, item_caps));
  while (!p.end()) {
    CInode *in = *p;
    ++p;
    in->move_to_realm(newparent);
  }
  ceph_assert(inodes_with_caps.empty());

  // delete this
  inode->close_snaprealm();
}

const bufferlist& SnapRealm::get_snap_trace() const
{
  check_cache();
  return cached_snap_trace;
}

void SnapRealm::build_snap_trace() const
{
  cached_snap_trace.clear();

  if (global) {
    SnapRealmInfo info(inode->ino(), 0, cached_seq, 0);
    info.my_snaps.reserve(cached_snaps.size());
    for (auto p = cached_snaps.rbegin(); p != cached_snaps.rend(); ++p)
      info.my_snaps.push_back(*p);

    dout(10) << "build_snap_trace my_snaps " << info.my_snaps << dendl;
    encode(info, cached_snap_trace);
    return;
  }

  SnapRealmInfo info(inode->ino(), srnode.created, srnode.seq, srnode.current_parent_since);
  if (parent) {
    info.h.parent = parent->inode->ino();

    set<snapid_t> past;
    if (!srnode.past_parent_snaps.empty()) {
      past = mdcache->mds->snapclient->filter(srnode.past_parent_snaps);
      if (srnode.is_parent_global()) {
	auto p = past.lower_bound(srnode.current_parent_since);
	past.erase(p, past.end());
      }
    } else if (!srnode.past_parents.empty()) {
      const set<snapid_t>& snaps = get_snaps();
      for (const auto& p : srnode.past_parents) {
	for (auto q = snaps.lower_bound(p.second.first);
	     q != snaps.end() && *q <= p.first;
	     q++) {
	  if (srnode.snaps.count(*q))
	    continue;
	  past.insert(*q);
	}
      }
    }

    if (!past.empty()) {
      info.prior_parent_snaps.reserve(past.size());
      for (set<snapid_t>::reverse_iterator p = past.rbegin(); p != past.rend(); ++p)
	info.prior_parent_snaps.push_back(*p);
      dout(10) << "build_snap_trace prior_parent_snaps from [1," << *past.rbegin() << "] "
	       << info.prior_parent_snaps << dendl;
    }
  }

  info.my_snaps.reserve(srnode.snaps.size());
  for (auto p = srnode.snaps.rbegin();
       p != srnode.snaps.rend();
       ++p)
    info.my_snaps.push_back(p->first);
  dout(10) << "build_snap_trace my_snaps " << info.my_snaps << dendl;

  encode(info, cached_snap_trace);

  if (parent)
    cached_snap_trace.append(parent->get_snap_trace());
}

void SnapRealm::prune_past_parents()
{
  dout(10) << "prune_past_parents" << dendl;
  check_cache();

  // convert past_parents to past_parent_snaps
  if (!srnode.past_parents.empty()) {
    for (auto p = cached_snaps.begin();
	 p != cached_snaps.end() && *p < srnode.current_parent_since;
	 ++p) {
      if (!srnode.snaps.count(*p))
	srnode.past_parent_snaps.insert(*p);
    }
    srnode.past_parents.clear();
    past_parents_dirty = true;
  }

  for (auto p = srnode.past_parent_snaps.begin();
       p != srnode.past_parent_snaps.end(); ) {
    auto q = cached_snaps.find(*p);
    if (q == cached_snaps.end()) {
      dout(10) << "prune_past_parents pruning " << *p << dendl;
      srnode.past_parent_snaps.erase(p++);
    } else {
      dout(10) << "prune_past_parents keeping " << *p << dendl;
      ++p;
    }
  }
}

