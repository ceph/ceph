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

using namespace std;

static std::ostream& _prefix(std::ostream *_dout, int whoami, const CInode *inode,
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
  if (realm.srnode.past_parent_snaps.size() > 0) {
    out << " past_parent_snaps=" << realm.srnode.past_parent_snaps;
  }

  if (realm.srnode.is_parent_global())
    out << " global ";
  out << " last_modified " << realm.srnode.last_modified
      << " change_attr " << realm.srnode.change_attr;
  out << " " << &realm << ")";
  return out;
}

SnapRealm::SnapRealm(MDCache *c, CInode *in) :
    mdcache(c), inode(in), inodes_with_caps(member_offset(CInode, item_caps))
{
  global = (inode->ino() == CEPH_INO_GLOBAL_SNAPREALM);
  if (inode->ino() == CEPH_INO_ROOT) {
    srnode.last_modified = in->get_inode()->mtime;
  }
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

  cached_subvolume_ino = 0;
  if (parent)
    cached_subvolume_ino = parent->get_subvolume_ino();
  if (!cached_subvolume_ino && srnode.is_subvolume())
    cached_subvolume_ino = inode->ino();

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
  }

  ceph_assert(srnode.current_parent_since <= snapid);
  ceph_assert(parent);
  return parent->get_snapname(snapid, atino);
}

snapid_t SnapRealm::resolve_snapname(std::string_view n, inodeno_t atino, snapid_t first, snapid_t last)
{
  // first try me
  dout(10) << "resolve_snapname '" << n << "' in [" << first << "," << last << "]" << dendl;

  bool actual = (atino == inode->ino());
  string pname;
  inodeno_t pino;
  if (n.length() && n[0] == '_') {
    size_t next_ = n.find_last_of('_');
    if (next_ > 1 && next_ + 1 < n.length()) {
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
  for (auto p = inodes_with_caps.begin(); !p.end(); ) {
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

  dout(10) << " open_children are " << open_children << dendl;
  for (auto realm : open_children) {
    dout(20) << " child realm " << *realm << " on " << *realm->inode << dendl;
    newparent->open_children.insert(realm);
    realm->parent = newparent;
  }
  open_children.clear();

  for (auto p = inodes_with_caps.begin(); !p.end(); ) {
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

const bufferlist& SnapRealm::get_snap_trace_new() const
{
  check_cache();
  return cached_snap_trace_new;
}

void SnapRealm::build_snap_trace() const
{
  cached_snap_trace.clear();
  cached_snap_trace_new.clear();

  if (global) {
    SnapRealmInfo info(inode->ino(), 0, cached_seq, 0);
    info.my_snaps.reserve(cached_snaps.size());
    for (auto p = cached_snaps.rbegin(); p != cached_snaps.rend(); ++p)
      info.my_snaps.push_back(*p);

    dout(10) << "build_snap_trace my_snaps " << info.my_snaps << dendl;

    SnapRealmInfoNew ninfo(info, srnode.last_modified, srnode.change_attr);
    encode(info, cached_snap_trace);
    encode(ninfo, cached_snap_trace_new);
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

  SnapRealmInfoNew ninfo(info, srnode.last_modified, srnode.change_attr);

  encode(info, cached_snap_trace);
  encode(ninfo, cached_snap_trace_new);

  if (parent) {
    cached_snap_trace.append(parent->get_snap_trace());
    cached_snap_trace_new.append(parent->get_snap_trace_new());
  }
}

void SnapRealm::prune_past_parent_snaps()
{
  dout(10) << __func__ << dendl;
  check_cache();

  for (auto p = srnode.past_parent_snaps.begin();
       p != srnode.past_parent_snaps.end(); ) {
    auto q = cached_snaps.find(*p);
    if (q == cached_snaps.end()) {
      dout(10) << __func__ << " pruning " << *p << dendl;
      srnode.past_parent_snaps.erase(p++);
    } else {
      dout(10) << __func__ << " keeping " << *p << dendl;
      ++p;
    }
  }
}

