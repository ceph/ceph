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

#include "include/int_types.h"
#include "common/errno.h"

#include <string>
#include <stdio.h>

#include "CInode.h"
#include "CDir.h"
#include "CDentry.h"

#include "MDS.h"
#include "MDCache.h"
#include "MDLog.h"
#include "Locker.h"
#include "Mutation.h"

#include "events/EUpdate.h"

#include "osdc/Objecter.h"

#include "snap.h"

#include "LogSegment.h"

#include "common/Clock.h"

#include "messages/MLock.h"
#include "messages/MClientCaps.h"

#include "common/config.h"
#include "global/global_context.h"
#include "include/assert.h"

#include "mds/MDSContinuation.h"

#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds." << mdcache->mds->get_nodeid() << ".cache.ino(" << inode.ino << ") "


class CInodeIOContext : public MDSIOContextBase
{
protected:
  CInode *in;
  MDS *get_mds() {return in->mdcache->mds;}
public:
  CInodeIOContext(CInode *in_) : in(in_) {
    assert(in != NULL);
  }
};


boost::pool<> CInode::pool(sizeof(CInode));
boost::pool<> Capability::pool(sizeof(Capability));

LockType CInode::versionlock_type(CEPH_LOCK_IVERSION);
LockType CInode::authlock_type(CEPH_LOCK_IAUTH);
LockType CInode::linklock_type(CEPH_LOCK_ILINK);
LockType CInode::dirfragtreelock_type(CEPH_LOCK_IDFT);
LockType CInode::filelock_type(CEPH_LOCK_IFILE);
LockType CInode::xattrlock_type(CEPH_LOCK_IXATTR);
LockType CInode::snaplock_type(CEPH_LOCK_ISNAP);
LockType CInode::nestlock_type(CEPH_LOCK_INEST);
LockType CInode::flocklock_type(CEPH_LOCK_IFLOCK);
LockType CInode::policylock_type(CEPH_LOCK_IPOLICY);

//int cinode_pins[CINODE_NUM_PINS];  // counts
ostream& CInode::print_db_line_prefix(ostream& out)
{
  return out << ceph_clock_now(g_ceph_context) << " mds." << mdcache->mds->get_nodeid() << ".cache.ino(" << inode.ino << ") ";
}

/*
 * write caps and lock ids
 */
struct cinode_lock_info_t cinode_lock_info[] = {
  { CEPH_LOCK_IFILE, CEPH_CAP_ANY_FILE_WR },
  { CEPH_LOCK_IAUTH, CEPH_CAP_AUTH_EXCL },
  { CEPH_LOCK_ILINK, CEPH_CAP_LINK_EXCL },
  { CEPH_LOCK_IXATTR, CEPH_CAP_XATTR_EXCL },
  { CEPH_LOCK_IFLOCK, CEPH_CAP_FLOCK_EXCL }  
};
int num_cinode_locks = 5;



ostream& operator<<(ostream& out, const CInode& in)
{
  string path;
  in.make_path_string_projected(path);

  out << "[inode " << in.inode.ino;
  out << " [" 
      << (in.is_multiversion() ? "...":"")
      << in.first << "," << in.last << "]";
  out << " " << path << (in.is_dir() ? "/":"");

  if (in.is_auth()) {
    out << " auth";
    if (in.is_replicated()) 
      out << in.get_replicas();
  } else {
    mds_authority_t a = in.authority();
    out << " rep@" << a.first;
    if (a.second != CDIR_AUTH_UNKNOWN)
      out << "," << a.second;
    out << "." << in.get_replica_nonce();
  }

  if (in.is_symlink())
    out << " symlink='" << in.symlink << "'";
  if (in.is_dir() && !in.dirfragtree.empty())
    out << " " << in.dirfragtree;
  
  out << " v" << in.get_version();
  if (in.get_projected_version() > in.get_version())
    out << " pv" << in.get_projected_version();

  if (in.is_auth_pinned()) {
    out << " ap=" << in.get_num_auth_pins() << "+" << in.get_num_nested_auth_pins();
#ifdef MDS_AUTHPIN_SET
    out << "(" << in.auth_pin_set << ")";
#endif
  }

  if (in.snaprealm)
    out << " snaprealm=" << in.snaprealm;

  if (in.state_test(CInode::STATE_AMBIGUOUSAUTH)) out << " AMBIGAUTH";
  if (in.state_test(CInode::STATE_NEEDSRECOVER)) out << " needsrecover";
  if (in.state_test(CInode::STATE_RECOVERING)) out << " recovering";
  if (in.state_test(CInode::STATE_DIRTYPARENT)) out << " dirtyparent";
  if (in.is_freezing_inode()) out << " FREEZING=" << in.auth_pin_freeze_allowance;
  if (in.is_frozen_inode()) out << " FROZEN";
  if (in.is_frozen_auth_pin()) out << " FROZEN_AUTHPIN";

  const inode_t *pi = in.get_projected_inode();
  if (pi->is_truncating())
    out << " truncating(" << pi->truncate_from << " to " << pi->truncate_size << ")";

  if (in.inode.is_dir()) {
    out << " " << in.inode.dirstat;
    if (g_conf->mds_debug_scatterstat && in.is_projected()) {
      const inode_t *pi = in.get_projected_inode();
      out << "->" << pi->dirstat;
    }
  } else {
    out << " s=" << in.inode.size;
    if (in.inode.nlink != 1)
      out << " nl=" << in.inode.nlink;
  }

  // rstat
  out << " " << in.inode.rstat;
  if (!(in.inode.rstat == in.inode.accounted_rstat))
    out << "/" << in.inode.accounted_rstat;
  if (g_conf->mds_debug_scatterstat && in.is_projected()) {
    const inode_t *pi = in.get_projected_inode();
    out << "->" << pi->rstat;
    if (!(pi->rstat == pi->accounted_rstat))
      out << "/" << pi->accounted_rstat;
  }

  if (!in.client_need_snapflush.empty())
    out << " need_snapflush=" << in.client_need_snapflush;


  // locks
  if (!in.authlock.is_sync_and_unlocked())
    out << " " << in.authlock;
  if (!in.linklock.is_sync_and_unlocked())
    out << " " << in.linklock;
  if (in.inode.is_dir()) {
    if (!in.dirfragtreelock.is_sync_and_unlocked())
      out << " " << in.dirfragtreelock;
    if (!in.snaplock.is_sync_and_unlocked())
      out << " " << in.snaplock;
    if (!in.nestlock.is_sync_and_unlocked())
      out << " " << in.nestlock;
    if (!in.policylock.is_sync_and_unlocked())
      out << " " << in.policylock;
  } else  {
    if (!in.flocklock.is_sync_and_unlocked())
      out << " " << in.flocklock;
  }
  if (!in.filelock.is_sync_and_unlocked())
    out << " " << in.filelock;
  if (!in.xattrlock.is_sync_and_unlocked())
    out << " " << in.xattrlock;
  if (!in.versionlock.is_sync_and_unlocked())  
    out << " " << in.versionlock;

  // hack: spit out crap on which clients have caps
  if (in.inode.client_ranges.size())
    out << " cr=" << in.inode.client_ranges;

  if (!in.get_client_caps().empty()) {
    out << " caps={";
    for (map<client_t,Capability*>::const_iterator it = in.get_client_caps().begin();
         it != in.get_client_caps().end();
         ++it) {
      if (it != in.get_client_caps().begin()) out << ",";
      out << it->first << "="
	  << ccap_string(it->second->pending());
      if (it->second->issued() != it->second->pending())
	out << "/" << ccap_string(it->second->issued());
      out << "/" << ccap_string(it->second->wanted())
	  << "@" << it->second->get_last_sent();
    }
    out << "}";
    if (in.get_loner() >= 0 || in.get_wanted_loner() >= 0) {
      out << ",l=" << in.get_loner();
      if (in.get_loner() != in.get_wanted_loner())
	out << "(" << in.get_wanted_loner() << ")";
    }
  }
  if (!in.get_mds_caps_wanted().empty()) {
    out << " mcw={";
    for (compact_map<int,int>::const_iterator p = in.get_mds_caps_wanted().begin();
	 p != in.get_mds_caps_wanted().end();
	 ++p) {
      if (p != in.get_mds_caps_wanted().begin())
	out << ',';
      out << p->first << '=' << ccap_string(p->second);
    }
    out << '}';
  }

  if (in.get_num_ref()) {
    out << " |";
    in.print_pin_set(out);
  }

  out << " " << &in;
  out << "]";
  return out;
}


void CInode::print(ostream& out)
{
  out << *this;
}



void CInode::add_need_snapflush(CInode *snapin, snapid_t snapid, client_t client)
{
  dout(10) << "add_need_snapflush client." << client << " snapid " << snapid << " on " << snapin << dendl;

  if (client_need_snapflush.empty()) {
    get(CInode::PIN_NEEDSNAPFLUSH);

    // FIXME: this is non-optimal, as we'll block freezes/migrations for potentially
    // long periods waiting for clients to flush their snaps.
    auth_pin(this);   // pin head inode...
  }

  set<client_t>& clients = client_need_snapflush[snapid];
  if (clients.empty())
    snapin->auth_pin(this);  // ...and pin snapped/old inode!
  
  clients.insert(client);
}

void CInode::remove_need_snapflush(CInode *snapin, snapid_t snapid, client_t client)
{
  dout(10) << "remove_need_snapflush client." << client << " snapid " << snapid << " on " << snapin << dendl;
  compact_map<snapid_t, std::set<client_t> >::iterator p = client_need_snapflush.find(snapid);
  if (p == client_need_snapflush.end()) {
    dout(10) << " snapid not found" << dendl;
    return;
  }
  if (!p->second.count(client)) {
    dout(10) << " client not found" << dendl;
    return;
  }
  p->second.erase(client);
  if (p->second.empty()) {
    client_need_snapflush.erase(p);
    snapin->auth_unpin(this);

    if (client_need_snapflush.empty()) {
      put(CInode::PIN_NEEDSNAPFLUSH);
      auth_unpin(this);
    }
  }
}

void CInode::split_need_snapflush(CInode *cowin, CInode *in)
{
  dout(10) << "split_need_snapflush [" << cowin->first << "," << cowin->last << "] for " << *cowin << dendl;
  for (compact_map<snapid_t, set<client_t> >::iterator p = client_need_snapflush.lower_bound(cowin->first);
       p != client_need_snapflush.end() && p->first < in->first; ) {
    compact_map<snapid_t, set<client_t> >::iterator q = p;
    ++p;
    assert(!q->second.empty());
    if (cowin->last >= q->first)
      cowin->auth_pin(this);
    else
      client_need_snapflush.erase(q);
    in->auth_unpin(this);
  }
}

void CInode::mark_dirty_rstat()
{
  if (!state_test(STATE_DIRTYRSTAT)) {
    dout(10) << "mark_dirty_rstat" << dendl;
    state_set(STATE_DIRTYRSTAT);
    get(PIN_DIRTYRSTAT);
    CDentry *dn = get_projected_parent_dn();
    CDir *pdir = dn->dir;
    pdir->dirty_rstat_inodes.push_back(&dirty_rstat_item);

    mdcache->mds->locker->mark_updated_scatterlock(&pdir->inode->nestlock);
  }
}
void CInode::clear_dirty_rstat()
{
  if (state_test(STATE_DIRTYRSTAT)) {
    dout(10) << "clear_dirty_rstat" << dendl;
    state_clear(STATE_DIRTYRSTAT);
    put(PIN_DIRTYRSTAT);
    dirty_rstat_item.remove_myself();
  }
}

inode_t *CInode::project_inode(map<string,bufferptr> *px) 
{
  if (projected_nodes.empty()) {
    projected_nodes.push_back(new projected_inode_t(new inode_t(inode)));
    if (px)
      *px = xattrs;
  } else {
    projected_nodes.push_back(new projected_inode_t(
        new inode_t(*projected_nodes.back()->inode)));
    if (px)
      *px = *get_projected_xattrs();
  }
  if (px) {
    projected_nodes.back()->xattrs = px;
    ++num_projected_xattrs;
  }
  dout(15) << "project_inode " << projected_nodes.back()->inode << dendl;
  return projected_nodes.back()->inode;
}

void CInode::pop_and_dirty_projected_inode(LogSegment *ls) 
{
  assert(!projected_nodes.empty());
  dout(15) << "pop_and_dirty_projected_inode " << projected_nodes.front()->inode
	   << " v" << projected_nodes.front()->inode->version << dendl;
  int64_t old_pool = inode.layout.fl_pg_pool;

  mark_dirty(projected_nodes.front()->inode->version, ls);
  inode = *projected_nodes.front()->inode;

  if (inode.is_backtrace_updated())
    _mark_dirty_parent(ls, old_pool != inode.layout.fl_pg_pool);

  map<string,bufferptr> *px = projected_nodes.front()->xattrs;
  if (px) {
    --num_projected_xattrs;
    xattrs = *px;
    delete px;
  }

  if (projected_nodes.front()->snapnode) {
    pop_projected_snaprealm(projected_nodes.front()->snapnode);
    --num_projected_srnodes;
  }

  delete projected_nodes.front()->inode;
  delete projected_nodes.front();

  projected_nodes.pop_front();
}

sr_t *CInode::project_snaprealm(snapid_t snapid)
{
  sr_t *cur_srnode = get_projected_srnode();
  sr_t *new_srnode;

  if (cur_srnode) {
    new_srnode = new sr_t(*cur_srnode);
  } else {
    new_srnode = new sr_t();
    new_srnode->created = snapid;
    new_srnode->current_parent_since = get_oldest_snap();
  }
  dout(10) << "project_snaprealm " << new_srnode << dendl;
  projected_nodes.back()->snapnode = new_srnode;
  ++num_projected_srnodes;
  return new_srnode;
}

/* if newparent != parent, add parent to past_parents
 if parent DNE, we need to find what the parent actually is and fill that in */
void CInode::project_past_snaprealm_parent(SnapRealm *newparent)
{
  sr_t *new_snap = project_snaprealm();
  SnapRealm *oldparent;
  if (!snaprealm) {
    oldparent = find_snaprealm();
    new_snap->seq = oldparent->get_newest_seq();
  }
  else
    oldparent = snaprealm->parent;

  if (newparent != oldparent) {
    snapid_t oldparentseq = oldparent->get_newest_seq();
    if (oldparentseq + 1 > new_snap->current_parent_since) {
      new_snap->past_parents[oldparentseq].ino = oldparent->inode->ino();
      new_snap->past_parents[oldparentseq].first = new_snap->current_parent_since;
    }
    new_snap->current_parent_since = MAX(oldparentseq, newparent->get_last_created()) + 1;
  }
}

void CInode::pop_projected_snaprealm(sr_t *next_snaprealm)
{
  assert(next_snaprealm);
  dout(10) << "pop_projected_snaprealm " << next_snaprealm
          << " seq" << next_snaprealm->seq << dendl;
  bool invalidate_cached_snaps = false;
  if (!snaprealm) {
    open_snaprealm();
  } else if (next_snaprealm->past_parents.size() !=
	     snaprealm->srnode.past_parents.size()) {
    invalidate_cached_snaps = true;
    // re-open past parents
    snaprealm->_close_parents();

    dout(10) << " realm " << *snaprealm << " past_parents " << snaprealm->srnode.past_parents
	     << " -> " << next_snaprealm->past_parents << dendl;
  }
  snaprealm->srnode = *next_snaprealm;
  delete next_snaprealm;

  // we should be able to open these up (or have them already be open).
  bool ok = snaprealm->_open_parents(NULL);
  assert(ok);

  if (invalidate_cached_snaps)
    snaprealm->invalidate_cached_snaps();

  if (snaprealm->parent)
    dout(10) << " realm " << *snaprealm << " parent " << *snaprealm->parent << dendl;
}


// ====== CInode =======

// dirfrags

__u32 CInode::hash_dentry_name(const string &dn)
{
  int which = inode.dir_layout.dl_dir_hash;
  if (!which)
    which = CEPH_STR_HASH_LINUX;
  return ceph_str_hash(which, dn.data(), dn.length());
}

frag_t CInode::pick_dirfrag(const string& dn)
{
  if (dirfragtree.empty())
    return frag_t();          // avoid the string hash if we can.

  __u32 h = hash_dentry_name(dn);
  return dirfragtree[h];
}

bool CInode::get_dirfrags_under(frag_t fg, list<CDir*>& ls)
{
  bool all = true;
  list<frag_t> fglist;
  dirfragtree.get_leaves_under(fg, fglist);
  for (list<frag_t>::iterator p = fglist.begin(); p != fglist.end(); ++p)
    if (dirfrags.count(*p))
      ls.push_back(dirfrags[*p]);
    else 
      all = false;

  if (all)
    return all;

  fragtree_t tmpdft;
  tmpdft.force_to_leaf(g_ceph_context, fg);
  for (compact_map<frag_t,CDir*>::iterator p = dirfrags.begin(); p != dirfrags.end(); ++p) {
    tmpdft.force_to_leaf(g_ceph_context, p->first);
    if (fg.contains(p->first) && !dirfragtree.is_leaf(p->first))
      ls.push_back(p->second);
  }

  all = true;
  tmpdft.get_leaves_under(fg, fglist);
  for (list<frag_t>::iterator p = fglist.begin(); p != fglist.end(); ++p)
    if (!dirfrags.count(*p)) {
      all = false;
      break;
    }

  return all;
}

void CInode::verify_dirfrags()
{
  bool bad = false;
  for (compact_map<frag_t,CDir*>::iterator p = dirfrags.begin(); p != dirfrags.end(); ++p) {
    if (!dirfragtree.is_leaf(p->first)) {
      dout(0) << "have open dirfrag " << p->first << " but not leaf in " << dirfragtree
	      << ": " << *p->second << dendl;
      bad = true;
    }
  }
  assert(!bad);
}

void CInode::force_dirfrags()
{
  bool bad = false;
  for (compact_map<frag_t,CDir*>::iterator p = dirfrags.begin(); p != dirfrags.end(); ++p) {
    if (!dirfragtree.is_leaf(p->first)) {
      dout(0) << "have open dirfrag " << p->first << " but not leaf in " << dirfragtree
	      << ": " << *p->second << dendl;
      bad = true;
    }
  }

  if (bad) {
    list<frag_t> leaves;
    dirfragtree.get_leaves(leaves);
    for (list<frag_t>::iterator p = leaves.begin(); p != leaves.end(); ++p)
      mdcache->get_force_dirfrag(dirfrag_t(ino(),*p), true);
  }

  verify_dirfrags();
}

CDir *CInode::get_approx_dirfrag(frag_t fg)
{
  CDir *dir = get_dirfrag(fg);
  if (dir) return dir;

  // find a child?
  list<CDir*> ls;
  get_dirfrags_under(fg, ls);
  if (!ls.empty()) 
    return ls.front();

  // try parents?
  while (fg.bits() > 0) {
    fg = fg.parent();
    dir = get_dirfrag(fg);
    if (dir) return dir;
  }
  return NULL;
}	

void CInode::get_dirfrags(list<CDir*>& ls) 
{
  // all dirfrags
  for (compact_map<frag_t,CDir*>::iterator p = dirfrags.begin();
       p != dirfrags.end();
       ++p)
    ls.push_back(p->second);
}
void CInode::get_nested_dirfrags(list<CDir*>& ls) 
{  
  // dirfrags in same subtree
  for (compact_map<frag_t,CDir*>::iterator p = dirfrags.begin();
       p != dirfrags.end();
       ++p)
    if (!p->second->is_subtree_root())
      ls.push_back(p->second);
}
void CInode::get_subtree_dirfrags(list<CDir*>& ls) 
{ 
  // dirfrags that are roots of new subtrees
  for (compact_map<frag_t,CDir*>::iterator p = dirfrags.begin();
       p != dirfrags.end();
       ++p)
    if (p->second->is_subtree_root())
      ls.push_back(p->second);
}


CDir *CInode::get_or_open_dirfrag(MDCache *mdcache, frag_t fg)
{
  assert(is_dir());

  // have it?
  CDir *dir = get_dirfrag(fg);
  if (!dir) {
    // create it.
    assert(is_auth() || mdcache->mds->is_any_replay());
    dir = new CDir(this, fg, mdcache, is_auth());
    add_dirfrag(dir);
  }
  return dir;
}

CDir *CInode::add_dirfrag(CDir *dir)
{
  assert(dirfrags.count(dir->dirfrag().frag) == 0);
  dirfrags[dir->dirfrag().frag] = dir;

  if (stickydir_ref > 0) {
    dir->state_set(CDir::STATE_STICKY);
    dir->get(CDir::PIN_STICKY);
  }

  return dir;
}

void CInode::close_dirfrag(frag_t fg)
{
  dout(14) << "close_dirfrag " << fg << dendl;
  assert(dirfrags.count(fg));
  
  CDir *dir = dirfrags[fg];
  dir->remove_null_dentries();
  
  // clear dirty flag
  if (dir->is_dirty())
    dir->mark_clean();
  
  if (stickydir_ref > 0) {
    dir->state_clear(CDir::STATE_STICKY);
    dir->put(CDir::PIN_STICKY);
  }
  
  // dump any remaining dentries, for debugging purposes
  for (CDir::map_t::iterator p = dir->items.begin();
       p != dir->items.end();
       ++p) 
    dout(14) << "close_dirfrag LEFTOVER dn " << *p->second << dendl;

  assert(dir->get_num_ref() == 0);
  delete dir;
  dirfrags.erase(fg);
}

void CInode::close_dirfrags()
{
  while (!dirfrags.empty()) 
    close_dirfrag(dirfrags.begin()->first);
}

bool CInode::has_subtree_root_dirfrag(int auth)
{
  for (compact_map<frag_t,CDir*>::iterator p = dirfrags.begin();
       p != dirfrags.end();
       ++p)
    if (p->second->is_subtree_root() &&
	(auth == -1 || p->second->dir_auth.first == auth))
      return true;
  return false;
}

bool CInode::has_subtree_or_exporting_dirfrag()
{
  for (compact_map<frag_t,CDir*>::iterator p = dirfrags.begin();
       p != dirfrags.end();
       ++p)
    if (p->second->is_subtree_root() ||
	p->second->state_test(CDir::STATE_EXPORTING))
      return true;
  return false;
}

void CInode::get_stickydirs()
{
  if (stickydir_ref == 0) {
    get(PIN_STICKYDIRS);
    for (compact_map<frag_t,CDir*>::iterator p = dirfrags.begin();
	 p != dirfrags.end();
	 ++p) {
      p->second->state_set(CDir::STATE_STICKY);
      p->second->get(CDir::PIN_STICKY);
    }
  }
  stickydir_ref++;
}

void CInode::put_stickydirs()
{
  assert(stickydir_ref > 0);
  stickydir_ref--;
  if (stickydir_ref == 0) {
    put(PIN_STICKYDIRS);
    for (compact_map<frag_t,CDir*>::iterator p = dirfrags.begin();
	 p != dirfrags.end();
	 ++p) {
      p->second->state_clear(CDir::STATE_STICKY);
      p->second->put(CDir::PIN_STICKY);
    }
  }
}





// pins

void CInode::first_get()
{
  // pin my dentry?
  if (parent) 
    parent->get(CDentry::PIN_INODEPIN);
}

void CInode::last_put() 
{
  // unpin my dentry?
  if (parent) 
    parent->put(CDentry::PIN_INODEPIN);
}

void CInode::_put()
{
  if (get_num_ref() == (int)is_dirty() + (int)is_dirty_parent())
    mdcache->maybe_eval_stray(this, true);
}

void CInode::add_remote_parent(CDentry *p) 
{
  if (remote_parents.empty())
    get(PIN_REMOTEPARENT);
  remote_parents.insert(p);
}
void CInode::remove_remote_parent(CDentry *p) 
{
  remote_parents.erase(p);
  if (remote_parents.empty())
    put(PIN_REMOTEPARENT);
}




CDir *CInode::get_parent_dir()
{
  if (parent)
    return parent->dir;
  return NULL;
}
CDir *CInode::get_projected_parent_dir()
{
  CDentry *p = get_projected_parent_dn();
  if (p)
    return p->dir;
  return NULL;
}
CInode *CInode::get_parent_inode() 
{
  if (parent) 
    return parent->dir->inode;
  return NULL;
}

bool CInode::is_projected_ancestor_of(CInode *other)
{
  while (other) {
    if (other == this)
      return true;
    if (!other->get_projected_parent_dn())
      break;
    other = other->get_projected_parent_dn()->get_dir()->get_inode();
  }
  return false;
}

void CInode::make_path_string(string& s, bool force, CDentry *use_parent) const
{
  if (!force)
    use_parent = parent;

  if (use_parent) {
    use_parent->make_path_string(s);
  } 
  else if (is_root()) {
    s = "";  // root
  } 
  else if (is_mdsdir()) {
    char t[40];
    uint64_t eino(ino());
    eino -= MDS_INO_MDSDIR_OFFSET;
    snprintf(t, sizeof(t), "~mds%" PRId64, eino);
    s = t;
  }
  else {
    char n[40];
    uint64_t eino(ino());
    snprintf(n, sizeof(n), "#%" PRIx64, eino);
    s += n;
  }
}
void CInode::make_path_string_projected(string& s) const
{
  make_path_string(s);
  
  if (!projected_parent.empty()) {
    string q;
    q.swap(s);
    s = "{" + q;
    for (list<CDentry*>::const_iterator p = projected_parent.begin();
	 p != projected_parent.end();
	 ++p) {
      string q;
      make_path_string(q, true, *p);
      s += " ";
      s += q;
    }
    s += "}";
  }
}

void CInode::make_path(filepath& fp) const
{
  if (parent) 
    parent->make_path(fp);
  else
    fp = filepath(ino());
}

void CInode::name_stray_dentry(string& dname)
{
  char s[20];
  snprintf(s, sizeof(s), "%llx", (unsigned long long)inode.ino.val);
  dname = s;
}

version_t CInode::pre_dirty()
{
  version_t pv;
  CDentry* _cdentry = get_projected_parent_dn(); 
  if (_cdentry) {
    pv = _cdentry->pre_dirty(get_projected_version());
    dout(10) << "pre_dirty " << pv << " (current v " << inode.version << ")" << dendl;
  } else {
    assert(is_base());
    pv = get_projected_version() + 1;
  }
  // force update backtrace for old format inode (see inode_t::decode)
  if (inode.backtrace_version == 0 && !projected_nodes.empty()) {
    inode_t *pi = projected_nodes.back()->inode;
    if (pi->backtrace_version == 0)
      pi->update_backtrace(pv);
  }
  return pv;
}

void CInode::_mark_dirty(LogSegment *ls)
{
  if (!state_test(STATE_DIRTY)) {
    state_set(STATE_DIRTY);
    get(PIN_DIRTY);
    assert(ls);
  }
  
  // move myself to this segment's dirty list
  if (ls) 
    ls->dirty_inodes.push_back(&item_dirty);
}

void CInode::mark_dirty(version_t pv, LogSegment *ls) {
  
  dout(10) << "mark_dirty " << *this << dendl;

  /*
    NOTE: I may already be dirty, but this fn _still_ needs to be called so that
    the directory is (perhaps newly) dirtied, and so that parent_dir_version is 
    updated below.
  */
  
  // only auth can get dirty.  "dirty" async data in replicas is relative to
  // filelock state, not the dirty flag.
  assert(is_auth());
  
  // touch my private version
  assert(inode.version < pv);
  inode.version = pv;
  _mark_dirty(ls);

  // mark dentry too
  if (parent)
    parent->mark_dirty(pv, ls);
}


void CInode::mark_clean()
{
  dout(10) << " mark_clean " << *this << dendl;
  if (state_test(STATE_DIRTY)) {
    state_clear(STATE_DIRTY);
    put(PIN_DIRTY);
    
    // remove myself from ls dirty list
    item_dirty.remove_myself();
  }
}    


// --------------
// per-inode storage
// (currently for root inode only)

struct C_IO_Inode_Stored : public CInodeIOContext {
  version_t version;
  Context *fin;
  C_IO_Inode_Stored(CInode *i, version_t v, Context *f) : CInodeIOContext(i), version(v), fin(f) {}
  void finish(int r) {
    in->_stored(r, version, fin);
  }
};

object_t InodeStoreBase::get_object_name(inodeno_t ino, frag_t fg, const char *suffix)
{
  char n[60];
  snprintf(n, sizeof(n), "%llx.%08llx%s", (long long unsigned)ino, (long long unsigned)fg, suffix ? suffix : "");
  return object_t(n);
}

void CInode::store(MDSInternalContextBase *fin)
{
  dout(10) << "store " << get_version() << dendl;
  assert(is_base());

  if (snaprealm)
    purge_stale_snap_data(snaprealm->get_snaps());

  // encode
  bufferlist bl;
  string magic = CEPH_FS_ONDISK_MAGIC;
  ::encode(magic, bl);
  encode_store(bl);

  // write it.
  SnapContext snapc;
  ObjectOperation m;
  m.write_full(bl);

  object_t oid = CInode::get_object_name(ino(), frag_t(), ".inode");
  object_locator_t oloc(mdcache->mds->mdsmap->get_metadata_pool());

  Context *newfin =
    new C_OnFinisher(new C_IO_Inode_Stored(this, get_version(), fin),
		     &mdcache->mds->finisher);
  mdcache->mds->objecter->mutate(oid, oloc, m, snapc,
				 ceph_clock_now(g_ceph_context), 0,
				 NULL, newfin);
}

void CInode::_stored(int r, version_t v, Context *fin)
{
  if (r < 0) {
    dout(1) << "store error " << r << " v " << v << " on " << *this << dendl;
    mdcache->mds->clog->error() << "failed to store ino " << ino() << " object,"
				<< " errno " << r << "\n";
    mdcache->mds->handle_write_error(r);
    return;
  }

  dout(10) << "_stored " << v << " on " << *this << dendl;
  if (v == get_projected_version())
    mark_clean();

  fin->complete(0);
}

void CInode::flush(MDSInternalContextBase *fin)
{
  dout(10) << "flush " << *this << dendl;
  assert(is_auth() && can_auth_pin());

  MDSGatherBuilder gather(g_ceph_context);

  if (is_dirty_parent()) {
    store_backtrace(gather.new_sub());
  }
  if (is_dirty()) {
    if (is_base()) {
      store(gather.new_sub());
    } else {
      parent->dir->commit(0, gather.new_sub());
    }
  }

  if (gather.has_subs()) {
    gather.set_finisher(fin);
    gather.activate();
  } else {
    fin->complete(0);
  }
}

struct C_IO_Inode_Fetched : public CInodeIOContext {
  bufferlist bl, bl2;
  Context *fin;
  C_IO_Inode_Fetched(CInode *i, Context *f) : CInodeIOContext(i), fin(f) {}
  void finish(int r) {
    // Ignore 'r', because we fetch from two places, so r is usually ENOENT
    in->_fetched(bl, bl2, fin);
  }
};

void CInode::fetch(MDSInternalContextBase *fin)
{
  dout(10) << "fetch" << dendl;

  C_IO_Inode_Fetched *c = new C_IO_Inode_Fetched(this, fin);
  C_GatherBuilder gather(g_ceph_context, new C_OnFinisher(c, &mdcache->mds->finisher));

  object_t oid = CInode::get_object_name(ino(), frag_t(), "");
  object_locator_t oloc(mdcache->mds->mdsmap->get_metadata_pool());

  // Old on-disk format: inode stored in xattr of a dirfrag
  ObjectOperation rd;
  rd.getxattr("inode", &c->bl, NULL);
  mdcache->mds->objecter->read(oid, oloc, rd, CEPH_NOSNAP, (bufferlist*)NULL, 0, gather.new_sub());

  // Current on-disk format: inode stored in a .inode object
  object_t oid2 = CInode::get_object_name(ino(), frag_t(), ".inode");
  mdcache->mds->objecter->read(oid2, oloc, 0, 0, CEPH_NOSNAP, &c->bl2, 0, gather.new_sub());

  gather.activate();
}

void CInode::_fetched(bufferlist& bl, bufferlist& bl2, Context *fin)
{
  dout(10) << "_fetched got " << bl.length() << " and " << bl2.length() << dendl;
  bufferlist::iterator p;
  if (bl2.length()) {
    p = bl2.begin();
  } else if (bl.length()) {
    p = bl.begin();
  } else {
    derr << "No data while reading inode 0x" << std::hex << ino()
      << std::dec << dendl;
    fin->complete(-ENOENT);
    return;
  }

  // Attempt decode
  try {
    string magic;
    ::decode(magic, p);
    dout(10) << " magic is '" << magic << "' (expecting '"
             << CEPH_FS_ONDISK_MAGIC << "')" << dendl;
    if (magic != CEPH_FS_ONDISK_MAGIC) {
      dout(0) << "on disk magic '" << magic << "' != my magic '" << CEPH_FS_ONDISK_MAGIC
              << "'" << dendl;
      fin->complete(-EINVAL);
    } else {
      decode_store(p);
      dout(10) << "_fetched " << *this << dendl;
      fin->complete(0);
    }
  } catch (buffer::error &err) {
    derr << "Corrupt inode 0x" << std::hex << ino() << std::dec
      << ": " << err << dendl;
    fin->complete(-EINVAL);
    return;
  }
}

void CInode::build_backtrace(int64_t pool, inode_backtrace_t& bt)
{
  bt.ino = inode.ino;
  bt.ancestors.clear();
  bt.pool = pool;

  CInode *in = this;
  CDentry *pdn = get_parent_dn();
  while (pdn) {
    CInode *diri = pdn->get_dir()->get_inode();
    bt.ancestors.push_back(inode_backpointer_t(diri->ino(), pdn->name, in->inode.version));
    in = diri;
    pdn = in->get_parent_dn();
  }
  for (compact_set<int64_t>::iterator i = inode.old_pools.begin();
       i != inode.old_pools.end();
       ++i) {
    // don't add our own pool id to old_pools to avoid looping (e.g. setlayout 0, 1, 0)
    if (*i != pool)
      bt.old_pools.insert(*i);
  }
}

struct C_IO_Inode_StoredBacktrace : public CInodeIOContext {
  version_t version;
  Context *fin;
  C_IO_Inode_StoredBacktrace(CInode *i, version_t v, Context *f) : CInodeIOContext(i), version(v), fin(f) {}
  void finish(int r) {
    in->_stored_backtrace(r, version, fin);
  }
};

void CInode::store_backtrace(MDSInternalContextBase *fin, int op_prio)
{
  dout(10) << "store_backtrace on " << *this << dendl;
  assert(is_dirty_parent());

  if (op_prio < 0)
    op_prio = CEPH_MSG_PRIO_DEFAULT;

  auth_pin(this);

  int64_t pool;
  if (is_dir())
    pool = mdcache->mds->mdsmap->get_metadata_pool();
  else
    pool = inode.layout.fl_pg_pool;

  inode_backtrace_t bt;
  build_backtrace(pool, bt);
  bufferlist bl;
  ::encode(bt, bl);

  ObjectOperation op;
  op.priority = op_prio;
  op.create(false);
  op.setxattr("parent", bl);

  SnapContext snapc;
  object_t oid = get_object_name(ino(), frag_t(), "");
  object_locator_t oloc(pool);
  Context *fin2 = new C_OnFinisher(
    new C_IO_Inode_StoredBacktrace(this, inode.backtrace_version, fin),
    &mdcache->mds->finisher);

  if (!state_test(STATE_DIRTYPOOL) || inode.old_pools.empty()) {
    mdcache->mds->objecter->mutate(oid, oloc, op, snapc, ceph_clock_now(g_ceph_context),
				   0, NULL, fin2);
    return;
  }

  C_GatherBuilder gather(g_ceph_context, fin2);
  mdcache->mds->objecter->mutate(oid, oloc, op, snapc, ceph_clock_now(g_ceph_context),
				 0, NULL, gather.new_sub());

  for (compact_set<int64_t>::iterator p = inode.old_pools.begin();
       p != inode.old_pools.end();
       ++p) {
    if (*p == pool)
      continue;

    ObjectOperation op;
    op.priority = op_prio;
    op.create(false);
    op.setxattr("parent", bl);

    object_locator_t oloc(*p);
    mdcache->mds->objecter->mutate(oid, oloc, op, snapc, ceph_clock_now(g_ceph_context),
				   0, NULL, gather.new_sub());
  }
  gather.activate();
}

void CInode::_stored_backtrace(int r, version_t v, Context *fin)
{
  if (r < 0) {
    dout(1) << "store backtrace error " << r << " v " << v << dendl;
    mdcache->mds->clog->error() << "failed to store backtrace on dir ino "
				<< ino() << " object, errno " << r << "\n";
    mdcache->mds->handle_write_error(r);
    return;
  }

  dout(10) << "_stored_backtrace v " << v <<  dendl;

  auth_unpin(this);
  if (v == inode.backtrace_version)
    clear_dirty_parent();
  if (fin)
    fin->complete(0);
}

void CInode::fetch_backtrace(Context *fin, bufferlist *backtrace)
{
  int64_t pool;
  if (is_dir())
    pool = mdcache->mds->mdsmap->get_metadata_pool();
  else
    pool = inode.layout.fl_pg_pool;

  mdcache->fetch_backtrace(inode.ino, pool, *backtrace, fin);
}

void CInode::_mark_dirty_parent(LogSegment *ls, bool dirty_pool)
{
  if (!state_test(STATE_DIRTYPARENT)) {
    dout(10) << "mark_dirty_parent" << dendl;
    state_set(STATE_DIRTYPARENT);
    get(PIN_DIRTYPARENT);
    assert(ls);
  }
  if (dirty_pool)
    state_set(STATE_DIRTYPOOL);
  if (ls)
    ls->dirty_parent_inodes.push_back(&item_dirty_parent);
}

void CInode::clear_dirty_parent()
{
  if (state_test(STATE_DIRTYPARENT)) {
    dout(10) << "clear_dirty_parent" << dendl;
    state_clear(STATE_DIRTYPARENT);
    state_clear(STATE_DIRTYPOOL);
    put(PIN_DIRTYPARENT);
    item_dirty_parent.remove_myself();
  }
}

void CInode::verify_diri_backtrace(bufferlist &bl, int err)
{
  if (is_base() || is_dirty_parent() || !is_auth())
    return;

  dout(10) << "verify_diri_backtrace" << dendl;

  if (err == 0) {
    inode_backtrace_t backtrace;
    ::decode(backtrace, bl);
    CDentry *pdn = get_parent_dn();
    if (backtrace.ancestors.empty() ||
	backtrace.ancestors[0].dname != pdn->name ||
	backtrace.ancestors[0].dirino != pdn->get_dir()->ino())
      err = -EINVAL;
  }

  if (err) {
    MDS *mds = mdcache->mds;
    mds->clog->error() << "bad backtrace on dir ino " << ino() << "\n";
    assert(!"bad backtrace" == (g_conf->mds_verify_backtrace > 1));

    _mark_dirty_parent(mds->mdlog->get_current_segment(), false);
    mds->mdlog->flush();
  }
}

// ------------------
// parent dir


void InodeStoreBase::encode_bare(bufferlist &bl, const bufferlist *snap_blob) const
{
  ::encode(inode, bl);
  if (is_symlink())
    ::encode(symlink, bl);
  ::encode(dirfragtree, bl);
  ::encode(xattrs, bl);
  if (snap_blob)
    ::encode(*snap_blob, bl);
  else
    ::encode(bufferlist(), bl);
  ::encode(old_inodes, bl);
  ::encode(oldest_snap, bl);
}

void InodeStoreBase::encode(bufferlist &bl, const bufferlist *snap_blob) const
{
  ENCODE_START(5, 4, bl);
  encode_bare(bl, snap_blob);
  ENCODE_FINISH(bl);
}

void CInode::encode_store(bufferlist& bl)
{
  bufferlist snap_blob;
  encode_snap_blob(snap_blob);
  InodeStoreBase::encode(bl, &snap_blob);
}

void InodeStoreBase::decode_bare(bufferlist::iterator &bl,
			      bufferlist& snap_blob, __u8 struct_v)
{
  ::decode(inode, bl);
  if (is_symlink())
    ::decode(symlink, bl);
  ::decode(dirfragtree, bl);
  ::decode(xattrs, bl);
  ::decode(snap_blob, bl);

  ::decode(old_inodes, bl);
  if (struct_v == 2 && inode.is_dir()) {
    bool default_layout_exists;
    ::decode(default_layout_exists, bl);
    if (default_layout_exists) {
      ::decode(struct_v, bl); // this was a default_file_layout
      ::decode(inode.layout, bl); // but we only care about the layout portion
    }
  }
  if (struct_v >= 5 && !bl.end())
    ::decode(oldest_snap, bl);
}


void InodeStoreBase::decode(bufferlist::iterator &bl, bufferlist& snap_blob)
{
  DECODE_START_LEGACY_COMPAT_LEN(5, 4, 4, bl);
  decode_bare(bl, snap_blob, struct_v);
  DECODE_FINISH(bl);
}

void CInode::decode_store(bufferlist::iterator& bl)
{
  bufferlist snap_blob;
  InodeStoreBase::decode(bl, snap_blob);
  decode_snap_blob(snap_blob);
}

// ------------------
// locking

void CInode::set_object_info(MDSCacheObjectInfo &info)
{
  info.ino = ino();
  info.snapid = last;
}

void CInode::encode_lock_state(int type, bufferlist& bl)
{
  ::encode(first, bl);

  switch (type) {
  case CEPH_LOCK_IAUTH:
    ::encode(inode.version, bl);
    ::encode(inode.ctime, bl);
    ::encode(inode.mode, bl);
    ::encode(inode.uid, bl);
    ::encode(inode.gid, bl);  
    break;
    
  case CEPH_LOCK_ILINK:
    ::encode(inode.version, bl);
    ::encode(inode.ctime, bl);
    ::encode(inode.nlink, bl);
    break;
    
  case CEPH_LOCK_IDFT:
    if (is_auth()) {
      ::encode(inode.version, bl);
    } else {
      // treat flushing as dirty when rejoining cache
      bool dirty = dirfragtreelock.is_dirty_or_flushing();
      ::encode(dirty, bl);
    }
    {
      // encode the raw tree
      ::encode(dirfragtree, bl);

      // also specify which frags are mine
      set<frag_t> myfrags;
      list<CDir*> dfls;
      get_dirfrags(dfls);
      for (list<CDir*>::iterator p = dfls.begin(); p != dfls.end(); ++p) 
	if ((*p)->is_auth()) {
	  frag_t fg = (*p)->get_frag();
	  myfrags.insert(fg);
	}
      ::encode(myfrags, bl);
    }
    break;
    
  case CEPH_LOCK_IFILE:
    if (is_auth()) {
      ::encode(inode.version, bl);
      ::encode(inode.mtime, bl);
      ::encode(inode.atime, bl);
      ::encode(inode.time_warp_seq, bl);
      if (!is_dir()) {
	::encode(inode.layout, bl);
	::encode(inode.size, bl);
	::encode(inode.truncate_seq, bl);
	::encode(inode.truncate_size, bl);
	::encode(inode.client_ranges, bl);
	::encode(inode.inline_data, bl);
      }
    } else {
      // treat flushing as dirty when rejoining cache
      bool dirty = filelock.is_dirty_or_flushing();
      ::encode(dirty, bl);
    }

    {
      dout(15) << "encode_lock_state inode.dirstat is " << inode.dirstat << dendl;
      ::encode(inode.dirstat, bl);  // only meaningful if i am auth.
      bufferlist tmp;
      __u32 n = 0;
      for (compact_map<frag_t,CDir*>::iterator p = dirfrags.begin();
	   p != dirfrags.end();
	   ++p) {
	frag_t fg = p->first;
	CDir *dir = p->second;
	if (is_auth() || dir->is_auth()) {
	  fnode_t *pf = dir->get_projected_fnode();
	  dout(15) << fg << " " << *dir << dendl;
	  dout(20) << fg << "           fragstat " << pf->fragstat << dendl;
	  dout(20) << fg << " accounted_fragstat " << pf->accounted_fragstat << dendl;
	  ::encode(fg, tmp);
	  ::encode(dir->first, tmp);
	  ::encode(pf->fragstat, tmp);
	  ::encode(pf->accounted_fragstat, tmp);
	  n++;
	}
      }
      ::encode(n, bl);
      bl.claim_append(tmp);
    }
    break;

  case CEPH_LOCK_INEST:
    if (is_auth()) {
      ::encode(inode.version, bl);
    } else {
      // treat flushing as dirty when rejoining cache
      bool dirty = nestlock.is_dirty_or_flushing();
      ::encode(dirty, bl);
    }
    {
      dout(15) << "encode_lock_state inode.rstat is " << inode.rstat << dendl;
      ::encode(inode.rstat, bl);  // only meaningful if i am auth.
      bufferlist tmp;
      __u32 n = 0;
      for (compact_map<frag_t,CDir*>::iterator p = dirfrags.begin();
	   p != dirfrags.end();
	   ++p) {
	frag_t fg = p->first;
	CDir *dir = p->second;
	if (is_auth() || dir->is_auth()) {
	  fnode_t *pf = dir->get_projected_fnode();
	  dout(10) << fg << " " << *dir << dendl;
	  dout(10) << fg << " " << pf->rstat << dendl;
	  dout(10) << fg << " " << pf->rstat << dendl;
	  dout(10) << fg << " " << dir->dirty_old_rstat << dendl;
	  ::encode(fg, tmp);
	  ::encode(dir->first, tmp);
	  ::encode(pf->rstat, tmp);
	  ::encode(pf->accounted_rstat, tmp);
	  ::encode(dir->dirty_old_rstat, tmp);
	  n++;
	}
      }
      ::encode(n, bl);
      bl.claim_append(tmp);
    }
    break;
    
  case CEPH_LOCK_IXATTR:
    ::encode(inode.version, bl);
    ::encode(xattrs, bl);
    break;

  case CEPH_LOCK_ISNAP:
    ::encode(inode.version, bl);
    encode_snap(bl);
    break;

  case CEPH_LOCK_IFLOCK:
    ::encode(inode.version, bl);
    _encode_file_locks(bl);
    break;

  case CEPH_LOCK_IPOLICY:
    if (inode.is_dir()) {
      ::encode(inode.version, bl);
      ::encode(inode.layout, bl);
      ::encode(inode.quota, bl);
    }
    break;
  
  default:
    assert(0);
  }
}


/* for more info on scatterlocks, see comments by Locker::scatter_writebehind */

void CInode::decode_lock_state(int type, bufferlist& bl)
{
  bufferlist::iterator p = bl.begin();
  utime_t tm;

  snapid_t newfirst;
  ::decode(newfirst, p);

  if (!is_auth() && newfirst != first) {
    dout(10) << "decode_lock_state first " << first << " -> " << newfirst << dendl;
    assert(newfirst > first);
    if (!is_multiversion() && parent) {
      assert(parent->first == first);
      parent->first = newfirst;
    }
    first = newfirst;
  }

  switch (type) {
  case CEPH_LOCK_IAUTH:
    ::decode(inode.version, p);
    ::decode(tm, p);
    if (inode.ctime < tm) inode.ctime = tm;
    ::decode(inode.mode, p);
    ::decode(inode.uid, p);
    ::decode(inode.gid, p);
    break;

  case CEPH_LOCK_ILINK:
    ::decode(inode.version, p);
    ::decode(tm, p);
    if (inode.ctime < tm) inode.ctime = tm;
    ::decode(inode.nlink, p);
    break;

  case CEPH_LOCK_IDFT:
    if (is_auth()) {
      bool replica_dirty;
      ::decode(replica_dirty, p);
      if (replica_dirty) {
	dout(10) << "decode_lock_state setting dftlock dirty flag" << dendl;
	dirfragtreelock.mark_dirty();  // ok bc we're auth and caller will handle
      }
    } else {
      ::decode(inode.version, p);
    }
    {
      fragtree_t temp;
      ::decode(temp, p);
      set<frag_t> authfrags;
      ::decode(authfrags, p);
      if (is_auth()) {
	// auth.  believe replica's auth frags only.
	for (set<frag_t>::iterator p = authfrags.begin(); p != authfrags.end(); ++p)
	  if (!dirfragtree.is_leaf(*p)) {
	    dout(10) << " forcing frag " << *p << " to leaf (split|merge)" << dendl;
	    dirfragtree.force_to_leaf(g_ceph_context, *p);
	    dirfragtreelock.mark_dirty();  // ok bc we're auth and caller will handle
	  }
      } else {
	// replica.  take the new tree, BUT make sure any open
	//  dirfrags remain leaves (they may have split _after_ this
	//  dft was scattered, or we may still be be waiting on the
	//  notify from the auth)
	dirfragtree.swap(temp);
	for (compact_map<frag_t,CDir*>::iterator p = dirfrags.begin();
	     p != dirfrags.end();
	     ++p) {
	  if (!dirfragtree.is_leaf(p->first)) {
	    dout(10) << " forcing open dirfrag " << p->first << " to leaf (racing with split|merge)" << dendl;
	    dirfragtree.force_to_leaf(g_ceph_context, p->first);
	  }
	  if (p->second->is_auth())
	    p->second->state_clear(CDir::STATE_DIRTYDFT);
	}
      }
      if (g_conf->mds_debug_frag)
	verify_dirfrags();
    }
    break;

  case CEPH_LOCK_IFILE:
    if (!is_auth()) {
      ::decode(inode.version, p);
      ::decode(inode.mtime, p);
      ::decode(inode.atime, p);
      ::decode(inode.time_warp_seq, p);
      if (!is_dir()) {
	::decode(inode.layout, p);
	::decode(inode.size, p);
	::decode(inode.truncate_seq, p);
	::decode(inode.truncate_size, p);
	::decode(inode.client_ranges, p);
	::decode(inode.inline_data, p);
      }
    } else {
      bool replica_dirty;
      ::decode(replica_dirty, p);
      if (replica_dirty) {
	dout(10) << "decode_lock_state setting filelock dirty flag" << dendl;
	filelock.mark_dirty();  // ok bc we're auth and caller will handle
      }
    }
    {
      frag_info_t dirstat;
      ::decode(dirstat, p);
      if (!is_auth()) {
	dout(10) << " taking inode dirstat " << dirstat << " for " << *this << dendl;
	inode.dirstat = dirstat;    // take inode summation if replica
      }
      __u32 n;
      ::decode(n, p);
      dout(10) << " ...got " << n << " fragstats on " << *this << dendl;
      while (n--) {
	frag_t fg;
	snapid_t fgfirst;
	frag_info_t fragstat;
	frag_info_t accounted_fragstat;
	::decode(fg, p);
	::decode(fgfirst, p);
	::decode(fragstat, p);
	::decode(accounted_fragstat, p);
	dout(10) << fg << " [" << fgfirst << ",head] " << dendl;
	dout(10) << fg << "           fragstat " << fragstat << dendl;
	dout(20) << fg << " accounted_fragstat " << accounted_fragstat << dendl;

	CDir *dir = get_dirfrag(fg);
	if (is_auth()) {
	  assert(dir);                // i am auth; i had better have this dir open
	  dout(10) << fg << " first " << dir->first << " -> " << fgfirst
		   << " on " << *dir << dendl;
	  dir->first = fgfirst;
	  dir->fnode.fragstat = fragstat;
	  dir->fnode.accounted_fragstat = accounted_fragstat;
	  dir->first = fgfirst;
	  if (!(fragstat == accounted_fragstat)) {
	    dout(10) << fg << " setting filelock updated flag" << dendl;
	    filelock.mark_dirty();  // ok bc we're auth and caller will handle
	  }
	} else {
	  if (dir && dir->is_auth()) {
	    dout(10) << fg << " first " << dir->first << " -> " << fgfirst
		     << " on " << *dir << dendl;
	    dir->first = fgfirst;
	    fnode_t *pf = dir->get_projected_fnode();
	    finish_scatter_update(&filelock, dir,
				  inode.dirstat.version, pf->accounted_fragstat.version);
	  }
	}
      }
    }
    break;

  case CEPH_LOCK_INEST:
    if (is_auth()) {
      bool replica_dirty;
      ::decode(replica_dirty, p);
      if (replica_dirty) {
	dout(10) << "decode_lock_state setting nestlock dirty flag" << dendl;
	nestlock.mark_dirty();  // ok bc we're auth and caller will handle
      }
    } else {
      ::decode(inode.version, p);
    }
    {
      nest_info_t rstat;
      ::decode(rstat, p);
      if (!is_auth()) {
	dout(10) << " taking inode rstat " << rstat << " for " << *this << dendl;
	inode.rstat = rstat;    // take inode summation if replica
      }
      __u32 n;
      ::decode(n, p);
      while (n--) {
	frag_t fg;
	snapid_t fgfirst;
	nest_info_t rstat;
	nest_info_t accounted_rstat;
	compact_map<snapid_t,old_rstat_t> dirty_old_rstat;
	::decode(fg, p);
	::decode(fgfirst, p);
	::decode(rstat, p);
	::decode(accounted_rstat, p);
	::decode(dirty_old_rstat, p);
	dout(10) << fg << " [" << fgfirst << ",head]" << dendl;
	dout(10) << fg << "               rstat " << rstat << dendl;
	dout(10) << fg << "     accounted_rstat " << accounted_rstat << dendl;
	dout(10) << fg << "     dirty_old_rstat " << dirty_old_rstat << dendl;

	CDir *dir = get_dirfrag(fg);
	if (is_auth()) {
	  assert(dir);                // i am auth; i had better have this dir open
	  dout(10) << fg << " first " << dir->first << " -> " << fgfirst
		   << " on " << *dir << dendl;
	  dir->first = fgfirst;
	  dir->fnode.rstat = rstat;
	  dir->fnode.accounted_rstat = accounted_rstat;
	  dir->dirty_old_rstat.swap(dirty_old_rstat);
	  if (!(rstat == accounted_rstat) || !dir->dirty_old_rstat.empty()) {
	    dout(10) << fg << " setting nestlock updated flag" << dendl;
	    nestlock.mark_dirty();  // ok bc we're auth and caller will handle
	  }
	} else {
	  if (dir && dir->is_auth()) {
	    dout(10) << fg << " first " << dir->first << " -> " << fgfirst
		     << " on " << *dir << dendl;
	    dir->first = fgfirst;
	    fnode_t *pf = dir->get_projected_fnode();
	    finish_scatter_update(&nestlock, dir,
				  inode.rstat.version, pf->accounted_rstat.version);
	  }
	}
      }
    }
    break;

  case CEPH_LOCK_IXATTR:
    ::decode(inode.version, p);
    ::decode(xattrs, p);
    break;

  case CEPH_LOCK_ISNAP:
    {
      ::decode(inode.version, p);
      snapid_t seq = 0;
      if (snaprealm)
	seq = snaprealm->srnode.seq;
      decode_snap(p);
      if (snaprealm && snaprealm->srnode.seq != seq)
	mdcache->do_realm_invalidate_and_update_notify(this, seq ? CEPH_SNAP_OP_UPDATE:CEPH_SNAP_OP_SPLIT);
    }
    break;

  case CEPH_LOCK_IFLOCK:
    ::decode(inode.version, p);
    _decode_file_locks(p);
    break;

  case CEPH_LOCK_IPOLICY:
    if (inode.is_dir()) {
      ::decode(inode.version, p);
      ::decode(inode.layout, p);
      ::decode(inode.quota, p);
    }
    break;

  default:
    assert(0);
  }
}


bool CInode::is_dirty_scattered()
{
  return
    filelock.is_dirty_or_flushing() ||
    nestlock.is_dirty_or_flushing() ||
    dirfragtreelock.is_dirty_or_flushing();
}

void CInode::clear_scatter_dirty()
{
  filelock.remove_dirty();
  nestlock.remove_dirty();
  dirfragtreelock.remove_dirty();
}

void CInode::clear_dirty_scattered(int type)
{
  dout(10) << "clear_dirty_scattered " << type << " on " << *this << dendl;
  switch (type) {
  case CEPH_LOCK_IFILE:
    item_dirty_dirfrag_dir.remove_myself();
    break;

  case CEPH_LOCK_INEST:
    item_dirty_dirfrag_nest.remove_myself();
    break;

  case CEPH_LOCK_IDFT:
    item_dirty_dirfrag_dirfragtree.remove_myself();
    break;

  default:
    assert(0);
  }
}


/*
 * when we initially scatter a lock, we need to check if any of the dirfrags
 * have out of date accounted_rstat/fragstat.  if so, mark the lock stale.
 */
/* for more info on scatterlocks, see comments by Locker::scatter_writebehind */
void CInode::start_scatter(ScatterLock *lock)
{
  dout(10) << "start_scatter " << *lock << " on " << *this << dendl;
  assert(is_auth());
  inode_t *pi = get_projected_inode();

  for (compact_map<frag_t,CDir*>::iterator p = dirfrags.begin();
       p != dirfrags.end();
       ++p) {
    frag_t fg = p->first;
    CDir *dir = p->second;
    fnode_t *pf = dir->get_projected_fnode();
    dout(20) << fg << " " << *dir << dendl;

    if (!dir->is_auth())
      continue;

    switch (lock->get_type()) {
    case CEPH_LOCK_IFILE:
      finish_scatter_update(lock, dir, pi->dirstat.version, pf->accounted_fragstat.version);
      break;

    case CEPH_LOCK_INEST:
      finish_scatter_update(lock, dir, pi->rstat.version, pf->accounted_rstat.version);
      break;

    case CEPH_LOCK_IDFT:
      dir->state_clear(CDir::STATE_DIRTYDFT);
      break;
    }
  }
}


class C_Inode_FragUpdate : public MDSInternalContextBase {
protected:
  CInode *in;
  CDir *dir;
  MutationRef mut;
  MDS *get_mds() {return in->mdcache->mds;}
  void finish(int r) {
    in->_finish_frag_update(dir, mut);
  }    

public:
  C_Inode_FragUpdate(CInode *i, CDir *d, MutationRef& m) : in(i), dir(d), mut(m) {}
};

void CInode::finish_scatter_update(ScatterLock *lock, CDir *dir,
				   version_t inode_version, version_t dir_accounted_version)
{
  frag_t fg = dir->get_frag();
  assert(dir->is_auth());

  if (dir->is_frozen()) {
    dout(10) << "finish_scatter_update " << fg << " frozen, marking " << *lock << " stale " << *dir << dendl;
  } else if (dir->get_version() == 0) {
    dout(10) << "finish_scatter_update " << fg << " not loaded, marking " << *lock << " stale " << *dir << dendl;
  } else {
    if (dir_accounted_version != inode_version) {
      dout(10) << "finish_scatter_update " << fg << " journaling accounted scatterstat update v" << inode_version << dendl;

      MDLog *mdlog = mdcache->mds->mdlog;
      MutationRef mut(new MutationImpl);
      mut->ls = mdlog->get_current_segment();

      inode_t *pi = get_projected_inode();
      fnode_t *pf = dir->project_fnode();
      pf->version = dir->pre_dirty();

      const char *ename = 0;
      switch (lock->get_type()) {
      case CEPH_LOCK_IFILE:
	pf->fragstat.version = pi->dirstat.version;
	pf->accounted_fragstat = pf->fragstat;
	ename = "lock ifile accounted scatter stat update";
	break;
      case CEPH_LOCK_INEST:
	pf->rstat.version = pi->rstat.version;
	pf->accounted_rstat = pf->rstat;
	ename = "lock inest accounted scatter stat update";
	break;
      default:
	assert(0);
      }
	
      mut->add_projected_fnode(dir);

      EUpdate *le = new EUpdate(mdlog, ename);
      mdlog->start_entry(le);
      le->metablob.add_dir_context(dir);
      le->metablob.add_dir(dir, true);
      
      assert(!dir->is_frozen());
      mut->auth_pin(dir);
      
      mdlog->submit_entry(le, new C_Inode_FragUpdate(this, dir, mut));
    } else {
      dout(10) << "finish_scatter_update " << fg << " accounted " << *lock
	       << " scatter stat unchanged at v" << dir_accounted_version << dendl;
    }
  }
}

void CInode::_finish_frag_update(CDir *dir, MutationRef& mut)
{
  dout(10) << "_finish_frag_update on " << *dir << dendl;
  mut->apply();
  mut->cleanup();
}


/*
 * when we gather a lock, we need to assimilate dirfrag changes into the inode
 * state.  it's possible we can't update the dirfrag accounted_rstat/fragstat
 * because the frag is auth and frozen, or that the replica couldn't for the same
 * reason.  hopefully it will get updated the next time the lock cycles.
 *
 * we have two dimensions of behavior:
 *  - we may be (auth and !frozen), and able to update, or not.
 *  - the frag may be stale, or not.
 *
 * if the frag is non-stale, we want to assimilate the diff into the
 * inode, regardless of whether it's auth or updateable.
 *
 * if we update the frag, we want to set accounted_fragstat = frag,
 * both if we took the diff or it was stale and we are making it
 * un-stale.
 */
/* for more info on scatterlocks, see comments by Locker::scatter_writebehind */
void CInode::finish_scatter_gather_update(int type)
{
  LogChannelRef clog = mdcache->mds->clog;

  dout(10) << "finish_scatter_gather_update " << type << " on " << *this << dendl;
  assert(is_auth());

  switch (type) {
  case CEPH_LOCK_IFILE:
    {
      fragtree_t tmpdft = dirfragtree;
      struct frag_info_t dirstat;

      // adjust summation
      assert(is_auth());
      inode_t *pi = get_projected_inode();

      bool touched_mtime = false;
      dout(20) << "  orig dirstat " << pi->dirstat << dendl;
      pi->dirstat.version++;
      for (compact_map<frag_t,CDir*>::iterator p = dirfrags.begin();
	   p != dirfrags.end();
	   ++p) {
	frag_t fg = p->first;
	CDir *dir = p->second;
	dout(20) << fg << " " << *dir << dendl;

	bool update = dir->is_auth() && dir->get_version() != 0 &&  !dir->is_frozen();

	fnode_t *pf = dir->get_projected_fnode();
	if (update)
	  pf = dir->project_fnode();

	if (pf->accounted_fragstat.version == pi->dirstat.version - 1) {
	  dout(20) << fg << "           fragstat " << pf->fragstat << dendl;
	  dout(20) << fg << " accounted_fragstat " << pf->accounted_fragstat << dendl;
	  pi->dirstat.add_delta(pf->fragstat, pf->accounted_fragstat, touched_mtime);
	} else {
	  dout(20) << fg << " skipping STALE accounted_fragstat " << pf->accounted_fragstat << dendl;
	}

	if (pf->fragstat.nfiles < 0 ||
	    pf->fragstat.nsubdirs < 0) {
	  clog->error() << "bad/negative dir size on "
	      << dir->dirfrag() << " " << pf->fragstat << "\n";
	  assert(!"bad/negative fragstat" == g_conf->mds_verify_scatter);
	  
	  if (pf->fragstat.nfiles < 0)
	    pf->fragstat.nfiles = 0;
	  if (pf->fragstat.nsubdirs < 0)
	    pf->fragstat.nsubdirs = 0;
	}

	if (update) {
	  pf->accounted_fragstat = pf->fragstat;
	  pf->fragstat.version = pf->accounted_fragstat.version = pi->dirstat.version;
	  dout(10) << fg << " updated accounted_fragstat " << pf->fragstat << " on " << *dir << dendl;
	}

	tmpdft.force_to_leaf(g_ceph_context, fg);
	dirstat.add(pf->fragstat);
      }
      if (touched_mtime)
	pi->mtime = pi->ctime = pi->dirstat.mtime;
      dout(20) << " final dirstat " << pi->dirstat << dendl;

      if (dirstat.nfiles != pi->dirstat.nfiles ||
	  dirstat.nsubdirs != pi->dirstat.nsubdirs) {
	bool all = true;
	list<frag_t> ls;
	tmpdft.get_leaves_under(frag_t(), ls);
	for (list<frag_t>::iterator p = ls.begin(); p != ls.end(); ++p)
	  if (!dirfrags.count(*p)) {
	    all = false;
	    break;
	  }
	if (all) {
	  clog->error() << "unmatched fragstat on " << ino() << ", inode has "
		       << pi->dirstat << ", dirfrags have " << dirstat << "\n";
	  assert(!"unmatched fragstat" == g_conf->mds_verify_scatter);
	  // trust the dirfrags for now
	  version_t v = pi->dirstat.version;
	  pi->dirstat = dirstat;
	  pi->dirstat.version = v;
	}
      }

      if (pi->dirstat.nfiles < 0 ||
	  pi->dirstat.nsubdirs < 0) {
	clog->error() << "bad/negative fragstat on " << ino()
	    << ", inode has " << pi->dirstat << "\n";
	assert(!"bad/negative fragstat" == g_conf->mds_verify_scatter);

	if (pi->dirstat.nfiles < 0)
	  pi->dirstat.nfiles = 0;
	if (pi->dirstat.nsubdirs < 0)
	  pi->dirstat.nsubdirs = 0;
      }
    }
    break;

  case CEPH_LOCK_INEST:
    {
      fragtree_t tmpdft = dirfragtree;
      nest_info_t rstat;
      rstat.rsubdirs = 1;

      // adjust summation
      assert(is_auth());
      inode_t *pi = get_projected_inode();
      dout(20) << "  orig rstat " << pi->rstat << dendl;
      pi->rstat.version++;
      for (compact_map<frag_t,CDir*>::iterator p = dirfrags.begin();
	   p != dirfrags.end();
	   ++p) {
	frag_t fg = p->first;
	CDir *dir = p->second;
	dout(20) << fg << " " << *dir << dendl;

	bool update = dir->is_auth() && dir->get_version() != 0 && !dir->is_frozen();

	fnode_t *pf = dir->get_projected_fnode();
	if (update)
	  pf = dir->project_fnode();

	if (pf->accounted_rstat.version == pi->rstat.version-1) {
	  // only pull this frag's dirty rstat inodes into the frag if
	  // the frag is non-stale and updateable.  if it's stale,
	  // that info will just get thrown out!
	  if (update)
	    dir->assimilate_dirty_rstat_inodes();

	  dout(20) << fg << "           rstat " << pf->rstat << dendl;
	  dout(20) << fg << " accounted_rstat " << pf->accounted_rstat << dendl;
	  dout(20) << fg << " dirty_old_rstat " << dir->dirty_old_rstat << dendl;
	  mdcache->project_rstat_frag_to_inode(pf->rstat, pf->accounted_rstat,
					       dir->first, CEPH_NOSNAP, this, true);
	  for (compact_map<snapid_t,old_rstat_t>::iterator q = dir->dirty_old_rstat.begin();
	       q != dir->dirty_old_rstat.end();
	       ++q)
	    mdcache->project_rstat_frag_to_inode(q->second.rstat, q->second.accounted_rstat,
						 q->second.first, q->first, this, true);
	  if (update)  // dir contents not valid if frozen or non-auth
	    dir->check_rstats();
	} else {
	  dout(20) << fg << " skipping STALE accounted_rstat " << pf->accounted_rstat << dendl;
	}
	if (update) {
	  pf->accounted_rstat = pf->rstat;
	  dir->dirty_old_rstat.clear();
	  pf->rstat.version = pf->accounted_rstat.version = pi->rstat.version;
	  dir->check_rstats();
	  dout(10) << fg << " updated accounted_rstat " << pf->rstat << " on " << *dir << dendl;
	}

	tmpdft.force_to_leaf(g_ceph_context, fg);
	rstat.add(pf->rstat);
      }
      dout(20) << " final rstat " << pi->rstat << dendl;

      if (rstat.rfiles != pi->rstat.rfiles ||
	  rstat.rsubdirs != pi->rstat.rsubdirs ||
	  rstat.rbytes != pi->rstat.rbytes) {
	bool all = true;
	list<frag_t> ls;
	tmpdft.get_leaves_under(frag_t(), ls);
	for (list<frag_t>::iterator p = ls.begin(); p != ls.end(); ++p)
	  if (!dirfrags.count(*p)) {
	    all = false;
	    break;
	  }
	if (all) {
	  clog->error() << "unmatched rstat on " << ino() << ", inode has "
		       << pi->rstat << ", dirfrags have " << rstat << "\n";
	  assert(!"unmatched rstat" == g_conf->mds_verify_scatter);
	  // trust the dirfrag for now
	  version_t v = pi->rstat.version;
	  pi->rstat = rstat;
	  pi->rstat.version = v;
	}
      }

      mdcache->broadcast_quota_to_client(this);
    }
    break;

  case CEPH_LOCK_IDFT:
    break;

  default:
    assert(0);
  }
}

void CInode::finish_scatter_gather_update_accounted(int type, MutationRef& mut, EMetaBlob *metablob)
{
  dout(10) << "finish_scatter_gather_update_accounted " << type << " on " << *this << dendl;
  assert(is_auth());

  for (compact_map<frag_t,CDir*>::iterator p = dirfrags.begin();
       p != dirfrags.end();
       ++p) {
    CDir *dir = p->second;
    if (!dir->is_auth() || dir->get_version() == 0 || dir->is_frozen())
      continue;
    
    if (type == CEPH_LOCK_IDFT)
      continue;  // nothing to do.

    dout(10) << " journaling updated frag accounted_ on " << *dir << dendl;
    assert(dir->is_projected());
    fnode_t *pf = dir->get_projected_fnode();
    pf->version = dir->pre_dirty();
    mut->add_projected_fnode(dir);
    metablob->add_dir(dir, true);
    mut->auth_pin(dir);

    if (type == CEPH_LOCK_INEST)
      dir->assimilate_dirty_rstat_inodes_finish(mut, metablob);
  }
}

// waiting

bool CInode::is_frozen() const
{
  if (is_frozen_inode()) return true;
  if (parent && parent->dir->is_frozen()) return true;
  return false;
}

bool CInode::is_frozen_dir() const
{
  if (parent && parent->dir->is_frozen_dir()) return true;
  return false;
}

bool CInode::is_freezing() const
{
  if (is_freezing_inode()) return true;
  if (parent && parent->dir->is_freezing()) return true;
  return false;
}

void CInode::add_dir_waiter(frag_t fg, MDSInternalContextBase *c)
{
  if (waiting_on_dir.empty())
    get(PIN_DIRWAITER);
  waiting_on_dir[fg].push_back(c);
  dout(10) << "add_dir_waiter frag " << fg << " " << c << " on " << *this << dendl;
}

void CInode::take_dir_waiting(frag_t fg, list<MDSInternalContextBase*>& ls)
{
  if (waiting_on_dir.empty())
    return;

  compact_map<frag_t, list<MDSInternalContextBase*> >::iterator p = waiting_on_dir.find(fg);
  if (p != waiting_on_dir.end()) {
    dout(10) << "take_dir_waiting frag " << fg << " on " << *this << dendl;
    ls.splice(ls.end(), p->second);
    waiting_on_dir.erase(p);

    if (waiting_on_dir.empty())
      put(PIN_DIRWAITER);
  }
}

void CInode::add_waiter(uint64_t tag, MDSInternalContextBase *c) 
{
  dout(10) << "add_waiter tag " << std::hex << tag << std::dec << " " << c
	   << " !ambig " << !state_test(STATE_AMBIGUOUSAUTH)
	   << " !frozen " << !is_frozen_inode()
	   << " !freezing " << !is_freezing_inode()
	   << dendl;
  // wait on the directory?
  //  make sure its not the inode that is explicitly ambiguous|freezing|frozen
  if (((tag & WAIT_SINGLEAUTH) && !state_test(STATE_AMBIGUOUSAUTH)) ||
      ((tag & WAIT_UNFREEZE) &&
       !is_frozen_inode() && !is_freezing_inode() && !is_frozen_auth_pin())) {
    dout(15) << "passing waiter up tree" << dendl;
    parent->dir->add_waiter(tag, c);
    return;
  }
  dout(15) << "taking waiter here" << dendl;
  MDSCacheObject::add_waiter(tag, c);
}

void CInode::take_waiting(uint64_t mask, list<MDSInternalContextBase*>& ls)
{
  if ((mask & WAIT_DIR) && !waiting_on_dir.empty()) {
    // take all dentry waiters
    while (!waiting_on_dir.empty()) {
      compact_map<frag_t, list<MDSInternalContextBase*> >::iterator p = waiting_on_dir.begin();
      dout(10) << "take_waiting dirfrag " << p->first << " on " << *this << dendl;
      ls.splice(ls.end(), p->second);
      waiting_on_dir.erase(p);
    }
    put(PIN_DIRWAITER);
  }

  // waiting
  MDSCacheObject::take_waiting(mask, ls);
}

bool CInode::freeze_inode(int auth_pin_allowance)
{
  assert(auth_pin_allowance > 0);  // otherwise we need to adjust parent's nested_auth_pins
  assert(auth_pins >= auth_pin_allowance);
  if (auth_pins > auth_pin_allowance) {
    dout(10) << "freeze_inode - waiting for auth_pins to drop to " << auth_pin_allowance << dendl;
    auth_pin_freeze_allowance = auth_pin_allowance;
    get(PIN_FREEZING);
    state_set(STATE_FREEZING);
    return false;
  }

  dout(10) << "freeze_inode - frozen" << dendl;
  assert(auth_pins == auth_pin_allowance);
  if (!state_test(STATE_FROZEN)) {
    get(PIN_FROZEN);
    state_set(STATE_FROZEN);
  }
  return true;
}

void CInode::unfreeze_inode(list<MDSInternalContextBase*>& finished) 
{
  dout(10) << "unfreeze_inode" << dendl;
  if (state_test(STATE_FREEZING)) {
    state_clear(STATE_FREEZING);
    put(PIN_FREEZING);
  } else if (state_test(STATE_FROZEN)) {
    state_clear(STATE_FROZEN);
    put(PIN_FROZEN);
  } else 
    assert(0);
  take_waiting(WAIT_UNFREEZE, finished);
}

void CInode::unfreeze_inode()
{
    list<MDSInternalContextBase*> finished;
    unfreeze_inode(finished);
    mdcache->mds->queue_waiters(finished);
}

void CInode::freeze_auth_pin()
{
  assert(state_test(CInode::STATE_FROZEN));
  state_set(CInode::STATE_FROZENAUTHPIN);
}

void CInode::unfreeze_auth_pin()
{
  assert(state_test(CInode::STATE_FROZENAUTHPIN));
  state_clear(CInode::STATE_FROZENAUTHPIN);
  if (!state_test(STATE_FREEZING|STATE_FROZEN)) {
    list<MDSInternalContextBase*> finished;
    take_waiting(WAIT_UNFREEZE, finished);
    mdcache->mds->queue_waiters(finished);
  }
}

void CInode::clear_ambiguous_auth(list<MDSInternalContextBase*>& finished)
{
  assert(state_test(CInode::STATE_AMBIGUOUSAUTH));
  state_clear(CInode::STATE_AMBIGUOUSAUTH);
  take_waiting(CInode::WAIT_SINGLEAUTH, finished);
}

void CInode::clear_ambiguous_auth()
{
  list<MDSInternalContextBase*> finished;
  clear_ambiguous_auth(finished);
  mdcache->mds->queue_waiters(finished);
}

// auth_pins
bool CInode::can_auth_pin() const {
  if (!is_auth() || is_freezing_inode() || is_frozen_inode() || is_frozen_auth_pin())
    return false;
  if (parent)
    return parent->can_auth_pin();
  return true;
}

void CInode::auth_pin(void *by) 
{
  if (auth_pins == 0)
    get(PIN_AUTHPIN);
  auth_pins++;

#ifdef MDS_AUTHPIN_SET
  auth_pin_set.insert(by);
#endif

  dout(10) << "auth_pin by " << by << " on " << *this
	   << " now " << auth_pins << "+" << nested_auth_pins
	   << dendl;
  
  if (parent)
    parent->adjust_nested_auth_pins(1, 1, this);
}

void CInode::auth_unpin(void *by) 
{
  auth_pins--;

#ifdef MDS_AUTHPIN_SET
  assert(auth_pin_set.count(by));
  auth_pin_set.erase(auth_pin_set.find(by));
#endif

  if (auth_pins == 0)
    put(PIN_AUTHPIN);
  
  dout(10) << "auth_unpin by " << by << " on " << *this
	   << " now " << auth_pins << "+" << nested_auth_pins
	   << dendl;
  
  assert(auth_pins >= 0);

  if (parent)
    parent->adjust_nested_auth_pins(-1, -1, by);

  if (is_freezing_inode() &&
      auth_pins == auth_pin_freeze_allowance) {
    dout(10) << "auth_unpin freezing!" << dendl;
    get(PIN_FROZEN);
    put(PIN_FREEZING);
    state_clear(STATE_FREEZING);
    state_set(STATE_FROZEN);
    finish_waiting(WAIT_FROZEN);
  }  
}

void CInode::adjust_nested_auth_pins(int a, void *by)
{
  assert(a);
  nested_auth_pins += a;
  dout(35) << "adjust_nested_auth_pins by " << by
	   << " change " << a << " yields "
	   << auth_pins << "+" << nested_auth_pins << dendl;
  assert(nested_auth_pins >= 0);

  if (g_conf->mds_debug_auth_pins) {
    // audit
    int s = 0;
    for (compact_map<frag_t,CDir*>::iterator p = dirfrags.begin();
	 p != dirfrags.end();
	 ++p) {
      CDir *dir = p->second;
      if (!dir->is_subtree_root() && dir->get_cum_auth_pins())
	s++;
    } 
    assert(s == nested_auth_pins);
  } 

  if (parent)
    parent->adjust_nested_auth_pins(a, 0, by);
}


// authority

mds_authority_t CInode::authority() const
{
  if (inode_auth.first >= 0) 
    return inode_auth;

  if (parent)
    return parent->dir->authority();

  // new items that are not yet linked in (in the committed plane) belong
  // to their first parent.
  if (!projected_parent.empty())
    return projected_parent.front()->dir->authority();

  return CDIR_AUTH_UNDEF;
}


// SNAP

snapid_t CInode::get_oldest_snap()
{
  snapid_t t = first;
  if (!old_inodes.empty())
    t = old_inodes.begin()->second.first;
  return MIN(t, oldest_snap);
}

old_inode_t& CInode::cow_old_inode(snapid_t follows, bool cow_head)
{
  assert(follows >= first);

  inode_t *pi = cow_head ? get_projected_inode() : get_previous_projected_inode();
  map<string,bufferptr> *px = cow_head ? get_projected_xattrs() : get_previous_projected_xattrs();

  old_inode_t &old = old_inodes[follows];
  old.first = first;
  old.inode = *pi;
  old.xattrs = *px;

  if (first < oldest_snap)
    oldest_snap = first;
  
  dout(10) << " " << px->size() << " xattrs cowed, " << *px << dendl;

  old.inode.trim_client_ranges(follows);

  if (g_conf->mds_snap_rstat &&
      !(old.inode.rstat == old.inode.accounted_rstat))
    dirty_old_rstats.insert(follows);
  
  first = follows+1;

  dout(10) << "cow_old_inode " << (cow_head ? "head" : "previous_head" )
	   << " to [" << old.first << "," << follows << "] on "
	   << *this << dendl;

  return old;
}

void CInode::split_old_inode(snapid_t snap)
{
  compact_map<snapid_t, old_inode_t>::iterator p = old_inodes.lower_bound(snap);
  assert(p != old_inodes.end() && p->second.first < snap);

  old_inode_t &old = old_inodes[snap - 1];
  old = p->second;

  p->second.first = snap;
  dout(10) << "split_old_inode " << "[" << old.first << "," << p->first
	   << "] to [" << snap << "," << p->first << "] on " << *this << dendl;
}

void CInode::pre_cow_old_inode()
{
  snapid_t follows = find_snaprealm()->get_newest_seq();
  if (first <= follows)
    cow_old_inode(follows, true);
}

void CInode::purge_stale_snap_data(const set<snapid_t>& snaps)
{
  dout(10) << "purge_stale_snap_data " << snaps << dendl;

  if (old_inodes.empty())
    return;

  compact_map<snapid_t,old_inode_t>::iterator p = old_inodes.begin();
  while (p != old_inodes.end()) {
    set<snapid_t>::const_iterator q = snaps.lower_bound(p->second.first);
    if (q == snaps.end() || *q > p->first) {
      dout(10) << " purging old_inode [" << p->second.first << "," << p->first << "]" << dendl;
      old_inodes.erase(p++);
    } else
      ++p;
  }
}

/*
 * pick/create an old_inode
 */
old_inode_t * CInode::pick_old_inode(snapid_t snap)
{
  compact_map<snapid_t, old_inode_t>::iterator p = old_inodes.lower_bound(snap);  // p is first key >= to snap
  if (p != old_inodes.end() && p->second.first <= snap) {
    dout(10) << "pick_old_inode snap " << snap << " -> [" << p->second.first << "," << p->first << "]" << dendl;
    return &p->second;
  }
  dout(10) << "pick_old_inode snap " << snap << " -> nothing" << dendl;
  return NULL;
}

void CInode::open_snaprealm(bool nosplit)
{
  if (!snaprealm) {
    SnapRealm *parent = find_snaprealm();
    snaprealm = new SnapRealm(mdcache, this);
    if (parent) {
      dout(10) << "open_snaprealm " << snaprealm
	       << " parent is " << parent
	       << dendl;
      dout(30) << " siblings are " << parent->open_children << dendl;
      snaprealm->parent = parent;
      if (!nosplit)
	parent->split_at(snaprealm);
      parent->open_children.insert(snaprealm);
    }
  }
}
void CInode::close_snaprealm(bool nojoin)
{
  if (snaprealm) {
    dout(15) << "close_snaprealm " << *snaprealm << dendl;
    snaprealm->close_parents();
    if (snaprealm->parent) {
      snaprealm->parent->open_children.erase(snaprealm);
      //if (!nojoin)
      //snaprealm->parent->join(snaprealm);
    }
    delete snaprealm;
    snaprealm = 0;
  }
}

SnapRealm *CInode::find_snaprealm()
{
  CInode *cur = this;
  while (!cur->snaprealm) {
    if (cur->get_parent_dn())
      cur = cur->get_parent_dn()->get_dir()->get_inode();
    else if (get_projected_parent_dn())
      cur = cur->get_projected_parent_dn()->get_dir()->get_inode();
    else
      break;
  }
  return cur->snaprealm;
}

void CInode::encode_snap_blob(bufferlist &snapbl)
{
  if (snaprealm) {
    ::encode(snaprealm->srnode, snapbl);
    dout(20) << "encode_snap_blob " << *snaprealm << dendl;
  }
}
void CInode::decode_snap_blob(bufferlist& snapbl)
{
  if (snapbl.length()) {
    open_snaprealm();
    bufferlist::iterator p = snapbl.begin();
    ::decode(snaprealm->srnode, p);
    dout(20) << "decode_snap_blob " << *snaprealm << dendl;
  }
}

void CInode::encode_snap(bufferlist& bl)
{
  bufferlist snapbl;
  encode_snap_blob(snapbl);
  ::encode(snapbl, bl);
}    

void CInode::decode_snap(bufferlist::iterator& p)
{
  bufferlist snapbl;
  ::decode(snapbl, p);
  decode_snap_blob(snapbl);
}

// =============================================

client_t CInode::calc_ideal_loner()
{
  if (mdcache->is_readonly())
    return -1;
  if (!mds_caps_wanted.empty())
    return -1;
  
  int n = 0;
  client_t loner = -1;
  for (map<client_t,Capability*>::iterator it = client_caps.begin();
       it != client_caps.end();
       ++it) 
    if (!it->second->is_stale() &&
	((it->second->wanted() & (CEPH_CAP_ANY_WR|CEPH_CAP_FILE_WR|CEPH_CAP_FILE_RD)) ||
	 (inode.is_dir() && !has_subtree_root_dirfrag()))) {
      if (n)
	return -1;
      n++;
      loner = it->first;
    }
  return loner;
}

client_t CInode::choose_ideal_loner()
{
  want_loner_cap = calc_ideal_loner();
  return want_loner_cap;
}

bool CInode::try_set_loner()
{
  assert(want_loner_cap >= 0);
  if (loner_cap >= 0 && loner_cap != want_loner_cap)
    return false;
  set_loner_cap(want_loner_cap);
  return true;
}

void CInode::set_loner_cap(client_t l)
{
  loner_cap = l;
  authlock.set_excl_client(loner_cap);
  filelock.set_excl_client(loner_cap);
  linklock.set_excl_client(loner_cap);
  xattrlock.set_excl_client(loner_cap);
}

bool CInode::try_drop_loner()
{
  if (loner_cap < 0)
    return true;

  int other_allowed = get_caps_allowed_by_type(CAP_ANY);
  Capability *cap = get_client_cap(loner_cap);
  if (!cap ||
      (cap->issued() & ~other_allowed) == 0) {
    set_loner_cap(-1);
    return true;
  }
  return false;
}


// choose new lock state during recovery, based on issued caps
void CInode::choose_lock_state(SimpleLock *lock, int allissued)
{
  int shift = lock->get_cap_shift();
  int issued = (allissued >> shift) & lock->get_cap_mask();
  if (is_auth()) {
    if (lock->is_xlocked()) {
      // do nothing here
    } else if (lock->get_state() != LOCK_MIX) {
      if (issued & CEPH_CAP_GEXCL)
	lock->set_state(LOCK_EXCL);
      else if (issued & CEPH_CAP_GWR)
	lock->set_state(LOCK_MIX);
      else if (lock->is_dirty()) {
	if (is_replicated())
	  lock->set_state(LOCK_MIX);
	else
	  lock->set_state(LOCK_LOCK);
      } else
	lock->set_state(LOCK_SYNC);
    }
  } else {
    // our states have already been chosen during rejoin.
    if (lock->is_xlocked())
      assert(lock->get_state() == LOCK_LOCK);
  }
}
 
void CInode::choose_lock_states()
{
  int issued = get_caps_issued();
  if (is_auth() && (issued & (CEPH_CAP_ANY_EXCL|CEPH_CAP_ANY_WR)) &&
      choose_ideal_loner() >= 0)
    try_set_loner();
  choose_lock_state(&filelock, issued);
  choose_lock_state(&nestlock, issued);
  choose_lock_state(&dirfragtreelock, issued);
  choose_lock_state(&authlock, issued);
  choose_lock_state(&xattrlock, issued);
  choose_lock_state(&linklock, issued);
}

Capability *CInode::add_client_cap(client_t client, Session *session, SnapRealm *conrealm)
{
  if (client_caps.empty()) {
    get(PIN_CAPS);
    if (conrealm)
      containing_realm = conrealm;
    else
      containing_realm = find_snaprealm();
    containing_realm->inodes_with_caps.push_back(&item_caps);
    dout(10) << "add_client_cap first cap, joining realm " << *containing_realm << dendl;
  }

  mdcache->num_caps++;
  if (client_caps.empty())
    mdcache->num_inodes_with_caps++;
  
  Capability *cap = new Capability(this, ++mdcache->last_cap_id, client);
  assert(client_caps.count(client) == 0);
  client_caps[client] = cap;

  session->add_cap(cap);
  if (session->is_stale())
    cap->mark_stale();
  
  cap->client_follows = first-1;
  
  containing_realm->add_cap(client, cap);
  
  return cap;
}

void CInode::remove_client_cap(client_t client)
{
  assert(client_caps.count(client) == 1);
  Capability *cap = client_caps[client];
  
  cap->item_session_caps.remove_myself();
  cap->item_revoking_caps.remove_myself();
  cap->item_client_revoking_caps.remove_myself();
  containing_realm->remove_cap(client, cap);
  
  if (client == loner_cap)
    loner_cap = -1;

  delete cap;
  client_caps.erase(client);
  if (client_caps.empty()) {
    dout(10) << "remove_client_cap last cap, leaving realm " << *containing_realm << dendl;
    put(PIN_CAPS);
    item_caps.remove_myself();
    containing_realm = NULL;
    item_open_file.remove_myself();  // unpin logsegment
    mdcache->num_inodes_with_caps--;
  }
  mdcache->num_caps--;

  //clean up advisory locks
  bool fcntl_removed = fcntl_locks ? fcntl_locks->remove_all_from(client) : false;
  bool flock_removed = flock_locks ? flock_locks->remove_all_from(client) : false; 
  if (fcntl_removed || flock_removed) {
    list<MDSInternalContextBase*> waiters;
    take_waiting(CInode::WAIT_FLOCK, waiters);
    mdcache->mds->queue_waiters(waiters);
  }
}

void CInode::move_to_realm(SnapRealm *realm)
{
  dout(10) << "move_to_realm joining realm " << *realm
	   << ", leaving realm " << *containing_realm << dendl;
  for (map<client_t,Capability*>::iterator q = client_caps.begin();
       q != client_caps.end();
       ++q) {
    containing_realm->remove_cap(q->first, q->second);
    realm->add_cap(q->first, q->second);
  }
  item_caps.remove_myself();
  realm->inodes_with_caps.push_back(&item_caps);
  containing_realm = realm;
}

Capability *CInode::reconnect_cap(client_t client, ceph_mds_cap_reconnect& icr, Session *session)
{
  Capability *cap = get_client_cap(client);
  if (cap) {
    // FIXME?
    cap->merge(icr.wanted, icr.issued);
  } else {
    cap = add_client_cap(client, session);
    cap->set_wanted(icr.wanted);
    cap->issue_norevoke(icr.issued);
    cap->reset_seq();
    cap->set_cap_id(icr.cap_id);
  }
  cap->set_last_issue_stamp(ceph_clock_now(g_ceph_context));
  return cap;
}

void CInode::clear_client_caps_after_export()
{
  while (!client_caps.empty())
    remove_client_cap(client_caps.begin()->first);
  loner_cap = -1;
  want_loner_cap = -1;
  mds_caps_wanted.clear();
}

void CInode::export_client_caps(map<client_t,Capability::Export>& cl)
{
  for (map<client_t,Capability*>::iterator it = client_caps.begin();
       it != client_caps.end();
       ++it) {
    cl[it->first] = it->second->make_export();
  }
}

  // caps allowed
int CInode::get_caps_liked() const
{
  if (is_dir())
    return CEPH_CAP_PIN | CEPH_CAP_ANY_EXCL | CEPH_CAP_ANY_SHARED;  // but not, say, FILE_RD|WR|WRBUFFER
  else
    return CEPH_CAP_ANY & ~CEPH_CAP_FILE_LAZYIO;
}

int CInode::get_caps_allowed_ever() const
{
  int allowed;
  if (is_dir())
    allowed = CEPH_CAP_PIN | CEPH_CAP_ANY_EXCL | CEPH_CAP_ANY_SHARED;
  else
    allowed = CEPH_CAP_ANY;
  return allowed & 
    (CEPH_CAP_PIN |
     (filelock.gcaps_allowed_ever() << filelock.get_cap_shift()) |
     (authlock.gcaps_allowed_ever() << authlock.get_cap_shift()) |
     (xattrlock.gcaps_allowed_ever() << xattrlock.get_cap_shift()) |
     (linklock.gcaps_allowed_ever() << linklock.get_cap_shift()));
}

int CInode::get_caps_allowed_by_type(int type) const
{
  return 
    CEPH_CAP_PIN |
    (filelock.gcaps_allowed(type) << filelock.get_cap_shift()) |
    (authlock.gcaps_allowed(type) << authlock.get_cap_shift()) |
    (xattrlock.gcaps_allowed(type) << xattrlock.get_cap_shift()) |
    (linklock.gcaps_allowed(type) << linklock.get_cap_shift());
}

int CInode::get_caps_careful() const
{
  return 
    (filelock.gcaps_careful() << filelock.get_cap_shift()) |
    (authlock.gcaps_careful() << authlock.get_cap_shift()) |
    (xattrlock.gcaps_careful() << xattrlock.get_cap_shift()) |
    (linklock.gcaps_careful() << linklock.get_cap_shift());
}

int CInode::get_xlocker_mask(client_t client) const
{
  return 
    (filelock.gcaps_xlocker_mask(client) << filelock.get_cap_shift()) |
    (authlock.gcaps_xlocker_mask(client) << authlock.get_cap_shift()) |
    (xattrlock.gcaps_xlocker_mask(client) << xattrlock.get_cap_shift()) |
    (linklock.gcaps_xlocker_mask(client) << linklock.get_cap_shift());
}

int CInode::get_caps_allowed_for_client(client_t client) const
{
  int allowed;
  if (client == get_loner()) {
    // as the loner, we get the loner_caps AND any xlocker_caps for things we have xlocked
    allowed =
      get_caps_allowed_by_type(CAP_LONER) |
      (get_caps_allowed_by_type(CAP_XLOCKER) & get_xlocker_mask(client));
  } else {
    allowed = get_caps_allowed_by_type(CAP_ANY);
  }
  if (inode.inline_data.version != CEPH_INLINE_NONE &&
      !mdcache->mds->get_session(client)->connection->has_feature(CEPH_FEATURE_MDS_INLINE_DATA))
    allowed &= ~(CEPH_CAP_FILE_RD | CEPH_CAP_FILE_WR);
  return allowed;
}

// caps issued, wanted
int CInode::get_caps_issued(int *ploner, int *pother, int *pxlocker,
			    int shift, int mask)
{
  int c = 0;
  int loner = 0, other = 0, xlocker = 0;
  if (!is_auth()) {
    loner_cap = -1;
  }

  for (map<client_t,Capability*>::const_iterator it = client_caps.begin();
       it != client_caps.end();
       ++it) {
    int i = it->second->issued();
    c |= i;
    if (it->first == loner_cap)
      loner |= i;
    else
      other |= i;
    xlocker |= get_xlocker_mask(it->first) & i;
  }
  if (ploner) *ploner = (loner >> shift) & mask;
  if (pother) *pother = (other >> shift) & mask;
  if (pxlocker) *pxlocker = (xlocker >> shift) & mask;
  return (c >> shift) & mask;
}

bool CInode::is_any_caps_wanted() const
{
  for (map<client_t,Capability*>::const_iterator it = client_caps.begin();
       it != client_caps.end();
       ++it)
    if (it->second->wanted())
      return true;
  return false;
}

int CInode::get_caps_wanted(int *ploner, int *pother, int shift, int mask) const
{
  int w = 0;
  int loner = 0, other = 0;
  for (map<client_t,Capability*>::const_iterator it = client_caps.begin();
       it != client_caps.end();
       ++it) {
    if (!it->second->is_stale()) {
      int t = it->second->wanted();
      w |= t;
      if (it->first == loner_cap)
	loner |= t;
      else
	other |= t;	
    }
    //cout << " get_caps_wanted client " << it->first << " " << cap_string(it->second.wanted()) << endl;
  }
  if (is_auth())
    for (compact_map<int,int>::const_iterator it = mds_caps_wanted.begin();
	 it != mds_caps_wanted.end();
	 ++it) {
      w |= it->second;
      other |= it->second;
      //cout << " get_caps_wanted mds " << it->first << " " << cap_string(it->second) << endl;
    }
  if (ploner) *ploner = (loner >> shift) & mask;
  if (pother) *pother = (other >> shift) & mask;
  return (w >> shift) & mask;
}

bool CInode::issued_caps_need_gather(SimpleLock *lock)
{
  int loner_issued, other_issued, xlocker_issued;
  get_caps_issued(&loner_issued, &other_issued, &xlocker_issued,
		  lock->get_cap_shift(), lock->get_cap_mask());
  if ((loner_issued & ~lock->gcaps_allowed(CAP_LONER)) ||
      (other_issued & ~lock->gcaps_allowed(CAP_ANY)) ||
      (xlocker_issued & ~lock->gcaps_allowed(CAP_XLOCKER)))
    return true;
  return false;
}

void CInode::replicate_relax_locks()
{
  //dout(10) << " relaxing locks on " << *this << dendl;
  assert(is_auth());
  assert(!is_replicated());
  
  authlock.replicate_relax();
  linklock.replicate_relax();
  dirfragtreelock.replicate_relax();
  filelock.replicate_relax();
  xattrlock.replicate_relax();
  snaplock.replicate_relax();
  nestlock.replicate_relax();
  flocklock.replicate_relax();
  policylock.replicate_relax();
}



// =============================================

int CInode::encode_inodestat(bufferlist& bl, Session *session,
			     SnapRealm *dir_realm,
			     snapid_t snapid,
			     unsigned max_bytes,
			     int getattr_caps)
{
  int client = session->info.inst.name.num();
  assert(snapid);
  assert(session->connection);
  
  bool valid = true;

  // do not issue caps if inode differs from readdir snaprealm
  SnapRealm *realm = find_snaprealm();
  bool no_caps = session->is_stale() ||
		 (realm && dir_realm && realm != dir_realm) ||
		 is_frozen() || state_test(CInode::STATE_EXPORTINGCAPS);
  if (no_caps)
    dout(20) << "encode_inodestat no caps"
	     << (session->is_stale()?", session stale ":"")
	     << ((realm && dir_realm && realm != dir_realm)?", snaprealm differs ":"")
	     << (state_test(CInode::STATE_EXPORTINGCAPS)?", exporting caps":"")
	     << (is_frozen()?", frozen inode":"") << dendl;

  // pick a version!
  inode_t *oi = &inode;
  inode_t *pi = get_projected_inode();

  map<string, bufferptr> *pxattrs = 0;

  if (snapid != CEPH_NOSNAP) {

    // for now at least, old_inodes is only defined/valid on the auth
    if (!is_auth())
      valid = false;

    if (is_multiversion()) {
      compact_map<snapid_t,old_inode_t>::iterator p = old_inodes.lower_bound(snapid);
      if (p != old_inodes.end()) {
	if (p->second.first > snapid) {
	  if  (p != old_inodes.begin())
	    --p;
	}
	if (p->second.first <= snapid && snapid <= p->first) {
	  dout(15) << "encode_inodestat snapid " << snapid
		   << " to old_inode [" << p->second.first << "," << p->first << "]"
		   << " " << p->second.inode.rstat
		   << dendl;
	  pi = oi = &p->second.inode;
	  pxattrs = &p->second.xattrs;
	} else {
	  // snapshoted remote dentry can result this
	  dout(0) << "encode_inodestat old_inode for snapid " << snapid
		  << " not found" << dendl;
	}
      }
    } else if (snapid < first || snapid > last) {
      // snapshoted remote dentry can result this
      dout(0) << "encode_inodestat [" << first << "," << last << "]"
	      << " not match snapid " << snapid << dendl;
    }
  }
  
  /*
   * note: encoding matches struct ceph_client_reply_inode
   */
  struct ceph_mds_reply_inode e;
  memset(&e, 0, sizeof(e));
  e.ino = oi->ino;
  e.snapid = snapid;  // 0 -> NOSNAP
  e.rdev = oi->rdev;

  // "fake" a version that is old (stable) version, +1 if projected.
  e.version = (oi->version * 2) + is_projected();


  Capability *cap = get_client_cap(client);
  bool pfile = filelock.is_xlocked_by_client(client) || get_loner() == client;
  //(cap && (cap->issued() & CEPH_CAP_FILE_EXCL));
  bool pauth = authlock.is_xlocked_by_client(client) || get_loner() == client;
  bool plink = linklock.is_xlocked_by_client(client) || get_loner() == client;
  bool pxattr = xattrlock.is_xlocked_by_client(client) || get_loner() == client;

  bool plocal = versionlock.get_last_wrlock_client() == client;
  bool ppolicy = policylock.is_xlocked_by_client(client) || get_loner()==client;
  
  inode_t *i = (pfile|pauth|plink|pxattr|plocal) ? pi : oi;
  i->ctime.encode_timeval(&e.ctime);
  
  dout(20) << " pfile " << pfile << " pauth " << pauth << " plink " << plink << " pxattr " << pxattr
	   << " plocal " << plocal
	   << " ctime " << i->ctime
	   << " valid=" << valid << dendl;

  // file
  i = pfile ? pi:oi;
  if (is_dir()) {
    e.layout = (ppolicy ? pi : oi)->layout;
  } else {
    e.layout = i->layout;
  }
  e.size = i->size;
  e.truncate_seq = i->truncate_seq;
  e.truncate_size = i->truncate_size;
  i->mtime.encode_timeval(&e.mtime);
  i->atime.encode_timeval(&e.atime);
  e.time_warp_seq = i->time_warp_seq;

  // max_size is min of projected, actual
  e.max_size = MIN(oi->client_ranges.count(client) ? oi->client_ranges[client].range.last : 0,
		   pi->client_ranges.count(client) ? pi->client_ranges[client].range.last : 0);

  e.files = i->dirstat.nfiles;
  e.subdirs = i->dirstat.nsubdirs;

  // inline data
  version_t inline_version = 0;
  bufferlist inline_data;
  if (i->inline_data.version == CEPH_INLINE_NONE) {
    inline_version = CEPH_INLINE_NONE;
  } else if ((!cap && !no_caps) ||
	     (cap && cap->client_inline_version < i->inline_data.version) ||
	     (getattr_caps & CEPH_CAP_FILE_RD)) { // client requests inline data
    inline_version = i->inline_data.version;
    if (i->inline_data.length() > 0)
      inline_data = i->inline_data.get_data();
  }

  // nest (do same as file... :/)
  i->rstat.rctime.encode_timeval(&e.rctime);
  e.rbytes = i->rstat.rbytes;
  e.rfiles = i->rstat.rfiles;
  e.rsubdirs = i->rstat.rsubdirs;
  if (cap) {
    cap->last_rbytes = i->rstat.rbytes;
    cap->last_rsize = i->rstat.rsize();
  }

  // auth
  i = pauth ? pi:oi;
  e.mode = i->mode;
  e.uid = i->uid;
  e.gid = i->gid;

  // link
  i = plink ? pi:oi;
  e.nlink = i->nlink;
  
  // xattr
  i = pxattr ? pi:oi;

  // xattr
  bufferlist xbl;
  if ((!cap && !no_caps) ||
      (cap && cap->client_xattr_version < i->xattr_version) ||
      (getattr_caps & CEPH_CAP_XATTR_SHARED)) { // client requests xattrs
    if (!pxattrs)
      pxattrs = pxattr ? get_projected_xattrs() : &xattrs;
    ::encode(*pxattrs, xbl);
    e.xattr_version = i->xattr_version;
  } else {
    e.xattr_version = 0;
  }
  
  // do we have room?
  if (max_bytes) {
    unsigned bytes = sizeof(e);
    bytes += sizeof(__u32);
    bytes += (sizeof(__u32) + sizeof(__u32)) * dirfragtree._splits.size();
    bytes += sizeof(__u32) + symlink.length();
    bytes += sizeof(__u32) + xbl.length();
    bytes += sizeof(version_t) + sizeof(__u32) + inline_data.length();
    if (bytes > max_bytes)
      return -ENOSPC;
  }


  // encode caps
  if (snapid != CEPH_NOSNAP) {
    /*
     * snapped inodes (files or dirs) only get read-only caps.  always
     * issue everything possible, since it is read only.
     *
     * if a snapped inode has caps, limit issued caps based on the
     * lock state.
     *
     * if it is a live inode, limit issued caps based on the lock
     * state.
     *
     * do NOT adjust cap issued state, because the client always
     * tracks caps per-snap and the mds does either per-interval or
     * multiversion.
     */
    e.cap.caps = valid ? get_caps_allowed_by_type(CAP_ANY) : CEPH_STAT_CAP_INODE;
    if (last == CEPH_NOSNAP || is_any_caps())
      e.cap.caps = e.cap.caps & get_caps_allowed_for_client(client);
    e.cap.seq = 0;
    e.cap.mseq = 0;
    e.cap.realm = 0;
  } else {
    if (!no_caps && valid && !cap) {
      // add a new cap
      cap = add_client_cap(client, session, realm);
      if (is_auth()) {
	if (choose_ideal_loner() >= 0)
	  try_set_loner();
	else if (get_wanted_loner() < 0)
	  try_drop_loner();
      }
    }

    if (!no_caps && valid && cap) {
      int likes = get_caps_liked();
      int allowed = get_caps_allowed_for_client(client);
      int issue = (cap->wanted() | likes) & allowed;
      cap->issue_norevoke(issue);
      issue = cap->pending();
      cap->set_last_issue();
      cap->set_last_issue_stamp(ceph_clock_now(g_ceph_context));
      cap->clear_new();
      e.cap.caps = issue;
      e.cap.wanted = cap->wanted();
      e.cap.cap_id = cap->get_cap_id();
      e.cap.seq = cap->get_last_seq();
      dout(10) << "encode_inodestat issueing " << ccap_string(issue) << " seq " << cap->get_last_seq() << dendl;
      e.cap.mseq = cap->get_mseq();
      e.cap.realm = realm->inode->ino();
    } else {
      if (cap)
	cap->clear_new();
      e.cap.cap_id = 0;
      e.cap.caps = 0;
      e.cap.seq = 0;
      e.cap.mseq = 0;
      e.cap.realm = 0;
      e.cap.wanted = 0;
    }
  }
  e.cap.flags = is_auth() ? CEPH_CAP_FLAG_AUTH:0;
  dout(10) << "encode_inodestat caps " << ccap_string(e.cap.caps)
	   << " seq " << e.cap.seq << " mseq " << e.cap.mseq
	   << " xattrv " << e.xattr_version << " len " << xbl.length()
	   << dendl;

  if (inline_data.length() && cap) {
    if ((cap->pending() | getattr_caps) & CEPH_CAP_FILE_SHARED) {
      dout(10) << "including inline version " << inline_version << dendl;
      cap->client_inline_version = inline_version;
    } else {
      dout(10) << "dropping inline version " << inline_version << dendl;
      inline_version = 0;
      inline_data.clear();
    }
  }

  // include those xattrs?
  if (xbl.length() && cap) {
    if ((cap->pending() | getattr_caps) & CEPH_CAP_XATTR_SHARED) {
      dout(10) << "including xattrs version " << i->xattr_version << dendl;
      cap->client_xattr_version = i->xattr_version;
    } else {
      dout(10) << "dropping xattrs version " << i->xattr_version << dendl;
      xbl.clear(); // no xattrs .. XXX what's this about?!?
      e.xattr_version = 0;
    }
  }

  // encode
  e.fragtree.nsplits = dirfragtree._splits.size();
  ::encode(e, bl);

  dirfragtree.encode_nohead(bl);

  ::encode(symlink, bl);
  if (session->connection->has_feature(CEPH_FEATURE_DIRLAYOUTHASH)) {
    i = pfile ? pi : oi;
    ::encode(i->dir_layout, bl);
  }
  ::encode(xbl, bl);
  if (session->connection->has_feature(CEPH_FEATURE_MDS_INLINE_DATA)) {
    ::encode(inline_version, bl);
    ::encode(inline_data, bl);
  }
  if (session->connection->has_feature(CEPH_FEATURE_MDS_QUOTA)) {
    i = ppolicy ? pi : oi;
    ::encode(i->quota, bl);
  }

  return valid;
}

void CInode::encode_cap_message(MClientCaps *m, Capability *cap)
{
  assert(cap);

  client_t client = cap->get_client();

  bool pfile = filelock.is_xlocked_by_client(client) || (cap->issued() & CEPH_CAP_FILE_EXCL);
  bool pauth = authlock.is_xlocked_by_client(client);
  bool plink = linklock.is_xlocked_by_client(client);
  bool pxattr = xattrlock.is_xlocked_by_client(client);
 
  inode_t *oi = &inode;
  inode_t *pi = get_projected_inode();
  inode_t *i = (pfile|pauth|plink|pxattr) ? pi : oi;
  i->ctime.encode_timeval(&m->head.ctime);
  
  dout(20) << "encode_cap_message pfile " << pfile
	   << " pauth " << pauth << " plink " << plink << " pxattr " << pxattr
	   << " ctime " << i->ctime << dendl;

  i = pfile ? pi:oi;
  m->head.layout = i->layout;
  m->head.size = i->size;
  m->head.truncate_seq = i->truncate_seq;
  m->head.truncate_size = i->truncate_size;
  i->mtime.encode_timeval(&m->head.mtime);
  i->atime.encode_timeval(&m->head.atime);
  m->head.time_warp_seq = i->time_warp_seq;

  if (cap->client_inline_version < i->inline_data.version) {
    m->inline_version = cap->client_inline_version = i->inline_data.version;
    if (i->inline_data.length() > 0)
      m->inline_data = i->inline_data.get_data();
  } else {
    m->inline_version = 0;
  }

  // max_size is min of projected, actual.
  uint64_t oldms = oi->client_ranges.count(client) ? oi->client_ranges[client].range.last : 0;
  uint64_t newms = pi->client_ranges.count(client) ? pi->client_ranges[client].range.last : 0;
  m->head.max_size = MIN(oldms, newms);

  i = pauth ? pi:oi;
  m->head.mode = i->mode;
  m->head.uid = i->uid;
  m->head.gid = i->gid;

  i = plink ? pi:oi;
  m->head.nlink = i->nlink;

  i = pxattr ? pi:oi;
  map<string,bufferptr> *ix = pxattr ? get_projected_xattrs() : &xattrs;
  if ((cap->pending() & CEPH_CAP_XATTR_SHARED) &&
      i->xattr_version > cap->client_xattr_version) {
    dout(10) << "    including xattrs v " << i->xattr_version << dendl;
    ::encode(*ix, m->xattrbl);
    m->head.xattr_version = i->xattr_version;
    cap->client_xattr_version = i->xattr_version;
  }
}



void CInode::_encode_base(bufferlist& bl)
{
  ::encode(first, bl);
  ::encode(inode, bl);
  ::encode(symlink, bl);
  ::encode(dirfragtree, bl);
  ::encode(xattrs, bl);
  ::encode(old_inodes, bl);
  encode_snap(bl);
}
void CInode::_decode_base(bufferlist::iterator& p)
{
  ::decode(first, p);
  ::decode(inode, p);
  ::decode(symlink, p);
  ::decode(dirfragtree, p);
  ::decode(xattrs, p);
  ::decode(old_inodes, p);
  decode_snap(p);
}

void CInode::_encode_locks_full(bufferlist& bl)
{
  ::encode(authlock, bl);
  ::encode(linklock, bl);
  ::encode(dirfragtreelock, bl);
  ::encode(filelock, bl);
  ::encode(xattrlock, bl);
  ::encode(snaplock, bl);
  ::encode(nestlock, bl);
  ::encode(flocklock, bl);
  ::encode(policylock, bl);

  ::encode(loner_cap, bl);
}
void CInode::_decode_locks_full(bufferlist::iterator& p)
{
  ::decode(authlock, p);
  ::decode(linklock, p);
  ::decode(dirfragtreelock, p);
  ::decode(filelock, p);
  ::decode(xattrlock, p);
  ::decode(snaplock, p);
  ::decode(nestlock, p);
  ::decode(flocklock, p);
  ::decode(policylock, p);

  ::decode(loner_cap, p);
  set_loner_cap(loner_cap);
  want_loner_cap = loner_cap;  // for now, we'll eval() shortly.
}

void CInode::_encode_locks_state_for_replica(bufferlist& bl)
{
  authlock.encode_state_for_replica(bl);
  linklock.encode_state_for_replica(bl);
  dirfragtreelock.encode_state_for_replica(bl);
  filelock.encode_state_for_replica(bl);
  nestlock.encode_state_for_replica(bl);
  xattrlock.encode_state_for_replica(bl);
  snaplock.encode_state_for_replica(bl);
  flocklock.encode_state_for_replica(bl);
  policylock.encode_state_for_replica(bl);
}
void CInode::_encode_locks_state_for_rejoin(bufferlist& bl, int rep)
{
  authlock.encode_state_for_replica(bl);
  linklock.encode_state_for_replica(bl);
  dirfragtreelock.encode_state_for_rejoin(bl, rep);
  filelock.encode_state_for_rejoin(bl, rep);
  nestlock.encode_state_for_rejoin(bl, rep);
  xattrlock.encode_state_for_replica(bl);
  snaplock.encode_state_for_replica(bl);
  flocklock.encode_state_for_replica(bl);
  policylock.encode_state_for_replica(bl);
}
void CInode::_decode_locks_state(bufferlist::iterator& p, bool is_new)
{
  authlock.decode_state(p, is_new);
  linklock.decode_state(p, is_new);
  dirfragtreelock.decode_state(p, is_new);
  filelock.decode_state(p, is_new);
  nestlock.decode_state(p, is_new);
  xattrlock.decode_state(p, is_new);
  snaplock.decode_state(p, is_new);
  flocklock.decode_state(p, is_new);
  policylock.decode_state(p, is_new);
}
void CInode::_decode_locks_rejoin(bufferlist::iterator& p, list<MDSInternalContextBase*>& waiters,
				  list<SimpleLock*>& eval_locks)
{
  authlock.decode_state_rejoin(p, waiters);
  linklock.decode_state_rejoin(p, waiters);
  dirfragtreelock.decode_state_rejoin(p, waiters);
  filelock.decode_state_rejoin(p, waiters);
  nestlock.decode_state_rejoin(p, waiters);
  xattrlock.decode_state_rejoin(p, waiters);
  snaplock.decode_state_rejoin(p, waiters);
  flocklock.decode_state_rejoin(p, waiters);
  policylock.decode_state_rejoin(p, waiters);

  if (!dirfragtreelock.is_stable() && !dirfragtreelock.is_wrlocked())
    eval_locks.push_back(&dirfragtreelock);
  if (!filelock.is_stable() && !filelock.is_wrlocked())
    eval_locks.push_back(&filelock);
  if (!nestlock.is_stable() && !nestlock.is_wrlocked())
    eval_locks.push_back(&nestlock);
}


// IMPORT/EXPORT

void CInode::encode_export(bufferlist& bl)
{
  ENCODE_START(5, 4, bl)
  _encode_base(bl);

  ::encode(state, bl);

  ::encode(pop, bl);

  ::encode(replica_map, bl);

  // include scatterlock info for any bounding CDirs
  bufferlist bounding;
  if (inode.is_dir())
    for (compact_map<frag_t,CDir*>::iterator p = dirfrags.begin();
	 p != dirfrags.end();
	 ++p) {
      CDir *dir = p->second;
      if (dir->state_test(CDir::STATE_EXPORTBOUND)) {
	::encode(p->first, bounding);
	::encode(dir->fnode.fragstat, bounding);
	::encode(dir->fnode.accounted_fragstat, bounding);
	::encode(dir->fnode.rstat, bounding);
	::encode(dir->fnode.accounted_rstat, bounding);
	dout(10) << " encoded fragstat/rstat info for " << *dir << dendl;
      }
    }
  ::encode(bounding, bl);

  _encode_locks_full(bl);

  _encode_file_locks(bl);

  ENCODE_FINISH(bl);

  get(PIN_TEMPEXPORTING);
}

void CInode::finish_export(utime_t now)
{
  state &= MASK_STATE_EXPORT_KEPT;

  pop.zero(now);

  // just in case!
  //dirlock.clear_updated();

  loner_cap = -1;

  put(PIN_TEMPEXPORTING);
}

void CInode::decode_import(bufferlist::iterator& p,
			   LogSegment *ls)
{
  DECODE_START(5, p);

  _decode_base(p);

  unsigned s;
  ::decode(s, p);
  state |= (s & MASK_STATE_EXPORTED);
  if (is_dirty()) {
    get(PIN_DIRTY);
    _mark_dirty(ls);
  }
  if (is_dirty_parent()) {
    get(PIN_DIRTYPARENT);
    _mark_dirty_parent(ls);
  }

  ::decode(pop, ceph_clock_now(g_ceph_context), p);

  ::decode(replica_map, p);
  if (!replica_map.empty())
    get(PIN_REPLICATED);

  // decode fragstat info on bounding cdirs
  bufferlist bounding;
  ::decode(bounding, p);
  bufferlist::iterator q = bounding.begin();
  while (!q.end()) {
    frag_t fg;
    ::decode(fg, q);
    CDir *dir = get_dirfrag(fg);
    assert(dir);  // we should have all bounds open

    // Only take the remote's fragstat/rstat if we are non-auth for
    // this dirfrag AND the lock is NOT in a scattered (MIX) state.
    // We know lock is stable, and MIX is the only state in which
    // the inode auth (who sent us this data) may not have the best
    // info.

    // HMM: Are there cases where dir->is_auth() is an insufficient
    // check because the dirfrag is under migration?  That implies
    // it is frozen (and in a SYNC or LOCK state).  FIXME.

    if (dir->is_auth() ||
        filelock.get_state() == LOCK_MIX) {
      dout(10) << " skipped fragstat info for " << *dir << dendl;
      frag_info_t f;
      ::decode(f, q);
      ::decode(f, q);
    } else {
      ::decode(dir->fnode.fragstat, q);
      ::decode(dir->fnode.accounted_fragstat, q);
      dout(10) << " took fragstat info for " << *dir << dendl;
    }
    if (dir->is_auth() ||
        nestlock.get_state() == LOCK_MIX) {
      dout(10) << " skipped rstat info for " << *dir << dendl;
      nest_info_t n;
      ::decode(n, q);
      ::decode(n, q);
    } else {
      ::decode(dir->fnode.rstat, q);
      ::decode(dir->fnode.accounted_rstat, q);
      dout(10) << " took rstat info for " << *dir << dendl;
    }
  }

  _decode_locks_full(p);

  _decode_file_locks(p);

  DECODE_FINISH(p);
}


void InodeStoreBase::dump(Formatter *f) const
{
  inode.dump(f);
  f->dump_string("symlink", symlink);
  f->open_array_section("old_inodes");
  for (compact_map<snapid_t, old_inode_t>::const_iterator i = old_inodes.begin();
      i != old_inodes.end(); ++i) {
    f->open_object_section("old_inode");
    {
      // The key is the last snapid, the first is in the old_inode_t
      f->dump_int("last", i->first);
      i->second.dump(f);
    }
    f->close_section();  // old_inode
  }
  f->close_section();  // old_inodes

  f->open_object_section("dirfragtree");
  dirfragtree.dump(f);
  f->close_section(); // dirfragtree
}


void InodeStore::generate_test_instances(list<InodeStore*> &ls)
{
  InodeStore *populated = new InodeStore;
  populated->inode.ino = 0xdeadbeef;
  populated->symlink = "rhubarb";
  ls.push_back(populated);
}

void CInode::validate_disk_state(CInode::validated_data *results,
                                 MDRequestRef &mdr)
{
  class ValidationContinuation : public MDSContinuation {
  public:
    CInode *in;
    CInode::validated_data *results;
    bufferlist bl;
    CInode *shadow_in;

    enum {
      START = 0,
      BACKTRACE,
      INODE,
      DIRFRAGS
    };

    ValidationContinuation(CInode *i,
                           CInode::validated_data *data_r,
                           MDRequestRef &mdr) :
                             MDSContinuation(mdr, i->mdcache->mds->server),
                             in(i),
                             results(data_r),
                             shadow_in(NULL) {
      set_callback(START, static_cast<Continuation::stagePtr>(&ValidationContinuation::_start));
      set_callback(BACKTRACE, static_cast<Continuation::stagePtr>(&ValidationContinuation::_backtrace));
      set_callback(INODE, static_cast<Continuation::stagePtr>(&ValidationContinuation::_inode_disk));
      set_callback(DIRFRAGS, static_cast<Continuation::stagePtr>(&ValidationContinuation::_dirfrags));
    }

    ~ValidationContinuation() {
      delete shadow_in;
    }

    bool _start(int rval) {
      if (in->is_dirty()) {
        MDCache *mdcache = in->mdcache;
        inode_t& inode = in->inode;
        dout(20) << "validating a dirty CInode; results will be inconclusive"
                 << dendl;
      }
      if (in->is_symlink()) {
        // there's nothing to do for symlinks!
        results->passed_validation = true;
        return true;
      }

      results->passed_validation = false; // we haven't finished it yet

      C_OnFinisher *conf = new C_OnFinisher(get_io_callback(BACKTRACE),
                                            &in->mdcache->mds->finisher);

      in->fetch_backtrace(conf, &bl);
      return false;
    }

    bool _backtrace(int rval) {
      // set up basic result reporting and make sure we got the data
      results->performed_validation = true; // at least, some of it!
      results->backtrace.checked = true;
      results->backtrace.ondisk_read_retval = rval;
      results->backtrace.passed = false; // we'll set it true if we make it
      if (rval != 0) {
        results->backtrace.error_str << "failed to read off disk; see retval";
        return true;
      }

      // extract the backtrace, and compare it to a newly-constructed one
      try {
        bufferlist::iterator p = bl.begin();
        ::decode(results->backtrace.ondisk_value, p);
      } catch (buffer::malformed_input&) {
        results->backtrace.passed = false;
        results->backtrace.error_str << "failed to decode on-disk backtrace!";
        return true;
      }
      int64_t pool;
      if (in->is_dir())
        pool = in->mdcache->mds->mdsmap->get_metadata_pool();
      else
        pool = in->inode.layout.fl_pg_pool;
      inode_backtrace_t& memory_backtrace = results->backtrace.memory_value;
      in->build_backtrace(pool, memory_backtrace);
      bool equivalent, divergent;
      int memory_newer =
          memory_backtrace.compare(results->backtrace.ondisk_value,
                                   &equivalent, &divergent);
      if (equivalent) {
        results->backtrace.passed = true;
      } else {
        results->backtrace.passed = false; // we couldn't validate :(
        if (divergent || memory_newer <= 0) {
          // we're divergent, or don't have a newer version to write
          results->backtrace.error_str <<
              "On-disk backtrace is divergent or newer";
          return true;
        }
      }

      // quit if we're a file, or kick off directory checks otherwise
      // TODO: validate on-disk inode for non-base directories
      if (in->is_file() || in->is_symlink()) {
        results->passed_validation = true;
        return true;
      }

      return validate_directory_data();
    }

    bool validate_directory_data() {
      assert(in->is_dir());

      if (in->is_base()) {
        shadow_in = new CInode(in->mdcache);
        in->mdcache->create_unlinked_system_inode(shadow_in,
                                                  in->inode.ino,
                                                  in->inode.mode);
        shadow_in->fetch(get_internal_callback(INODE));
        return false;
      } else {
        return fetch_dirfrag_rstats();
      }
    }

    bool _inode_disk(int rval) {
      results->inode.checked = true;
      results->inode.ondisk_read_retval = rval;
      results->inode.passed = false;
      results->inode.ondisk_value = shadow_in->inode;
      results->inode.memory_value = in->inode;

      inode_t& si = shadow_in->inode;
      inode_t& i = in->inode;
      if (si.version > i.version) {
        // uh, what?
        results->inode.error_str << "On-disk inode is newer than in-memory one!";
        return true;
      } else {
        bool divergent = false;
        int r = i.compare(si, &divergent);
        results->inode.passed = !divergent && r >= 0;
        if (!results->inode.passed) {
          results->inode.error_str <<
              "On-disk inode is divergent or newer than in-memory one!";
          return true;
        }
      }
      return fetch_dirfrag_rstats();
    }

    bool fetch_dirfrag_rstats() {
      MDSGatherBuilder gather(g_ceph_context);
      std::list<frag_t> frags;
      in->dirfragtree.get_leaves(frags);
      for (list<frag_t>::iterator p = frags.begin();
          p != frags.end();
          ++p) {
        CDir *dirfrag = in->get_or_open_dirfrag(in->mdcache, *p);
        if (!dirfrag->is_complete())
          dirfrag->fetch(gather.new_sub(), false);
      }
      if (gather.has_subs()) {
        gather.set_finisher(get_internal_callback(DIRFRAGS));
        gather.activate();
        return false;
      } else {
        return immediate(DIRFRAGS, 0);
      }
    }

    bool _dirfrags(int rval) {
      // basic reporting setup
      results->raw_rstats.checked = true;
      results->raw_rstats.ondisk_read_retval = rval;
      results->raw_rstats.passed = false; // we'll set it true if we make it
      if (rval != 0) {
        results->raw_rstats.error_str << "Failed to read dirfrags off disk";
        return true;
      }

      // check each dirfrag...
      nest_info_t& sub_info = results->raw_rstats.ondisk_value;
      for (compact_map<frag_t,CDir*>::iterator p = in->dirfrags.begin();
	   p != in->dirfrags.end();
	   ++p) {
        if (!p->second->is_complete()) {
          results->raw_rstats.error_str << "dirfrag is INCOMPLETE despite fetching; probably too large compared to MDS cache size?\n";
          return true;
        }
        assert(p->second->check_rstats());
        sub_info.add(p->second->fnode.accounted_rstat);
      }
      // ...and that their sum matches our inode settings
      results->raw_rstats.memory_value = in->inode.rstat;
      sub_info.rsubdirs++; // it gets one to account for self
      if (!sub_info.same_sums(in->inode.rstat)) {
        results->raw_rstats.error_str
        << "freshly-calculated rstats don't match existing ones";
        return true;
      }
      results->raw_rstats.passed = true;
      // Hurray! We made it through!
      results->passed_validation = true;
      return true;
    }
  };


  ValidationContinuation *vc = new ValidationContinuation(this,
                                                          results,
                                                          mdr);
  vc->begin();
}

void CInode::validated_data::dump(Formatter *f) const
{
  f->open_object_section("results");
  {
    f->dump_bool("performed_validation", performed_validation);
    f->dump_bool("passed_validation", passed_validation);
    f->open_object_section("backtrace");
    {
      f->dump_bool("checked", backtrace.checked);
      f->dump_bool("passed", backtrace.passed);
      f->dump_int("read_ret_val", backtrace.ondisk_read_retval);
      f->dump_stream("ondisk_value") << backtrace.ondisk_value;
      f->dump_stream("memoryvalue") << backtrace.memory_value;
      f->dump_string("error_str", backtrace.error_str.str());
    }
    f->close_section(); // backtrace
    f->open_object_section("raw_rstats");
    {
      f->dump_bool("checked", raw_rstats.checked);
      f->dump_bool("passed", raw_rstats.passed);
      f->dump_int("read_ret_val", raw_rstats.ondisk_read_retval);
      f->dump_stream("ondisk_value") << raw_rstats.ondisk_value;
      f->dump_stream("memory_value") << raw_rstats.memory_value;
      f->dump_string("error_str", raw_rstats.error_str.str());
    }
    f->close_section(); // raw_rstats
    // dump failure return code
    int rc = 0;
    if (backtrace.checked && backtrace.ondisk_read_retval)
      rc = backtrace.ondisk_read_retval;
    if (inode.checked && inode.ondisk_read_retval)
      rc = inode.ondisk_read_retval;
    if (raw_rstats.checked && raw_rstats.ondisk_read_retval)
      rc = raw_rstats.ondisk_read_retval;
    f->dump_int("return_code", rc);
  }
  f->close_section(); // results
}

void CInode::dump(Formatter *f) const
{
  InodeStoreBase::dump(f);

  MDSCacheObject::dump(f);

  f->open_object_section("versionlock");
  versionlock.dump(f);
  f->close_section();

  f->open_object_section("authlock");
  authlock.dump(f);
  f->close_section();

  f->open_object_section("linklock");
  linklock.dump(f);
  f->close_section();

  f->open_object_section("dirfragtreelock");
  dirfragtreelock.dump(f);
  f->close_section();

  f->open_object_section("filelock");
  filelock.dump(f);
  f->close_section();

  f->open_object_section("xattrlock");
  xattrlock.dump(f);
  f->close_section();

  f->open_object_section("snaplock");
  snaplock.dump(f);
  f->close_section();

  f->open_object_section("nestlock");
  nestlock.dump(f);
  f->close_section();

  f->open_object_section("flocklock");
  flocklock.dump(f);
  f->close_section();

  f->open_object_section("policylock");
  policylock.dump(f);
  f->close_section();

  f->open_array_section("states");
  MDSCacheObject::dump_states(f);
  if (state_test(STATE_EXPORTING))
    f->dump_string("state", "exporting");
  if (state_test(STATE_OPENINGDIR))
    f->dump_string("state", "openingdir");
  if (state_test(STATE_FREEZING))
    f->dump_string("state", "freezing");
  if (state_test(STATE_FROZEN))
    f->dump_string("state", "frozen");
  if (state_test(STATE_AMBIGUOUSAUTH))
    f->dump_string("state", "ambiguousauth");
  if (state_test(STATE_EXPORTINGCAPS))
    f->dump_string("state", "exportingcaps");
  if (state_test(STATE_NEEDSRECOVER))
    f->dump_string("state", "needsrecover");
  if (state_test(STATE_PURGING))
    f->dump_string("state", "purging");
  if (state_test(STATE_DIRTYPARENT))
    f->dump_string("state", "dirtyparent");
  if (state_test(STATE_DIRTYRSTAT))
    f->dump_string("state", "dirtyrstat");
  if (state_test(STATE_STRAYPINNED))
    f->dump_string("state", "straypinned");
  if (state_test(STATE_FROZENAUTHPIN))
    f->dump_string("state", "frozenauthpin");
  if (state_test(STATE_DIRTYPOOL))
    f->dump_string("state", "dirtypool");
  if (state_test(STATE_ORPHAN))
    f->dump_string("state", "orphan");
  f->close_section();

  f->open_array_section("client_caps");
  for (map<client_t,Capability*>::const_iterator it = client_caps.begin();
       it != client_caps.end(); ++it) {
    f->open_object_section("client_cap");
    f->dump_int("client_id", it->first.v);
    f->dump_string("pending", ccap_string(it->second->pending()));
    f->dump_string("issued", ccap_string(it->second->issued()));
    f->dump_string("wanted", ccap_string(it->second->wanted()));
    f->dump_string("last_sent", ccap_string(it->second->get_last_sent()));
    f->close_section();
  }
  f->close_section();

  f->dump_int("loner", loner_cap.v);
  f->dump_int("want_loner", want_loner_cap.v);

  f->open_array_section("mds_caps_wanted");
  for (compact_map<int,int>::const_iterator p = mds_caps_wanted.begin();
       p != mds_caps_wanted.end(); ++p) {
    f->open_object_section("mds_cap_wanted");
    f->dump_int("rank", p->first);
    f->dump_string("cap", ccap_string(p->second));
    f->close_section();
  }
  f->close_section();
}

