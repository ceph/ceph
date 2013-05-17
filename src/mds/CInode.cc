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

#include <inttypes.h>
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

#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds." << mdcache->mds->get_nodeid() << ".cache.ino(" << inode.ino << ") "


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



ostream& operator<<(ostream& out, CInode& in)
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
    pair<int,int> a = in.authority();
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

  inode_t *pi = in.get_projected_inode();
  if (pi->is_truncating())
    out << " truncating(" << pi->truncate_from << " to " << pi->truncate_size << ")";

  // anchors
  if (in.is_anchored())
    out << " anc";
  if (in.get_nested_anchors())
    out << " na=" << in.get_nested_anchors();

  if (in.inode.is_dir()) {
    out << " " << in.inode.dirstat;
    if (g_conf->mds_debug_scatterstat && in.is_projected()) {
      inode_t *pi = in.get_projected_inode();
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
    inode_t *pi = in.get_projected_inode();
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
    for (map<client_t,Capability*>::iterator it = in.get_client_caps().begin();
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
    for (map<int,int>::iterator p = in.get_mds_caps_wanted().begin();
	 p != in.get_mds_caps_wanted().end(); ++p) {
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
  set<client_t>& clients = client_need_snapflush[snapid];
  clients.erase(client);
  if (clients.empty()) {
    client_need_snapflush.erase(snapid);
    snapin->auth_unpin(this);

    if (client_need_snapflush.empty()) {
      put(CInode::PIN_NEEDSNAPFLUSH);
      auth_unpin(this);
    }
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
  projected_nodes.back()->xattrs = px;
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
    xattrs = *px;
    delete px;
  }

  if (projected_nodes.front()->snapnode)
    pop_projected_snaprealm(projected_nodes.front()->snapnode);

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
    new_srnode->current_parent_since = snapid;
  }
  dout(10) << "project_snaprealm " << new_srnode << dendl;
  projected_nodes.back()->snapnode = new_srnode;
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

    // update parent pointer
    assert(snaprealm->open);
    assert(snaprealm->parent);   // had a parent before
    SnapRealm *new_parent = get_parent_inode()->find_snaprealm();
    assert(new_parent);
    CInode *parenti = new_parent->inode;
    assert(parenti);
    assert(parenti->snaprealm);
    snaprealm->parent = new_parent;
    snaprealm->add_open_past_parent(new_parent);
    dout(10) << " realm " << *snaprealm << " past_parents " << snaprealm->srnode.past_parents
	     << " -> " << next_snaprealm->past_parents << dendl;
    dout(10) << " pinning new parent " << *parenti << dendl;
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
  for (map<frag_t,CDir*>::iterator p = dirfrags.begin(); p != dirfrags.end(); ++p) {
    if (fg.contains(p->first))
      ls.push_back(p->second);
    else
      all = false;
  }
  /*
  list<frag_t> fglist;
  dirfragtree.get_leaves_under(fg, fglist);
  for (list<frag_t>::iterator p = fglist.begin();
       p != fglist.end();
       ++p) 
    if (dirfrags.count(*p))
      ls.push_back(dirfrags[*p]);
    else 
      all = false;
  */
  return all;
}

void CInode::verify_dirfrags()
{
  bool bad = false;
  for (map<frag_t,CDir*>::iterator p = dirfrags.begin(); p != dirfrags.end(); ++p) {
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
  for (map<frag_t,CDir*>::iterator p = dirfrags.begin(); p != dirfrags.end(); ++p) {
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
      mdcache->get_force_dirfrag(dirfrag_t(ino(),*p));
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
  while (1) {
    fg = fg.parent();
    dir = get_dirfrag(fg);
    if (dir) return dir;
  }
}	

void CInode::get_dirfrags(list<CDir*>& ls) 
{
  // all dirfrags
  for (map<frag_t,CDir*>::iterator p = dirfrags.begin();
       p != dirfrags.end();
       ++p)
    ls.push_back(p->second);
}
void CInode::get_nested_dirfrags(list<CDir*>& ls) 
{  
  // dirfrags in same subtree
  for (map<frag_t,CDir*>::iterator p = dirfrags.begin();
       p != dirfrags.end();
       ++p)
    if (!p->second->is_subtree_root())
      ls.push_back(p->second);
}
void CInode::get_subtree_dirfrags(list<CDir*>& ls) 
{ 
  // dirfrags that are roots of new subtrees
  for (map<frag_t,CDir*>::iterator p = dirfrags.begin();
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
    assert(is_auth());
    dir = new CDir(this, fg, mdcache, true);
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
  for (map<frag_t,CDir*>::iterator p = dirfrags.begin();
       p != dirfrags.end();
       ++p)
    if (p->second->is_subtree_root() &&
	(auth == -1 || p->second->dir_auth.first == auth))
      return true;
  return false;
}


void CInode::get_stickydirs()
{
  if (stickydir_ref == 0) {
    get(PIN_STICKYDIRS);
    for (map<frag_t,CDir*>::iterator p = dirfrags.begin();
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
    for (map<frag_t,CDir*>::iterator p = dirfrags.begin();
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

void CInode::make_path_string(string& s, bool force, CDentry *use_parent)
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
void CInode::make_path_string_projected(string& s)
{
  make_path_string(s);
  
  if (!projected_parent.empty()) {
    string q;
    q.swap(s);
    s = "{" + q;
    for (list<CDentry*>::iterator p = projected_parent.begin();
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

void CInode::make_path(filepath& fp)
{
  if (parent) 
    parent->make_path(fp);
  else
    fp = filepath(ino());
}

void CInode::make_anchor_trace(vector<Anchor>& trace)
{
  if (get_projected_parent_dn())
    get_projected_parent_dn()->make_anchor_trace(trace, this);
  else 
    assert(is_base());
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
  if (parent || !projected_parent.empty()) {
    pv = get_projected_parent_dn()->pre_dirty(get_projected_version());
    dout(10) << "pre_dirty " << pv << " (current v " << inode.version << ")" << dendl;
  } else {
    assert(is_base());
    pv = get_projected_version() + 1;
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

struct C_Inode_Stored : public Context {
  CInode *in;
  version_t version;
  Context *fin;
  C_Inode_Stored(CInode *i, version_t v, Context *f) : in(i), version(v), fin(f) {}
  void finish(int r) {
    in->_stored(version, fin);
  }
};

object_t CInode::get_object_name(inodeno_t ino, frag_t fg, const char *suffix)
{
  char n[60];
  snprintf(n, sizeof(n), "%llx.%08llx%s", (long long unsigned)ino, (long long unsigned)fg, suffix ? suffix : "");
  return object_t(n);
}

void CInode::store(Context *fin)
{
  dout(10) << "store " << get_version() << dendl;
  assert(is_base());

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

  mdcache->mds->objecter->mutate(oid, oloc, m, snapc, ceph_clock_now(g_ceph_context), 0,
				 NULL, new C_Inode_Stored(this, get_version(), fin) );
}

void CInode::_stored(version_t v, Context *fin)
{
  dout(10) << "_stored " << v << " " << *this << dendl;
  if (v == get_projected_version())
    mark_clean();

  fin->finish(0);
  delete fin;
}

struct C_Inode_Fetched : public Context {
  CInode *in;
  bufferlist bl, bl2;
  Context *fin;
  C_Inode_Fetched(CInode *i, Context *f) : in(i), fin(f) {}
  void finish(int r) {
    in->_fetched(bl, bl2, fin);
  }
};

void CInode::fetch(Context *fin)
{
  dout(10) << "fetch" << dendl;

  C_Inode_Fetched *c = new C_Inode_Fetched(this, fin);
  C_GatherBuilder gather(g_ceph_context, c);

  object_t oid = CInode::get_object_name(ino(), frag_t(), "");
  object_locator_t oloc(mdcache->mds->mdsmap->get_metadata_pool());

  ObjectOperation rd;
  rd.getxattr("inode", &c->bl, NULL);

  mdcache->mds->objecter->read(oid, oloc, rd, CEPH_NOSNAP, (bufferlist*)NULL, 0, gather.new_sub());

  // read from separate object too
  object_t oid2 = CInode::get_object_name(ino(), frag_t(), ".inode");
  mdcache->mds->objecter->read(oid2, oloc, 0, 0, CEPH_NOSNAP, &c->bl2, 0, gather.new_sub());

  gather.activate();
}

void CInode::_fetched(bufferlist& bl, bufferlist& bl2, Context *fin)
{
  dout(10) << "_fetched got " << bl.length() << " and " << bl2.length() << dendl;
  bufferlist::iterator p;
  if (bl2.length())
    p = bl2.begin();
  else
    p = bl.begin();
  string magic;
  ::decode(magic, p);
  dout(10) << " magic is '" << magic << "' (expecting '" << CEPH_FS_ONDISK_MAGIC << "')" << dendl;
  if (magic != CEPH_FS_ONDISK_MAGIC) {
    dout(0) << "on disk magic '" << magic << "' != my magic '" << CEPH_FS_ONDISK_MAGIC
	    << "'" << dendl;
    fin->finish(-EINVAL);
  } else {
    decode_store(p);
    dout(10) << "_fetched " << *this << dendl;
    fin->finish(0);
  }
  delete fin;
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
  vector<int64_t>::iterator i = inode.old_pools.begin();
  while(i != inode.old_pools.end()) {
    // don't add our own pool id to old_pools to avoid looping (e.g. setlayout 0, 1, 0)
    if (*i == pool) {
      ++i;
      continue;
    }
    bt.old_pools.insert(*i);
    ++i;
  }
}

struct C_Inode_StoredBacktrace : public Context {
  CInode *in;
  version_t version;
  Context *fin;
  C_Inode_StoredBacktrace(CInode *i, version_t v, Context *f) : in(i), version(v), fin(f) {}
  void finish(int r) {
    in->_stored_backtrace(version, fin);
  }
};

void CInode::store_backtrace(Context *fin)
{
  dout(10) << "store_backtrace on " << *this << dendl;
  assert(is_dirty_parent());

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
  op.create(false);
  op.setxattr("parent", bl);

  SnapContext snapc;
  object_t oid = get_object_name(ino(), frag_t(), "");
  object_locator_t oloc(pool);
  Context *fin2 = new C_Inode_StoredBacktrace(this, inode.backtrace_version, fin);

  if (!state_test(STATE_DIRTYPOOL)) {
    mdcache->mds->objecter->mutate(oid, oloc, op, snapc, ceph_clock_now(g_ceph_context),
				   0, NULL, fin2);
    return;
  }

  C_GatherBuilder gather(g_ceph_context, fin2);
  mdcache->mds->objecter->mutate(oid, oloc, op, snapc, ceph_clock_now(g_ceph_context),
				 0, NULL, gather.new_sub());

  set<int64_t> old_pools;
  for (vector<int64_t>::iterator p = inode.old_pools.begin();
      p != inode.old_pools.end();
      ++p) {
    if (*p == pool || old_pools.count(*p))
      continue;

    ObjectOperation op;
    op.create(false);
    op.setxattr("parent", bl);

    object_locator_t oloc(*p);
    mdcache->mds->objecter->mutate(oid, oloc, op, snapc, ceph_clock_now(g_ceph_context),
				   0, NULL, gather.new_sub());
    old_pools.insert(*p);
  }
  gather.activate();
}

void CInode::_stored_backtrace(version_t v, Context *fin)
{
  dout(10) << "_stored_backtrace" << dendl;

  if (v == inode.backtrace_version)
    clear_dirty_parent();
  auth_unpin(this);
  if (fin)
    fin->complete(0);
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

// ------------------
// parent dir

void CInode::encode_store(bufferlist& bl)
{
  ENCODE_START(4, 4, bl);
  ::encode(inode, bl);
  if (is_symlink())
    ::encode(symlink, bl);
  ::encode(dirfragtree, bl);
  ::encode(xattrs, bl);
  bufferlist snapbl;
  encode_snap_blob(snapbl);
  ::encode(snapbl, bl);
  ::encode(old_inodes, bl);
  ENCODE_FINISH(bl);
}

void CInode::decode_store(bufferlist::iterator& bl) {
  DECODE_START_LEGACY_COMPAT_LEN(4, 4, 4, bl);
  ::decode(inode, bl);
  if (is_symlink())
    ::decode(symlink, bl);
  ::decode(dirfragtree, bl);
  ::decode(xattrs, bl);
  bufferlist snapbl;
  ::decode(snapbl, bl);
  decode_snap_blob(snapbl);
  ::decode(old_inodes, bl);
  if (struct_v == 2 && inode.is_dir()) {
    bool default_layout_exists;
    ::decode(default_layout_exists, bl);
    if (default_layout_exists) {
      ::decode(struct_v, bl); // this was a default_file_layout
      ::decode(inode.layout, bl); // but we only care about the layout portion
    }
  }
  DECODE_FINISH(bl);
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
    ::encode(inode.ctime, bl);
    ::encode(inode.mode, bl);
    ::encode(inode.uid, bl);
    ::encode(inode.gid, bl);  
    break;
    
  case CEPH_LOCK_ILINK:
    ::encode(inode.ctime, bl);
    ::encode(inode.nlink, bl);
    ::encode(inode.anchored, bl);
    break;
    
  case CEPH_LOCK_IDFT:
    if (!is_auth()) {
      bool dirty = dirfragtreelock.is_dirty();
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
      ::encode(inode.mtime, bl);
      ::encode(inode.atime, bl);
      ::encode(inode.time_warp_seq, bl);
      if (!is_dir()) {
	::encode(inode.layout, bl);
	::encode(inode.size, bl);
	::encode(inode.client_ranges, bl);
      }
    } else {
      bool dirty = filelock.is_dirty();
      ::encode(dirty, bl);
    }

    {
      dout(15) << "encode_lock_state inode.dirstat is " << inode.dirstat << dendl;
      ::encode(inode.dirstat, bl);  // only meaningful if i am auth.
      bufferlist tmp;
      __u32 n = 0;
      for (map<frag_t,CDir*>::iterator p = dirfrags.begin();
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
    if (!is_auth()) {
      bool dirty = nestlock.is_dirty();
      ::encode(dirty, bl);
    }
    {
      dout(15) << "encode_lock_state inode.rstat is " << inode.rstat << dendl;
      ::encode(inode.rstat, bl);  // only meaningful if i am auth.
      bufferlist tmp;
      __u32 n = 0;
      for (map<frag_t,CDir*>::iterator p = dirfrags.begin();
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
    ::encode(xattrs, bl);
    break;

  case CEPH_LOCK_ISNAP:
    encode_snap(bl);
    break;

  case CEPH_LOCK_IFLOCK:
    ::encode(fcntl_locks, bl);
    ::encode(flock_locks, bl);
    break;

  case CEPH_LOCK_IPOLICY:
    if (inode.is_dir()) {
      ::encode(inode.layout, bl);
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
    ::decode(tm, p);
    if (inode.ctime < tm) inode.ctime = tm;
    ::decode(inode.mode, p);
    ::decode(inode.uid, p);
    ::decode(inode.gid, p);
    break;

  case CEPH_LOCK_ILINK:
    ::decode(tm, p);
    if (inode.ctime < tm) inode.ctime = tm;
    ::decode(inode.nlink, p);
    {
      bool was_anchored = inode.anchored;
      ::decode(inode.anchored, p);
      if (parent && was_anchored != inode.anchored)
	parent->adjust_nested_anchors((int)inode.anchored - (int)was_anchored);
    }
    break;

  case CEPH_LOCK_IDFT:
    if (is_auth()) {
      bool replica_dirty;
      ::decode(replica_dirty, p);
      if (replica_dirty) {
	dout(10) << "decode_lock_state setting dftlock dirty flag" << dendl;
	dirfragtreelock.mark_dirty();  // ok bc we're auth and caller will handle
      }
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
	for (map<frag_t,CDir*>::iterator p = dirfrags.begin();
	     p != dirfrags.end();
	     ++p)
	  if (!dirfragtree.is_leaf(p->first)) {
	    dout(10) << " forcing open dirfrag " << p->first << " to leaf (racing with split|merge)" << dendl;
	    dirfragtree.force_to_leaf(g_ceph_context, p->first);
	  }
      }
      if (g_conf->mds_debug_frag)
	verify_dirfrags();
    }
    break;

  case CEPH_LOCK_IFILE:
    if (!is_auth()) {
      ::decode(inode.mtime, p);
      ::decode(inode.atime, p);
      ::decode(inode.time_warp_seq, p);
      if (!is_dir()) {
	::decode(inode.layout, p);
	::decode(inode.size, p);
	::decode(inode.client_ranges, p);
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
	map<snapid_t,old_rstat_t> dirty_old_rstat;
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
    ::decode(xattrs, p);
    break;

  case CEPH_LOCK_ISNAP:
    {
      snapid_t seq = 0;
      if (snaprealm)
	seq = snaprealm->srnode.seq;
      decode_snap(p);
      if (snaprealm && snaprealm->srnode.seq != seq)
	mdcache->do_realm_invalidate_and_update_notify(this, seq ? CEPH_SNAP_OP_UPDATE:CEPH_SNAP_OP_SPLIT);
    }
    break;

  case CEPH_LOCK_IFLOCK:
    ::decode(fcntl_locks, p);
    ::decode(flock_locks, p);
    break;

  case CEPH_LOCK_IPOLICY:
    if (inode.is_dir()) {
      ::decode(inode.layout, p);
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

  for (map<frag_t,CDir*>::iterator p = dirfrags.begin();
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
    }
  }
}

struct C_Inode_FragUpdate : public Context {
  CInode *in;
  CDir *dir;
  Mutation *mut;

  C_Inode_FragUpdate(CInode *i, CDir *d, Mutation *m) : in(i), dir(d), mut(m) {}
  void finish(int r) {
    in->_finish_frag_update(dir, mut);
  }    
};

void CInode::finish_scatter_update(ScatterLock *lock, CDir *dir,
				   version_t inode_version, version_t dir_accounted_version)
{
  frag_t fg = dir->get_frag();
  assert(dir->is_auth());

  if (dir->is_frozen()) {
    dout(10) << "finish_scatter_update " << fg << " frozen, marking " << *lock << " stale " << *dir << dendl;
  } else {
    if (dir_accounted_version != inode_version) {
      dout(10) << "finish_scatter_update " << fg << " journaling accounted scatterstat update v" << inode_version << dendl;

      MDLog *mdlog = mdcache->mds->mdlog;
      Mutation *mut = new Mutation;
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

void CInode::_finish_frag_update(CDir *dir, Mutation *mut)
{
  dout(10) << "_finish_frag_update on " << *dir << dendl;
  mut->apply();
  mut->cleanup();
  delete mut;
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
  LogClient &clog = mdcache->mds->clog;

  dout(10) << "finish_scatter_gather_update " << type << " on " << *this << dendl;
  assert(is_auth());

  switch (type) {
  case CEPH_LOCK_IFILE:
    {
      // adjust summation
      assert(is_auth());
      inode_t *pi = get_projected_inode();

      bool touched_mtime = false;
      dout(20) << "  orig dirstat " << pi->dirstat << dendl;
      pi->dirstat.version++;
      for (map<frag_t,CDir*>::iterator p = dirfrags.begin();
	   p != dirfrags.end();
	   ++p) {
	frag_t fg = p->first;
	CDir *dir = p->second;
	dout(20) << fg << " " << *dir << dendl;

	bool update = dir->is_auth() && !dir->is_frozen();

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
	  clog.error() << "bad/negative dir size on "
	      << dir->dirfrag() << " " << pf->fragstat << "\n";
	  
	  if (pf->fragstat.nfiles < 0)
	    pf->fragstat.nfiles = 0;
	  if (pf->fragstat.nsubdirs < 0)
	    pf->fragstat.nsubdirs = 0;

	  assert(!"bad/negative frag size" == g_conf->mds_verify_scatter);
	}

	if (update) {
	  pf->accounted_fragstat = pf->fragstat;
	  pf->fragstat.version = pf->accounted_fragstat.version = pi->dirstat.version;
	  dout(10) << fg << " updated accounted_fragstat " << pf->fragstat << " on " << *dir << dendl;
	}

	if (fg == frag_t()) { // i.e., we are the only frag
	  if (pi->dirstat.size() != pf->fragstat.size()) {
	    clog.error() << "unmatched fragstat size on single "
	       << "dirfrag " << dir->dirfrag() << ", inode has " 
	       << pi->dirstat << ", dirfrag has " << pf->fragstat << "\n";
	    
	    // trust the dirfrag for now
	    version_t v = pi->dirstat.version;
	    pi->dirstat = pf->fragstat;
	    pi->dirstat.version = v;

	    assert(!"unmatched fragstat size" == g_conf->mds_verify_scatter);
	  }
	}
      }
      if (touched_mtime)
	pi->mtime = pi->ctime = pi->dirstat.mtime;
      dout(20) << " final dirstat " << pi->dirstat << dendl;

      if (pi->dirstat.nfiles < 0 ||
	  pi->dirstat.nsubdirs < 0) {
	clog.error() << "bad/negative dir size on " << ino()
	    << ", inode has " << pi->dirstat << "\n";

	if (pi->dirstat.nfiles < 0)
	  pi->dirstat.nfiles = 0;
	if (pi->dirstat.nsubdirs < 0)
	  pi->dirstat.nsubdirs = 0;

	assert(!"bad/negative dir size" == g_conf->mds_verify_scatter);
      }
    }
    break;

  case CEPH_LOCK_INEST:
    {
      // adjust summation
      assert(is_auth());
      inode_t *pi = get_projected_inode();
      dout(20) << "  orig rstat " << pi->rstat << dendl;
      pi->rstat.version++;
      for (map<frag_t,CDir*>::iterator p = dirfrags.begin();
	   p != dirfrags.end();
	   ++p) {
	frag_t fg = p->first;
	CDir *dir = p->second;
	dout(20) << fg << " " << *dir << dendl;

	bool update = dir->is_auth() && !dir->is_frozen();

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
	  for (map<snapid_t,old_rstat_t>::iterator q = dir->dirty_old_rstat.begin();
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
	  dout(10) << fg << " updated accounted_rstat " << pf->rstat << " on " << *dir << dendl;
	}

	if (fg == frag_t()) { // i.e., we are the only frag
	  if (pi->rstat.rbytes != pf->rstat.rbytes) { 
	    clog.error() << "unmatched rstat rbytes on single dirfrag "
		<< dir->dirfrag() << ", inode has " << pi->rstat
		<< ", dirfrag has " << pf->rstat << "\n";
	    
	    // trust the dirfrag for now
	    version_t v = pi->rstat.version;
	    pi->rstat = pf->rstat;
	    pi->rstat.version = v;
	    
	    assert(!"unmatched rstat rbytes" == g_conf->mds_verify_scatter);
	  }
	}
	if (update)
	  dir->check_rstats();
      }
      dout(20) << " final rstat " << pi->rstat << dendl;

      //assert(pi->rstat.rfiles >= 0);
      if (pi->rstat.rfiles < 0) {
	clog.error() << "rfiles underflow " << pi->rstat.rfiles
		     << " on " << *this << "\n";
	pi->rstat.rfiles = 0;
      }

      //assert(pi->rstat.rsubdirs >= 0);
      if (pi->rstat.rsubdirs < 0) {
	clog.error() << "rsubdirs underflow " << pi->rstat.rsubdirs
		     << " on " << *this << "\n";
	pi->rstat.rsubdirs = 0;
      }
    }
    break;

  case CEPH_LOCK_IDFT:
    break;

  default:
    assert(0);
  }
}

void CInode::finish_scatter_gather_update_accounted(int type, Mutation *mut, EMetaBlob *metablob)
{
  dout(10) << "finish_scatter_gather_update_accounted " << type << " on " << *this << dendl;
  assert(is_auth());

  for (map<frag_t,CDir*>::iterator p = dirfrags.begin();
       p != dirfrags.end();
       ++p) {
    CDir *dir = p->second;
    if (!dir->is_auth() || dir->is_frozen())
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

bool CInode::is_frozen()
{
  if (is_frozen_inode()) return true;
  if (parent && parent->dir->is_frozen()) return true;
  return false;
}

bool CInode::is_frozen_dir()
{
  if (parent && parent->dir->is_frozen_dir()) return true;
  return false;
}

bool CInode::is_freezing()
{
  if (is_freezing_inode()) return true;
  if (parent && parent->dir->is_freezing()) return true;
  return false;
}

void CInode::add_waiter(uint64_t tag, Context *c) 
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

void CInode::unfreeze_inode(list<Context*>& finished) 
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
    list<Context*> finished;
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
    list<Context*> finished;
    take_waiting(WAIT_UNFREEZE, finished);
    mdcache->mds->queue_waiters(finished);
  }
}

void CInode::clear_ambiguous_auth(list<Context*>& finished)
{
  assert(state_test(CInode::STATE_AMBIGUOUSAUTH));
  state_clear(CInode::STATE_AMBIGUOUSAUTH);
  take_waiting(CInode::WAIT_SINGLEAUTH, finished);
}

void CInode::clear_ambiguous_auth()
{
  list<Context*> finished;
  clear_ambiguous_auth(finished);
  mdcache->mds->queue_waiters(finished);
}

// auth_pins
bool CInode::can_auth_pin() {
  if (is_freezing_inode() || is_frozen_inode() || is_frozen_auth_pin())
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
    for (map<frag_t,CDir*>::iterator p = dirfrags.begin();
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

void CInode::adjust_nested_anchors(int by)
{
  assert(by);
  nested_anchors += by;
  dout(20) << "adjust_nested_anchors by " << by << " -> " << nested_anchors << dendl;
  assert(nested_anchors >= 0);
  if (parent)
    parent->adjust_nested_anchors(by);
}

// authority

pair<int,int> CInode::authority() 
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
  return MIN(t, first);
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
  
  dout(10) << " " << px->size() << " xattrs cowed, " << *px << dendl;

  old.inode.trim_client_ranges(follows);

  if (!(old.inode.rstat == old.inode.accounted_rstat))
    dirty_old_rstats.insert(follows);
  
  first = follows+1;

  dout(10) << "cow_old_inode " << (cow_head ? "head" : "previous_head" )
	   << " to [" << old.first << "," << follows << "] on "
	   << *this << dendl;

  return old;
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

  map<snapid_t,old_inode_t>::iterator p = old_inodes.begin();
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
  map<snapid_t, old_inode_t>::iterator p = old_inodes.lower_bound(snap);  // p is first key >= to snap
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
    } else {
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
  if (session)
    session->add_cap(cap);
  
  cap->client_follows = first-1;
  
  containing_realm->add_cap(client, cap);
  
  return cap;
}

void CInode::remove_client_cap(client_t client)
{
  assert(client_caps.count(client) == 1);
  Capability *cap = client_caps[client];
  
  cap->item_session_caps.remove_myself();
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
  bool fcntl_removed = fcntl_locks.remove_all_from(client);
  bool flock_removed = flock_locks.remove_all_from(client);
  if (fcntl_removed || flock_removed) {
    list<Context*> waiters;
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
  }
  cap->set_cap_id(icr.cap_id);
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
int CInode::get_caps_liked()
{
  if (is_dir())
    return CEPH_CAP_PIN | CEPH_CAP_ANY_EXCL | CEPH_CAP_ANY_SHARED;  // but not, say, FILE_RD|WR|WRBUFFER
  else
    return CEPH_CAP_ANY & ~CEPH_CAP_FILE_LAZYIO;
}

int CInode::get_caps_allowed_ever()
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

int CInode::get_caps_allowed_by_type(int type)
{
  return 
    CEPH_CAP_PIN |
    (filelock.gcaps_allowed(type) << filelock.get_cap_shift()) |
    (authlock.gcaps_allowed(type) << authlock.get_cap_shift()) |
    (xattrlock.gcaps_allowed(type) << xattrlock.get_cap_shift()) |
    (linklock.gcaps_allowed(type) << linklock.get_cap_shift());
}

int CInode::get_caps_careful()
{
  return 
    (filelock.gcaps_careful() << filelock.get_cap_shift()) |
    (authlock.gcaps_careful() << authlock.get_cap_shift()) |
    (xattrlock.gcaps_careful() << xattrlock.get_cap_shift()) |
    (linklock.gcaps_careful() << linklock.get_cap_shift());
}

int CInode::get_xlocker_mask(client_t client)
{
  return 
    (filelock.gcaps_xlocker_mask(client) << filelock.get_cap_shift()) |
    (authlock.gcaps_xlocker_mask(client) << authlock.get_cap_shift()) |
    (xattrlock.gcaps_xlocker_mask(client) << xattrlock.get_cap_shift()) |
    (linklock.gcaps_xlocker_mask(client) << linklock.get_cap_shift());
}

int CInode::get_caps_allowed_for_client(client_t client)
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
  return allowed;
}

// caps issued, wanted
int CInode::get_caps_issued(int *ploner, int *pother, int *pxlocker,
			    int shift, int mask)
{
  int c = 0;
  int loner = 0, other = 0, xlocker = 0;
  if (!is_auth())
    loner_cap = -1;
  for (map<client_t,Capability*>::iterator it = client_caps.begin();
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

bool CInode::is_any_caps_wanted()
{
  for (map<client_t,Capability*>::iterator it = client_caps.begin();
       it != client_caps.end();
       ++it)
    if (it->second->wanted())
      return true;
  return false;
}

int CInode::get_caps_wanted(int *ploner, int *pother, int shift, int mask)
{
  int w = 0;
  int loner = 0, other = 0;
  for (map<client_t,Capability*>::iterator it = client_caps.begin();
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
    for (map<int,int>::iterator it = mds_caps_wanted.begin();
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
			      snapid_t snapid, unsigned max_bytes)
{
  int client = session->info.inst.name.num();
  assert(snapid);
  assert(session->connection);
  
  bool valid = true;

  // do not issue caps if inode differs from readdir snaprealm
  SnapRealm *realm = find_snaprealm();
  bool no_caps = (realm && dir_realm && realm != dir_realm) ||
		 is_frozen() || state_test(CInode::STATE_EXPORTINGCAPS);
  if (no_caps)
    dout(20) << "encode_inodestat no caps"
	     << ((realm && dir_realm && realm != dir_realm)?", snaprealm differs ":"")
	     << (state_test(CInode::STATE_EXPORTINGCAPS)?", exporting caps":"")
	     << (is_frozen()?", frozen inode":"") << dendl;

  // pick a version!
  inode_t *oi = &inode;
  inode_t *pi = get_projected_inode();

  map<string, bufferptr> *pxattrs = 0;

  if (snapid != CEPH_NOSNAP && is_multiversion()) {

    // for now at least, old_inodes is only defined/valid on the auth
    if (!is_auth())
      valid = false;

    map<snapid_t,old_inode_t>::iterator p = old_inodes.lower_bound(snapid);
    if (p != old_inodes.end()) {
      if (p->second.first > snapid) {
        if  (p != old_inodes.begin())
          --p;
        else dout(0) << "old_inode lower_bound starts after snapid!" << dendl;
      }
      dout(15) << "encode_inodestat snapid " << snapid
	       << " to old_inode [" << p->second.first << "," << p->first << "]" 
	       << " " << p->second.inode.rstat
	       << dendl;
      assert(p->second.first <= snapid && snapid <= p->first);
      pi = oi = &p->second.inode;
      pxattrs = &p->second.xattrs;
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

  // nest (do same as file... :/)
  i->rstat.rctime.encode_timeval(&e.rctime);
  e.rbytes = i->rstat.rbytes;
  e.rfiles = i->rstat.rfiles;
  e.rsubdirs = i->rstat.rsubdirs;

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
  bool had_latest_xattrs = cap && (cap->issued() & CEPH_CAP_XATTR_SHARED) &&
    cap->client_xattr_version == i->xattr_version &&
    snapid == CEPH_NOSNAP;

  // xattr
  bufferlist xbl;
  e.xattr_version = i->xattr_version;
  if (!had_latest_xattrs) {
    if (!pxattrs)
      pxattrs = pxattr ? get_projected_xattrs() : &xattrs;
    ::encode(*pxattrs, xbl);
  }
  
  // do we have room?
  if (max_bytes) {
    unsigned bytes = sizeof(e);
    bytes += sizeof(__u32);
    bytes += (sizeof(__u32) + sizeof(__u32)) * dirfragtree._splits.size();
    bytes += sizeof(__u32) + symlink.length();
    bytes += sizeof(__u32) + xbl.length();
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
      e.cap.caps = issue;
      e.cap.wanted = cap->wanted();
      e.cap.cap_id = cap->get_cap_id();
      e.cap.seq = cap->get_last_seq();
      dout(10) << "encode_inodestat issueing " << ccap_string(issue) << " seq " << cap->get_last_seq() << dendl;
      e.cap.mseq = cap->get_mseq();
      e.cap.realm = realm->inode->ino();
    } else {
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

  // include those xattrs?
  if (xbl.length() && cap) {
    if (cap->pending() & CEPH_CAP_XATTR_SHARED) {
      dout(10) << "including xattrs version " << i->xattr_version << dendl;
      cap->client_xattr_version = i->xattr_version;
    } else {
      dout(10) << "dropping xattrs version " << i->xattr_version << dendl;
      xbl.clear(); // no xattrs .. XXX what's this about?!?
    }
  }

  // encode
  e.fragtree.nsplits = dirfragtree._splits.size();
  ::encode(e, bl);
  for (map<frag_t,int32_t>::iterator p = dirfragtree._splits.begin();
       p != dirfragtree._splits.end();
       ++p) {
    ::encode(p->first, bl);
    ::encode(p->second, bl);
  }
  ::encode(symlink, bl);
  if (session->connection->has_feature(CEPH_FEATURE_DIRLAYOUTHASH)) {
    i = pfile ? pi : oi;
    ::encode(i->dir_layout, bl);
  }
  ::encode(xbl, bl);

  return valid;
}

void CInode::encode_cap_message(MClientCaps *m, Capability *cap)
{
  client_t client = cap->get_client();

  bool pfile = filelock.is_xlocked_by_client(client) ||
    (cap && (cap->issued() & CEPH_CAP_FILE_EXCL));
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
  bool was_anchored = inode.anchored;
  ::decode(inode, p);
  if (parent && was_anchored != inode.anchored)
    parent->adjust_nested_anchors((int)inode.anchored - (int)was_anchored);

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
void CInode::_decode_locks_rejoin(bufferlist::iterator& p, list<Context*>& waiters)
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
}


// IMPORT/EXPORT

void CInode::encode_export(bufferlist& bl)
{
  ENCODE_START(4, 4, bl)
  _encode_base(bl);

  ::encode(state, bl);

  ::encode(pop, bl);

  ::encode(replica_map, bl);

  // include scatterlock info for any bounding CDirs
  bufferlist bounding;
  if (inode.is_dir())
    for (map<frag_t,CDir*>::iterator p = dirfrags.begin();
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
  get(PIN_TEMPEXPORTING);
  ENCODE_FINISH(bl);
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
  DECODE_START_LEGACY_COMPAT_LEN(4, 4, 4, p);

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

  if (struct_v >= 2) {
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
  }

  _decode_locks_full(p);
  DECODE_FINISH(p);
}
