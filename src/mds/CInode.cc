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



#include "CInode.h"
#include "CDir.h"
#include "CDentry.h"

#include "MDS.h"
#include "MDCache.h"

#include "snap.h"

#include "LogSegment.h"

#include "common/Clock.h"

#include "messages/MLock.h"

#include <string>
#include <stdio.h>

#include "config.h"

#define DOUT_SUBSYS mds
#undef dout_prefix
#define dout_prefix *_dout << dbeginl << "mds" << mdcache->mds->get_nodeid() << ".cache.ino(" << inode.ino << ") "



//int cinode_pins[CINODE_NUM_PINS];  // counts
ostream& CInode::print_db_line_prefix(ostream& out)
{
  return out << g_clock.now() << " mds" << mdcache->mds->get_nodeid() << ".cache.ino(" << inode.ino << ") ";
}



ostream& operator<<(ostream& out, CInode& in)
{
  filepath path;
  in.make_path(path);
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
    assert(in.get_replica_nonce() >= 0);
  }

  if (in.is_symlink())
    out << " symlink='" << in.symlink << "'";
  if (in.is_dir() && !in.dirfragtree.empty())
    out << " " << in.dirfragtree;
  
  out << " v" << in.get_version();

  if (in.is_auth_pinned()) {
    out << " ap=" << in.get_num_auth_pins();
#ifdef MDS_AUTHPIN_SET
    out << "(" << in.auth_pin_set << ")";
#endif
  }

  if (in.snaprealm)
    out << " snaprealm=" << in.snaprealm;

  if (in.state_test(CInode::STATE_AMBIGUOUSAUTH)) out << " AMBIGAUTH";
  if (in.state_test(CInode::STATE_NEEDSRECOVER)) out << " needsrecover";
  if (in.state_test(CInode::STATE_RECOVERING)) out << " recovering";
  if (in.is_freezing_inode()) out << " FREEZING=" << in.auth_pin_freeze_allowance;
  if (in.is_frozen_inode()) out << " FROZEN";

  if (in.get_nested_anchors())
    out << " na=" << in.get_nested_anchors();

  if (in.inode.is_dir()) {
    out << " " << in.inode.dirstat;
    out << " ds=" << in.inode.dirstat.size() << "=" 
	<< in.inode.dirstat.nfiles << "+" << in.inode.dirstat.nsubdirs;
    //if (in.inode.dirstat.version > 10000) out << " BADDIRSTAT";
  } else {
    out << " s=" << in.inode.size;
    if (in.inode.max_size)
      out << "/" << in.inode.max_size;
    out << " nl=" << in.inode.nlink;
  }

  out << " rb=" << in.inode.rstat.rbytes;
  if (in.is_projected()) out << "/" << in.inode.accounted_rstat.rbytes;
  out << " rf=" << in.inode.rstat.rfiles;
  if (in.is_projected()) out << "/" << in.inode.accounted_rstat.rfiles;
  out << " rd=" << in.inode.rstat.rsubdirs;
  if (in.is_projected()) out << "/" << in.inode.accounted_rstat.rsubdirs;
  
  // locks
  out << " " << in.authlock;
  out << " " << in.linklock;
  if (in.inode.is_dir()) {
    out << " " << in.dirfragtreelock;
    out << " " << in.snaplock;
    out << " " << in.nestlock;
  }
  out << " " << in.filelock;
  out << " " << in.xattrlock;
  
  // hack: spit out crap on which clients have caps
  if (!in.get_client_caps().empty()) {
    out << " caps={";
    for (map<int,Capability*>::iterator it = in.get_client_caps().begin();
         it != in.get_client_caps().end();
         it++) {
      if (it != in.get_client_caps().begin()) out << ",";
      out << it->first << "=" << ccap_string(it->second->issued())
	  << "/" << ccap_string(it->second->wanted());
    }
    out << "}";
    if (in.get_loner() >= 0)
      out << ",l=" << in.get_loner();
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


inode_t *CInode::project_inode() 
{
  if (projected_inode.empty()) {
    projected_inode.push_back(new inode_t(inode));
  } else {
    projected_inode.push_back(new inode_t(*projected_inode.back()));
  }
  dout(15) << "project_inode " << projected_inode.back() << dendl;
  return projected_inode.back();
}

void CInode::pop_and_dirty_projected_inode(LogSegment *ls) 
{
  assert(!projected_inode.empty());
  dout(15) << "pop_and_dirty_projected_inode " << projected_inode.front()
	   << " v" << projected_inode.front()->version << dendl;
  mark_dirty(projected_inode.front()->version, ls);
  inode = *projected_inode.front();
  delete projected_inode.front();
  projected_inode.pop_front();
}


// ====== CInode =======

// dirfrags

frag_t CInode::pick_dirfrag(const nstring& dn)
{
  if (dirfragtree.empty())
    return frag_t();          // avoid the string hash if we can.

  __u32 h = ceph_full_name_hash(dn.data(), dn.length());
  return dirfragtree[h];
}

void CInode::get_dirfrags_under(frag_t fg, list<CDir*>& ls)
{
  list<frag_t> fglist;
  dirfragtree.get_leaves_under(fg, fglist);
  for (list<frag_t>::iterator p = fglist.begin();
       p != fglist.end();
       ++p) 
    if (dirfrags.count(*p))
      ls.push_back(dirfrags[*p]);
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

bool CInode::has_subtree_root_dirfrag()
{
  for (map<frag_t,CDir*>::iterator p = dirfrags.begin();
       p != dirfrags.end();
       ++p)
    if (p->second->is_subtree_root())
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
CInode *CInode::get_parent_inode() 
{
  if (parent) 
    return parent->dir->inode;
  return NULL;
}


bool CInode::is_ancestor_of(CInode *other)
{
  while (other) {
    if (other == this)
      return true;
    if (!other->get_parent_dn())
      break;
    other = other->get_parent_dn()->get_dir()->get_inode();
  }
  return false;
}

void CInode::make_path_string(string& s)
{
  if (parent) {
    parent->make_path_string(s);
  } 
  else if (is_root()) {
    s = "";  // root
  } 
  else if (is_stray()) {
    s = "~stray";
    char n[10];
    sprintf(n, "%d", (int)(ino()-MDS_INO_STRAY_OFFSET));
    s += n;
  }
  else {
    s = "(dangling)";  // dangling
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
  if (parent)
    parent->make_anchor_trace(trace, this);
  else 
    assert(is_root() || is_stray());
}

void CInode::name_stray_dentry(string& dname)
{
  char s[20];
#ifdef __LP64__
  sprintf(s, "%lx", inode.ino.val);
#else
  sprintf(s, "%llx", inode.ino.val);
#endif
  dname = s;
}


version_t CInode::pre_dirty()
{    
  assert(parent || projected_parent);
  version_t pv;
  if (projected_parent)
    pv = projected_parent->pre_dirty(get_projected_version());
  else
    pv = parent->pre_dirty();
  dout(10) << "pre_dirty " << pv << " (current v " << inode.version << ")" << dendl;
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
    ls->dirty_inodes.push_back(&xlist_dirty);
}

void CInode::mark_dirty(version_t pv, LogSegment *ls) {
  
  dout(10) << "mark_dirty " << *this << dendl;

  assert(parent || projected_parent);

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
  parent->mark_dirty(pv, ls);
}


void CInode::mark_clean()
{
  dout(10) << " mark_clean " << *this << dendl;
  if (state_test(STATE_DIRTY)) {
    state_clear(STATE_DIRTY);
    put(PIN_DIRTY);
    
    // remove myself from ls dirty list
    xlist_dirty.remove_myself();
  }
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
      ::encode(inode.layout, bl);
      ::encode(inode.size, bl);
      ::encode(inode.max_size, bl);
      ::encode(inode.mtime, bl);
      ::encode(inode.atime, bl);
      ::encode(inode.time_warp_seq, bl);
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
	  dout(15) << fg << " " << *dir << dendl;
	  dout(20) << fg << "           fragstat " << dir->fnode.fragstat << dendl;
	  dout(20) << fg << " accounted_fragstat " << dir->fnode.accounted_fragstat << dendl;
	  ::encode(fg, tmp);
	  ::encode(dir->first, tmp);
	  ::encode(dir->fnode.fragstat, tmp);
	  ::encode(dir->fnode.accounted_fragstat, tmp);
	  n++;
	}
      }
      ::encode(n, bl);
      bl.claim_append(tmp);
    }
    break;

  case CEPH_LOCK_INEST:
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
	  dout(10) << fg << " " << *dir << dendl;
	  dout(10) << fg << " " << dir->fnode.rstat << dendl;
	  dout(10) << fg << " " << dir->fnode.rstat << dendl;
	  dout(10) << fg << " " << dir->dirty_old_rstat << dendl;
	  ::encode(fg, tmp);
	  ::encode(dir->first, tmp);
	  ::encode(dir->fnode.rstat, tmp);
	  ::encode(dir->fnode.accounted_rstat, tmp);
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

  
  default:
    assert(0);
  }
}

void CInode::decode_lock_state(int type, bufferlist& bl)
{
  bufferlist::iterator p = bl.begin();
  utime_t tm;

  snapid_t newfirst;
  ::decode(newfirst, p);

  if (!is_auth() && newfirst != first) {
    dout(10) << "decode_lock_state first " << first << " -> " << newfirst << dendl;
    assert(newfirst > first);
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
	    dirfragtree.force_to_leaf(*p);
	    dirfragtreelock.set_updated();
	  }
      } else {
	// replica.  take the new tree, BUT make sure any open
	//  dirfrags remain leaves (they may have split _after_ this
	//  dft was scattered, or we may still be be waiting on the
	//  notify from the auth)
	dirfragtree.swap(temp);
	for (map<frag_t,CDir*>::iterator p = dirfrags.begin();
	     p != dirfrags.end();
	     p++)
	  if (!dirfragtree.is_leaf(p->first)) {
	    dout(10) << " forcing open dirfrag " << p->first << " to leaf (racing with split|merge)" << dendl;
	    dirfragtree.force_to_leaf(p->first);
	  }
      }
    }
    break;

  case CEPH_LOCK_IFILE:
    if (!is_auth()) {
      ::decode(inode.layout, p);
      ::decode(inode.size, p);
      ::decode(inode.max_size, p);
      ::decode(inode.mtime, p);
      ::decode(inode.atime, p);
      ::decode(inode.time_warp_seq, p);
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
	    filelock.set_updated();
	  }
	} else {
	  if (dir && dir->is_auth()) {
	    dout(10) << fg << " first " << dir->first << " -> " << fgfirst
		     << " on " << *dir << dendl;
	    dir->first = fgfirst;

	    dout(10) << fg << " setting accounted_fragstat and setting dirty bit" << dendl;
	    fnode_t *pf = dir->get_projected_fnode();
	    pf->accounted_fragstat = fragstat;
	    pf->fragstat.version = fragstat.version;
	    assert(pf->fragstat == fragstat);
	    dir->_set_dirty_flag();	    // bit of a hack
	  }
	}
      }
    }
    break;

  case CEPH_LOCK_INEST:
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
	  if (!(rstat == accounted_rstat) || dir->dirty_old_rstat.size()) {
	    dout(10) << fg << " setting nestlock updated flag" << dendl;
	    nestlock.set_updated();
	  }
	} else {
	  if (dir && dir->is_auth()) {
	    dout(10) << fg << " first " << dir->first << " -> " << fgfirst
		     << " on " << *dir << dendl;
	    dir->first = fgfirst;
	    
	    dout(10) << fg << " resetting accounted_rstat and setting dirty bit" << dendl;
	    fnode_t *pf = dir->get_projected_fnode();
	    pf->accounted_rstat = rstat;
	    pf->rstat.version = rstat.version;
	    dir->dirty_old_rstat.clear();
	    dir->_set_dirty_flag();	    // bit of a hack, FIXME?
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
	seq = snaprealm->seq;
      decode_snap(p);
      if (snaprealm && snaprealm->seq != seq)
	mdcache->do_realm_invalidate_and_update_notify(this, seq ? CEPH_SNAP_OP_UPDATE:CEPH_SNAP_OP_SPLIT);
    }
    break;

  default:
    assert(0);
  }
}

void CInode::clear_dirty_scattered(int type)
{
  dout(10) << "clear_dirty_scattered " << type << " on " << *this << dendl;
  switch (type) {
  case CEPH_LOCK_IFILE:
    xlist_dirty_dirfrag_dir.remove_myself();
    break;

  case CEPH_LOCK_INEST:
    xlist_dirty_dirfrag_nest.remove_myself();
    break;

  case CEPH_LOCK_IDFT:
    xlist_dirty_dirfrag_dirfragtree.remove_myself();
    break;

  default:
    assert(0);
  }
}


void CInode::finish_scatter_gather_update(int type)
{
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
      for (map<frag_t,CDir*>::iterator p = dirfrags.begin();
	   p != dirfrags.end();
	   p++) {
	frag_t fg = p->first;
	CDir *dir = p->second;
	dout(20) << fg << " " << *dir << dendl;
	fnode_t *pf = dir->get_projected_fnode();
	if (pf->accounted_fragstat.version == pi->dirstat.version) {
	  dout(20) << fg << "           fragstat " << pf->fragstat << dendl;
	  dout(20) << fg << " accounted_fragstat " << pf->accounted_fragstat << dendl;
	  pi->dirstat.take_diff(pf->fragstat, pf->accounted_fragstat, touched_mtime);
	} else {
	  dout(20) << fg << " skipping OLD accounted_fragstat " << pf->accounted_fragstat << dendl;
	  pf->accounted_fragstat = pf->fragstat;
	}
	pf->fragstat.version = pf->accounted_fragstat.version = pi->dirstat.version + 1;
      }
      if (touched_mtime)
	pi->mtime = pi->ctime = pi->dirstat.mtime;
      pi->dirstat.version++;
      dout(20) << " final dirstat " << pi->dirstat << dendl;
      assert(pi->dirstat.size() >= 0);
      assert(pi->dirstat.nfiles >= 0);
      assert(pi->dirstat.nsubdirs >= 0);
    }
    break;

  case CEPH_LOCK_INEST:
    {
      // adjust summation
      assert(is_auth());
      inode_t *pi = get_projected_inode();
      dout(20) << "  orig rstat " << pi->rstat << dendl;
      for (map<frag_t,CDir*>::iterator p = dirfrags.begin();
	   p != dirfrags.end();
	   p++) {
	frag_t fg = p->first;
	CDir *dir = p->second;
	dout(20) << fg << " " << *dir << dendl;
	fnode_t *pf = dir->get_projected_fnode();
	if (pf->accounted_rstat.version == pi->rstat.version) {
	  dout(20) << fg << "           rstat " << pf->rstat << dendl;
	  dout(20) << fg << " accounted_rstat " << pf->accounted_rstat << dendl;
	  dout(20) << fg << " dirty_old_rstat " << dir->dirty_old_rstat << dendl;
	  mdcache->project_rstat_frag_to_inode(pf->rstat, pf->accounted_rstat,
					       dir->first, CEPH_NOSNAP, this, true);
	  for (map<snapid_t,old_rstat_t>::iterator q = dir->dirty_old_rstat.begin();
	       q != dir->dirty_old_rstat.end();
	       q++)
	    mdcache->project_rstat_frag_to_inode(q->second.rstat, q->second.accounted_rstat,
						 q->second.first, q->first, this, true);
	} else {
	  dout(20) << fg << " skipping OLD accounted_rstat " << pf->accounted_rstat << dendl;
	  pf->accounted_rstat = pf->rstat;
	}
	dir->dirty_old_rstat.clear();
	pf->rstat.version = pf->accounted_rstat.version = pi->rstat.version + 1;
      }
      pi->rstat.version++;
      dout(20) << " final rstat " << pi->rstat << dendl;
      assert(pi->rstat.rfiles >= 0);
      assert(pi->rstat.rsubdirs >= 0);
    }
    break;

  case CEPH_LOCK_IDFT:
    break;

  default:
    assert(0);
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

void CInode::add_waiter(__u64 tag, Context *c) 
{
  dout(10) << "add_waiter tag " << tag 
	   << " !ambig " << !state_test(STATE_AMBIGUOUSAUTH)
	   << " !frozen " << !is_frozen_inode()
	   << " !freezing " << !is_freezing_inode()
	   << dendl;
  // wait on the directory?
  //  make sure its not the inode that is explicitly ambiguous|freezing|frozen
  if (((tag & WAIT_SINGLEAUTH) && !state_test(STATE_AMBIGUOUSAUTH)) ||
      ((tag & WAIT_UNFREEZE) && !is_frozen_inode() && !is_freezing_inode())) {
    parent->dir->add_waiter(tag, c);
    return;
  }
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
  get(PIN_FROZEN);
  state_set(STATE_FROZEN);
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


// auth_pins
bool CInode::can_auth_pin() {
  if (is_freezing_inode() || is_frozen_inode()) return false;
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
    parent->adjust_nested_auth_pins(1, 1);
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
    parent->adjust_nested_auth_pins(-1, -1);

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

void CInode::adjust_nested_auth_pins(int a)
{
  nested_auth_pins += a;
  dout(35) << "adjust_nested_auth_pins by " << a
	   << " now " << auth_pins << "+" << nested_auth_pins
	   << dendl;
  assert(nested_auth_pins >= 0);

  if (parent)
    parent->adjust_nested_auth_pins(a, 0);
}

void CInode::adjust_nested_anchors(int by)
{
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

  return CDIR_AUTH_UNDEF;
}


// SNAP

snapid_t CInode::get_oldest_snap()
{
  snapid_t t = CEPH_NOSNAP;
  if (!old_inodes.empty())
    t = old_inodes.begin()->second.first;
  return MIN(t, first);
}


old_inode_t& CInode::cow_old_inode(snapid_t follows, inode_t *pi)
{
  assert(follows >= first);

  old_inode_t &old = old_inodes[follows];
  old.first = first;
  old.inode = *pi;
  old.xattrs = xattrs;
  
  if (!(old.inode.rstat == old.inode.accounted_rstat))
    dirty_old_rstats.insert(follows);
  
  first = follows+1;

  dout(10) << "cow_old_inode to [" << old.first << "," << follows << "] on "
	   << *this << dendl;

  return old;
}

void CInode::pre_cow_old_inode()
{
  snapid_t follows = find_snaprealm()->get_newest_snap();
  if (first <= follows)
    cow_old_inode(follows, get_projected_inode());
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
      p++;
  }
}

/*
 * pick/create an old_inode that we can write into.
 */
/*map<snapid_t,old_inode_t>::iterator CInode::pick_dirty_old_inode(snapid_t last)
{
  dout(10) << "pick_dirty_old_inode last " << last << dendl;
  SnapRealm *realm = find_snaprealm();
  dout(10) << " realm " << *realm << dendl;
  const set<snapid_t>& snaps = realm->get_snaps();
  dout(10) << " snaps " << snaps << dendl;
  
  //snapid_t snap = snaps.lower_bound(last);


  
}
*/

void CInode::open_snaprealm(bool nosplit)
{
  if (!snaprealm) {
    SnapRealm *parent = find_snaprealm();
    snaprealm = new SnapRealm(mdcache, this);
    if (parent) {
      dout(10) << "open_snaprealm " << snaprealm
	       << " parent is " << parent
	       << " siblings are " << parent->open_children
	       << dendl;
      snaprealm->parent = parent;
      if (!nosplit && 
	  is_dir() &&
	  !get_parent_dir()->get_inode()->is_stray())  // optimization
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
    else if (cur == this && get_projected_parent_dn())
      cur = get_projected_parent_dn()->get_dir()->get_inode();
    else
      break;
  }
  return cur->snaprealm;
}

void CInode::encode_snap_blob(bufferlist &snapbl)
{
  if (snaprealm) {
    ::encode(*snaprealm, snapbl);
    dout(20) << "encode_snap_blob " << *snaprealm << dendl;
  }
}
void CInode::decode_snap_blob(bufferlist& snapbl) 
{
  if (snapbl.length()) {
    open_snaprealm();
    bufferlist::iterator p = snapbl.begin();
    ::decode(*snaprealm, p);
    dout(20) << "decode_snap_blob " << *snaprealm << dendl;
  }
}


bool CInode::encode_inodestat(bufferlist& bl, Session *session,
			      snapid_t snapid, bool projected)
{
  int client = session->inst.name.num();

  bool valid = true;

  // pick a version!
  inode_t *i;
  if (projected)
    i = get_projected_inode();
  else 
    i = &inode;

  map<string, bufferptr> *pxattrs = &xattrs;

  if (snapid && is_multiversion()) {

    // for now at least, old_inodes is only defined/valid on the auth
    if (!is_auth())
      valid = false;

    map<snapid_t,old_inode_t>::iterator p = old_inodes.lower_bound(snapid);
    if (p != old_inodes.end()) {
      dout(15) << "encode_inodestat snapid " << snapid
	       << " to old_inode [" << p->second.first << "," << p->first << "]" 
	       << " " << p->second.inode.rstat
	       << dendl;
      assert(p->second.first <= snapid && snapid <= p->first);
      i = &p->second.inode;
      pxattrs = &p->second.xattrs;
    }
  }
  
  /*
   * note: encoding matches struct ceph_client_reply_inode
   */
  struct ceph_mds_reply_inode e;
  memset(&e, 0, sizeof(e));
  e.ino = i->ino;
  e.snapid = snapid ? (__u64)snapid:CEPH_NOSNAP;  // 0 -> NOSNAP
  e.version = i->version;
  e.layout = i->layout;
  e.size = i->size;
  e.max_size = i->max_size;
  e.truncate_seq = i->truncate_seq;
  i->ctime.encode_timeval(&e.ctime);
  i->mtime.encode_timeval(&e.mtime);
  i->atime.encode_timeval(&e.atime);
  e.time_warp_seq = i->time_warp_seq;
  e.mode = i->mode;
  e.uid = i->uid;
  e.gid = i->gid;
  e.nlink = i->nlink;
  
  e.files = i->dirstat.nfiles;
  e.subdirs = i->dirstat.nsubdirs;
  i->rstat.rctime.encode_timeval(&e.rctime);
  e.rbytes = i->rstat.rbytes;
  e.rfiles = i->rstat.rfiles;
  e.rsubdirs = i->rstat.rsubdirs;
  
  e.rdev = i->rdev;
  e.fragtree.nsplits = dirfragtree._splits.size();

  Capability *cap = get_client_cap(client);

  bool had_latest_xattrs = cap && (cap->issued() & CEPH_CAP_XATTR_RDCACHE) &&
    cap->client_xattr_version == i->xattr_version;
  
  // include capability?
  if (snapid != CEPH_NOSNAP && !cap) {
    e.cap.caps = valid ? get_caps_allowed(false) : CEPH_STAT_CAP_INODE;
    e.cap.seq = 0;
    e.cap.mseq = 0;
    e.cap.realm = 0;
  } else {
    if (valid && !cap && is_auth())
      // add a new cap
      cap = add_client_cap(client, session, &mdcache->client_rdcaps, find_snaprealm());

    if (cap && valid) {
      bool loner = (get_loner() == client);
      int likes = get_caps_liked();
      int issue = (cap->wanted() | likes) & get_caps_allowed(loner);
      if (loner && (issue & CEPH_CAP_FILE_EXCL))
	issue |= CEPH_CAP_AUTH_EXCL; // | CEPH_CAP_XATTR_EXCL; 
      cap->issue(issue);
      cap->set_last_issue_stamp(g_clock.recent_now());
      cap->touch();   // move to back of session cap LRU
      e.cap.caps = issue;
      e.cap.wanted = cap->wanted();
      e.cap.seq = cap->get_last_seq();
      dout(10) << "encode_inodestat issueing " << ccap_string(issue) << " seq " << cap->get_last_seq() << dendl;
      e.cap.mseq = cap->get_mseq();
      e.cap.realm = find_snaprealm()->inode->ino();
      e.cap.ttl_ms = g_conf.mds_rdcap_ttl_ms;
    } else {
      e.cap.caps = 0;
      e.cap.seq = 0;
      e.cap.mseq = 0;
      e.cap.realm = 0;
      e.cap.wanted = 0;
      e.cap.ttl_ms = 0;
    }
  }
  dout(10) << "encode_inodestat caps " << ccap_string(e.cap.caps)
	   << " seq " << e.cap.seq
	   << " mseq " << e.cap.mseq << dendl;

  // xattr
  bufferlist xbl;
  e.xattr_version = i->xattr_version;
  if (!had_latest_xattrs &&
      cap &&
      (cap->pending() & CEPH_CAP_XATTR_RDCACHE)) {
    ::encode(*pxattrs, xbl);
    if (cap)
      cap->client_xattr_version = i->xattr_version;
    dout(10) << "including xattrs version " << i->xattr_version << dendl;
  }

  // encode
  ::encode(e, bl);
  for (map<frag_t,int32_t>::iterator p = dirfragtree._splits.begin();
       p != dirfragtree._splits.end();
       p++) {
    ::encode(p->first, bl);
    ::encode(p->second, bl);
  }
  ::encode(symlink, bl);
  ::encode(xbl, bl);

  return valid;
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
}


// IMPORT/EXPORT

void CInode::encode_export(bufferlist& bl)
{
  _encode_base(bl);

  bool dirty = is_dirty();
  ::encode(dirty, bl);

  ::encode(pop, bl);

  ::encode(replica_map, bl);

  _encode_locks_full(bl);
  get(PIN_TEMPEXPORTING);
}

void CInode::finish_export(utime_t now)
{
  pop.zero(now);

  // just in case!
  //dirlock.clear_updated();

  loner_cap = -1;

  put(PIN_TEMPEXPORTING);
}

void CInode::decode_import(bufferlist::iterator& p,
			   LogSegment *ls)
{
  _decode_base(p);

  bool dirty;
  ::decode(dirty, p);
  if (dirty) 
    _mark_dirty(ls);

  ::decode(pop, p);

  ::decode(replica_map, p);
  if (!replica_map.empty()) get(PIN_REPLICATED);

  _decode_locks_full(p);
}
