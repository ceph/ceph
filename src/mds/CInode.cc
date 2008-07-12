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
#include "AnchorTable.h"

#include "snap.h"

#include "LogSegment.h"

#include "common/Clock.h"

#include "messages/MLock.h"

#include <string>
#include <stdio.h>

#include "config.h"

#define dout(x)  if (x <= g_conf.debug || x <= g_conf.debug_mds) *_dout << dbeginl << g_clock.now() << " mds" << mdcache->mds->get_nodeid() << ".cache.ino(" << inode.ino << ") "


//int cinode_pins[CINODE_NUM_PINS];  // counts
ostream& CInode::print_db_line_prefix(ostream& out)
{
  return out << g_clock.now() << " mds" << mdcache->mds->get_nodeid() << ".cache.ino(" << inode.ino << ") ";
}



ostream& operator<<(ostream& out, CInode& in)
{
  filepath path;
  in.make_path(path);
  out << "[inode " << in.inode.ino << " " << path << (in.is_dir() ? "/":"");
  if (in.is_multiversion())
    out << " [" << in.first << ",...]";
  else 
    out << " [" << in.first << "," << in.last << "]";

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

  out << " rb=" << in.inode.dirstat.rbytes;
  if (in.is_projected()) out << "/" << in.inode.accounted_dirstat.rbytes;
  out << " rf=" << in.inode.dirstat.rfiles;
  if (in.is_projected()) out << "/" << in.inode.accounted_dirstat.rfiles;
  out << " rd=" << in.inode.dirstat.rsubdirs;
  if (in.is_projected()) out << "/" << in.inode.accounted_dirstat.rsubdirs;
  
  // locks
  out << " " << in.authlock;
  out << " " << in.linklock;
  if (in.inode.is_dir()) {
    out << " " << in.dirfragtreelock;
    out << " " << in.dirlock;
    out << " " << in.snaplock;
  } else
    out << " " << in.filelock;
  out << " " << in.xattrlock;
  
  // hack: spit out crap on which clients have caps
  /*
  if (!in.get_client_caps().empty()) {
    out << " caps={";
    for (map<int,Capability*>::iterator it = in.get_client_caps().begin();
         it != in.get_client_caps().end();
         it++) {
      if (it != in.get_client_caps().begin()) out << ",";
      out << it->first << "=" << it->second->issued();
    }
    out << "}";
  }
  */

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
}

void CInode::encode_lock_state(int type, bufferlist& bl)
{
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
    ::encode(inode.size, bl);
    ::encode(inode.max_size, bl);
    ::encode(inode.mtime, bl);
    ::encode(inode.atime, bl);
    ::encode(inode.time_warp_seq, bl);
    break;

  case CEPH_LOCK_IDIR:
    {
      dout(15) << "encode_lock_state inode.dirstat is " << inode.dirstat << dendl;
      ::encode(inode.dirstat, bl);  // only meaningful if i am auth.
      bufferlist tmp;
      __u32 n = 0;
      for (map<frag_t,CDir*>::iterator p = dirfrags.begin();
	   p != dirfrags.end();
	   ++p)
	if (is_auth() || p->second->is_auth()) {
	  dout(15) << "encode_lock_state fragstat for " << *p->second << dendl;
	  dout(20) << "             fragstat " << p->second->fnode.fragstat << dendl;
	  dout(20) << "   accounted_fragstat " << p->second->fnode.accounted_fragstat << dendl;
	  frag_t fg = p->second->dirfrag().frag;
	  ::encode(fg, tmp);
	  ::encode(p->second->fnode.fragstat, tmp);
	  ::encode(p->second->fnode.accounted_fragstat, tmp);
	  n++;
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
    ::decode(inode.size, p);
    ::decode(inode.max_size, p);
    ::decode(inode.mtime, p);
    ::decode(inode.atime, p);
    ::decode(inode.time_warp_seq, p);
    break;

  case CEPH_LOCK_IDIR:
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
	frag_info_t fragstat;
	frag_info_t accounted_fragstat;
	::decode(fg, p);
	::decode(fragstat, p);
	::decode(accounted_fragstat, p);
	dout(10) << fg << " got changed fragstat " << fragstat << dendl;
	dout(20) << fg << "   accounted_fragstat " << accounted_fragstat << dendl;

	CDir *dir = get_dirfrag(fg);
	if (is_auth()) {
	  assert(dir);                // i am auth; i had better have this dir open
	  dout(10) << " " << fg << "           fragstat " << fragstat << " on " << *dir << dendl;
	  dout(20) << " " << fg << " accounted_fragstat " << accounted_fragstat << dendl;
	  dir->fnode.fragstat = fragstat;
	  dir->fnode.accounted_fragstat = accounted_fragstat;
	  if (!(fragstat == accounted_fragstat))
	    dirlock.set_updated();
	} else {
	  if (dir &&
	      dir->is_auth() &&
	      !(dir->fnode.accounted_fragstat == fragstat)) {
	    dout(10) << " setting accounted_fragstat " << fragstat << " and setting dirty bit on "
		     << *dir << dendl;
	    fnode_t *pf = dir->get_projected_fnode();
	    pf->accounted_fragstat = fragstat;
	    if (dir->is_auth())
	      dir->_set_dirty_flag();	    // bit of a hack
	  }
	}
      }
    }
    break;

  case CEPH_LOCK_IXATTR:
    ::decode(xattrs, p);
    break;

  case CEPH_LOCK_ISNAP:
    decode_snap(p);
    break;

  default:
    assert(0);
  }
}

void CInode::clear_dirty_scattered(int type)
{
  dout(10) << "clear_dirty_scattered " << type << " on " << *this << dendl;
  switch (type) {
  case CEPH_LOCK_IDIR:
    xlist_dirty_dirfrag_dir.remove_myself();
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
  switch (type) {
  case CEPH_LOCK_IDIR:
    {
      // adjust summation
      assert(is_auth());
      inode_t *pi = get_projected_inode();
      bool touched_mtime = false;
      dout(20) << "         orig dirstat " << pi->dirstat << dendl;
      for (map<frag_t,CDir*>::iterator p = dirfrags.begin();
	   p != dirfrags.end();
	   p++) {
	fnode_t *pf = p->second->get_projected_fnode();
	if (pf->accounted_fragstat.version == pi->dirstat.version) {
	  dout(20) << "  frag " << p->first << " " << *p->second << dendl;
	  dout(20) << "             fragstat " << pf->fragstat << dendl;
	  dout(20) << "   accounted_fragstat " << pf->fragstat << dendl;
	  pi->dirstat.take_diff(pf->fragstat, 
				pf->accounted_fragstat, touched_mtime);
	} else {
	  dout(20) << "  frag " << p->first << " on " << *p->second << dendl;
	  dout(20) << "    ignoring OLD accounted_fragstat " << pf->fragstat << dendl;
	}
      }
      if (touched_mtime)
	pi->mtime = pi->ctime = pi->dirstat.mtime;
      pi->dirstat.version++;
      dout(20) << "        final dirstat " << pi->dirstat << dendl;
      assert(pi->dirstat.size() >= 0);
      assert(pi->dirstat.nfiles >= 0);
      assert(pi->dirstat.nsubdirs >= 0);
    }
    break;

  case CEPH_LOCK_IDFT:
    {
      assert(is_auth());
    }
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

void CInode::add_waiter(int tag, Context *c) 
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
  if (!parent) return;
  nested_auth_pins += a;

  dout(35) << "adjust_nested_auth_pins by " << a
	   << " now " << auth_pins << "+" << nested_auth_pins
	   << dendl;
  assert(nested_auth_pins >= 0);

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


CInodeDiscover* CInode::replicate_to( int rep )
{
  assert(is_auth());

  // relax locks?
  if (!is_replicated())
    replicate_relax_locks();
  
  // return the thinger
  int nonce = add_replica( rep );
  return new CInodeDiscover( this, nonce );
}


// SNAP

snapid_t CInode::get_oldest_snap()
{
  snapid_t t = CEPH_NOSNAP;
  if (!old_inodes.empty())
    t = old_inodes.begin()->second.first;
  return MIN(t, first);
}

void CInode::open_snaprealm()
{
  if (!snaprealm) {
    SnapRealm *parent = find_snaprealm();
    snaprealm = new SnapRealm(mdcache, this);
    if (parent) {
      snaprealm->parent = parent;
      parent->open_children.insert(snaprealm);
      dout(10) << " opened snaprealm " << snaprealm
	       << " parent is " << parent
	       << " siblings are " << parent->open_children
	       << dendl;
    }
  }
}
void CInode::close_snaprealm()
{
  if (snaprealm) {
    if (snaprealm->parent)
      snaprealm->parent->open_children.erase(snaprealm);
    delete snaprealm;
    snaprealm = 0;
  }
}

/*
 * note: this is _not_ inclusive of *this->snaprealm, as that is for
 * nested directory content.
 */ 
SnapRealm *CInode::find_snaprealm()
{
  CInode *cur = this;
  while (cur->get_parent_dn() && !cur->snaprealm)
    cur = cur->get_parent_dn()->get_dir()->get_inode();
  return cur->snaprealm;
}

void CInode::encode_snap(bufferlist &bl)
{
  bufferlist snapbl;
  if (snaprealm)
    ::encode(snaprealm, snapbl);
  ::encode(snapbl, bl);
}

void CInode::decode_snap(bufferlist& snapbl) 
{
  if (snapbl.length()) {
    open_snaprealm();
    bufferlist::iterator p = snapbl.begin();
    ::decode(*snaprealm, p);
  }
}


void CInode::encode_inodestat(bufferlist& bl, snapid_t snapid)
{
  // pick a version!
  inode_t *i = &inode;
  bufferlist xbl;
  if (!old_inodes.empty()) {
    map<snapid_t,old_inode_t>::iterator p = old_inodes.lower_bound(snapid);
    if (p != old_inodes.end()) {
      assert(p->second.first <= snapid && snapid <= p->first);
      i = &p->second.inode;
      ::encode(p->second.xattrs, xbl);
    }
  }
  
  /*
   * note: encoding matches struct ceph_client_reply_inode
   */
  struct ceph_mds_reply_inode e;
  memset(&e, 0, sizeof(e));
  e.ino = i->ino;
  e.snapid = snapid;
  e.version = i->version;
  e.layout = i->layout;
  i->ctime.encode_timeval(&e.ctime);
  i->mtime.encode_timeval(&e.mtime);
  i->atime.encode_timeval(&e.atime);
  e.time_warp_seq = i->time_warp_seq;
  e.mode = i->mode;
  e.uid = i->uid;
  e.gid = i->gid;
  e.nlink = i->nlink;
  e.size = i->size;
  e.max_size = i->max_size;
  
  e.files = i->dirstat.nfiles;
  e.subdirs = i->dirstat.nsubdirs;
  i->dirstat.rctime.encode_timeval(&e.rctime);
  e.rbytes = i->dirstat.rbytes;
  e.rfiles = i->dirstat.rfiles;
  e.rsubdirs = i->dirstat.rsubdirs;
  
  e.rdev = i->rdev;
  e.fragtree.nsplits = dirfragtree._splits.size();
  ::encode(e, bl);
  for (map<frag_t,int32_t>::iterator p = dirfragtree._splits.begin();
       p != dirfragtree._splits.end();
       p++) {
    ::encode(p->first, bl);
    ::encode(p->second, bl);
  }
  ::encode(symlink, bl);
  
  if (!xattrs.empty() && xbl.length() == 0)
    ::encode(xattrs, xbl);
  ::encode(xbl, bl);
}




// IMPORT/EXPORT

void CInode::encode_export(bufferlist& bl)
{
  ::encode(inode, bl);
  ::encode(symlink, bl);
  ::encode(dirfragtree, bl);
  ::encode(xattrs, bl);
  encode_snap(bl);
  
  bool dirty = is_dirty();
  ::encode(dirty, bl);

  ::encode(pop, bl);
 
  ::encode(replica_map, bl);

  ::encode(authlock, bl);
  ::encode(linklock, bl);
  ::encode(dirfragtreelock, bl);
  ::encode(filelock, bl);
  ::encode(dirlock, bl);
  ::encode(xattrlock, bl);
  ::encode(snaplock, bl);

  get(PIN_TEMPEXPORTING);
}

void CInode::finish_export(utime_t now)
{
  pop.zero(now);

  // just in case!
  //dirlock.clear_updated();

  put(PIN_TEMPEXPORTING);
}

void CInode::decode_import(bufferlist::iterator& p,
			   LogSegment *ls)
{
  bool was_anchored = inode.anchored;
  ::decode(inode, p);
  if (parent && was_anchored != inode.anchored)
    parent->adjust_nested_anchors((int)inode.anchored - (int)was_anchored);

  ::decode(symlink, p);
  ::decode(dirfragtree, p);
  ::decode(xattrs, p);
  decode_snap(p);

  bool dirty;
  ::decode(dirty, p);
  if (dirty) 
    _mark_dirty(ls);

  ::decode(pop, p);

  ::decode(replica_map, p);
  if (!replica_map.empty()) get(PIN_REPLICATED);

  ::decode(authlock, p);
  ::decode(linklock, p);
  ::decode(dirfragtreelock, p);
  ::decode(filelock, p);
  ::decode(dirlock, p);
  ::decode(xattrlock, p);
  ::decode(snaplock, p);
}
