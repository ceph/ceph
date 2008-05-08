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
  out << "[inode " << in.inode.ino << " " << path << (in.is_dir() ? "/ ":" ");
  if (in.is_auth()) {
    out << "auth";
    if (in.is_replicated()) 
      out << in.get_replicas();
  } else {
    out << "rep@" << in.authority();
    out << "." << in.get_replica_nonce();
    assert(in.get_replica_nonce() >= 0);
  }

  if (in.is_symlink()) out << " symlink='" << in.symlink << "'";
  if (in.is_dir() && !in.dirfragtree.empty()) out << " " << in.dirfragtree;
  
  out << " v" << in.get_version();

  if (in.state_test(CInode::STATE_AMBIGUOUSAUTH)) out << " AMBIGAUTH";
  if (in.state_test(CInode::STATE_NEEDSRECOVER)) out << " needsrecover";
  if (in.state_test(CInode::STATE_RECOVERING)) out << " recovering";
  if (in.is_freezing_inode()) out << " FREEZING=" << in.auth_pin_freeze_allowance;
  if (in.is_frozen_inode()) out << " FROZEN";

  // locks
  out << " " << in.authlock;
  out << " " << in.linklock;
  out << " " << in.dirfragtreelock;
  out << " " << in.filelock;
  out << " " << in.dirlock;
  out << " " << in.xattrlock;
  
  if (in.inode.max_size)
    out << " size=" << in.inode.size << "/" << in.inode.max_size;

  // hack: spit out crap on which clients have caps
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

frag_t CInode::pick_dirfrag(const string& dn)
{
  if (dirfragtree.empty())
    return frag_t();          // avoid the string hash if we can.

  __u32 h = ceph_full_name_hash((const unsigned char *)dn.data(), dn.length());
  return dirfragtree[h*h];
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
  if (parent) {
    parent->dir->inode->make_anchor_trace(trace);
    trace.push_back(Anchor(ino(), parent->dir->dirfrag()));
    dout(10) << "make_anchor_trace added " << trace.back() << dendl;
  }
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
  assert(parent);
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

  assert(parent);

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
    ::encode(inode.mtime, bl);
    ::encode(inode.atime, bl);
    break;

  case CEPH_LOCK_IDIR:
    ::encode(inode.mtime, bl);
    if (0) {
      map<frag_t,int> frag_sizes;
      for (map<frag_t,CDir*>::iterator p = dirfrags.begin();
	   p != dirfrags.end();
	   ++p) 
	if (p->second->is_auth()) {
	  //frag_t fg = (*p)->get_frag();
	  //frag_sizes[f] = dirfrag_size[fg];
	}
      ::encode(frag_sizes, bl);
    }
    break;

  case CEPH_LOCK_IXATTR:
    ::encode(xattrs, bl);
    break;
  
  case CEPH_LOCK_INESTED:
    //_encode(inode.nested_ctime, bl);
    //_encode(inode.nested_size, bl);
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
    ::decode(inode.anchored, p);
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
	  dirfragtree.force_to_leaf(*p);
      } else {
	// replica.  just take the tree.
	dirfragtree.swap(temp);
      }
    }
    break;

  case CEPH_LOCK_IFILE:
    ::decode(inode.size, p);
    ::decode(inode.mtime, p);
    ::decode(inode.atime, p);
    break;

  case CEPH_LOCK_IDIR:
    //::_decode(inode.size, p);
    ::decode(tm, p);
    if (inode.mtime < tm) {
      inode.mtime = tm;
      if (is_auth()) {
	dout(10) << "decode_lock_state auth got mtime " << tm << " > my " << inode.mtime
		 << ", setting dirlock updated flag on " << *this
		 << dendl;
	dirlock.set_updated();
      }
    }
    if (0) {
      map<frag_t,int> dfsz;
      ::decode(dfsz, p);
      // hmm which to keep?
    }
    break;

  case CEPH_LOCK_IXATTR:
    ::decode(xattrs, p);
    break;

  case CEPH_LOCK_INESTED:
    // ***
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
    xlist_dirty_inode_mtime.remove_myself();
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

void CInode::auth_pin() 
{
  if (auth_pins == 0)
    get(PIN_AUTHPIN);
  auth_pins++;

  dout(10) << "auth_pin on " << *this
	   << " now " << auth_pins << "+" << nested_auth_pins
	   << dendl;
  
  if (parent)
    parent->adjust_nested_auth_pins( 1 );
}

void CInode::auth_unpin() 
{
  auth_pins--;
  if (auth_pins == 0)
    put(PIN_AUTHPIN);
  
  dout(10) << "auth_unpin on " << *this
	   << " now " << auth_pins << "+" << nested_auth_pins
	   << dendl;
  
  assert(auth_pins >= 0);

  if (parent)
    parent->adjust_nested_auth_pins( -1 );

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

  dout(15) << "adjust_nested_auth_pins by " << a
	   << " now " << auth_pins << "+" << nested_auth_pins
	   << dendl;
  assert(nested_auth_pins >= 0);

  parent->adjust_nested_auth_pins(a);
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




// IMPORT/EXPORT

void CInode::encode_export(bufferlist& bl)
{
  ::encode(inode, bl);
  ::encode(symlink, bl);
  ::encode(dirfragtree, bl);
  ::encode(xattrs, bl);

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

  get(PIN_TEMPEXPORTING);
}

void CInode::finish_export(utime_t now)
{
  pop.zero(now);

  // just in case!
  dirlock.clear_updated();

  put(PIN_TEMPEXPORTING);
}

void CInode::decode_import(bufferlist::iterator& p,
			   LogSegment *ls)
{
  utime_t old_mtime = inode.mtime;
  ::decode(inode, p);
  if (old_mtime > inode.mtime) {
    assert(dirlock.is_updated());
    inode.mtime = old_mtime;     // preserve our mtime, if it is larger
  }

  ::decode(symlink, p);
  ::decode(dirfragtree, p);
  ::decode(xattrs, p);

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
}
