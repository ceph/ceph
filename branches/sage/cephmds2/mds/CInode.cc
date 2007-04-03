// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
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

#include "common/Clock.h"

#include "messages/MLock.h"

#include <string>
#include <sstream>

#include "config.h"
#undef dout
#define dout(x)  if (x <= g_conf.debug || x <= g_conf.debug_mds) cout << g_clock.now() << " mds" << mdcache->mds->get_nodeid() << ".cache.ino(" << inode.ino << ") "


//int cinode_pins[CINODE_NUM_PINS];  // counts
ostream& CInode::print_db_line_prefix(ostream& out)
{
  return out << g_clock.now() << " mds" << mdcache->mds->get_nodeid() << ".cache.ino(" << inode.ino << ") ";
}



ostream& operator<<(ostream& out, CInode& in)
{
  string path;
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

  if (in.is_symlink()) out << " symlink";

  out << " v" << in.get_version();

  out << " auth=" << in.authlock;
  out << " link=" << in.linklock;
  out << " dft=" << in.dirfragtreelock;
  out << " file=" << in.filelock;
  
  if (in.get_num_ref()) {
    out << " |";
    in.print_pin_set(out);
  }

  // hack: spit out crap on which clients have caps
  if (!in.get_client_caps().empty()) {
    out << " caps={";
    for (map<int,Capability>::iterator it = in.get_client_caps().begin();
         it != in.get_client_caps().end();
         it++) {
      if (it != in.get_client_caps().begin()) out << ",";
      out << it->first;
    }
    out << "}";
  }
  out << " " << &in;
  out << "]";
  return out;
}


void CInode::print(ostream& out)
{
  out << *this;
}


// ====== CInode =======
CInode::CInode(MDCache *c, bool auth) : 
  authlock(this, LOCK_OTYPE_IAUTH, WAIT_AUTHLOCK_OFFSET),
  linklock(this, LOCK_OTYPE_ILINK, WAIT_LINKLOCK_OFFSET),
  dirfragtreelock(this, LOCK_OTYPE_IDIRFRAGTREE, WAIT_DIRFRAGTREELOCK_OFFSET),
  filelock(this, LOCK_OTYPE_IFILE, WAIT_FILELOCK_OFFSET)
{
  mdcache = c;

  //num_parents = 0;
  parent = NULL;
  
  auth_pins = 0;
  nested_auth_pins = 0;
  //num_request_pins = 0;

  state = 0;  

  if (auth) state_set(STATE_AUTH);
}

CInode::~CInode() {
  for (map<frag_t,CDir*>::iterator p = dirfrags.begin();
       p != dirfrags.end();
       ++p)
    delete p->second;
}


// dirfrags

frag_t CInode::pick_dirfrag(const string& dn)
{
  if (dirfragtree.empty())
    return frag_t();          // avoid the string hash if we can.

  static hash<string> H;
  return dirfragtree[H(dn)];
}

void CInode::get_dirfrags(list<CDir*>& ls) 
{
  for (map<frag_t,CDir*>::iterator p = dirfrags.begin();
       p != dirfrags.end();
       ++p)
    ls.push_back(p->second);
}
void CInode::get_nested_dirfrags(list<CDir*>& ls) 
{  // same subtree
  for (map<frag_t,CDir*>::iterator p = dirfrags.begin();
       p != dirfrags.end();
       ++p)
    if (!p->second->is_subtree_root())
      ls.push_back(p->second);
}
void CInode::get_subtree_dirfrags(list<CDir*>& ls) 
{  // new subtree
  for (map<frag_t,CDir*>::iterator p = dirfrags.begin();
       p != dirfrags.end();
       ++p)
    if (p->second->is_subtree_root())
      ls.push_back(p->second);
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
  if (parent) {
    parent->put(CDentry::PIN_INODEPIN);
  } 
  //if (num_parents == 0 && get_num_ref() == 0)
  //mdcache->inode_expire_queue.push_back(this);  // queue myself for garbage collection
}

/*
void CInode::get_parent()
{
  num_parents++;
}
void CInode::put_parent()
{
  num_parents--;
  if (num_parents == 0 && get_num_ref() == 0)
    mdcache->inode_expire_queue.push_back(this);    // queue myself for garbage collection
}
*/

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

CDir *CInode::get_or_open_dirfrag(MDCache *mdcache, frag_t fg)
{
  assert(is_dir());

  // have it?
  CDir *dir = get_dirfrag(fg);
  if (dir) return dir;
  
  // create it.
  assert(is_auth());
  dir = dirfrags[fg] = new CDir(this, fg, mdcache, true);
  return dir;
}

CDir *CInode::add_dirfrag(CDir *dir)
{
  assert(dirfrags.count(dir->dirfrag().frag) == 0);
  dirfrags[dir->dirfrag().frag] = dir;
  return dir;
}

void CInode::close_dirfrag(frag_t fg)
{
  assert(dirfrags.count(fg));
  
  dirfrags[fg]->remove_null_dentries();
  
  assert(dirfrags[fg]->get_num_ref() == 0);
  delete dirfrags[fg];
  dirfrags.erase(fg);
}

void CInode::close_dirfrags()
{
  while (!dirfrags.empty()) 
    close_dirfrag(dirfrags.begin()->first);
}



void CInode::make_path(string& s)
{
  if (parent) {
    parent->make_path(s);
  } 
  else if (is_root()) {
    s = "";  // root
  } 
  else if (is_stray()) {
    s = "~";
  }
  else {
    s = "(dangling)";  // dangling
  }
}

void CInode::make_anchor_trace(vector<Anchor>& trace)
{
  if (parent) {
    parent->dir->inode->make_anchor_trace(trace);
    trace.push_back(Anchor(ino(), parent->dir->dirfrag()));
    dout(10) << "make_anchor_trace added " << trace.back() << endl;
  }
  else 
    assert(is_root());
}

void CInode::name_stray_dentry(string& dname)
{
  stringstream ss;
  ss << inode.ino;
  ss >> dname;
}


version_t CInode::pre_dirty()
{    
  assert(parent);
  return parent->pre_dirty();
}

void CInode::_mark_dirty()
{
  if (!state_test(STATE_DIRTY)) {
    state_set(STATE_DIRTY);
    get(PIN_DIRTY);
  }
}

void CInode::mark_dirty(version_t pv) {
  
  dout(10) << "mark_dirty " << *this << endl;

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
  _mark_dirty();

  // mark dentry too
  parent->mark_dirty(pv);
}


void CInode::mark_clean()
{
  dout(10) << " mark_clean " << *this << endl;
  if (state_test(STATE_DIRTY)) {
    state_clear(STATE_DIRTY);
    put(PIN_DIRTY);
  }
}    



// ------------------
// locking

void CInode::set_mlock_info(MLock *m)
{
  m->set_ino(ino());
}

void CInode::encode_lock_state(int type, bufferlist& bl)
{
  switch (type) {
  case LOCK_OTYPE_IAUTH:
    ::_encode(inode.mode, bl);
    ::_encode(inode.uid, bl);
    ::_encode(inode.gid, bl);  
    break;
    
  case LOCK_OTYPE_ILINK:
    ::_encode(inode.nlink, bl);
    ::_encode(inode.anchored, bl);
    break;
    
  case LOCK_OTYPE_IDIRFRAGTREE:
    dirfragtree._encode(bl);
    break;
    
  case LOCK_OTYPE_IFILE:
    ::_encode(inode.size, bl);
    ::_encode(inode.mtime, bl);
    ::_encode(inode.atime, bl);
    break;
  
  default:
    assert(0);
  }
}

void CInode::decode_lock_state(int type, bufferlist& bl)
{
  int off = 0;
  switch (type) {
  case LOCK_OTYPE_IAUTH:
    ::_decode(inode.mode, bl, off);
    ::_decode(inode.uid, bl, off);
    ::_decode(inode.gid, bl, off);
    break;

  case LOCK_OTYPE_ILINK:
    ::_decode(inode.nlink, bl, off);
    ::_decode(inode.anchored, bl, off);
    break;

  case LOCK_OTYPE_IDIRFRAGTREE:
    dirfragtree._decode(bl, off);
    break;

  case LOCK_OTYPE_IFILE:
    ::_decode(inode.size, bl, off);
    ::_decode(inode.mtime, bl, off);
    ::_decode(inode.atime, bl, off);
    break;

  default:
    assert(0);
  }
}




// waiting

bool CInode::is_frozen()
{
  if (parent && parent->dir->is_frozen())
    return true;
  return false;
}

bool CInode::is_frozen_dir()
{
  if (parent && parent->dir->is_frozen_dir())
    return true;
  return false;
}

bool CInode::is_freezing()
{
  if (parent && parent->dir->is_freezing())
    return true;
  return false;
}

void CInode::add_waiter(int tag, Context *c) 
{
  // wait on the directory?
  if (tag & WAIT_AUTHPINNABLE) {
    parent->dir->add_waiter(CDir::WAIT_AUTHPINNABLE, c);
    return;
  }
  if (tag & WAIT_SINGLEAUTH) {
    parent->dir->add_waiter(CDir::WAIT_SINGLEAUTH, c);
    return;
  }
  MDSCacheObject::add_waiter(tag, c);
}


// auth_pins
bool CInode::can_auth_pin() {
  if (parent)
    return parent->dir->can_auth_pin();
  return true;
}

void CInode::auth_pin() 
{
  if (auth_pins == 0)
    get(PIN_AUTHPIN);
  auth_pins++;

  dout(7) << "auth_pin on " << *this << " count now " << auth_pins << " + " << nested_auth_pins << endl;
  
  if (parent)
    parent->dir->adjust_nested_auth_pins( 1 );
}

void CInode::auth_unpin() 
{
  auth_pins--;
  if (auth_pins == 0)
    put(PIN_AUTHPIN);
  
  dout(7) << "auth_unpin on " << *this << " count now " << auth_pins << " + " << nested_auth_pins << endl;
  
  assert(auth_pins >= 0);
  
  if (parent)
    parent->dir->adjust_nested_auth_pins( -1 );
}

void CInode::adjust_nested_auth_pins(int a)
{
  if (!parent) return;
  nested_auth_pins += a;
  parent->get_dir()->adjust_nested_auth_pins(a);
}



// authority

pair<int,int> CInode::authority() 
{
  if (is_root())
    return CDIR_AUTH_ROOTINODE;  // root _inode_ is locked to mds0.

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



