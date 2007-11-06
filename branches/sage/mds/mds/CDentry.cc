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



#include "CDentry.h"
#include "CInode.h"
#include "CDir.h"
#include "Anchor.h"

#include "MDS.h"
#include "MDCache.h"
#include "LogSegment.h"

#include "messages/MLock.h"

#include <cassert>

#define dout(x)  if (x <= g_conf.debug || x <= g_conf.debug_mds) *_dout << dbeginl << g_clock.now() << " mds" << dir->cache->mds->get_nodeid() << ".cache.den(" << dir->dirfrag() << " " << name << ") "



ostream& CDentry::print_db_line_prefix(ostream& out) 
{
  return out << g_clock.now() << " mds" << dir->cache->mds->get_nodeid() << ".cache.den(" << dir->ino() << " " << name << ") ";
}


// CDentry

ostream& operator<<(ostream& out, CDentry& dn)
{
  filepath path;
  dn.make_path(path);
  
  out << "[dentry " << path;
  
  if (dn.is_auth()) {
    out << " auth";
    if (dn.is_replicated()) 
      out << dn.get_replicas();
  } else {
    out << " rep@" << dn.authority();
    out << "." << dn.get_replica_nonce();
    assert(dn.get_replica_nonce() >= 0);
  }

  if (dn.is_null()) out << " NULL";
  if (dn.is_remote()) {
    out << " REMOTE(";
    switch (dn.get_remote_d_type() << 12) {
    case S_IFREG: out << "reg"; break;
    case S_IFDIR: out << "dir"; break;
    case S_IFLNK: out << "lnk"; break;
    default: assert(0);
    }
    out << ")";
  }

  out << " " << dn.lock;

  out << " v=" << dn.get_version();
  out << " pv=" << dn.get_projected_version();

  out << " inode=" << dn.get_inode();

  if (dn.is_new()) out << " state=new";

  if (dn.get_num_ref()) {
    out << " |";
    dn.print_pin_set(out);
  }

  out << " " << &dn;
  out << "]";
  return out;
}


bool operator<(const CDentry& l, const CDentry& r)
{
  if (l.get_dir()->ino() < r.get_dir()->ino()) return true;
  if (l.get_dir()->ino() == r.get_dir()->ino() &&
      l.get_name() < r.get_name()) return true;
  return false;
}


void CDentry::print(ostream& out)
{
  out << *this;
}


inodeno_t CDentry::get_ino()
{
  if (inode) 
    return inode->ino();
  return inodeno_t();
}


pair<int,int> CDentry::authority()
{
  return dir->authority();
}


void CDentry::add_waiter(int tag, Context *c)
{
  // wait on the directory?
  if (tag & (WAIT_UNFREEZE|WAIT_SINGLEAUTH)) {
    dir->add_waiter(tag, c);
    return;
  }
  MDSCacheObject::add_waiter(tag, c);
}


version_t CDentry::pre_dirty(version_t min)
{
  projected_version = dir->pre_dirty(min);
  dout(10) << " pre_dirty " << *this << dendl;
  return projected_version;
}


void CDentry::_mark_dirty(LogSegment *ls)
{
  // state+pin
  if (!state_test(STATE_DIRTY)) {
    state_set(STATE_DIRTY);
    dir->inc_num_dirty();
    get(PIN_DIRTY);
    assert(ls);
  }
  if (ls) 
    ls->dirty_dentries.push_back(&xlist_dirty);
}

void CDentry::mark_dirty(version_t pv, LogSegment *ls) 
{
  dout(10) << " mark_dirty " << *this << dendl;

  // i now live in this new dir version
  assert(pv <= projected_version);
  version = pv;
  _mark_dirty(ls);

  // mark dir too
  dir->mark_dirty(pv, ls);
}


void CDentry::mark_clean() 
{
  dout(10) << " mark_clean " << *this << dendl;
  assert(is_dirty());
  assert(dir->get_version() == 0 || version <= dir->get_version());  // hmm?

  // state+pin
  state_clear(STATE_DIRTY);
  dir->dec_num_dirty();
  put(PIN_DIRTY);
  
  xlist_dirty.remove_myself();

  if (state_test(STATE_NEW)) 
    state_clear(STATE_NEW);
}    

void CDentry::mark_new() 
{
  dout(10) << " mark_new " << *this << dendl;
  state_set(STATE_NEW);
}

void CDentry::make_path_string(string& s)
{
  if (dir) {
    dir->inode->make_path_string(s);
  } else {
    s = "???";
  }
  s += "/";
  s += name;
}

void CDentry::make_path(filepath& fp)
{
  assert(dir);
  if (dir->inode->is_base())
    fp.set_ino(dir->inode->ino());               // base case
  else if (dir->inode->get_parent_dn())
    dir->inode->get_parent_dn()->make_path(fp);  // recurse
  else
    fp.set_ino(dir->inode->ino());               // relative but not base?  hrm!
  fp.push_dentry(name);
}

/*
void CDentry::make_path(string& s, inodeno_t tobase)
{
  assert(dir);
  
  if (dir->inode->is_root()) {
    s += "/";  // make it an absolute path (no matter what) if we hit the root.
  } 
  else if (dir->inode->get_parent_dn() &&
	   dir->inode->ino() != tobase) {
    dir->inode->get_parent_dn()->make_path(s, tobase);
    s += "/";
  }
  s += name;
}
*/

/** make_anchor_trace
 * construct an anchor trace for this dentry, as if it were linked to *in.
 */
void CDentry::make_anchor_trace(vector<Anchor>& trace, CInode *in)
{
  // start with parent dir inode
  if (dir)
    dir->inode->make_anchor_trace(trace);

  // add this inode (in my dirfrag) to the end
  trace.push_back(Anchor(in->ino(), dir->dirfrag()));
  dout(10) << "make_anchor_trace added " << trace.back() << dendl;
}



void CDentry::link_remote(CInode *in)
{
  assert(is_remote());
  assert(in->ino() == remote_ino);

  inode = in;
  in->add_remote_parent(this);
}

void CDentry::unlink_remote()
{
  assert(is_remote());
  assert(inode);
  
  inode->remove_remote_parent(this);
  inode = 0;
}


CDentryDiscover *CDentry::replicate_to(int who)
{
  int nonce = add_replica(who);
  return new CDentryDiscover(this, nonce);
}


// ----------------------------
// auth pins

bool CDentry::can_auth_pin()
{
  assert(dir);
  return dir->can_auth_pin();
}

void CDentry::auth_pin()
{
  if (auth_pins == 0)
    get(PIN_AUTHPIN);
  auth_pins++;

  dout(10) << "auth_pin on " << *this 
	   << " now " << auth_pins << "+" << nested_auth_pins
	   << dendl;

  dir->adjust_nested_auth_pins(1);
}

void CDentry::auth_unpin()
{
  auth_pins--;
  if (auth_pins == 0)
    put(PIN_AUTHPIN);

  dout(10) << "auth_unpin on " << *this
	   << " now " << auth_pins << "+" << nested_auth_pins
	   << dendl;
  assert(auth_pins >= 0);

  dir->adjust_nested_auth_pins(-1);
}

void CDentry::adjust_nested_auth_pins(int by)
{
  nested_auth_pins += by;

  dout(15) << "adjust_nested_auth_pins by " << by 
	   << " now " << auth_pins << "+" << nested_auth_pins
	   << dendl;
  assert(nested_auth_pins >= 0);

  dir->adjust_nested_auth_pins(by);
}

bool CDentry::is_frozen()
{
  return dir->is_frozen();
}


// ----------------------------
// locking

void CDentry::set_object_info(MDSCacheObjectInfo &info) 
{
  info.dirfrag = dir->dirfrag();
  info.dname = name;
}

void CDentry::encode_lock_state(int type, bufferlist& bl)
{
  // null, ino, or remote_ino?
  int c;
  if (is_primary()) {
    c = 1;
    ::_encode(c, bl);
    ::_encode(inode->inode.ino, bl);
  }
  else if (is_remote()) {
    c = 2;
    ::_encode(c, bl);
    ::_encode(remote_ino, bl);
  }
  else if (is_null()) {
    // encode nothing.
  }
  else assert(0);  
}

void CDentry::decode_lock_state(int type, bufferlist& bl)
{  
  if (bl.length() == 0) {
    // null
    assert(is_null());
    return;
  }

  int off = 0;
  char c;
  inodeno_t ino;
  ::_decode(c, bl, off);

  switch (c) {
  case 1:
  case 2:
    _decode(ino, bl, off);
    // newly linked?
    if (is_null() && !is_auth()) {
      // force trim from cache!
      dout(10) << "decode_lock_state replica dentry null -> non-null, must trim" << dendl;
      //assert(get_num_ref() == 0);
    } else {
      // verify?
      
    }
    break;
  default: 
    assert(0);
  }
}
