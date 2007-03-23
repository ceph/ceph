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



#include "CDentry.h"
#include "CInode.h"
#include "CDir.h"
#include "Anchor.h"

#include "MDS.h"
#include "MDCache.h"

#include <cassert>

#undef dout
#define dout(x)  if (x <= g_conf.debug || x <= g_conf.debug_mds) cout << g_clock.now() << " mds" << dir->cache->mds->get_nodeid() << ".cache.den(" << dir->ino() << " " << name << ") "


// CDentry

ostream& operator<<(ostream& out, CDentry& dn)
{
  string path;
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
  if (dn.is_remote()) out << " REMOTE";

  if (dn.is_pinned()) out << " " << dn.num_pins() << " pathpins";

  if (dn.get_lockstate() == DN_LOCK_UNPINNING) out << " unpinning";
  if (dn.get_lockstate() == DN_LOCK_PREXLOCK) out << " prexlock=" << dn.get_xlockedby() << " g=" << dn.get_gather_set();
  if (dn.get_lockstate() == DN_LOCK_XLOCK) out << " xlock=" << dn.get_xlockedby();

  out << " v=" << dn.get_version();
  out << " pv=" << dn.get_projected_version();

  out << " inode=" << dn.get_inode();

  if (dn.get_num_ref()) {
    out << " |";
    dn.print_pin_set(out);
  }

  out << " " << &dn;
  out << "]";
  return out;
}


bool operator<(CDentry& l, CDentry& r)
{
  if (l.get_dir()->ino() < r.get_dir()->ino()) return true;
  if (l.get_dir()->ino() == r.get_dir()->ino() &&
      l.get_name() < r.get_name()) return true;
  return false;
}



CDentry::CDentry(const CDentry& m) {
  assert(0); //std::cerr << "copy cons called, implement me" << endl;
}


inodeno_t CDentry::get_ino()
{
  if (inode) 
    return inode->ino();
  return inodeno_t();
}


pair<int,int> CDentry::authority()
{
  return dir->dentry_authority(name);
}


version_t CDentry::pre_dirty(version_t min)
{
  projected_version = dir->pre_dirty(min);
  dout(10) << " pre_dirty " << *this << endl;
  return projected_version;
}


void CDentry::_mark_dirty()
{
  // state+pin
  if (!state_test(STATE_DIRTY)) {
    state_set(STATE_DIRTY);
    get(PIN_DIRTY);
  }
}

void CDentry::mark_dirty(version_t pv) 
{
  dout(10) << " mark_dirty " << *this << endl;

  // i now live in this new dir version
  assert(pv == projected_version);
  version = pv;
  _mark_dirty();

  // mark dir too
  dir->mark_dirty(pv);
}

void CDentry::mark_clean() {
  dout(10) << " mark_clean " << *this << endl;
  assert(is_dirty());
  assert(version <= dir->get_version());

  // this happens on export.
  //assert(version <= dir->get_last_committed_version());  

  // state+pin
  state_clear(STATE_DIRTY);
  put(PIN_DIRTY);
}    


void CDentry::make_path(string& s)
{
  if (dir) {
    dir->inode->make_path(s);
  } else {
    s = "???";
  }
  s += "/";
  s += name;
}

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
  dout(10) << "make_anchor_trace added " << trace.back() << endl;
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




// =
const CDentry& CDentry::operator= (const CDentry& right) {
  assert(0); //std::cerr << "copy op called, implement me" << endl;
  return *this;
}

  // comparisons
  bool CDentry::operator== (const CDentry& right) const {
    return name == right.name;
  }
  bool CDentry::operator!= (const CDentry& right) const {
    return name == right.name;
  }
  bool CDentry::operator< (const CDentry& right) const {
    return name < right.name;
  }
  bool CDentry::operator> (const CDentry& right) const {
    return name > right.name;
  }
  bool CDentry::operator>= (const CDentry& right) const {
    return name >= right.name;
  }
  bool CDentry::operator<= (const CDentry& right) const {
    return name <= right.name;
  }
