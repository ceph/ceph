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

#include "MDSRank.h"
#include "MDCache.h"
#include "Locker.h"
#include "LogSegment.h"

#include "messages/MLock.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds." << dir->cache->mds->get_nodeid() << ".cache.den(" << dir->dirfrag() << " " << name << ") "


ostream& CDentry::print_db_line_prefix(ostream& out)
{
  return out << ceph_clock_now() << " mds." << dir->cache->mds->get_nodeid() << ".cache.den(" << dir->ino() << " " << name << ") ";
}

LockType CDentry::lock_type(CEPH_LOCK_DN);
LockType CDentry::versionlock_type(CEPH_LOCK_DVERSION);


// CDentry

ostream& operator<<(ostream& out, const CDentry& dn)
{
  filepath path;
  dn.make_path(path);
  
  out << "[dentry " << path;
  
  if (true || dn.first != 0 || dn.last != CEPH_NOSNAP) {
    out << " [" << dn.first << ",";
    if (dn.last == CEPH_NOSNAP) 
      out << "head";
    else
      out << dn.last;
    out << ']';
  }

  if (dn.is_auth()) {
    out << " auth";
    if (dn.is_replicated()) 
      out << dn.get_replicas();
  } else {
    out << " rep@" << dn.authority();
    out << "." << dn.get_replica_nonce();
  }

  if (dn.get_linkage()->is_null()) out << " NULL";
  if (dn.get_linkage()->is_remote()) {
    out << " REMOTE(";
    out << dn.get_linkage()->get_remote_d_type_string();
    out << ")";
  }

  if (!dn.lock.is_sync_and_unlocked())
    out << " " << dn.lock;
  if (!dn.versionlock.is_sync_and_unlocked())
    out << " " << dn.versionlock;

  if (dn.get_projected_version() != dn.get_version())
    out << " pv=" << dn.get_projected_version();
  out << " v=" << dn.get_version();

  if (dn.is_auth_pinned())
    out << " ap=" << dn.get_num_auth_pins() << "+" << dn.get_num_nested_auth_pins();

  out << " inode=" << dn.get_linkage()->get_inode();

  out << " state=" << dn.get_state();
  if (dn.is_new()) out << "|new";
  if (dn.state_test(CDentry::STATE_BOTTOMLRU)) out << "|bottomlru";

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
  if ((l.get_dir()->ino() < r.get_dir()->ino()) ||
      (l.get_dir()->ino() == r.get_dir()->ino() &&
       (l.get_name() < r.get_name() ||
	(l.get_name() == r.get_name() && l.last < r.last))))
    return true;
  return false;
}


void CDentry::print(ostream& out)
{
  out << *this;
}


/*
inodeno_t CDentry::get_ino()
{
  if (get_inode()) 
    return get_inode()->ino();
  return inodeno_t();
}
*/

mds_authority_t CDentry::authority() const
{
  return dir->authority();
}


void CDentry::add_waiter(uint64_t tag, MDSInternalContextBase *c)
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
    ls->dirty_dentries.push_back(&item_dirty);
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

  // not always true for recalc_auth_bits during resolve finish
  //assert(dir->get_version() == 0 || version <= dir->get_version());  // hmm?

  // state+pin
  state_clear(STATE_DIRTY);
  dir->dec_num_dirty();
  put(PIN_DIRTY);
  
  item_dirty.remove_myself();

  clear_new();
}    

void CDentry::mark_new() 
{
  dout(10) << " mark_new " << *this << dendl;
  state_set(STATE_NEW);
}

void CDentry::make_path_string(string& s, bool projected) const
{
  if (dir) {
    dir->inode->make_path_string(s, projected);
  } else {
    s = "???";
  }
  s += "/";
  s.append(name.data(), name.length());
}

void CDentry::make_path(filepath& fp, bool projected) const
{
  assert(dir);
  dir->inode->make_path(fp, projected);
  fp.push_dentry(name);
}

/*
 * we only add ourselves to remote_parents when the linkage is
 * active (no longer projected).  if the passed dnl is projected,
 * don't link in, and do that work later in pop_projected_linkage().
 */
void CDentry::link_remote(CDentry::linkage_t *dnl, CInode *in)
{
  assert(dnl->is_remote());
  assert(in->ino() == dnl->get_remote_ino());
  dnl->inode = in;

  if (dnl == &linkage)
    in->add_remote_parent(this);
}

void CDentry::unlink_remote(CDentry::linkage_t *dnl)
{
  assert(dnl->is_remote());
  assert(dnl->inode);
  
  if (dnl == &linkage)
    dnl->inode->remove_remote_parent(this);

  dnl->inode = 0;
}

void CDentry::push_projected_linkage()
{
  _project_linkage();

  if (is_auth()) {
    CInode *diri = dir->inode;
    if (diri->is_stray())
      diri->mdcache->notify_stray_removed();
  }
}


void CDentry::push_projected_linkage(CInode *inode)
{
  // dirty rstat tracking is in the projected plane
  bool dirty_rstat = inode->is_dirty_rstat();
  if (dirty_rstat)
    inode->clear_dirty_rstat();

  _project_linkage()->inode = inode;
  inode->push_projected_parent(this);

  if (dirty_rstat)
    inode->mark_dirty_rstat();

  if (is_auth()) {
    CInode *diri = dir->inode;
    if (diri->is_stray())
      diri->mdcache->notify_stray_created();
  }
}

CDentry::linkage_t *CDentry::pop_projected_linkage()
{
  assert(projected.size());
  
  linkage_t& n = projected.front();

  /*
   * the idea here is that the link_remote_inode(), link_primary_inode(), 
   * etc. calls should make linkage identical to &n (and we assert as
   * much).
   */

  if (n.remote_ino) {
    dir->link_remote_inode(this, n.remote_ino, n.remote_d_type);
    if (n.inode) {
      linkage.inode = n.inode;
      linkage.inode->add_remote_parent(this);
    }
  } else if (n.inode) {
    dir->link_primary_inode(this, n.inode);
    n.inode->pop_projected_parent();
  }

  assert(n.inode == linkage.inode);
  assert(n.remote_ino == linkage.remote_ino);
  assert(n.remote_d_type == linkage.remote_d_type);

  projected.pop_front();

  return &linkage;
}



// ----------------------------
// auth pins

int CDentry::get_num_dir_auth_pins() const
{
  assert(!is_projected());
  if (get_linkage()->is_primary())
    return auth_pins + get_linkage()->get_inode()->get_num_auth_pins();
  return auth_pins;
}

bool CDentry::can_auth_pin() const
{
  assert(dir);
  return dir->can_auth_pin();
}

void CDentry::auth_pin(void *by)
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

  dir->adjust_nested_auth_pins(1, 1, by);
}

void CDentry::auth_unpin(void *by)
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

  dir->adjust_nested_auth_pins(-1, -1, by);
}

void CDentry::adjust_nested_auth_pins(int adjustment, int diradj, void *by)
{
  nested_auth_pins += adjustment;

  dout(35) << "adjust_nested_auth_pins by " << by 
	   << ", change " << adjustment << " yields "
	   << auth_pins << "+" << nested_auth_pins
	   << dendl;
  assert(nested_auth_pins >= 0);

  dir->adjust_nested_auth_pins(adjustment, diradj, by);
}

bool CDentry::is_frozen() const
{
  return dir->is_frozen();
}

bool CDentry::is_freezing() const
{
  return dir->is_freezing();
}

void CDentry::decode_replica(bufferlist::iterator& p, bool is_new)
{
  __u32 nonce;
  ::decode(nonce, p);
  replica_nonce = nonce;
  
  ::decode(first, p);

  inodeno_t rino;
  unsigned char rdtype;
  __s32 ls;
  ::decode(rino, p);
  ::decode(rdtype, p);
  ::decode(ls, p);

  if (is_new) {
    if (rino)
      dir->link_remote_inode(this, rino, rdtype);
    lock.set_state(ls);
  }
}

// ----------------------------
// locking

void CDentry::set_object_info(MDSCacheObjectInfo &info) 
{
  info.dirfrag = dir->dirfrag();
  info.dname = name;
  info.snapid = last;
}

void CDentry::encode_lock_state(int type, bufferlist& bl)
{
  ::encode(first, bl);

  // null, ino, or remote_ino?
  char c;
  if (linkage.is_primary()) {
    c = 1;
    ::encode(c, bl);
    ::encode(linkage.get_inode()->inode.ino, bl);
  }
  else if (linkage.is_remote()) {
    c = 2;
    ::encode(c, bl);
    ::encode(linkage.get_remote_ino(), bl);
  }
  else if (linkage.is_null()) {
    // encode nothing.
  }
  else ceph_abort();
}

void CDentry::decode_lock_state(int type, bufferlist& bl)
{  
  bufferlist::iterator p = bl.begin();

  snapid_t newfirst;
  ::decode(newfirst, p);

  if (!is_auth() && newfirst != first) {
    dout(10) << "decode_lock_state first " << first << " -> " << newfirst << dendl;
    assert(newfirst > first);
    first = newfirst;
  }

  if (p.end()) {
    // null
    assert(linkage.is_null());
    return;
  }

  char c;
  inodeno_t ino;
  ::decode(c, p);

  switch (c) {
  case 1:
  case 2:
    ::decode(ino, p);
    // newly linked?
    if (linkage.is_null() && !is_auth()) {
      // force trim from cache!
      dout(10) << "decode_lock_state replica dentry null -> non-null, must trim" << dendl;
      //assert(get_num_ref() == 0);
    } else {
      // verify?
      
    }
    break;
  default: 
    ceph_abort();
  }
}


ClientLease *CDentry::add_client_lease(client_t c, Session *session) 
{
  ClientLease *l;
  if (client_lease_map.count(c))
    l = client_lease_map[c];
  else {
    dout(20) << "add_client_lease client." << c << " on " << lock << dendl;
    if (client_lease_map.empty())
      get(PIN_CLIENTLEASE);
    l = client_lease_map[c] = new ClientLease(c, this);
    l->seq = ++session->lease_seq;
  
    lock.get_client_lease();
  }
  
  return l;
}

void CDentry::remove_client_lease(ClientLease *l, Locker *locker) 
{
  assert(l->parent == this);

  bool gather = false;

  dout(20) << "remove_client_lease client." << l->client << " on " << lock << dendl;
  lock.put_client_lease();
  if (lock.get_num_client_lease() == 0 && !lock.is_stable())
    gather = true;

  client_lease_map.erase(l->client);
  l->item_lease.remove_myself();
  l->item_session_lease.remove_myself();
  delete l;

  if (client_lease_map.empty())
    put(PIN_CLIENTLEASE);

  if (gather)
    locker->eval_gather(&lock);
}

void CDentry::remove_client_leases(Locker *locker)
{
  while (!client_lease_map.empty())
    remove_client_lease(client_lease_map.begin()->second, locker);
}

void CDentry::_put()
{
  if (get_num_ref() <= ((int)is_dirty() + 1)) {
    CDentry::linkage_t *dnl = get_projected_linkage();
    if (dnl->is_primary()) {
      CInode *in = dnl->get_inode();
      if (get_num_ref() == (int)is_dirty() + !!in->get_num_ref())
	in->mdcache->maybe_eval_stray(in, true);
    }
  }
}

void CDentry::dump(Formatter *f) const
{
  assert(f != NULL);

  filepath path;
  make_path(path);

  f->dump_string("path", path.get_path());
  f->dump_unsigned("path_ino", path.get_ino().val);
  f->dump_unsigned("snap_first", first);
  f->dump_unsigned("snap_last", last);
  
  f->dump_bool("is_primary", get_linkage()->is_primary());
  f->dump_bool("is_remote", get_linkage()->is_remote());
  f->dump_bool("is_null", get_linkage()->is_null());
  f->dump_bool("is_new", is_new());
  if (get_linkage()->get_inode()) {
    f->dump_unsigned("inode", get_linkage()->get_inode()->ino());
  } else {
    f->dump_unsigned("inode", 0);
  }

  if (linkage.is_remote()) {
    f->dump_string("remote_type", linkage.get_remote_d_type_string());
  } else {
    f->dump_string("remote_type", "");
  }

  f->dump_unsigned("version", get_version());
  f->dump_unsigned("projected_version", get_projected_version());

  f->dump_int("auth_pins", auth_pins);
  f->dump_int("nested_auth_pins", nested_auth_pins);

  MDSCacheObject::dump(f);

  f->open_object_section("lock");
  lock.dump(f);
  f->close_section();

  f->open_object_section("versionlock");
  versionlock.dump(f);
  f->close_section();

  f->open_array_section("states");
  MDSCacheObject::dump_states(f);
  if (state_test(STATE_NEW))
    f->dump_string("state", "new");
  if (state_test(STATE_FRAGMENTING))
    f->dump_string("state", "fragmenting");
  if (state_test(STATE_PURGING))
    f->dump_string("state", "purging");
  if (state_test(STATE_BADREMOTEINO))
    f->dump_string("state", "badremoteino");
  if (state_test(STATE_STRAY))
    f->dump_string("state", "stray");
  f->close_section();
}

std::string CDentry::linkage_t::get_remote_d_type_string() const
{
  switch (DTTOIF(remote_d_type)) {
    case S_IFSOCK: return "sock";
    case S_IFLNK: return "lnk";
    case S_IFREG: return "reg";
    case S_IFBLK: return "blk";
    case S_IFDIR: return "dir";
    case S_IFCHR: return "chr";
    case S_IFIFO: return "fifo";
    default: ceph_abort(); return "";
  }
}

MEMPOOL_DEFINE_OBJECT_FACTORY(CDentry, co_dentry, mds_co);
