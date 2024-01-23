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
#include "SnapClient.h"

#include "MDSRank.h"
#include "MDCache.h"
#include "Locker.h"
#include "LogSegment.h"

#include "messages/MLock.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds." << dir->mdcache->mds->get_nodeid() << ".cache.den(" << dir->dirfrag() << " " << name << ") "

using namespace std;

ostream& CDentry::print_db_line_prefix(ostream& out) const
{
  return out << ceph_clock_now() << " mds." << dir->mdcache->mds->get_nodeid() << ".cache.den(" << dir->ino() << " " << name << ") ";
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
    mds_authority_t a = dn.authority();
    out << " rep@" << a.first;
    if (a.second != CDIR_AUTH_UNKNOWN)
      out << "," << a.second;
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

  if (dn.get_num_auth_pins()) {
    out << " ap=" << dn.get_num_auth_pins();
#ifdef MDS_AUTHPIN_SET
    dn.print_authpin_set(out);
#endif
  }

  {
    const CInode *inode = dn.get_linkage()->get_inode();
    out << " ino=";
     if (inode) {
       out << inode->ino();
     } else {
       out << "(nil)";
     }
  }

  out << " state=" << dn.get_state();
  if (dn.is_new()) out << "|new";
  if (dn.state_test(CDentry::STATE_BOTTOMLRU)) out << "|bottomlru";
  if (dn.state_test(CDentry::STATE_UNLINKING)) out << "|unlinking";
  if (dn.state_test(CDentry::STATE_REINTEGRATING)) out << "|reintegrating";

  if (dn.get_num_ref()) {
    out << " |";
    dn.print_pin_set(out);
  }

  if (dn.get_alternate_name().size()) {
    out << " altname=" << binstrprint(dn.get_alternate_name(), 16);
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


void CDentry::print(ostream& out) const
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


void CDentry::add_waiter(uint64_t tag, MDSContext *c)
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
  dout(10) << __func__ << " " << *this << dendl;
  return projected_version;
}


void CDentry::_mark_dirty(LogSegment *ls)
{
  // state+pin
  if (!state_test(STATE_DIRTY)) {
    state_set(STATE_DIRTY);
    get(PIN_DIRTY);
    dir->inc_num_dirty();
    dir->dirty_dentries.push_back(&item_dir_dirty);
    ceph_assert(ls);
  }
  if (ls) 
    ls->dirty_dentries.push_back(&item_dirty);
}

void CDentry::mark_dirty(version_t pv, LogSegment *ls) 
{
  dout(10) << __func__ << " " << *this << dendl;

  // i now live in this new dir version
  ceph_assert(pv <= projected_version);
  version = pv;
  _mark_dirty(ls);

  // mark dir too
  dir->mark_dirty(ls, pv);
}


void CDentry::mark_clean() 
{
  dout(10) << __func__ << " " << *this << dendl;
  ceph_assert(is_dirty());

  // not always true for recalc_auth_bits during resolve finish
  //assert(dir->get_version() == 0 || version <= dir->get_version());  // hmm?

  state_clear(STATE_DIRTY|STATE_NEW);
  dir->dec_num_dirty();

  item_dir_dirty.remove_myself();
  item_dirty.remove_myself();

  put(PIN_DIRTY);
}

void CDentry::mark_new() 
{
  dout(10) << __func__ << " " << *this << dendl;
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
  ceph_assert(dir);
  dir->inode->make_path(fp, projected);
  fp.push_dentry(get_name());
}

/*
 * we only add ourselves to remote_parents when the linkage is
 * active (no longer projected).  if the passed dnl is projected,
 * don't link in, and do that work later in pop_projected_linkage().
 */
void CDentry::link_remote(CDentry::linkage_t *dnl, CInode *in)
{
  ceph_assert(dnl->is_remote());
  ceph_assert(in->ino() == dnl->get_remote_ino());
  dnl->inode = in;

  if (dnl == &linkage)
    in->add_remote_parent(this);

  // check for reintegration
  dir->mdcache->eval_remote(this);
}

void CDentry::unlink_remote(CDentry::linkage_t *dnl)
{
  ceph_assert(dnl->is_remote());
  ceph_assert(dnl->inode);
  
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
  ceph_assert(projected.size());
  
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
  } else {
    if (n.inode) {
      dir->link_primary_inode(this, n.inode);
      n.inode->pop_projected_parent();
    }
  }

  ceph_assert(n.inode == linkage.inode);
  ceph_assert(n.remote_ino == linkage.remote_ino);
  ceph_assert(n.remote_d_type == linkage.remote_d_type);

  projected.pop_front();

  return &linkage;
}



// ----------------------------
// auth pins

int CDentry::get_num_dir_auth_pins() const
{
  ceph_assert(!is_projected());
  if (get_linkage()->is_primary())
    return auth_pins + get_linkage()->get_inode()->get_num_auth_pins();
  return auth_pins;
}

bool CDentry::can_auth_pin(int *err_ret) const
{
  ceph_assert(dir);
  return dir->can_auth_pin(err_ret);
}

void CDentry::auth_pin(void *by)
{
  if (auth_pins == 0)
    get(PIN_AUTHPIN);
  auth_pins++;

#ifdef MDS_AUTHPIN_SET
  auth_pin_set.insert(by);
#endif

  dout(10) << "auth_pin by " << by << " on " << *this << " now " << auth_pins << dendl;

  dir->adjust_nested_auth_pins(1, by);
}

void CDentry::auth_unpin(void *by)
{
  auth_pins--;

#ifdef MDS_AUTHPIN_SET
  {
    auto it = auth_pin_set.find(by);
    ceph_assert(it != auth_pin_set.end());
    auth_pin_set.erase(it);
  }
#endif

  if (auth_pins == 0)
    put(PIN_AUTHPIN);

  dout(10) << "auth_unpin by " << by << " on " << *this << " now " << auth_pins << dendl;
  ceph_assert(auth_pins >= 0);

  dir->adjust_nested_auth_pins(-1, by);
}

void CDentry::adjust_nested_auth_pins(int diradj, void *by)
{
  dir->adjust_nested_auth_pins(diradj, by);
}

bool CDentry::is_frozen() const
{
  return dir->is_frozen();
}

bool CDentry::is_freezing() const
{
  return dir->is_freezing();
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
  encode(first, bl);

  // null, ino, or remote_ino?
  char c;
  if (linkage.is_primary()) {
    c = 1;
    encode(c, bl);
    encode(linkage.get_inode()->ino(), bl);
  }
  else if (linkage.is_remote()) {
    c = 2;
    encode(c, bl);
    encode(linkage.get_remote_ino(), bl);
  }
  else if (linkage.is_null()) {
    // encode nothing.
  }
  else ceph_abort();
}

void CDentry::decode_lock_state(int type, const bufferlist& bl)
{  
  auto p = bl.cbegin();

  snapid_t newfirst;
  decode(newfirst, p);

  if (!is_auth() && newfirst != first) {
    dout(10) << __func__ << " first " << first << " -> " << newfirst << dendl;
    ceph_assert(newfirst > first);
    first = newfirst;
  }

  if (p.end()) {
    // null
    ceph_assert(linkage.is_null());
    return;
  }

  char c;
  inodeno_t ino;
  decode(c, p);

  switch (c) {
  case 1:
  case 2:
    decode(ino, p);
    // newly linked?
    if (linkage.is_null() && !is_auth()) {
      // force trim from cache!
      dout(10) << __func__ << " replica dentry null -> non-null, must trim" << dendl;
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
    dout(20) << __func__ << " client." << c << " on " << lock << dendl;
    if (client_lease_map.empty()) {
      get(PIN_CLIENTLEASE);
      lock.get_client_lease();
    }
    l = client_lease_map[c] = new ClientLease(c, this);
    l->seq = ++session->lease_seq;
  
  }
  
  return l;
}

void CDentry::remove_client_lease(ClientLease *l, Locker *locker) 
{
  ceph_assert(l->parent == this);

  bool gather = false;

  dout(20) << __func__ << " client." << l->client << " on " << lock << dendl;

  client_lease_map.erase(l->client);
  l->item_lease.remove_myself();
  l->item_session_lease.remove_myself();
  delete l;

  if (client_lease_map.empty()) {
    gather = !lock.is_stable();
    lock.put_client_lease();
    put(PIN_CLIENTLEASE);
  }

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

void CDentry::encode_remote(inodeno_t& ino, unsigned char d_type,
                            std::string_view alternate_name,
                            bufferlist &bl)
{
  bl.append('l');  // remote link

  // marker, name, ino
  ENCODE_START(2, 1, bl);
  encode(ino, bl);
  encode(d_type, bl);
  encode(alternate_name, bl);
  ENCODE_FINISH(bl);
}

void CDentry::decode_remote(char icode, inodeno_t& ino, unsigned char& d_type,
                            mempool::mds_co::string& alternate_name,
                            ceph::buffer::list::const_iterator& bl)
{
  if (icode == 'l') {
    DECODE_START(2, bl);
    decode(ino, bl);
    decode(d_type, bl);
    if (struct_v >= 2)
      decode(alternate_name, bl);
    DECODE_FINISH(bl);
  } else if (icode == 'L') {
    decode(ino, bl);
    decode(d_type, bl);
  } else ceph_assert(0);
}

void CDentry::dump(Formatter *f) const
{
  ceph_assert(f != NULL);

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

bool CDentry::scrub(snapid_t next_seq)
{
  dout(20) << "scrubbing " << *this << " next_seq = " << next_seq << dendl;

  /* attempt to locate damage in first of CDentry, see:
   * https://tracker.ceph.com/issues/56140
   */
  /* skip projected dentries as first/last may have placeholder values */
  if (!is_projected()) {
    CDir* dir = get_dir();

    if (first > next_seq) {
      derr << __func__ << ": first > next_seq (" << next_seq << ") " << *this << dendl;
      dir->go_bad_dentry(last, get_name());
      return true;
    } else if (first > last) {
      derr << __func__ << ": first > last " << *this << dendl;
      dir->go_bad_dentry(last, get_name());
      return true;
    }

    auto&& realm = dir->get_inode()->find_snaprealm();
    if (realm) {
      auto&& snaps = realm->get_snaps();
      auto it = snaps.lower_bound(first);
      bool stale = last != CEPH_NOSNAP && (it == snaps.end() || *it > last);
      if (stale) {
        dout(20) << "is stale" << dendl;
        /* TODO: maybe trim? */
      }
    }
  }
  return false;
}

bool CDentry::check_corruption(bool load)
{
  auto&& snapclient = dir->mdcache->mds->snapclient;
  auto next_snap = snapclient->get_last_seq()+1;
  if (first > last || (snapclient->is_server_ready() && first > next_snap)) {
    if (load) {
      dout(1) << "loaded already corrupt dentry: " << *this << dendl;
      corrupt_first_loaded = true;
    } else {
      derr << "newly corrupt dentry to be committed: " << *this << dendl;
    }
    if (g_conf().get_val<bool>("mds_go_bad_corrupt_dentry")) {
      dir->go_bad_dentry(last, get_name());
    }
    if (!load && g_conf().get_val<bool>("mds_abort_on_newly_corrupt_dentry")) {
      dir->mdcache->mds->clog->error() << "MDS abort because newly corrupt dentry to be committed: " << *this;
      dir->mdcache->mds->abort("detected newly corrupt dentry"); /* avoid writing out newly corrupted dn */
    }
    return true;
  }
  return false;
}

MEMPOOL_DEFINE_OBJECT_FACTORY(CDentry, co_dentry, mds_co);
