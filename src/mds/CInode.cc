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

#include "MDSRank.h"
#include "MDCache.h"
#include "MDLog.h"
#include "Locker.h"
#include "Mutation.h"

#include "events/EUpdate.h"

#include "osdc/Objecter.h"

#include "snap.h"

#include "LogSegment.h"

#include "common/Clock.h"

#include "common/config.h"
#include "global/global_context.h"
#include "include/ceph_assert.h"

#include "mds/MDSContinuation.h"
#include "mds/InoTable.h"
#include "cephfs_features.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds." << mdcache->mds->get_nodeid() << ".cache.ino(" << ino() << ") "


class CInodeIOContext : public MDSIOContextBase
{
protected:
  CInode *in;
  MDSRank *get_mds() override {return in->mdcache->mds;}
public:
  explicit CInodeIOContext(CInode *in_) : in(in_) {
    ceph_assert(in != NULL);
  }
};

sr_t* const CInode::projected_inode::UNDEF_SRNODE = (sr_t*)(unsigned long)-1;

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

std::string_view CInode::pin_name(int p) const
{
  switch (p) {
    case PIN_DIRFRAG: return "dirfrag";
    case PIN_CAPS: return "caps";
    case PIN_IMPORTING: return "importing";
    case PIN_OPENINGDIR: return "openingdir";
    case PIN_REMOTEPARENT: return "remoteparent";
    case PIN_BATCHOPENJOURNAL: return "batchopenjournal";
    case PIN_SCATTERED: return "scattered";
    case PIN_STICKYDIRS: return "stickydirs";
      //case PIN_PURGING: return "purging";
    case PIN_FREEZING: return "freezing";
    case PIN_FROZEN: return "frozen";
    case PIN_IMPORTINGCAPS: return "importingcaps";
    case PIN_EXPORTINGCAPS: return "exportingcaps";
    case PIN_PASTSNAPPARENT: return "pastsnapparent";
    case PIN_OPENINGSNAPPARENTS: return "openingsnapparents";
    case PIN_TRUNCATING: return "truncating";
    case PIN_STRAY: return "stray";
    case PIN_NEEDSNAPFLUSH: return "needsnapflush";
    case PIN_DIRTYRSTAT: return "dirtyrstat";
    case PIN_DIRTYPARENT: return "dirtyparent";
    case PIN_DIRWAITER: return "dirwaiter";
    case PIN_SCRUBQUEUE: return "scrubqueue";
    default: return generic_pin_name(p);
  }
}

//int cinode_pins[CINODE_NUM_PINS];  // counts
ostream& CInode::print_db_line_prefix(ostream& out)
{
  return out << ceph_clock_now() << " mds." << mdcache->mds->get_nodeid() << ".cache.ino(" << ino() << ") ";
}

/*
 * write caps and lock ids
 */
struct cinode_lock_info_t cinode_lock_info[] = {
  { CEPH_LOCK_IFILE, CEPH_CAP_ANY_FILE_WR },
  { CEPH_LOCK_IAUTH, CEPH_CAP_AUTH_EXCL },
  { CEPH_LOCK_ILINK, CEPH_CAP_LINK_EXCL },
  { CEPH_LOCK_IXATTR, CEPH_CAP_XATTR_EXCL },
};
int num_cinode_locks = sizeof(cinode_lock_info) / sizeof(cinode_lock_info[0]);

ostream& operator<<(ostream& out, const CInode& in)
{
  string path;
  in.make_path_string(path, true);

  out << "[inode " << in.ino();
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

  if (in.get_num_auth_pins()) {
    out << " ap=" << in.get_num_auth_pins();
#ifdef MDS_AUTHPIN_SET
    in.print_authpin_set(out);
#endif
  }

  if (in.snaprealm)
    out << " snaprealm=" << in.snaprealm;

  if (in.state_test(CInode::STATE_AMBIGUOUSAUTH)) out << " AMBIGAUTH";
  if (in.state_test(CInode::STATE_NEEDSRECOVER)) out << " needsrecover";
  if (in.state_test(CInode::STATE_RECOVERING)) out << " recovering";
  if (in.state_test(CInode::STATE_DIRTYPARENT)) out << " dirtyparent";
  if (in.state_test(CInode::STATE_MISSINGOBJS)) out << " missingobjs";
  if (in.is_freezing_inode()) out << " FREEZING=" << in.auth_pin_freeze_allowance;
  if (in.is_frozen_inode()) out << " FROZEN";
  if (in.is_frozen_auth_pin()) out << " FROZEN_AUTHPIN";

  const auto& pi = in.get_projected_inode();
  if (pi->is_truncating())
    out << " truncating(" << pi->truncate_from << " to " << pi->truncate_size << ")";

  if (in.is_dir()) {
    out << " " << in.get_inode()->dirstat;
    if (g_conf()->mds_debug_scatterstat && in.is_projected()) {
      out << "->" << pi->dirstat;
    }
  } else {
    out << " s=" << in.get_inode()->size;
    if (in.get_inode()->nlink != 1)
      out << " nl=" << in.get_inode()->nlink;
  }

  // rstat
  out << " " << in.get_inode()->rstat;
  if (!(in.get_inode()->rstat == in.get_inode()->accounted_rstat))
    out << "/" << in.get_inode()->accounted_rstat;
  if (g_conf()->mds_debug_scatterstat && in.is_projected()) {
    out << "->" << pi->rstat;
    if (!(pi->rstat == pi->accounted_rstat))
      out << "/" << pi->accounted_rstat;
  }

  if (in.is_any_old_inodes()) {
    out << " old_inodes=" << in.get_old_inodes()->size();
  }

  if (!in.client_need_snapflush.empty())
    out << " need_snapflush=" << in.client_need_snapflush;

  // locks
  if (!in.authlock.is_sync_and_unlocked())
    out << " " << in.authlock;
  if (!in.linklock.is_sync_and_unlocked())
    out << " " << in.linklock;
  if (in.get_inode()->is_dir()) {
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
  if (in.get_inode()->client_ranges.size())
    out << " cr=" << in.get_inode()->client_ranges;

  if (!in.get_client_caps().empty()) {
    out << " caps={";
    bool first = true;
    for (const auto &p : in.get_client_caps()) {
      if (!first) out << ",";
      out << p.first << "="
	  << ccap_string(p.second.pending());
      if (p.second.issued() != p.second.pending())
	out << "/" << ccap_string(p.second.issued());
      out << "/" << ccap_string(p.second.wanted())
	  << "@" << p.second.get_last_seq();
      first = false;
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
    bool first = true;
    for (const auto &p : in.get_mds_caps_wanted()) {
      if (!first)
	out << ',';
      out << p.first << '=' << ccap_string(p.second);
      first = false;
    }
    out << '}';
  }

  if (in.get_num_ref()) {
    out << " |";
    in.print_pin_set(out);
  }

  if (in.get_inode()->export_pin != MDS_RANK_NONE) {
    out << " export_pin=" << in.get_inode()->export_pin;
  }
  if (in.state_test(CInode::STATE_DISTEPHEMERALPIN)) {
    out << " distepin";
  }
  if (in.state_test(CInode::STATE_RANDEPHEMERALPIN)) {
    out << " randepin";
  }

  out << " " << &in;
  out << "]";
  return out;
}

ostream& operator<<(ostream& out, const CInode::scrub_stamp_info_t& si)
{
  out << "{scrub_start_version: " << si.scrub_start_version
      << ", scrub_start_stamp: " << si.scrub_start_stamp
      << ", last_scrub_version: " << si.last_scrub_version
      << ", last_scrub_stamp: " << si.last_scrub_stamp;
  return out;
}

CInode::CInode(MDCache *c, bool auth, snapid_t f, snapid_t l) :
    mdcache(c), first(f), last(l), item_dirty(this),
    item_caps(this),
    item_open_file(this),
    item_dirty_parent(this),
    item_dirty_dirfrag_dir(this),
    item_dirty_dirfrag_nest(this),
    item_dirty_dirfrag_dirfragtree(this),
    pop(c->decayrate),
    versionlock(this, &versionlock_type),
    authlock(this, &authlock_type),
    linklock(this, &linklock_type),
    dirfragtreelock(this, &dirfragtreelock_type),
    filelock(this, &filelock_type),
    xattrlock(this, &xattrlock_type),
    snaplock(this, &snaplock_type),
    nestlock(this, &nestlock_type),
    flocklock(this, &flocklock_type),
    policylock(this, &policylock_type)
{
  if (auth)
    state_set(STATE_AUTH);
}

void CInode::print(ostream& out)
{
  out << *this;
}

void CInode::add_need_snapflush(CInode *snapin, snapid_t snapid, client_t client)
{
  dout(10) << __func__ << " client." << client << " snapid " << snapid << " on " << snapin << dendl;

  if (client_need_snapflush.empty()) {
    get(CInode::PIN_NEEDSNAPFLUSH);

    // FIXME: this is non-optimal, as we'll block freezes/migrations for potentially
    // long periods waiting for clients to flush their snaps.
    auth_pin(this);   // pin head get_inode()->..
  }

  auto &clients = client_need_snapflush[snapid];
  if (clients.empty())
    snapin->auth_pin(this);  // ...and pin snapped/old inode!
  
  clients.insert(client);
}

void CInode::remove_need_snapflush(CInode *snapin, snapid_t snapid, client_t client)
{
  dout(10) << __func__ << " client." << client << " snapid " << snapid << " on " << snapin << dendl;
  auto it = client_need_snapflush.find(snapid);
  if (it == client_need_snapflush.end()) {
    dout(10) << " snapid not found" << dendl;
    return;
  }
  size_t n = it->second.erase(client);
  if (n == 0) {
    dout(10) << " client not found" << dendl;
    return;
  }
  if (it->second.empty()) {
    client_need_snapflush.erase(it);
    snapin->auth_unpin(this);

    if (client_need_snapflush.empty()) {
      put(CInode::PIN_NEEDSNAPFLUSH);
      auth_unpin(this);
    }
  }
}

pair<bool,bool> CInode::split_need_snapflush(CInode *cowin, CInode *in)
{
  dout(10) << __func__ << " [" << cowin->first << "," << cowin->last << "] for " << *cowin << dendl;
  bool cowin_need_flush = false;
  bool orig_need_flush = false;
  auto it = client_need_snapflush.lower_bound(cowin->first);
  while (it != client_need_snapflush.end() && it->first < in->first) {
    ceph_assert(!it->second.empty());
    if (cowin->last >= it->first) {
      cowin->auth_pin(this);
      cowin_need_flush = true;
      ++it;
    } else {
      it = client_need_snapflush.erase(it);
    }
    in->auth_unpin(this);
  }

  if (it != client_need_snapflush.end() && it->first <= in->last)
    orig_need_flush = true;

  return make_pair(cowin_need_flush, orig_need_flush);
}

void CInode::mark_dirty_rstat()
{
  if (!state_test(STATE_DIRTYRSTAT)) {
    dout(10) << __func__ << dendl;
    state_set(STATE_DIRTYRSTAT);
    get(PIN_DIRTYRSTAT);
    CDentry *pdn = get_projected_parent_dn();
    if (pdn->is_auth()) {
      CDir *pdir = pdn->dir;
      pdir->dirty_rstat_inodes.push_back(&dirty_rstat_item);
      mdcache->mds->locker->mark_updated_scatterlock(&pdir->inode->nestlock);
    } else {
      // under cross-MDS rename.
      // DIRTYRSTAT flag will get cleared when rename finishes
      ceph_assert(state_test(STATE_AMBIGUOUSAUTH));
    }
  }
}
void CInode::clear_dirty_rstat()
{
  if (state_test(STATE_DIRTYRSTAT)) {
    dout(10) << __func__ << dendl;
    state_clear(STATE_DIRTYRSTAT);
    put(PIN_DIRTYRSTAT);
    dirty_rstat_item.remove_myself();
  }
}

CInode::projected_inode CInode::project_inode(bool xattr, bool snap)
{
  auto pi = allocate_inode(*get_projected_inode());

  if (scrub_infop && scrub_infop->last_scrub_dirty) {
    pi->last_scrub_stamp = scrub_infop->last_scrub_stamp;
    pi->last_scrub_version = scrub_infop->last_scrub_version;
    scrub_infop->last_scrub_dirty = false;
    scrub_maybe_delete_info();
  }

  const auto& ox = get_projected_xattrs();
  xattr_map_ptr px;
  if (xattr) {
    px = allocate_xattr_map();
    if (ox)
      *px = *ox;
  }

  sr_t* ps = projected_inode::UNDEF_SRNODE;
  if (snap) {
    ps = prepare_new_srnode(0);
    ++num_projected_srnodes;
  }

  projected_nodes.emplace_back(pi, xattr ? px : ox , ps);

  dout(15) << __func__ << " " << pi->ino << dendl;
  return projected_inode(std::move(pi), std::move(px), ps);
}

void CInode::pop_and_dirty_projected_inode(LogSegment *ls) 
{
  ceph_assert(!projected_nodes.empty());
  auto front = std::move(projected_nodes.front());
  dout(15) << __func__ << " v" << front.inode->version << dendl;

  projected_nodes.pop_front();

  bool pool_update = get_inode()->layout.pool_id != front.inode->layout.pool_id;
  bool pin_update = get_inode()->export_pin != front.inode->export_pin;
  bool dist_update = get_inode()->export_ephemeral_distributed_pin !=
		     front.inode->export_ephemeral_distributed_pin;

  reset_inode(std::move(front.inode));
  if (front.xattrs != get_xattrs())
    reset_xattrs(std::move(front.xattrs));

  if (front.snapnode != projected_inode::UNDEF_SRNODE) {
    --num_projected_srnodes;
    pop_projected_snaprealm(front.snapnode, false);
  }

  mark_dirty(ls);
  if (get_inode()->is_backtrace_updated())
    mark_dirty_parent(ls, pool_update);

  if (pin_update)
    maybe_export_pin(true);
  if (dist_update)
    maybe_ephemeral_dist_children(true);
}

sr_t *CInode::prepare_new_srnode(snapid_t snapid)
{
  const sr_t *cur_srnode = get_projected_srnode();
  sr_t *new_srnode;

  if (cur_srnode) {
    new_srnode = new sr_t(*cur_srnode);
    if (!new_srnode->past_parents.empty()) {
      // convert past_parents to past_parent_snaps
      ceph_assert(snaprealm);
      auto& snaps = snaprealm->get_snaps();
      for (auto p : snaps) {
	if (p >= new_srnode->current_parent_since)
	  break;
	if (!new_srnode->snaps.count(p))
	  new_srnode->past_parent_snaps.insert(p);
      }
      new_srnode->seq = snaprealm->get_newest_seq();
      new_srnode->past_parents.clear();
    }
    if (snaprealm)
      snaprealm->past_parents_dirty = false;
  } else {
    if (snapid == 0)
      snapid = mdcache->get_global_snaprealm()->get_newest_seq();
    new_srnode = new sr_t();
    new_srnode->seq = snapid;
    new_srnode->created = snapid;
    new_srnode->current_parent_since = get_oldest_snap();
  }
  return new_srnode;
}

const sr_t *CInode::get_projected_srnode() const {
  if (num_projected_srnodes > 0) {
    for (auto it = projected_nodes.rbegin(); it != projected_nodes.rend(); ++it)
      if (it->snapnode != projected_inode::UNDEF_SRNODE)
	return it->snapnode;
  }
  if (snaprealm)
    return &snaprealm->srnode;
  else
    return NULL;
}

void CInode::project_snaprealm(sr_t *new_srnode)
{
  dout(10) << __func__ << " " << new_srnode << dendl;
  ceph_assert(projected_nodes.back().snapnode == projected_inode::UNDEF_SRNODE);
  projected_nodes.back().snapnode = new_srnode;
  ++num_projected_srnodes;
}

void CInode::mark_snaprealm_global(sr_t *new_srnode)
{
  ceph_assert(!is_dir());
  // 'last_destroyed' is no longer used, use it to store origin 'current_parent_since'
  new_srnode->last_destroyed = new_srnode->current_parent_since;
  new_srnode->current_parent_since = mdcache->get_global_snaprealm()->get_newest_seq() + 1;
  new_srnode->mark_parent_global();
}

void CInode::clear_snaprealm_global(sr_t *new_srnode)
{
  // restore 'current_parent_since'
  new_srnode->current_parent_since = new_srnode->last_destroyed;
  new_srnode->last_destroyed = 0;
  new_srnode->seq = mdcache->get_global_snaprealm()->get_newest_seq();
  new_srnode->clear_parent_global();
}

bool CInode::is_projected_snaprealm_global() const
{
  const sr_t *srnode = get_projected_srnode();
  if (srnode && srnode->is_parent_global())
    return true;
  return false;
}

void CInode::project_snaprealm_past_parent(SnapRealm *newparent)
{
  sr_t *new_snap = project_snaprealm();
  record_snaprealm_past_parent(new_snap, newparent);
}


/* if newparent != parent, add parent to past_parents
 if parent DNE, we need to find what the parent actually is and fill that in */
void CInode::record_snaprealm_past_parent(sr_t *new_snap, SnapRealm *newparent)
{
  ceph_assert(!new_snap->is_parent_global());
  SnapRealm *oldparent;
  if (!snaprealm) {
    oldparent = find_snaprealm();
  } else {
    oldparent = snaprealm->parent;
  }

  if (newparent != oldparent) {
    snapid_t oldparentseq = oldparent->get_newest_seq();
    if (oldparentseq + 1 > new_snap->current_parent_since) {
      // copy old parent's snaps
      const set<snapid_t>& snaps = oldparent->get_snaps();
      auto p = snaps.lower_bound(new_snap->current_parent_since);
      if (p != snaps.end())
	new_snap->past_parent_snaps.insert(p, snaps.end());
      if (oldparentseq > new_snap->seq)
	new_snap->seq = oldparentseq;
    }
    new_snap->current_parent_since = mdcache->get_global_snaprealm()->get_newest_seq() + 1;
  }
}

void CInode::record_snaprealm_parent_dentry(sr_t *new_snap, SnapRealm *newparent,
					    CDentry *dn, bool primary_dn)
{
  ceph_assert(new_snap->is_parent_global());
  SnapRealm *oldparent = dn->get_dir()->inode->find_snaprealm();
  auto& snaps = oldparent->get_snaps();

  if (!primary_dn) {
    auto p = snaps.lower_bound(dn->first);
    if (p != snaps.end())
      new_snap->past_parent_snaps.insert(p, snaps.end());
  } else if (newparent != oldparent) {
    // 'last_destroyed' is used as 'current_parent_since'
    auto p = snaps.lower_bound(new_snap->last_destroyed);
    if (p != snaps.end())
      new_snap->past_parent_snaps.insert(p, snaps.end());
    new_snap->last_destroyed = mdcache->get_global_snaprealm()->get_newest_seq() + 1;
  }
}

void CInode::early_pop_projected_snaprealm()
{
  ceph_assert(!projected_nodes.empty());
  if (projected_nodes.front().snapnode != projected_inode::UNDEF_SRNODE) {
    pop_projected_snaprealm(projected_nodes.front().snapnode, true);
    projected_nodes.front().snapnode = projected_inode::UNDEF_SRNODE;
    --num_projected_srnodes;
  }
}

void CInode::pop_projected_snaprealm(sr_t *next_snaprealm, bool early)
{
  if (next_snaprealm) {
    dout(10) << __func__ << (early ? " (early) " : " ")
	     << next_snaprealm << " seq " << next_snaprealm->seq << dendl;
    bool invalidate_cached_snaps = false;
    if (!snaprealm) {
      open_snaprealm();
    } else if (!snaprealm->srnode.past_parents.empty()) {
      invalidate_cached_snaps = true;
      // re-open past parents
      snaprealm->close_parents();

      dout(10) << " realm " << *snaprealm << " past_parents " << snaprealm->srnode.past_parents
	       << " -> " << next_snaprealm->past_parents << dendl;
    }
    auto old_flags = snaprealm->srnode.flags;
    snaprealm->srnode = *next_snaprealm;
    delete next_snaprealm;

    if ((snaprealm->srnode.flags ^ old_flags) & sr_t::PARENT_GLOBAL) {
      snaprealm->close_parents();
      snaprealm->adjust_parent();
    }

    // we should be able to open these up (or have them already be open).
    bool ok = snaprealm->_open_parents(NULL);
    ceph_assert(ok);

    if (invalidate_cached_snaps)
      snaprealm->invalidate_cached_snaps();

    if (snaprealm->parent)
      dout(10) << " realm " << *snaprealm << " parent " << *snaprealm->parent << dendl;
  } else {
    dout(10) << __func__ << (early ? " (early) null" : " null") << dendl;
    ceph_assert(snaprealm);
    snaprealm->merge_to(NULL);
  }
}


// ====== CInode =======

// dirfrags

InodeStoreBase::inode_const_ptr InodeStoreBase::empty_inode = InodeStoreBase::allocate_inode();

__u32 InodeStoreBase::hash_dentry_name(std::string_view dn)
{
  int which = inode->dir_layout.dl_dir_hash;
  if (!which)
    which = CEPH_STR_HASH_LINUX;
  ceph_assert(ceph_str_hash_valid(which));
  return ceph_str_hash(which, dn.data(), dn.length());
}

frag_t InodeStoreBase::pick_dirfrag(std::string_view dn)
{
  if (dirfragtree.empty())
    return frag_t();          // avoid the string hash if we can.

  __u32 h = hash_dentry_name(dn);
  return dirfragtree[h];
}

std::pair<bool, std::vector<CDir*>> CInode::get_dirfrags_under(frag_t fg)
{
  std::pair<bool, std::vector<CDir*>> result;
  auto& all = result.first;
  auto& dirs = result.second;
  all = false;
  
  if (auto it = dirfrags.find(fg); it != dirfrags.end()){
    all = true;
    dirs.push_back(it->second);
    return result;
  }
  
  int total = 0;
  for(auto &[_fg, _dir] : dirfrags){
    // frag_t.bits() can indicate the depth of the partition in the directory tree
    // e.g. 
    // 01*  : bit = 2, on the second floor
    // *
    // 0*      1*
    // 00* 01* 10* 11*     -- > level 2, bit = 2
    // so fragA.bits > fragB.bits means fragA is deeper than fragB

    if (fg.bits() >= _fg.bits()) {
      if (_fg.contains(fg)) {
	all = true;
	return result;
      }
    } else {
      if (fg.contains(_fg)) {
	dirs.push_back(_dir);
	// we can calculate how many sub slices a slice can be divided into
	// frag_t(*) can be divided into two frags belonging to the first layer(0* 1*)
	//           or 2^2 frags belonging to the second layer(00* 01* 10* 11*)
	//           or (1 << (24 - frag_t(*).bits)) frags belonging to the 24th level
	total += 1 << (24 - _fg.bits());
      }
    }
  }

  // we convert all the frags into the frags of 24th layer to calculate whether all the frags are included in the memory cache
  all = ((1<<(24-fg.bits())) == total);
  return result;
}

void CInode::verify_dirfrags()
{
  bool bad = false;
  for (const auto &p : dirfrags) {
    if (!dirfragtree.is_leaf(p.first)) {
      dout(0) << "have open dirfrag " << p.first << " but not leaf in " << dirfragtree
	      << ": " << *p.second << dendl;
      bad = true;
    }
  }
  ceph_assert(!bad);
}

void CInode::force_dirfrags()
{
  bool bad = false;
  for (auto &p : dirfrags) {
    if (!dirfragtree.is_leaf(p.first)) {
      dout(0) << "have open dirfrag " << p.first << " but not leaf in " << dirfragtree
	      << ": " << *p.second << dendl;
      bad = true;
    }
  }

  if (bad) {
    frag_vec_t leaves;
    dirfragtree.get_leaves(leaves);
    for (const auto& leaf : leaves) {
      mdcache->get_force_dirfrag(dirfrag_t(ino(), leaf), true);
    }
  }

  verify_dirfrags();
}

CDir *CInode::get_approx_dirfrag(frag_t fg)
{
  CDir *dir = get_dirfrag(fg);
  if (dir) return dir;

  // find a child?
  auto&& p = get_dirfrags_under(fg);
  if (!p.second.empty())
    return p.second.front();

  // try parents?
  while (fg.bits() > 0) {
    fg = fg.parent();
    dir = get_dirfrag(fg);
    if (dir) return dir;
  }
  return NULL;
}	

CDir *CInode::get_or_open_dirfrag(MDCache *mdcache, frag_t fg)
{
  ceph_assert(is_dir());

  // have it?
  CDir *dir = get_dirfrag(fg);
  if (!dir) {
    // create it.
    ceph_assert(is_auth() || mdcache->mds->is_any_replay());
    dir = new CDir(this, fg, mdcache, is_auth());
    add_dirfrag(dir);
  }
  return dir;
}

CDir *CInode::add_dirfrag(CDir *dir)
{
  auto em = dirfrags.emplace(std::piecewise_construct, std::forward_as_tuple(dir->dirfrag().frag), std::forward_as_tuple(dir));
  ceph_assert(em.second);

  if (stickydir_ref > 0) {
    dir->state_set(CDir::STATE_STICKY);
    dir->get(CDir::PIN_STICKY);
  }

  maybe_pin();

  return dir;
}

void CInode::close_dirfrag(frag_t fg)
{
  dout(14) << __func__ << " " << fg << dendl;
  ceph_assert(dirfrags.count(fg));
  
  CDir *dir = dirfrags[fg];
  dir->remove_null_dentries();
  
  // clear dirty flag
  if (dir->is_dirty())
    dir->mark_clean();
  
  if (stickydir_ref > 0) {
    dir->state_clear(CDir::STATE_STICKY);
    dir->put(CDir::PIN_STICKY);
  }

  if (dir->is_subtree_root())
    num_subtree_roots--;
  
  // dump any remaining dentries, for debugging purposes
  for (const auto &p : dir->items)
    dout(14) << __func__ << " LEFTOVER dn " << *p.second << dendl;

  ceph_assert(dir->get_num_ref() == 0);
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
  if (num_subtree_roots > 0) {
    if (auth == -1)
      return true;
    for (const auto &p : dirfrags) {
      if (p.second->is_subtree_root() &&
	  p.second->dir_auth.first == auth)
	return true;
    }
  }
  return false;
}

bool CInode::has_subtree_or_exporting_dirfrag()
{
  if (num_subtree_roots > 0 || num_exporting_dirs > 0)
    return true;
  return false;
}

void CInode::get_stickydirs()
{
  if (stickydir_ref == 0) {
    get(PIN_STICKYDIRS);
    for (const auto &p : dirfrags) {
      p.second->state_set(CDir::STATE_STICKY);
      p.second->get(CDir::PIN_STICKY);
    }
  }
  stickydir_ref++;
}

void CInode::put_stickydirs()
{
  ceph_assert(stickydir_ref > 0);
  stickydir_ref--;
  if (stickydir_ref == 0) {
    put(PIN_STICKYDIRS);
    for (const auto &p : dirfrags) {
      p.second->state_clear(CDir::STATE_STICKY);
      p.second->put(CDir::PIN_STICKY);
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

bool CInode::is_ancestor_of(const CInode *other) const
{
  while (other) {
    if (other == this)
      return true;
    const CDentry *pdn = other->get_oldest_parent_dn();
    if (!pdn) {
      ceph_assert(other->is_base());
      break;
    }
    other = pdn->get_dir()->get_inode();
  }
  return false;
}

bool CInode::is_projected_ancestor_of(const CInode *other) const
{
  while (other) {
    if (other == this)
      return true;
    const CDentry *pdn = other->get_projected_parent_dn();
    if (!pdn) {
      ceph_assert(other->is_base());
      break;
    }
    other = pdn->get_dir()->get_inode();
  }
  return false;
}

/*
 * Because a non-directory inode may have multiple links, the use_parent
 * argument allows selecting which parent to use for path construction. This
 * argument is only meaningful for the final component (i.e. the first of the
 * nested calls) because directories cannot have multiple hard links. If
 * use_parent is NULL and projected is true, the primary parent's projected
 * inode is used all the way up the path chain. Otherwise the primary parent
 * stable inode is used.
 */
void CInode::make_path_string(string& s, bool projected, const CDentry *use_parent) const
{
  if (!use_parent) {
    use_parent = projected ? get_projected_parent_dn() : parent;
  }

  if (use_parent) {
    use_parent->make_path_string(s, projected);
  } else if (is_root()) {
    s = "";
  } else if (is_mdsdir()) {
    char t[40];
    uint64_t eino(ino());
    eino -= MDS_INO_MDSDIR_OFFSET;
    snprintf(t, sizeof(t), "~mds%" PRId64, eino);
    s = t;
  } else {
    char n[40];
    uint64_t eino(ino());
    snprintf(n, sizeof(n), "#%" PRIx64, eino);
    s += n;
  }
}

void CInode::make_path(filepath& fp, bool projected) const
{
  const CDentry *use_parent = projected ? get_projected_parent_dn() : parent;
  if (use_parent) {
    ceph_assert(!is_base());
    use_parent->make_path(fp, projected);
  } else {
    fp = filepath(ino());
  }
}

void CInode::name_stray_dentry(string& dname)
{
  char s[20];
  snprintf(s, sizeof(s), "%llx", (unsigned long long)ino().val);
  dname = s;
}

version_t CInode::pre_dirty()
{
  version_t pv;
  CDentry* _cdentry = get_projected_parent_dn(); 
  if (_cdentry) {
    pv = _cdentry->pre_dirty(get_projected_version());
    dout(10) << "pre_dirty " << pv << " (current v " << get_inode()->version << ")" << dendl;
  } else {
    ceph_assert(is_base());
    pv = get_projected_version() + 1;
  }
  // force update backtrace for old format inode (see mempool_inode::decode)
  if (get_inode()->backtrace_version == 0 && !projected_nodes.empty()) {
    auto pi = _get_projected_inode();
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
    ceph_assert(ls);
  }
  
  // move myself to this segment's dirty list
  if (ls) 
    ls->dirty_inodes.push_back(&item_dirty);
}

void CInode::mark_dirty(LogSegment *ls) {
  
  dout(10) << __func__ << " " << *this << dendl;

  /*
    NOTE: I may already be dirty, but this fn _still_ needs to be called so that
    the directory is (perhaps newly) dirtied, and so that parent_dir_version is 
    updated below.
  */
  
  // only auth can get dirty.  "dirty" async data in replicas is relative to
  // filelock state, not the dirty flag.
  ceph_assert(is_auth());
  
  // touch my private version
  _mark_dirty(ls);

  // mark dentry too
  if (parent)
    parent->mark_dirty(get_version(), ls);
}


void CInode::mark_clean()
{
  dout(10) << __func__ << " " << *this << dendl;
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
  void finish(int r) override {
    in->_stored(r, version, fin);
  }
  void print(ostream& out) const override {
    out << "inode_store(" << in->ino() << ")";
  }
};

object_t InodeStoreBase::get_object_name(inodeno_t ino, frag_t fg, std::string_view suffix)
{
  char n[60];
  snprintf(n, sizeof(n), "%llx.%08llx", (long long unsigned)ino, (long long unsigned)fg);
  ceph_assert(strlen(n) + suffix.size() < sizeof n);
  strncat(n, suffix.data(), suffix.size());
  return object_t(n);
}

void CInode::store(MDSContext *fin)
{
  dout(10) << __func__ << " " << get_version() << dendl;
  ceph_assert(is_base());

  if (snaprealm)
    purge_stale_snap_data(snaprealm->get_snaps());

  // encode
  bufferlist bl;
  string magic = CEPH_FS_ONDISK_MAGIC;
  using ceph::encode;
  encode(magic, bl);
  encode_store(bl, mdcache->mds->mdsmap->get_up_features());

  // write it.
  SnapContext snapc;
  ObjectOperation m;
  m.write_full(bl);

  object_t oid = CInode::get_object_name(ino(), frag_t(), ".inode");
  object_locator_t oloc(mdcache->mds->mdsmap->get_metadata_pool());

  Context *newfin =
    new C_OnFinisher(new C_IO_Inode_Stored(this, get_version(), fin),
		     mdcache->mds->finisher);
  mdcache->mds->objecter->mutate(oid, oloc, m, snapc,
				 ceph::real_clock::now(), 0,
				 newfin);
}

void CInode::_stored(int r, version_t v, Context *fin)
{
  if (r < 0) {
    dout(1) << "store error " << r << " v " << v << " on " << *this << dendl;
    mdcache->mds->clog->error() << "failed to store inode " << ino()
                                << " object: " << cpp_strerror(r);
    mdcache->mds->handle_write_error(r);
    fin->complete(r);
    return;
  }

  dout(10) << __func__ << " " << v << " on " << *this << dendl;
  if (v == get_projected_version())
    mark_clean();

  fin->complete(0);
}

void CInode::flush(MDSContext *fin)
{
  dout(10) << __func__ << " " << *this << dendl;
  ceph_assert(is_auth() && can_auth_pin());

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
  void finish(int r) override {
    // Ignore 'r', because we fetch from two places, so r is usually ENOENT
    in->_fetched(bl, bl2, fin);
  }
  void print(ostream& out) const override {
    out << "inode_fetch(" << in->ino() << ")";
  }
};

void CInode::fetch(MDSContext *fin)
{
  dout(10) << __func__  << dendl;

  C_IO_Inode_Fetched *c = new C_IO_Inode_Fetched(this, fin);
  C_GatherBuilder gather(g_ceph_context, new C_OnFinisher(c, mdcache->mds->finisher));

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
  dout(10) << __func__ << " got " << bl.length() << " and " << bl2.length() << dendl;
  bufferlist::const_iterator p;
  if (bl2.length()) {
    p = bl2.cbegin();
  } else if (bl.length()) {
    p = bl.cbegin();
  } else {
    derr << "No data while reading inode " << ino() << dendl;
    fin->complete(-ENOENT);
    return;
  }

  using ceph::decode;
  // Attempt decode
  try {
    string magic;
    decode(magic, p);
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
    derr << "Corrupt inode " << ino() << ": " << err.what() << dendl;
    fin->complete(-EINVAL);
    return;
  }
}

void CInode::build_backtrace(int64_t pool, inode_backtrace_t& bt)
{
  bt.ino = ino();
  bt.ancestors.clear();
  bt.pool = pool;

  CInode *in = this;
  CDentry *pdn = get_parent_dn();
  while (pdn) {
    CInode *diri = pdn->get_dir()->get_inode();
    bt.ancestors.push_back(inode_backpointer_t(diri->ino(), pdn->get_name(), in->get_inode()->version));
    in = diri;
    pdn = in->get_parent_dn();
  }
  for (auto &p : get_inode()->old_pools) {
    // don't add our own pool id to old_pools to avoid looping (e.g. setlayout 0, 1, 0)
    if (p != pool)
      bt.old_pools.insert(p);
  }
}

struct C_IO_Inode_StoredBacktrace : public CInodeIOContext {
  version_t version;
  Context *fin;
  C_IO_Inode_StoredBacktrace(CInode *i, version_t v, Context *f) : CInodeIOContext(i), version(v), fin(f) {}
  void finish(int r) override {
    in->_stored_backtrace(r, version, fin);
  }
  void print(ostream& out) const override {
    out << "backtrace_store(" << in->ino() << ")";
  }
};

void CInode::store_backtrace(MDSContext *fin, int op_prio)
{
  dout(10) << __func__ << " on " << *this << dendl;
  ceph_assert(is_dirty_parent());

  if (op_prio < 0)
    op_prio = CEPH_MSG_PRIO_DEFAULT;

  auth_pin(this);

  const int64_t pool = get_backtrace_pool();
  inode_backtrace_t bt;
  build_backtrace(pool, bt);
  bufferlist parent_bl;
  using ceph::encode;
  encode(bt, parent_bl);

  ObjectOperation op;
  op.priority = op_prio;
  op.create(false);
  op.setxattr("parent", parent_bl);

  bufferlist layout_bl;
  encode(get_inode()->layout, layout_bl, mdcache->mds->mdsmap->get_up_features());
  op.setxattr("layout", layout_bl);

  SnapContext snapc;
  object_t oid = get_object_name(ino(), frag_t(), "");
  object_locator_t oloc(pool);
  Context *fin2 = new C_OnFinisher(
    new C_IO_Inode_StoredBacktrace(this, get_inode()->backtrace_version, fin),
    mdcache->mds->finisher);

  if (!state_test(STATE_DIRTYPOOL) || get_inode()->old_pools.empty()) {
    dout(20) << __func__ << ": no dirtypool or no old pools" << dendl;
    mdcache->mds->objecter->mutate(oid, oloc, op, snapc,
				   ceph::real_clock::now(),
				   0, fin2);
    return;
  }

  C_GatherBuilder gather(g_ceph_context, fin2);
  mdcache->mds->objecter->mutate(oid, oloc, op, snapc,
				 ceph::real_clock::now(),
				 0, gather.new_sub());

  // In the case where DIRTYPOOL is set, we update all old pools backtraces
  // such that anyone reading them will see the new pool ID in
  // inode_backtrace_t::pool and go read everything else from there.
  for (const auto &p : get_inode()->old_pools) {
    if (p == pool)
      continue;

    dout(20) << __func__ << ": updating old pool " << p << dendl;

    ObjectOperation op;
    op.priority = op_prio;
    op.create(false);
    op.setxattr("parent", parent_bl);

    object_locator_t oloc(p);
    mdcache->mds->objecter->mutate(oid, oloc, op, snapc,
				   ceph::real_clock::now(),
				   0, gather.new_sub());
  }
  gather.activate();
}

void CInode::_stored_backtrace(int r, version_t v, Context *fin)
{
  if (r == -ENOENT) {
    const int64_t pool = get_backtrace_pool();
    bool exists = mdcache->mds->objecter->with_osdmap(
        [pool](const OSDMap &osd_map) {
          return osd_map.have_pg_pool(pool);
        });

    // This ENOENT is because the pool doesn't exist (the user deleted it
    // out from under us), so the backtrace can never be written, so pretend
    // to succeed so that the user can proceed to e.g. delete the file.
    if (!exists) {
      dout(4) << __func__ << " got ENOENT: a data pool was deleted "
                 "beneath us!" << dendl;
      r = 0;
    }
  }

  if (r < 0) {
    dout(1) << "store backtrace error " << r << " v " << v << dendl;
    mdcache->mds->clog->error() << "failed to store backtrace on ino "
				<< ino() << " object"
                                << ", pool " << get_backtrace_pool()
                                << ", errno " << r;
    mdcache->mds->handle_write_error(r);
    if (fin)
      fin->complete(r);
    return;
  }

  dout(10) << __func__ << " v " << v <<  dendl;

  auth_unpin(this);
  if (v == get_inode()->backtrace_version)
    clear_dirty_parent();
  if (fin)
    fin->complete(0);
}

void CInode::fetch_backtrace(Context *fin, bufferlist *backtrace)
{
  mdcache->fetch_backtrace(ino(), get_backtrace_pool(), *backtrace, fin);
}

void CInode::mark_dirty_parent(LogSegment *ls, bool dirty_pool)
{
  if (!state_test(STATE_DIRTYPARENT)) {
    dout(10) << __func__ << dendl;
    state_set(STATE_DIRTYPARENT);
    get(PIN_DIRTYPARENT);
    ceph_assert(ls);
  }
  if (dirty_pool)
    state_set(STATE_DIRTYPOOL);
  if (ls)
    ls->dirty_parent_inodes.push_back(&item_dirty_parent);
}

void CInode::clear_dirty_parent()
{
  if (state_test(STATE_DIRTYPARENT)) {
    dout(10) << __func__ << dendl;
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

  dout(10) << __func__ << dendl;

  if (err == 0) {
    inode_backtrace_t backtrace;
    using ceph::decode;
    decode(backtrace, bl);
    CDentry *pdn = get_parent_dn();
    if (backtrace.ancestors.empty() ||
	backtrace.ancestors[0].dname != pdn->get_name() ||
	backtrace.ancestors[0].dirino != pdn->get_dir()->ino())
      err = -EINVAL;
  }

  if (err) {
    MDSRank *mds = mdcache->mds;
    mds->clog->error() << "bad backtrace on directory inode " << ino();
    ceph_assert(!"bad backtrace" == (g_conf()->mds_verify_backtrace > 1));

    mark_dirty_parent(mds->mdlog->get_current_segment(), false);
    mds->mdlog->flush();
  }
}

// ------------------
// parent dir


void InodeStoreBase::encode_xattrs(bufferlist &bl) const {
  using ceph::encode;
  if (xattrs)
    encode(*xattrs, bl);
  else
    encode((__u32)0, bl);
}

void InodeStoreBase::decode_xattrs(bufferlist::const_iterator &p) {
  using ceph::decode;
  mempool_xattr_map tmp;
  decode_noshare(tmp, p);
  if (tmp.empty()) {
    reset_xattrs(xattr_map_ptr());
  } else {
    reset_xattrs(allocate_xattr_map(std::move(tmp)));
  }
}

void InodeStoreBase::encode_old_inodes(bufferlist &bl, uint64_t features) const {
  using ceph::encode;
  if (old_inodes)
    encode(*old_inodes, bl, features);
  else
    encode((__u32)0, bl);
}

void InodeStoreBase::decode_old_inodes(bufferlist::const_iterator &p) {
  using ceph::decode;
  mempool_old_inode_map tmp;
  decode(tmp, p);
  if (tmp.empty()) {
    reset_old_inodes(old_inode_map_ptr());
  } else {
    reset_old_inodes(allocate_old_inode_map(std::move(tmp)));
  }
}

void InodeStoreBase::encode_bare(bufferlist &bl, uint64_t features,
				 const bufferlist *snap_blob) const
{
  using ceph::encode;
  encode(*inode, bl, features);
  if (inode->is_symlink())
    encode(symlink, bl);
  encode(dirfragtree, bl);
  encode_xattrs(bl);

  if (snap_blob)
    encode(*snap_blob, bl);
  else
    encode(bufferlist(), bl);
  encode_old_inodes(bl, features);
  encode(oldest_snap, bl);
  encode(damage_flags, bl);
}

void InodeStoreBase::encode(bufferlist &bl, uint64_t features,
			    const bufferlist *snap_blob) const
{
  ENCODE_START(6, 4, bl);
  encode_bare(bl, features, snap_blob);
  ENCODE_FINISH(bl);
}

void CInode::encode_store(bufferlist& bl, uint64_t features)
{
  bufferlist snap_blob;
  encode_snap_blob(snap_blob);
  InodeStoreBase::encode(bl, mdcache->mds->mdsmap->get_up_features(),
			 &snap_blob);
}

void InodeStoreBase::decode_bare(bufferlist::const_iterator &bl,
			      bufferlist& snap_blob, __u8 struct_v)
{
  using ceph::decode;

  auto _inode = allocate_inode();
  decode(*_inode, bl);

  if (_inode->is_symlink()) {
    std::string tmp;
    decode(tmp, bl);
    symlink = std::string_view(tmp);
  }
  decode(dirfragtree, bl);
  decode_xattrs(bl);
  decode(snap_blob, bl);

  decode_old_inodes(bl);
  if (struct_v == 2 && _inode->is_dir()) {
    bool default_layout_exists;
    decode(default_layout_exists, bl);
    if (default_layout_exists) {
      decode(struct_v, bl); // this was a default_file_layout
      decode(_inode->layout, bl); // but we only care about the layout portion
    }
  }

  if (struct_v >= 5) {
    // InodeStore is embedded in dentries without proper versioning, so
    // we consume up to the end of the buffer
    if (!bl.end()) {
      decode(oldest_snap, bl);
    }

    if (!bl.end()) {
      decode(damage_flags, bl);
    }
  }

  reset_inode(std::move(_inode));
}


void InodeStoreBase::decode(bufferlist::const_iterator &bl, bufferlist& snap_blob)
{
  DECODE_START_LEGACY_COMPAT_LEN(5, 4, 4, bl);
  decode_bare(bl, snap_blob, struct_v);
  DECODE_FINISH(bl);
}

void CInode::decode_store(bufferlist::const_iterator& bl)
{
  bufferlist snap_blob;
  InodeStoreBase::decode(bl, snap_blob);
  decode_snap_blob(snap_blob);
}

// ------------------
// locking

SimpleLock* CInode::get_lock(int type)
{
  switch (type) {
    case CEPH_LOCK_IVERSION: return &versionlock;
    case CEPH_LOCK_IFILE: return &filelock;
    case CEPH_LOCK_IAUTH: return &authlock;
    case CEPH_LOCK_ILINK: return &linklock;
    case CEPH_LOCK_IDFT: return &dirfragtreelock;
    case CEPH_LOCK_IXATTR: return &xattrlock;
    case CEPH_LOCK_ISNAP: return &snaplock;
    case CEPH_LOCK_INEST: return &nestlock;
    case CEPH_LOCK_IFLOCK: return &flocklock;
    case CEPH_LOCK_IPOLICY: return &policylock;
  }
  return 0;
}

void CInode::set_object_info(MDSCacheObjectInfo &info)
{
  info.ino = ino();
  info.snapid = last;
}

void CInode::encode_lock_iauth(bufferlist& bl)
{
  ENCODE_START(1, 1, bl);
  encode(get_inode()->version, bl);
  encode(get_inode()->ctime, bl);
  encode(get_inode()->mode, bl);
  encode(get_inode()->uid, bl);
  encode(get_inode()->gid, bl);  
  ENCODE_FINISH(bl);
}

void CInode::decode_lock_iauth(bufferlist::const_iterator& p)
{
  ceph_assert(!is_auth());
  auto _inode = allocate_inode(*get_inode());
  DECODE_START(1, p);
  decode(_inode->version, p);
  utime_t tm;
  decode(tm, p);
  if (_inode->ctime < tm) _inode->ctime = tm;
  decode(_inode->mode, p);
  decode(_inode->uid, p);
  decode(_inode->gid, p);
  DECODE_FINISH(p);
  reset_inode(std::move(_inode));
}

void CInode::encode_lock_ilink(bufferlist& bl)
{
  ENCODE_START(1, 1, bl);
  encode(get_inode()->version, bl);
  encode(get_inode()->ctime, bl);
  encode(get_inode()->nlink, bl);
  ENCODE_FINISH(bl);
}

void CInode::decode_lock_ilink(bufferlist::const_iterator& p)
{
  ceph_assert(!is_auth());
  auto _inode = allocate_inode(*get_inode());
  DECODE_START(1, p);
  decode(_inode->version, p);
  utime_t tm;
  decode(tm, p);
  if (_inode->ctime < tm) _inode->ctime = tm;
  decode(_inode->nlink, p);
  DECODE_FINISH(p);
  reset_inode(std::move(_inode));
}

void CInode::encode_lock_idft(bufferlist& bl)
{
  ENCODE_START(1, 1, bl);
  if (is_auth()) {
    encode(get_inode()->version, bl);
  } else {
    // treat flushing as dirty when rejoining cache
    bool dirty = dirfragtreelock.is_dirty_or_flushing();
    encode(dirty, bl);
  }
  {
    // encode the raw tree
    encode(dirfragtree, bl);

    // also specify which frags are mine
    set<frag_t> myfrags;
    auto&& dfls = get_dirfrags();
    for (const auto& dir : dfls) {
      if (dir->is_auth()) {
	frag_t fg = dir->get_frag();
	myfrags.insert(fg);
      }
    }
    encode(myfrags, bl);
  }
  ENCODE_FINISH(bl);
}

void CInode::decode_lock_idft(bufferlist::const_iterator& p)
{
  inode_ptr _inode;

  DECODE_START(1, p);
  if (is_auth()) {
    bool replica_dirty;
    decode(replica_dirty, p);
    if (replica_dirty) {
      dout(10) << __func__ << " setting dftlock dirty flag" << dendl;
      dirfragtreelock.mark_dirty();  // ok bc we're auth and caller will handle
    }
  } else {
    _inode = allocate_inode(*get_inode());
    decode(_inode->version, p);
  }
  {
    fragtree_t temp;
    decode(temp, p);
    set<frag_t> authfrags;
    decode(authfrags, p);
    if (is_auth()) {
      // auth.  believe replica's auth frags only.
      for (auto fg : authfrags) {
        if (!dirfragtree.is_leaf(fg)) {
          dout(10) << " forcing frag " << fg << " to leaf (split|merge)" << dendl;
          dirfragtree.force_to_leaf(g_ceph_context, fg);
          dirfragtreelock.mark_dirty();  // ok bc we're auth and caller will handle
        }
      }
    } else {
      // replica.  take the new tree, BUT make sure any open
      //  dirfrags remain leaves (they may have split _after_ this
      //  dft was scattered, or we may still be be waiting on the
      //  notify from the auth)
      dirfragtree.swap(temp);
      for (const auto &p : dirfrags) {
        if (!dirfragtree.is_leaf(p.first)) {
          dout(10) << " forcing open dirfrag " << p.first << " to leaf (racing with split|merge)" << dendl;
          dirfragtree.force_to_leaf(g_ceph_context, p.first);
        }
	if (p.second->is_auth())
	  p.second->state_clear(CDir::STATE_DIRTYDFT);
      }
    }
    if (g_conf()->mds_debug_frag)
      verify_dirfrags();
  }
  DECODE_FINISH(p);

  if (_inode)
    reset_inode(std::move(_inode));
}

void CInode::encode_lock_ifile(bufferlist& bl)
{
  ENCODE_START(1, 1, bl);
  if (is_auth()) {
    encode(get_inode()->version, bl); 
    encode(get_inode()->ctime, bl); 
    encode(get_inode()->mtime, bl); 
    encode(get_inode()->atime, bl); 
    encode(get_inode()->time_warp_seq, bl); 
    if (!is_dir()) {
      encode(get_inode()->layout, bl, mdcache->mds->mdsmap->get_up_features());
      encode(get_inode()->size, bl); 
      encode(get_inode()->truncate_seq, bl); 
      encode(get_inode()->truncate_size, bl); 
      encode(get_inode()->client_ranges, bl); 
      encode(get_inode()->inline_data, bl); 
    }    
  } else {
    // treat flushing as dirty when rejoining cache
    bool dirty = filelock.is_dirty_or_flushing();
    encode(dirty, bl); 
  }    
  dout(15) << __func__ << " inode.dirstat is " << get_inode()->dirstat << dendl;
  encode(get_inode()->dirstat, bl);  // only meaningful if i am auth.
  bufferlist tmp;
  __u32 n = 0;
  for (const auto &p : dirfrags) {
    frag_t fg = p.first;
    CDir *dir = p.second;
    if (is_auth() || dir->is_auth()) {
      fnode_t *pf = dir->get_projected_fnode();
      dout(15) << fg << " " << *dir << dendl;
      dout(20) << fg << "           fragstat " << pf->fragstat << dendl;
      dout(20) << fg << " accounted_fragstat " << pf->accounted_fragstat << dendl;
      encode(fg, tmp);
      encode(dir->first, tmp);
      encode(pf->fragstat, tmp);
      encode(pf->accounted_fragstat, tmp);
      n++;
    }
  }
  encode(n, bl);
  bl.claim_append(tmp);
  ENCODE_FINISH(bl);
}

void CInode::decode_lock_ifile(bufferlist::const_iterator& p)
{
  inode_ptr _inode;

  DECODE_START(1, p);
  if (!is_auth()) {
    _inode = allocate_inode(*get_inode());

    decode(_inode->version, p);
    utime_t tm;
    decode(tm, p);
    if (_inode->ctime < tm) _inode->ctime = tm;
    decode(_inode->mtime, p);
    decode(_inode->atime, p);
    decode(_inode->time_warp_seq, p);
    if (!is_dir()) {
      decode(_inode->layout, p);
      decode(_inode->size, p);
      decode(_inode->truncate_seq, p);
      decode(_inode->truncate_size, p);
      decode(_inode->client_ranges, p);
      decode(_inode->inline_data, p);
    }
  } else {
    bool replica_dirty;
    decode(replica_dirty, p);
    if (replica_dirty) {
      dout(10) << __func__ << " setting filelock dirty flag" << dendl;
      filelock.mark_dirty();  // ok bc we're auth and caller will handle
    }
  }
 
  frag_info_t dirstat;
  decode(dirstat, p);
  if (!is_auth()) {
    dout(10) << " taking inode dirstat " << dirstat << " for " << *this << dendl;
    _inode->dirstat = dirstat;    // take inode summation if replica
  }
  __u32 n;
  decode(n, p);
  dout(10) << " ...got " << n << " fragstats on " << *this << dendl;
  while (n--) {
    frag_t fg;
    snapid_t fgfirst;
    frag_info_t fragstat;
    frag_info_t accounted_fragstat;
    decode(fg, p);
    decode(fgfirst, p);
    decode(fragstat, p);
    decode(accounted_fragstat, p);
    dout(10) << fg << " [" << fgfirst << ",head] " << dendl;
    dout(10) << fg << "           fragstat " << fragstat << dendl;
    dout(20) << fg << " accounted_fragstat " << accounted_fragstat << dendl;

    CDir *dir = get_dirfrag(fg);
    if (is_auth()) {
      ceph_assert(dir);                // i am auth; i had better have this dir open
      dout(10) << fg << " first " << dir->first << " -> " << fgfirst
               << " on " << *dir << dendl;
      dir->first = fgfirst;
      dir->fnode.fragstat = fragstat;
      dir->fnode.accounted_fragstat = accounted_fragstat;
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
                              _inode->dirstat.version, pf->accounted_fragstat.version);
      }
    }
  }
  DECODE_FINISH(p);

  if (_inode)
    reset_inode(std::move(_inode));
}

void CInode::encode_lock_inest(bufferlist& bl)
{
  ENCODE_START(1, 1, bl);
  if (is_auth()) {
    encode(get_inode()->version, bl);
  } else {
    // treat flushing as dirty when rejoining cache
    bool dirty = nestlock.is_dirty_or_flushing();
    encode(dirty, bl);
  }
  dout(15) << __func__ << " inode.rstat is " << get_inode()->rstat << dendl;
  encode(get_inode()->rstat, bl);  // only meaningful if i am auth.
  bufferlist tmp;
  __u32 n = 0;
  for (const auto &p : dirfrags) {
    frag_t fg = p.first;
    CDir *dir = p.second;
    if (is_auth() || dir->is_auth()) {
      fnode_t *pf = dir->get_projected_fnode();
      dout(10) << __func__ << " " << fg << " dir " << *dir << dendl;
      dout(10) << __func__ << " " << fg << " rstat " << pf->rstat << dendl;
      dout(10) << __func__ << " " << fg << " accounted_rstat " << pf->rstat << dendl;
      dout(10) << __func__ << " " << fg << " dirty_old_rstat " << dir->dirty_old_rstat << dendl;
      encode(fg, tmp);
      encode(dir->first, tmp);
      encode(pf->rstat, tmp);
      encode(pf->accounted_rstat, tmp);
      encode(dir->dirty_old_rstat, tmp);
      n++;
    }
  }
  encode(n, bl);
  bl.claim_append(tmp);
  ENCODE_FINISH(bl);
}

void CInode::decode_lock_inest(bufferlist::const_iterator& p)
{
  inode_ptr _inode;

  DECODE_START(1, p);
  if (is_auth()) {
    bool replica_dirty;
    decode(replica_dirty, p);
    if (replica_dirty) {
      dout(10) << __func__ << " setting nestlock dirty flag" << dendl;
      nestlock.mark_dirty();  // ok bc we're auth and caller will handle
    }
  } else {
    _inode = allocate_inode(*get_inode());
    decode(_inode->version, p);
  }
  nest_info_t rstat;
  decode(rstat, p);
  if (!is_auth()) {
    dout(10) << __func__ << " taking inode rstat " << rstat << " for " << *this << dendl;
    _inode->rstat = rstat;    // take inode summation if replica
  }
  __u32 n;
  decode(n, p);
  while (n--) {
    frag_t fg;
    snapid_t fgfirst;
    nest_info_t rstat;
    nest_info_t accounted_rstat;
    decltype(CDir::dirty_old_rstat) dirty_old_rstat;
    decode(fg, p);
    decode(fgfirst, p);
    decode(rstat, p);
    decode(accounted_rstat, p);
    decode(dirty_old_rstat, p);
    dout(10) << __func__ << " " << fg << " [" << fgfirst << ",head]" << dendl;
    dout(10) << __func__ << " " << fg << " rstat " << rstat << dendl;
    dout(10) << __func__ << " " << fg << " accounted_rstat " << accounted_rstat << dendl;
    dout(10) << __func__ << " " << fg << " dirty_old_rstat " << dirty_old_rstat << dendl;
    CDir *dir = get_dirfrag(fg);
    if (is_auth()) {
      ceph_assert(dir);                // i am auth; i had better have this dir open
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
                              _inode->rstat.version, pf->accounted_rstat.version);
      }
    }
  }
  DECODE_FINISH(p);

  if (_inode)
    reset_inode(std::move(_inode));
}

void CInode::encode_lock_ixattr(bufferlist& bl)
{
  ENCODE_START(1, 1, bl);
  encode(get_inode()->version, bl);
  encode(get_inode()->ctime, bl);
  encode_xattrs(bl);
  ENCODE_FINISH(bl);
}

void CInode::decode_lock_ixattr(bufferlist::const_iterator& p)
{
  ceph_assert(!is_auth());
  auto _inode = allocate_inode(*get_inode());
  DECODE_START(1, p);
  decode(_inode->version, p);
  utime_t tm;
  decode(tm, p);
  if (_inode->ctime < tm)
    _inode->ctime = tm;
  decode_xattrs(p);
  DECODE_FINISH(p);
  reset_inode(std::move(_inode));
}

void CInode::encode_lock_isnap(bufferlist& bl)
{
  ENCODE_START(1, 1, bl);
  encode(get_inode()->version, bl);
  encode(get_inode()->ctime, bl);
  encode_snap(bl);
  ENCODE_FINISH(bl);
}

void CInode::decode_lock_isnap(bufferlist::const_iterator& p)
{
  ceph_assert(!is_auth());
  auto _inode = allocate_inode(*get_inode());
  DECODE_START(1, p);
  decode(_inode->version, p);
  utime_t tm;
  decode(tm, p);
  if (_inode->ctime < tm) _inode->ctime = tm;
  decode_snap(p);
  DECODE_FINISH(p);
  reset_inode(std::move(_inode));
}

void CInode::encode_lock_iflock(bufferlist& bl)
{
  ENCODE_START(1, 1, bl);
  encode(get_inode()->version, bl);
  _encode_file_locks(bl);
  ENCODE_FINISH(bl);
}

void CInode::decode_lock_iflock(bufferlist::const_iterator& p)
{
  ceph_assert(!is_auth());
  auto _inode = allocate_inode(*get_inode());
  DECODE_START(1, p);
  decode(_inode->version, p);
  _decode_file_locks(p);
  DECODE_FINISH(p);
  reset_inode(std::move(_inode));
}

void CInode::encode_lock_ipolicy(bufferlist& bl)
{
  ENCODE_START(2, 1, bl);
  if (is_dir()) {
    encode(get_inode()->version, bl);
    encode(get_inode()->ctime, bl);
    encode(get_inode()->layout, bl, mdcache->mds->mdsmap->get_up_features());
    encode(get_inode()->quota, bl);
    encode(get_inode()->export_pin, bl);
    encode(get_inode()->export_ephemeral_distributed_pin, bl);
    encode(get_inode()->export_ephemeral_random_pin, bl);
  }
  ENCODE_FINISH(bl);
}

void CInode::decode_lock_ipolicy(bufferlist::const_iterator& p)
{
  ceph_assert(!is_auth());
  auto _inode = allocate_inode(*get_inode());
  DECODE_START(1, p);
  if (is_dir()) {
    decode(_inode->version, p);
    utime_t tm;
    decode(tm, p);
    if (_inode->ctime < tm)
      _inode->ctime = tm;
    decode(_inode->layout, p);
    decode(_inode->quota, p);
    decode(_inode->export_pin, p);
    if (struct_v >= 2) {
      decode(_inode->export_ephemeral_distributed_pin, p);
      decode(_inode->export_ephemeral_random_pin, p);
    }
  }
  DECODE_FINISH(p);
  mds_rank_t old_export_pin = get_inode()->export_pin;
  bool old_ephemeral_pin = get_inode()->export_ephemeral_distributed_pin;
  reset_inode(std::move(_inode));
  maybe_export_pin(old_export_pin != get_inode()->export_pin);
  maybe_ephemeral_dist_children(old_ephemeral_pin != get_inode()->export_ephemeral_distributed_pin);
}

void CInode::encode_lock_state(int type, bufferlist& bl)
{
  ENCODE_START(1, 1, bl);
  encode(first, bl);
  if (!is_base())
    encode(parent->first, bl);

  switch (type) {
  case CEPH_LOCK_IAUTH:
    encode_lock_iauth(bl);
    break;

  case CEPH_LOCK_ILINK:
    encode_lock_ilink(bl);
    break;

  case CEPH_LOCK_IDFT:
    encode_lock_idft(bl);
    break;

  case CEPH_LOCK_IFILE:
    encode_lock_ifile(bl);
    break;

  case CEPH_LOCK_INEST:
    encode_lock_inest(bl);
    break;
    
  case CEPH_LOCK_IXATTR:
    encode_lock_ixattr(bl);
    break;

  case CEPH_LOCK_ISNAP:
    encode_lock_isnap(bl);
    break;

  case CEPH_LOCK_IFLOCK:
    encode_lock_iflock(bl);
    break;

  case CEPH_LOCK_IPOLICY:
    encode_lock_ipolicy(bl);
    break;
  
  default:
    ceph_abort();
  }
  ENCODE_FINISH(bl);
}

/* for more info on scatterlocks, see comments by Locker::scatter_writebehind */

void CInode::decode_lock_state(int type, const bufferlist& bl)
{
  auto p = bl.cbegin();

  DECODE_START(1, p);
  utime_t tm;

  snapid_t newfirst;
  using ceph::decode;
  decode(newfirst, p);
  if (!is_auth() && newfirst != first) {
    dout(10) << __func__ << " first " << first << " -> " << newfirst << dendl;
    first = newfirst;
  }
  if (!is_base()) {
    decode(newfirst, p);
    if (!parent->is_auth() && newfirst != parent->first) {
      dout(10) << __func__ << " parent first " << first << " -> " << newfirst << dendl;
      parent->first = newfirst;
    }
  }

  switch (type) {
  case CEPH_LOCK_IAUTH:
    decode_lock_iauth(p);
    break;

  case CEPH_LOCK_ILINK:
    decode_lock_ilink(p);
    break;

  case CEPH_LOCK_IDFT:
    decode_lock_idft(p);
    break;

  case CEPH_LOCK_IFILE:
    decode_lock_ifile(p);
    break;

  case CEPH_LOCK_INEST:
    decode_lock_inest(p);
    break;

  case CEPH_LOCK_IXATTR:
    decode_lock_ixattr(p);
    break;

  case CEPH_LOCK_ISNAP:
    decode_lock_isnap(p);
    break;

  case CEPH_LOCK_IFLOCK:
    decode_lock_iflock(p);
    break;

  case CEPH_LOCK_IPOLICY:
    decode_lock_ipolicy(p);
    break;

  default:
    ceph_abort();
  }
  DECODE_FINISH(p);
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
  dout(10) << __func__ << " " << type << " on " << *this << dendl;
  ceph_assert(is_dir());
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
    ceph_abort();
  }
}


/*
 * when we initially scatter a lock, we need to check if any of the dirfrags
 * have out of date accounted_rstat/fragstat.  if so, mark the lock stale.
 */
/* for more info on scatterlocks, see comments by Locker::scatter_writebehind */
void CInode::start_scatter(ScatterLock *lock)
{
  dout(10) << __func__ << " " << *lock << " on " << *this << dendl;
  ceph_assert(is_auth());
  const auto& pi = get_projected_inode();

  for (const auto &p : dirfrags) {
    frag_t fg = p.first;
    CDir *dir = p.second;
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


class C_Inode_FragUpdate : public MDSLogContextBase {
protected:
  CInode *in;
  CDir *dir;
  MutationRef mut;
  MDSRank *get_mds() override {return in->mdcache->mds;}
  void finish(int r) override {
    in->_finish_frag_update(dir, mut);
  }    

public:
  C_Inode_FragUpdate(CInode *i, CDir *d, MutationRef& m) : in(i), dir(d), mut(m) {}
};

void CInode::finish_scatter_update(ScatterLock *lock, CDir *dir,
				   version_t inode_version, version_t dir_accounted_version)
{
  frag_t fg = dir->get_frag();
  ceph_assert(dir->is_auth());

  if (dir->is_frozen()) {
    dout(10) << __func__ << " " << fg << " frozen, marking " << *lock << " stale " << *dir << dendl;
  } else if (dir->get_version() == 0) {
    dout(10) << __func__ << " " << fg << " not loaded, marking " << *lock << " stale " << *dir << dendl;
  } else {
    if (dir_accounted_version != inode_version) {
      dout(10) << __func__ << " " << fg << " journaling accounted scatterstat update v" << inode_version << dendl;

      MDLog *mdlog = mdcache->mds->mdlog;
      MutationRef mut(new MutationImpl());
      mut->ls = mdlog->get_current_segment();

      fnode_t *pf = dir->project_fnode();

      std::string_view ename;
      switch (lock->get_type()) {
      case CEPH_LOCK_IFILE:
	pf->fragstat.version = inode_version;
	pf->accounted_fragstat = pf->fragstat;
	ename = "lock ifile accounted scatter stat update";
	break;
      case CEPH_LOCK_INEST:
	pf->rstat.version = inode_version;
	pf->accounted_rstat = pf->rstat;
	ename = "lock inest accounted scatter stat update";

	if (!is_auth() && lock->get_state() == LOCK_MIX) {
	  dout(10) << __func__ << " try to assimilate dirty rstat on " 
	    << *dir << dendl; 
	  dir->assimilate_dirty_rstat_inodes();
       }

	break;
      default:
	ceph_abort();
      }
	
      pf->version = dir->pre_dirty();
      mut->add_projected_fnode(dir);

      EUpdate *le = new EUpdate(mdlog, ename);
      mdlog->start_entry(le);
      le->metablob.add_dir_context(dir);
      le->metablob.add_dir(dir, true);
      
      ceph_assert(!dir->is_frozen());
      mut->auth_pin(dir);

      if (lock->get_type() == CEPH_LOCK_INEST && 
	  !is_auth() && lock->get_state() == LOCK_MIX) {
        dout(10) << __func__ << " finish assimilating dirty rstat on " 
          << *dir << dendl; 
        dir->assimilate_dirty_rstat_inodes_finish(mut, &le->metablob);

        if (!(pf->rstat == pf->accounted_rstat)) {
          if (!mut->is_wrlocked(&nestlock)) {
            mdcache->mds->locker->wrlock_force(&nestlock, mut);
          }

          mdcache->mds->locker->mark_updated_scatterlock(&nestlock);
          mut->ls->dirty_dirfrag_nest.push_back(&item_dirty_dirfrag_nest);
        }
      }
      
      mdlog->submit_entry(le, new C_Inode_FragUpdate(this, dir, mut));
    } else {
      dout(10) << __func__ << " " << fg << " accounted " << *lock
	       << " scatter stat unchanged at v" << dir_accounted_version << dendl;
    }
  }
}

void CInode::_finish_frag_update(CDir *dir, MutationRef& mut)
{
  dout(10) << __func__ << " on " << *dir << dendl;
  mut->apply();
  mdcache->mds->locker->drop_locks(mut.get());
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

  dout(10) << __func__ << " " << type << " on " << *this << dendl;
  ceph_assert(is_auth());

  switch (type) {
  case CEPH_LOCK_IFILE:
    {
      fragtree_t tmpdft = dirfragtree;
      struct frag_info_t dirstat;
      bool dirstat_valid = true;

      // adjust summation
      ceph_assert(is_auth());
      auto pi = _get_projected_inode();

      bool touched_mtime = false, touched_chattr = false;
      dout(20) << "  orig dirstat " << pi->dirstat << dendl;
      pi->dirstat.version++;
      for (const auto &p : dirfrags) {
	frag_t fg = p.first;
	CDir *dir = p.second;
	dout(20) << fg << " " << *dir << dendl;

	bool update;
	if (dir->get_version() != 0) {
	  update = dir->is_auth() && !dir->is_frozen();
	} else {
	  update = false;
	  dirstat_valid = false;
	}

	fnode_t *pf = dir->get_projected_fnode();
	if (update)
	  pf = dir->project_fnode();

	if (pf->accounted_fragstat.version == pi->dirstat.version - 1) {
	  dout(20) << fg << "           fragstat " << pf->fragstat << dendl;
	  dout(20) << fg << " accounted_fragstat " << pf->accounted_fragstat << dendl;
	  pi->dirstat.add_delta(pf->fragstat, pf->accounted_fragstat, &touched_mtime, &touched_chattr);
	} else {
	  dout(20) << fg << " skipping STALE accounted_fragstat " << pf->accounted_fragstat << dendl;
	}

	if (pf->fragstat.nfiles < 0 ||
	    pf->fragstat.nsubdirs < 0) {
	  clog->error() << "bad/negative dir size on "
	      << dir->dirfrag() << " " << pf->fragstat;
	  ceph_assert(!"bad/negative fragstat" == g_conf()->mds_verify_scatter);
	  
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
      if (touched_chattr)
	pi->change_attr = pi->dirstat.change_attr;
      dout(20) << " final dirstat " << pi->dirstat << dendl;

      if (dirstat_valid && !dirstat.same_sums(pi->dirstat)) {
        frag_vec_t leaves;
        tmpdft.get_leaves_under(frag_t(), leaves);
	for (const auto& leaf : leaves) {
	  if (!dirfrags.count(leaf)) {
	    dirstat_valid = false;
	    break;
	  }
        }
	if (dirstat_valid) {
	  if (state_test(CInode::STATE_REPAIRSTATS)) {
	    dout(20) << " dirstat mismatch, fixing" << dendl;
	  } else {
	    clog->error() << "unmatched fragstat on " << ino() << ", inode has "
			  << pi->dirstat << ", dirfrags have " << dirstat;
	    ceph_assert(!"unmatched fragstat" == g_conf()->mds_verify_scatter);
	  }
	  // trust the dirfrags for now
	  version_t v = pi->dirstat.version;
	  if (pi->dirstat.mtime > dirstat.mtime)
	    dirstat.mtime = pi->dirstat.mtime;
	  if (pi->dirstat.change_attr > dirstat.change_attr)
	    dirstat.change_attr = pi->dirstat.change_attr;
	  pi->dirstat = dirstat;
	  pi->dirstat.version = v;
	}
      }

      if (pi->dirstat.nfiles < 0 || pi->dirstat.nsubdirs < 0)
      {
        std::string path;
        make_path_string(path);
	clog->error() << "Inconsistent statistics detected: fragstat on inode "
                      << ino() << " (" << path << "), inode has " << pi->dirstat;
	ceph_assert(!"bad/negative fragstat" == g_conf()->mds_verify_scatter);

	if (pi->dirstat.nfiles < 0)
	  pi->dirstat.nfiles = 0;
	if (pi->dirstat.nsubdirs < 0)
	  pi->dirstat.nsubdirs = 0;
      }
    }
    break;

  case CEPH_LOCK_INEST:
    {
      // adjust summation
      ceph_assert(is_auth());

      fragtree_t tmpdft = dirfragtree;
      nest_info_t rstat;
      bool rstat_valid = true;

      rstat.rsubdirs = 1;
      if (const sr_t *srnode = get_projected_srnode(); srnode)
	rstat.rsnaps = srnode->snaps.size();

      auto pi = _get_projected_inode();
      dout(20) << "  orig rstat " << pi->rstat << dendl;
      pi->rstat.version++;
      for (const auto &p : dirfrags) {
	frag_t fg = p.first;
	CDir *dir = p.second;
	dout(20) << fg << " " << *dir << dendl;

	bool update;
	if (dir->get_version() != 0) {
	  update = dir->is_auth() && !dir->is_frozen();
	} else {
	  update = false;
	  rstat_valid = false;
	}

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
	  for (auto &p : dir->dirty_old_rstat) {
	    mdcache->project_rstat_frag_to_inode(p.second.rstat, p.second.accounted_rstat,
						 p.second.first, p.first, this, true);
          }
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

      if (rstat_valid && !rstat.same_sums(pi->rstat)) {
        frag_vec_t leaves;
        tmpdft.get_leaves_under(frag_t(), leaves);
        for (const auto& leaf : leaves) {
          if (!dirfrags.count(leaf)) {
	    rstat_valid = false;
	    break;
	  }
        }
	if (rstat_valid) {
	  if (state_test(CInode::STATE_REPAIRSTATS)) {
	    dout(20) << " rstat mismatch, fixing" << dendl;
	  } else {
	    clog->error() << "inconsistent rstat on inode " << ino()
                          << ", inode has " << pi->rstat
                          << ", directory fragments have " << rstat;
	    ceph_assert(!"unmatched rstat" == g_conf()->mds_verify_scatter);
	  }
	  // trust the dirfrag for now
	  version_t v = pi->rstat.version;
	  if (pi->rstat.rctime > rstat.rctime)
	    rstat.rctime = pi->rstat.rctime;
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
    ceph_abort();
  }
}

void CInode::finish_scatter_gather_update_accounted(int type, MutationRef& mut, EMetaBlob *metablob)
{
  dout(10) << __func__ << " " << type << " on " << *this << dendl;
  ceph_assert(is_auth());

  for (const auto &p : dirfrags) {
    CDir *dir = p.second;
    if (!dir->is_auth() || dir->get_version() == 0 || dir->is_frozen())
      continue;
    
    if (type == CEPH_LOCK_IDFT)
      continue;  // nothing to do.

    dout(10) << " journaling updated frag accounted_ on " << *dir << dendl;
    ceph_assert(dir->is_projected());
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

void CInode::add_dir_waiter(frag_t fg, MDSContext *c)
{
  if (waiting_on_dir.empty())
    get(PIN_DIRWAITER);
  waiting_on_dir[fg].push_back(c);
  dout(10) << __func__ << " frag " << fg << " " << c << " on " << *this << dendl;
}

void CInode::take_dir_waiting(frag_t fg, MDSContext::vec& ls)
{
  if (waiting_on_dir.empty())
    return;

  auto it = waiting_on_dir.find(fg);
  if (it != waiting_on_dir.end()) {
    dout(10) << __func__ << " frag " << fg << " on " << *this << dendl;
    auto& waiting = it->second;
    ls.insert(ls.end(), waiting.begin(), waiting.end());
    waiting_on_dir.erase(it);

    if (waiting_on_dir.empty())
      put(PIN_DIRWAITER);
  }
}

void CInode::add_waiter(uint64_t tag, MDSContext *c) 
{
  dout(10) << __func__ << " tag " << std::hex << tag << std::dec << " " << c
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

void CInode::take_waiting(uint64_t mask, MDSContext::vec& ls)
{
  if ((mask & WAIT_DIR) && !waiting_on_dir.empty()) {
    // take all dentry waiters
    while (!waiting_on_dir.empty()) {
      auto it = waiting_on_dir.begin();
      dout(10) << __func__ << " dirfrag " << it->first << " on " << *this << dendl;
      auto& waiting = it->second;
      ls.insert(ls.end(), waiting.begin(), waiting.end());
      waiting_on_dir.erase(it);
    }
    put(PIN_DIRWAITER);
  }

  // waiting
  MDSCacheObject::take_waiting(mask, ls);
}

void CInode::maybe_finish_freeze_inode()
{
  CDir *dir = get_parent_dir();
  if (auth_pins > auth_pin_freeze_allowance || dir->frozen_inode_suppressed)
    return;

  dout(10) << "maybe_finish_freeze_inode - frozen" << dendl;
  ceph_assert(auth_pins == auth_pin_freeze_allowance);
  get(PIN_FROZEN);
  put(PIN_FREEZING);
  state_clear(STATE_FREEZING);
  state_set(STATE_FROZEN);

  item_freezing_inode.remove_myself();
  dir->num_frozen_inodes++;

  finish_waiting(WAIT_FROZEN);
}

bool CInode::freeze_inode(int auth_pin_allowance)
{
  CDir *dir = get_parent_dir();
  ceph_assert(dir);

  ceph_assert(auth_pin_allowance > 0);  // otherwise we need to adjust parent's nested_auth_pins
  ceph_assert(auth_pins >= auth_pin_allowance);
  if (auth_pins == auth_pin_allowance && !dir->frozen_inode_suppressed) {
    dout(10) << "freeze_inode - frozen" << dendl;
    if (!state_test(STATE_FROZEN)) {
      get(PIN_FROZEN);
      state_set(STATE_FROZEN);
      dir->num_frozen_inodes++;
    }
    return true;
  }

  dout(10) << "freeze_inode - waiting for auth_pins to drop to " << auth_pin_allowance << dendl;
  auth_pin_freeze_allowance = auth_pin_allowance;
  dir->freezing_inodes.push_back(&item_freezing_inode);

  get(PIN_FREEZING);
  state_set(STATE_FREEZING);

  if (!dir->lock_caches_with_auth_pins.empty())
    mdcache->mds->locker->invalidate_lock_caches(dir);

  const static int lock_types[] = {
    CEPH_LOCK_IVERSION, CEPH_LOCK_IFILE, CEPH_LOCK_IAUTH, CEPH_LOCK_ILINK, CEPH_LOCK_IDFT,
    CEPH_LOCK_IXATTR, CEPH_LOCK_ISNAP, CEPH_LOCK_INEST, CEPH_LOCK_IFLOCK, CEPH_LOCK_IPOLICY, 0
  };
  for (int i = 0; lock_types[i]; ++i) {
    auto lock = get_lock(lock_types[i]);
    if (lock->is_cached())
      mdcache->mds->locker->invalidate_lock_caches(lock);
  }
  // invalidate_lock_caches() may decrease dir->frozen_inode_suppressed
  // and finish freezing the inode
  return state_test(STATE_FROZEN);
}

void CInode::unfreeze_inode(MDSContext::vec& finished) 
{
  dout(10) << __func__ << dendl;
  if (state_test(STATE_FREEZING)) {
    state_clear(STATE_FREEZING);
    put(PIN_FREEZING);
    item_freezing_inode.remove_myself();
  } else if (state_test(STATE_FROZEN)) {
    state_clear(STATE_FROZEN);
    put(PIN_FROZEN);
    get_parent_dir()->num_frozen_inodes--;
  } else 
    ceph_abort();
  take_waiting(WAIT_UNFREEZE, finished);
}

void CInode::unfreeze_inode()
{
    MDSContext::vec finished;
    unfreeze_inode(finished);
    mdcache->mds->queue_waiters(finished);
}

void CInode::freeze_auth_pin()
{
  ceph_assert(state_test(CInode::STATE_FROZEN));
  state_set(CInode::STATE_FROZENAUTHPIN);
  get_parent_dir()->num_frozen_inodes++;
}

void CInode::unfreeze_auth_pin()
{
  ceph_assert(state_test(CInode::STATE_FROZENAUTHPIN));
  state_clear(CInode::STATE_FROZENAUTHPIN);
  get_parent_dir()->num_frozen_inodes--;
  if (!state_test(STATE_FREEZING|STATE_FROZEN)) {
    MDSContext::vec finished;
    take_waiting(WAIT_UNFREEZE, finished);
    mdcache->mds->queue_waiters(finished);
  }
}

void CInode::clear_ambiguous_auth(MDSContext::vec& finished)
{
  ceph_assert(state_test(CInode::STATE_AMBIGUOUSAUTH));
  state_clear(CInode::STATE_AMBIGUOUSAUTH);
  take_waiting(CInode::WAIT_SINGLEAUTH, finished);
}

void CInode::clear_ambiguous_auth()
{
  MDSContext::vec finished;
  clear_ambiguous_auth(finished);
  mdcache->mds->queue_waiters(finished);
}

// auth_pins
bool CInode::can_auth_pin(int *err_ret) const {
  int err;
  if (!is_auth()) {
    err = ERR_NOT_AUTH;
  } else if (is_freezing_inode() || is_frozen_inode() || is_frozen_auth_pin()) {
    err = ERR_EXPORTING_INODE;
  } else {
    if (parent)
      return parent->can_auth_pin(err_ret);
    err = 0;
  }
  if (err && err_ret)
    *err_ret = err;
  return !err;
}

void CInode::auth_pin(void *by) 
{
  if (auth_pins == 0)
    get(PIN_AUTHPIN);
  auth_pins++;

#ifdef MDS_AUTHPIN_SET
  auth_pin_set.insert(by);
#endif

  dout(10) << "auth_pin by " << by << " on " << *this << " now " << auth_pins << dendl;
  
  if (parent)
    parent->adjust_nested_auth_pins(1, this);
}

void CInode::auth_unpin(void *by) 
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

  if (parent)
    parent->adjust_nested_auth_pins(-1, by);

  if (is_freezing_inode())
    maybe_finish_freeze_inode();
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
  if (is_any_old_inodes())
    t = get_old_inodes()->begin()->second.first;
  return std::min(t, oldest_snap);
}

const CInode::mempool_old_inode& CInode::cow_old_inode(snapid_t follows, bool cow_head)
{
  ceph_assert(follows >= first);

  const auto& pi = cow_head ? get_projected_inode() : get_previous_projected_inode();
  const auto& px = cow_head ? get_projected_xattrs() : get_previous_projected_xattrs();

  auto _old_inodes = allocate_old_inode_map();
  if (old_inodes)
    *_old_inodes = *old_inodes;

  mempool_old_inode &old = (*_old_inodes)[follows];
  old.first = first;
  old.inode = *pi;
  if (px) {
    dout(10) << " " << px->size() << " xattrs cowed, " << *px << dendl;
    old.xattrs = *px;
  }

  if (first < oldest_snap)
    oldest_snap = first;

  old.inode.trim_client_ranges(follows);

  if (g_conf()->mds_snap_rstat &&
      !(old.inode.rstat == old.inode.accounted_rstat))
    dirty_old_rstats.insert(follows);
  
  first = follows+1;

  dout(10) << __func__ << " " << (cow_head ? "head" : "previous_head" )
	   << " to [" << old.first << "," << follows << "] on "
	   << *this << dendl;

  reset_old_inodes(std::move(_old_inodes));
  return old;
}

void CInode::pre_cow_old_inode()
{
  snapid_t follows = mdcache->get_global_snaprealm()->get_newest_seq();
  if (first <= follows)
    cow_old_inode(follows, true);
}

bool CInode::has_snap_data(snapid_t snapid)
{
  bool found = snapid >= first && snapid <= last;
  if (!found && is_any_old_inodes()) {
    auto p = old_inodes->lower_bound(snapid);
    if (p != old_inodes->end()) {
      if (p->second.first > snapid) {
	if  (p != old_inodes->begin())
	  --p;
      }
      if (p->second.first <= snapid && snapid <= p->first) {
	found = true;
      }
    }
  }
  return found;
}

void CInode::purge_stale_snap_data(const set<snapid_t>& snaps)
{
  dout(10) << __func__ << " " << snaps << dendl;

  if (!get_old_inodes())
    return;

  std::vector<snapid_t> to_remove;
  for (auto p : *get_old_inodes()) {
    const snapid_t &id = p.first;
    const auto &s = snaps.lower_bound(p.second.first);
    if (s == snaps.end() || *s > id) {
      dout(10) << " purging old_inode [" << p.second.first << "," << id << "]" << dendl;
      to_remove.push_back(id);
    }
  }

  if (to_remove.size() == get_old_inodes()->size()) {
    reset_old_inodes(old_inode_map_ptr());
  } else if (!to_remove.empty()) {
    auto _old_inodes = allocate_old_inode_map(*get_old_inodes());
    for (auto id : to_remove)
      _old_inodes->erase(id);
    reset_old_inodes(std::move(_old_inodes));
  }
}

/*
 * pick/create an old_inode
 */
snapid_t CInode::pick_old_inode(snapid_t snap) const
{
  if (is_any_old_inodes()) {
    auto it = old_inodes->lower_bound(snap);  // p is first key >= to snap
    if (it != old_inodes->end() && it->second.first <= snap) {
      dout(10) << __func__ << " snap " << snap << " -> [" << it->second.first << "," << it->first << "]" << dendl;
      return it->first;
    }
  }
  dout(10) << __func__ << " snap " << snap << " -> nothing" << dendl;
  return 0;
}

void CInode::open_snaprealm(bool nosplit)
{
  if (!snaprealm) {
    SnapRealm *parent = find_snaprealm();
    snaprealm = new SnapRealm(mdcache, this);
    if (parent) {
      dout(10) << __func__ << " " << snaprealm
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
    dout(15) << __func__ << " " << *snaprealm << dendl;
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

SnapRealm *CInode::find_snaprealm() const
{
  const CInode *cur = this;
  while (!cur->snaprealm) {
    const CDentry *pdn = cur->get_oldest_parent_dn();
    if (!pdn)
      break;
    cur = pdn->get_dir()->get_inode();
  }
  return cur->snaprealm;
}

void CInode::encode_snap_blob(bufferlist &snapbl)
{
  if (snaprealm) {
    using ceph::encode;
    encode(snaprealm->srnode, snapbl);
    dout(20) << __func__ << " " << *snaprealm << dendl;
  }
}
void CInode::decode_snap_blob(const bufferlist& snapbl)
{
  using ceph::decode;
  if (snapbl.length()) {
    open_snaprealm();
    auto old_flags = snaprealm->srnode.flags;
    auto p = snapbl.cbegin();
    decode(snaprealm->srnode, p);
    if (is_base()) {
      bool ok = snaprealm->_open_parents(NULL);
      ceph_assert(ok);
    } else {
      if ((snaprealm->srnode.flags ^ old_flags) & sr_t::PARENT_GLOBAL) {
	snaprealm->close_parents();
	snaprealm->adjust_parent();
      }
    }
    dout(20) << __func__ << " " << *snaprealm << dendl;
  } else if (snaprealm &&
	     !is_root() && !is_mdsdir()) { // see https://tracker.ceph.com/issues/42675
    ceph_assert(mdcache->mds->is_any_replay());
    snaprealm->merge_to(NULL);
  }
}

void CInode::encode_snap(bufferlist& bl)
{
  ENCODE_START(1, 1, bl);
  bufferlist snapbl;
  encode_snap_blob(snapbl);
  encode(snapbl, bl);
  encode(oldest_snap, bl);
  ENCODE_FINISH(bl);
}

void CInode::decode_snap(bufferlist::const_iterator& p)
{
  DECODE_START(1, p);
  bufferlist snapbl;
  decode(snapbl, p);
  decode(oldest_snap, p);
  decode_snap_blob(snapbl);
  DECODE_FINISH(p);
}

// =============================================

client_t CInode::calc_ideal_loner()
{
  if (mdcache->is_readonly())
    return -1;
  if (!get_mds_caps_wanted().empty())
    return -1;
  
  int n = 0;
  client_t loner = -1;
  for (const auto &p : client_caps) {
    if (!p.second.is_stale() &&
	(is_dir() ?
	 !has_subtree_or_exporting_dirfrag() :
	 (p.second.wanted() & (CEPH_CAP_ANY_WR|CEPH_CAP_FILE_RD)))) {
      if (n)
	return -1;
      n++;
      loner = p.first;
    }
  }
  return loner;
}

bool CInode::choose_ideal_loner()
{
  want_loner_cap = calc_ideal_loner();
  int changed = false;
  if (loner_cap >= 0 && loner_cap != want_loner_cap) {
    if (!try_drop_loner())
      return false;
    changed = true;
  }

  if (want_loner_cap >= 0) {
    if (loner_cap < 0) {
      set_loner_cap(want_loner_cap);
      changed = true;
    } else
      ceph_assert(loner_cap == want_loner_cap);
  }
  return changed;
}

bool CInode::try_set_loner()
{
  ceph_assert(want_loner_cap >= 0);
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
      if (issued & (CEPH_CAP_GEXCL | CEPH_CAP_GBUFFER))
	lock->set_state(LOCK_EXCL);
      else if (issued & CEPH_CAP_GWR) {
        if (issued & (CEPH_CAP_GCACHE | CEPH_CAP_GSHARED))
          lock->set_state(LOCK_EXCL);
        else
          lock->set_state(LOCK_MIX);
      } else if (lock->is_dirty()) {
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
      ceph_assert(lock->get_state() == LOCK_LOCK);
  }
}
 
void CInode::choose_lock_states(int dirty_caps)
{
  int issued = get_caps_issued() | dirty_caps;
  if (is_auth() && (issued & (CEPH_CAP_ANY_EXCL|CEPH_CAP_ANY_WR)))
    choose_ideal_loner();
  choose_lock_state(&filelock, issued);
  choose_lock_state(&nestlock, issued);
  choose_lock_state(&dirfragtreelock, issued);
  choose_lock_state(&authlock, issued);
  choose_lock_state(&xattrlock, issued);
  choose_lock_state(&linklock, issued);
}

int CInode::count_nonstale_caps()
{
  int n = 0;
  for (const auto &p : client_caps) {
    if (!p.second.is_stale())
      n++;
  }
  return n;
}

bool CInode::multiple_nonstale_caps()
{
  int n = 0;
  for (const auto &p : client_caps) {
    if (!p.second.is_stale()) {
      if (n)
	return true;
      n++;
    }
  }
  return false;
}

void CInode::set_mds_caps_wanted(mempool::mds_co::compact_map<int32_t,int32_t>& m)
{
  bool old_empty = mds_caps_wanted.empty();
  mds_caps_wanted.swap(m);
  if (old_empty != (bool)mds_caps_wanted.empty()) {
    if (old_empty)
      adjust_num_caps_wanted(1);
    else
      adjust_num_caps_wanted(-1);
  }
}

void CInode::set_mds_caps_wanted(mds_rank_t mds, int32_t wanted)
{
  bool old_empty = mds_caps_wanted.empty();
  if (wanted) {
    mds_caps_wanted[mds] = wanted;
    if (old_empty)
      adjust_num_caps_wanted(1);
  } else if (!old_empty) {
    mds_caps_wanted.erase(mds);
    if (mds_caps_wanted.empty())
      adjust_num_caps_wanted(-1);
  }
}

void CInode::adjust_num_caps_wanted(int d)
{
  if (!num_caps_wanted && d > 0)
    mdcache->open_file_table.add_inode(this);
  else if (num_caps_wanted > 0 && num_caps_wanted == -d)
    mdcache->open_file_table.remove_inode(this);

  num_caps_wanted +=d;
  ceph_assert(num_caps_wanted >= 0);
}

Capability *CInode::add_client_cap(client_t client, Session *session,
				   SnapRealm *conrealm, bool new_inode)
{
  ceph_assert(last == CEPH_NOSNAP);
  if (client_caps.empty()) {
    get(PIN_CAPS);
    if (conrealm)
      containing_realm = conrealm;
    else
      containing_realm = find_snaprealm();
    containing_realm->inodes_with_caps.push_back(&item_caps);
    dout(10) << __func__ << " first cap, joining realm " << *containing_realm << dendl;

    mdcache->num_inodes_with_caps++;
    if (parent)
      parent->dir->adjust_num_inodes_with_caps(1);
  }

  uint64_t cap_id = new_inode ? 1 : ++mdcache->last_cap_id;
  auto ret = client_caps.emplace(std::piecewise_construct, std::forward_as_tuple(client),
                                 std::forward_as_tuple(this, session, cap_id));
  ceph_assert(ret.second == true);
  Capability *cap = &ret.first->second;

  cap->client_follows = first-1;
  containing_realm->add_cap(client, cap);

  return cap;
}

void CInode::remove_client_cap(client_t client)
{
  auto it = client_caps.find(client);
  ceph_assert(it != client_caps.end());
  Capability *cap = &it->second;
  
  cap->item_session_caps.remove_myself();
  cap->item_revoking_caps.remove_myself();
  cap->item_client_revoking_caps.remove_myself();
  containing_realm->remove_cap(client, cap);
  
  if (client == loner_cap)
    loner_cap = -1;

  if (cap->wanted())
    adjust_num_caps_wanted(-1);

  client_caps.erase(it);
  if (client_caps.empty()) {
    dout(10) << __func__ << " last cap, leaving realm " << *containing_realm << dendl;
    put(PIN_CAPS);
    item_caps.remove_myself();
    containing_realm = NULL;
    mdcache->num_inodes_with_caps--;
    if (parent)
      parent->dir->adjust_num_inodes_with_caps(-1);
  }

  //clean up advisory locks
  bool fcntl_removed = fcntl_locks ? fcntl_locks->remove_all_from(client) : false;
  bool flock_removed = flock_locks ? flock_locks->remove_all_from(client) : false; 
  if (fcntl_removed || flock_removed) {
    MDSContext::vec waiters;
    take_waiting(CInode::WAIT_FLOCK, waiters);
    mdcache->mds->queue_waiters(waiters);
  }
}

void CInode::move_to_realm(SnapRealm *realm)
{
  dout(10) << __func__ << " joining realm " << *realm
	   << ", leaving realm " << *containing_realm << dendl;
  for (auto& p : client_caps) {
    containing_realm->remove_cap(p.first, &p.second);
    realm->add_cap(p.first, &p.second);
  }
  item_caps.remove_myself();
  realm->inodes_with_caps.push_back(&item_caps);
  containing_realm = realm;
}

Capability *CInode::reconnect_cap(client_t client, const cap_reconnect_t& icr, Session *session)
{
  Capability *cap = get_client_cap(client);
  if (cap) {
    // FIXME?
    cap->merge(icr.capinfo.wanted, icr.capinfo.issued);
  } else {
    cap = add_client_cap(client, session);
    cap->set_cap_id(icr.capinfo.cap_id);
    cap->set_wanted(icr.capinfo.wanted);
    cap->issue_norevoke(icr.capinfo.issued);
    cap->reset_seq();
  }
  cap->set_last_issue_stamp(ceph_clock_now());
  return cap;
}

void CInode::clear_client_caps_after_export()
{
  while (!client_caps.empty())
    remove_client_cap(client_caps.begin()->first);
  loner_cap = -1;
  want_loner_cap = -1;
  if (!get_mds_caps_wanted().empty()) {
    mempool::mds_co::compact_map<int32_t,int32_t> empty;
    set_mds_caps_wanted(empty);
  }
}

void CInode::export_client_caps(map<client_t,Capability::Export>& cl)
{
  for (const auto &p : client_caps) {
    cl[p.first] = p.second.make_export();
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

int CInode::get_caps_allowed_for_client(Session *session, Capability *cap,
					const mempool_inode *file_i) const
{
  client_t client = session->get_client();
  int allowed;
  if (client == get_loner()) {
    // as the loner, we get the loner_caps AND any xlocker_caps for things we have xlocked
    allowed =
      get_caps_allowed_by_type(CAP_LONER) |
      (get_caps_allowed_by_type(CAP_XLOCKER) & get_xlocker_mask(client));
  } else {
    allowed = get_caps_allowed_by_type(CAP_ANY);
  }

  if (is_dir()) {
    allowed &= ~CEPH_CAP_ANY_DIR_OPS;
    if (cap && (allowed & CEPH_CAP_FILE_EXCL))
      allowed |= cap->get_lock_cache_allowed();
  } else {
    if (file_i->inline_data.version == CEPH_INLINE_NONE &&
	file_i->layout.pool_ns.empty()) {
      // noop
    } else if (cap) {
      if ((file_i->inline_data.version != CEPH_INLINE_NONE &&
	   cap->is_noinline()) ||
	  (!file_i->layout.pool_ns.empty() &&
	   cap->is_nopoolns()))
	allowed &= ~(CEPH_CAP_FILE_RD | CEPH_CAP_FILE_WR);
    } else {
      auto& conn = session->get_connection();
      if ((file_i->inline_data.version != CEPH_INLINE_NONE &&
	   !conn->has_feature(CEPH_FEATURE_MDS_INLINE_DATA)) ||
	  (!file_i->layout.pool_ns.empty() &&
	   !conn->has_feature(CEPH_FEATURE_FS_FILE_LAYOUT_V2)))
	allowed &= ~(CEPH_CAP_FILE_RD | CEPH_CAP_FILE_WR);
    }
  }
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

  for (const auto &p : client_caps) {
    int i = p.second.issued();
    c |= i;
    if (p.first == loner_cap)
      loner |= i;
    else
      other |= i;
    xlocker |= get_xlocker_mask(p.first) & i;
  }
  if (ploner) *ploner = (loner >> shift) & mask;
  if (pother) *pother = (other >> shift) & mask;
  if (pxlocker) *pxlocker = (xlocker >> shift) & mask;
  return (c >> shift) & mask;
}

bool CInode::is_any_caps_wanted() const
{
  for (const auto &p : client_caps) {
    if (p.second.wanted())
      return true;
  }
  return false;
}

int CInode::get_caps_wanted(int *ploner, int *pother, int shift, int mask) const
{
  int w = 0;
  int loner = 0, other = 0;
  for (const auto &p : client_caps) {
    if (!p.second.is_stale()) {
      int t = p.second.wanted();
      w |= t;
      if (p.first == loner_cap)
	loner |= t;
      else
	other |= t;	
    }
    //cout << " get_caps_wanted client " << it->first << " " << cap_string(it->second.wanted()) << endl;
  }
  if (is_auth())
    for (const auto &p : mds_caps_wanted) {
      w |= p.second;
      other |= p.second;
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


// =============================================

int CInode::encode_inodestat(bufferlist& bl, Session *session,
			     SnapRealm *dir_realm,
			     snapid_t snapid,
			     unsigned max_bytes,
			     int getattr_caps)
{
  client_t client = session->get_client();
  ceph_assert(snapid);
  
  bool valid = true;

  // pick a version!
  const mempool_inode *oi = get_inode().get();
  const mempool_inode *pi = get_projected_inode().get();

  const mempool_xattr_map *pxattrs = nullptr;

  if (snapid != CEPH_NOSNAP) {

    // for now at least, old_inodes is only defined/valid on the auth
    if (!is_auth())
      valid = false;

    if (is_any_old_inodes()) {
      auto it = old_inodes->lower_bound(snapid);
      if (it != old_inodes->end()) {
	if (it->second.first > snapid) {
	  if  (it != old_inodes->begin())
	    --it;
	}
	if (it->second.first <= snapid && snapid <= it->first) {
	  dout(15) << __func__ << " snapid " << snapid
		   << " to old_inode [" << it->second.first << "," << it->first << "]"
		   << " " << it->second.inode.rstat
		   << dendl;
	  pi = oi = &it->second.inode;
	  pxattrs = &it->second.xattrs;
	} else {
	  // snapshoted remote dentry can result this
	  dout(0) << __func__ << " old_inode for snapid " << snapid
		  << " not found" << dendl;
	}
      }
    } else if (snapid < first || snapid > last) {
      // snapshoted remote dentry can result this
      dout(0) << __func__ << " [" << first << "," << last << "]"
	      << " not match snapid " << snapid << dendl;
    }
  }

  utime_t snap_btime;
  SnapRealm *realm = find_snaprealm();
  if (snapid != CEPH_NOSNAP && realm) {
    // add snapshot timestamp vxattr
    map<snapid_t,const SnapInfo*> infomap;
    realm->get_snap_info(infomap,
                         snapid,  // min
                         snapid); // max
    if (!infomap.empty()) {
      ceph_assert(infomap.size() == 1);
      const SnapInfo *si = infomap.begin()->second;
      snap_btime = si->stamp;
    }
  }


  bool no_caps = !valid ||
		 session->is_stale() ||
		 (dir_realm && realm != dir_realm) ||
		 is_frozen() ||
		 state_test(CInode::STATE_EXPORTINGCAPS);
  if (no_caps)
    dout(20) << __func__ << " no caps"
	     << (!valid?", !valid":"")
	     << (session->is_stale()?", session stale ":"")
	     << ((dir_realm && realm != dir_realm)?", snaprealm differs ":"")
	     << (is_frozen()?", frozen inode":"")
	     << (state_test(CInode::STATE_EXPORTINGCAPS)?", exporting caps":"")
	     << dendl;

  
  // "fake" a version that is old (stable) version, +1 if projected.
  version_t version = (oi->version * 2) + is_projected();

  Capability *cap = get_client_cap(client);
  bool pfile = filelock.is_xlocked_by_client(client) || get_loner() == client;
  //(cap && (cap->issued() & CEPH_CAP_FILE_EXCL));
  bool pauth = authlock.is_xlocked_by_client(client) || get_loner() == client;
  bool plink = linklock.is_xlocked_by_client(client) || get_loner() == client;
  bool pxattr = xattrlock.is_xlocked_by_client(client) || get_loner() == client;

  bool plocal = versionlock.get_last_wrlock_client() == client;
  bool ppolicy = policylock.is_xlocked_by_client(client) || get_loner()==client;
  
  const mempool_inode *any_i = (pfile|pauth|plink|pxattr|plocal) ? pi : oi;
  
  dout(20) << " pfile " << pfile << " pauth " << pauth
	   << " plink " << plink << " pxattr " << pxattr
	   << " plocal " << plocal
	   << " ctime " << any_i->ctime
	   << " valid=" << valid << dendl;

  // file
  const mempool_inode *file_i = pfile ? pi:oi;
  file_layout_t layout;
  if (is_dir()) {
    layout = (ppolicy ? pi : oi)->layout;
  } else {
    layout = file_i->layout;
  }

  // max_size is min of projected, actual
  uint64_t max_size;
  {
    auto it = oi->client_ranges.find(client);
    if (it == oi->client_ranges.end()) {
      max_size = 0;
    } else {
      max_size = it->second.range.last;
      if (oi != pi) {
	it = pi->client_ranges.find(client);
	if (it == pi->client_ranges.end()) {
	  max_size = 0;
	} else {
	  max_size = std::min(max_size, it->second.range.last);
	}
      }
    }
  }

  // inline data
  version_t inline_version = 0;
  bufferlist inline_data;
  if (file_i->inline_data.version == CEPH_INLINE_NONE) {
    inline_version = CEPH_INLINE_NONE;
  } else if ((!cap && !no_caps) ||
	     (cap && cap->client_inline_version < file_i->inline_data.version) ||
	     (getattr_caps & CEPH_CAP_FILE_RD)) { // client requests inline data
    inline_version = file_i->inline_data.version;
    if (file_i->inline_data.length() > 0)
      file_i->inline_data.get_data(inline_data);
  }

  // nest (do same as file... :/)
  if (cap) {
    cap->last_rbytes = file_i->rstat.rbytes;
    cap->last_rsize = file_i->rstat.rsize();
  }

  // auth
  const mempool_inode *auth_i = pauth ? pi:oi;

  // link
  const mempool_inode *link_i = plink ? pi:oi;
  
  // xattr
  const mempool_inode *xattr_i = pxattr ? pi:oi;

  using ceph::encode;
  // xattr
  version_t xattr_version;
  if ((!cap && !no_caps) ||
      (cap && cap->client_xattr_version < xattr_i->xattr_version) ||
      (getattr_caps & CEPH_CAP_XATTR_SHARED)) { // client requests xattrs
    if (!pxattrs)
      pxattrs = pxattr ? get_projected_xattrs().get() : get_xattrs().get();
    xattr_version = xattr_i->xattr_version;
  } else {
    xattr_version = 0;
  }
  
  // do we have room?
  if (max_bytes) {
    unsigned bytes =
      8 + 8 + 4 + 8 + 8 + sizeof(ceph_mds_reply_cap) +
      sizeof(struct ceph_file_layout) +
      sizeof(struct ceph_timespec) * 3 + 4 + // ctime ~ time_warp_seq
      8 + 8 + 8 + 4 + 4 + 4 + 4 + 4 + // size ~ nlink
      8 + 8 + 8 + 8 + 8 + sizeof(struct ceph_timespec) + // dirstat.nfiles ~ rstat.rctime
      sizeof(__u32) + sizeof(__u32) * 2 * dirfragtree._splits.size() + // dirfragtree
      sizeof(__u32) + symlink.length() + // symlink
      sizeof(struct ceph_dir_layout); // dir_layout

    if (xattr_version) {
      bytes += sizeof(__u32) + sizeof(__u32); // xattr buffer len + number entries
      if (pxattrs) {
	for (const auto &p : *pxattrs)
	  bytes += sizeof(__u32) * 2 + p.first.length() + p.second.length();
      }
    } else {
      bytes += sizeof(__u32); // xattr buffer len
    }
    bytes +=
      sizeof(version_t) + sizeof(__u32) + inline_data.length() + // inline data
      1 + 1 + 8 + 8 + 4 + // quota
      4 + layout.pool_ns.size() + // pool ns
      sizeof(struct ceph_timespec) + 8; // btime + change_attr

    if (bytes > max_bytes)
      return -ENOSPC;
  }


  // encode caps
  struct ceph_mds_reply_cap ecap;
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
    ecap.caps = valid ? get_caps_allowed_by_type(CAP_ANY) : CEPH_STAT_CAP_INODE;
    if (last == CEPH_NOSNAP || is_any_caps())
      ecap.caps = ecap.caps & get_caps_allowed_for_client(session, nullptr, file_i);
    ecap.seq = 0;
    ecap.mseq = 0;
    ecap.realm = 0;
  } else {
    if (!no_caps && !cap) {
      // add a new cap
      cap = add_client_cap(client, session, realm);
      if (is_auth())
	choose_ideal_loner();
    }

    int issue = 0;
    if (!no_caps && cap) {
      int likes = get_caps_liked();
      int allowed = get_caps_allowed_for_client(session, cap, file_i);
      issue = (cap->wanted() | likes) & allowed;
      cap->issue_norevoke(issue, true);
      issue = cap->pending();
      dout(10) << "encode_inodestat issuing " << ccap_string(issue)
	       << " seq " << cap->get_last_seq() << dendl;
    } else if (cap && cap->is_new() && !dir_realm) {
      // alway issue new caps to client, otherwise the caps get lost
      ceph_assert(cap->is_stale());
      ceph_assert(!cap->pending());
      issue = CEPH_CAP_PIN;
      cap->issue_norevoke(issue, true);
      dout(10) << "encode_inodestat issuing " << ccap_string(issue)
	       << " seq " << cap->get_last_seq()
	       << "(stale&new caps)" << dendl;
    }

    if (issue) {
      cap->set_last_issue();
      cap->set_last_issue_stamp(ceph_clock_now());
      ecap.caps = issue;
      ecap.wanted = cap->wanted();
      ecap.cap_id = cap->get_cap_id();
      ecap.seq = cap->get_last_seq();
      ecap.mseq = cap->get_mseq();
      ecap.realm = realm->inode->ino();
    } else {
      ecap.cap_id = 0;
      ecap.caps = 0;
      ecap.seq = 0;
      ecap.mseq = 0;
      ecap.realm = 0;
      ecap.wanted = 0;
    }
  }
  ecap.flags = is_auth() ? CEPH_CAP_FLAG_AUTH : 0;
  dout(10) << "encode_inodestat caps " << ccap_string(ecap.caps)
	   << " seq " << ecap.seq << " mseq " << ecap.mseq
	   << " xattrv " << xattr_version << dendl;

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
  if (xattr_version && cap) {
    if ((cap->pending() | getattr_caps) & CEPH_CAP_XATTR_SHARED) {
      dout(10) << "including xattrs version " << xattr_version << dendl;
      cap->client_xattr_version = xattr_version;
    } else {
      dout(10) << "dropping xattrs version " << xattr_version << dendl;
      xattr_version = 0;
    }
  }

  // The end result of encode_xattrs() is equivalent to:
  // {
  //   bufferlist xbl;
  //   if (xattr_version) {
  //     if (pxattrs)
  //       encode(*pxattrs, bl);
  //     else
  //       encode((__u32)0, bl);
  //   }
  //   encode(xbl, bl);
  // }
  //
  // But encoding xattrs into the 'xbl' requires a memory allocation.
  // The 'bl' should have enough pre-allocated memory in most cases.
  // Encoding xattrs directly into it can avoid the extra allocation.
  auto encode_xattrs = [xattr_version, pxattrs, &bl]() {
    using ceph::encode;
    if (xattr_version) {
      ceph_le32 xbl_len;
      auto filler = bl.append_hole(sizeof(xbl_len));
      const auto starting_bl_len = bl.length();
      if (pxattrs)
	encode(*pxattrs, bl);
      else
	encode((__u32)0, bl);
      xbl_len = bl.length() - starting_bl_len;
      filler.copy_in(sizeof(xbl_len), (char *)&xbl_len);
    } else {
      encode((__u32)0, bl);
    }
  };

  /*
   * note: encoding matches MClientReply::InodeStat
   */
  if (session->info.has_feature(CEPHFS_FEATURE_REPLY_ENCODING)) {
    ENCODE_START(3, 1, bl);
    encode(oi->ino, bl);
    encode(snapid, bl);
    encode(oi->rdev, bl);
    encode(version, bl);
    encode(xattr_version, bl);
    encode(ecap, bl);
    {
      ceph_file_layout legacy_layout;
      layout.to_legacy(&legacy_layout);
      encode(legacy_layout, bl);
    }
    encode(any_i->ctime, bl);
    encode(file_i->mtime, bl);
    encode(file_i->atime, bl);
    encode(file_i->time_warp_seq, bl);
    encode(file_i->size, bl);
    encode(max_size, bl);
    encode(file_i->truncate_size, bl);
    encode(file_i->truncate_seq, bl);
    encode(auth_i->mode, bl);
    encode((uint32_t)auth_i->uid, bl);
    encode((uint32_t)auth_i->gid, bl);
    encode(link_i->nlink, bl);
    encode(file_i->dirstat.nfiles, bl);
    encode(file_i->dirstat.nsubdirs, bl);
    encode(file_i->rstat.rbytes, bl);
    encode(file_i->rstat.rfiles, bl);
    encode(file_i->rstat.rsubdirs, bl);
    encode(file_i->rstat.rctime, bl);
    dirfragtree.encode(bl);
    encode(symlink, bl);
    encode(file_i->dir_layout, bl);
    encode_xattrs();
    encode(inline_version, bl);
    encode(inline_data, bl);
    const mempool_inode *policy_i = ppolicy ? pi : oi;
    encode(policy_i->quota, bl);
    encode(layout.pool_ns, bl);
    encode(any_i->btime, bl);
    encode(any_i->change_attr, bl);
    encode(file_i->export_pin, bl);
    encode(snap_btime, bl);
    ENCODE_FINISH(bl);
  }
  else {
    ceph_assert(session->get_connection());

    encode(oi->ino, bl);
    encode(snapid, bl);
    encode(oi->rdev, bl);
    encode(version, bl);
    encode(xattr_version, bl);
    encode(ecap, bl);
    {
      ceph_file_layout legacy_layout;
      layout.to_legacy(&legacy_layout);
      encode(legacy_layout, bl);
    }
    encode(any_i->ctime, bl);
    encode(file_i->mtime, bl);
    encode(file_i->atime, bl);
    encode(file_i->time_warp_seq, bl);
    encode(file_i->size, bl);
    encode(max_size, bl);
    encode(file_i->truncate_size, bl);
    encode(file_i->truncate_seq, bl);
    encode(auth_i->mode, bl);
    encode((uint32_t)auth_i->uid, bl);
    encode((uint32_t)auth_i->gid, bl);
    encode(link_i->nlink, bl);
    encode(file_i->dirstat.nfiles, bl);
    encode(file_i->dirstat.nsubdirs, bl);
    encode(file_i->rstat.rbytes, bl);
    encode(file_i->rstat.rfiles, bl);
    encode(file_i->rstat.rsubdirs, bl);
    encode(file_i->rstat.rctime, bl);
    dirfragtree.encode(bl);
    encode(symlink, bl);
    auto& conn = session->get_connection();
    if (conn->has_feature(CEPH_FEATURE_DIRLAYOUTHASH)) {
      encode(file_i->dir_layout, bl);
    }
    encode_xattrs();
    if (conn->has_feature(CEPH_FEATURE_MDS_INLINE_DATA)) {
      encode(inline_version, bl);
      encode(inline_data, bl);
    }
    if (conn->has_feature(CEPH_FEATURE_MDS_QUOTA)) {
      const mempool_inode *policy_i = ppolicy ? pi : oi;
      encode(policy_i->quota, bl);
    }
    if (conn->has_feature(CEPH_FEATURE_FS_FILE_LAYOUT_V2)) {
      encode(layout.pool_ns, bl);
    }
    if (conn->has_feature(CEPH_FEATURE_FS_BTIME)) {
      encode(any_i->btime, bl);
      encode(any_i->change_attr, bl);
    }
  }

  return valid;
}

void CInode::encode_cap_message(const ref_t<MClientCaps> &m, Capability *cap)
{
  ceph_assert(cap);

  client_t client = cap->get_client();

  bool pfile = filelock.is_xlocked_by_client(client) || (cap->issued() & CEPH_CAP_FILE_EXCL);
  bool pauth = authlock.is_xlocked_by_client(client);
  bool plink = linklock.is_xlocked_by_client(client);
  bool pxattr = xattrlock.is_xlocked_by_client(client);
 
  const mempool_inode *oi = get_inode().get();
  const mempool_inode *pi = get_projected_inode().get();
  const mempool_inode *i = (pfile|pauth|plink|pxattr) ? pi : oi;

  dout(20) << __func__ << " pfile " << pfile
	   << " pauth " << pauth << " plink " << plink << " pxattr " << pxattr
	   << " ctime " << i->ctime << dendl;

  i = pfile ? pi:oi;
  m->set_layout(i->layout);
  m->size = i->size;
  m->truncate_seq = i->truncate_seq;
  m->truncate_size = i->truncate_size;
  m->mtime = i->mtime;
  m->atime = i->atime;
  m->ctime = i->ctime;
  m->change_attr = i->change_attr;
  m->time_warp_seq = i->time_warp_seq;
  m->nfiles = i->dirstat.nfiles;
  m->nsubdirs = i->dirstat.nsubdirs;

  if (cap->client_inline_version < i->inline_data.version) {
    m->inline_version = cap->client_inline_version = i->inline_data.version;
    if (i->inline_data.length() > 0)
      i->inline_data.get_data(m->inline_data);
  } else {
    m->inline_version = 0;
  }

  // max_size is min of projected, actual.
  {
    uint64_t max_size;
    auto it = oi->client_ranges.find(client);
    if (it == oi->client_ranges.end()) {
      max_size = 0;
    } else {
      max_size = it->second.range.last;
      if (oi != pi) {
	it = pi->client_ranges.find(client);
	if (it == pi->client_ranges.end()) {
	  max_size = 0;
	} else {
	  max_size = std::min(max_size, it->second.range.last);
	}
      }
    }
    m->max_size = max_size;
  }

  i = pauth ? pi:oi;
  m->head.mode = i->mode;
  m->head.uid = i->uid;
  m->head.gid = i->gid;

  i = plink ? pi:oi;
  m->head.nlink = i->nlink;

  using ceph::encode;
  i = pxattr ? pi:oi;
  const auto& ix = pxattr ? get_projected_xattrs() : get_xattrs();
  if ((cap->pending() & CEPH_CAP_XATTR_SHARED) &&
      i->xattr_version > cap->client_xattr_version) {
    dout(10) << "    including xattrs v " << i->xattr_version << dendl;
    if (ix)
      encode(*ix, m->xattrbl);
    else
      encode((__u32)0, m->xattrbl);
    m->head.xattr_version = i->xattr_version;
    cap->client_xattr_version = i->xattr_version;
  }
}



void CInode::_encode_base(bufferlist& bl, uint64_t features)
{
  ENCODE_START(1, 1, bl);
  encode(first, bl);
  encode(*get_inode(), bl, features);
  encode(symlink, bl);
  encode(dirfragtree, bl);
  encode_xattrs(bl);
  encode_old_inodes(bl, features);
  encode(damage_flags, bl);
  encode_snap(bl);
  ENCODE_FINISH(bl);
}
void CInode::_decode_base(bufferlist::const_iterator& p)
{
  DECODE_START(1, p);
  decode(first, p);
  {
    auto _inode = allocate_inode();
    decode(*_inode, p);
    reset_inode(std::move(_inode));
  }
  {
    std::string tmp;
    decode(tmp, p);
    symlink = std::string_view(tmp);
  }
  decode(dirfragtree, p);
  decode_xattrs(p);
  decode_old_inodes(p);
  decode(damage_flags, p);
  decode_snap(p);
  DECODE_FINISH(p);
}

void CInode::_encode_locks_full(bufferlist& bl)
{
  using ceph::encode;
  encode(authlock, bl);
  encode(linklock, bl);
  encode(dirfragtreelock, bl);
  encode(filelock, bl);
  encode(xattrlock, bl);
  encode(snaplock, bl);
  encode(nestlock, bl);
  encode(flocklock, bl);
  encode(policylock, bl);

  encode(loner_cap, bl);
}
void CInode::_decode_locks_full(bufferlist::const_iterator& p)
{
  using ceph::decode;
  decode(authlock, p);
  decode(linklock, p);
  decode(dirfragtreelock, p);
  decode(filelock, p);
  decode(xattrlock, p);
  decode(snaplock, p);
  decode(nestlock, p);
  decode(flocklock, p);
  decode(policylock, p);

  decode(loner_cap, p);
  set_loner_cap(loner_cap);
  want_loner_cap = loner_cap;  // for now, we'll eval() shortly.
}

void CInode::_encode_locks_state_for_replica(bufferlist& bl, bool need_recover)
{
  ENCODE_START(1, 1, bl);
  authlock.encode_state_for_replica(bl);
  linklock.encode_state_for_replica(bl);
  dirfragtreelock.encode_state_for_replica(bl);
  filelock.encode_state_for_replica(bl);
  nestlock.encode_state_for_replica(bl);
  xattrlock.encode_state_for_replica(bl);
  snaplock.encode_state_for_replica(bl);
  flocklock.encode_state_for_replica(bl);
  policylock.encode_state_for_replica(bl);
  encode(need_recover, bl);
  ENCODE_FINISH(bl);
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

void CInode::_decode_locks_state_for_replica(bufferlist::const_iterator& p, bool is_new)
{
  DECODE_START(1, p);
  authlock.decode_state(p, is_new);
  linklock.decode_state(p, is_new);
  dirfragtreelock.decode_state(p, is_new);
  filelock.decode_state(p, is_new);
  nestlock.decode_state(p, is_new);
  xattrlock.decode_state(p, is_new);
  snaplock.decode_state(p, is_new);
  flocklock.decode_state(p, is_new);
  policylock.decode_state(p, is_new);

  bool need_recover;
  decode(need_recover, p);
  if (need_recover && is_new) {
    // Auth mds replicated this inode while it's recovering. Auth mds may take xlock on the lock
    // and change the object when replaying unsafe requests.
    authlock.mark_need_recover();
    linklock.mark_need_recover();
    dirfragtreelock.mark_need_recover();
    filelock.mark_need_recover();
    nestlock.mark_need_recover();
    xattrlock.mark_need_recover();
    snaplock.mark_need_recover();
    flocklock.mark_need_recover();
    policylock.mark_need_recover();
  }
  DECODE_FINISH(p);
}
void CInode::_decode_locks_rejoin(bufferlist::const_iterator& p, MDSContext::vec& waiters,
				  list<SimpleLock*>& eval_locks, bool survivor)
{
  authlock.decode_state_rejoin(p, waiters, survivor);
  linklock.decode_state_rejoin(p, waiters, survivor);
  dirfragtreelock.decode_state_rejoin(p, waiters, survivor);
  filelock.decode_state_rejoin(p, waiters, survivor);
  nestlock.decode_state_rejoin(p, waiters, survivor);
  xattrlock.decode_state_rejoin(p, waiters, survivor);
  snaplock.decode_state_rejoin(p, waiters, survivor);
  flocklock.decode_state_rejoin(p, waiters, survivor);
  policylock.decode_state_rejoin(p, waiters, survivor);

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
  ENCODE_START(5, 4, bl);
  _encode_base(bl, mdcache->mds->mdsmap->get_up_features());

  encode(state, bl);

  encode(pop, bl);

  encode(get_replicas(), bl);

  // include scatterlock info for any bounding CDirs
  bufferlist bounding;
  if (get_inode()->is_dir())
    for (const auto &p : dirfrags) {
      CDir *dir = p.second;
      if (dir->state_test(CDir::STATE_EXPORTBOUND)) {
	encode(p.first, bounding);
	encode(dir->fnode.fragstat, bounding);
	encode(dir->fnode.accounted_fragstat, bounding);
	encode(dir->fnode.rstat, bounding);
	encode(dir->fnode.accounted_rstat, bounding);
	dout(10) << " encoded fragstat/rstat info for " << *dir << dendl;
      }
    }
  encode(bounding, bl);

  _encode_locks_full(bl);

  _encode_file_locks(bl);

  ENCODE_FINISH(bl);

  get(PIN_TEMPEXPORTING);
}

void CInode::finish_export()
{
  state &= MASK_STATE_EXPORT_KEPT;

  pop.zero();

  // just in case!
  //dirlock.clear_updated();

  loner_cap = -1;

  put(PIN_TEMPEXPORTING);
}

void CInode::decode_import(bufferlist::const_iterator& p,
			   LogSegment *ls)
{
  DECODE_START(5, p);

  _decode_base(p);

  {
    unsigned s;
    decode(s, p);
    s &= MASK_STATE_EXPORTED;

    if (s & STATE_RANDEPHEMERALPIN) {
      set_ephemeral_rand(true);
    }
    if (s & STATE_DISTEPHEMERALPIN) {
      set_ephemeral_dist(true);
    }

    state_set(STATE_AUTH | s);
  }

  if (is_dirty()) {
    get(PIN_DIRTY);
    _mark_dirty(ls);
  }
  if (is_dirty_parent()) {
    get(PIN_DIRTYPARENT);
    mark_dirty_parent(ls);
  }

  decode(pop, p);

  decode(get_replicas(), p);
  if (is_replicated())
    get(PIN_REPLICATED);
  replica_nonce = 0;

  // decode fragstat info on bounding cdirs
  bufferlist bounding;
  decode(bounding, p);
  auto q = bounding.cbegin();
  while (!q.end()) {
    frag_t fg;
    decode(fg, q);
    CDir *dir = get_dirfrag(fg);
    ceph_assert(dir);  // we should have all bounds open

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
      decode(f, q);
      decode(f, q);
    } else {
      decode(dir->fnode.fragstat, q);
      decode(dir->fnode.accounted_fragstat, q);
      dout(10) << " took fragstat info for " << *dir << dendl;
    }
    if (dir->is_auth() ||
        nestlock.get_state() == LOCK_MIX) {
      dout(10) << " skipped rstat info for " << *dir << dendl;
      nest_info_t n;
      decode(n, q);
      decode(n, q);
    } else {
      decode(dir->fnode.rstat, q);
      decode(dir->fnode.accounted_rstat, q);
      dout(10) << " took rstat info for " << *dir << dendl;
    }
  }

  _decode_locks_full(p);

  _decode_file_locks(p);

  DECODE_FINISH(p);
}


void InodeStoreBase::dump(Formatter *f) const
{
  inode->dump(f);
  f->dump_string("symlink", symlink);

  f->open_array_section("xattrs");
  if (xattrs) {
    for (const auto& [key, val] : *xattrs) {
      f->open_object_section("xattr");
      f->dump_string("key", key);
      std::string v(val.c_str(), val.length());
      f->dump_string("val", v);
      f->close_section();
    }
  }
  f->close_section();
  f->open_object_section("dirfragtree");
  dirfragtree.dump(f);
  f->close_section(); // dirfragtree
  
  f->open_array_section("old_inodes");
  if (old_inodes) {
    for (const auto &p : *old_inodes) {
      f->open_object_section("old_inode");
      // The key is the last snapid, the first is in the mempool_old_inode
      f->dump_int("last", p.first);
      p.second.dump(f);
      f->close_section();  // old_inode
    }
  }
  f->close_section();  // old_inodes

  f->dump_unsigned("oldest_snap", oldest_snap);
  f->dump_unsigned("damage_flags", damage_flags);
}

template <>
void decode_json_obj(mempool::mds_co::string& t, JSONObj *obj){

  t = mempool::mds_co::string(std::string_view(obj->get_data()));
}

void InodeStoreBase::decode_json(JSONObj *obj)
{
  {
    auto _inode = allocate_inode();
    _inode->decode_json(obj);
    reset_inode(std::move(_inode));
  }

  JSONDecoder::decode_json("symlink", symlink, obj, true);
  // JSONDecoder::decode_json("dirfragtree", dirfragtree, obj, true); // cann't decode it now
  //
  //
  {
    mempool_xattr_map tmp;
    JSONDecoder::decode_json("xattrs", tmp, xattrs_cb, obj, true);
    if (tmp.empty())
      reset_xattrs(xattr_map_ptr());
    else
      reset_xattrs(allocate_xattr_map(std::move(tmp)));
  }
  // JSONDecoder::decode_json("old_inodes", old_inodes, InodeStoreBase::old_indoes_cb, obj, true); // cann't decode old_inodes now
  JSONDecoder::decode_json("oldest_snap", oldest_snap.val, obj, true);
  JSONDecoder::decode_json("damage_flags", damage_flags, obj, true);
  //sr_t srnode;
  //JSONDecoder::decode_json("snap_blob", srnode, obj, true);   // cann't decode it now
  //snap_blob = srnode;
}

void InodeStoreBase::xattrs_cb(InodeStoreBase::mempool_xattr_map& c, JSONObj *obj){

  string k;
  JSONDecoder::decode_json("key", k, obj, true);
  string v;
  JSONDecoder::decode_json("val", v, obj, true);
  c[k.c_str()] = buffer::copy(v.c_str(), v.size());
}

void InodeStoreBase::old_indoes_cb(InodeStoreBase::mempool_old_inode_map& c, JSONObj *obj){

  snapid_t s;
  JSONDecoder::decode_json("last", s.val, obj, true);
  InodeStoreBase::mempool_old_inode i;
  // i.decode_json(obj); // cann't decode now, simon
  c[s] = i;
}

void InodeStore::generate_test_instances(std::list<InodeStore*> &ls)
{
  InodeStore *populated = new InodeStore;
  populated->get_inode()->ino = 0xdeadbeef;
  populated->symlink = "rhubarb";
  ls.push_back(populated);
}

void InodeStoreBare::generate_test_instances(std::list<InodeStoreBare*> &ls)
{
  InodeStoreBare *populated = new InodeStoreBare;
  populated->get_inode()->ino = 0xdeadbeef;
  populated->symlink = "rhubarb";
  ls.push_back(populated);
}

void CInode::validate_disk_state(CInode::validated_data *results,
                                 MDSContext *fin)
{
  class ValidationContinuation : public MDSContinuation {
  public:
    MDSContext *fin;
    CInode *in;
    CInode::validated_data *results;
    bufferlist bl;
    CInode *shadow_in;

    enum {
      START = 0,
      BACKTRACE,
      INODE,
      DIRFRAGS,
      SNAPREALM,
    };

    ValidationContinuation(CInode *i,
                           CInode::validated_data *data_r,
                           MDSContext *fin_) :
                             MDSContinuation(i->mdcache->mds->server),
                             fin(fin_),
                             in(i),
                             results(data_r),
                             shadow_in(NULL) {
      set_callback(START, static_cast<Continuation::stagePtr>(&ValidationContinuation::_start));
      set_callback(BACKTRACE, static_cast<Continuation::stagePtr>(&ValidationContinuation::_backtrace));
      set_callback(INODE, static_cast<Continuation::stagePtr>(&ValidationContinuation::_inode_disk));
      set_callback(DIRFRAGS, static_cast<Continuation::stagePtr>(&ValidationContinuation::_dirfrags));
      set_callback(SNAPREALM, static_cast<Continuation::stagePtr>(&ValidationContinuation::_snaprealm));
    }

    ~ValidationContinuation() override {
      if (shadow_in) {
	delete shadow_in;
	in->mdcache->num_shadow_inodes--;
      }
    }

    /**
     * Fetch backtrace and set tag if tag is non-empty
     */
    void fetch_backtrace_and_tag(CInode *in,
                                 std::string_view tag, bool is_internal,
                                 Context *fin, int *bt_r, bufferlist *bt)
    {
      const int64_t pool = in->get_backtrace_pool();
      object_t oid = CInode::get_object_name(in->ino(), frag_t(), "");

      ObjectOperation fetch;
      fetch.getxattr("parent", bt, bt_r);
      in->mdcache->mds->objecter->read(oid, object_locator_t(pool), fetch, CEPH_NOSNAP,
				       NULL, 0, fin);
      using ceph::encode;
      if (!is_internal) {
        ObjectOperation scrub_tag;
        bufferlist tag_bl;
        encode(tag, tag_bl);
        scrub_tag.setxattr("scrub_tag", tag_bl);
        SnapContext snapc;
        in->mdcache->mds->objecter->mutate(oid, object_locator_t(pool), scrub_tag, snapc,
					   ceph::real_clock::now(),
					   0, NULL);
      }
    }

    bool _start(int rval) {
      if (in->is_dirty()) {
	MDCache *mdcache = in->mdcache;  // For the benefit of dout
	auto ino = [this]() { return in->ino(); }; // For the benefit of dout
	dout(20) << "validating a dirty CInode; results will be inconclusive"
	  << dendl;
      }
      if (in->is_symlink()) {
	// there's nothing to do for symlinks!
	return true;
      }

      // prefetch snaprealm's past parents
      if (in->snaprealm && !in->snaprealm->have_past_parents_open())
	in->snaprealm->open_parents(nullptr);

      C_OnFinisher *conf = new C_OnFinisher(get_io_callback(BACKTRACE),
					    in->mdcache->mds->finisher);

      std::string_view tag = in->scrub_infop->header->get_tag();
      bool is_internal = in->scrub_infop->header->is_internal_tag();
      // Rather than using the usual CInode::fetch_backtrace,
      // use a special variant that optionally writes a tag in the same
      // operation.
      fetch_backtrace_and_tag(in, tag, is_internal, conf, &results->backtrace.ondisk_read_retval, &bl);
      return false;
    }

    bool _backtrace(int rval) {
      // set up basic result reporting and make sure we got the data
      results->performed_validation = true; // at least, some of it!
      results->backtrace.checked = true;

      const int64_t pool = in->get_backtrace_pool();
      inode_backtrace_t& memory_backtrace = results->backtrace.memory_value;
      in->build_backtrace(pool, memory_backtrace);
      bool equivalent, divergent;
      int memory_newer;

      MDCache *mdcache = in->mdcache;  // For the benefit of dout
      auto ino = [this]() { return in->ino(); }; // For the benefit of dout

      // Ignore rval because it's the result of a FAILOK operation
      // from fetch_backtrace_and_tag: the real result is in
      // backtrace.ondisk_read_retval
      dout(20) << "ondisk_read_retval: " << results->backtrace.ondisk_read_retval << dendl;
      if (results->backtrace.ondisk_read_retval != 0) {
        results->backtrace.error_str << "failed to read off disk; see retval";
        // we probably have a new unwritten file!
        // so skip the backtrace scrub for this entry and say that all's well
        if (in->is_dirty_parent())
          results->backtrace.passed = true;
        goto next;
      }

      // extract the backtrace, and compare it to a newly-constructed one
      try {
        auto p = bl.cbegin();
	using ceph::decode;
        decode(results->backtrace.ondisk_value, p);
        dout(10) << "decoded " << bl.length() << " bytes of backtrace successfully" << dendl;
      } catch (buffer::error&) {
        if (results->backtrace.ondisk_read_retval == 0 && rval != 0) {
          // Cases where something has clearly gone wrong with the overall
          // fetch op, though we didn't get a nonzero rc from the getxattr
          // operation.  e.g. object missing.
          results->backtrace.ondisk_read_retval = rval;
        }
        results->backtrace.error_str << "failed to decode on-disk backtrace ("
                                     << bl.length() << " bytes)!";
        // we probably have a new unwritten file!
        // so skip the backtrace scrub for this entry and say that all's well
        if (in->is_dirty_parent())
          results->backtrace.passed = true;

	goto next;
      }

      memory_newer = memory_backtrace.compare(results->backtrace.ondisk_value,
					      &equivalent, &divergent);

      if (divergent || memory_newer < 0) {
        // we're divergent, or on-disk version is newer
        results->backtrace.error_str << "On-disk backtrace is divergent or newer";
        // we probably have a new unwritten file!
        // so skip the backtrace scrub for this entry and say that all's well
        if (divergent && in->is_dirty_parent())
          results->backtrace.passed = true;
      } else {
        results->backtrace.passed = true;
      }
next:

      if (!results->backtrace.passed && in->scrub_infop->header->get_repair()) {
        std::string path;
        in->make_path_string(path);
        in->mdcache->mds->clog->warn() << "bad backtrace on inode " << in->ino()
                                       << "(" << path << "), rewriting it";
        in->mark_dirty_parent(in->mdcache->mds->mdlog->get_current_segment(),
                           false);
        // Flag that we repaired this BT so that it won't go into damagetable
        results->backtrace.repaired = true;
      }

      // If the inode's number was free in the InoTable, fix that
      // (#15619)
      {
        InoTable *inotable = mdcache->mds->inotable;

        dout(10) << "scrub: inotable ino = " << in->ino() << dendl;
        dout(10) << "scrub: inotable free says "
          << inotable->is_marked_free(in->ino()) << dendl;

        if (inotable->is_marked_free(in->ino())) {
          LogChannelRef clog = in->mdcache->mds->clog;
          clog->error() << "scrub: inode wrongly marked free: " << in->ino();

          if (in->scrub_infop->header->get_repair()) {
            bool repaired = inotable->repair(in->ino());
            if (repaired) {
              clog->error() << "inode table repaired for inode: " << in->ino();

              inotable->save();
            } else {
              clog->error() << "Cannot repair inotable while other operations"
                " are in progress";
            }
          }
        }
      }


      if (in->is_dir()) {
	return validate_directory_data();
      } else {
	// TODO: validate on-disk inode for normal files
	return check_inode_snaprealm();
      }
    }

    bool validate_directory_data() {
      ceph_assert(in->is_dir());

      if (in->is_base()) {
	if (!shadow_in) {
	  shadow_in = new CInode(in->mdcache);
	  in->mdcache->create_unlinked_system_inode(shadow_in, in->ino(), in->get_inode()->mode);
	  in->mdcache->num_shadow_inodes++;
	}
        shadow_in->fetch(get_internal_callback(INODE));
        return false;
      } else {
	// TODO: validate on-disk inode for non-base directories
	results->inode.passed = true;
	return check_dirfrag_rstats();
      }
    }

    bool _inode_disk(int rval) {
      const auto& si = shadow_in->get_inode();
      const auto& i = in->get_inode();

      results->inode.checked = true;
      results->inode.ondisk_read_retval = rval;
      results->inode.ondisk_value = *si;
      results->inode.memory_value = *i;

      if (si->version > i->version) {
        // uh, what?
        results->inode.error_str << "On-disk inode is newer than in-memory one; ";
	goto next;
      } else {
        bool divergent = false;
        int r = i->compare(*si, &divergent);
        results->inode.passed = !divergent && r >= 0;
        if (!results->inode.passed) {
          results->inode.error_str <<
              "On-disk inode is divergent or newer than in-memory one; ";
	  goto next;
        }
      }
next:
      return check_dirfrag_rstats();
    }

    bool check_dirfrag_rstats() {
      MDSGatherBuilder gather(g_ceph_context);
      frag_vec_t leaves;
      in->dirfragtree.get_leaves(leaves);
      for (const auto& leaf : leaves) {
        CDir *dir = in->get_or_open_dirfrag(in->mdcache, leaf);
	dir->scrub_info();
	if (!dir->scrub_infop->header)
	  dir->scrub_infop->header = in->scrub_infop->header;
        if (dir->is_complete()) {
	  dir->scrub_local();
	} else {
	  dir->scrub_infop->need_scrub_local = true;
	  dir->fetch(gather.new_sub(), false);
	}
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
      int frags_errors = 0;
      // basic reporting setup
      results->raw_stats.checked = true;
      results->raw_stats.ondisk_read_retval = rval;

      results->raw_stats.memory_value.dirstat = in->get_inode()->dirstat;
      results->raw_stats.memory_value.rstat = in->get_inode()->rstat;
      frag_info_t& dir_info = results->raw_stats.ondisk_value.dirstat;
      nest_info_t& nest_info = results->raw_stats.ondisk_value.rstat;

      if (rval != 0) {
        results->raw_stats.error_str << "Failed to read dirfrags off disk";
	goto next;
      }

      // check each dirfrag...
      for (const auto &p : in->dirfrags) {
	CDir *dir = p.second;
	ceph_assert(dir->get_version() > 0);
	nest_info.add(dir->fnode.accounted_rstat);
	dir_info.add(dir->fnode.accounted_fragstat);
	if (dir->scrub_infop->pending_scrub_error) {
	  dir->scrub_infop->pending_scrub_error = false;
	  if (dir->scrub_infop->header->get_repair()) {
            results->raw_stats.repaired = true;
	    results->raw_stats.error_str
	      << "dirfrag(" << p.first << ") has bad stats (will be fixed); ";
	  } else {
	    results->raw_stats.error_str
	      << "dirfrag(" << p.first << ") has bad stats; ";
	  }
	  frags_errors++;
	}
      }
      nest_info.rsubdirs++; // it gets one to account for self
      if (const sr_t *srnode = in->get_projected_srnode(); srnode)
	nest_info.rsnaps += srnode->snaps.size();

      // ...and that their sum matches our inode settings
      if (!dir_info.same_sums(in->get_inode()->dirstat) ||
	  !nest_info.same_sums(in->get_inode()->rstat)) {
	if (in->scrub_infop->header->get_repair()) {
	  results->raw_stats.error_str
	    << "freshly-calculated rstats don't match existing ones (will be fixed)";
	  in->mdcache->repair_inode_stats(in);
          results->raw_stats.repaired = true;
	} else {
	  results->raw_stats.error_str
	    << "freshly-calculated rstats don't match existing ones";
	}
	goto next;
      }
      if (frags_errors > 0)
	goto next;

      results->raw_stats.passed = true;
next:
      // snaprealm
      return check_inode_snaprealm();
    }

    bool check_inode_snaprealm() {
      if (!in->snaprealm)
	return true;

      if (!in->snaprealm->have_past_parents_open()) {
	in->snaprealm->open_parents(get_internal_callback(SNAPREALM));
	return false;
      } else {
	return immediate(SNAPREALM, 0);
      }
    }

    bool _snaprealm(int rval) {

      if (in->snaprealm->past_parents_dirty ||
	  !in->get_projected_srnode()->past_parents.empty()) {
	// temporarily store error in field of on-disk inode validation temporarily
	results->inode.checked = true;
	results->inode.passed = false;
	if (in->scrub_infop->header->get_repair()) {
	  results->inode.error_str << "Inode has old format snaprealm (will upgrade)";
	  results->inode.repaired = true;
	  in->mdcache->upgrade_inode_snaprealm(in);
	} else {
	  results->inode.error_str << "Inode has old format snaprealm";
	}
      }
      return true;
    }

    void _done() override {
      if ((!results->raw_stats.checked || results->raw_stats.passed) &&
	  (!results->backtrace.checked || results->backtrace.passed) &&
	  (!results->inode.checked || results->inode.passed))
	results->passed_validation = true;

      // Flag that we did some repair work so that our repair operation
      // can be flushed at end of scrub
      if (results->backtrace.repaired ||
	  results->inode.repaired ||
	  results->raw_stats.repaired)
	in->scrub_infop->header->set_repaired();
      if (fin)
	fin->complete(get_rval());
    }
  };


  dout(10) << "scrub starting validate_disk_state on " << *this << dendl;
  ValidationContinuation *vc = new ValidationContinuation(this,
                                                          results,
                                                          fin);
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
    f->open_object_section("raw_stats");
    {
      f->dump_bool("checked", raw_stats.checked);
      f->dump_bool("passed", raw_stats.passed);
      f->dump_int("read_ret_val", raw_stats.ondisk_read_retval);
      f->dump_stream("ondisk_value.dirstat") << raw_stats.ondisk_value.dirstat;
      f->dump_stream("ondisk_value.rstat") << raw_stats.ondisk_value.rstat;
      f->dump_stream("memory_value.dirrstat") << raw_stats.memory_value.dirstat;
      f->dump_stream("memory_value.rstat") << raw_stats.memory_value.rstat;
      f->dump_string("error_str", raw_stats.error_str.str());
    }
    f->close_section(); // raw_stats
    // dump failure return code
    int rc = 0;
    if (backtrace.checked && backtrace.ondisk_read_retval)
      rc = backtrace.ondisk_read_retval;
    if (inode.checked && inode.ondisk_read_retval)
      rc = inode.ondisk_read_retval;
    if (raw_stats.checked && raw_stats.ondisk_read_retval)
      rc = raw_stats.ondisk_read_retval;
    f->dump_int("return_code", rc);
  }
  f->close_section(); // results
}

bool CInode::validated_data::all_damage_repaired() const
{
  bool unrepaired =
    (raw_stats.checked && !raw_stats.passed && !raw_stats.repaired)
    ||
    (backtrace.checked && !backtrace.passed && !backtrace.repaired)
    ||
    (inode.checked && !inode.passed && !inode.repaired);

  return !unrepaired;
}

void CInode::dump(Formatter *f, int flags) const
{
  if (flags & DUMP_PATH) {
    std::string path;
    make_path_string(path, true);
    if (path.empty())
      path = "/";
    f->dump_string("path", path);
  }

  if (flags & DUMP_INODE_STORE_BASE)
    InodeStoreBase::dump(f);
  
  if (flags & DUMP_MDS_CACHE_OBJECT)
    MDSCacheObject::dump(f);

  if (flags & DUMP_LOCKS) {
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
  }

  if (flags & DUMP_STATE) {
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
    if (state_test(STATE_MISSINGOBJS))
      f->dump_string("state", "missingobjs");
    f->close_section();
  }

  if (flags & DUMP_CAPS) {
    f->open_array_section("client_caps");
    for (const auto &p : client_caps) {
      auto &client = p.first;
      auto cap = &p.second;
      f->open_object_section("client_cap");
      f->dump_int("client_id", client.v);
      f->dump_string("pending", ccap_string(cap->pending()));
      f->dump_string("issued", ccap_string(cap->issued()));
      f->dump_string("wanted", ccap_string(cap->wanted()));
      f->dump_int("last_sent", cap->get_last_seq());
      f->close_section();
    }
    f->close_section();

    f->dump_int("loner", loner_cap.v);
    f->dump_int("want_loner", want_loner_cap.v);

    f->open_array_section("mds_caps_wanted");
    for (const auto &p : mds_caps_wanted) {
      f->open_object_section("mds_cap_wanted");
      f->dump_int("rank", p.first);
      f->dump_string("cap", ccap_string(p.second));
      f->close_section();
    }
    f->close_section();
  }

  if (flags & DUMP_DIRFRAGS) {
    f->open_array_section("dirfrags");
    auto&& dfs = get_dirfrags();
    for(const auto &dir: dfs) {
      f->open_object_section("dir");
      dir->dump(f, CDir::DUMP_DEFAULT | CDir::DUMP_ITEMS);
      dir->check_rstats();
      f->close_section();
    }
    f->close_section();
  }
}

/****** Scrub Stuff *****/
void CInode::scrub_info_create() const
{
  dout(25) << __func__ << dendl;
  ceph_assert(!scrub_infop);

  // break out of const-land to set up implicit initial state
  CInode *me = const_cast<CInode*>(this);
  const auto& pi = me->get_projected_inode();

  scrub_info_t *si = new scrub_info_t();
  si->scrub_start_stamp = si->last_scrub_stamp = pi->last_scrub_stamp;
  si->scrub_start_version = si->last_scrub_version = pi->last_scrub_version;

  me->scrub_infop = si;
}

void CInode::scrub_maybe_delete_info()
{
  if (scrub_infop &&
      !scrub_infop->scrub_in_progress &&
      !scrub_infop->last_scrub_dirty) {
    delete scrub_infop;
    scrub_infop = NULL;
  }
}

void CInode::scrub_initialize(CDentry *scrub_parent,
			      ScrubHeaderRef& header,
			      MDSContext *f)
{
  dout(20) << __func__ << " with scrub_version " << get_version() << dendl;
  if (scrub_is_in_progress()) {
    dout(20) << __func__ << " inode moved during scrub, reinitializing "
	     << dendl;
    ceph_assert(scrub_infop->scrub_parent);
    CDentry *dn = scrub_infop->scrub_parent;
    CDir *dir = dn->dir;
    dn->put(CDentry::PIN_SCRUBPARENT);
    ceph_assert(dir->scrub_infop && dir->scrub_infop->directory_scrubbing);
    dir->scrub_infop->directories_scrubbing.erase(dn->key());
    dir->scrub_infop->others_scrubbing.erase(dn->key());
  }
  scrub_info();
  if (!scrub_infop)
    scrub_infop = new scrub_info_t();

  if (get_projected_inode()->is_dir()) {
    // fill in dirfrag_stamps with initial state
    frag_vec_t leaves;
    dirfragtree.get_leaves(leaves);
    for (const auto& leaf : leaves) {
      if (header->get_force())
	scrub_infop->dirfrag_stamps[leaf].reset();
      else
	scrub_infop->dirfrag_stamps[leaf];
    }
  }

  if (scrub_parent)
    scrub_parent->get(CDentry::PIN_SCRUBPARENT);
  scrub_infop->scrub_parent = scrub_parent;
  scrub_infop->on_finish = f;
  scrub_infop->scrub_in_progress = true;
  scrub_infop->children_scrubbed = false;
  scrub_infop->header = header;

  scrub_infop->scrub_start_version = get_version();
  scrub_infop->scrub_start_stamp = ceph_clock_now();
  // right now we don't handle remote inodes
}

int CInode::scrub_dirfrag_next(frag_t* out_dirfrag)
{
  dout(20) << __func__ << dendl;
  ceph_assert(scrub_is_in_progress());

  if (!is_dir()) {
    return -ENOTDIR;
  }

  std::map<frag_t, scrub_stamp_info_t>::iterator i =
      scrub_infop->dirfrag_stamps.begin();

  while (i != scrub_infop->dirfrag_stamps.end()) {
    if (i->second.scrub_start_version < scrub_infop->scrub_start_version) {
      i->second.scrub_start_version = get_projected_version();
      i->second.scrub_start_stamp = ceph_clock_now();
      *out_dirfrag = i->first;
      dout(20) << " return frag " << *out_dirfrag << dendl;
      return 0;
    }
    ++i;
  }

  dout(20) << " no frags left, ENOENT " << dendl;
  return ENOENT;
}

void CInode::scrub_dirfrags_scrubbing(frag_vec_t* out_dirfrags)
{
  ceph_assert(out_dirfrags != NULL);
  ceph_assert(scrub_infop != NULL);

  out_dirfrags->clear();
  std::map<frag_t, scrub_stamp_info_t>::iterator i =
      scrub_infop->dirfrag_stamps.begin();

  while (i != scrub_infop->dirfrag_stamps.end()) {
    if (i->second.scrub_start_version >= scrub_infop->scrub_start_version) {
      if (i->second.last_scrub_version < scrub_infop->scrub_start_version)
        out_dirfrags->push_back(i->first);
    } else {
      return;
    }

    ++i;
  }
}

void CInode::scrub_dirfrag_finished(frag_t dirfrag)
{
  dout(20) << __func__ << " on frag " << dirfrag << dendl;
  ceph_assert(scrub_is_in_progress());

  std::map<frag_t, scrub_stamp_info_t>::iterator i =
      scrub_infop->dirfrag_stamps.find(dirfrag);
  ceph_assert(i != scrub_infop->dirfrag_stamps.end());

  scrub_stamp_info_t &si = i->second;
  si.last_scrub_stamp = si.scrub_start_stamp;
  si.last_scrub_version = si.scrub_start_version;
}

void CInode::scrub_aborted(MDSContext **c) {
  dout(20) << __func__ << dendl;
  ceph_assert(scrub_is_in_progress());

  *c = nullptr;
  std::swap(*c, scrub_infop->on_finish);

  if (scrub_infop->scrub_parent) {
    CDentry *dn = scrub_infop->scrub_parent;
    scrub_infop->scrub_parent = NULL;
    dn->dir->scrub_dentry_finished(dn);
    dn->put(CDentry::PIN_SCRUBPARENT);
  }

  delete scrub_infop;
  scrub_infop = nullptr;
}

void CInode::scrub_finished(MDSContext **c) {
  dout(20) << __func__ << dendl;
  ceph_assert(scrub_is_in_progress());
  for (std::map<frag_t, scrub_stamp_info_t>::iterator i =
      scrub_infop->dirfrag_stamps.begin();
      i != scrub_infop->dirfrag_stamps.end();
      ++i) {
    if(i->second.last_scrub_version != i->second.scrub_start_version) {
      derr << i->second.last_scrub_version << " != "
        << i->second.scrub_start_version << dendl;
    }
    ceph_assert(i->second.last_scrub_version == i->second.scrub_start_version);
  }

  scrub_infop->last_scrub_version = scrub_infop->scrub_start_version;
  scrub_infop->last_scrub_stamp = scrub_infop->scrub_start_stamp;
  scrub_infop->last_scrub_dirty = true;
  scrub_infop->scrub_in_progress = false;

  if (scrub_infop->scrub_parent) {
    CDentry *dn = scrub_infop->scrub_parent;
    scrub_infop->scrub_parent = NULL;
    dn->dir->scrub_dentry_finished(dn);
    dn->put(CDentry::PIN_SCRUBPARENT);
  }

  *c = scrub_infop->on_finish;
  scrub_infop->on_finish = NULL;

  if (scrub_infop->header->get_origin() == this) {
    // We are at the point that a tagging scrub was initiated
    LogChannelRef clog = mdcache->mds->clog;
    clog->info() << "scrub complete with tag '"
                 << scrub_infop->header->get_tag() << "'";
  }
}

int64_t CInode::get_backtrace_pool() const
{
  if (is_dir()) {
    return mdcache->mds->mdsmap->get_metadata_pool();
  } else {
    // Files are required to have an explicit layout that specifies
    // a pool
    ceph_assert(get_inode()->layout.pool_id != -1);
    return get_inode()->layout.pool_id;
  }
}

void CInode::queue_export_pin(mds_rank_t target)
{
  if (state_test(CInode::STATE_QUEUEDEXPORTPIN))
    return;

  bool queue = false;
  for (auto& p : dirfrags) {
    CDir *dir = p.second;
    if (!dir->is_auth())
      continue;
    if (target != MDS_RANK_NONE) {
      if (dir->is_subtree_root()) {
	// set auxsubtree bit or export it
	if (!dir->state_test(CDir::STATE_AUXSUBTREE) ||
	    target != dir->get_dir_auth().first)
	  queue = true;
      } else {
	// create aux subtree or export it
	queue = true;
      }
    } else {
      // clear aux subtrees ?
      queue = dir->state_test(CDir::STATE_AUXSUBTREE);
    }
    if (queue) {
      state_set(CInode::STATE_QUEUEDEXPORTPIN);
      mdcache->export_pin_queue.insert(this);
      break;
    }
  }
}

void CInode::maybe_export_pin(bool update)
{
  if (!g_conf()->mds_bal_export_pin)
    return;
  if (!is_dir() || !is_normal())
    return;

  dout(15) << __func__ << " update=" << update << " " << *this << dendl;

  mds_rank_t export_pin = get_export_pin(false, false);
  if (export_pin == MDS_RANK_NONE && !update) {
    return;
  }

  /* disable ephemeral pins */
  set_ephemeral_dist(false);
  set_ephemeral_rand(false);
  queue_export_pin(export_pin);
}

void CInode::set_ephemeral_dist(bool yes)
{
  if (yes) {
    if (!state_test(CInode::STATE_DISTEPHEMERALPIN)) {
      state_set(CInode::STATE_DISTEPHEMERALPIN);
      auto p = mdcache->dist_ephemeral_pins.insert(this);
      ceph_assert(p.second);
    }
  } else {
    /* avoid std::set::erase if unnecessary */
    if (state_test(CInode::STATE_DISTEPHEMERALPIN)) {
      dout(10) << "clearing ephemeral distributed pin on " << *this << dendl;
      state_clear(CInode::STATE_DISTEPHEMERALPIN);
      auto count = mdcache->dist_ephemeral_pins.erase(this);
      ceph_assert(count == 1);
      queue_export_pin(MDS_RANK_NONE);
    }
  }
}

void CInode::maybe_ephemeral_dist(bool update)
{
  if (!mdcache->get_export_ephemeral_distributed_config()) {
    dout(15) << __func__ << " config false: cannot ephemeral distributed pin " << *this << dendl;
    set_ephemeral_dist(false);
    return;
  } else if (!is_dir() || !is_normal()) {
    dout(15) << __func__ << " !dir or !normal: cannot ephemeral distributed pin " << *this << dendl;
    set_ephemeral_dist(false);
    return;
  } else if (get_inode()->nlink == 0) {
    dout(15) << __func__ << " unlinked directory: cannot ephemeral distributed pin " << *this << dendl;
    set_ephemeral_dist(false);
    return;
  } else if (!update && state_test(CInode::STATE_DISTEPHEMERALPIN)) {
    dout(15) << __func__ << " requeueing already pinned " << *this << dendl;
    queue_export_pin(mdcache->hash_into_rank_bucket(ino()));
    return;
  }

  dout(15) << __func__ << " update=" << update << " " << *this << dendl;

  auto dir = get_parent_dir();
  if (!dir) {
    return;
  }

  bool pin = dir->get_inode()->get_inode()->export_ephemeral_distributed_pin;
  if (pin) {
    dout(10) << __func__ << "  ephemeral distributed pinning " << *this << dendl;
    set_ephemeral_dist(true);
    queue_export_pin(mdcache->hash_into_rank_bucket(ino()));
  } else if (update) {
    set_ephemeral_dist(false);
    queue_export_pin(MDS_RANK_NONE);
  }
}

void CInode::maybe_ephemeral_dist_children(bool update)
{
  if (!mdcache->get_export_ephemeral_distributed_config()) {
    dout(15) << __func__ << " config false: cannot ephemeral distributed pin " << *this << dendl;
    return;
  } else if (!is_dir() || !is_normal()) {
    dout(15) << __func__ << " !dir or !normal: cannot ephemeral distributed pin " << *this << dendl;
    return;
  } else if (get_inode()->nlink == 0) {
    dout(15) << __func__ << " unlinked directory: cannot ephemeral distributed pin " << *this << dendl;
    return;
  }

  bool pin = get_inode()->export_ephemeral_distributed_pin;
  /* FIXME: expensive to iterate children when not updating */
  if (!pin && !update) {
    return;
  }

  dout(10) << __func__ << " maybe ephemerally pinning children of " << *this << dendl;
  for (auto& p : dirfrags) {
    auto& dir = p.second;
    for (auto& q : *dir) {
      auto& dn = q.second;
      auto&& in = dn->get_linkage()->get_inode();
      if (in && in->is_dir()) {
        in->maybe_ephemeral_dist(update);
      }
    }
  }
}

void CInode::set_ephemeral_rand(bool yes)
{
  if (yes) {
    if (!state_test(CInode::STATE_RANDEPHEMERALPIN)) {
      state_set(CInode::STATE_RANDEPHEMERALPIN);
      auto p = mdcache->rand_ephemeral_pins.insert(this);
      ceph_assert(p.second);
    }
  } else {
    if (state_test(CInode::STATE_RANDEPHEMERALPIN)) {
      dout(10) << "clearing ephemeral random pin on " << *this << dendl;
      state_clear(CInode::STATE_RANDEPHEMERALPIN);
      auto count = mdcache->rand_ephemeral_pins.erase(this);
      ceph_assert(count == 1);
      queue_export_pin(MDS_RANK_NONE);
    }
  }
}

void CInode::maybe_ephemeral_rand(bool fresh, double threshold)
{
  if (!mdcache->get_export_ephemeral_random_config()) {
    dout(15) << __func__ << " config false: cannot ephemeral random pin " << *this << dendl;
    set_ephemeral_rand(false);
    return;
  } else if (!is_dir() || !is_normal()) {
    dout(15) << __func__ << " !dir or !normal: cannot ephemeral random pin " << *this << dendl;
    set_ephemeral_rand(false);
    return;
  } else if (get_inode()->nlink == 0) {
    dout(15) << __func__ << " unlinked directory: cannot ephemeral random pin " << *this << dendl;
    set_ephemeral_rand(false);
    return;
  } else if (state_test(CInode::STATE_RANDEPHEMERALPIN)) {
    dout(10) << __func__ << " already ephemeral random pinned: requeueing " << *this << dendl;
    queue_export_pin(mdcache->hash_into_rank_bucket(ino()));
    return;
  } else if (!fresh) {
    return;
  }

  /* not precomputed? */
  if (threshold < 0.0) {
    threshold = get_ephemeral_rand();
  }
  if (threshold <= 0.0) {
    return;
  }
  double n = ceph::util::generate_random_number(0.0, 1.0);

  dout(15) << __func__ << " rand " << n << " <?= " << threshold
           << " " << *this << dendl;

  if (n <= threshold) {
    dout(10) << __func__ << " randomly export pinning " << *this << dendl;
    set_ephemeral_rand(true);
    queue_export_pin(mdcache->hash_into_rank_bucket(ino()));
  }
}

void CInode::setxattr_ephemeral_rand(double probability)
{
  ceph_assert(is_dir());
  _get_projected_inode()->export_ephemeral_random_pin = probability;
}

void CInode::setxattr_ephemeral_dist(bool val)
{
  ceph_assert(is_dir());
  _get_projected_inode()->export_ephemeral_distributed_pin = val;
}

void CInode::set_export_pin(mds_rank_t rank)
{
  ceph_assert(is_dir());
  _get_projected_inode()->export_pin = rank;
  maybe_export_pin(true);
}

void CInode::check_pin_policy()
{
  const CInode *in = this;
  mds_rank_t etarget = MDS_RANK_NONE;
  while (true) {
    if (in->is_system())
      break;
    const CDentry *pdn = in->get_parent_dn();
    if (!pdn)
      break;
    if (in->get_inode()->nlink == 0) {
      // ignore export pin for unlinked directory
      return;
    } else if (etarget != MDS_RANK_NONE && in->has_ephemeral_policy()) {
      return;
    } else if (in->get_inode()->export_pin >= 0) {
      /* clear any epin policy */
      set_ephemeral_dist(false);
      set_ephemeral_rand(false);
      return;
    } else if (etarget == MDS_RANK_NONE && in->is_ephemerally_pinned()) {
      /* If a parent overrides a grandparent ephemeral pin policy with an export pin, we use that export pin instead. */
      etarget = mdcache->hash_into_rank_bucket(in->ino());
    }
    in = pdn->get_dir()->inode;
  }
}

mds_rank_t CInode::get_export_pin(bool inherit, bool ephemeral) const
{
  /* An inode that is export pinned may not necessarily be a subtree root, we
   * need to traverse the parents. A base or system inode cannot be pinned.
   * N.B. inodes not yet linked into a dir (i.e. anonymous inodes) will not
   * have a parent yet.
   */
  const CInode *in = this;
  mds_rank_t etarget = MDS_RANK_NONE;
  while (true) {
    if (in->is_system())
      break;
    const CDentry *pdn = in->get_parent_dn();
    if (!pdn)
      break;
    if (in->get_inode()->nlink == 0) {
      // ignore export pin for unlinked directory
      return MDS_RANK_NONE;
    } else if (etarget != MDS_RANK_NONE && in->has_ephemeral_policy()) {
      return etarget;
    } else if (in->get_inode()->export_pin >= 0) {
      return in->get_inode()->export_pin;
    } else if (etarget == MDS_RANK_NONE && ephemeral && in->is_ephemerally_pinned()) {
      /* If a parent overrides a grandparent ephemeral pin policy with an export pin, we use that export pin instead. */
      etarget = mdcache->hash_into_rank_bucket(in->ino());
      if (!inherit) return etarget;
    }

    if (!inherit) {
      break;
    }
    in = pdn->get_dir()->inode;
  }
  return MDS_RANK_NONE;
}

double CInode::get_ephemeral_rand(bool inherit) const
{
  /* N.B. inodes not yet linked into a dir (i.e. anonymous inodes) will not
   * have a parent yet.
   */
  const CInode *in = this;
  double max = mdcache->export_ephemeral_random_max;
  while (true) {
    if (in->is_system())
      break;
    const CDentry *pdn = in->get_parent_dn();
    if (!pdn)
      break;
    // ignore export pin for unlinked directory
    if (in->get_inode()->nlink == 0)
      break;

    if (in->get_inode()->export_ephemeral_random_pin > 0.0)
      return std::min(in->get_inode()->export_ephemeral_random_pin, max);

    /* An export_pin overrides only if no closer parent (incl. this one) has a
     * random pin set.
     */
    if (in->get_inode()->export_pin >= 0)
      return 0.0;

    if (!inherit)
      break;
    in = pdn->get_dir()->inode;
  }
  return 0.0;
}

bool CInode::is_exportable(mds_rank_t dest) const
{
  mds_rank_t pin = get_export_pin();
  if (pin == dest) {
    return true;
  } else if (pin >= 0) {
    return false;
  } else {
    return true;
  }
}

void CInode::get_nested_dirfrags(std::vector<CDir*>& v) const
{
  for (const auto &p : dirfrags) {
    const auto& dir = p.second;
    if (!dir->is_subtree_root())
      v.push_back(dir);
  }
}

void CInode::get_subtree_dirfrags(std::vector<CDir*>& v) const
{
  for (const auto &p : dirfrags) {
    const auto& dir = p.second;
    if (dir->is_subtree_root())
      v.push_back(dir);
  }
}

MEMPOOL_DEFINE_OBJECT_FACTORY(CInode, co_inode, mds_co);
