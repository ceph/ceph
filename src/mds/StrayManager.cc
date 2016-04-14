// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */


#include "common/perf_counters.h"

#include "osdc/Objecter.h"
#include "osdc/Filer.h"
#include "mds/MDSRank.h"
#include "mds/MDCache.h"
#include "mds/MDLog.h"
#include "mds/CDir.h"
#include "mds/CDentry.h"
#include "events/EUpdate.h"
#include "messages/MClientRequest.h"

#include "StrayManager.h"

#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix _prefix(_dout, mds)
static ostream& _prefix(std::ostream *_dout, MDSRank *mds) {
  return *_dout << "mds." << mds->get_nodeid() << ".cache.strays ";
}

class StrayManagerIOContext : public virtual MDSIOContextBase {
protected:
  StrayManager *sm;
  virtual MDSRank *get_mds()
  {
    return sm->mds;
  }
public:
  explicit StrayManagerIOContext(StrayManager *sm_) : sm(sm_) {}
};


class StrayManagerContext : public virtual MDSInternalContextBase {
protected:
  StrayManager *sm;
  virtual MDSRank *get_mds()
  {
    return sm->mds;
  }
public:
  explicit StrayManagerContext(StrayManager *sm_) : sm(sm_) {}
};


/**
 * Context wrapper for _purge_stray_purged completion
 */
class C_IO_PurgeStrayPurged : public StrayManagerIOContext {
  CDentry *dn;
  bool only_head;
  // How many ops_in_flight were allocated to this purge?
  uint32_t ops_allowance;
public:
  C_IO_PurgeStrayPurged(StrayManager *sm_, CDentry *d, bool oh, uint32_t ops) : 
    StrayManagerIOContext(sm_), dn(d), only_head(oh), ops_allowance(ops) { }
  void finish(int r) {
    assert(r == 0 || r == -ENOENT);
    sm->_purge_stray_purged(dn, ops_allowance, only_head);
  }
};

void StrayManager::purge(CDentry *dn, uint32_t op_allowance)
{
  CDentry::linkage_t *dnl = dn->get_projected_linkage();
  CInode *in = dnl->get_inode();
  dout(10) << __func__ << " " << *dn << " " << *in << dendl;
  assert(!dn->is_replicated());

  num_strays_purging++;
  logger->set(l_mdc_num_strays_purging, num_strays_purging);


  // CHEAT.  there's no real need to journal our intent to purge, since
  // that is implicit in the dentry's presence and non-use in the stray
  // dir.  on recovery, we'll need to re-eval all strays anyway.
  
  SnapContext nullsnapc;
  C_GatherBuilder gather(
    g_ceph_context,
    new C_OnFinisher(new C_IO_PurgeStrayPurged(
        this, dn, false, op_allowance), mds->finisher));

  if (in->is_dir()) {
    object_locator_t oloc(mds->mdsmap->get_metadata_pool());
    std::list<frag_t> ls;
    if (!in->dirfragtree.is_leaf(frag_t()))
      in->dirfragtree.get_leaves(ls);
    ls.push_back(frag_t());
    for (std::list<frag_t>::iterator p = ls.begin();
         p != ls.end();
         ++p) {
      object_t oid = CInode::get_object_name(in->inode.ino, *p, "");
      dout(10) << __func__ << " remove dirfrag " << oid << dendl;
      mds->objecter->remove(oid, oloc, nullsnapc,
			    ceph::real_clock::now(g_ceph_context),
			    0, NULL, gather.new_sub());
    }
    assert(gather.has_subs());
    gather.activate();
    return;
  }

  const SnapContext *snapc;
  SnapRealm *realm = in->find_snaprealm();
  if (realm) {
    dout(10) << " realm " << *realm << dendl;
    snapc = &realm->get_snap_context();
  } else {
    dout(10) << " NO realm, using null context" << dendl;
    snapc = &nullsnapc;
    assert(in->last == CEPH_NOSNAP);
  }

  if (in->is_file()) {
    uint64_t to = in->inode.get_max_size();
    to = MAX(in->inode.size, to);
    // when truncating a file, the filer does not delete stripe objects that are
    // truncated to zero. so we need to purge stripe objects up to the max size
    // the file has ever been.
    to = MAX(in->inode.max_size_ever, to);
    if (to > 0) {
      uint64_t num = Striper::get_num_objects(in->inode.layout, to);
      dout(10) << __func__ << " 0~" << to << " objects 0~" << num
	       << " snapc " << snapc << " on " << *in << dendl;
      filer.purge_range(in->inode.ino, &in->inode.layout, *snapc,
			0, num, ceph::real_clock::now(g_ceph_context), 0,
			gather.new_sub());
    }
  }

  inode_t *pi = in->get_projected_inode();
  object_t oid = CInode::get_object_name(pi->ino, frag_t(), "");
  // remove the backtrace object if it was not purged
  if (!gather.has_subs() || !pi->layout.pool_ns.empty()) {
    object_locator_t oloc(pi->layout.pool_id);
    dout(10) << __func__ << " remove backtrace object " << oid
	     << " pool " << oloc.pool << " snapc " << snapc << dendl;
    mds->objecter->remove(oid, oloc, *snapc,
			  ceph::real_clock::now(g_ceph_context), 0,
			  NULL, gather.new_sub());
  }
  // remove old backtrace objects
  for (compact_set<int64_t>::iterator p = pi->old_pools.begin();
       p != pi->old_pools.end();
       ++p) {
    object_locator_t oloc(*p);
    dout(10) << __func__ << " remove backtrace object " << oid
	     << " old pool " << *p << " snapc " << snapc << dendl;
    mds->objecter->remove(oid, oloc, *snapc,
			  ceph::real_clock::now(g_ceph_context), 0,
			  NULL, gather.new_sub());
  }
  assert(gather.has_subs());
  gather.activate();
}

class C_PurgeStrayLogged : public StrayManagerContext {
  CDentry *dn;
  version_t pdv;
  LogSegment *ls;
public:
  C_PurgeStrayLogged(StrayManager *sm_, CDentry *d, version_t v, LogSegment *s) : 
    StrayManagerContext(sm_), dn(d), pdv(v), ls(s) { }
  void finish(int r) {
    sm->_purge_stray_logged(dn, pdv, ls);
  }
};

class C_TruncateStrayLogged : public StrayManagerContext {
  CDentry *dn;
  LogSegment *ls;
public:
  C_TruncateStrayLogged(StrayManager *sm, CDentry *d, LogSegment *s) :
    StrayManagerContext(sm), dn(d), ls(s) { }
  void finish(int r) {
    sm->_truncate_stray_logged(dn, ls);
  }
};

void StrayManager::_purge_stray_purged(
    CDentry *dn, uint32_t ops_allowance, bool only_head)
{
  CInode *in = dn->get_projected_linkage()->get_inode();
  dout(10) << "_purge_stray_purged " << *dn << " " << *in << dendl;

  if (only_head) {
    /* This was a ::truncate */
    EUpdate *le = new EUpdate(mds->mdlog, "purge_stray truncate");
    mds->mdlog->start_entry(le);
    
    inode_t *pi = in->project_inode();
    pi->size = 0;
    pi->max_size_ever = 0;
    pi->client_ranges.clear();
    pi->truncate_size = 0;
    pi->truncate_from = 0;
    pi->version = in->pre_dirty();

    le->metablob.add_dir_context(dn->dir);
    le->metablob.add_primary_dentry(dn, in, true);

    mds->mdlog->submit_entry(le,
        new C_TruncateStrayLogged(
          this, dn, mds->mdlog->get_current_segment()));
  } else {
    if (in->get_num_ref() != (int)in->is_dirty() ||
        dn->get_num_ref() != (int)dn->is_dirty() + !!in->get_num_ref() + 1/*PIN_PURGING*/) {
      // Nobody should be taking new references to an inode when it
      // is being purged (aside from it it were 

      derr << "Rogue reference after purge to " << *dn << dendl;
      assert(0 == "rogue reference to purging inode");
    }

    // kill dentry.
    version_t pdv = dn->pre_dirty();
    dn->push_projected_linkage(); // NULL

    EUpdate *le = new EUpdate(mds->mdlog, "purge_stray");
    mds->mdlog->start_entry(le);

    // update dirfrag fragstat, rstat
    CDir *dir = dn->get_dir();
    fnode_t *pf = dir->project_fnode();
    pf->version = dir->pre_dirty();
    if (in->is_dir())
      pf->fragstat.nsubdirs--;
    else
      pf->fragstat.nfiles--;
    pf->rstat.sub(in->inode.accounted_rstat);

    le->metablob.add_dir_context(dn->dir);
    EMetaBlob::dirlump& dl = le->metablob.add_dir(dn->dir, true);
    le->metablob.add_null_dentry(dl, dn, true);
    le->metablob.add_destroyed_inode(in->ino());

    mds->mdlog->submit_entry(le, new C_PurgeStrayLogged(this, dn, pdv,
          mds->mdlog->get_current_segment()));

    num_strays--;
    logger->set(l_mdc_num_strays, num_strays);
    logger->inc(l_mdc_strays_purged);
  }

  num_strays_purging--;
  logger->set(l_mdc_num_strays_purging, num_strays_purging);

  // Release resources
  dout(10) << __func__ << ": decrementing op allowance "
    << ops_allowance << " from " << ops_in_flight << " in flight" << dendl;
  assert(ops_in_flight >= ops_allowance);
  ops_in_flight -= ops_allowance;
  logger->set(l_mdc_num_purge_ops, ops_in_flight);
  files_purging -= 1;
  _advance();
}

void StrayManager::_purge_stray_logged(CDentry *dn, version_t pdv, LogSegment *ls)
{
  CInode *in = dn->get_linkage()->get_inode();
  dout(10) << "_purge_stray_logged " << *dn << " " << *in << dendl;

  assert(!in->state_test(CInode::STATE_RECOVERING));

  // unlink
  assert(dn->get_projected_linkage()->is_null());
  dn->dir->unlink_inode(dn);
  dn->pop_projected_linkage();
  dn->mark_dirty(pdv, ls);

  dn->dir->pop_and_dirty_projected_fnode(ls);

  in->state_clear(CInode::STATE_ORPHAN);
  dn->state_clear(CDentry::STATE_PURGING | CDentry::STATE_PURGINGPINNED);
  dn->put(CDentry::PIN_PURGING);

  // drop inode
  if (in->is_dirty())
    in->mark_clean();
  in->mdcache->remove_inode(in);

  // drop dentry?
  if (dn->is_new()) {
    dout(20) << " dn is new, removing" << dendl;
    dn->mark_clean();
    dn->dir->remove_dentry(dn);
  } else {
    in->mdcache->touch_dentry_bottom(dn);  // drop dn as quickly as possible.
  }
}

void StrayManager::enqueue(CDentry *dn, bool trunc)
{
  CDentry::linkage_t *dnl = dn->get_projected_linkage();
  assert(dnl);
  CInode *in = dnl->get_inode();
  assert(in);

  /* We consider a stray to be purging as soon as it is enqueued, to avoid
   * enqueing it twice */
  dn->state_set(CDentry::STATE_PURGING);
  in->state_set(CInode::STATE_PURGING);

  /* We must clear this as soon as enqueuing it, to prevent the journal
   * expiry code from seeing a dirty parent and trying to write a backtrace */
  if (!trunc) {
    if (in->is_dirty_parent()) {
      in->clear_dirty_parent();
    }
  }

  const uint32_t ops_required = _calculate_ops_required(in, trunc);

  // Try to purge immediately if there is nothing in the queue, otherwise
  // we will go to the back of the queue (even if there is allowance available
  // to run us immediately) in order to be fair to others.
  bool consumed = false;
  if (ready_for_purge.empty()) {
    consumed = _consume(dn, trunc, ops_required);
  }

  if (consumed) {
    dout(10) << __func__ << ": purging this dentry immediately: "
      << *dn << dendl;
  } else {
    dout(10) << __func__ << ": enqueuing this dentry for later purge: "
      << *dn << dendl;
    if (!dn->state_test(CDentry::STATE_PURGINGPINNED) &&
        ready_for_purge.size() < g_conf->mds_max_purge_files) {
      dn->get(CDentry::PIN_PURGING);
      dn->state_set(CDentry::STATE_PURGINGPINNED);
    }
    ready_for_purge.push_back(QueuedStray(dn, trunc, ops_required));
  }
}

class C_StraysFetched : public StrayManagerContext {
public:
  C_StraysFetched(StrayManager *sm_) :
    StrayManagerContext(sm_) { }
  void finish(int r) {
    sm->_advance();
  }
};

void StrayManager::_advance()
{
  std::map<CDir*, std::set<dentry_key_t> > to_fetch;

  for (auto p = ready_for_purge.begin();
       p != ready_for_purge.end();) {
    const QueuedStray &qs = *p;
    auto q = p++;
    CDentry *dn = qs.dir->lookup_exact_snap(qs.name, CEPH_NOSNAP);
    if (!dn) {
      assert(trimmed_strays.count(qs.name) > 0);
      if (fetching_strays.size() >= g_conf->mds_max_purge_files) {
	break;
      }
      
      dout(10) << __func__ << ": fetching stray dentry " << qs.name << dendl;

      auto it = fetching_strays.insert(qs);
      assert(it.second);
      to_fetch[qs.dir].insert(dentry_key_t(CEPH_NOSNAP, (it.first)->name.c_str()));
      ready_for_purge.erase(q);
      continue;
    }

    const bool consumed = _consume(dn, qs.trunc, qs.ops_required);
    if (!consumed) {
      break;
    }
    ready_for_purge.erase(q);
  }

  MDSGatherBuilder gather(g_ceph_context);
  for (auto p = to_fetch.begin(); p != to_fetch.end(); ++p)
    p->first->fetch(gather.new_sub(), p->second);

  if (gather.has_subs()) {
    gather.set_finisher(new C_StraysFetched(this));
    gather.activate();
  }
}

/*
 * Note that there are compromises to how throttling
 * is implemented here, in the interests of simplicity:
 *  * If insufficient ops are available to execute
 *    the next item on the queue, we block even if
 *    there are items further down the queue requiring
 *    fewer ops which might be executable
 *  * The ops considered "in use" by a purge will be
 *    an overestimate over the period of execution, as
 *    we count filer_max_purge_ops and ops for old backtraces
 *    as in use throughout, even though towards the end
 *    of the purge the actual ops in flight will be
 *    lower.
 *  * The ops limit may be exceeded if the number of ops
 *    required by a single inode is greater than the
 *    limit, for example directories with very many
 *    fragments.
 */
bool StrayManager::_consume(CDentry *dn, bool trunc, uint32_t ops_required)
{
  const int files_avail = g_conf->mds_max_purge_files - files_purging;

  if (files_avail <= 0) {
    dout(20) << __func__ << ": throttling on max files" << dendl;
    return false;
  } else {
    dout(20) << __func__ << ": purging dn: " << *dn << dendl;
  }

  // Calculate how much of the ops allowance is available, allowing
  // for the case where the limit is currently being exceeded.
  uint32_t ops_avail;
  if (ops_in_flight <= max_purge_ops) {
    ops_avail = max_purge_ops - ops_in_flight;
  } else {
    ops_avail = 0;
  }

  /* The ops_in_flight > 0 condition here handles the case where the
   * ops required by this inode would never fit in the limit: we wait
   * instead until nothing else is running */
  if (ops_in_flight > 0 && ops_avail < ops_required) {
    dout(20) << __func__ << ": throttling on max ops (require "
             << ops_required << ", " << ops_in_flight << " in flight" << dendl;
    return false;
  }

  if (!dn->state_test(CDentry::STATE_PURGINGPINNED)) {
    dn->get(CDentry::PIN_PURGING);
    dn->state_set(CDentry::STATE_PURGINGPINNED);
  }

  // Resources are available, acquire them and execute the purge
  files_purging += 1;
  dout(10) << __func__ << ": allocating allowance "
    << ops_required << " to " << ops_in_flight << " in flight" << dendl;
  ops_in_flight += ops_required;
  logger->set(l_mdc_num_purge_ops, ops_in_flight);

  _process(dn, trunc, ops_required);
  return true;
}

class C_OpenSnapParents : public StrayManagerContext {
  CDentry *dn;
  bool trunc;
  uint32_t ops_required;
  public:
    C_OpenSnapParents(StrayManager *sm_, CDentry *dn_, bool t, uint32_t ops) :
      StrayManagerContext(sm_), dn(dn_), trunc(t), ops_required(ops) { }
    void finish(int r) {
      sm->_process(dn, trunc, ops_required);
    }
};

void StrayManager::_process(CDentry *dn, bool trunc, uint32_t ops_required)
{
  CInode *in = dn->get_linkage()->get_inode();
  if (in->snaprealm &&
      !in->snaprealm->have_past_parents_open() &&
      !in->snaprealm->open_parents(new C_OpenSnapParents(this, dn, trunc,
							 ops_required))) {
    // this can happen if the dentry had been trimmed from cache.
    return;
  }

  if (trunc) {
    truncate(dn, ops_required);
  } else {
    purge(dn, ops_required);
  }
}

uint32_t StrayManager::_calculate_ops_required(CInode *in, bool trunc)
{
  uint32_t ops_required = 0;
  if (in->is_dir()) {
    // Directory, count dirfrags to be deleted
    std::list<frag_t> ls;
    if (!in->dirfragtree.is_leaf(frag_t())) {
      in->dirfragtree.get_leaves(ls);
    }
    // One for the root, plus any leaves
    ops_required = 1 + ls.size();
  } else {
    // File, work out concurrent Filer::purge deletes
    const uint64_t to = MAX(in->inode.max_size_ever,
            MAX(in->inode.size, in->inode.get_max_size()));

    const uint64_t num = (to > 0) ? Striper::get_num_objects(in->inode.layout, to) : 1;
    ops_required = MIN(num, g_conf->filer_max_purge_ops);

    // Account for removing (or zeroing) backtrace
    ops_required += 1;

    // Account for deletions for old pools
    if (!trunc) {
      ops_required += in->get_projected_inode()->old_pools.size();
    }
  }

  return ops_required;
}

void StrayManager::advance_delayed()
{
  for (elist<CDentry*>::iterator p = delayed_eval_stray.begin(); !p.end(); ) {
    CDentry *dn = *p;
    ++p;
    dn->item_stray.remove_myself();
    num_strays_delayed--;

    if (dn->get_projected_linkage()->is_null()) {
      /* A special case: a stray dentry can go null if its inode is being
       * re-linked into another MDS's stray dir during a shutdown migration. */
      dout(4) << __func__ << ": delayed dentry is now null: " << *dn << dendl;
      continue;
    }

    const bool purging = eval_stray(dn);
    if (!purging) {
      derr << "Dentry " << *dn << " was purgeable but no longer is!" << dendl;
      /*
       * This can happen if a stray is purgeable, but has gained an extra
       * reference by virtue of having its backtrace updated.
       * FIXME perhaps we could simplify this further by
       * avoiding writing the backtrace of purge-ready strays, so
       * that this code could be more rigid?
       */
    }
  }
  logger->set(l_mdc_num_strays_delayed, num_strays_delayed);
}

void StrayManager::notify_stray_created()
{
  num_strays++;
  logger->set(l_mdc_num_strays, num_strays);
  logger->inc(l_mdc_strays_created);
}

void StrayManager::notify_stray_removed()
{
  num_strays--;
  logger->set(l_mdc_num_strays, num_strays);
}

struct C_EvalStray : public StrayManagerContext {
  CDentry *dn;
  C_EvalStray(StrayManager *sm_, CDentry *d) : StrayManagerContext(sm_), dn(d) {}
  void finish(int r) {
    sm->eval_stray(dn);
  }
};

struct C_MDC_EvalStray : public StrayManagerContext {
  CDentry *dn;
  C_MDC_EvalStray(StrayManager *sm_, CDentry *d) : StrayManagerContext(sm_), dn(d) {}
  void finish(int r) {
    sm->eval_stray(dn);
  }
};

bool StrayManager::__eval_stray(CDentry *dn, bool delay)
{
  dout(10) << "eval_stray " << *dn << dendl;
  CDentry::linkage_t *dnl = dn->get_projected_linkage();
  assert(dnl->is_primary());
  dout(10) << " inode is " << *dnl->get_inode() << dendl;
  CInode *in = dnl->get_inode();
  assert(in);

  // The only dentries elegible for purging are those
  // in the stray directories
  assert(dn->get_dir()->get_inode()->is_stray());

  // Inode may not pass through this function if it
  // was already identified for purging (i.e. cannot
  // call eval_stray() after purge()
  assert(!dn->state_test(CDentry::STATE_PURGING));

  if (!dn->is_auth()) {
    // has to be mine
    // move to bottom of lru so that we trim quickly!

    in->mdcache->touch_dentry_bottom(dn);
    return false;
  }

  if (dn->item_stray.is_on_list()) {
    if (delay)
      return false;

    dn->item_stray.remove_myself();
    num_strays_delayed--;
    logger->set(l_mdc_num_strays_delayed, num_strays_delayed);
  }

  // purge?
  if (in->inode.nlink == 0) {
    // past snaprealm parents imply snapped dentry remote links.
    // only important for directories.  normal file data snaps are handled
    // by the object store.
    if (in->snaprealm) {
      if (!in->snaprealm->have_past_parents_open() &&
          !in->snaprealm->open_parents(new C_MDC_EvalStray(this, dn))) {
        return false;
      }
      in->snaprealm->prune_past_parents();
      in->purge_stale_snap_data(in->snaprealm->get_snaps());
    }
    if (in->is_dir()) {
      if (in->snaprealm && in->snaprealm->has_past_parents()) {
	dout(20) << "  directory has past parents "
          << in->snaprealm->srnode.past_parents << dendl;
	return false;  // not until some snaps are deleted.
      }

      if (in->has_dirfrags()) {
        list<CDir*> ls;
        in->get_nested_dirfrags(ls);
        for (list<CDir*>::iterator p = ls.begin(); p != ls.end(); ++p) {
          (*p)->try_remove_dentries_for_stray();
        }
      }

      if (!in->remote_parents.empty()) {
	// unlink any stale remote snap dentry.
	for (compact_set<CDentry*>::iterator p = in->remote_parents.begin();
	     p != in->remote_parents.end(); ) {
	  CDentry *remote_dn = *p;
	  ++p;
	  assert(remote_dn->last != CEPH_NOSNAP);
	  remote_dn->unlink_remote(remote_dn->get_linkage());
	}
      }
    }
    if (dn->is_replicated()) {
      dout(20) << " replicated" << dendl;
      return false;
    }
    if (dn->is_any_leases() || in->is_any_caps()) {
      dout(20) << " caps | leases" << dendl;
      return false;  // wait
    }
    if (in->state_test(CInode::STATE_NEEDSRECOVER) ||
	in->state_test(CInode::STATE_RECOVERING)) {
      dout(20) << " pending recovery" << dendl;
      return false;  // don't mess with file size probing
    }
    if (in->get_num_ref() > (int)in->is_dirty() + (int)in->is_dirty_parent()) {
      dout(20) << " too many inode refs" << dendl;
      return false;
    }
    if (dn->get_num_ref() > (int)dn->is_dirty() + !!in->get_num_ref()) {
      dout(20) << " too many dn refs" << dendl;
      return false;
    }
    if (delay) {
      if (!dn->item_stray.is_on_list()) {
	delayed_eval_stray.push_back(&dn->item_stray);
	num_strays_delayed++;
	logger->set(l_mdc_num_strays_delayed, num_strays_delayed);
      }
    // don't purge multiversion inode with snap data
    } else if (in->snaprealm && in->snaprealm->has_past_parents() &&
              !in->old_inodes.empty()) {
      // A file with snapshots: we will truncate the HEAD revision
      // but leave the metadata intact.
      assert(!in->is_dir());
      dout(20) << " file has past parents "
        << in->snaprealm->srnode.past_parents << dendl;
      if (in->is_file() && in->get_projected_inode()->size > 0) {
	enqueue(dn, true); // truncate head objects    
      }
    } else {
      // A straightforward file, ready to be purged.  Enqueue it.
      if (in->is_dir()) {
	in->close_dirfrags();
      }

      enqueue(dn, false);
    }

    return true;
  } else {
    /*
     * Where a stray has some links, they should be remotes, check
     * if we can do anything with them if we happen to have them in
     * cache.
     */
    eval_remote_stray(dn, NULL);
    return false;
  }
}

bool StrayManager::eval_stray(CDentry *dn, bool delay)
{
  // avoid nested eval_stray
  if (dn->state_test(CDentry::STATE_EVALUATINGSTRAY))
      return false;

  dn->state_set(CDentry::STATE_EVALUATINGSTRAY);
  bool ret = __eval_stray(dn, delay);
  dn->state_clear(CDentry::STATE_EVALUATINGSTRAY);
  return ret;
}

void StrayManager::eval_remote_stray(CDentry *stray_dn, CDentry *remote_dn)
{
  assert(stray_dn != NULL);
  assert(stray_dn->get_dir()->get_inode()->is_stray());
  CDentry::linkage_t *stray_dnl = stray_dn->get_projected_linkage();
  assert(stray_dnl->is_primary());
  CInode *stray_in = stray_dnl->get_inode();
  assert(stray_in->inode.nlink >= 1);
  assert(stray_in->last == CEPH_NOSNAP);

  /* If no remote_dn hinted, pick one arbitrarily */
  if (remote_dn == NULL) {
    if (!stray_in->remote_parents.empty()) {
      for (compact_set<CDentry*>::iterator p = stray_in->remote_parents.begin();
	   p != stray_in->remote_parents.end();
	   ++p)
	if ((*p)->last == CEPH_NOSNAP) {
	  remote_dn = *p;
	  break;
	}
    }
    if (!remote_dn) {
      dout(20) << __func__ << ": not reintegrating (no remote parents in cache)" << dendl;
      return;
    }
  }
  assert(remote_dn->last == CEPH_NOSNAP);
    // NOTE: we repeat this check in _rename(), since our submission path is racey.
    if (!remote_dn->is_projected()) {
      if (remote_dn->is_auth() && remote_dn->dir->can_auth_pin()) {
        reintegrate_stray(stray_dn, remote_dn);
      } else if (!remote_dn->is_auth() && stray_dn->is_auth()) {
        migrate_stray(stray_dn, remote_dn->authority().first);
      } else {
        dout(20) << __func__ << ": not reintegrating" << dendl;
      }
    } else {
      // don't do anything if the remote parent is projected, or we may
      // break user-visible semantics!
      dout(20) << __func__ << ": not reintegrating (projected)" << dendl;
    }
}

void StrayManager::reintegrate_stray(CDentry *straydn, CDentry *rdn)
{
  dout(10) << __func__ << " " << *straydn << " into " << *rdn << dendl;

  logger->inc(l_mdc_strays_reintegrated);
  
  // rename it to another mds.
  filepath src;
  straydn->make_path(src);
  filepath dst;
  rdn->make_path(dst);

  MClientRequest *req = new MClientRequest(CEPH_MDS_OP_RENAME);
  req->set_filepath(dst);
  req->set_filepath2(src);
  req->set_tid(mds->issue_tid());

  mds->send_message_mds(req, rdn->authority().first);
}
 
void StrayManager::migrate_stray(CDentry *dn, mds_rank_t to)
{
  CInode *in = dn->get_linkage()->get_inode();
  assert(in);
  CInode *diri = dn->dir->get_inode();
  assert(diri->is_stray());
  dout(10) << "migrate_stray from mds." << MDS_INO_STRAY_OWNER(diri->inode.ino)
	   << " to mds." << to
	   << " " << *dn << " " << *in << dendl;

  logger->inc(l_mdc_strays_migrated);

  // rename it to another mds.
  filepath src;
  dn->make_path(src);

  string dname;
  in->name_stray_dentry(dname);
  filepath dst(dname, MDS_INO_STRAY(to, 0));

  MClientRequest *req = new MClientRequest(CEPH_MDS_OP_RENAME);
  req->set_filepath(dst);
  req->set_filepath2(src);
  req->set_tid(mds->issue_tid());

  mds->send_message_mds(req, to);
}

StrayManager::StrayManager(MDSRank *mds)
  : delayed_eval_stray(member_offset(CDentry, item_stray)),
    mds(mds), logger(NULL),
    ops_in_flight(0), files_purging(0),
    max_purge_ops(0), 
    num_strays(0), num_strays_purging(0), num_strays_delayed(0),
    filer(mds->objecter, mds->finisher)
{
  assert(mds != NULL);
}

void StrayManager::abort_queue()
{
  for (std::list<QueuedStray>::iterator i = ready_for_purge.begin();
       i != ready_for_purge.end(); ++i)
  {
    const QueuedStray &qs = *i;
    CDentry *dn = qs.dir->lookup_exact_snap(qs.name, CEPH_NOSNAP);
    if (!dn)
      continue;

    dout(10) << __func__ << ": aborting enqueued purge " << *dn << dendl;

    CDentry::linkage_t *dnl = dn->get_projected_linkage();
    assert(dnl);
    CInode *in = dnl->get_inode();
    assert(in);

    // Clear flags set in enqueue
    if (dn->state_test(CDentry::STATE_PURGINGPINNED))
      dn->put(CDentry::PIN_PURGING);
    dn->state_clear(CDentry::STATE_PURGING | CDentry::STATE_PURGINGPINNED);
    in->state_clear(CInode::STATE_PURGING);
  }
  ready_for_purge.clear();

  trimmed_strays.clear();
  fetching_strays.clear();
}

void StrayManager::truncate(CDentry *dn, uint32_t op_allowance)
{
  CDentry::linkage_t *dnl = dn->get_projected_linkage();
  CInode *in = dnl->get_inode();
  assert(in);
  dout(10) << __func__ << ": " << *dn << " " << *in << dendl;
  assert(!dn->is_replicated());

  num_strays_purging++;
  logger->set(l_mdc_num_strays_purging, num_strays_purging);

  C_GatherBuilder gather(
    g_ceph_context,
    new C_OnFinisher(new C_IO_PurgeStrayPurged(this, dn, true, 0),
		     mds->finisher));

  SnapRealm *realm = in->find_snaprealm();
  assert(realm);
  dout(10) << " realm " << *realm << dendl;
  const SnapContext *snapc = &realm->get_snap_context();

  uint64_t to = in->inode.get_max_size();
  to = MAX(in->inode.size, to);
  // when truncating a file, the filer does not delete stripe objects that are
  // truncated to zero. so we need to purge stripe objects up to the max size
  // the file has ever been.
  to = MAX(in->inode.max_size_ever, to);
  if (to > 0) {
    uint64_t num = Striper::get_num_objects(in->inode.layout, to);
    dout(10) << __func__ << " 0~" << to << " objects 0~" << num
	     << " snapc " << snapc << " on " << *in << dendl;

    // keep backtrace object
    if (num > 1) {
      filer.purge_range(in->ino(), &in->inode.layout, *snapc,
			1, num - 1, ceph::real_clock::now(g_ceph_context),
			0, gather.new_sub());
    }
    filer.zero(in->ino(), &in->inode.layout, *snapc,
	       0, in->inode.layout.object_size,
	       ceph::real_clock::now(g_ceph_context),
	       0, true, NULL, gather.new_sub());
  }

  assert(gather.has_subs());
  gather.activate();
}

void StrayManager::_truncate_stray_logged(CDentry *dn, LogSegment *ls)
{
  CInode *in = dn->get_projected_linkage()->get_inode();

  dout(10) << __func__ << ": " << *dn << " " << *in << dendl;

  dn->state_clear(CDentry::STATE_PURGING | CDentry::STATE_PURGINGPINNED);
  dn->put(CDentry::PIN_PURGING);

  in->pop_and_dirty_projected_inode(ls);

  eval_stray(dn);
}


void StrayManager::update_op_limit()
{
  uint64_t pg_count = 0;
  mds->objecter->with_osdmap([&](const OSDMap& o) {
      // Number of PGs across all data pools
      const std::set<int64_t> &data_pools = mds->mdsmap->get_data_pools();
      for (const auto dp : data_pools) {
	if (o.get_pg_pool(dp) == NULL) {
	  // It is possible that we have an older OSDMap than MDSMap,
	  // because we don't start watching every OSDMap until after
	  // MDSRank is initialized
	  dout(4) << __func__ << " data pool " << dp
		  << " not found in OSDMap" << dendl;
	  continue;
	}
	pg_count += o.get_pg_num(dp);
      }
    });

  uint64_t mds_count = mds->mdsmap->get_max_mds();

  // Work out a limit based on n_pgs / n_mdss, multiplied by the user's
  // preference for how many ops per PG
  max_purge_ops = uint64_t(((double)pg_count / (double)mds_count) *
			   g_conf->mds_max_purge_ops_per_pg);

  // User may also specify a hard limit, apply this if so.
  if (g_conf->mds_max_purge_ops) {
    max_purge_ops = MIN(max_purge_ops, g_conf->mds_max_purge_ops);
  }
}

void StrayManager::notify_stray_loaded(CDentry *dn)
{
  dout(10) << __func__ << ": " << *dn << dendl;

  dn->state_set(CDentry::STATE_STRAY);
  CInode *in = dn->get_linkage()->get_inode();
  if (in->inode.nlink == 0)
    in->state_set(CInode::STATE_ORPHAN);

  auto p = trimmed_strays.find(dn->name);
  if (p != trimmed_strays.end()) {
    dn->state_set(CDentry::STATE_PURGING);
    in->state_set(CInode::STATE_PURGING);
    trimmed_strays.erase(p);

    QueuedStray key(dn, false, 0);
    auto q = fetching_strays.find(key);
    if (q != fetching_strays.end()) {
      ready_for_purge.push_front(*q);
      fetching_strays.erase(q);
    }
  }
}

void StrayManager::notify_stray_trimmed(CDentry *dn)
{
  dout(10) << __func__ << ": " << *dn << dendl;

  trimmed_strays.insert(dn->name);
}
